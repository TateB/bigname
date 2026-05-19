use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::{
    address_names, children, name_current, permissions, primary_name, record_inventory, resolver,
};

const FAILURE_RETRY_DELAY: &str = "60 seconds";
const CLAIM_RETRY_DELAY: &str = "5 minutes";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct ProjectionInvalidationApplySummary {
    pub(super) claimed_invalidation_count: usize,
    pub(super) applied_invalidation_count: usize,
    pub(super) failed_invalidation_count: usize,
}

#[derive(Clone, Debug)]
struct ClaimedInvalidation {
    projection: String,
    projection_key: String,
    key_payload: Value,
    generation: i64,
    claim_token: Uuid,
}

pub(super) async fn apply_pending_invalidations(
    pool: &PgPool,
    batch_limit: i64,
    text_hydration_config: Option<&record_inventory::RecordInventoryTextHydrationConfig>,
) -> Result<ProjectionInvalidationApplySummary> {
    if batch_limit <= 0 {
        bail!("projection apply batch limit must be positive, got {batch_limit}");
    }

    let claim_token = Uuid::new_v4();
    let invalidations = claim_pending_invalidations(pool, batch_limit, claim_token).await?;
    let mut summary = ProjectionInvalidationApplySummary {
        claimed_invalidation_count: invalidations.len(),
        ..ProjectionInvalidationApplySummary::default()
    };

    for invalidation in invalidations {
        match apply_one(pool, &invalidation, text_hydration_config).await {
            Ok(()) => {
                complete_invalidation(pool, &invalidation).await?;
                summary.applied_invalidation_count += 1;
            }
            Err(error) => {
                fail_invalidation(pool, &invalidation, &error).await?;
                summary.failed_invalidation_count += 1;
            }
        }
    }

    Ok(summary)
}

async fn claim_pending_invalidations(
    pool: &PgPool,
    batch_limit: i64,
    claim_token: Uuid,
) -> Result<Vec<ClaimedInvalidation>> {
    let rows = sqlx::query(
        r#"
        WITH candidates AS (
            SELECT projection, projection_key
            FROM projection_invalidations
            WHERE (
                  claim_token IS NULL
                  OR claimed_at < now() - $3::INTERVAL
              )
              AND (
                  last_failure_at IS NULL
                  OR last_failure_at < now() - $2::INTERVAL
              )
            ORDER BY
                CASE projection
                    WHEN 'name_current' THEN 10
                    WHEN 'children_current' THEN 20
                    WHEN 'permissions_current' THEN 30
                    WHEN 'record_inventory_current' THEN 40
                    WHEN 'resolver_current' THEN 50
                    WHEN 'address_names_current' THEN 60
                    WHEN 'primary_names_current' THEN 70
                    ELSE 1000
                END,
                last_changed_at ASC,
                projection_key ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE projection_invalidations invalidation
        SET
            claim_token = $4,
            claimed_at = now(),
            attempt_count = attempt_count + 1
        FROM candidates
        WHERE invalidation.projection = candidates.projection
          AND invalidation.projection_key = candidates.projection_key
        RETURNING
            invalidation.projection,
            invalidation.projection_key,
            invalidation.key_payload,
            invalidation.generation,
            invalidation.claim_token
        "#,
    )
    .bind(batch_limit)
    .bind(FAILURE_RETRY_DELAY)
    .bind(CLAIM_RETRY_DELAY)
    .bind(claim_token)
    .fetch_all(pool)
    .await
    .context("failed to claim projection invalidations")?;

    rows.into_iter()
        .map(|row| {
            Ok(ClaimedInvalidation {
                projection: row.try_get("projection")?,
                projection_key: row.try_get("projection_key")?,
                key_payload: row.try_get("key_payload")?,
                generation: row.try_get("generation")?,
                claim_token: row.try_get("claim_token")?,
            })
        })
        .collect()
}

async fn apply_one(
    pool: &PgPool,
    invalidation: &ClaimedInvalidation,
    text_hydration_config: Option<&record_inventory::RecordInventoryTextHydrationConfig>,
) -> Result<()> {
    match invalidation.projection.as_str() {
        "name_current" => {
            name_current::rebuild_name_current(pool, Some(&invalidation.projection_key)).await?;
        }
        "children_current" => {
            children::rebuild_children_current(pool, Some(&invalidation.projection_key)).await?;
        }
        "permissions_current" => {
            permissions::rebuild_permissions_current(pool, Some(&invalidation.projection_key))
                .await?;
        }
        "record_inventory_current" => {
            record_inventory::rebuild_record_inventory_current(
                pool,
                Some(&invalidation.projection_key),
            )
            .await?;
            if let Some(config) = text_hydration_config {
                let hydration_summary = record_inventory::hydrate_record_inventory_text_values(
                    pool,
                    Some(&invalidation.projection_key),
                    config.clone(),
                )
                .await?;
                record_inventory::log_text_hydration_summary(
                    Some(&invalidation.projection_key),
                    &hydration_summary,
                );
            }
        }
        "resolver_current" => {
            let chain_id = payload_str(&invalidation.key_payload, "chain_id")?;
            let resolver_address = payload_str(&invalidation.key_payload, "resolver_address")?;
            resolver::rebuild_resolver_current(pool, Some(chain_id), Some(resolver_address))
                .await?;
        }
        "address_names_current" => {
            address_names::rebuild_address_names_current(pool, Some(&invalidation.projection_key))
                .await?;
        }
        "primary_names_current" => {
            let address = payload_str(&invalidation.key_payload, "address")?;
            let namespace = payload_str(&invalidation.key_payload, "namespace")?;
            let coin_type = payload_str(&invalidation.key_payload, "coin_type")?;
            primary_name::rebuild_primary_names_current(
                pool,
                Some(address),
                Some(namespace),
                Some(coin_type),
            )
            .await?;
        }
        projection => bail!("unsupported projection invalidation family {projection}"),
    }

    Ok(())
}

fn payload_str<'a>(payload: &'a Value, field: &str) -> Result<&'a str> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("projection invalidation payload missing {field}"))
}

async fn complete_invalidation(pool: &PgPool, invalidation: &ClaimedInvalidation) -> Result<()> {
    sqlx::query(
        r#"
        DELETE FROM projection_invalidations
        WHERE projection = $1
          AND projection_key = $2
          AND generation = $3
          AND claim_token = $4
        "#,
    )
    .bind(&invalidation.projection)
    .bind(&invalidation.projection_key)
    .bind(invalidation.generation)
    .bind(invalidation.claim_token)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to complete projection invalidation {}:{}",
            invalidation.projection, invalidation.projection_key
        )
    })?;

    Ok(())
}

async fn fail_invalidation(
    pool: &PgPool,
    invalidation: &ClaimedInvalidation,
    error: &anyhow::Error,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE projection_invalidations
        SET
            claim_token = NULL,
            claimed_at = NULL,
            last_failure_reason = $5,
            last_failure_at = now()
        WHERE projection = $1
          AND projection_key = $2
          AND generation = $3
          AND claim_token = $4
        "#,
    )
    .bind(&invalidation.projection)
    .bind(&invalidation.projection_key)
    .bind(invalidation.generation)
    .bind(invalidation.claim_token)
    .bind(postgres_text_safe(&format!("{error:#}")))
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to record projection invalidation failure {}:{}",
            invalidation.projection, invalidation.projection_key
        )
    })?;

    Ok(())
}

fn postgres_text_safe(text: &str) -> String {
    text.replace('\0', "\\u0000")
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};

    async fn test_database() -> Result<TestDatabase> {
        TestDatabase::create_migrated(
            TestDatabaseConfig::new("bigname_worker_projection_apply_claim_test")
                .admin_database("postgres")
                .pool_max_connections(5)
                .parse_context("failed to parse database URL for projection apply claim tests")
                .admin_connect_context(
                    "failed to connect admin pool for projection apply claim tests",
                )
                .pool_connect_context("failed to connect projection apply claim test pool"),
            &bigname_storage::MIGRATOR,
            "failed to apply migrations for projection apply claim tests",
        )
        .await
    }

    #[tokio::test]
    async fn stale_projection_invalidation_claims_are_reclaimed() -> Result<()> {
        let database = test_database().await?;
        let stale_token = Uuid::new_v4();
        let new_token = Uuid::new_v4();

        insert_claimed_invalidation(
            &database,
            "name_current",
            "ens:stale.eth",
            stale_token,
            "10 minutes",
        )
        .await?;

        let claimed = claim_pending_invalidations(database.pool(), 10, new_token).await?;
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].projection, "name_current");
        assert_eq!(claimed[0].projection_key, "ens:stale.eth");
        assert_eq!(claimed[0].claim_token, new_token);

        let (claim_token, attempt_count): (Uuid, i64) = sqlx::query_as(
            r#"
            SELECT claim_token, attempt_count
            FROM projection_invalidations
            WHERE projection = 'name_current'
              AND projection_key = 'ens:stale.eth'
            "#,
        )
        .fetch_one(database.pool())
        .await
        .context("failed to load reclaimed projection invalidation")?;
        assert_eq!(claim_token, new_token);
        assert_eq!(attempt_count, 1);

        database.cleanup().await
    }

    #[tokio::test]
    async fn fresh_projection_invalidation_claims_are_not_reclaimed() -> Result<()> {
        let database = test_database().await?;
        let fresh_token = Uuid::new_v4();

        insert_claimed_invalidation(
            &database,
            "name_current",
            "ens:fresh.eth",
            fresh_token,
            "1 minute",
        )
        .await?;

        let claimed = claim_pending_invalidations(database.pool(), 10, Uuid::new_v4()).await?;
        assert!(claimed.is_empty());

        let claim_token: Uuid = sqlx::query_scalar(
            r#"
            SELECT claim_token
            FROM projection_invalidations
            WHERE projection = 'name_current'
              AND projection_key = 'ens:fresh.eth'
            "#,
        )
        .fetch_one(database.pool())
        .await
        .context("failed to load fresh projection invalidation")?;
        assert_eq!(claim_token, fresh_token);

        database.cleanup().await
    }

    async fn insert_claimed_invalidation(
        database: &TestDatabase,
        projection: &str,
        projection_key: &str,
        claim_token: Uuid,
        claim_age: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO projection_invalidations (
                projection,
                projection_key,
                key_payload,
                claim_token,
                claimed_at
            )
            VALUES ($1, $2, '{}'::jsonb, $3, now() - $4::INTERVAL)
            "#,
        )
        .bind(projection)
        .bind(projection_key)
        .bind(claim_token)
        .bind(claim_age)
        .execute(database.pool())
        .await
        .context("failed to insert claimed projection invalidation")?;

        Ok(())
    }
}
