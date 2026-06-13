use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{PgPool, Row};
use tokio::{
    task::JoinHandle,
    time::{MissedTickBehavior, interval, timeout},
};
use uuid::Uuid;

use crate::{
    address_names, children, name_current, permissions, primary_name, record_inventory, resolver,
};

use super::{
    CLAIM_RETRY_DELAY, FAILURE_RETRY_DELAY,
    apply_locks::{
        acquire_invalidation_apply_locks, ensure_invalidation_apply_locks_alive,
        release_invalidation_apply_locks,
    },
    dead_letters::dead_letter_invalidation,
};

const NAME_CURRENT_SINGLE_APPLY_TIMEOUT: Duration = Duration::from_secs(120);
const CLAIM_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);
const MAX_PROJECTION_INVALIDATION_ATTEMPTS: i64 = 5;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct ProjectionInvalidationApplySummary {
    pub(super) claimed_invalidation_count: usize,
    pub(super) applied_invalidation_count: usize,
    pub(super) failed_invalidation_count: usize,
}

#[derive(Clone, Debug)]
pub(super) struct ClaimedInvalidation {
    pub(super) projection: String,
    pub(super) projection_key: String,
    pub(super) key_payload: Value,
    pub(super) generation: i64,
    pub(super) claim_token: Uuid,
    pub(super) attempt_count: i64,
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
    let mut invalidations = claim_pending_invalidations(pool, batch_limit, claim_token).await?;
    sort_claimed_invalidations_for_apply(&mut invalidations);
    let heartbeats = spawn_claim_heartbeats(pool, &invalidations);
    let result = async {
        let mut summary = ProjectionInvalidationApplySummary {
            claimed_invalidation_count: invalidations.len(),
            ..ProjectionInvalidationApplySummary::default()
        };
        while !invalidations.is_empty() {
            if invalidations[0].projection == "address_names_current" {
                let group = drain_address_names_group(&mut invalidations);
                let group_len = group.len();
                let mut locks = acquire_invalidation_apply_locks(pool, &group).await?;
                let result = apply_address_names_group(pool, &group).await;
                let finish = async {
                    ensure_invalidation_apply_locks_alive(&mut locks).await?;
                    match result {
                        Ok(()) => {
                            for invalidation in &group {
                                complete_invalidation(pool, invalidation).await?;
                            }
                            summary.applied_invalidation_count += group_len;
                        }
                        Err(error) => {
                            for invalidation in &group {
                                fail_invalidation(pool, invalidation, &error).await?;
                            }
                            summary.failed_invalidation_count += group_len;
                        }
                    }
                    Ok::<(), anyhow::Error>(())
                }
                .await;
                let unlock = release_invalidation_apply_locks(&mut locks).await;
                finish?;
                unlock?;
                continue;
            }

            let invalidation = invalidations.remove(0);
            let mut locks =
                acquire_invalidation_apply_locks(pool, std::slice::from_ref(&invalidation)).await?;
            let result = if invalidation.projection == "name_current" {
                apply_name_current_single(pool, &invalidation).await
            } else {
                apply_one(pool, &invalidation, text_hydration_config).await
            };
            let finish = async {
                ensure_invalidation_apply_locks_alive(&mut locks).await?;
                match result {
                    Ok(()) => {
                        complete_invalidation(pool, &invalidation).await?;
                        summary.applied_invalidation_count += 1;
                    }
                    Err(error) => {
                        fail_invalidation(pool, &invalidation, &error).await?;
                        summary.failed_invalidation_count += 1;
                    }
                }
                Ok::<(), anyhow::Error>(())
            }
            .await;
            let unlock = release_invalidation_apply_locks(&mut locks).await;
            finish?;
            unlock?;
        }

        Ok(summary)
    }
    .await;
    stop_claim_heartbeats(heartbeats).await;
    result
}

fn drain_address_names_group(
    invalidations: &mut Vec<ClaimedInvalidation>,
) -> Vec<ClaimedInvalidation> {
    let Ok(address) = address_names_invalidation_address(&invalidations[0]) else {
        return invalidations.drain(..1).collect();
    };
    let address = address.to_owned();
    let split_at = invalidations
        .iter()
        .position(|invalidation| {
            invalidation.projection != "address_names_current"
                || address_names_invalidation_address(invalidation)
                    .map(|candidate| candidate != address)
                    .unwrap_or(true)
        })
        .unwrap_or(invalidations.len());

    invalidations.drain(..split_at).collect()
}

fn spawn_claim_heartbeats(
    pool: &PgPool,
    invalidations: &[ClaimedInvalidation],
) -> Vec<JoinHandle<()>> {
    invalidations
        .iter()
        .cloned()
        .map(|invalidation| spawn_claim_heartbeat(pool.clone(), invalidation))
        .collect()
}

async fn apply_address_names_group(
    pool: &PgPool,
    invalidations: &[ClaimedInvalidation],
) -> Result<()> {
    let Some(first) = invalidations.first() else {
        return Ok(());
    };
    let address = address_names_invalidation_address(first)?;
    if invalidations.iter().any(|invalidation| {
        optional_payload_str(&invalidation.key_payload, "logical_name_id").is_none()
    }) {
        address_names::rebuild_address_names_current(pool, Some(address)).await?;
        return Ok(());
    }

    let logical_name_ids = invalidations
        .iter()
        .map(|invalidation| payload_str(&invalidation.key_payload, "logical_name_id"))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    address_names::rebuild_address_names_current_logical_names(pool, address, &logical_name_ids)
        .await?;
    Ok(())
}

fn address_names_invalidation_address(invalidation: &ClaimedInvalidation) -> Result<&str> {
    optional_payload_str(&invalidation.key_payload, "address")
        .or_else(|| nonblank_str(&invalidation.projection_key))
        .context("address_names_current invalidation missing address")
}

fn spawn_claim_heartbeat(pool: PgPool, invalidation: ClaimedInvalidation) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut heartbeat = interval(CLAIM_HEARTBEAT_INTERVAL);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            heartbeat.tick().await;
            if let Err(error) = refresh_claimed_invalidation_claim(&pool, &invalidation).await {
                tracing::warn!(
                    projection = %invalidation.projection,
                    projection_key = %invalidation.projection_key,
                    error = %error,
                    "failed to refresh projection invalidation claim heartbeat"
                );
            }
        }
    })
}

async fn stop_claim_heartbeats(mut heartbeats: Vec<JoinHandle<()>>) {
    while let Some(heartbeat) = heartbeats.pop() {
        heartbeat.abort();
        let _ = heartbeat.await;
    }
}

async fn apply_name_current_single(
    pool: &PgPool,
    invalidation: &ClaimedInvalidation,
) -> Result<()> {
    timeout(
        NAME_CURRENT_SINGLE_APPLY_TIMEOUT,
        name_current::rebuild_name_current(pool, Some(&invalidation.projection_key)),
    )
    .await
    .with_context(|| {
        format!(
            "timed out applying name_current invalidation {}",
            invalidation.projection_key
        )
    })??;
    Ok(())
}

async fn refresh_claimed_invalidation_claim(
    pool: &PgPool,
    invalidation: &ClaimedInvalidation,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE projection_invalidations
        SET claimed_at = now()
        WHERE projection = $1
          AND projection_key = $2
          AND claim_token = $3
        "#,
    )
    .bind(&invalidation.projection)
    .bind(&invalidation.projection_key)
    .bind(invalidation.claim_token)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to refresh projection invalidation claim {}:{}",
            invalidation.projection, invalidation.projection_key
        )
    })?;

    Ok(())
}

fn sort_claimed_invalidations_for_apply(invalidations: &mut [ClaimedInvalidation]) {
    invalidations.sort_by(|left, right| {
        claimed_invalidation_apply_key(left).cmp(&claimed_invalidation_apply_key(right))
    });
}

fn claimed_invalidation_apply_key(invalidation: &ClaimedInvalidation) -> (u16, u8, &str) {
    let family_rank = match invalidation.projection.as_str() {
        "name_current" => 10,
        "children_current" => 20,
        "permissions_current" => 30,
        "record_inventory_current" => 40,
        "resolver_current" => 50,
        "address_names_current" => 60,
        "primary_names_current" => 70,
        _ => 1000,
    };
    let namespace_rank = if invalidation.projection == "name_current"
        && invalidation.projection_key.starts_with("basenames:")
    {
        0
    } else {
        1
    };

    (
        family_rank,
        namespace_rank,
        invalidation.projection_key.as_str(),
    )
}

async fn claim_pending_invalidations(
    pool: &PgPool,
    batch_limit: i64,
    claim_token: Uuid,
) -> Result<Vec<ClaimedInvalidation>> {
    let rows = sqlx::query(
        r#"
        WITH unclaimed_candidates AS (
            SELECT
                projection,
                projection_key,
                CASE projection
                    WHEN 'name_current' THEN 10
                    WHEN 'children_current' THEN 20
                    WHEN 'permissions_current' THEN 30
                    WHEN 'record_inventory_current' THEN 40
                    WHEN 'resolver_current' THEN 50
                    WHEN 'address_names_current' THEN 60
                    WHEN 'primary_names_current' THEN 70
                    ELSE 1000
                END AS projection_priority,
                CASE
                    WHEN projection = 'name_current'
                     AND projection_key LIKE 'basenames:%' THEN 0
                    ELSE 1
                END AS namespace_priority,
                last_changed_at
            FROM projection_invalidations
            WHERE claim_token IS NULL
              AND state = 'pending'::projection_invalidation_state
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
                CASE
                    WHEN projection = 'name_current'
                     AND projection_key LIKE 'basenames:%' THEN 0
                    ELSE 1
                END,
                last_changed_at ASC,
                projection_key ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        ),
        stale_claim_candidates AS (
            SELECT
                projection,
                projection_key,
                CASE projection
                    WHEN 'name_current' THEN 10
                    WHEN 'children_current' THEN 20
                    WHEN 'permissions_current' THEN 30
                    WHEN 'record_inventory_current' THEN 40
                    WHEN 'resolver_current' THEN 50
                    WHEN 'address_names_current' THEN 60
                    WHEN 'primary_names_current' THEN 70
                    ELSE 1000
                END AS projection_priority,
                CASE
                    WHEN projection = 'name_current'
                     AND projection_key LIKE 'basenames:%' THEN 0
                    ELSE 1
                END AS namespace_priority,
                last_changed_at
            FROM projection_invalidations
            WHERE claim_token IS NOT NULL
              AND claimed_at < now() - $3::INTERVAL
              AND state = 'pending'::projection_invalidation_state
              AND (
                  last_failure_at IS NULL
                  OR last_failure_at < now() - $2::INTERVAL
              )
            ORDER BY
                claimed_at ASC,
                projection ASC,
                projection_key ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        ),
        candidates AS (
            SELECT projection, projection_key
            FROM (
                SELECT * FROM unclaimed_candidates
                UNION ALL
                SELECT * FROM stale_claim_candidates
            ) candidate
            ORDER BY
                projection_priority ASC,
                namespace_priority ASC,
                last_changed_at ASC,
                projection_key ASC
            LIMIT $1
        )
        UPDATE projection_invalidations invalidation
        SET
            claim_token = $4,
            claimed_at = now()
        FROM candidates
        WHERE invalidation.projection = candidates.projection
          AND invalidation.projection_key = candidates.projection_key
        RETURNING
            invalidation.projection,
            invalidation.projection_key,
            invalidation.key_payload,
            invalidation.generation,
            invalidation.claim_token,
            invalidation.attempt_count
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
                attempt_count: row.try_get("attempt_count")?,
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
            if let Some(logical_name_id) =
                optional_payload_str(&invalidation.key_payload, "logical_name_id")
            {
                let address = payload_str(&invalidation.key_payload, "address")?;
                address_names::rebuild_address_names_current_logical_name(
                    pool,
                    address,
                    logical_name_id,
                )
                .await?;
            } else {
                address_names::rebuild_address_names_current(
                    pool,
                    Some(&invalidation.projection_key),
                )
                .await?;
            }
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

fn optional_payload_str<'a>(payload: &'a Value, field: &str) -> Option<&'a str> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
}

fn nonblank_str(value: &str) -> Option<&str> {
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

async fn complete_invalidation(pool: &PgPool, invalidation: &ClaimedInvalidation) -> Result<()> {
    let result = sqlx::query(
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
    if result.rows_affected() == 0 {
        release_superseded_claim(pool, invalidation).await?;
    }

    Ok(())
}

async fn fail_invalidation(
    pool: &PgPool,
    invalidation: &ClaimedInvalidation,
    error: &anyhow::Error,
) -> Result<()> {
    let failure_reason = postgres_text_safe(&format!("{error:#}"));
    let failed_attempt_count = invalidation.attempt_count + 1;
    let should_dead_letter = failed_attempt_count >= MAX_PROJECTION_INVALIDATION_ATTEMPTS;
    let rows_affected = if should_dead_letter {
        dead_letter_invalidation(pool, invalidation, &failure_reason, failed_attempt_count).await?
    } else {
        sqlx::query(
            r#"
        UPDATE projection_invalidations
        SET
            claim_token = NULL,
            claimed_at = NULL,
            attempt_count = $6,
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
        .bind(&failure_reason)
        .bind(failed_attempt_count)
        .execute(pool)
        .await
        .with_context(|| {
            format!(
                "failed to record projection invalidation failure {}:{}",
                invalidation.projection, invalidation.projection_key
            )
        })?
        .rows_affected()
    };
    if should_dead_letter && rows_affected > 0 {
        tracing::warn!(
            projection = %invalidation.projection,
            projection_key = %invalidation.projection_key,
            generation = invalidation.generation,
            failed_attempt_count = failed_attempt_count,
            max_attempts = MAX_PROJECTION_INVALIDATION_ATTEMPTS,
            failure_reason = %failure_reason,
            "moved projection invalidation to dead letter"
        );
    }
    if rows_affected == 0 {
        release_superseded_claim(pool, invalidation).await?;
    }

    Ok(())
}

async fn release_superseded_claim(pool: &PgPool, invalidation: &ClaimedInvalidation) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE projection_invalidations
        SET claim_token = NULL,
            claimed_at = NULL
        WHERE projection = $1
          AND projection_key = $2
          AND generation > $3
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
            "failed to release superseded projection invalidation claim {}:{}",
            invalidation.projection, invalidation.projection_key
        )
    })?;

    Ok(())
}

fn postgres_text_safe(text: &str) -> String {
    text.replace('\0', "\\u0000")
}

#[cfg(test)]
#[path = "apply/tests.rs"]
mod tests;
