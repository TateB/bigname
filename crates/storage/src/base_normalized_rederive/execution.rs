use anyhow::{Context, Result, ensure};
use sqlx::Row;

use super::{
    reverse_claim_derivation_kind, reverse_claim_source_families, subregistry_derivation_kinds,
    subregistry_source_families, unwrapped_authority_derivation_kind,
    unwrapped_authority_source_families,
};

pub(super) async fn refuse_if_bigname_runtime_sessions(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let rows = sqlx::query(
        r#"
        SELECT pid, application_name, state
        FROM pg_stat_activity
        WHERE datname = current_database()
          AND pid <> pg_backend_pid()
          AND application_name = ANY($1::TEXT[])
        ORDER BY pid
        "#,
    )
    .bind(vec![
        "bigname-indexer".to_owned(),
        "bigname-worker".to_owned(),
    ])
    .fetch_all(&mut **transaction)
    .await
    .context("failed to inspect PostgreSQL sessions before Base normalized-event rederive")?;
    ensure!(
        rows.is_empty(),
        "refusing Base normalized-event rederive while bigname runtime sessions are connected: {:?}",
        rows.iter()
            .map(|row| {
                (
                    row.get::<i32, _>("pid"),
                    row.get::<String, _>("application_name"),
                    row.get::<String, _>("state"),
                )
            })
            .collect::<Vec<_>>()
    );
    Ok(())
}

pub(super) async fn create_scope_tables(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    replay_target_block: i64,
) -> Result<()> {
    for table in [
        "base_rederive_scope_normalized_events",
        "base_rederive_scope_resources",
        "base_rederive_scope_token_lineages",
        "base_rederive_scope_name_surfaces",
        "base_rederive_scope_surface_bindings",
    ] {
        sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
            .execute(&mut **transaction)
            .await
            .with_context(|| format!("failed to drop temporary scope table {table}"))?;
    }

    execute(transaction, "CREATE TEMP TABLE base_rederive_scope_normalized_events (normalized_event_id BIGINT PRIMARY KEY) ON COMMIT DROP").await?;
    execute(transaction, "CREATE TEMP TABLE base_rederive_scope_resources (resource_id UUID PRIMARY KEY) ON COMMIT DROP").await?;
    execute(transaction, "CREATE TEMP TABLE base_rederive_scope_token_lineages (token_lineage_id UUID PRIMARY KEY) ON COMMIT DROP").await?;
    execute(transaction, "CREATE TEMP TABLE base_rederive_scope_name_surfaces (logical_name_id TEXT PRIMARY KEY) ON COMMIT DROP").await?;
    execute(transaction, "CREATE TEMP TABLE base_rederive_scope_surface_bindings (surface_binding_id UUID PRIMARY KEY) ON COMMIT DROP").await?;

    sqlx::query(
        r#"
        INSERT INTO base_rederive_scope_normalized_events (normalized_event_id)
        SELECT normalized_event_id
        FROM normalized_events
        WHERE chain_id = 'base-mainnet'
          AND block_number BETWEEN 17571485 AND $1
          AND block_hash IS NOT NULL
          AND (
              (derivation_kind = $2 AND source_family = ANY($3::TEXT[]))
              OR (derivation_kind = ANY($4::TEXT[]) AND source_family = ANY($5::TEXT[]))
              OR (derivation_kind = $6 AND source_family = ANY($7::TEXT[]))
          )
        "#,
    )
    .bind(replay_target_block)
    .bind(reverse_claim_derivation_kind())
    .bind(reverse_claim_source_families())
    .bind(subregistry_derivation_kinds())
    .bind(subregistry_source_families())
    .bind(unwrapped_authority_derivation_kind())
    .bind(unwrapped_authority_source_families())
    .execute(&mut **transaction)
    .await
    .context("failed to materialize Base normalized-event rederive event scope")?;
    execute(transaction, "INSERT INTO base_rederive_scope_resources SELECT resource_id FROM resources WHERE chain_id = 'base-mainnet' AND provenance->>'adapter' = 'ens_v1_unwrapped_authority'").await?;
    execute(transaction, "INSERT INTO base_rederive_scope_token_lineages SELECT token_lineage_id FROM token_lineages WHERE chain_id = 'base-mainnet' AND provenance->>'adapter' = 'ens_v1_unwrapped_authority'").await?;
    execute(transaction, "INSERT INTO base_rederive_scope_name_surfaces SELECT logical_name_id FROM name_surfaces WHERE chain_id = 'base-mainnet' AND provenance->>'adapter' = 'ens_v1_unwrapped_authority'").await?;
    execute(transaction, "INSERT INTO base_rederive_scope_surface_bindings SELECT surface_binding_id FROM surface_bindings WHERE chain_id = 'base-mainnet' AND provenance->>'adapter' = 'ens_v1_unwrapped_authority'").await?;
    Ok(())
}

pub(super) async fn refuse_if_out_of_scope_identity_dependencies(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    let row = sqlx::query(
        r#"
        SELECT
            (
                SELECT COUNT(*)::BIGINT
                FROM resources resource
                JOIN base_rederive_scope_token_lineages token
                  ON token.token_lineage_id = resource.token_lineage_id
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM base_rederive_scope_resources scoped
                    WHERE scoped.resource_id = resource.resource_id
                )
            ) AS resources_blocking_token_lineages,
            (
                SELECT COUNT(*)::BIGINT
                FROM surface_bindings binding
                WHERE (
                    EXISTS (
                        SELECT 1
                        FROM base_rederive_scope_resources scoped
                        WHERE scoped.resource_id = binding.resource_id
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM base_rederive_scope_name_surfaces scoped
                        WHERE scoped.logical_name_id = binding.logical_name_id
                    )
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM base_rederive_scope_surface_bindings scoped
                    WHERE scoped.surface_binding_id = binding.surface_binding_id
                )
            ) AS surface_bindings_blocking_identity,
            (
                SELECT COUNT(*)::BIGINT
                FROM normalized_events event
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM base_rederive_scope_normalized_events scoped
                    WHERE scoped.normalized_event_id = event.normalized_event_id
                )
                AND (
                    EXISTS (
                        SELECT 1
                        FROM base_rederive_scope_resources scoped
                        WHERE scoped.resource_id = event.resource_id
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM base_rederive_scope_name_surfaces scoped
                        WHERE scoped.logical_name_id = event.logical_name_id
                    )
                )
            ) AS remaining_events_referencing_identity
        "#,
    )
    .fetch_one(&mut **transaction)
    .await
    .context("failed to inspect out-of-scope identity dependencies")?;
    let resources_blocking: i64 = row.try_get("resources_blocking_token_lineages")?;
    let bindings_blocking: i64 = row.try_get("surface_bindings_blocking_identity")?;
    let events_blocking: i64 = row.try_get("remaining_events_referencing_identity")?;
    ensure!(
        resources_blocking == 0 && bindings_blocking == 0 && events_blocking == 0,
        "Base normalized-event rederive scope has out-of-scope identity dependencies: resources_blocking_token_lineages={resources_blocking}, surface_bindings_blocking_identity={bindings_blocking}, remaining_events_referencing_identity={events_blocking}"
    );
    Ok(())
}

async fn execute(transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>, sql: &str) -> Result<()> {
    sqlx::query(sql)
        .execute(&mut **transaction)
        .await
        .with_context(|| format!("failed to execute Base normalized-event rederive SQL: {sql}"))?;
    Ok(())
}
