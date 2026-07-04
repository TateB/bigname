use anyhow::{Context, Result, ensure};
use sqlx::{PgPool, Row};

use super::{
    BASE_NORMALIZED_REDERIVE_CHAIN_ID, BASE_NORMALIZED_REDERIVE_CURSOR_KIND,
    BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK, BASE_NORMALIZED_REDERIVE_REPLAY_TARGET_BLOCK,
    BaseNormalizedRederiveCounts, checkpoint_adapters, expected_manifest_ids,
};

pub(super) async fn refuse_if_bigname_runtime_sessions(pool: &PgPool) -> Result<()> {
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
    .fetch_all(pool)
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
          AND source_manifest_id = ANY($1::BIGINT[])
          AND block_number BETWEEN 17571485 AND 46954147
          AND block_hash IS NOT NULL
          AND derivation_kind NOT IN ('manifest_sync', 'manifest_alert')
        "#,
    )
    .bind(expected_manifest_ids())
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

pub(super) async fn delete_scoped_rows_and_reset_replay(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deployment_profile: &str,
) -> Result<BaseNormalizedRederiveCounts> {
    let address_names_current = delete_count(
        transaction,
        identity_projection_delete_sql("address_names_current"),
    )
    .await?;
    let name_current =
        delete_count(transaction, identity_projection_delete_sql("name_current")).await?;
    let children_current = delete_count(transaction, "DELETE FROM children_current p WHERE EXISTS (SELECT 1 FROM base_rederive_scope_name_surfaces s WHERE s.logical_name_id IN (p.parent_logical_name_id, p.child_logical_name_id))").await?;
    let permissions_current = delete_count(transaction, "DELETE FROM permissions_current p WHERE EXISTS (SELECT 1 FROM base_rederive_scope_resources s WHERE s.resource_id = p.resource_id)").await?;
    let record_inventory_current = delete_count(transaction, "DELETE FROM record_inventory_current p WHERE EXISTS (SELECT 1 FROM base_rederive_scope_resources s WHERE s.resource_id = p.resource_id)").await?;
    let projection_normalized_event_changes = delete_count(transaction, "DELETE FROM projection_normalized_event_changes p WHERE EXISTS (SELECT 1 FROM base_rederive_scope_normalized_events s WHERE s.normalized_event_id = p.normalized_event_id)").await?;
    let normalized_events = delete_count(transaction, "DELETE FROM normalized_events p WHERE EXISTS (SELECT 1 FROM base_rederive_scope_normalized_events s WHERE s.normalized_event_id = p.normalized_event_id)").await?;
    let surface_bindings = delete_count(transaction, "DELETE FROM surface_bindings p WHERE EXISTS (SELECT 1 FROM base_rederive_scope_surface_bindings s WHERE s.surface_binding_id = p.surface_binding_id)").await?;
    let resources = delete_count(transaction, "DELETE FROM resources p WHERE EXISTS (SELECT 1 FROM base_rederive_scope_resources s WHERE s.resource_id = p.resource_id)").await?;
    let name_surfaces = delete_count(transaction, "DELETE FROM name_surfaces p WHERE EXISTS (SELECT 1 FROM base_rederive_scope_name_surfaces s WHERE s.logical_name_id = p.logical_name_id)").await?;
    let token_lineages = delete_count(transaction, "DELETE FROM token_lineages p WHERE EXISTS (SELECT 1 FROM base_rederive_scope_token_lineages s WHERE s.token_lineage_id = p.token_lineage_id)").await?;
    let adapter_checkpoint_item_rows =
        delete_replay_checkpoint_items(transaction, deployment_profile).await?;
    let adapter_checkpoint_rows =
        delete_replay_checkpoints(transaction, deployment_profile).await?;
    let replay_cursor_rows = reset_replay_cursor(transaction, deployment_profile).await?;
    Ok(BaseNormalizedRederiveCounts {
        normalized_events,
        resources,
        token_lineages,
        name_surfaces,
        surface_bindings,
        name_current,
        address_names_current,
        children_current,
        permissions_current,
        record_inventory_current,
        projection_normalized_event_changes,
        replay_cursor_rows,
        adapter_checkpoint_rows,
        adapter_checkpoint_item_rows,
    })
}

async fn delete_replay_checkpoint_items(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deployment_profile: &str,
) -> Result<i64> {
    delete_count_bound(
        transaction,
        r#"
        DELETE FROM normalized_replay_adapter_checkpoint_items
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = ANY($4::TEXT[])
        "#,
        deployment_profile,
    )
    .await
}

async fn delete_replay_checkpoints(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deployment_profile: &str,
) -> Result<i64> {
    delete_count_bound(
        transaction,
        r#"
        DELETE FROM normalized_replay_adapter_checkpoints
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND adapter = ANY($4::TEXT[])
        "#,
        deployment_profile,
    )
    .await
}

async fn reset_replay_cursor(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    deployment_profile: &str,
) -> Result<i64> {
    let result = sqlx::query(
        r#"
        DELETE FROM normalized_replay_cursors
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
        "#,
    )
    .bind(deployment_profile)
    .bind(BASE_NORMALIZED_REDERIVE_CHAIN_ID)
    .bind(BASE_NORMALIZED_REDERIVE_CURSOR_KIND)
    .execute(&mut **transaction)
    .await
    .context("failed to delete Base normalized-event replay cursor")?;
    let deleted = i64::try_from(result.rows_affected()).context("delete count overflowed i64")?;
    sqlx::query(
        r#"
        INSERT INTO normalized_replay_cursors (
            deployment_profile,
            chain_id,
            cursor_kind,
            range_start_block_number,
            next_block_number,
            target_block_number
        )
        VALUES ($1, $2, $3, $4, $4, $5)
        "#,
    )
    .bind(deployment_profile)
    .bind(BASE_NORMALIZED_REDERIVE_CHAIN_ID)
    .bind(BASE_NORMALIZED_REDERIVE_CURSOR_KIND)
    .bind(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK)
    .bind(BASE_NORMALIZED_REDERIVE_REPLAY_TARGET_BLOCK)
    .execute(&mut **transaction)
    .await
    .context("failed to reset Base normalized-event replay cursor")?;
    Ok(deleted)
}

fn identity_projection_delete_sql(table: &'static str) -> &'static str {
    match table {
        "address_names_current" => {
            r#"
            DELETE FROM address_names_current p
            WHERE EXISTS (SELECT 1 FROM base_rederive_scope_resources s WHERE s.resource_id = p.resource_id)
               OR EXISTS (SELECT 1 FROM base_rederive_scope_token_lineages s WHERE s.token_lineage_id = p.token_lineage_id)
               OR EXISTS (SELECT 1 FROM base_rederive_scope_name_surfaces s WHERE s.logical_name_id = p.logical_name_id)
               OR EXISTS (SELECT 1 FROM base_rederive_scope_surface_bindings s WHERE s.surface_binding_id = p.surface_binding_id)
            "#
        }
        "name_current" => {
            r#"
            DELETE FROM name_current p
            WHERE EXISTS (SELECT 1 FROM base_rederive_scope_resources s WHERE s.resource_id = p.resource_id)
               OR EXISTS (SELECT 1 FROM base_rederive_scope_token_lineages s WHERE s.token_lineage_id = p.token_lineage_id)
               OR EXISTS (SELECT 1 FROM base_rederive_scope_name_surfaces s WHERE s.logical_name_id = p.logical_name_id)
               OR EXISTS (SELECT 1 FROM base_rederive_scope_surface_bindings s WHERE s.surface_binding_id = p.surface_binding_id)
            "#
        }
        _ => unreachable!("unsupported identity projection table"),
    }
}

async fn execute(transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>, sql: &str) -> Result<()> {
    sqlx::query(sql)
        .execute(&mut **transaction)
        .await
        .with_context(|| format!("failed to execute Base normalized-event rederive SQL: {sql}"))?;
    Ok(())
}

async fn delete_count(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sql: &str,
) -> Result<i64> {
    let result = sqlx::query(sql)
        .execute(&mut **transaction)
        .await
        .with_context(|| {
            format!("failed to execute Base normalized-event rederive delete: {sql}")
        })?;
    i64::try_from(result.rows_affected()).context("delete count overflowed i64")
}

async fn delete_count_bound(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sql: &str,
    deployment_profile: &str,
) -> Result<i64> {
    let result = sqlx::query(sql)
        .bind(deployment_profile)
        .bind(BASE_NORMALIZED_REDERIVE_CHAIN_ID)
        .bind(BASE_NORMALIZED_REDERIVE_CURSOR_KIND)
        .bind(checkpoint_adapters())
        .execute(&mut **transaction)
        .await
        .with_context(|| {
            format!("failed to execute Base normalized-event rederive delete: {sql}")
        })?;
    i64::try_from(result.rows_affected()).context("delete count overflowed i64")
}
