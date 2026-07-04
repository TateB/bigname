use anyhow::{Context, Result, bail};
use sqlx::Row;

use super::super::{
    reverse_claim_derivation_kind, reverse_claim_source_families, subregistry_derivation_kinds,
    subregistry_source_families, unwrapped_authority_derivation_kind,
    unwrapped_authority_source_families,
};
use super::state::Step;

pub(super) struct DeletedBatch {
    pub(super) row_count: i64,
    pub(super) range_start: Option<String>,
    pub(super) range_end: Option<String>,
}

pub(super) async fn delete_step_batch(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    step: Step,
    batch_size: i64,
    replay_target_block: i64,
) -> Result<DeletedBatch> {
    match step {
        Step::AddressNamesCurrent => {
            query_batch(transaction, address_names_sql(), batch_size).await
        }
        Step::NameCurrent => query_batch(transaction, name_current_sql(), batch_size).await,
        Step::ChildrenCurrent => query_batch(transaction, children_current_sql(), batch_size).await,
        Step::PermissionsCurrent => {
            query_batch(transaction, permissions_current_sql(), batch_size).await
        }
        Step::RecordInventoryCurrent => {
            query_batch(transaction, record_inventory_current_sql(), batch_size).await
        }
        Step::ProjectionNormalizedEventChanges => {
            query_scoped_event_batch(
                transaction,
                projection_changes_sql(),
                batch_size,
                replay_target_block,
            )
            .await
        }
        Step::NormalizedEvents => {
            query_scoped_event_batch(
                transaction,
                normalized_events_sql(),
                batch_size,
                replay_target_block,
            )
            .await
        }
        Step::SurfaceBindings => query_batch(transaction, surface_bindings_sql(), batch_size).await,
        Step::Resources => query_batch(transaction, resources_sql(), batch_size).await,
        Step::NameSurfaces => query_batch(transaction, name_surfaces_sql(), batch_size).await,
        Step::TokenLineages => query_batch(transaction, token_lineages_sql(), batch_size).await,
        Step::FinalReplayReset | Step::Completed => {
            bail!("unsupported delete batch step {}", step.as_str())
        }
    }
}

async fn query_batch(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sql: impl AsRef<str>,
    batch_size: i64,
) -> Result<DeletedBatch> {
    let sql = sql.as_ref();
    let row = sqlx::query(sql)
        .bind(batch_size)
        .fetch_one(&mut **transaction)
        .await
        .with_context(|| format!("failed to delete Base normalized-event rederive batch: {sql}"))?;
    deleted_batch_from_row(row)
}

async fn query_scoped_event_batch(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sql: impl AsRef<str>,
    batch_size: i64,
    replay_target_block: i64,
) -> Result<DeletedBatch> {
    let sql = sql.as_ref();
    let row = sqlx::query(sql)
        .bind(batch_size)
        .bind(replay_target_block)
        .bind(reverse_claim_derivation_kind())
        .bind(reverse_claim_source_families())
        .bind(subregistry_derivation_kinds())
        .bind(subregistry_source_families())
        .bind(unwrapped_authority_derivation_kind())
        .bind(unwrapped_authority_source_families())
        .fetch_one(&mut **transaction)
        .await
        .with_context(|| {
            format!("failed to delete Base normalized-event rederive event batch: {sql}")
        })?;
    deleted_batch_from_row(row)
}

fn deleted_batch_from_row(row: sqlx::postgres::PgRow) -> Result<DeletedBatch> {
    Ok(DeletedBatch {
        row_count: row.try_get("row_count")?,
        range_start: row.try_get("range_start")?,
        range_end: row.try_get("range_end")?,
    })
}

fn scoped_identity_projection_exists() -> &'static str {
    r#"
    EXISTS (
        SELECT 1 FROM resources s
        WHERE s.chain_id = 'base-mainnet'
          AND s.provenance->>'adapter' = 'ens_v1_unwrapped_authority'
          AND s.resource_id = p.resource_id
    )
    OR EXISTS (
        SELECT 1 FROM token_lineages s
        WHERE s.chain_id = 'base-mainnet'
          AND s.provenance->>'adapter' = 'ens_v1_unwrapped_authority'
          AND s.token_lineage_id = p.token_lineage_id
    )
    OR EXISTS (
        SELECT 1 FROM name_surfaces s
        WHERE s.chain_id = 'base-mainnet'
          AND s.provenance->>'adapter' = 'ens_v1_unwrapped_authority'
          AND s.logical_name_id = p.logical_name_id
    )
    OR EXISTS (
        SELECT 1 FROM surface_bindings s
        WHERE s.chain_id = 'base-mainnet'
          AND s.provenance->>'adapter' = 'ens_v1_unwrapped_authority'
          AND s.surface_binding_id = p.surface_binding_id
    )
    "#
}

fn address_names_sql() -> String {
    format!(
        r#"
        WITH candidates AS (
            SELECT p.address, p.logical_name_id, p.relation
            FROM address_names_current p
            WHERE {}
            ORDER BY p.address, p.logical_name_id, p.relation
            LIMIT $1
        ),
        deleted AS (
            DELETE FROM address_names_current p
            USING candidates c
            WHERE p.address = c.address
              AND p.logical_name_id = c.logical_name_id
              AND p.relation = c.relation
            RETURNING p.address || '|' || p.logical_name_id || '|' || p.relation AS key_text
        )
        SELECT COUNT(*)::BIGINT AS row_count,
               MIN(key_text)::TEXT AS range_start,
               MAX(key_text)::TEXT AS range_end
        FROM deleted
        "#,
        scoped_identity_projection_exists()
    )
}

fn name_current_sql() -> String {
    format!(
        r#"
        WITH candidates AS (
            SELECT p.logical_name_id
            FROM name_current p
            WHERE {}
            ORDER BY p.logical_name_id
            LIMIT $1
        ),
        deleted AS (
            DELETE FROM name_current p
            USING candidates c
            WHERE p.logical_name_id = c.logical_name_id
            RETURNING p.logical_name_id AS key_text
        )
        SELECT COUNT(*)::BIGINT AS row_count,
               MIN(key_text)::TEXT AS range_start,
               MAX(key_text)::TEXT AS range_end
        FROM deleted
        "#,
        scoped_identity_projection_exists()
    )
}

fn children_current_sql() -> &'static str {
    r#"
    WITH candidates AS (
        SELECT p.parent_logical_name_id, p.child_logical_name_id, p.surface_class
        FROM children_current p
        WHERE EXISTS (
            SELECT 1 FROM name_surfaces s
            WHERE s.chain_id = 'base-mainnet'
              AND s.provenance->>'adapter' = 'ens_v1_unwrapped_authority'
              AND s.logical_name_id IN (p.parent_logical_name_id, p.child_logical_name_id)
        )
        ORDER BY p.parent_logical_name_id, p.child_logical_name_id, p.surface_class
        LIMIT $1
    ),
    deleted AS (
        DELETE FROM children_current p
        USING candidates c
        WHERE p.parent_logical_name_id = c.parent_logical_name_id
          AND p.child_logical_name_id = c.child_logical_name_id
          AND p.surface_class = c.surface_class
        RETURNING p.parent_logical_name_id || '|' || p.child_logical_name_id || '|' || p.surface_class AS key_text
    )
    SELECT COUNT(*)::BIGINT AS row_count,
           MIN(key_text)::TEXT AS range_start,
           MAX(key_text)::TEXT AS range_end
    FROM deleted
    "#
}

fn permissions_current_sql() -> &'static str {
    r#"
    WITH candidates AS (
        SELECT p.resource_id, p.subject, p.scope
        FROM permissions_current p
        WHERE EXISTS (
            SELECT 1 FROM resources s
            WHERE s.chain_id = 'base-mainnet'
              AND s.provenance->>'adapter' = 'ens_v1_unwrapped_authority'
              AND s.resource_id = p.resource_id
        )
        ORDER BY p.resource_id, p.subject, p.scope
        LIMIT $1
    ),
    deleted AS (
        DELETE FROM permissions_current p
        USING candidates c
        WHERE p.resource_id = c.resource_id
          AND p.subject = c.subject
          AND p.scope = c.scope
        RETURNING p.resource_id::TEXT || '|' || p.subject || '|' || p.scope AS key_text
    )
    SELECT COUNT(*)::BIGINT AS row_count,
           MIN(key_text)::TEXT AS range_start,
           MAX(key_text)::TEXT AS range_end
    FROM deleted
    "#
}

fn record_inventory_current_sql() -> &'static str {
    r#"
    WITH candidates AS (
        SELECT p.resource_id, p.record_version_boundary_key
        FROM record_inventory_current p
        WHERE EXISTS (
            SELECT 1 FROM resources s
            WHERE s.chain_id = 'base-mainnet'
              AND s.provenance->>'adapter' = 'ens_v1_unwrapped_authority'
              AND s.resource_id = p.resource_id
        )
        ORDER BY p.resource_id, p.record_version_boundary_key
        LIMIT $1
    ),
    deleted AS (
        DELETE FROM record_inventory_current p
        USING candidates c
        WHERE p.resource_id = c.resource_id
          AND p.record_version_boundary_key = c.record_version_boundary_key
        RETURNING p.resource_id::TEXT || '|' || p.record_version_boundary_key AS key_text
    )
    SELECT COUNT(*)::BIGINT AS row_count,
           MIN(key_text)::TEXT AS range_start,
           MAX(key_text)::TEXT AS range_end
    FROM deleted
    "#
}

fn projection_changes_sql() -> &'static str {
    r#"
    WITH candidates AS (
        SELECT p.change_id
        FROM projection_normalized_event_changes p
        WHERE EXISTS (
            SELECT 1
            FROM normalized_events event
            WHERE event.normalized_event_id = p.normalized_event_id
              AND event.chain_id = 'base-mainnet'
              AND event.block_number BETWEEN 17571485 AND $2
              AND event.block_hash IS NOT NULL
              AND (
                  (event.derivation_kind = $3 AND event.source_family = ANY($4::TEXT[]))
                  OR (event.derivation_kind = ANY($5::TEXT[]) AND event.source_family = ANY($6::TEXT[]))
                  OR (event.derivation_kind = $7 AND event.source_family = ANY($8::TEXT[]))
              )
        )
        ORDER BY p.change_id
        LIMIT $1
    ),
    deleted AS (
        DELETE FROM projection_normalized_event_changes p
        USING candidates c
        WHERE p.change_id = c.change_id
        RETURNING p.change_id::TEXT AS key_text
    )
    SELECT COUNT(*)::BIGINT AS row_count,
           MIN(key_text)::TEXT AS range_start,
           MAX(key_text)::TEXT AS range_end
    FROM deleted
    "#
}

fn normalized_events_sql() -> &'static str {
    r#"
    WITH candidates AS (
        SELECT normalized_event_id
        FROM normalized_events
        WHERE chain_id = 'base-mainnet'
          AND block_number BETWEEN 17571485 AND $2
          AND block_hash IS NOT NULL
          AND (
              (derivation_kind = $3 AND source_family = ANY($4::TEXT[]))
              OR (derivation_kind = ANY($5::TEXT[]) AND source_family = ANY($6::TEXT[]))
              OR (derivation_kind = $7 AND source_family = ANY($8::TEXT[]))
          )
        ORDER BY block_number, normalized_event_id
        LIMIT $1
    ),
    deleted AS (
        DELETE FROM normalized_events p
        USING candidates c
        WHERE p.normalized_event_id = c.normalized_event_id
        RETURNING p.block_number::TEXT || ':' || p.normalized_event_id::TEXT AS key_text
    )
    SELECT COUNT(*)::BIGINT AS row_count,
           MIN(key_text)::TEXT AS range_start,
           MAX(key_text)::TEXT AS range_end
    FROM deleted
    "#
}

fn surface_bindings_sql() -> &'static str {
    r#"
    WITH candidates AS (
        SELECT surface_binding_id
        FROM surface_bindings
        WHERE chain_id = 'base-mainnet'
          AND provenance->>'adapter' = 'ens_v1_unwrapped_authority'
        ORDER BY surface_binding_id
        LIMIT $1
    ),
    deleted AS (
        DELETE FROM surface_bindings p
        USING candidates c
        WHERE p.surface_binding_id = c.surface_binding_id
        RETURNING p.surface_binding_id::TEXT AS key_text
    )
    SELECT COUNT(*)::BIGINT AS row_count,
           MIN(key_text)::TEXT AS range_start,
           MAX(key_text)::TEXT AS range_end
    FROM deleted
    "#
}

fn resources_sql() -> &'static str {
    r#"
    WITH candidates AS (
        SELECT resource_id
        FROM resources
        WHERE chain_id = 'base-mainnet'
          AND provenance->>'adapter' = 'ens_v1_unwrapped_authority'
        ORDER BY resource_id
        LIMIT $1
    ),
    deleted AS (
        DELETE FROM resources p
        USING candidates c
        WHERE p.resource_id = c.resource_id
        RETURNING p.resource_id::TEXT AS key_text
    )
    SELECT COUNT(*)::BIGINT AS row_count,
           MIN(key_text)::TEXT AS range_start,
           MAX(key_text)::TEXT AS range_end
    FROM deleted
    "#
}

fn name_surfaces_sql() -> &'static str {
    r#"
    WITH candidates AS (
        SELECT logical_name_id
        FROM name_surfaces
        WHERE chain_id = 'base-mainnet'
          AND provenance->>'adapter' = 'ens_v1_unwrapped_authority'
        ORDER BY logical_name_id
        LIMIT $1
    ),
    deleted AS (
        DELETE FROM name_surfaces p
        USING candidates c
        WHERE p.logical_name_id = c.logical_name_id
        RETURNING p.logical_name_id AS key_text
    )
    SELECT COUNT(*)::BIGINT AS row_count,
           MIN(key_text)::TEXT AS range_start,
           MAX(key_text)::TEXT AS range_end
    FROM deleted
    "#
}

fn token_lineages_sql() -> &'static str {
    r#"
    WITH candidates AS (
        SELECT token_lineage_id
        FROM token_lineages
        WHERE chain_id = 'base-mainnet'
          AND provenance->>'adapter' = 'ens_v1_unwrapped_authority'
        ORDER BY token_lineage_id
        LIMIT $1
    ),
    deleted AS (
        DELETE FROM token_lineages p
        USING candidates c
        WHERE p.token_lineage_id = c.token_lineage_id
        RETURNING p.token_lineage_id::TEXT AS key_text
    )
    SELECT COUNT(*)::BIGINT AS row_count,
           MIN(key_text)::TEXT AS range_start,
           MAX(key_text)::TEXT AS range_end
    FROM deleted
    "#
}
