use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::snapshot_selection::{
    ChainPosition, ChainPositions, SnapshotProjectionRead, SnapshotSelectionError,
    ensure_projection_chain_positions_match,
};

use super::{
    boundary_key::record_version_boundary_storage_key,
    row_decode::{RecordInventoryCurrentRow, decode_record_inventory_current_row},
};

const DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER: &str = r#"
  AND resource.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
"#;

/// Load one record-inventory projection row by resource and exact version boundary.
pub async fn load_record_inventory_current(
    pool: &PgPool,
    resource_id: Uuid,
    record_version_boundary: &Value,
) -> Result<Option<RecordInventoryCurrentRow>> {
    let record_version_boundary_key = record_version_boundary_storage_key(
        record_version_boundary,
        resource_id,
    )
    .with_context(|| {
        format!(
            "failed to derive record_inventory_current boundary key for resource_id {resource_id}"
        )
    })?;

    let row = sqlx::query(&format!(
        r#"
        SELECT
            ric.resource_id,
            ric.record_version_boundary,
            ric.enumeration_basis,
            ric.selectors,
            ric.explicit_gaps,
            ric.unsupported_families,
            ric.last_change,
            ric.entries,
            ric.provenance,
            ric.coverage,
            ric.chain_positions,
            ric.canonicality_summary,
            ric.manifest_version,
            ric.last_recomputed_at
        FROM record_inventory_current ric
        JOIN resources resource
          ON resource.resource_id = ric.resource_id
        WHERE ric.resource_id = $1
          AND ric.record_version_boundary_key = $2
        {DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER}
        "#,
    ))
    .bind(resource_id)
    .bind(&record_version_boundary_key)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!("failed to load record_inventory_current row for resource_id {resource_id}")
    })?;

    row.map(decode_record_inventory_current_row).transpose()
}

/// Load one record-inventory projection row only if it is eligible for the selected snapshot.
///
/// A present row with different chain-position context is reported as `stale`
/// instead of being joined into an exact-name response for another snapshot.
pub async fn load_record_inventory_current_for_snapshot(
    pool: &PgPool,
    resource_id: Uuid,
    record_version_boundary: &Value,
    selected_chain_positions: &ChainPositions,
) -> std::result::Result<SnapshotProjectionRead<RecordInventoryCurrentRow>, SnapshotSelectionError>
{
    let row = load_record_inventory_current(pool, resource_id, record_version_boundary)
        .await
        .map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to load record_inventory_current row for resource_id {resource_id}: {error}"
            ))
        })?;

    let Some(row) = row else {
        return Ok(SnapshotProjectionRead::NotFound);
    };

    match ensure_projection_chain_positions_match(
        "record_inventory_current",
        &row.chain_positions,
        selected_chain_positions,
    ) {
        Ok(()) => {}
        Err(error) => {
            if !record_inventory_projection_covers_selected_snapshot(
                pool,
                &row,
                selected_chain_positions,
            )
            .await?
            {
                return Err(error);
            }
        }
    }
    Ok(SnapshotProjectionRead::Found(row))
}

async fn record_inventory_projection_covers_selected_snapshot(
    pool: &PgPool,
    row: &RecordInventoryCurrentRow,
    selected_chain_positions: &ChainPositions,
) -> std::result::Result<bool, SnapshotSelectionError> {
    let projected = ChainPositions::from_value(&row.chain_positions).map_err(|error| {
        SnapshotSelectionError::stale(format!(
            "record_inventory_current projection has unusable chain_positions: {}",
            error.message()
        ))
    })?;

    let projected_by_chain_id = positions_by_chain_id(&projected)?;
    let selected_by_chain_id = positions_by_chain_id(selected_chain_positions)?;

    for (chain_id, selected_position) in &selected_by_chain_id {
        let Some(projected_position) = projected_by_chain_id.get(chain_id) else {
            return Ok(false);
        };
        if selected_position.block_number < projected_position.block_number {
            return Ok(false);
        }
        if selected_position.block_number == projected_position.block_number {
            if selected_position.block_hash != projected_position.block_hash
                || selected_position.timestamp != projected_position.timestamp
            {
                return Ok(false);
            }
            continue;
        }
        if !position_is_canonical_lineage_member(pool, chain_id, projected_position).await? {
            return Ok(false);
        }
        if !position_is_canonical_lineage_member(pool, chain_id, selected_position).await? {
            return Ok(false);
        }
        if record_inventory_has_newer_projection_inputs(
            pool,
            row,
            chain_id,
            projected_position.block_number,
            selected_position.block_number,
        )
        .await?
        {
            return Ok(false);
        }
    }

    Ok(true)
}

fn positions_by_chain_id(
    positions: &ChainPositions,
) -> std::result::Result<BTreeMap<String, &ChainPosition>, SnapshotSelectionError> {
    let mut by_chain_id = BTreeMap::new();
    for position in positions.as_map().values() {
        if by_chain_id
            .insert(position.chain_id.clone(), position)
            .is_some()
        {
            return Err(SnapshotSelectionError::stale(format!(
                "record_inventory_current projection repeats chain_id {} in chain_positions",
                position.chain_id
            )));
        }
    }
    Ok(by_chain_id)
}

async fn position_is_canonical_lineage_member(
    pool: &PgPool,
    chain_id: &str,
    position: &ChainPosition,
) -> std::result::Result<bool, SnapshotSelectionError> {
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM chain_lineage
            WHERE chain_id = $1
              AND block_hash = $2
              AND block_number = $3
              AND canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        )
        "#,
    )
    .bind(chain_id)
    .bind(&position.block_hash)
    .bind(position.block_number)
    .fetch_one(pool)
    .await
    .map_err(|error| {
        SnapshotSelectionError::internal(format!(
            "failed to check record_inventory_current chain position block {} on chain {chain_id}: {error}",
            position.block_hash
        ))
    })
}

async fn record_inventory_has_newer_projection_inputs(
    pool: &PgPool,
    row: &RecordInventoryCurrentRow,
    chain_id: &str,
    projected_block_number: i64,
    selected_block_number: i64,
) -> std::result::Result<bool, SnapshotSelectionError> {
    let logical_name_id = row
        .record_version_boundary
        .get("logical_name_id")
        .and_then(Value::as_str);

    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM normalized_events ne
            WHERE ne.chain_id = $1
              AND ne.block_number > $2
              AND ne.block_number <= $3
              AND ne.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND ne.event_kind IN (
                  'RecordChanged',
                  'RecordVersionChanged',
                  'ResolverChanged'
              )
              AND (
                  ne.resource_id = $4
                  OR ($5::TEXT IS NOT NULL AND ne.logical_name_id = $5)
              )
            LIMIT 1
        )
        "#,
    )
    .bind(chain_id)
    .bind(projected_block_number)
    .bind(selected_block_number)
    .bind(row.resource_id)
    .bind(logical_name_id)
    .fetch_one(pool)
    .await
    .map_err(|error| {
        SnapshotSelectionError::internal(format!(
            "failed to check record_inventory_current normalized-event invalidation for resource_id {}: {error}",
            row.resource_id
        ))
    })
}

/// Delete one record-inventory projection row so a worker can rebuild that exact key.
pub async fn delete_record_inventory_current(
    pool: &PgPool,
    resource_id: Uuid,
    record_version_boundary: &Value,
) -> Result<u64> {
    let record_version_boundary_key = record_version_boundary_storage_key(
        record_version_boundary,
        resource_id,
    )
    .with_context(|| {
        format!(
            "failed to derive record_inventory_current delete key for resource_id {resource_id}"
        )
    })?;

    sqlx::query(
        r#"
        DELETE FROM record_inventory_current
        WHERE resource_id = $1
          AND record_version_boundary_key = $2
        "#,
    )
    .bind(resource_id)
    .bind(&record_version_boundary_key)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to delete record_inventory_current row for resource_id {resource_id}")
    })
    .map(|result| result.rows_affected())
}

/// Clear the record-inventory projection so a worker can perform a one-shot rebuild.
pub async fn clear_record_inventory_current(pool: &PgPool) -> Result<u64> {
    sqlx::query("DELETE FROM record_inventory_current")
        .execute(pool)
        .await
        .context("failed to clear record_inventory_current rows")
        .map(|result| result.rows_affected())
}
