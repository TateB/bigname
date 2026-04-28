use std::collections::BTreeMap;

use sqlx::PgPool;

use crate::snapshot_selection::{
    ChainPosition, ChainPositions, SnapshotProjectionRead, SnapshotSelectionError,
    ensure_projection_chain_positions_match,
};

use super::{NameCurrentRow, load_name_current};

/// Load one exact-name projection row only if it is eligible for the selected snapshot.
///
/// Missing rows stay distinguishable from stale rows so API callers can preserve
/// the route-specific `not_found` behavior without filling stale snapshots from
/// raw facts.
pub async fn load_name_current_for_snapshot(
    pool: &PgPool,
    logical_name_id: &str,
    selected_chain_positions: &ChainPositions,
) -> std::result::Result<SnapshotProjectionRead<NameCurrentRow>, SnapshotSelectionError> {
    let row = load_name_current(pool, logical_name_id)
        .await
        .map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to load name_current row for logical_name_id {logical_name_id}: {error}"
            ))
        })?;

    let Some(row) = row else {
        return Ok(SnapshotProjectionRead::NotFound);
    };

    match ensure_projection_chain_positions_match(
        "name_current",
        &row.chain_positions,
        selected_chain_positions,
    ) {
        Ok(()) => {}
        Err(error) => {
            if !name_current_projection_covers_selected_snapshot(
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

async fn name_current_projection_covers_selected_snapshot(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_chain_positions: &ChainPositions,
) -> std::result::Result<bool, SnapshotSelectionError> {
    let projected = ChainPositions::from_value(&row.chain_positions).map_err(|error| {
        SnapshotSelectionError::stale(format!(
            "name_current projection has unusable chain_positions: {}",
            error.message()
        ))
    })?;

    let projected_by_chain_id = positions_by_chain_id(&projected)?;
    let selected_by_chain_id = positions_by_chain_id(selected_chain_positions)?;
    if projected_by_chain_id.keys().ne(selected_by_chain_id.keys()) {
        return Ok(false);
    }

    for (chain_id, projected_position) in &projected_by_chain_id {
        let selected_position = selected_by_chain_id
            .get(chain_id)
            .expect("selected map must have the same chain_id keys as projected map");
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
        if !selected_extends_projected_ancestry(
            pool,
            chain_id,
            projected_position,
            selected_position,
        )
        .await?
        {
            return Ok(false);
        }
        if name_current_has_newer_projection_inputs(
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
                "name_current projection repeats chain_id {} in chain_positions",
                position.chain_id
            )));
        }
    }
    Ok(by_chain_id)
}

async fn selected_extends_projected_ancestry(
    pool: &PgPool,
    chain_id: &str,
    projected_position: &ChainPosition,
    selected_position: &ChainPosition,
) -> std::result::Result<bool, SnapshotSelectionError> {
    sqlx::query_scalar::<_, bool>(
        r#"
        WITH RECURSIVE selected_ancestry AS (
            SELECT block_hash, parent_hash, block_number, canonicality_state
            FROM chain_lineage
            WHERE chain_id = $1
              AND block_hash = $2
              AND block_number = $3

            UNION ALL

            SELECT parent.block_hash, parent.parent_hash, parent.block_number, parent.canonicality_state
            FROM chain_lineage parent
            JOIN selected_ancestry child
              ON parent.chain_id = $1
             AND parent.block_hash = child.parent_hash
            WHERE child.block_number > $4
        )
        SELECT EXISTS (
            SELECT 1
            FROM selected_ancestry
            WHERE block_number = $4
              AND block_hash = $5
              AND canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        )
        "#,
    )
    .bind(chain_id)
    .bind(&selected_position.block_hash)
    .bind(selected_position.block_number)
    .bind(projected_position.block_number)
    .bind(&projected_position.block_hash)
    .fetch_one(pool)
    .await
    .map_err(|error| {
        SnapshotSelectionError::internal(format!(
            "failed to check name_current ancestry from selected block {} to projected block {} on chain {chain_id}: {error}",
            selected_position.block_hash, projected_position.block_hash
        ))
    })
}

async fn name_current_has_newer_projection_inputs(
    pool: &PgPool,
    row: &NameCurrentRow,
    chain_id: &str,
    projected_block_number: i64,
    selected_block_number: i64,
) -> std::result::Result<bool, SnapshotSelectionError> {
    let newer_event = sqlx::query_scalar::<_, bool>(
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
              AND (
                  ne.logical_name_id = $4
                  OR ($5::UUID IS NOT NULL AND ne.resource_id = $5)
              )
            LIMIT 1
        )
        "#,
    )
    .bind(chain_id)
    .bind(projected_block_number)
    .bind(selected_block_number)
    .bind(&row.logical_name_id)
    .bind(row.resource_id)
    .fetch_one(pool)
    .await
    .map_err(|error| {
        SnapshotSelectionError::internal(format!(
            "failed to check name_current normalized-event invalidation for {}: {error}",
            row.logical_name_id
        ))
    })?;
    if newer_event {
        return Ok(true);
    }

    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM surface_bindings sb
            WHERE sb.logical_name_id = $1
              AND sb.chain_id = $2
              AND sb.block_number > $3
              AND sb.block_number <= $4
              AND sb.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            LIMIT 1
        )
        "#,
    )
    .bind(&row.logical_name_id)
    .bind(chain_id)
    .bind(projected_block_number)
    .bind(selected_block_number)
    .fetch_one(pool)
    .await
    .map_err(|error| {
        SnapshotSelectionError::internal(format!(
            "failed to check name_current surface-binding invalidation for {}: {error}",
            row.logical_name_id
        ))
    })
}
