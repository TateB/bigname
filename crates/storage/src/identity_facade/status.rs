use anyhow::{Context, Result};
use sqlx::PgPool;

use super::{IndexingStatusChainRow, IndexingStatusRead};

pub async fn load_indexing_status(pool: &PgPool) -> Result<IndexingStatusRead> {
    let rows = sqlx::query(
        r#"
        WITH apply_cursor AS (
            SELECT COALESCE((
                SELECT last_change_id
                FROM projection_apply_cursors
                WHERE cursor_name = 'normalized_events_to_projection_invalidations'
            ), 0) AS last_change_id
        ),
        change_progress AS (
            SELECT MAX(change_id) AS max_change_id
            FROM projection_normalized_event_changes
        ),
        pending_projection AS (
            SELECT
                event.chain_id,
                MIN(event.block_number) AS first_pending_block,
                COUNT(*) AS pending_count
            FROM projection_invalidations invalidation
            JOIN normalized_events event
              ON event.normalized_event_id = COALESCE(
                  invalidation.first_normalized_event_id,
                  invalidation.last_normalized_event_id
              )
            WHERE event.chain_id IS NOT NULL
              AND event.block_number IS NOT NULL
            GROUP BY event.chain_id
        ),
        projected AS (
            SELECT
                cc.chain_id,
                CASE
                    WHEN cc.canonical_block_number IS NOT NULL
                      AND pending_projection.pending_count IS NULL
                      AND (
                          change_progress.max_change_id IS NULL
                          OR apply_cursor.last_change_id >= change_progress.max_change_id
                      )
                    THEN cc.canonical_block_number
                    WHEN pending_projection.first_pending_block IS NOT NULL
                    THEN GREATEST(pending_projection.first_pending_block - 1, 0)
                    ELSE latest_applied_event.block_number
                END AS latest_projected_block
            FROM chain_checkpoints cc
            CROSS JOIN apply_cursor
            CROSS JOIN change_progress
            LEFT JOIN pending_projection
              ON pending_projection.chain_id = cc.chain_id
            LEFT JOIN LATERAL (
                SELECT event.block_number
                FROM normalized_events event
                JOIN projection_normalized_event_changes change
                  ON change.normalized_event_id = event.normalized_event_id
                WHERE pending_projection.first_pending_block IS NULL
                  AND apply_cursor.last_change_id < COALESCE(
                      change_progress.max_change_id,
                      apply_cursor.last_change_id
                  )
                  AND event.chain_id = cc.chain_id
                  AND event.block_number IS NOT NULL
                  AND change.change_id <= apply_cursor.last_change_id
                ORDER BY event.block_number DESC, event.normalized_event_id DESC
                LIMIT 1
            ) latest_applied_event ON TRUE
        )
        SELECT
            cc.chain_id,
            cc.canonical_block_number,
            cc.safe_block_number,
            cc.finalized_block_number,
            canonical_lineage.block_timestamp AS canonical_timestamp,
            projected.latest_projected_block,
            projected_lineage.block_timestamp AS latest_projected_timestamp
        FROM chain_checkpoints cc
        LEFT JOIN projected
          ON projected.chain_id = cc.chain_id
        LEFT JOIN chain_lineage canonical_lineage
          ON canonical_lineage.chain_id = cc.chain_id
         AND canonical_lineage.block_number = cc.canonical_block_number
         AND canonical_lineage.block_hash = cc.canonical_block_hash
        LEFT JOIN LATERAL (
            SELECT chain_lineage.block_timestamp
            FROM chain_lineage
            WHERE chain_lineage.chain_id = cc.chain_id
              AND projected.latest_projected_block IS NOT NULL
              AND chain_lineage.block_number <= projected.latest_projected_block
              AND chain_lineage.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            ORDER BY chain_lineage.block_number DESC
            LIMIT 1
        ) projected_lineage ON TRUE
        ORDER BY cc.chain_id
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load indexing status checkpoints")?;

    let chains = rows
        .into_iter()
        .map(|row| {
            Ok(IndexingStatusChainRow {
                chain_id: crate::sql_row::get(&row, "chain_id")?,
                canonical_block: crate::sql_row::get(&row, "canonical_block_number")?,
                safe_block: crate::sql_row::get(&row, "safe_block_number")?,
                finalized_block: crate::sql_row::get(&row, "finalized_block_number")?,
                canonical_timestamp: crate::sql_row::get(&row, "canonical_timestamp")?,
                latest_projected_block: crate::sql_row::get(&row, "latest_projected_block")?,
                latest_projected_timestamp: crate::sql_row::get(
                    &row,
                    "latest_projected_timestamp",
                )?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(IndexingStatusRead { chains })
}
