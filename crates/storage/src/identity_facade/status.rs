use anyhow::{Context, Result};
use sqlx::PgPool;

use super::{IndexingStatusChainRow, IndexingStatusRead};

pub async fn load_indexing_status(pool: &PgPool) -> Result<IndexingStatusRead> {
    let rows = sqlx::query(
        r#"
        WITH known_chains AS (
            SELECT chain_id
            FROM chain_checkpoints
            UNION
            SELECT chain AS chain_id
            FROM manifest_versions
            WHERE chain IS NOT NULL
              AND rollout_status IN (
                'active'::manifest_rollout_status,
                'shadow'::manifest_rollout_status
            )
        ),
        apply_cursor AS (
            SELECT COALESCE((
                SELECT last_change_id
                FROM projection_apply_cursors
                WHERE cursor_name = 'normalized_events_to_projection_invalidations'
            ), 0) AS last_change_id
        ),
        unscanned_changes AS MATERIALIZED (
            SELECT change.normalized_event_id
            FROM projection_normalized_event_changes change
            CROSS JOIN apply_cursor
            WHERE change.change_id > apply_cursor.last_change_id
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
        unscanned_projection AS (
            SELECT
                event.chain_id,
                MIN(event.block_number) AS first_unscanned_block,
                COUNT(*) AS unscanned_count
            FROM unscanned_changes change
            CROSS JOIN LATERAL (
                SELECT event.chain_id, event.block_number
                FROM normalized_events event
                WHERE event.normalized_event_id = change.normalized_event_id
                OFFSET 0
            ) event
            WHERE event.chain_id IS NOT NULL
              AND event.block_number IS NOT NULL
            GROUP BY event.chain_id
        ),
        projected AS (
            SELECT
                known_chains.chain_id,
                CASE
                    WHEN cc.canonical_block_number IS NOT NULL
                      AND pending_projection.pending_count IS NULL
                      AND unscanned_projection.unscanned_count IS NULL
                    THEN cc.canonical_block_number
                    WHEN pending_projection.first_pending_block IS NOT NULL
                    THEN GREATEST(pending_projection.first_pending_block - 1, 0)
                    WHEN unscanned_projection.first_unscanned_block IS NOT NULL
                    THEN GREATEST(unscanned_projection.first_unscanned_block - 1, 0)
                    ELSE latest_applied_event.block_number
                END AS latest_projected_block
            FROM known_chains
            CROSS JOIN apply_cursor
            LEFT JOIN chain_checkpoints cc
              ON cc.chain_id = known_chains.chain_id
            LEFT JOIN pending_projection
              ON pending_projection.chain_id = known_chains.chain_id
            LEFT JOIN unscanned_projection
              ON unscanned_projection.chain_id = known_chains.chain_id
            LEFT JOIN LATERAL (
                SELECT event.block_number
                FROM normalized_events event
                JOIN projection_normalized_event_changes change
                  ON change.normalized_event_id = event.normalized_event_id
                WHERE cc.canonical_block_number IS NULL
                  AND pending_projection.first_pending_block IS NULL
                  AND unscanned_projection.first_unscanned_block IS NULL
                  AND event.chain_id = known_chains.chain_id
                  AND event.block_number IS NOT NULL
                  AND change.change_id <= apply_cursor.last_change_id
                ORDER BY event.block_number DESC, event.normalized_event_id DESC
                LIMIT 1
            ) latest_applied_event ON TRUE
        )
        SELECT
            known_chains.chain_id,
            cc.canonical_block_number,
            cc.safe_block_number,
            cc.finalized_block_number,
            canonical_lineage.block_timestamp AS canonical_timestamp,
            projected.latest_projected_block,
            projected_lineage.block_timestamp AS latest_projected_timestamp
        FROM known_chains
        LEFT JOIN chain_checkpoints cc
          ON cc.chain_id = known_chains.chain_id
        LEFT JOIN projected
          ON projected.chain_id = known_chains.chain_id
        LEFT JOIN chain_lineage canonical_lineage
          ON canonical_lineage.chain_id = known_chains.chain_id
         AND canonical_lineage.block_number = cc.canonical_block_number
         AND canonical_lineage.block_hash = cc.canonical_block_hash
        LEFT JOIN LATERAL (
            SELECT chain_lineage.block_timestamp
            FROM chain_lineage
            WHERE chain_lineage.chain_id = known_chains.chain_id
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
        ORDER BY known_chains.chain_id
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

    let has_unscoped_pending_invalidations = sqlx::query_scalar::<_, bool>(
        r#"
        WITH apply_cursor AS (
            SELECT COALESCE((
                SELECT last_change_id
                FROM projection_apply_cursors
                WHERE cursor_name = 'normalized_events_to_projection_invalidations'
            ), 0) AS last_change_id
        ),
        unscanned_changes AS MATERIALIZED (
            SELECT change.normalized_event_id
            FROM projection_normalized_event_changes change
            CROSS JOIN apply_cursor
            WHERE change.change_id > apply_cursor.last_change_id
        )
        SELECT EXISTS (
            SELECT 1
            FROM projection_invalidations invalidation
            LEFT JOIN normalized_events event
              ON event.normalized_event_id = COALESCE(
                  invalidation.first_normalized_event_id,
                  invalidation.last_normalized_event_id
              )
            WHERE event.normalized_event_id IS NULL
               OR event.chain_id IS NULL
               OR event.block_number IS NULL
        )
        OR EXISTS (
            SELECT 1
            FROM unscanned_changes change
            CROSS JOIN LATERAL (
                SELECT event.chain_id, event.block_number
                FROM normalized_events event
                WHERE event.normalized_event_id = change.normalized_event_id
                OFFSET 0
            ) event
            WHERE event.chain_id IS NULL
               OR event.block_number IS NULL
        )
        "#,
    )
    .fetch_one(pool)
    .await
    .context("failed to load unscoped indexing invalidation status")?;

    Ok(IndexingStatusRead {
        chains,
        has_unscoped_pending_invalidations,
    })
}
