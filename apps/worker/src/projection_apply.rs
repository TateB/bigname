mod apply;
mod derive;
mod derive_queries;

#[cfg(test)]
mod tests;

use anyhow::{Context, Result};
use sqlx::PgPool;
use tracing::info;

use crate::record_inventory;

pub(crate) use derive::{normalized_event_cursor_exists, seed_normalized_event_cursor_if_absent};

const NORMALIZED_EVENT_CURSOR: &str = "normalized_events_to_projection_invalidations";
const NORMALIZED_EVENT_DERIVE_BATCH_LIMIT: i64 = 5_000;
const PROJECTION_APPLY_BATCH_LIMIT: i64 = 100;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedEventChangeCursor {
    pub(crate) change_id: i64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ProjectionApplyIterationSummary {
    pub(crate) scanned_event_count: i64,
    pub(crate) enqueued_invalidation_count: u64,
    pub(crate) claimed_invalidation_count: usize,
    pub(crate) applied_invalidation_count: usize,
    pub(crate) failed_invalidation_count: usize,
}

impl ProjectionApplyIterationSummary {
    pub(crate) fn made_progress(&self) -> bool {
        self.scanned_event_count > 0
            || self.claimed_invalidation_count > 0
            || self.applied_invalidation_count > 0
            || self.failed_invalidation_count > 0
    }
}

pub(crate) async fn run_once(
    pool: &PgPool,
    text_hydration_config: Option<&record_inventory::RecordInventoryTextHydrationConfig>,
) -> Result<ProjectionApplyIterationSummary> {
    let derived =
        derive::derive_normalized_event_invalidations(pool, NORMALIZED_EVENT_DERIVE_BATCH_LIMIT)
            .await?;
    let applied = apply::apply_pending_invalidations(
        pool,
        PROJECTION_APPLY_BATCH_LIMIT,
        text_hydration_config,
    )
    .await?;

    let summary = ProjectionApplyIterationSummary {
        scanned_event_count: derived.scanned_event_count,
        enqueued_invalidation_count: derived.enqueued_invalidation_count,
        claimed_invalidation_count: applied.claimed_invalidation_count,
        applied_invalidation_count: applied.applied_invalidation_count,
        failed_invalidation_count: applied.failed_invalidation_count,
    };

    if summary.made_progress() {
        info!(
            service = "worker",
            projection_apply = true,
            scanned_event_count = summary.scanned_event_count,
            enqueued_invalidation_count = summary.enqueued_invalidation_count,
            claimed_invalidation_count = summary.claimed_invalidation_count,
            applied_invalidation_count = summary.applied_invalidation_count,
            failed_invalidation_count = summary.failed_invalidation_count,
            "continuous projection apply iteration completed"
        );
    }

    Ok(summary)
}

pub(crate) async fn load_normalized_event_change_watermark(
    pool: &PgPool,
) -> Result<NormalizedEventChangeCursor> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COALESCE(MAX(change_id), 0)
        FROM projection_normalized_event_changes
        "#,
    )
    .fetch_one(pool)
    .await
    .context("failed to load normalized-event projection apply watermark")
    .map(|change_id| NormalizedEventChangeCursor { change_id })
}

pub(crate) async fn load_chain_checkpoint_max_block(pool: &PgPool) -> Result<Option<i64>> {
    sqlx::query_scalar::<_, Option<i64>>(
        r#"
        SELECT NULLIF(MAX(GREATEST(
            COALESCE(canonical_block_number, -1),
            COALESCE(safe_block_number, -1),
            COALESCE(finalized_block_number, -1)
        )), -1)
        FROM chain_checkpoints
        "#,
    )
    .fetch_one(pool)
    .await
    .context("failed to load chain-checkpoint projection replay target block")
}
