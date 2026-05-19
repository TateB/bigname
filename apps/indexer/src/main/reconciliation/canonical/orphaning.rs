use anyhow::Result;
use bigname_storage::{
    invalidate_execution_outcomes_for_orphaned_blocks,
    mark_block_derived_normalized_events_range_orphaned, mark_identity_rows_range_orphaned,
    mark_raw_block_facts_range_orphaned,
};
use tracing::info;

use super::super::persistence::ensure_losing_branch_raw_blocks_exist;

pub(super) async fn orphan_reorg_losing_branch_payloads(
    pool: &sqlx::PgPool,
    chain: &str,
    current_canonical_hash: Option<&str>,
    stop_before_hash: Option<&str>,
) -> Result<()> {
    if let Some(current_canonical_hash) = current_canonical_hash {
        ensure_losing_branch_raw_blocks_exist(
            pool,
            chain,
            current_canonical_hash,
            stop_before_hash,
        )
        .await?;
        mark_raw_block_facts_range_orphaned(pool, chain, current_canonical_hash, stop_before_hash)
            .await?;
        let orphaned_normalized_event_count = mark_block_derived_normalized_events_range_orphaned(
            pool,
            chain,
            current_canonical_hash,
            stop_before_hash,
        )
        .await?;
        if orphaned_normalized_event_count > 0 {
            info!(
                service = "indexer",
                chain = %chain,
                orphaned_normalized_event_count,
                "block-derived normalized events orphaned for the losing branch"
            );
        }
        let orphaned_identity_counts = mark_identity_rows_range_orphaned(
            pool,
            chain,
            current_canonical_hash,
            stop_before_hash,
        )
        .await?;
        if orphaned_identity_counts.token_lineage_count > 0
            || orphaned_identity_counts.resource_count > 0
            || orphaned_identity_counts.name_surface_count > 0
            || orphaned_identity_counts.surface_binding_count > 0
        {
            info!(
                service = "indexer",
                chain = %chain,
                orphaned_token_lineage_count = orphaned_identity_counts.token_lineage_count,
                orphaned_resource_count = orphaned_identity_counts.resource_count,
                orphaned_name_surface_count = orphaned_identity_counts.name_surface_count,
                orphaned_surface_binding_count = orphaned_identity_counts.surface_binding_count,
                "identity rows orphaned for the losing branch"
            );
        }
    }

    let execution_invalidation_summary =
        invalidate_execution_outcomes_for_orphaned_blocks(pool).await?;
    if execution_invalidation_summary.deleted_outcome_count > 0 {
        info!(
            service = "indexer",
            chain = %chain,
            invalidated_execution_outcome_count =
                execution_invalidation_summary.deleted_outcome_count,
            "execution cache outcomes invalidated for orphaned block dependencies"
        );
    }

    Ok(())
}
