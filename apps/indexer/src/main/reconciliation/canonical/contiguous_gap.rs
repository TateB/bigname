use anyhow::Result;
use bigname_storage::{
    CanonicalityState,
    upsert_chain_lineage_blocks_without_snapshots_recanonicalizing_orphaned as upsert_recanonicalized_lineage_blocks_without_snapshots,
};

use crate::provider::{ChainProviderOps, ProviderBlock};

use super::super::{
    lineage::{provider_block_to_checkpoint_ref, provider_block_to_lineage_with_header_audit_mode},
    types::{CanonicalReconciliation, CanonicalReconciliationStatus, HeaderAuditMode},
};
use super::{MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS, MAX_PARENT_FETCH_DEPTH};

pub(super) async fn reconcile_contiguous_checkpoint_gap(
    pool: &sqlx::PgPool,
    provider: &(impl ChainProviderOps + ?Sized),
    chain: &str,
    current_canonical_hash: &str,
    current_canonical_number: i64,
    latest_head: &ProviderBlock,
    header_audit_mode: HeaderAuditMode,
) -> Result<Option<CanonicalReconciliation>> {
    if latest_head.block_number <= current_canonical_number {
        return Ok(None);
    }
    let gap_blocks = latest_head.block_number - current_canonical_number;
    if gap_blocks > MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS {
        return Ok(None);
    }
    if gap_blocks as usize > MAX_PARENT_FETCH_DEPTH {
        return Ok(None);
    }

    let block_numbers =
        ((current_canonical_number + 1)..=latest_head.block_number).collect::<Vec<_>>();
    let resolved_blocks = provider
        .fetch_block_hashes_by_numbers(&block_numbers)
        .await?;
    if resolved_blocks
        .last()
        .map(|block| block.block_hash.as_str())
        != Some(latest_head.block_hash.as_str())
    {
        return Ok(None);
    }

    let mut path = provider
        .fetch_block_headers_by_hashes(&resolved_blocks)
        .await?;
    let first_parent_hash = path.first().and_then(|block| block.parent_hash.as_deref());
    if first_parent_hash != Some(current_canonical_hash) {
        return Ok(None);
    }
    if !path
        .windows(2)
        .all(|window| window[1].parent_hash.as_deref() == Some(window[0].block_hash.as_str()))
    {
        return Ok(None);
    }

    let lineage_blocks = path
        .iter()
        .map(|block| {
            provider_block_to_lineage_with_header_audit_mode(
                chain,
                block,
                CanonicalityState::Canonical,
                header_audit_mode,
            )
        })
        .collect::<Vec<_>>();
    upsert_recanonicalized_lineage_blocks_without_snapshots(pool, &lineage_blocks).await?;

    let status = if path.len() == 1 {
        CanonicalReconciliationStatus::Appended
    } else {
        CanonicalReconciliationStatus::GapBackfilled
    };
    let fetched_parent_count = path.len().saturating_sub(1);
    path.reverse();

    Ok(Some(CanonicalReconciliation {
        status,
        canonical: Some(provider_block_to_checkpoint_ref(latest_head)),
        fetched_parent_count,
        orphaned_block_count: 0,
        reconciled_blocks: path,
        raw_orphan_stop_before_hash: None,
    }))
}
