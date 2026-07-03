use anyhow::Result;
use bigname_storage::{
    CanonicalityState, ChainLineageBlock, CheckpointBlockRef,
    chain_lineage_contains_canonical_ancestor_position, load_chain_lineage_block,
    load_chain_lineage_canonical_child_path,
};

use crate::provider::{ChainProviderOps, ProviderBlock, ProviderHeadSnapshot};

use super::super::{
    lineage::lineage_block_to_provider,
    types::{CanonicalReconciliation, CanonicalReconciliationStatus},
};
use super::MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS;

#[path = "stored_lineage/coverage.rs"]
mod coverage;

use coverage::stored_path_has_required_raw_fact_coverage;

const MAX_STORED_ANCHOR_PARENT_FETCH_DEPTH: usize =
    (MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS as usize) * 4;

pub(super) enum StoredLineagePromotion {
    Promoted(CanonicalReconciliation),
    NotApplicable,
    Refused(String),
}

pub(super) fn stored_lineage_promotion_anchors(heads: &ProviderHeadSnapshot) -> Vec<ProviderBlock> {
    let mut anchors = heads.safe.iter().cloned().collect::<Vec<_>>();
    if let Some(finalized) = &heads.finalized
        && !anchors
            .iter()
            .any(|anchor| anchor.block_hash == finalized.block_hash)
    {
        anchors.push(finalized.clone());
    }
    anchors
}

pub(super) async fn reconcile_large_checkpoint_gap_from_stored_lineage(
    pool: &sqlx::PgPool,
    provider: &(impl ChainProviderOps + ?Sized),
    chain: &str,
    current_canonical_hash: &str,
    current_canonical_number: i64,
    latest_head: &ProviderBlock,
    stored_anchor_candidates: &[ProviderBlock],
    selected_raw_payload_addresses: &[String],
    requires_event_silent_payloads: bool,
) -> Result<StoredLineagePromotion> {
    if latest_head.block_number <= current_canonical_number {
        return Ok(StoredLineagePromotion::NotApplicable);
    }
    let live_gap_blocks = latest_head.block_number - current_canonical_number;
    if live_gap_blocks <= MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS {
        return Ok(StoredLineagePromotion::NotApplicable);
    }

    let Some((stored_anchor, provider_anchor)) = select_stored_promotion_anchor(
        pool,
        provider,
        chain,
        current_canonical_number,
        stored_anchor_candidates,
    )
    .await?
    else {
        return Ok(StoredLineagePromotion::Refused(format!(
            "canonical gap of {live_gap_blocks} blocks for chain {chain} exceeds live gap fill limit {MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS}; stored-lineage checkpoint promotion requires a canonical/safe/finalized chain_lineage ancestor at or below the provider safe/finalized head within {MAX_STORED_ANCHOR_PARENT_FETCH_DEPTH} parent fetches; run hash-pinned backfill through a stored safe/finalized ancestor, then retry live reconciliation"
        )));
    };
    let anchor_gap_blocks = stored_anchor.block_number - current_canonical_number;
    let batch_blocks = usize::try_from(anchor_gap_blocks.min(MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS))
        .expect("positive live gap batch size must fit in usize");
    let path = load_chain_lineage_canonical_child_path(
        pool,
        chain,
        current_canonical_hash,
        current_canonical_number,
        batch_blocks,
    )
    .await?;
    if path.len() != batch_blocks {
        return Ok(StoredLineagePromotion::Refused(format!(
            "canonical gap of {live_gap_blocks} blocks for chain {chain} exceeds live gap fill limit {MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS}; stored lineage path from checkpoint {} is incomplete or has duplicate canonical children before the stored safe/finalized anchor {}; rerun hash-pinned backfill for the missing range and retry",
            current_canonical_number, stored_anchor.block_number
        )));
    }
    if let Err(reason) = stored_path_has_required_raw_fact_coverage(
        pool,
        chain,
        &path,
        selected_raw_payload_addresses,
        requires_event_silent_payloads,
    )
    .await
    {
        return Ok(StoredLineagePromotion::Refused(format!(
            "canonical gap of {live_gap_blocks} blocks for chain {chain} exceeds live gap fill limit {MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS}; {reason}"
        )));
    }

    let target = path
        .last()
        .expect("non-empty stored lineage promotion path");
    let target_is_anchor = target.block_hash == stored_anchor.block_hash;
    if !target_is_anchor
        && !chain_lineage_contains_canonical_ancestor_position(
            pool,
            chain,
            &stored_anchor.block_hash,
            stored_anchor.block_number,
            target.block_number,
            &target.block_hash,
        )
        .await?
    {
        return Ok(StoredLineagePromotion::Refused(format!(
            "canonical gap of {live_gap_blocks} blocks for chain {chain} exceeds live gap fill limit {MAX_LIVE_CONTIGUOUS_GAP_FILL_BLOCKS}; promoted target block {} is not a canonical ancestor of stored safe/finalized anchor {} (provider anchor hash {}); rerun hash-pinned backfill for the canonical range before retrying",
            target.block_number, stored_anchor.block_number, provider_anchor.block_hash
        )));
    }

    let canonical = CheckpointBlockRef {
        block_hash: target.block_hash.clone(),
        block_number: target.block_number,
    };
    let reconciled_blocks = path
        .into_iter()
        .rev()
        .map(|block| lineage_block_to_provider(&block))
        .collect::<Vec<_>>();

    Ok(StoredLineagePromotion::Promoted(CanonicalReconciliation {
        status: CanonicalReconciliationStatus::StoredLineagePromoted,
        canonical: Some(canonical),
        fetched_parent_count: 0,
        orphaned_block_count: 0,
        reconciled_blocks,
        raw_orphan_stop_before_hash: None,
    }))
}

async fn select_stored_promotion_anchor(
    pool: &sqlx::PgPool,
    provider: &(impl ChainProviderOps + ?Sized),
    chain: &str,
    current_canonical_number: i64,
    candidates: &[ProviderBlock],
) -> Result<Option<(ChainLineageBlock, ProviderBlock)>> {
    for candidate in candidates {
        if candidate.block_number <= current_canonical_number {
            continue;
        }
        let mut cursor = candidate.clone();
        for parent_fetch_depth in 0..=MAX_STORED_ANCHOR_PARENT_FETCH_DEPTH {
            if cursor.block_number <= current_canonical_number {
                break;
            }
            if let Some(stored) = load_chain_lineage_block(pool, chain, &cursor.block_hash).await?
                && stored_lineage_matches_provider_block(&stored, &cursor)
                && stored_anchor_is_canonical(stored.canonicality_state)
            {
                return Ok(Some((stored, candidate.clone())));
            }
            if parent_fetch_depth == MAX_STORED_ANCHOR_PARENT_FETCH_DEPTH {
                break;
            }

            let Some(parent_hash) = cursor.parent_hash.clone() else {
                break;
            };
            let Ok(parent) = provider.fetch_block_by_hash(&parent_hash).await else {
                break;
            };
            if parent.block_hash != parent_hash || parent.block_number >= cursor.block_number {
                break;
            }
            cursor = parent;
        }
    }

    Ok(None)
}

fn stored_anchor_is_canonical(state: CanonicalityState) -> bool {
    matches!(
        state,
        CanonicalityState::Canonical | CanonicalityState::Safe | CanonicalityState::Finalized
    )
}

fn stored_lineage_matches_provider_block(
    stored: &ChainLineageBlock,
    provider: &ProviderBlock,
) -> bool {
    stored.block_hash == provider.block_hash
        && stored.parent_hash == provider.parent_hash
        && stored.block_number == provider.block_number
        && stored.block_timestamp.unix_timestamp() == provider.block_timestamp_unix_secs
        && optional_field_matches(&stored.logs_bloom, &provider.logs_bloom)
        && optional_field_matches(&stored.transactions_root, &provider.transactions_root)
        && optional_field_matches(&stored.receipts_root, &provider.receipts_root)
        && optional_field_matches(&stored.state_root, &provider.state_root)
}

fn optional_field_matches<T: Eq>(stored: &Option<T>, provider: &Option<T>) -> bool {
    matches!((stored, provider), (Some(stored), Some(provider)) if stored == provider)
        || stored.is_none()
}
