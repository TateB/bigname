use anyhow::Result;
use bigname_storage::{
    CanonicalityState, CheckpointBlockRef, chain_lineage_contains_ancestor,
    load_chain_lineage_block, upsert_chain_lineage_blocks_recanonicalizing_orphaned,
};
use tracing::{info, warn};

use crate::provider::{ChainProviderOps, ProviderBlock, ProviderHeadSnapshot};

use super::super::{
    lineage::{
        lineage_block_to_provider, provider_block_to_checkpoint_ref,
        provider_block_to_lineage_with_header_audit_mode,
    },
    types::{CanonicalReconciliation, HeaderAuditMode},
};
use super::MAX_PARENT_FETCH_DEPTH;

pub(super) async fn fill_checkpoint_ancestor_path(
    pool: &sqlx::PgPool,
    provider: &(impl ChainProviderOps + ?Sized),
    chain: &str,
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
    header_audit_mode: HeaderAuditMode,
) -> Result<usize> {
    let Some(canonical_update) = &canonical.canonical else {
        return Ok(0);
    };
    let lowest_checkpoint_block = [heads.safe.as_ref(), heads.finalized.as_ref()]
        .into_iter()
        .flatten()
        .filter(|head| head.block_number <= canonical_update.block_number)
        .filter(|head| head.block_hash != canonical_update.block_hash)
        .map(|head| head.block_number)
        .min();
    let Some(lowest_checkpoint_block) = lowest_checkpoint_block else {
        return Ok(0);
    };

    if checkpoint_head_known_on_branch(pool, chain, canonical_update, heads.safe.as_ref()).await?
        && checkpoint_head_known_on_branch(pool, chain, canonical_update, heads.finalized.as_ref())
            .await?
    {
        return Ok(0);
    }

    let mut cursor = heads.canonical.clone();
    let mut fetched_parent_count = 0usize;
    for _ in 0..MAX_PARENT_FETCH_DEPTH {
        if cursor.block_number <= lowest_checkpoint_block {
            break;
        }
        let Some(parent_hash) = cursor.parent_hash.clone() else {
            break;
        };

        cursor = if let Some(stored_parent) =
            load_chain_lineage_block(pool, chain, &parent_hash).await?
        {
            lineage_block_to_provider(&stored_parent)
        } else {
            fetched_parent_count += 1;
            provider.fetch_block_by_hash(&parent_hash).await?
        };
        upsert_chain_lineage_blocks_recanonicalizing_orphaned(
            pool,
            &[provider_block_to_lineage_with_header_audit_mode(
                chain,
                &cursor,
                CanonicalityState::Canonical,
                header_audit_mode,
            )],
        )
        .await?;
    }

    Ok(fetched_parent_count)
}

pub(super) async fn checkpoint_update_for_head(
    pool: &sqlx::PgPool,
    chain: &str,
    checkpoint_name: &str,
    current_hash: Option<&str>,
    current_number: Option<i64>,
    canonical: &CheckpointBlockRef,
    head: Option<&ProviderBlock>,
) -> Result<Option<CheckpointBlockRef>> {
    let Some(head) = head else {
        return Ok(None);
    };
    if checkpoint_head_would_regress(chain, checkpoint_name, current_hash, current_number, head) {
        return Ok(None);
    }

    if checkpoint_head_known_on_branch(pool, chain, canonical, Some(head)).await? {
        return Ok(Some(provider_block_to_checkpoint_ref(head)));
    }

    warn!(
        service = "indexer",
        chain,
        checkpoint_name,
        checkpoint_block_number = head.block_number,
        canonical_block_number = canonical.block_number,
        "provider checkpoint head skipped because it is not on the known canonical branch"
    );
    Ok(None)
}

async fn checkpoint_head_known_on_branch(
    pool: &sqlx::PgPool,
    chain: &str,
    canonical: &CheckpointBlockRef,
    head: Option<&ProviderBlock>,
) -> Result<bool> {
    let Some(head) = head else {
        return Ok(true);
    };
    if head.block_hash == canonical.block_hash {
        return Ok(true);
    }
    if head.block_number > canonical.block_number {
        return Ok(false);
    }

    chain_lineage_contains_ancestor(pool, chain, &canonical.block_hash, &head.block_hash).await
}

fn checkpoint_head_would_regress(
    chain: &str,
    checkpoint_name: &str,
    current_hash: Option<&str>,
    current_number: Option<i64>,
    head: &ProviderBlock,
) -> bool {
    let Some(current_number) = current_number else {
        return false;
    };
    if head.block_number < current_number {
        info!(
            service = "indexer",
            chain,
            checkpoint_name,
            checkpoint_block_number = head.block_number,
            current_checkpoint_block_number = current_number,
            "provider checkpoint head skipped because it would move backward"
        );
        return true;
    }

    if head.block_number == current_number
        && let Some(current_hash) = current_hash
        && head.block_hash != current_hash
    {
        warn!(
            service = "indexer",
            chain,
            checkpoint_name,
            checkpoint_block_number = head.block_number,
            checkpoint_block_hash = %head.block_hash,
            current_checkpoint_block_hash = current_hash,
            "provider checkpoint head skipped because it would switch hash at the current block number"
        );
        return true;
    }

    false
}
