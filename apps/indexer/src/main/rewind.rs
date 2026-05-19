use anyhow::{Context, Result, bail};
use bigname_storage::{
    CheckpointBlockRef, IdentityOrphanCounts, RawFactOrphanCounts, chain_lineage_contains_ancestor,
    invalidate_execution_outcomes_for_orphaned_blocks, load_chain_checkpoint,
    load_chain_lineage_block, mark_block_derived_normalized_events_range_orphaned,
    mark_chain_lineage_range_orphaned, mark_identity_rows_range_orphaned,
    mark_raw_block_facts_range_orphaned, rewind_chain_checkpoints_to_ancestor,
};

use crate::{cli::RewindArgs, reconciliation::ensure_losing_branch_raw_blocks_exist};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RewindOutcome {
    pub(crate) deployment_profile: String,
    pub(crate) chain: String,
    pub(crate) from_block_hash: String,
    pub(crate) ancestor_block_hash: String,
    pub(crate) ancestor_block_number: i64,
    pub(crate) orphaned_lineage_count: usize,
    pub(crate) orphaned_raw_fact_counts: RawFactOrphanCounts,
    pub(crate) orphaned_normalized_event_count: u64,
    pub(crate) orphaned_identity_counts: IdentityOrphanCounts,
    pub(crate) invalidated_execution_outcome_count: u64,
}

pub(crate) async fn run_rewind(args: RewindArgs) -> Result<RewindOutcome> {
    let pool = bigname_storage::connect(&args.database).await?;
    rewind_to_exact_ancestor(
        &pool,
        args.deployment_profile,
        args.chain,
        args.from_block_hash,
        CheckpointBlockRef {
            block_hash: args.ancestor_block_hash,
            block_number: args.ancestor_block_number,
        },
    )
    .await
}

async fn rewind_to_exact_ancestor(
    pool: &sqlx::PgPool,
    deployment_profile: String,
    chain: String,
    from_block_hash: Option<String>,
    ancestor: CheckpointBlockRef,
) -> Result<RewindOutcome> {
    if ancestor.block_number < 0 {
        bail!(
            "rewind ancestor for chain {chain} has negative block number {}",
            ancestor.block_number
        );
    }

    let ancestor_row = load_chain_lineage_block(pool, &chain, &ancestor.block_hash)
        .await?
        .with_context(|| {
            format!(
                "rewind ancestor block {} is not stored for chain {chain}",
                ancestor.block_hash
            )
        })?;
    if ancestor_row.block_number != ancestor.block_number {
        bail!(
            "rewind ancestor block {} for chain {chain} has stored block number {}, expected {}",
            ancestor.block_hash,
            ancestor_row.block_number,
            ancestor.block_number
        );
    }

    let from_block_hash = match from_block_hash {
        Some(from_block_hash) => from_block_hash,
        None => load_chain_checkpoint(pool, &chain)
            .await?
            .and_then(|checkpoint| checkpoint.canonical_block_hash)
            .with_context(|| {
                format!(
                    "rewind requires --from-block-hash or a stored canonical checkpoint for chain {chain}"
                )
            })?,
    };

    if !chain_lineage_contains_ancestor(pool, &chain, &from_block_hash, &ancestor.block_hash)
        .await?
    {
        bail!(
            "rewind ancestor {} is not on the stored lineage path from {} for chain {chain}",
            ancestor.block_hash,
            from_block_hash
        );
    }

    ensure_losing_branch_raw_blocks_exist(
        pool,
        &chain,
        &from_block_hash,
        Some(&ancestor.block_hash),
    )
    .await?;
    let orphaned_lineage = mark_chain_lineage_range_orphaned(
        pool,
        &chain,
        &from_block_hash,
        Some(&ancestor.block_hash),
    )
    .await?;
    let orphaned_raw_fact_counts = mark_raw_block_facts_range_orphaned(
        pool,
        &chain,
        &from_block_hash,
        Some(&ancestor.block_hash),
    )
    .await?;
    let orphaned_normalized_event_count = mark_block_derived_normalized_events_range_orphaned(
        pool,
        &chain,
        &from_block_hash,
        Some(&ancestor.block_hash),
    )
    .await?;
    let orphaned_identity_counts = mark_identity_rows_range_orphaned(
        pool,
        &chain,
        &from_block_hash,
        Some(&ancestor.block_hash),
    )
    .await?;
    let execution_summary = invalidate_execution_outcomes_for_orphaned_blocks(pool).await?;
    rewind_chain_checkpoints_to_ancestor(pool, &chain, &ancestor).await?;

    Ok(RewindOutcome {
        deployment_profile,
        chain,
        from_block_hash,
        ancestor_block_hash: ancestor.block_hash,
        ancestor_block_number: ancestor.block_number,
        orphaned_lineage_count: orphaned_lineage.len(),
        orphaned_raw_fact_counts,
        orphaned_normalized_event_count,
        orphaned_identity_counts,
        invalidated_execution_outcome_count: execution_summary.deleted_outcome_count,
    })
}
