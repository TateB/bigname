use anyhow::{Context, Result, bail};

use crate::provider::{ChainProviderOps, ProviderResolvedBlock};

use super::BackfillBlockRange;

pub(super) async fn resolve_backfill_range(
    provider: &(impl ChainProviderOps + ?Sized),
    range: BackfillBlockRange,
) -> Result<Vec<ProviderResolvedBlock>> {
    let block_numbers = (range.from_block..=range.to_block).collect::<Vec<_>>();
    resolve_backfill_block_numbers(provider, &block_numbers, range).await
}

pub(super) async fn resolve_backfill_block_numbers(
    provider: &(impl ChainProviderOps + ?Sized),
    block_numbers: &[i64],
    containing_range: BackfillBlockRange,
) -> Result<Vec<ProviderResolvedBlock>> {
    let resolved_blocks = provider
        .fetch_block_hashes_by_numbers(&block_numbers)
        .await
        .with_context(|| {
            format!(
                "failed to resolve backfill block numbers {}..={}",
                containing_range.from_block, containing_range.to_block
            )
        })?;
    if resolved_blocks.len() != block_numbers.len() {
        bail!(
            "provider resolved {} backfill blocks for range {}..={} but expected {}",
            resolved_blocks.len(),
            containing_range.from_block,
            containing_range.to_block,
            block_numbers.len()
        );
    }
    for (requested_number, resolved_block) in block_numbers.iter().zip(&resolved_blocks) {
        if resolved_block.block_number != *requested_number {
            bail!(
                "provider resolved requested backfill block {} as {} for range {}..={}",
                requested_number,
                resolved_block.block_number,
                containing_range.from_block,
                containing_range.to_block
            );
        }
    }

    Ok(resolved_blocks)
}
