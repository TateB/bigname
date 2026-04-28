use anyhow::{Context, Result, bail};

use crate::provider::{ChainProviderOps, ProviderResolvedBlock};

use super::BackfillBlockRange;

pub(super) async fn resolve_backfill_range(
    provider: &(impl ChainProviderOps + ?Sized),
    range: BackfillBlockRange,
) -> Result<Vec<ProviderResolvedBlock>> {
    let block_numbers = (range.from_block..=range.to_block).collect::<Vec<_>>();
    let resolved_blocks = provider
        .fetch_block_hashes_by_numbers(&block_numbers)
        .await
        .with_context(|| {
            format!(
                "failed to resolve backfill block numbers {}..={}",
                range.from_block, range.to_block
            )
        })?;
    if resolved_blocks.len() != block_count(range)? {
        bail!(
            "provider resolved {} backfill blocks for range {}..={} but expected {}",
            resolved_blocks.len(),
            range.from_block,
            range.to_block,
            block_count(range)?
        );
    }

    Ok(resolved_blocks)
}

fn block_count(range: BackfillBlockRange) -> Result<usize> {
    let span = range
        .to_block
        .checked_sub(range.from_block)
        .and_then(|span| span.checked_add(1))
        .context("backfill range block count overflowed i64")?;
    usize::try_from(span).context("backfill range block count does not fit in usize")
}
