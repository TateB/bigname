use anyhow::{Result, bail};

use super::types::ChainLineageBlock;

pub(crate) fn ensure_lineage_identity_matches(
    existing: &ChainLineageBlock,
    candidate: &ChainLineageBlock,
) -> Result<()> {
    if existing.chain_id != candidate.chain_id
        || existing.block_hash != candidate.block_hash
        || existing.parent_hash != candidate.parent_hash
        || existing.block_number != candidate.block_number
        || existing.block_timestamp != candidate.block_timestamp
        || existing.logs_bloom != candidate.logs_bloom
        || existing.transactions_root != candidate.transactions_root
        || existing.receipts_root != candidate.receipts_root
        || existing.state_root != candidate.state_root
    {
        bail!(
            "stored lineage row for chain {} block {} does not match the supplied immutable block metadata",
            candidate.chain_id,
            candidate.block_hash
        );
    }

    Ok(())
}

pub(crate) fn validate_lineage_block(block: &ChainLineageBlock) -> Result<()> {
    if block.block_number < 0 {
        bail!(
            "lineage block {} for chain {} has negative block number {}",
            block.block_hash,
            block.chain_id,
            block.block_number
        );
    }

    Ok(())
}
