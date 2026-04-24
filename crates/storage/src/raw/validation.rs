use anyhow::{Result, bail};

use super::types::RawBlock;
use crate::CanonicalityState;

pub(super) fn validate_raw_block(block: &RawBlock) -> Result<()> {
    if block.block_number < 0 {
        bail!(
            "raw block for chain {} hash {} has negative block number {}",
            block.chain_id,
            block.block_hash,
            block.block_number
        );
    }

    Ok(())
}

pub(super) fn validate_replay_range(chain_id: &str, start: i64, end: i64) -> Result<()> {
    if chain_id.trim().is_empty() {
        bail!("chain_id must not be empty");
    }
    if start < 0 {
        bail!("raw log replay range start {start} is negative");
    }
    if end < start {
        bail!("raw log replay range end {end} is before start {start}");
    }
    Ok(())
}

pub(super) fn validate_replay_hashes(chain_id: &str, block_hashes: &[String]) -> Result<()> {
    if chain_id.trim().is_empty() {
        bail!("chain_id must not be empty");
    }
    for block_hash in block_hashes {
        if block_hash.trim().is_empty() {
            bail!("raw log replay block hash set contains an empty block hash");
        }
    }
    Ok(())
}

pub(super) fn ensure_raw_identity_matches(existing: &RawBlock, incoming: &RawBlock) -> Result<()> {
    if existing.parent_hash != incoming.parent_hash
        || existing.block_number != incoming.block_number
        || existing.block_timestamp != incoming.block_timestamp
        || existing.logs_bloom != incoming.logs_bloom
        || existing.transactions_root != incoming.transactions_root
        || existing.receipts_root != incoming.receipts_root
        || existing.state_root != incoming.state_root
    {
        bail!(
            "raw block identity mismatch for chain {} block {}",
            existing.chain_id,
            existing.block_hash
        );
    }

    Ok(())
}

pub(super) fn merge_canonicality(
    current: CanonicalityState,
    incoming: CanonicalityState,
) -> CanonicalityState {
    match incoming {
        CanonicalityState::Orphaned => CanonicalityState::Orphaned,
        CanonicalityState::Observed => {
            if current == CanonicalityState::Orphaned {
                CanonicalityState::Observed
            } else {
                current
            }
        }
        CanonicalityState::Canonical | CanonicalityState::Safe | CanonicalityState::Finalized => {
            if current == CanonicalityState::Orphaned {
                incoming
            } else {
                current.promote_to(incoming)
            }
        }
    }
}
