use anyhow::{Result, bail};

use super::types::{RawLog, RawReceipt, RawTransaction};
pub(super) fn validate_raw_transaction(transaction: &RawTransaction) -> Result<()> {
    if transaction.block_number < 0 {
        bail!(
            "raw transaction for chain {} block {} has negative block number {}",
            transaction.chain_id,
            transaction.block_hash,
            transaction.block_number
        );
    }
    if transaction.transaction_index < 0 {
        bail!(
            "raw transaction for chain {} block {} transaction {} has negative transaction index {}",
            transaction.chain_id,
            transaction.block_hash,
            transaction.transaction_hash,
            transaction.transaction_index
        );
    }
    Ok(())
}

pub(super) fn validate_raw_receipt(receipt: &RawReceipt) -> Result<()> {
    if receipt.block_number < 0 {
        bail!(
            "raw receipt for chain {} block {} has negative block number {}",
            receipt.chain_id,
            receipt.block_hash,
            receipt.block_number
        );
    }
    if receipt.transaction_index < 0 {
        bail!(
            "raw receipt for chain {} block {} transaction {} has negative transaction index {}",
            receipt.chain_id,
            receipt.block_hash,
            receipt.transaction_hash,
            receipt.transaction_index
        );
    }
    if let Some(cumulative_gas_used) = receipt.cumulative_gas_used
        && cumulative_gas_used < 0
    {
        bail!(
            "raw receipt for chain {} block {} transaction {} has negative cumulative gas used {}",
            receipt.chain_id,
            receipt.block_hash,
            receipt.transaction_hash,
            cumulative_gas_used
        );
    }
    if let Some(gas_used) = receipt.gas_used
        && gas_used < 0
    {
        bail!(
            "raw receipt for chain {} block {} transaction {} has negative gas used {}",
            receipt.chain_id,
            receipt.block_hash,
            receipt.transaction_hash,
            gas_used
        );
    }

    Ok(())
}

pub(super) fn validate_raw_log(log: &RawLog) -> Result<()> {
    if log.block_number < 0 {
        bail!(
            "raw log for chain {} block {} has negative block number {}",
            log.chain_id,
            log.block_hash,
            log.block_number
        );
    }
    if log.transaction_index < 0 {
        bail!(
            "raw log for chain {} block {} log {} has negative transaction index {}",
            log.chain_id,
            log.block_hash,
            log.log_index,
            log.transaction_index
        );
    }
    if log.log_index < 0 {
        bail!(
            "raw log for chain {} block {} has negative log index {}",
            log.chain_id,
            log.block_hash,
            log.log_index
        );
    }

    Ok(())
}

pub(super) fn ensure_raw_transaction_identity_matches(
    existing: &RawTransaction,
    incoming: &RawTransaction,
) -> Result<()> {
    if existing.transaction_hash != incoming.transaction_hash
        || existing.block_number != incoming.block_number
        || existing.from_address != incoming.from_address
        || existing.to_address != incoming.to_address
    {
        bail!(
            "raw transaction identity mismatch for chain {} block {} index {}",
            existing.chain_id,
            existing.block_hash,
            existing.transaction_index
        );
    }

    Ok(())
}

pub(super) fn ensure_raw_receipt_identity_matches(
    existing: &RawReceipt,
    incoming: &RawReceipt,
) -> Result<()> {
    if existing.transaction_hash != incoming.transaction_hash
        || existing.block_number != incoming.block_number
        || existing.contract_address != incoming.contract_address
        || existing.status != incoming.status
        || existing.gas_used != incoming.gas_used
        || existing.cumulative_gas_used != incoming.cumulative_gas_used
        || existing.logs_bloom != incoming.logs_bloom
    {
        bail!(
            "raw receipt identity mismatch for chain {} block {} index {}",
            existing.chain_id,
            existing.block_hash,
            existing.transaction_index
        );
    }

    Ok(())
}

pub(super) fn ensure_raw_log_identity_matches(existing: &RawLog, incoming: &RawLog) -> Result<()> {
    if existing.transaction_hash != incoming.transaction_hash
        || existing.block_number != incoming.block_number
        || existing.transaction_index != incoming.transaction_index
        || existing.emitting_address != incoming.emitting_address
        || existing.topics != incoming.topics
        || existing.data != incoming.data
    {
        bail!(
            "raw log identity mismatch for chain {} block {} log {}",
            existing.chain_id,
            existing.block_hash,
            existing.log_index
        );
    }

    Ok(())
}
