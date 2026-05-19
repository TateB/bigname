use std::collections::BTreeSet;

use alloy_consensus::{Transaction as _, TxReceipt};
use alloy_primitives::{Address, B256};
use anyhow::{Context, Result, bail};

use crate::provider::{
    ProviderBlock, ProviderLog, ProviderReceipt, ProviderResolvedBlock, ProviderTransaction,
    ZERO_HASH,
    decode::{
        address_hex as provider_address_hex, bytes_hex as provider_bytes_hex,
        hash_hex as provider_hash_hex, parse_address as parse_provider_address,
        parse_b256 as parse_provider_b256,
    },
};

pub(super) fn provider_block_from_header(
    block_hash: B256,
    header: &impl reth_ethereum::primitives::BlockHeader,
) -> Result<ProviderBlock> {
    Ok(ProviderBlock {
        block_hash: hash_hex(block_hash),
        parent_hash: optional_parent_hash(header.parent_hash()),
        block_number: u64_to_i64(header.number(), "block number")?,
        block_timestamp_unix_secs: u64_to_i64(header.timestamp(), "block timestamp")?,
        logs_bloom: Some(header.logs_bloom().data().to_vec()),
        transactions_root: Some(hash_hex(header.transactions_root())),
        receipts_root: Some(hash_hex(header.receipts_root())),
        state_root: Some(hash_hex(header.state_root())),
    })
}

pub(super) fn provider_transactions_from_recovered(
    recovered: &reth_ethereum::primitives::RecoveredBlock<reth_ethereum::Block>,
    block: &ProviderBlock,
) -> Result<Vec<ProviderTransaction>> {
    recovered
        .transactions_with_sender()
        .enumerate()
        .map(|(index, (sender, transaction))| {
            Ok(ProviderTransaction {
                transaction_hash: hash_hex(*transaction.tx_hash()),
                block_hash: block.block_hash.clone(),
                block_number: block.block_number,
                transaction_index: usize_to_i64(index, "transaction index")?,
                from: address_hex(*sender),
                to: transaction.to().map(address_hex),
            })
        })
        .collect()
}

pub(super) fn provider_receipts_and_logs_from_recovered(
    receipts: &[reth_ethereum::Receipt],
    recovered: &reth_ethereum::primitives::RecoveredBlock<reth_ethereum::Block>,
    block: &ProviderBlock,
    include_logs: bool,
) -> Result<(Vec<ProviderReceipt>, Vec<ProviderLog>)> {
    let transactions = recovered.transactions_with_sender().collect::<Vec<_>>();
    if receipts.len() != transactions.len() {
        bail!(
            "Reth DB returned {} receipts for block {} with {} transactions",
            receipts.len(),
            block.block_hash,
            transactions.len()
        );
    }

    let mut provider_receipts = Vec::with_capacity(receipts.len());
    let mut provider_logs = Vec::new();
    let mut previous_cumulative_gas = 0u64;
    let mut next_log_index = 0usize;

    for (index, (receipt, (sender, transaction))) in receipts.iter().zip(transactions).enumerate() {
        let cumulative_gas = receipt.cumulative_gas_used();
        let gas_used = cumulative_gas
            .checked_sub(previous_cumulative_gas)
            .with_context(|| {
                format!(
                    "Reth DB receipt gas regressed in block {}",
                    block.block_hash
                )
            })?;
        previous_cumulative_gas = cumulative_gas;
        let status = receipt
            .status_or_post_state()
            .as_eip658()
            .map(|status| if status { 1 } else { 0 });
        let contract_address = if transaction.is_create() && status != Some(0) {
            Some(address_hex(sender.create(transaction.nonce())))
        } else {
            None
        };

        provider_receipts.push(ProviderReceipt {
            transaction_hash: hash_hex(*transaction.tx_hash()),
            block_hash: block.block_hash.clone(),
            block_number: block.block_number,
            transaction_index: usize_to_i64(index, "transaction index")?,
            contract_address,
            status,
            cumulative_gas_used: Some(u64_to_i64(cumulative_gas, "cumulative gas used")?),
            gas_used: Some(u64_to_i64(gas_used, "gas used")?),
            logs_bloom: Some(receipt.bloom().data().to_vec()),
        });

        if include_logs {
            for log in receipt.logs() {
                provider_logs.push(ProviderLog {
                    block_hash: block.block_hash.clone(),
                    block_number: block.block_number,
                    transaction_hash: hash_hex(*transaction.tx_hash()),
                    transaction_index: usize_to_i64(index, "transaction index")?,
                    log_index: usize_to_i64(next_log_index, "log index")?,
                    address: address_hex(log.address),
                    topics: log
                        .data
                        .topics()
                        .iter()
                        .map(|topic| hash_hex(*topic))
                        .collect(),
                    data: bytes_hex(log.data.data.as_ref()),
                });
                next_log_index = next_log_index
                    .checked_add(1)
                    .context("Reth DB log index overflow")?;
            }
        }
    }

    Ok((provider_receipts, provider_logs))
}

pub(super) fn normalized_resolved_blocks(
    resolved_blocks: &[ProviderResolvedBlock],
) -> Result<Vec<ProviderResolvedBlock>> {
    let mut seen = BTreeSet::new();
    let mut normalized = Vec::with_capacity(resolved_blocks.len());
    for resolved in resolved_blocks {
        i64_to_u64(resolved.block_number, "provider block number")?;
        if !seen.insert(resolved.block_number) {
            bail!(
                "provider requested duplicate block number {}",
                resolved.block_number
            );
        }
        normalized.push(ProviderResolvedBlock {
            block_number: resolved.block_number,
            block_hash: hash_hex(parse_b256(&resolved.block_hash, "block hash")?),
        });
    }
    Ok(normalized)
}

pub(super) fn normalized_contiguous_resolved_blocks(
    resolved_blocks: &[ProviderResolvedBlock],
) -> Result<Vec<ProviderResolvedBlock>> {
    let normalized = normalized_resolved_blocks(resolved_blocks)?;
    let mut previous = None;
    for resolved in &normalized {
        if let Some(previous) = previous {
            let expected = previous + 1;
            if resolved.block_number != expected {
                bail!(
                    "provider log range requested non-contiguous block numbers: expected {} after {}, got {}",
                    expected,
                    previous,
                    resolved.block_number
                );
            }
        }
        previous = Some(resolved.block_number);
    }
    Ok(normalized)
}

pub(super) fn parse_b256(value: &str, label: &str) -> Result<B256> {
    parse_provider_b256(value, label)
}

pub(super) fn parse_address(value: &str) -> Result<Address> {
    parse_provider_address(value)
}

pub(super) fn hash_hex(value: B256) -> String {
    provider_hash_hex(value)
}

pub(super) fn address_hex(value: Address) -> String {
    provider_address_hex(value)
}

pub(super) fn i64_to_u64(value: i64, label: &str) -> Result<u64> {
    if value < 0 {
        bail!("{label} cannot be negative: {value}");
    }
    Ok(value as u64)
}

fn optional_parent_hash(value: B256) -> Option<String> {
    let value = hash_hex(value);
    if value == ZERO_HASH || value.is_empty() {
        None
    } else {
        Some(value)
    }
}

pub(super) fn bytes_hex(bytes: &[u8]) -> String {
    provider_bytes_hex(bytes)
}

fn u64_to_i64(value: u64, label: &str) -> Result<i64> {
    i64::try_from(value).with_context(|| format!("{label} {value} does not fit in i64"))
}

fn usize_to_i64(value: usize, label: &str) -> Result<i64> {
    i64::try_from(value).with_context(|| format!("{label} {value} does not fit in i64"))
}
