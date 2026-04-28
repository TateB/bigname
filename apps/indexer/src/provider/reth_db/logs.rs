use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

use alloy_consensus::{BlockHeader as _, Transaction as _, TxReceipt as _};
use alloy_rpc_types_eth::{Filter, Topic};
use anyhow::{Context, Result, bail};
use reth_ethereum::provider::{
    BlockBodyIndicesProvider, HeaderProvider, ReceiptProvider, TransactionsProvider,
};
use tracing::info;

use super::{
    RethDbReader,
    convert::{address_hex, bytes_hex, hash_hex, i64_to_u64, parse_address, parse_b256},
};
use crate::provider::{ProviderLog, ProviderResolvedBlock};

impl RethDbReader {
    pub(super) fn fetch_logs_by_block_range_sync(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
        addresses: &[String],
    ) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
        let started = Instant::now();
        let mut logs_by_number = BTreeMap::<i64, Vec<ProviderLog>>::new();
        let addresses = addresses
            .iter()
            .map(|address| parse_address(address))
            .collect::<Result<BTreeSet<_>>>()?;
        if addresses.is_empty() {
            return Ok(logs_by_number);
        }

        let factory = self.factory()?;
        let from_block = resolved_blocks
            .first()
            .map(|block| i64_to_u64(block.block_number, "provider block number"))
            .transpose()?;
        let to_block = resolved_blocks
            .last()
            .map(|block| i64_to_u64(block.block_number, "provider block number"))
            .transpose()?;
        let address_filter = Filter::new().address(addresses.iter().copied().collect::<Vec<_>>());
        let headers = match (from_block, to_block) {
            (Some(from_block), Some(to_block)) => {
                factory.sealed_headers_range(from_block..=to_block)?
            }
            _ => Vec::new(),
        };
        if headers.len() != resolved_blocks.len() {
            bail!(
                "Reth DB returned {} sealed headers for {} resolved log-range blocks",
                headers.len(),
                resolved_blocks.len()
            );
        }

        let mut bloom_positive_block_count = 0usize;
        let mut matched_block_count = 0usize;
        let mut scanned_receipt_count = 0usize;
        for (resolved_block, header) in resolved_blocks.iter().zip(headers.iter()) {
            if header.number() != i64_to_u64(resolved_block.block_number, "provider block number")?
            {
                bail!(
                    "Reth DB returned sealed header number {} for requested log block {}",
                    header.number(),
                    resolved_block.block_number
                );
            }
            let block_hash = header.hash();
            let header_hash = hash_hex(block_hash);
            if header_hash != resolved_block.block_hash {
                bail!(
                    "Reth DB resolved log block number {} to hash {}, but range header returned hash {}",
                    resolved_block.block_number,
                    resolved_block.block_hash,
                    header_hash
                );
            }
            if !address_filter.matches_bloom(header.header().logs_bloom()) {
                continue;
            }
            bloom_positive_block_count += 1;

            let receipts = factory
                .receipts_by_block(block_hash.into())?
                .with_context(|| {
                    format!(
                        "Reth DB did not return receipts for block {}",
                        hash_hex(block_hash)
                    )
                })?;
            scanned_receipt_count += receipts.len();
            let mut next_log_index = 0u64;
            let mut first_tx_num = None;
            let mut block_matched_log_count = 0usize;

            for (transaction_index, receipt) in receipts.iter().enumerate() {
                let mut transaction_hash = None::<String>;
                for log in receipt.logs() {
                    if addresses.contains(&log.address) {
                        let transaction_hash = match &transaction_hash {
                            Some(transaction_hash) => transaction_hash.clone(),
                            None => {
                                let first_tx_num = match first_tx_num {
                                    Some(first_tx_num) => first_tx_num,
                                    None => {
                                        let indices = factory
                                            .block_body_indices(header.number())?
                                            .with_context(|| {
                                                format!(
                                                    "Reth DB did not return block body indices for block {}",
                                                    header.number()
                                                )
                                            })?;
                                        first_tx_num = Some(indices.first_tx_num);
                                        indices.first_tx_num
                                    }
                                };
                                let transaction_id = first_tx_num
                                    .checked_add(u64::try_from(transaction_index).with_context(
                                        || {
                                            format!(
                                                "transaction index {transaction_index} does not fit in u64"
                                            )
                                        },
                                    )?)
                                    .context("Reth DB transaction id overflow")?;
                                let transaction = factory
                                    .transaction_by_id(transaction_id)?
                                    .with_context(|| {
                                        format!(
                                            "Reth DB did not return transaction id {transaction_id} for block {}",
                                            header_hash
                                        )
                                    })?;
                                let hash = hash_hex(*transaction.tx_hash());
                                transaction_hash = Some(hash.clone());
                                hash
                            }
                        };
                        logs_by_number
                            .entry(resolved_block.block_number)
                            .or_default()
                            .push(ProviderLog {
                                block_hash: header_hash.clone(),
                                block_number: resolved_block.block_number,
                                transaction_hash,
                                transaction_index: i64::try_from(transaction_index).with_context(
                                    || {
                                        format!(
                                            "transaction index {transaction_index} does not fit in i64"
                                        )
                                    },
                                )?,
                                log_index: i64::try_from(next_log_index).with_context(|| {
                                    format!("log index {next_log_index} does not fit in i64")
                                })?,
                                address: address_hex(log.address),
                                topics: log
                                    .data
                                    .topics()
                                    .iter()
                                    .map(|topic| hash_hex(*topic))
                                    .collect(),
                                data: bytes_hex(log.data.data.as_ref()),
                            });
                        block_matched_log_count += 1;
                    }
                    next_log_index = next_log_index
                        .checked_add(1)
                        .context("Reth DB log index overflow")?;
                }
            }

            if block_matched_log_count > 0 {
                matched_block_count += 1;
            }
        }

        self.revalidate_resolved_blocks(resolved_blocks)
            .context("Reth DB failed to revalidate block hashes after range log lookup")?;

        let total_log_count = logs_by_number
            .values()
            .map(std::vec::Vec::len)
            .sum::<usize>();
        info!(
            service = "indexer",
            command = "provider",
            chain = %self.chain,
            from_block = resolved_blocks.first().map(|block| block.block_number),
            to_block = resolved_blocks.last().map(|block| block.block_number),
            resolved_block_count = resolved_blocks.len(),
            bloom_positive_block_count,
            matched_block_count,
            scanned_receipt_count,
            matched_log_count = total_log_count,
            elapsed_ms = started.elapsed().as_millis(),
            "Reth DB log-range lookup completed"
        );

        Ok(logs_by_number)
    }

    pub(super) fn fetch_logs_by_block_range_for_topic0s_and_addresses_sync(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
        topic0s: &[String],
        addresses: &[String],
    ) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
        let started = Instant::now();
        let mut logs_by_number = BTreeMap::<i64, Vec<ProviderLog>>::new();
        let topic0s = topic0s
            .iter()
            .map(|topic0| parse_b256(topic0, "log topic0"))
            .collect::<Result<BTreeSet<_>>>()?;
        let addresses = addresses
            .iter()
            .map(|address| parse_address(address))
            .collect::<Result<BTreeSet<_>>>()?;
        if topic0s.is_empty() {
            return Ok(logs_by_number);
        }

        let factory = self.factory()?;
        let from_block = resolved_blocks
            .first()
            .map(|block| i64_to_u64(block.block_number, "provider block number"))
            .transpose()?;
        let to_block = resolved_blocks
            .last()
            .map(|block| i64_to_u64(block.block_number, "provider block number"))
            .transpose()?;
        let topic_filter: Topic = topic0s.iter().copied().collect();
        let mut filter = Filter::new().event_signature(topic_filter);
        if !addresses.is_empty() {
            filter = filter.address(addresses.iter().copied().collect::<Vec<_>>());
        }
        let headers = match (from_block, to_block) {
            (Some(from_block), Some(to_block)) => {
                factory.sealed_headers_range(from_block..=to_block)?
            }
            _ => Vec::new(),
        };
        if headers.len() != resolved_blocks.len() {
            bail!(
                "Reth DB returned {} sealed headers for {} resolved topic0 log-range blocks",
                headers.len(),
                resolved_blocks.len()
            );
        }

        let mut bloom_positive_block_count = 0usize;
        let mut matched_block_count = 0usize;
        let mut scanned_receipt_count = 0usize;
        for (resolved_block, header) in resolved_blocks.iter().zip(headers.iter()) {
            if header.number() != i64_to_u64(resolved_block.block_number, "provider block number")?
            {
                bail!(
                    "Reth DB returned sealed header number {} for requested topic0 log block {}",
                    header.number(),
                    resolved_block.block_number
                );
            }
            let block_hash = header.hash();
            let header_hash = hash_hex(block_hash);
            if header_hash != resolved_block.block_hash {
                bail!(
                    "Reth DB resolved topic0 log block number {} to hash {}, but range header returned hash {}",
                    resolved_block.block_number,
                    resolved_block.block_hash,
                    header_hash
                );
            }
            if !filter.matches_bloom(header.header().logs_bloom()) {
                continue;
            }
            bloom_positive_block_count += 1;

            let receipts = factory
                .receipts_by_block(block_hash.into())?
                .with_context(|| {
                    format!(
                        "Reth DB did not return receipts for block {}",
                        hash_hex(block_hash)
                    )
                })?;
            scanned_receipt_count += receipts.len();
            let mut next_log_index = 0u64;
            let mut first_tx_num = None;
            let mut block_matched_log_count = 0usize;

            for (transaction_index, receipt) in receipts.iter().enumerate() {
                let mut transaction_hash = None::<String>;
                for log in receipt.logs() {
                    if filter.matches(log) {
                        let transaction_hash = match &transaction_hash {
                            Some(transaction_hash) => transaction_hash.clone(),
                            None => {
                                let first_tx_num = match first_tx_num {
                                    Some(first_tx_num) => first_tx_num,
                                    None => {
                                        let indices = factory
                                            .block_body_indices(header.number())?
                                            .with_context(|| {
                                                format!(
                                                    "Reth DB did not return block body indices for block {}",
                                                    header.number()
                                                )
                                            })?;
                                        first_tx_num = Some(indices.first_tx_num);
                                        indices.first_tx_num
                                    }
                                };
                                let transaction_id = first_tx_num
                                    .checked_add(u64::try_from(transaction_index).with_context(
                                        || {
                                            format!(
                                                "transaction index {transaction_index} does not fit in u64"
                                            )
                                        },
                                    )?)
                                    .context("Reth DB transaction id overflow")?;
                                let transaction = factory
                                    .transaction_by_id(transaction_id)?
                                    .with_context(|| {
                                        format!(
                                            "Reth DB did not return transaction id {transaction_id} for block {}",
                                            header_hash
                                        )
                                    })?;
                                let hash = hash_hex(*transaction.tx_hash());
                                transaction_hash = Some(hash.clone());
                                hash
                            }
                        };
                        logs_by_number
                            .entry(resolved_block.block_number)
                            .or_default()
                            .push(ProviderLog {
                                block_hash: header_hash.clone(),
                                block_number: resolved_block.block_number,
                                transaction_hash,
                                transaction_index: i64::try_from(transaction_index).with_context(
                                    || {
                                        format!(
                                            "transaction index {transaction_index} does not fit in i64"
                                        )
                                    },
                                )?,
                                log_index: i64::try_from(next_log_index).with_context(|| {
                                    format!("log index {next_log_index} does not fit in i64")
                                })?,
                                address: address_hex(log.address),
                                topics: log
                                    .data
                                    .topics()
                                    .iter()
                                    .map(|topic| hash_hex(*topic))
                                    .collect(),
                                data: bytes_hex(log.data.data.as_ref()),
                            });
                        block_matched_log_count += 1;
                    }
                    next_log_index = next_log_index
                        .checked_add(1)
                        .context("Reth DB log index overflow")?;
                }
            }

            if block_matched_log_count > 0 {
                matched_block_count += 1;
            }
        }

        self.revalidate_resolved_blocks(resolved_blocks)
            .context("Reth DB failed to revalidate block hashes after topic0 range log lookup")?;

        let total_log_count = logs_by_number
            .values()
            .map(std::vec::Vec::len)
            .sum::<usize>();
        info!(
            service = "indexer",
            command = "provider",
            chain = %self.chain,
            from_block = resolved_blocks.first().map(|block| block.block_number),
            to_block = resolved_blocks.last().map(|block| block.block_number),
            resolved_block_count = resolved_blocks.len(),
            topic0_count = topic0s.len(),
            address_count = addresses.len(),
            bloom_positive_block_count,
            matched_block_count,
            scanned_receipt_count,
            matched_log_count = total_log_count,
            elapsed_ms = started.elapsed().as_millis(),
            "Reth DB topic0/address log-range lookup completed"
        );

        Ok(logs_by_number)
    }
}
