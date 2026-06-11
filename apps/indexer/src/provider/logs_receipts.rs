use std::collections::{BTreeMap, VecDeque};

use anyhow::{Context, Result, bail};
use serde_json::Value;

use super::{
    JsonRpcProvider, ProviderBlockLogRequest, ProviderLog, ProviderRawPayloadCacheMetadata,
    ProviderReceipt, ProviderResolvedBlock,
    decode::{address_hex_from_str, hash_hex_from_str, normalize_hash},
    provider_batch_item_limit,
    request::JsonRpcBatchCall,
    types::ProviderLogFilter,
};

mod exact;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    dead_code,
    reason = "exact block helpers keep the explicit log-fetch mode"
)]
pub(super) enum ProviderBlockLogFetch {
    Fetch,
    Skip,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ProviderLogsPayload {
    pub(super) logs: Vec<ProviderLog>,
    pub(super) cache_metadata: ProviderRawPayloadCacheMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ProviderReceiptsPayload {
    pub(super) receipts: Vec<ProviderReceipt>,
    pub(super) cache_metadata: Vec<ProviderRawPayloadCacheMetadata>,
}

impl JsonRpcProvider {
    #[allow(dead_code, reason = "staged provider helper covered by tests")]
    pub async fn fetch_logs_by_block_hashes(
        &self,
        requests: &[ProviderBlockLogRequest],
    ) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
        let mut logs_by_block_number = BTreeMap::<i64, Vec<ProviderLog>>::new();
        let requests = requests
            .iter()
            .map(|request| {
                Ok(ProviderBlockLogRequest {
                    block_number: request.block_number,
                    block_hash: hash_hex_from_str(
                        &request.block_hash,
                        "provider log request block hash",
                    )?,
                    addresses: request
                        .addresses
                        .iter()
                        .map(|address| address_hex_from_str(address))
                        .collect::<Result<Vec<_>>>()?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        for request in &requests {
            if logs_by_block_number
                .insert(request.block_number, Vec::new())
                .is_some()
            {
                bail!(
                    "provider log batch requested duplicate block number {}",
                    request.block_number
                );
            }
        }

        let fetch_requests = requests
            .iter()
            .filter(|request| !request.addresses.is_empty())
            .collect::<Vec<_>>();

        for chunk in fetch_requests.chunks(provider_batch_item_limit()) {
            let calls = chunk
                .iter()
                .map(|request| {
                    let filter = ProviderLogFilter::block_hash(&request.block_hash)?
                        .with_addresses(&request.addresses)?
                        .json_rpc_parameter()?;

                    Ok(JsonRpcBatchCall {
                        method: "eth_getLogs",
                        params: vec![filter],
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let results = self.fetch_json_rpc_batch_results(calls).await?;

            for (request, result) in chunk.iter().zip(results) {
                let logs = result.with_context(|| {
                    format!(
                        "provider returned null logs for exact block hash lookup {}",
                        request.block_hash
                    )
                })?;
                let logs = logs
                    .as_array()
                    .context("expected logs array in JSON-RPC result")?;
                let logs = logs
                    .iter()
                    .map(|value| {
                        ProviderLog::from_value(value, &request.block_hash, request.block_number)
                    })
                    .collect::<Result<Vec<_>>>()?;
                logs_by_block_number.insert(request.block_number, logs);
            }
        }

        Ok(logs_by_block_number)
    }

    pub async fn fetch_logs_by_block_range(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
        addresses: &[String],
    ) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
        let mut logs_by_block_number = BTreeMap::<i64, Vec<ProviderLog>>::new();
        let mut block_hash_by_number = BTreeMap::<i64, String>::new();
        let mut previous_block_number: Option<i64> = None;

        for resolved_block in resolved_blocks {
            if resolved_block.block_number < 0 {
                bail!(
                    "provider log range requested negative block number {}",
                    resolved_block.block_number
                );
            }

            let block_hash = normalize_hash(&resolved_block.block_hash);
            if block_hash.is_empty() {
                bail!(
                    "provider log range requested block number {} with empty block hash",
                    resolved_block.block_number
                );
            }
            let block_hash = hash_hex_from_str(&block_hash, "provider log range block hash")?;

            if block_hash_by_number
                .insert(resolved_block.block_number, block_hash)
                .is_some()
            {
                bail!(
                    "provider log range requested duplicate block number {}",
                    resolved_block.block_number
                );
            }
            logs_by_block_number.insert(resolved_block.block_number, Vec::new());

            if let Some(previous_block_number) = previous_block_number {
                let expected_block_number =
                    previous_block_number.checked_add(1).with_context(|| {
                        format!(
                            "provider log range requested malformed block number after {previous_block_number}"
                        )
                    })?;
                if resolved_block.block_number != expected_block_number {
                    bail!(
                        "provider log range requested non-contiguous block numbers: expected {} after {}, got {}",
                        expected_block_number,
                        previous_block_number,
                        resolved_block.block_number
                    );
                }
            }

            previous_block_number = Some(resolved_block.block_number);
        }

        if resolved_blocks.is_empty() {
            return Ok(logs_by_block_number);
        }

        if addresses.is_empty() {
            return Ok(logs_by_block_number);
        }

        let logs = self
            .fetch_logs_by_block_range_segments(resolved_blocks, &addresses)
            .await?;
        for (log_position, value) in logs.iter().enumerate() {
            let block_number = ProviderLog::block_number_from_value(value)?;
            let block_hash = block_hash_by_number.get(&block_number).with_context(|| {
                format!(
                    "provider returned log {log_position} for unrequested block number {block_number}"
                )
            })?;
            let log = ProviderLog::from_value(value, block_hash, block_number)?;
            logs_by_block_number
                .get_mut(&block_number)
                .expect("validated log block number must have an output group")
                .push(log);
        }

        self.revalidate_range_log_block_hashes(resolved_blocks)
            .await?;

        Ok(logs_by_block_number)
    }

    pub async fn fetch_logs_by_block_range_for_topic0s_and_addresses(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
        topic0s: &[String],
        addresses: &[String],
    ) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
        let mut logs_by_block_number = BTreeMap::<i64, Vec<ProviderLog>>::new();
        let block_hash_by_number = validate_contiguous_log_range(resolved_blocks)?;
        for block_number in block_hash_by_number.keys() {
            logs_by_block_number.insert(*block_number, Vec::new());
        }

        if resolved_blocks.is_empty() {
            return Ok(logs_by_block_number);
        }

        if topic0s.is_empty() {
            return Ok(logs_by_block_number);
        }

        let logs = self
            .fetch_logs_by_block_range_topic0_segments(resolved_blocks, &topic0s, &addresses)
            .await?;
        for (log_position, value) in logs.iter().enumerate() {
            let block_number = ProviderLog::block_number_from_value(value)?;
            let block_hash = block_hash_by_number.get(&block_number).with_context(|| {
                format!(
                    "provider returned log {log_position} for unrequested block number {block_number}"
                )
            })?;
            let log = ProviderLog::from_value(value, block_hash, block_number)?;
            logs_by_block_number
                .get_mut(&block_number)
                .expect("validated log block number must have an output group")
                .push(log);
        }

        self.revalidate_range_log_block_hashes(resolved_blocks)
            .await?;

        Ok(logs_by_block_number)
    }

    async fn fetch_logs_by_block_range_segments(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
        addresses: &[String],
    ) -> Result<Vec<Value>> {
        let mut logs = Vec::new();
        let mut pending_ranges = VecDeque::from([(0usize, resolved_blocks.len())]);

        while let Some((start_index, end_index)) = pending_ranges.pop_front() {
            let range_blocks = &resolved_blocks[start_index..end_index];
            let from_block = range_blocks
                .first()
                .expect("pending log range segment must not be empty")
                .block_number;
            let to_block = range_blocks
                .last()
                .expect("pending log range segment must not be empty")
                .block_number;
            let filter = range_log_filter(from_block, to_block, addresses)?;
            let result = self
                .fetch_json_rpc_result("eth_getLogs", vec![filter])
                .await;
            let segment_logs = match result {
                Ok(Some(logs)) => logs,
                Ok(None) => bail!(
                    "provider returned null logs for block range lookup {}..={}",
                    from_block,
                    to_block
                ),
                Err(error)
                    if end_index - start_index > 1 && is_log_range_result_limit_error(&error) =>
                {
                    let midpoint = start_index + ((end_index - start_index) / 2);
                    pending_ranges.push_front((midpoint, end_index));
                    pending_ranges.push_front((start_index, midpoint));
                    continue;
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "failed to fetch provider logs for block range {}..={}",
                            from_block, to_block
                        )
                    });
                }
            };
            let segment_logs = segment_logs
                .as_array()
                .context("expected logs array in JSON-RPC result")?;
            logs.extend(segment_logs.iter().cloned());
        }

        Ok(logs)
    }

    async fn fetch_logs_by_block_range_topic0_segments(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
        topic0s: &[String],
        addresses: &[String],
    ) -> Result<Vec<Value>> {
        let mut logs = Vec::new();
        let mut pending_ranges = VecDeque::from([(0usize, resolved_blocks.len())]);

        while let Some((start_index, end_index)) = pending_ranges.pop_front() {
            let range_blocks = &resolved_blocks[start_index..end_index];
            let from_block = range_blocks
                .first()
                .expect("pending log range segment must not be empty")
                .block_number;
            let to_block = range_blocks
                .last()
                .expect("pending log range segment must not be empty")
                .block_number;
            let filter = range_topic0_log_filter(from_block, to_block, topic0s, addresses)?;
            let result = self
                .fetch_json_rpc_result("eth_getLogs", vec![filter])
                .await;
            let segment_logs = match result {
                Ok(Some(logs)) => logs,
                Ok(None) => bail!(
                    "provider returned null logs for block range topic0 lookup {}..={}",
                    from_block,
                    to_block
                ),
                Err(error)
                    if end_index - start_index > 1 && is_log_range_result_limit_error(&error) =>
                {
                    let midpoint = start_index + ((end_index - start_index) / 2);
                    pending_ranges.push_front((midpoint, end_index));
                    pending_ranges.push_front((start_index, midpoint));
                    continue;
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "failed to fetch provider topic0 logs for block range {}..={}",
                            from_block, to_block
                        )
                    });
                }
            };
            let segment_logs = segment_logs
                .as_array()
                .context("expected logs array in JSON-RPC result")?;
            logs.extend(segment_logs.iter().cloned());
        }

        Ok(logs)
    }

    async fn revalidate_range_log_block_hashes(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<()> {
        let block_numbers = resolved_blocks
            .iter()
            .map(|resolved_block| resolved_block.block_number)
            .collect::<Vec<_>>();
        let revalidated_blocks = self
            .fetch_block_hashes_by_numbers(&block_numbers)
            .await
            .context("provider failed to revalidate block hashes after range log lookup")?;

        if revalidated_blocks.len() != resolved_blocks.len() {
            bail!(
                "provider revalidated {} blocks after range log lookup for {} requested blocks",
                revalidated_blocks.len(),
                resolved_blocks.len()
            );
        }

        for (expected, actual) in resolved_blocks.iter().zip(revalidated_blocks) {
            let expected_hash =
                hash_hex_from_str(&expected.block_hash, "provider revalidation block hash")?;
            if actual.block_number != expected.block_number {
                bail!(
                    "provider revalidated block number {} after range log lookup, but received block number {}",
                    expected.block_number,
                    actual.block_number
                );
            }
            if actual.block_hash != expected_hash {
                bail!(
                    "provider block hash changed after range log lookup for block number {}: expected {}, got {}",
                    expected.block_number,
                    expected_hash,
                    actual.block_hash
                );
            }
        }

        Ok(())
    }
}

fn range_log_filter(from_block: i64, to_block: i64, addresses: &[String]) -> Result<Value> {
    ProviderLogFilter::block_range(from_block, to_block)
        .with_addresses(addresses)?
        .json_rpc_parameter()
}

fn range_topic0_log_filter(
    from_block: i64,
    to_block: i64,
    topic0s: &[String],
    addresses: &[String],
) -> Result<Value> {
    ProviderLogFilter::block_range(from_block, to_block)
        .with_topic0s(topic0s)?
        .with_addresses(addresses)?
        .json_rpc_parameter()
}

fn validate_contiguous_log_range(
    resolved_blocks: &[ProviderResolvedBlock],
) -> Result<BTreeMap<i64, String>> {
    let mut block_hash_by_number = BTreeMap::<i64, String>::new();
    let mut previous_block_number: Option<i64> = None;

    for resolved_block in resolved_blocks {
        if resolved_block.block_number < 0 {
            bail!(
                "provider log range requested negative block number {}",
                resolved_block.block_number
            );
        }

        let block_hash = normalize_hash(&resolved_block.block_hash);
        if block_hash.is_empty() {
            bail!(
                "provider log range requested block number {} with empty block hash",
                resolved_block.block_number
            );
        }
        let block_hash = hash_hex_from_str(&block_hash, "provider log range block hash")?;

        if block_hash_by_number
            .insert(resolved_block.block_number, block_hash)
            .is_some()
        {
            bail!(
                "provider log range requested duplicate block number {}",
                resolved_block.block_number
            );
        }

        if let Some(previous_block_number) = previous_block_number {
            let expected_block_number = previous_block_number.checked_add(1).with_context(|| {
                format!(
                    "provider log range requested malformed block number after {previous_block_number}"
                )
            })?;
            if resolved_block.block_number != expected_block_number {
                bail!(
                    "provider log range requested non-contiguous block numbers: expected {} after {}, got {}",
                    expected_block_number,
                    previous_block_number,
                    resolved_block.block_number
                );
            }
        }

        previous_block_number = Some(resolved_block.block_number);
    }

    Ok(block_hash_by_number)
}

fn is_log_range_result_limit_error(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    message.contains("query exceeds max results")
        || message.contains("query returned more than")
        || message.contains("log response size exceeded")
        || message.contains("response size exceeded")
        || message.contains("result size exceeded")
        || message.contains("more than 10000 results")
}
