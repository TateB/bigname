use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, Result, bail};
use reqwest::Url;
use serde_json::Value;
use tokio::{task::JoinSet, time::sleep};
use tracing::warn;

use super::{
    JsonRpcProvider, ProviderBlockBundle, ProviderReceipt, ProviderResolvedBlock,
    ProviderTransaction, ProviderTransactionReceiptBundle, ProviderTransactionReceiptRequest,
    provider_batch_item_limit, provider_batch_request_concurrency, request::JsonRpcBatchCall,
};
use validation::{fallback_receipts_by_key, fallback_transactions_by_key};

mod validation;
pub(super) use validation::validate_transaction_receipt_pair;

struct FallbackBlockPayload {
    transactions_by_key: BTreeMap<(String, i64), ProviderTransaction>,
    receipts_by_key: BTreeMap<(String, i64), ProviderReceipt>,
}

const SELECTED_TRANSACTION_RECEIPT_RETRY_ATTEMPTS: usize = 3;
const SELECTED_TRANSACTION_RECEIPT_RETRY_DELAY: Duration = Duration::from_millis(250);

impl JsonRpcProvider {
    pub async fn fetch_transaction_receipt_pairs_by_hashes(
        &self,
        requests: &[ProviderTransactionReceiptRequest],
    ) -> Result<Vec<ProviderTransactionReceiptBundle>> {
        let mut bundles = vec![None; requests.len()];
        let batch_pair_limit = (provider_batch_item_limit() / 2).max(1);
        let direct_batches = requests
            .chunks(batch_pair_limit)
            .enumerate()
            .map(|(chunk_index, chunk)| {
                let start_index = chunk_index * batch_pair_limit;
                let chunk_requests = chunk.to_vec();
                let calls = chunk
                    .iter()
                    .flat_map(|request| {
                        [
                            JsonRpcBatchCall {
                                method: "eth_getTransactionByHash",
                                params: vec![Value::String(request.transaction_hash.clone())],
                            },
                            JsonRpcBatchCall {
                                method: "eth_getTransactionReceipt",
                                params: vec![Value::String(request.transaction_hash.clone())],
                            },
                        ]
                    })
                    .collect::<Vec<_>>();
                (start_index, chunk_requests, calls)
            })
            .collect::<Vec<_>>();
        let direct_results = self
            .fetch_json_rpc_batch_results_bounded(
                direct_batches
                    .iter()
                    .map(|(_, _, calls)| calls.clone())
                    .collect(),
            )
            .await?;
        let mut fallback_requests = Vec::new();

        for ((start_index, chunk_requests, _), results) in
            direct_batches.into_iter().zip(direct_results)
        {
            let mut result_pairs = results.chunks_exact(2);

            for ((chunk_index, request), pair) in
                chunk_requests.iter().enumerate().zip(&mut result_pairs)
            {
                let bundle_index = start_index + chunk_index;
                let Some(transaction_value) = pair[0].clone() else {
                    fallback_requests.push((bundle_index, request.clone()));
                    continue;
                };
                let Some(receipt_value) = pair[1].clone() else {
                    fallback_requests.push((bundle_index, request.clone()));
                    continue;
                };
                let transaction = ProviderTransaction::from_value(&transaction_value)?;
                let receipt = ProviderReceipt::from_value(&receipt_value)?;
                validate_transaction_receipt_pair(request, &transaction, &receipt)?;
                bundles[bundle_index] = Some(ProviderTransactionReceiptBundle {
                    transaction,
                    receipt,
                });
            }
            if !result_pairs.remainder().is_empty() {
                bail!("provider returned an odd number of transaction/receipt batch results");
            }
        }

        if !fallback_requests.is_empty() {
            warn!(
                selected_transaction_receipt_fallback_count = fallback_requests.len(),
                "falling back to block-scoped receipts for selected transaction/receipt lookup"
            );
            let fallback_only_requests = fallback_requests
                .iter()
                .map(|(_, request)| request.clone())
                .collect::<Vec<_>>();
            let fallback_bundles = self
                .fetch_transaction_receipt_pairs_by_block_fallback(&fallback_only_requests)
                .await?;
            for ((bundle_index, _), bundle) in fallback_requests.into_iter().zip(fallback_bundles) {
                bundles[bundle_index] = Some(bundle);
            }
        }

        bundles
            .into_iter()
            .map(|bundle| bundle.context("provider omitted selected transaction/receipt pair"))
            .collect()
    }

    async fn fetch_transaction_receipt_pairs_by_block_fallback(
        &self,
        requests: &[ProviderTransactionReceiptRequest],
    ) -> Result<Vec<ProviderTransactionReceiptBundle>> {
        let mut resolved_by_block = BTreeMap::<(String, i64), ProviderResolvedBlock>::new();
        for request in requests {
            resolved_by_block
                .entry((request.block_hash.clone(), request.block_number))
                .or_insert_with(|| ProviderResolvedBlock {
                    block_hash: request.block_hash.clone(),
                    block_number: request.block_number,
                });
        }
        let resolved_blocks = resolved_by_block.into_values().collect::<Vec<_>>();
        let block_payloads = match self
            .fetch_selected_transaction_receipt_fallback_blocks(&resolved_blocks)
            .await
        {
            Ok(block_payloads) => block_payloads,
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    selected_transaction_receipt_fallback_count = requests.len(),
                    "block-scoped selected transaction/receipt fallback failed; retrying direct lookup"
                );
                let retry_bundles = self
                    .recover_selected_transaction_receipt_pairs_by_direct_retry(requests)
                    .await?;
                return retry_bundles
                    .into_iter()
                    .map(|bundle| {
                        bundle.context(
                            "provider block fallback and direct retry did not return selected transaction/receipt pair",
                        )
                    })
                    .collect();
            }
        };
        let mut selected_bundles = vec![None; requests.len()];
        let mut direct_retry_requests = Vec::new();

        for (request_index, request) in requests.iter().enumerate() {
            let block_payload = block_payloads.get(&request.block_hash).with_context(|| {
                format!(
                    "provider did not return fallback block {} for selected transaction {}",
                    request.block_hash, request.transaction_hash
                )
            })?;
            let key = (request.transaction_hash.clone(), request.transaction_index);
            let transaction = block_payload.transactions_by_key.get(&key).cloned();
            let receipt = block_payload.receipts_by_key.get(&key).cloned();
            let Some(transaction) = transaction else {
                direct_retry_requests.push((request_index, request.clone()));
                continue;
            };
            let Some(receipt) = receipt else {
                direct_retry_requests.push((request_index, request.clone()));
                continue;
            };
            validate_transaction_receipt_pair(request, &transaction, &receipt)?;
            selected_bundles[request_index] = Some(ProviderTransactionReceiptBundle {
                transaction,
                receipt,
            });
        }

        if !direct_retry_requests.is_empty() {
            warn!(
                selected_transaction_receipt_direct_retry_count = direct_retry_requests.len(),
                "re-reading selected transaction/receipt pairs after block-scoped fallback omitted them"
            );
            let retry_only_requests = direct_retry_requests
                .iter()
                .map(|(_, request)| request.clone())
                .collect::<Vec<_>>();
            let retry_bundles = self
                .recover_selected_transaction_receipt_pairs_by_direct_retry(&retry_only_requests)
                .await?;
            for ((request_index, request), retry_bundle) in
                direct_retry_requests.into_iter().zip(retry_bundles)
            {
                let retry_bundle = retry_bundle.with_context(|| {
                    format!(
                        "provider block fallback and direct retry did not return selected transaction/receipt {}",
                        request.transaction_hash
                    )
                })?;
                selected_bundles[request_index] = Some(retry_bundle);
            }
        }

        selected_bundles
            .into_iter()
            .map(|bundle| bundle.context("provider omitted selected transaction/receipt pair"))
            .collect()
    }

    async fn recover_selected_transaction_receipt_pairs_by_direct_retry(
        &self,
        requests: &[ProviderTransactionReceiptRequest],
    ) -> Result<Vec<Option<ProviderTransactionReceiptBundle>>> {
        let retry_bundles = self
            .fetch_transaction_receipt_pairs_by_direct_retry(requests)
            .await?;
        Ok(self
            .fill_missing_transaction_receipt_pairs_from_fallback(requests.to_vec(), retry_bundles)
            .await)
    }

    async fn fetch_selected_transaction_receipt_fallback_blocks(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<BTreeMap<String, FallbackBlockPayload>> {
        let mut block_payloads = BTreeMap::<String, FallbackBlockPayload>::new();
        let block_chunks = resolved_blocks
            .chunks(provider_batch_item_limit())
            .map(<[_]>::to_vec)
            .collect::<Vec<_>>();
        let block_call_batches = block_chunks
            .iter()
            .map(|chunk| {
                chunk
                    .iter()
                    .map(|resolved_block| JsonRpcBatchCall {
                        method: "eth_getBlockByHash",
                        params: vec![
                            Value::String(resolved_block.block_hash.clone()),
                            Value::Bool(true),
                        ],
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let block_results = self
            .fetch_json_rpc_batch_results_bounded(block_call_batches)
            .await?;

        for (chunk, results) in block_chunks.iter().zip(block_results) {
            for (resolved_block, result) in chunk.iter().zip(results) {
                let block_value = result.with_context(|| {
                    format!(
                        "provider did not return fallback block {}",
                        resolved_block.block_hash
                    )
                })?;
                let block_bundle = ProviderBlockBundle::from_value(block_value)?;
                if block_bundle.block.block_hash != resolved_block.block_hash {
                    bail!(
                        "provider returned fallback block {} for requested hash {}",
                        block_bundle.block.block_hash,
                        resolved_block.block_hash
                    );
                }
                if block_bundle.block.block_number != resolved_block.block_number {
                    bail!(
                        "provider returned fallback block {} with number {}; expected {}",
                        resolved_block.block_hash,
                        block_bundle.block.block_number,
                        resolved_block.block_number
                    );
                }

                let transactions_by_key =
                    fallback_transactions_by_key(resolved_block, block_bundle.transactions)?;
                block_payloads.insert(
                    resolved_block.block_hash.clone(),
                    FallbackBlockPayload {
                        transactions_by_key,
                        receipts_by_key: BTreeMap::new(),
                    },
                );
            }
        }

        let receipt_call_batches = block_chunks
            .iter()
            .map(|chunk| {
                chunk
                    .iter()
                    .map(|resolved_block| JsonRpcBatchCall {
                        method: "eth_getBlockReceipts",
                        params: vec![Value::String(resolved_block.block_hash.clone())],
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let receipt_results = self
            .fetch_json_rpc_batch_results_bounded(receipt_call_batches)
            .await?;

        for (chunk, results) in block_chunks.iter().zip(receipt_results) {
            for (resolved_block, result) in chunk.iter().zip(results) {
                let receipts = result.with_context(|| {
                    format!(
                        "provider returned null fallback receipts for block {}",
                        resolved_block.block_hash
                    )
                })?;
                let receipts = receipts
                    .as_array()
                    .context("expected fallback receipts array in JSON-RPC result")?
                    .iter()
                    .map(ProviderReceipt::from_value)
                    .collect::<Result<Vec<_>>>()?;
                let receipts_by_key = fallback_receipts_by_key(resolved_block, receipts)?;
                let block_payload = block_payloads
                    .get_mut(&resolved_block.block_hash)
                    .with_context(|| {
                        format!(
                            "provider returned fallback receipts for unrequested block {}",
                            resolved_block.block_hash
                        )
                    })?;
                block_payload.receipts_by_key = receipts_by_key;
            }
        }

        Ok(block_payloads)
    }

    async fn fill_missing_transaction_receipt_pairs_from_fallback(
        &self,
        requests: Vec<ProviderTransactionReceiptRequest>,
        mut bundles: Vec<Option<ProviderTransactionReceiptBundle>>,
    ) -> Vec<Option<ProviderTransactionReceiptBundle>> {
        let Some(endpoint) = self.receipt_fallback_endpoint.as_ref() else {
            return bundles;
        };
        let pending = requests
            .iter()
            .enumerate()
            .filter(|(index, _)| bundles[*index].is_none())
            .map(|(index, request)| (index, request.clone()))
            .collect::<Vec<_>>();
        if pending.is_empty() {
            return bundles;
        }

        warn!(
            selected_transaction_receipt_fallback_provider_count = pending.len(),
            "reading omitted selected transaction/receipt pairs from fallback JSON-RPC provider"
        );
        let fallback_requests = pending
            .iter()
            .map(|(_, request)| request.clone())
            .collect::<Vec<_>>();
        let fallback_bundles = match self
            .fetch_transaction_receipt_pairs_by_hashes_once_from_endpoint(
                &fallback_requests,
                endpoint,
            )
            .await
        {
            Ok(fallback_bundles) => fallback_bundles,
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    "fallback JSON-RPC provider failed selected transaction/receipt lookup"
                );
                return bundles;
            }
        };
        let mut recovered_count = 0usize;
        for ((index, _), fallback_bundle) in pending.into_iter().zip(fallback_bundles) {
            if fallback_bundle.is_some() {
                recovered_count += 1;
                bundles[index] = fallback_bundle;
            }
        }
        warn!(
            selected_transaction_receipt_fallback_provider_recovered_count = recovered_count,
            "fallback JSON-RPC provider selected transaction/receipt lookup completed"
        );

        bundles
    }

    async fn fetch_transaction_receipt_pairs_by_direct_retry(
        &self,
        requests: &[ProviderTransactionReceiptRequest],
    ) -> Result<Vec<Option<ProviderTransactionReceiptBundle>>> {
        let mut bundles = vec![None; requests.len()];

        for attempt in 0..SELECTED_TRANSACTION_RECEIPT_RETRY_ATTEMPTS {
            let pending = requests
                .iter()
                .enumerate()
                .filter(|(index, _)| bundles[*index].is_none())
                .map(|(index, request)| (index, request.clone()))
                .collect::<Vec<_>>();
            if pending.is_empty() {
                break;
            }
            if attempt > 0 {
                sleep(SELECTED_TRANSACTION_RECEIPT_RETRY_DELAY).await;
            }

            let retry_requests = pending
                .iter()
                .map(|(_, request)| request.clone())
                .collect::<Vec<_>>();
            let retry_bundles = self
                .fetch_transaction_receipt_pairs_by_hashes_once(&retry_requests)
                .await?;
            for ((index, _), retry_bundle) in pending.into_iter().zip(retry_bundles) {
                if retry_bundle.is_some() {
                    bundles[index] = retry_bundle;
                }
            }
        }

        Ok(bundles)
    }

    async fn fetch_transaction_receipt_pairs_by_hashes_once(
        &self,
        requests: &[ProviderTransactionReceiptRequest],
    ) -> Result<Vec<Option<ProviderTransactionReceiptBundle>>> {
        self.fetch_transaction_receipt_pairs_by_hashes_once_from_endpoint(requests, &self.endpoint)
            .await
    }

    async fn fetch_transaction_receipt_pairs_by_hashes_once_from_endpoint(
        &self,
        requests: &[ProviderTransactionReceiptRequest],
        endpoint: &Url,
    ) -> Result<Vec<Option<ProviderTransactionReceiptBundle>>> {
        let mut bundles = vec![None; requests.len()];
        let batch_pair_limit = (provider_batch_item_limit() / 2).max(1);
        let direct_batches = requests
            .chunks(batch_pair_limit)
            .enumerate()
            .map(|(chunk_index, chunk)| {
                let start_index = chunk_index * batch_pair_limit;
                let chunk_requests = chunk.to_vec();
                let calls = chunk
                    .iter()
                    .flat_map(|request| {
                        [
                            JsonRpcBatchCall {
                                method: "eth_getTransactionByHash",
                                params: vec![Value::String(request.transaction_hash.clone())],
                            },
                            JsonRpcBatchCall {
                                method: "eth_getTransactionReceipt",
                                params: vec![Value::String(request.transaction_hash.clone())],
                            },
                        ]
                    })
                    .collect::<Vec<_>>();
                (start_index, chunk_requests, calls)
            })
            .collect::<Vec<_>>();
        let direct_results = self
            .fetch_json_rpc_batch_results_bounded_at_endpoint(
                endpoint,
                direct_batches
                    .iter()
                    .map(|(_, _, calls)| calls.clone())
                    .collect(),
            )
            .await?;

        for ((start_index, chunk_requests, _), results) in
            direct_batches.into_iter().zip(direct_results)
        {
            let mut result_pairs = results.chunks_exact(2);

            for ((chunk_index, request), pair) in
                chunk_requests.iter().enumerate().zip(&mut result_pairs)
            {
                let bundle_index = start_index + chunk_index;
                let Some(transaction_value) = pair[0].clone() else {
                    continue;
                };
                let Some(receipt_value) = pair[1].clone() else {
                    continue;
                };
                let transaction = ProviderTransaction::from_value(&transaction_value)?;
                let receipt = ProviderReceipt::from_value(&receipt_value)?;
                validate_transaction_receipt_pair(request, &transaction, &receipt)?;
                bundles[bundle_index] = Some(ProviderTransactionReceiptBundle {
                    transaction,
                    receipt,
                });
            }
            if !result_pairs.remainder().is_empty() {
                bail!("provider returned an odd number of transaction/receipt batch results");
            }
        }

        Ok(bundles)
    }

    async fn fetch_json_rpc_batch_results_bounded(
        &self,
        call_batches: Vec<Vec<JsonRpcBatchCall>>,
    ) -> Result<Vec<Vec<Option<Value>>>> {
        self.fetch_json_rpc_batch_results_bounded_at_endpoint(&self.endpoint, call_batches)
            .await
    }

    async fn fetch_json_rpc_batch_results_bounded_at_endpoint(
        &self,
        endpoint: &Url,
        mut call_batches: Vec<Vec<JsonRpcBatchCall>>,
    ) -> Result<Vec<Vec<Option<Value>>>> {
        if call_batches.is_empty() {
            return Ok(Vec::new());
        }

        let concurrency = provider_batch_request_concurrency();
        if concurrency <= 1 || call_batches.len() == 1 {
            let mut batch_results = Vec::with_capacity(call_batches.len());
            for calls in call_batches {
                batch_results.push(
                    self.fetch_json_rpc_batch_results_at_endpoint(endpoint, calls)
                        .await?,
                );
            }
            return Ok(batch_results);
        }

        let mut results = (0..call_batches.len()).map(|_| None).collect::<Vec<_>>();
        let mut next_batch_index = 0usize;
        let mut tasks = JoinSet::new();
        let endpoint = endpoint.clone();

        while next_batch_index < call_batches.len() || !tasks.is_empty() {
            while next_batch_index < call_batches.len() && tasks.len() < concurrency {
                let provider = self.clone();
                let endpoint = endpoint.clone();
                let batch_index = next_batch_index;
                let calls = std::mem::take(&mut call_batches[batch_index]);
                tasks.spawn(async move {
                    provider
                        .fetch_json_rpc_batch_results_at_endpoint(&endpoint, calls)
                        .await
                        .map(|batch_results| (batch_index, batch_results))
                });
                next_batch_index += 1;
            }

            let joined = tasks
                .join_next()
                .await
                .context("JSON-RPC batch worker set ended unexpectedly")?;
            let (batch_index, batch_results) =
                joined.context("JSON-RPC batch worker panicked")??;
            results[batch_index] = Some(batch_results);
        }

        results
            .into_iter()
            .map(|result| result.context("JSON-RPC batch worker omitted a result"))
            .collect()
    }
}
