use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use serde_json::Value;

use super::{
    JsonRpcProvider, PROVIDER_BATCH_ITEM_LIMIT, ProviderBlock, ProviderBlockBundle,
    ProviderBlockSelection, ProviderHeadHashSnapshot, ProviderHeadSnapshot, ProviderResolvedBlock,
    RAW_PAYLOAD_KIND_FULL_BLOCK,
    decode::{block_hash_from_value, normalize_hash},
    logs_receipts::ProviderBlockLogFetch,
    request::JsonRpcBatchCall,
};

mod cache_fill;

fn required_fetched_block(
    blocks: &BTreeMap<String, ProviderBlock>,
    block_hash: &str,
) -> Result<ProviderBlock> {
    blocks
        .get(block_hash)
        .cloned()
        .with_context(|| format!("provider did not return fetched block {block_hash}"))
}

fn validate_resolved_bundle_scope(
    resolved_block: &ProviderResolvedBlock,
    bundle: &ProviderBlockBundle,
) -> Result<()> {
    if bundle.block.block_hash != resolved_block.block_hash {
        bail!(
            "provider returned block {} for requested hash {}",
            bundle.block.block_hash,
            resolved_block.block_hash
        );
    }
    if bundle.block.block_number != resolved_block.block_number {
        bail!(
            "provider resolved block number {} to hash {}, but hash-scoped fetch returned block number {}",
            resolved_block.block_number,
            resolved_block.block_hash,
            bundle.block.block_number
        );
    }

    Ok(())
}

fn validate_bundle_transactions(bundle: &ProviderBlockBundle) -> Result<()> {
    for transaction in &bundle.transactions {
        if transaction.block_hash != bundle.block.block_hash {
            bail!(
                "provider returned transaction {} for block {} with mismatched block hash {}",
                transaction.transaction_hash,
                bundle.block.block_hash,
                transaction.block_hash
            );
        }
        if transaction.block_number != bundle.block.block_number {
            bail!(
                "provider returned transaction {} for block {} with mismatched block number {}",
                transaction.transaction_hash,
                bundle.block.block_hash,
                transaction.block_number
            );
        }
    }

    Ok(())
}

impl JsonRpcProvider {
    pub async fn fetch_chain_heads(&self) -> Result<ProviderHeadSnapshot> {
        let head_hashes = self.fetch_chain_head_hashes().await?;
        let blocks = self
            .fetch_blocks_by_hashes([
                Some(head_hashes.canonical.clone()),
                head_hashes.safe.clone(),
                head_hashes.finalized.clone(),
            ])
            .await?;

        Ok(ProviderHeadSnapshot {
            canonical: required_fetched_block(&blocks, &head_hashes.canonical)?,
            safe: head_hashes
                .safe
                .as_deref()
                .map(|block_hash| required_fetched_block(&blocks, block_hash))
                .transpose()?,
            finalized: head_hashes
                .finalized
                .as_deref()
                .map(|block_hash| required_fetched_block(&blocks, block_hash))
                .transpose()?,
        })
    }

    async fn fetch_chain_head_hashes(&self) -> Result<ProviderHeadHashSnapshot> {
        let tags = ["latest", "safe", "finalized"];
        let calls = tags
            .iter()
            .map(|tag| JsonRpcBatchCall {
                method: "eth_getBlockByNumber",
                params: vec![Value::String((*tag).to_owned()), Value::Bool(false)],
            })
            .collect::<Vec<_>>();
        let mut results = self.fetch_json_rpc_batch_results(calls).await?.into_iter();

        let canonical = head_hash_from_tag_result(
            "latest",
            results
                .next()
                .context("provider omitted latest head hash result")?,
        )?
        .context("provider did not return a latest block")?;
        let safe = head_hash_from_tag_result(
            "safe",
            results
                .next()
                .context("provider omitted safe head hash result")?,
        )?;
        let finalized = head_hash_from_tag_result(
            "finalized",
            results
                .next()
                .context("provider omitted finalized head hash result")?,
        )?;

        Ok(ProviderHeadHashSnapshot {
            canonical,
            safe,
            finalized,
        })
    }

    #[allow(dead_code, reason = "staged provider helper covered by tests")]
    pub async fn fetch_block_hash_by_number(&self, block_number: i64) -> Result<String> {
        let block_parameter = ProviderBlockSelection::Number(block_number).json_rpc_parameter()?;
        let block = self
            .fetch_block(
                "eth_getBlockByNumber",
                vec![block_parameter, Value::Bool(false)],
            )
            .await?
            .with_context(|| format!("provider did not return block number {block_number}"))?;

        if block.block_number != block_number {
            bail!(
                "provider returned block {} for requested number {} with mismatched block number {}",
                block.block_hash,
                block_number,
                block.block_number
            );
        }

        Ok(block.block_hash)
    }

    pub async fn fetch_block_hashes_by_numbers(
        &self,
        block_numbers: &[i64],
    ) -> Result<Vec<ProviderResolvedBlock>> {
        let mut resolved = Vec::with_capacity(block_numbers.len());

        for chunk in block_numbers.chunks(PROVIDER_BATCH_ITEM_LIMIT) {
            let calls = chunk
                .iter()
                .map(|block_number| {
                    Ok(JsonRpcBatchCall {
                        method: "eth_getBlockByNumber",
                        params: vec![
                            ProviderBlockSelection::Number(*block_number).json_rpc_parameter()?,
                            Value::Bool(false),
                        ],
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let results = self.fetch_json_rpc_batch_results(calls).await?;

            for (block_number, result) in chunk.iter().zip(results) {
                let block = result
                    .with_context(|| format!("provider did not return block number {block_number}"))
                    .and_then(ProviderBlock::from_value)?;
                if block.block_number != *block_number {
                    bail!(
                        "provider returned block {} for requested number {} with mismatched block number {}",
                        block.block_hash,
                        block_number,
                        block.block_number
                    );
                }
                resolved.push(ProviderResolvedBlock {
                    block_number: *block_number,
                    block_hash: block.block_hash,
                });
            }
        }

        Ok(resolved)
    }

    pub async fn fetch_block_by_hash(&self, block_hash: &str) -> Result<ProviderBlock> {
        let block_hash = normalize_hash(block_hash);
        let block = self
            .fetch_block(
                "eth_getBlockByHash",
                vec![Value::String(block_hash.clone()), Value::Bool(false)],
            )
            .await?
            .with_context(|| format!("provider did not return block {block_hash}"))?;

        if block.block_hash != block_hash {
            bail!(
                "provider returned block {} for requested hash {}",
                block.block_hash,
                block_hash
            );
        }

        Ok(block)
    }

    pub async fn fetch_block_headers_by_hashes(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlock>> {
        let mut blocks = Vec::with_capacity(resolved_blocks.len());

        for chunk in resolved_blocks.chunks(PROVIDER_BATCH_ITEM_LIMIT) {
            let calls = chunk
                .iter()
                .map(|resolved_block| JsonRpcBatchCall {
                    method: "eth_getBlockByHash",
                    params: vec![
                        Value::String(resolved_block.block_hash.clone()),
                        Value::Bool(false),
                    ],
                })
                .collect::<Vec<_>>();
            let results = self.fetch_json_rpc_batch_results(calls).await?;

            for (resolved_block, result) in chunk.iter().zip(results) {
                let block = result
                    .with_context(|| {
                        format!(
                            "provider did not return block {}",
                            resolved_block.block_hash
                        )
                    })
                    .and_then(ProviderBlock::from_value)?;
                if block.block_hash != resolved_block.block_hash {
                    bail!(
                        "provider returned block {} for requested hash {}",
                        block.block_hash,
                        resolved_block.block_hash
                    );
                }
                if block.block_number != resolved_block.block_number {
                    bail!(
                        "provider resolved block number {} to hash {}, but hash-scoped fetch returned block number {}",
                        resolved_block.block_number,
                        resolved_block.block_hash,
                        block.block_number
                    );
                }
                blocks.push(block);
            }
        }

        Ok(blocks)
    }

    pub async fn fetch_block_bundles_by_hashes(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlockBundle>> {
        let mut bundles = Vec::with_capacity(resolved_blocks.len());

        // Keep retained payload fetches single-response scoped: cache-fill verifies the stored
        // digest against the same full block/log/receipt JSON-RPC response body.
        for chunk in resolved_blocks.chunks(PROVIDER_BATCH_ITEM_LIMIT) {
            for resolved_block in chunk {
                let bundle = self
                    .fetch_block_bundle_by_hash(&resolved_block.block_hash)
                    .await?;
                if bundle.block.block_number != resolved_block.block_number {
                    bail!(
                        "provider resolved block number {} to hash {}, but hash-scoped fetch returned block number {}",
                        resolved_block.block_number,
                        resolved_block.block_hash,
                        bundle.block.block_number
                    );
                }
                bundles.push(bundle);
            }
        }

        Ok(bundles)
    }

    pub async fn fetch_block_bundles_without_logs_by_hashes(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlockBundle>> {
        let mut bundles = Vec::with_capacity(resolved_blocks.len());

        for chunk in resolved_blocks.chunks(PROVIDER_BATCH_ITEM_LIMIT) {
            let calls = chunk
                .iter()
                .map(|resolved_block| JsonRpcBatchCall {
                    method: "eth_getBlockByHash",
                    params: vec![
                        Value::String(resolved_block.block_hash.clone()),
                        Value::Bool(true),
                    ],
                })
                .collect::<Vec<_>>();
            let results = self.fetch_json_rpc_batch_results(calls).await?;
            let mut chunk_bundles = Vec::with_capacity(chunk.len());

            for (resolved_block, result) in chunk.iter().zip(results) {
                let block_value = result.with_context(|| {
                    format!(
                        "provider did not return block {}",
                        resolved_block.block_hash
                    )
                })?;
                let bundle = ProviderBlockBundle::from_value(block_value)?;
                validate_resolved_bundle_scope(resolved_block, &bundle)?;
                validate_bundle_transactions(&bundle)?;
                chunk_bundles.push(bundle);
            }

            self.fill_batched_block_receipts(&mut chunk_bundles).await?;
            bundles.extend(chunk_bundles);
        }

        Ok(bundles)
    }

    pub async fn fetch_block_bundle_by_hash(
        &self,
        block_hash: &str,
    ) -> Result<ProviderBlockBundle> {
        self.fetch_block_bundle_by_hash_with_log_fetch(block_hash, ProviderBlockLogFetch::Fetch)
            .await
    }

    async fn fetch_block_bundle_by_hash_with_log_fetch(
        &self,
        block_hash: &str,
        log_fetch: ProviderBlockLogFetch,
    ) -> Result<ProviderBlockBundle> {
        let block_hash = normalize_hash(block_hash);
        let block_payload = self
            .fetch_json_rpc_result_with_payload(
                "eth_getBlockByHash",
                vec![Value::String(block_hash.clone()), Value::Bool(true)],
            )
            .await?
            .with_cache_metadata(
                RAW_PAYLOAD_KIND_FULL_BLOCK,
                "eth_getBlockByHash",
                "block_hash",
            );
        let block_value = block_payload
            .result
            .with_context(|| format!("provider did not return block {block_hash}"))?;
        let mut bundle = ProviderBlockBundle::from_value(block_value)?;
        bundle.raw_payloads.push(block_payload.cache_metadata);

        if bundle.block.block_hash != block_hash {
            bail!(
                "provider returned block {} for requested hash {}",
                bundle.block.block_hash,
                block_hash
            );
        }

        for transaction in &bundle.transactions {
            if transaction.block_hash != block_hash {
                bail!(
                    "provider returned transaction {} for block {} with mismatched block hash {}",
                    transaction.transaction_hash,
                    block_hash,
                    transaction.block_hash
                );
            }
            if transaction.block_number != bundle.block.block_number {
                bail!(
                    "provider returned transaction {} for block {} with mismatched block number {}",
                    transaction.transaction_hash,
                    block_hash,
                    transaction.block_number
                );
            }
        }

        if log_fetch == ProviderBlockLogFetch::Fetch {
            let logs = self
                .fetch_logs_by_block_hash(&block_hash, bundle.block.block_number)
                .await?;
            bundle.raw_payloads.push(logs.cache_metadata);
            bundle.logs = logs.logs;
        }

        let receipts = self
            .fetch_receipts_by_block_hash(
                &block_hash,
                bundle.block.block_number,
                &bundle.transactions,
            )
            .await?;
        bundle.raw_payloads.extend(receipts.cache_metadata);
        bundle.receipts = receipts.receipts;

        Ok(bundle)
    }

    async fn fill_batched_block_receipts(&self, bundles: &mut [ProviderBlockBundle]) -> Result<()> {
        let calls = bundles
            .iter()
            .map(|bundle| JsonRpcBatchCall {
                method: "eth_getBlockReceipts",
                params: vec![Value::String(bundle.block.block_hash.clone())],
            })
            .collect::<Vec<_>>();

        match self.fetch_json_rpc_batch_results(calls).await {
            Ok(results) => {
                for (bundle, result) in bundles.iter_mut().zip(results) {
                    let receipts = result.with_context(|| {
                        format!(
                            "provider returned null receipts for exact block hash lookup {}",
                            bundle.block.block_hash
                        )
                    })?;
                    let receipts = receipts
                        .as_array()
                        .context("expected receipts array in JSON-RPC result")?
                        .iter()
                        .map(super::ProviderReceipt::from_value)
                        .collect::<Result<Vec<_>>>()?;
                    bundle.receipts = self.order_receipts_by_transaction_hash(
                        &bundle.block.block_hash,
                        bundle.block.block_number,
                        receipts,
                        &bundle.transactions,
                    )?;
                }
            }
            Err(batch_error) => {
                for bundle in bundles {
                    let receipts = self
                        .fetch_receipts_by_block_hash(
                            &bundle.block.block_hash,
                            bundle.block.block_number,
                            &bundle.transactions,
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "batched block-scoped receipt fetch failed ({batch_error}); individual receipt fetch for {} also failed",
                                bundle.block.block_hash
                            )
                        })?;
                    bundle.raw_payloads.extend(receipts.cache_metadata);
                    bundle.receipts = receipts.receipts;
                }
            }
        }

        Ok(())
    }

    async fn fetch_blocks_by_hashes<I>(&self, hashes: I) -> Result<BTreeMap<String, ProviderBlock>>
    where
        I: IntoIterator<Item = Option<String>>,
    {
        let mut blocks = BTreeMap::new();

        for block_hash in hashes.into_iter().flatten() {
            if blocks.contains_key(&block_hash) {
                continue;
            }

            blocks.insert(
                block_hash.clone(),
                self.fetch_block_by_hash(&block_hash).await?,
            );
        }

        Ok(blocks)
    }

    async fn fetch_block(&self, method: &str, params: Vec<Value>) -> Result<Option<ProviderBlock>> {
        self.fetch_json_rpc_result(method, params)
            .await?
            .map(ProviderBlock::from_value)
            .transpose()
    }
}

fn head_hash_from_tag_result(tag: &str, result: Option<Value>) -> Result<Option<String>> {
    result
        .map(|value| {
            block_hash_from_value(&value)
                .with_context(|| format!("failed to decode {tag} block hash from provider result"))
        })
        .transpose()
}
