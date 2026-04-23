use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use bigname_storage::{RawPayloadCacheDigestVerification, verify_raw_payload_cache_digest};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Uri};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use serde_json::{Value, json};
use sha3::{Digest, Keccak256};

const ZERO_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";
const PROVIDER_BATCH_ITEM_LIMIT: usize = 32;
const MAX_TRANSACTION_RECEIPT_FALLBACK: usize = 128;
pub(crate) const RAW_PAYLOAD_KIND_FULL_BLOCK: &str = "full_block";
pub(crate) const RAW_PAYLOAD_KIND_BLOCK_LOGS: &str = "block_logs";
pub(crate) const RAW_PAYLOAD_KIND_BLOCK_RECEIPTS: &str = "block_receipts";
pub(crate) const JSON_RPC_PAYLOAD_CONTENT_TYPE: &str = "application/json";
pub(crate) const JSON_RPC_PAYLOAD_CONTENT_ENCODING: &str = "identity";
const RAW_PAYLOAD_DIGEST_ALGORITHM: &str = "keccak256";

#[derive(Clone)]
pub struct ProviderRegistry {
    providers: BTreeMap<String, JsonRpcProvider>,
}

impl ProviderRegistry {
    pub fn from_chain_rpc_urls(entries: &[String]) -> Result<Self> {
        let mut providers = BTreeMap::new();

        for entry in entries {
            let (chain, url) = entry.split_once('=').with_context(|| {
                format!("invalid chain RPC entry {entry}; expected <chain>=<url>")
            })?;
            let chain = chain.trim();
            let url = url.trim();
            if chain.is_empty() || url.is_empty() {
                bail!("invalid chain RPC entry {entry}; expected non-empty <chain>=<url>");
            }
            if providers.contains_key(chain) {
                bail!("duplicate chain RPC configuration for {chain}");
            }

            providers.insert(chain.to_owned(), JsonRpcProvider::new(url)?);
        }

        Ok(Self { providers })
    }

    pub fn provider_for(&self, chain: &str) -> Option<&JsonRpcProvider> {
        self.providers.get(chain)
    }

    pub fn configured_chain_count(&self) -> usize {
        self.providers.len()
    }
}

#[derive(Clone)]
pub struct JsonRpcProvider {
    endpoint: Uri,
    client: Client<HttpConnector, Full<Bytes>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderBlockBundle {
    pub block: ProviderBlock,
    pub transactions: Vec<ProviderTransaction>,
    pub logs: Vec<ProviderLog>,
    pub receipts: Vec<ProviderReceipt>,
    pub raw_payloads: Vec<ProviderRawPayloadCacheMetadata>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderResolvedBlock {
    pub block_number: i64,
    pub block_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderBlockCodeObservationRequest {
    pub block_hash: String,
    pub addresses: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderBlockLogRequest {
    pub block_number: i64,
    pub block_hash: String,
    pub addresses: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderBlockCodeObservations {
    pub block_hash: String,
    pub observations: Vec<ProviderCodeObservation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderRawPayloadCacheMetadata {
    pub payload_kind: String,
    pub digest_algorithm: String,
    pub retained_digest: String,
    pub payload_size_bytes: i64,
    pub cache_metadata: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderTransaction {
    pub transaction_hash: String,
    pub block_hash: String,
    pub block_number: i64,
    pub transaction_index: i64,
    pub from: String,
    pub to: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderReceipt {
    pub transaction_hash: String,
    pub block_hash: String,
    pub block_number: i64,
    pub transaction_index: i64,
    pub contract_address: Option<String>,
    pub status: Option<i64>,
    pub cumulative_gas_used: Option<i64>,
    pub gas_used: Option<i64>,
    pub logs_bloom: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderLog {
    pub block_hash: String,
    pub block_number: i64,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub log_index: i64,
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderBlockTag {
    Latest,
    Safe,
    Finalized,
}

impl ProviderBlockTag {
    fn as_json_rpc_tag(self) -> &'static str {
        match self {
            Self::Latest => "latest",
            Self::Safe => "safe",
            Self::Finalized => "finalized",
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderBlockSelection {
    Number(i64),
    Hash(String),
    Tag(ProviderBlockTag),
}

impl ProviderBlockSelection {
    fn json_rpc_parameter(self) -> Result<Value> {
        match self {
            Self::Number(number) => {
                if number < 0 {
                    bail!("provider block selection number cannot be negative: {number}");
                }

                Ok(Value::String(format!("0x{number:x}")))
            }
            Self::Hash(block_hash) => {
                let block_hash = normalize_hash(&block_hash);
                if block_hash.is_empty() {
                    bail!("provider block selection hash cannot be empty");
                }

                Ok(json!({ "blockHash": block_hash }))
            }
            Self::Tag(tag) => Ok(Value::String(tag.as_json_rpc_tag().to_owned())),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderCodeObservation {
    pub address: String,
    pub code: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProviderHeadHashSnapshot {
    canonical: String,
    safe: Option<String>,
    finalized: Option<String>,
}

impl JsonRpcProvider {
    pub fn new(endpoint: &str) -> Result<Self> {
        let endpoint = endpoint
            .parse::<Uri>()
            .with_context(|| format!("failed to parse RPC endpoint {endpoint}"))?;
        if endpoint.scheme_str() != Some("http") {
            bail!(
                "unsupported RPC endpoint scheme for {endpoint}; bootstrap head fetch currently supports only http:// URLs"
            );
        }

        let connector = HttpConnector::new();
        let client = Client::builder(TokioExecutor::new()).build(connector);

        Ok(Self { endpoint, client })
    }

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
        let canonical = self
            .fetch_head_hash_by_tag("latest")
            .await?
            .context("provider did not return a latest block")?;
        let safe = self.fetch_head_hash_by_tag("safe").await?;
        let finalized = self.fetch_head_hash_by_tag("finalized").await?;

        Ok(ProviderHeadHashSnapshot {
            canonical,
            safe,
            finalized,
        })
    }

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
            for resolved_block in chunk {
                let bundle = self
                    .fetch_block_bundle_by_hash_with_log_fetch(
                        &resolved_block.block_hash,
                        ProviderBlockLogFetch::Skip,
                    )
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

    pub async fn fetch_logs_by_block_hashes(
        &self,
        requests: &[ProviderBlockLogRequest],
    ) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
        let mut logs_by_block_number = BTreeMap::<i64, Vec<ProviderLog>>::new();
        let requests = requests
            .iter()
            .map(|request| ProviderBlockLogRequest {
                block_number: request.block_number,
                block_hash: normalize_hash(&request.block_hash),
                addresses: request
                    .addresses
                    .iter()
                    .map(|address| normalize_address(address))
                    .collect(),
            })
            .collect::<Vec<_>>();

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

        for chunk in fetch_requests.chunks(PROVIDER_BATCH_ITEM_LIMIT) {
            let calls = chunk
                .iter()
                .map(|request| {
                    let mut filter = serde_json::Map::new();
                    filter.insert(
                        "blockHash".to_owned(),
                        Value::String(request.block_hash.clone()),
                    );
                    filter.insert(
                        "address".to_owned(),
                        Value::Array(
                            request
                                .addresses
                                .iter()
                                .map(|address| Value::String(address.clone()))
                                .collect(),
                        ),
                    );

                    JsonRpcBatchCall {
                        method: "eth_getLogs",
                        params: vec![Value::Object(filter)],
                    }
                })
                .collect::<Vec<_>>();
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

        let addresses = addresses
            .iter()
            .map(|address| normalize_address(address))
            .collect::<Vec<_>>();
        if addresses.is_empty() {
            return Ok(logs_by_block_number);
        }

        let from_block = resolved_blocks
            .first()
            .expect("resolved block range must be non-empty after validation")
            .block_number;
        let to_block = resolved_blocks
            .last()
            .expect("resolved block range must be non-empty after validation")
            .block_number;
        let mut filter = serde_json::Map::new();
        filter.insert(
            "fromBlock".to_owned(),
            ProviderBlockSelection::Number(from_block).json_rpc_parameter()?,
        );
        filter.insert(
            "toBlock".to_owned(),
            ProviderBlockSelection::Number(to_block).json_rpc_parameter()?,
        );
        filter.insert(
            "address".to_owned(),
            Value::Array(addresses.into_iter().map(Value::String).collect::<Vec<_>>()),
        );

        let logs = self
            .fetch_json_rpc_result("eth_getLogs", vec![Value::Object(filter)])
            .await?
            .context("provider returned null logs for block range lookup")?;
        let logs = logs
            .as_array()
            .context("expected logs array in JSON-RPC result")?;

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
            let expected_hash = normalize_hash(&expected.block_hash);
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

    pub async fn cache_fill_full_block_by_hash(
        &self,
        pool: &sqlx::PgPool,
        chain: &str,
        block_hash: &str,
        expected_block_number: i64,
    ) -> Result<ProviderBlock> {
        if expected_block_number < 0 {
            bail!("provider cache-fill expected block number cannot be negative");
        }

        let block_hash = normalize_hash(block_hash);
        let payload = self
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

        verify_raw_payload_cache_digest(
            pool,
            &RawPayloadCacheDigestVerification {
                chain_id: chain.to_owned(),
                block_hash: block_hash.clone(),
                payload_kind: RAW_PAYLOAD_KIND_FULL_BLOCK.to_owned(),
                digest_algorithm: payload.cache_metadata.digest_algorithm.clone(),
                candidate_digest: payload.cache_metadata.retained_digest.clone(),
                payload_size_bytes: payload.cache_metadata.payload_size_bytes,
            },
        )
        .await?;

        let block = ProviderBlock::from_value(
            payload
                .result
                .context("provider cache-fill returned null full block payload")?,
        )?;
        if block.block_hash != block_hash {
            bail!(
                "provider cache-fill returned block {} for requested hash {}",
                block.block_hash,
                block_hash
            );
        }
        if block.block_number != expected_block_number {
            bail!(
                "provider cache-fill returned block {} for requested hash {} with block number {}; expected {}",
                block.block_hash,
                block_hash,
                block.block_number,
                expected_block_number
            );
        }

        Ok(block)
    }

    pub async fn fetch_code_observations_at_block(
        &self,
        addresses: &[String],
        block: ProviderBlockSelection,
    ) -> Result<Vec<ProviderCodeObservation>> {
        let block_parameter = block.json_rpc_parameter()?;
        let mut cached_observations: BTreeMap<String, ProviderCodeObservation> = BTreeMap::new();
        let mut observations = Vec::with_capacity(addresses.len());

        for address in addresses {
            let address = normalize_address(address);
            if let Some(observation) = cached_observations.get(&address) {
                observations.push(observation.clone());
                continue;
            }

            let observation = ProviderCodeObservation {
                address: address.clone(),
                code: self
                    .fetch_code_for_address_at_block(&address, &block_parameter)
                    .await?,
            };
            cached_observations.insert(address, observation.clone());
            observations.push(observation);
        }

        Ok(observations)
    }

    pub async fn fetch_code_observations_at_block_hashes(
        &self,
        requests: &[ProviderBlockCodeObservationRequest],
    ) -> Result<Vec<ProviderBlockCodeObservations>> {
        let mut normalized_requests = Vec::with_capacity(requests.len());
        let mut seen_call_keys = BTreeMap::<(String, String), ()>::new();
        let mut call_keys = Vec::<(String, String, Value)>::new();

        for request in requests {
            let block_hash = normalize_hash(&request.block_hash);
            let block_parameter =
                ProviderBlockSelection::Hash(block_hash.clone()).json_rpc_parameter()?;
            let addresses = request
                .addresses
                .iter()
                .map(|address| normalize_address(address))
                .collect::<Vec<_>>();

            for address in &addresses {
                let key = (block_hash.clone(), address.clone());
                if seen_call_keys.insert(key.clone(), ()).is_none() {
                    call_keys.push((block_hash.clone(), address.clone(), block_parameter.clone()));
                }
            }

            normalized_requests.push((block_hash, addresses));
        }

        let mut code_by_key = BTreeMap::<(String, String), Vec<u8>>::new();
        for chunk in call_keys.chunks(PROVIDER_BATCH_ITEM_LIMIT) {
            let calls = chunk
                .iter()
                .map(|(_, address, block_parameter)| JsonRpcBatchCall {
                    method: "eth_getCode",
                    params: vec![Value::String(address.clone()), block_parameter.clone()],
                })
                .collect::<Vec<_>>();
            let results = self.fetch_json_rpc_batch_results(calls).await?;

            for ((block_hash, address, _), result) in chunk.iter().zip(results) {
                let code = result.with_context(|| {
                    format!(
                        "provider did not return code for address {address} at block hash {block_hash}"
                    )
                })?;
                let code = code
                    .as_str()
                    .context("expected code string in JSON-RPC result")?;
                code_by_key.insert(
                    (block_hash.clone(), address.clone()),
                    parse_hex_bytes(code)?,
                );
            }
        }

        let mut observations = Vec::with_capacity(normalized_requests.len());
        for (block_hash, addresses) in normalized_requests {
            let mut block_observations = Vec::with_capacity(addresses.len());
            for address in addresses {
                let code = code_by_key
                    .get(&(block_hash.clone(), address.clone()))
                    .with_context(|| {
                        format!(
                            "provider batch omitted code for address {address} at block hash {block_hash}"
                        )
                    })?
                    .clone();
                block_observations.push(ProviderCodeObservation { address, code });
            }

            observations.push(ProviderBlockCodeObservations {
                block_hash,
                observations: block_observations,
            });
        }

        Ok(observations)
    }

    async fn fetch_head_hash_by_tag(&self, tag: &str) -> Result<Option<String>> {
        self.fetch_json_rpc_result(
            "eth_getBlockByNumber",
            vec![Value::String(tag.to_owned()), Value::Bool(false)],
        )
        .await?
        .map(|value| block_hash_from_value(&value))
        .transpose()
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

    async fn fetch_logs_by_block_hash(
        &self,
        block_hash: &str,
        expected_block_number: i64,
    ) -> Result<ProviderLogsPayload> {
        let payload = self
            .fetch_json_rpc_result_with_payload(
                "eth_getLogs",
                vec![json!({
                    "blockHash": block_hash,
                })],
            )
            .await?
            .with_cache_metadata(RAW_PAYLOAD_KIND_BLOCK_LOGS, "eth_getLogs", "block_hash");
        let logs = payload
            .result
            .context("provider returned null logs for exact block hash lookup")?;
        let logs = logs
            .as_array()
            .context("expected logs array in JSON-RPC result")?;

        let logs = logs
            .iter()
            .map(|value| ProviderLog::from_value(value, block_hash, expected_block_number))
            .collect::<Result<Vec<_>>>()?;

        Ok(ProviderLogsPayload {
            logs,
            cache_metadata: payload.cache_metadata,
        })
    }

    async fn fetch_receipts_by_block_hash(
        &self,
        block_hash: &str,
        expected_block_number: i64,
        transactions: &[ProviderTransaction],
    ) -> Result<ProviderReceiptsPayload> {
        match self
            .fetch_block_receipts_by_block_hash(block_hash, expected_block_number, transactions)
            .await
        {
            Ok(receipts) => Ok(receipts),
            Err(scoped_error) => self
                .fetch_receipts_by_transaction_hashes(
                    block_hash,
                    expected_block_number,
                    transactions,
                )
                .await
                .with_context(|| {
                    format!("block-scoped receipt fetch for {block_hash} failed: {scoped_error}")
                }),
        }
    }

    async fn fetch_block_receipts_by_block_hash(
        &self,
        block_hash: &str,
        expected_block_number: i64,
        transactions: &[ProviderTransaction],
    ) -> Result<ProviderReceiptsPayload> {
        let payload = self
            .fetch_json_rpc_result_with_payload(
                "eth_getBlockReceipts",
                vec![Value::String(block_hash.to_owned())],
            )
            .await?
            .with_cache_metadata(
                RAW_PAYLOAD_KIND_BLOCK_RECEIPTS,
                "eth_getBlockReceipts",
                "block_hash",
            );
        let receipts = payload
            .result
            .context("provider returned null receipts for exact block hash lookup")?;
        let receipts = receipts
            .as_array()
            .context("expected receipts array in JSON-RPC result")?;
        let receipts = receipts
            .iter()
            .map(ProviderReceipt::from_value)
            .collect::<Result<Vec<_>>>()?;

        let receipts = self.order_receipts_by_transaction_hash(
            block_hash,
            expected_block_number,
            receipts,
            transactions,
        )?;

        Ok(ProviderReceiptsPayload {
            receipts,
            cache_metadata: vec![payload.cache_metadata],
        })
    }

    async fn fetch_receipts_by_transaction_hashes(
        &self,
        block_hash: &str,
        expected_block_number: i64,
        transactions: &[ProviderTransaction],
    ) -> Result<ProviderReceiptsPayload> {
        if transactions.len() > MAX_TRANSACTION_RECEIPT_FALLBACK {
            bail!(
                "refusing to fan out {} transaction receipts for block {}",
                transactions.len(),
                block_hash
            );
        }

        let mut receipts = Vec::with_capacity(transactions.len());
        for transaction in transactions {
            let receipt = self
                .fetch_json_rpc_result(
                    "eth_getTransactionReceipt",
                    vec![Value::String(transaction.transaction_hash.clone())],
                )
                .await?
                .with_context(|| {
                    format!(
                        "provider did not return receipt for transaction {}",
                        transaction.transaction_hash
                    )
                })?;
            let receipt = ProviderReceipt::from_value(&receipt)?;
            receipts.push(receipt);
        }

        let receipts = self.order_receipts_by_transaction_hash(
            block_hash,
            expected_block_number,
            receipts,
            transactions,
        )?;

        Ok(ProviderReceiptsPayload {
            receipts,
            cache_metadata: Vec::new(),
        })
    }

    fn order_receipts_by_transaction_hash(
        &self,
        block_hash: &str,
        expected_block_number: i64,
        receipts: Vec<ProviderReceipt>,
        transactions: &[ProviderTransaction],
    ) -> Result<Vec<ProviderReceipt>> {
        let mut receipts_by_hash = BTreeMap::new();
        for receipt in receipts {
            if receipt.block_hash != block_hash {
                bail!(
                    "provider returned receipt {} for block {} with mismatched block hash {}",
                    receipt.transaction_hash,
                    block_hash,
                    receipt.block_hash
                );
            }
            if receipt.block_number != expected_block_number {
                bail!(
                    "provider returned receipt {} for block {} with mismatched block number {}",
                    receipt.transaction_hash,
                    block_hash,
                    receipt.block_number
                );
            }

            if receipts_by_hash
                .insert(receipt.transaction_hash.clone(), receipt)
                .is_some()
            {
                bail!("provider returned duplicate receipt for block {block_hash}");
            }
        }

        let mut ordered = Vec::new();
        for transaction in transactions {
            let receipt = receipts_by_hash
                .remove(&transaction.transaction_hash)
                .with_context(|| {
                    format!(
                        "provider did not return receipt for transaction {} in block {}",
                        transaction.transaction_hash, block_hash
                    )
                })?;

            if receipt.block_hash != block_hash {
                bail!(
                    "provider returned receipt {} for block {} with mismatched block hash {}",
                    receipt.transaction_hash,
                    block_hash,
                    receipt.block_hash
                );
            }
            if receipt.block_number != expected_block_number {
                bail!(
                    "provider returned receipt {} for block {} with mismatched block number {}",
                    receipt.transaction_hash,
                    block_hash,
                    receipt.block_number
                );
            }

            ordered.push(receipt);
        }

        if !receipts_by_hash.is_empty() {
            bail!("provider returned extra receipts for block {block_hash}");
        }

        Ok(ordered)
    }

    async fn fetch_code_for_address_at_block(
        &self,
        address: &str,
        block_parameter: &Value,
    ) -> Result<Vec<u8>> {
        let code = self
            .fetch_json_rpc_result(
                "eth_getCode",
                vec![Value::String(address.to_owned()), block_parameter.clone()],
            )
            .await?
            .with_context(|| {
                format!(
                    "provider did not return code for address {address} at block {block_parameter}"
                )
            })?;
        let code = code
            .as_str()
            .context("expected code string in JSON-RPC result")?;

        parse_hex_bytes(code)
    }

    async fn fetch_json_rpc_result(
        &self,
        method: &str,
        params: Vec<Value>,
    ) -> Result<Option<Value>> {
        Ok(self
            .fetch_json_rpc_result_with_payload(method, params)
            .await?
            .result)
    }

    async fn fetch_json_rpc_result_with_payload(
        &self,
        method: &str,
        params: Vec<Value>,
    ) -> Result<JsonRpcResultPayload> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });
        let body = self.send_json_rpc_payload(method, payload).await?;

        let fingerprint = JsonRpcPayloadFingerprint::for_body(&body)?;
        let response = serde_json::from_slice::<JsonRpcResponse>(&body)
            .context("failed to decode JSON-RPC response")?;
        if let Some(error) = response.error {
            bail!(
                "provider returned JSON-RPC error {}: {}",
                error.code,
                error.message
            );
        }

        Ok(JsonRpcResultPayload {
            result: response.result,
            fingerprint,
        })
    }

    async fn fetch_json_rpc_batch_results(
        &self,
        calls: Vec<JsonRpcBatchCall>,
    ) -> Result<Vec<Option<Value>>> {
        if calls.is_empty() {
            return Ok(Vec::new());
        }

        match self.try_fetch_json_rpc_batch_results(&calls).await {
            Ok(results) => Ok(results),
            Err(batch_error) => {
                let mut results = Vec::with_capacity(calls.len());
                for call in calls {
                    let method = call.method;
                    let result = self
                        .fetch_json_rpc_result(method, call.params)
                        .await
                        .with_context(|| {
                            format!(
                                "provider JSON-RPC batch failed ({batch_error}); individual retry for {} also failed",
                                method
                            )
                        })?;
                    results.push(result);
                }
                Ok(results)
            }
        }
    }

    async fn try_fetch_json_rpc_batch_results(
        &self,
        calls: &[JsonRpcBatchCall],
    ) -> Result<Vec<Option<Value>>> {
        let payload = Value::Array(
            calls
                .iter()
                .enumerate()
                .map(|(index, call)| {
                    json!({
                        "jsonrpc": "2.0",
                        "id": index + 1,
                        "method": call.method,
                        "params": call.params.clone(),
                    })
                })
                .collect(),
        );
        let body = self.send_json_rpc_payload("batch", payload).await?;
        let response_value = serde_json::from_slice::<Value>(&body)
            .context("failed to decode JSON-RPC batch response")?;
        let response_values = response_value
            .as_array()
            .context("expected JSON-RPC batch response array")?;
        let expected_methods = calls
            .iter()
            .enumerate()
            .map(|(index, call)| ((index + 1) as i64, call.method))
            .collect::<BTreeMap<_, _>>();
        let mut results_by_id = BTreeMap::<i64, Option<Value>>::new();

        for response_value in response_values {
            let response = serde_json::from_value::<JsonRpcResponse>(response_value.clone())
                .context("failed to decode JSON-RPC batch response item")?;
            let id = response.response_id()?;
            let method = expected_methods
                .get(&id)
                .with_context(|| format!("provider returned unexpected JSON-RPC batch id {id}"))?;
            if let Some(error) = response.error {
                bail!(
                    "provider returned JSON-RPC error for batched {method} id {id}: {}: {}",
                    error.code,
                    error.message
                );
            }
            if results_by_id.insert(id, response.result).is_some() {
                bail!("provider returned duplicate JSON-RPC batch response id {id}");
            }
        }

        let mut results = Vec::with_capacity(calls.len());
        for id in 1..=calls.len() as i64 {
            results.push(
                results_by_id
                    .remove(&id)
                    .with_context(|| format!("provider omitted JSON-RPC batch response id {id}"))?,
            );
        }
        if !results_by_id.is_empty() {
            bail!("provider returned extra JSON-RPC batch responses");
        }

        Ok(results)
    }

    async fn send_json_rpc_payload(&self, request_context: &str, payload: Value) -> Result<Bytes> {
        let request = Request::post(self.endpoint.clone())
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(payload.to_string())))
            .context("failed to build JSON-RPC request")?;
        let response =
            self.client.request(request).await.with_context(|| {
                format!("failed to send JSON-RPC request for {request_context}")
            })?;
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .context("failed to read JSON-RPC response body")?
            .to_bytes();

        if !status.is_success() {
            let response_body = String::from_utf8_lossy(&body);
            bail!(
                "provider request for {request_context} failed with HTTP {status}: {response_body}"
            );
        }

        Ok(body)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct JsonRpcBatchCall {
    method: &'static str,
    params: Vec<Value>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProviderBlockLogFetch {
    Fetch,
    Skip,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProviderLogsPayload {
    logs: Vec<ProviderLog>,
    cache_metadata: ProviderRawPayloadCacheMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProviderReceiptsPayload {
    receipts: Vec<ProviderReceipt>,
    cache_metadata: Vec<ProviderRawPayloadCacheMetadata>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct JsonRpcResultPayload {
    result: Option<Value>,
    fingerprint: JsonRpcPayloadFingerprint,
}

impl JsonRpcResultPayload {
    fn with_cache_metadata(
        self,
        payload_kind: &str,
        method: &str,
        fetch_mode: &str,
    ) -> JsonRpcResultWithCacheMetadata {
        JsonRpcResultWithCacheMetadata {
            result: self.result,
            cache_metadata: self
                .fingerprint
                .cache_metadata(payload_kind, method, fetch_mode),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct JsonRpcResultWithCacheMetadata {
    result: Option<Value>,
    cache_metadata: ProviderRawPayloadCacheMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct JsonRpcPayloadFingerprint {
    digest_algorithm: String,
    retained_digest: String,
    payload_size_bytes: i64,
}

impl JsonRpcPayloadFingerprint {
    fn for_body(body: &[u8]) -> Result<Self> {
        let payload_size_bytes =
            i64::try_from(body.len()).context("JSON-RPC payload size does not fit in i64")?;

        Ok(Self {
            digest_algorithm: RAW_PAYLOAD_DIGEST_ALGORITHM.to_owned(),
            retained_digest: keccak256_hex(body),
            payload_size_bytes,
        })
    }

    fn cache_metadata(
        self,
        payload_kind: &str,
        method: &str,
        fetch_mode: &str,
    ) -> ProviderRawPayloadCacheMetadata {
        ProviderRawPayloadCacheMetadata {
            payload_kind: payload_kind.to_owned(),
            digest_algorithm: self.digest_algorithm,
            retained_digest: self.retained_digest,
            payload_size_bytes: self.payload_size_bytes,
            cache_metadata: json!({
                "source": "json-rpc",
                "method": method,
                "fetch_mode": fetch_mode,
                "digest_scope": "json_rpc_response_body",
            }),
        }
    }
}

fn required_fetched_block(
    blocks: &BTreeMap<String, ProviderBlock>,
    block_hash: &str,
) -> Result<ProviderBlock> {
    blocks
        .get(block_hash)
        .cloned()
        .with_context(|| format!("provider did not return fetched block {block_hash}"))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderHeadSnapshot {
    pub canonical: ProviderBlock,
    pub safe: Option<ProviderBlock>,
    pub finalized: Option<ProviderBlock>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderBlock {
    pub block_hash: String,
    pub parent_hash: Option<String>,
    pub block_number: i64,
    pub block_timestamp_unix_secs: i64,
    pub logs_bloom: Option<Vec<u8>>,
    pub transactions_root: Option<String>,
    pub receipts_root: Option<String>,
    pub state_root: Option<String>,
}

impl ProviderBlock {
    fn from_value(value: Value) -> Result<Self> {
        let block_hash = block_hash_from_value(&value)?;
        let object = value
            .as_object()
            .context("expected block object in JSON-RPC result")?;
        let parent_hash = normalize_parent_hash(
            object
                .get("parentHash")
                .and_then(Value::as_str)
                .context("missing parent hash in JSON-RPC result")?,
        );
        let block_number = parse_hex_i64(
            object
                .get("number")
                .and_then(Value::as_str)
                .context("missing block number in JSON-RPC result")?,
        )?;
        let block_timestamp_unix_secs = parse_hex_i64(
            object
                .get("timestamp")
                .and_then(Value::as_str)
                .context("missing block timestamp in JSON-RPC result")?,
        )?;

        Ok(Self {
            block_hash,
            parent_hash,
            block_number,
            block_timestamp_unix_secs,
            logs_bloom: object
                .get("logsBloom")
                .and_then(Value::as_str)
                .map(parse_hex_bytes)
                .transpose()?,
            transactions_root: object
                .get("transactionsRoot")
                .and_then(Value::as_str)
                .map(normalize_hash),
            receipts_root: object
                .get("receiptsRoot")
                .and_then(Value::as_str)
                .map(normalize_hash),
            state_root: object
                .get("stateRoot")
                .and_then(Value::as_str)
                .map(normalize_hash),
        })
    }
}

impl ProviderBlockBundle {
    fn from_value(value: Value) -> Result<Self> {
        let block = ProviderBlock::from_value(value.clone())?;
        let object = value
            .as_object()
            .context("expected block object in JSON-RPC result")?;
        let transactions = object
            .get("transactions")
            .and_then(Value::as_array)
            .context("missing transactions in JSON-RPC result")?
            .iter()
            .map(ProviderTransaction::from_value)
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            block,
            transactions,
            logs: Vec::new(),
            receipts: Vec::new(),
            raw_payloads: Vec::new(),
        })
    }
}

impl ProviderTransaction {
    fn from_value(value: &Value) -> Result<Self> {
        let object = value
            .as_object()
            .context("expected transaction object in JSON-RPC result")?;
        let transaction_hash = object
            .get("hash")
            .and_then(Value::as_str)
            .context("missing transaction hash in JSON-RPC result")?;
        let block_hash = object
            .get("blockHash")
            .and_then(Value::as_str)
            .context("missing transaction block hash in JSON-RPC result")?;
        let block_number = parse_hex_i64(
            object
                .get("blockNumber")
                .and_then(Value::as_str)
                .context("missing transaction block number in JSON-RPC result")?,
        )?;
        let transaction_index = parse_hex_i64(
            object
                .get("transactionIndex")
                .and_then(Value::as_str)
                .context("missing transaction index in JSON-RPC result")?,
        )?;
        let from = object
            .get("from")
            .and_then(Value::as_str)
            .context("missing transaction from address in JSON-RPC result")?;

        Ok(Self {
            transaction_hash: normalize_hash(transaction_hash),
            block_hash: normalize_hash(block_hash),
            block_number,
            transaction_index,
            from: normalize_address(from),
            to: object
                .get("to")
                .and_then(Value::as_str)
                .map(normalize_address),
        })
    }
}

impl ProviderReceipt {
    fn from_value(value: &Value) -> Result<Self> {
        let object = value
            .as_object()
            .context("expected receipt object in JSON-RPC result")?;
        let transaction_hash = object
            .get("transactionHash")
            .and_then(Value::as_str)
            .context("missing receipt transaction hash in JSON-RPC result")?;
        let block_hash = object
            .get("blockHash")
            .and_then(Value::as_str)
            .context("missing receipt block hash in JSON-RPC result")?;
        let block_number = parse_hex_i64(
            object
                .get("blockNumber")
                .and_then(Value::as_str)
                .context("missing receipt block number in JSON-RPC result")?,
        )?;
        let transaction_index = parse_hex_i64(
            object
                .get("transactionIndex")
                .and_then(Value::as_str)
                .context("missing receipt transaction index in JSON-RPC result")?,
        )?;

        Ok(Self {
            transaction_hash: normalize_hash(transaction_hash),
            block_hash: normalize_hash(block_hash),
            block_number,
            transaction_index,
            contract_address: object
                .get("contractAddress")
                .and_then(Value::as_str)
                .map(normalize_address),
            status: object
                .get("status")
                .and_then(Value::as_str)
                .map(parse_hex_i64)
                .transpose()?,
            cumulative_gas_used: object
                .get("cumulativeGasUsed")
                .and_then(Value::as_str)
                .map(parse_hex_i64)
                .transpose()?,
            gas_used: object
                .get("gasUsed")
                .and_then(Value::as_str)
                .map(parse_hex_i64)
                .transpose()?,
            logs_bloom: object
                .get("logsBloom")
                .and_then(Value::as_str)
                .map(parse_hex_bytes)
                .transpose()?,
        })
    }
}

impl ProviderLog {
    fn from_value(value: &Value, block_hash: &str, expected_block_number: i64) -> Result<Self> {
        let object = value
            .as_object()
            .context("expected log object in JSON-RPC result")?;
        let log_block_hash = object
            .get("blockHash")
            .and_then(Value::as_str)
            .context("missing log block hash in JSON-RPC result")?;
        let block_number = parse_hex_i64(
            object
                .get("blockNumber")
                .and_then(Value::as_str)
                .context("missing log block number in JSON-RPC result")?,
        )?;
        let transaction_hash = object
            .get("transactionHash")
            .and_then(Value::as_str)
            .context("missing log transaction hash in JSON-RPC result")?;
        let transaction_index = parse_hex_i64(
            object
                .get("transactionIndex")
                .and_then(Value::as_str)
                .context("missing log transaction index in JSON-RPC result")?,
        )?;
        let log_index = parse_hex_i64(
            object
                .get("logIndex")
                .and_then(Value::as_str)
                .context("missing log index in JSON-RPC result")?,
        )?;
        let address = object
            .get("address")
            .and_then(Value::as_str)
            .context("missing log address in JSON-RPC result")?;
        let topics = object
            .get("topics")
            .and_then(Value::as_array)
            .context("missing log topics in JSON-RPC result")?
            .iter()
            .map(|topic| {
                topic
                    .as_str()
                    .context("expected log topic string in JSON-RPC result")
                    .map(normalize_hash)
            })
            .collect::<Result<Vec<_>>>()?;
        let data = object
            .get("data")
            .and_then(Value::as_str)
            .context("missing log data in JSON-RPC result")?;

        if normalize_hash(log_block_hash) != block_hash {
            bail!(
                "provider returned log {} for block {} with mismatched block hash {}",
                log_index,
                block_hash,
                normalize_hash(log_block_hash)
            );
        }
        if block_number != expected_block_number {
            bail!(
                "provider returned log {} for block {} with mismatched block number {}",
                log_index,
                block_hash,
                block_number
            );
        }

        Ok(Self {
            block_hash: normalize_hash(log_block_hash),
            block_number,
            transaction_hash: normalize_hash(transaction_hash),
            transaction_index,
            log_index,
            address: normalize_address(address),
            topics,
            data: data.to_owned(),
        })
    }

    fn block_number_from_value(value: &Value) -> Result<i64> {
        let object = value
            .as_object()
            .context("expected log object in JSON-RPC result")?;

        parse_hex_i64(
            object
                .get("blockNumber")
                .and_then(Value::as_str)
                .context("missing log block number in JSON-RPC result")?,
        )
    }
}

fn block_hash_from_value(value: &Value) -> Result<String> {
    let object = value
        .as_object()
        .context("expected block object in JSON-RPC result")?;
    let block_hash = object
        .get("hash")
        .and_then(Value::as_str)
        .context("missing block hash in JSON-RPC result")?;

    Ok(normalize_hash(block_hash))
}

fn parse_hex_i64(value: &str) -> Result<i64> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    i64::from_str_radix(value, 16).with_context(|| format!("failed to parse hex integer {value}"))
}

fn parse_hex_bytes(value: &str) -> Result<Vec<u8>> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if !value.len().is_multiple_of(2) {
        bail!("invalid hex byte string with odd length");
    }

    let mut bytes = Vec::with_capacity(value.len() / 2);
    let chars = value.as_bytes();
    let mut index = 0;
    while index < chars.len() {
        let byte =
            std::str::from_utf8(&chars[index..index + 2]).context("invalid UTF-8 in hex string")?;
        bytes.push(
            u8::from_str_radix(byte, 16)
                .with_context(|| format!("failed to parse hex byte {byte}"))?,
        );
        index += 2;
    }
    Ok(bytes)
}

fn keccak256_hex(bytes: &[u8]) -> String {
    let mut hasher = Keccak256::new();
    hasher.update(bytes);
    hex_string(&hasher.finalize())
}

fn hex_string(bytes: &[u8]) -> String {
    let mut output = String::from("0x");
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

fn normalize_hash(value: &str) -> String {
    value.to_ascii_lowercase()
}

fn normalize_parent_hash(value: &str) -> Option<String> {
    let value = normalize_hash(value);
    if value == ZERO_HASH || value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn normalize_address(value: &str) -> String {
    value.to_ascii_lowercase()
}

#[derive(Debug)]
struct JsonRpcResponse {
    id: Option<Value>,
    result: Option<Value>,
    error: Option<JsonRpcError>,
}

impl JsonRpcResponse {
    fn response_id(&self) -> Result<i64> {
        self.id
            .as_ref()
            .and_then(Value::as_i64)
            .context("missing or non-integer JSON-RPC response id")
    }
}

impl<'de> serde::Deserialize<'de> for JsonRpcResponse {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct RawJsonRpcResponse {
            id: Option<Value>,
            result: Option<Value>,
            error: Option<JsonRpcError>,
        }

        let raw = RawJsonRpcResponse::deserialize(deserializer)?;
        Ok(Self {
            id: raw.id,
            result: raw.result,
            error: raw.error,
        })
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use anyhow::Result;
    use serde_json::Value;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        task::JoinHandle,
    };

    use super::*;

    #[test]
    fn provider_registry_parses_chain_rpc_urls() -> Result<()> {
        let registry = ProviderRegistry::from_chain_rpc_urls(&[
            "ethereum-mainnet=http://127.0.0.1:8545".to_owned(),
            "base-mainnet=http://127.0.0.1:9545".to_owned(),
        ])?;

        assert_eq!(registry.configured_chain_count(), 2);
        assert!(registry.provider_for("ethereum-mainnet").is_some());
        assert!(registry.provider_for("base-mainnet").is_some());
        assert!(registry.provider_for("optimism-mainnet").is_none());
        Ok(())
    }

    #[test]
    fn provider_block_selection_formats_json_rpc_parameters() -> Result<()> {
        assert_eq!(
            ProviderBlockSelection::Number(42).json_rpc_parameter()?,
            json!("0x2a")
        );
        assert_eq!(
            ProviderBlockSelection::Tag(ProviderBlockTag::Safe).json_rpc_parameter()?,
            json!("safe")
        );
        assert_eq!(
            ProviderBlockSelection::Hash(
                "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_owned()
            )
            .json_rpc_parameter()?,
            json!({
                "blockHash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            })
        );

        let error = ProviderBlockSelection::Number(-1)
            .json_rpc_parameter()
            .expect_err("negative block selections must fail");
        assert!(
            error
                .to_string()
                .contains("provider block selection number cannot be negative: -1")
        );

        Ok(())
    }

    #[tokio::test]
    async fn json_rpc_provider_resolves_block_numbers_to_hashes() -> Result<()> {
        let requested_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let requests = Arc::new(Mutex::new(Vec::new()));
        let request_log = Arc::clone(&requests);

        let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
            let method = body
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let params = body
                .get("params")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            request_log
                .lock()
                .expect("request log must not be poisoned")
                .push((method.to_owned(), params.clone()));

            let result = match method {
                "eth_getBlockByNumber" => {
                    assert_eq!(params.first().and_then(Value::as_str), Some("0x2a"));
                    assert_eq!(params.get(1), Some(&Value::Bool(false)));
                    rpc_block_payload(requested_hash, ZERO_HASH, 42, None)
                }
                _ => panic!("unexpected RPC request: {body}"),
            };

            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": result
            })
        }))
        .await?;
        let provider = JsonRpcProvider::new(&url)?;

        let block_hash = provider.fetch_block_hash_by_number(42).await?;
        assert_eq!(block_hash, requested_hash);

        let requests = requests
            .lock()
            .expect("request log must not be poisoned")
            .clone();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0, "eth_getBlockByNumber");

        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn json_rpc_provider_fetches_chain_heads_via_tag_hash_discovery() -> Result<()> {
        let canonical_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let canonical_parent = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let safe_hash = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        let safe_parent = "0x1111111111111111111111111111111111111111111111111111111111111111";
        let requests = Arc::new(Mutex::new(Vec::new()));
        let request_log = Arc::clone(&requests);

        let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
            let method = body
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let first_param = body
                .get("params")
                .and_then(Value::as_array)
                .and_then(|params| params.first())
                .and_then(Value::as_str)
                .unwrap_or_default();
            request_log
                .lock()
                .expect("request log must not be poisoned")
                .push((method.to_owned(), first_param.to_owned()));

            let result = match (method, first_param) {
                ("eth_getBlockByNumber", "latest") => json!({
                    "hash": canonical_hash.to_ascii_uppercase(),
                }),
                ("eth_getBlockByNumber", "safe") => json!({
                    "hash": safe_hash,
                }),
                ("eth_getBlockByNumber", "finalized") => json!({
                    "hash": safe_hash,
                }),
                ("eth_getBlockByHash", hash) if hash == canonical_hash => {
                    rpc_block_payload(canonical_hash, canonical_parent, 43, Some("0x0102"))
                }
                ("eth_getBlockByHash", hash) if hash == safe_hash => {
                    rpc_block_payload(safe_hash, safe_parent, 42, None)
                }
                _ => panic!("unexpected RPC request: {body}"),
            };

            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": result
            })
        }))
        .await?;
        let provider = JsonRpcProvider::new(&url)?;

        let heads = provider.fetch_chain_heads().await?;
        assert_eq!(heads.canonical.block_number, 43);
        assert_eq!(
            heads.canonical.parent_hash,
            Some(canonical_parent.to_owned())
        );
        assert_eq!(heads.canonical.logs_bloom, Some(vec![0x01, 0x02]));
        assert_eq!(
            heads.safe.as_ref().map(|block| block.block_number),
            Some(42)
        );
        assert_eq!(
            heads
                .finalized
                .as_ref()
                .map(|block| block.block_hash.as_str()),
            Some(safe_hash)
        );

        let requests = requests
            .lock()
            .expect("request log must not be poisoned")
            .clone();
        assert_eq!(
            requests,
            vec![
                ("eth_getBlockByNumber".to_owned(), "latest".to_owned()),
                ("eth_getBlockByNumber".to_owned(), "safe".to_owned()),
                ("eth_getBlockByNumber".to_owned(), "finalized".to_owned()),
                ("eth_getBlockByHash".to_owned(), canonical_hash.to_owned()),
                ("eth_getBlockByHash".to_owned(), safe_hash.to_owned()),
            ]
        );

        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn json_rpc_provider_rejects_mismatched_hash_payloads() -> Result<()> {
        let requested_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let returned_hash = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
            let method = body
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let first_param = body
                .get("params")
                .and_then(Value::as_array)
                .and_then(|params| params.first())
                .and_then(Value::as_str)
                .unwrap_or_default();

            let result = match (method, first_param) {
                ("eth_getBlockByHash", hash) if hash == requested_hash => {
                    rpc_block_payload(returned_hash, ZERO_HASH, 43, None)
                }
                _ => panic!("unexpected RPC request: {body}"),
            };

            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": result
            })
        }))
        .await?;
        let provider = JsonRpcProvider::new(&url)?;

        let error = provider
            .fetch_block_by_hash(&requested_hash.to_ascii_uppercase())
            .await
            .expect_err("mismatched hash payload must fail");
        assert!(
            error
                .to_string()
                .contains("provider returned block 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff for requested hash 0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );

        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn json_rpc_provider_fetches_code_observations_by_block_number() -> Result<()> {
        let contract_address = "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        let proxy_address = "0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB";
        let requests = Arc::new(Mutex::new(Vec::new()));
        let request_log = Arc::clone(&requests);

        let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
            let method = body
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let params = body
                .get("params")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let address = params
                .first()
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let block = params
                .get(1)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            request_log
                .lock()
                .expect("request log must not be poisoned")
                .push((method.to_owned(), address.clone(), block.clone()));

            let result = match (method, address.as_str(), block.as_str()) {
                ("eth_getCode", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "0x2a") => {
                    Value::String("0x6001600155".to_owned())
                }
                ("eth_getCode", "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "0x2a") => {
                    Value::String("0x".to_owned())
                }
                _ => panic!("unexpected RPC request: {body}"),
            };

            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": result
            })
        }))
        .await?;
        let provider = JsonRpcProvider::new(&url)?;

        let observations = provider
            .fetch_code_observations_at_block(
                &[
                    contract_address.to_owned(),
                    proxy_address.to_owned(),
                    contract_address.to_ascii_lowercase(),
                ],
                ProviderBlockSelection::Number(42),
            )
            .await?;

        assert_eq!(
            observations,
            vec![
                ProviderCodeObservation {
                    address: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                    code: vec![0x60, 0x01, 0x60, 0x01, 0x55],
                },
                ProviderCodeObservation {
                    address: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                    code: Vec::new(),
                },
                ProviderCodeObservation {
                    address: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                    code: vec![0x60, 0x01, 0x60, 0x01, 0x55],
                },
            ]
        );

        let requests = requests
            .lock()
            .expect("request log must not be poisoned")
            .clone();
        assert_eq!(
            requests,
            vec![
                (
                    "eth_getCode".to_owned(),
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                    "0x2a".to_owned(),
                ),
                (
                    "eth_getCode".to_owned(),
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                    "0x2a".to_owned(),
                ),
            ]
        );

        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn json_rpc_provider_fetches_code_observations_by_tag() -> Result<()> {
        let contract_address = "0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC";
        let requests = Arc::new(Mutex::new(Vec::new()));
        let request_log = Arc::clone(&requests);

        let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
            let method = body
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let params = body
                .get("params")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let address = params
                .first()
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let block = params
                .get(1)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            request_log
                .lock()
                .expect("request log must not be poisoned")
                .push((method.to_owned(), address.clone(), block.clone()));

            let result = match (method, address.as_str(), block.as_str()) {
                ("eth_getCode", "0xcccccccccccccccccccccccccccccccccccccccc", "finalized") => {
                    Value::String("0x600a600b".to_owned())
                }
                _ => panic!("unexpected RPC request: {body}"),
            };

            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": result
            })
        }))
        .await?;
        let provider = JsonRpcProvider::new(&url)?;

        let observations = provider
            .fetch_code_observations_at_block(
                &[contract_address.to_owned()],
                ProviderBlockSelection::Tag(ProviderBlockTag::Finalized),
            )
            .await?;
        assert_eq!(
            observations,
            vec![ProviderCodeObservation {
                address: "0xcccccccccccccccccccccccccccccccccccccccc".to_owned(),
                code: vec![0x60, 0x0a, 0x60, 0x0b],
            }]
        );

        let requests = requests
            .lock()
            .expect("request log must not be poisoned")
            .clone();
        assert_eq!(
            requests,
            vec![(
                "eth_getCode".to_owned(),
                "0xcccccccccccccccccccccccccccccccccccccccc".to_owned(),
                "finalized".to_owned(),
            )]
        );

        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn json_rpc_provider_retries_batched_code_items_after_item_error() -> Result<()> {
        let block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let address_one = "0x1111111111111111111111111111111111111111";
        let address_two = "0x2222222222222222222222222222222222222222";
        let requests = Arc::new(Mutex::new(Vec::new()));
        let request_log = Arc::clone(&requests);

        let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
            if let Some(batch) = body.as_array() {
                request_log
                    .lock()
                    .expect("request log must not be poisoned")
                    .push(("batch".to_owned(), batch.len(), Vec::new()));
                assert_eq!(batch.len(), 2);
                return json!([
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "error": {
                            "code": -32000,
                            "message": "temporary upstream item failure"
                        }
                    },
                    {
                        "jsonrpc": "2.0",
                        "id": 2,
                        "result": "0xffff"
                    }
                ]);
            }

            let method = body
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let params = body
                .get("params")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            request_log
                .lock()
                .expect("request log must not be poisoned")
                .push((method.to_owned(), 1, params.clone()));

            let result = match (method, params.first().and_then(Value::as_str)) {
                ("eth_getCode", Some(address)) if address == address_one => {
                    Value::String("0x6001".to_owned())
                }
                ("eth_getCode", Some(address)) if address == address_two => {
                    Value::String("0x6002".to_owned())
                }
                _ => panic!("unexpected RPC request: {body}"),
            };

            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": result
            })
        }))
        .await?;
        let provider = JsonRpcProvider::new(&url)?;

        let observations = provider
            .fetch_code_observations_at_block_hashes(&[ProviderBlockCodeObservationRequest {
                block_hash: block_hash.to_owned(),
                addresses: vec![address_one.to_owned(), address_two.to_owned()],
            }])
            .await?;

        assert_eq!(
            observations,
            vec![ProviderBlockCodeObservations {
                block_hash: block_hash.to_owned(),
                observations: vec![
                    ProviderCodeObservation {
                        address: address_one.to_owned(),
                        code: vec![0x60, 0x01],
                    },
                    ProviderCodeObservation {
                        address: address_two.to_owned(),
                        code: vec![0x60, 0x02],
                    },
                ],
            }]
        );

        let requests = requests
            .lock()
            .expect("request log must not be poisoned")
            .clone();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].0, "batch");
        assert_eq!(requests[0].1, 2);
        assert_eq!(requests[1].0, "eth_getCode");
        assert_eq!(
            requests[1].2.first().and_then(Value::as_str),
            Some(address_one)
        );
        assert_eq!(requests[2].0, "eth_getCode");
        assert_eq!(
            requests[2].2.first().and_then(Value::as_str),
            Some(address_two)
        );

        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn json_rpc_provider_fetches_logs_by_block_range() -> Result<()> {
        let block_hash_42 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let block_hash_43 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let address_one = "0x1111111111111111111111111111111111111111";
        let address_two = "0x2222222222222222222222222222222222222222";
        let requests = Arc::new(Mutex::new(Vec::new()));
        let request_log = Arc::clone(&requests);

        let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
            if let Some(response) =
                rpc_block_number_batch_response(&body, &[(42, block_hash_42), (43, block_hash_43)])
            {
                request_log
                    .lock()
                    .expect("request log must not be poisoned")
                    .push(body.clone());
                return response;
            }

            request_log
                .lock()
                .expect("request log must not be poisoned")
                .push(body.clone());
            assert_eq!(
                body.get("method").and_then(Value::as_str),
                Some("eth_getLogs")
            );
            let filter = body
                .get("params")
                .and_then(Value::as_array)
                .and_then(|params| params.first())
                .and_then(Value::as_object)
                .expect("range log request must include a filter object");
            assert_eq!(
                filter.get("fromBlock").and_then(Value::as_str),
                Some("0x2a")
            );
            assert_eq!(filter.get("toBlock").and_then(Value::as_str), Some("0x2b"));
            assert_eq!(
                filter.get("address").and_then(Value::as_array),
                Some(&vec![
                    Value::String(address_one.to_owned()),
                    Value::String(address_two.to_owned())
                ])
            );
            assert!(!filter.contains_key("blockHash"));

            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": [
                    rpc_log_payload(
                        "0x3333333333333333333333333333333333333333333333333333333333333333",
                        block_hash_43,
                        43,
                        1,
                        4,
                        address_two,
                        "0x4444444444444444444444444444444444444444444444444444444444444444",
                    ),
                    rpc_log_payload(
                        "0x5555555555555555555555555555555555555555555555555555555555555555",
                        block_hash_42,
                        42,
                        0,
                        2,
                        address_one,
                        "0x6666666666666666666666666666666666666666666666666666666666666666",
                    )
                ]
            })
        }))
        .await?;
        let provider = JsonRpcProvider::new(&url)?;
        let resolved_blocks = vec![
            ProviderResolvedBlock {
                block_number: 42,
                block_hash: block_hash_42.to_ascii_uppercase(),
            },
            ProviderResolvedBlock {
                block_number: 43,
                block_hash: block_hash_43.to_owned(),
            },
        ];
        let addresses = vec![address_one.to_owned(), address_two.to_ascii_uppercase()];

        let logs_by_block_number = provider
            .fetch_logs_by_block_range(&resolved_blocks, &addresses)
            .await?;

        assert_eq!(logs_by_block_number.len(), 2);
        assert_eq!(logs_by_block_number.get(&42).expect("block 42").len(), 1);
        assert_eq!(logs_by_block_number.get(&43).expect("block 43").len(), 1);
        assert_eq!(
            logs_by_block_number.get(&42).expect("block 42")[0].block_hash,
            block_hash_42
        );
        assert_eq!(
            logs_by_block_number.get(&43).expect("block 43")[0].address,
            address_two
        );
        let requests = requests
            .lock()
            .expect("request log must not be poisoned")
            .clone();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].get("method").and_then(Value::as_str),
            Some("eth_getLogs")
        );
        let revalidation_batch = requests[1]
            .as_array()
            .expect("post-range hash revalidation must be batched");
        assert_eq!(revalidation_batch.len(), 2);
        assert_eq!(
            revalidation_batch
                .iter()
                .map(|request| {
                    request
                        .get("params")
                        .and_then(Value::as_array)
                        .and_then(|params| params.first())
                        .and_then(Value::as_str)
                })
                .collect::<Vec<_>>(),
            vec![Some("0x2a"), Some("0x2b")]
        );

        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn json_rpc_provider_rejects_empty_range_when_post_range_block_hash_drifts() -> Result<()>
    {
        let block_hash_42 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let block_hash_43 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let drifted_hash = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let address = "0x1111111111111111111111111111111111111111";
        let requests = Arc::new(Mutex::new(Vec::new()));
        let request_log = Arc::clone(&requests);

        let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
            request_log
                .lock()
                .expect("request log must not be poisoned")
                .push(body.clone());

            if let Some(response) =
                rpc_block_number_batch_response(&body, &[(42, drifted_hash), (43, block_hash_43)])
            {
                return response;
            }

            assert_eq!(
                body.get("method").and_then(Value::as_str),
                Some("eth_getLogs")
            );
            let filter = body
                .get("params")
                .and_then(Value::as_array)
                .and_then(|params| params.first())
                .and_then(Value::as_object)
                .expect("range log request must include a filter object");
            assert_eq!(
                filter.get("fromBlock").and_then(Value::as_str),
                Some("0x2a")
            );
            assert_eq!(filter.get("toBlock").and_then(Value::as_str), Some("0x2b"));
            assert!(!filter.contains_key("blockHash"));

            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": []
            })
        }))
        .await?;
        let provider = JsonRpcProvider::new(&url)?;
        let resolved_blocks = vec![
            ProviderResolvedBlock {
                block_number: 42,
                block_hash: block_hash_42.to_owned(),
            },
            ProviderResolvedBlock {
                block_number: 43,
                block_hash: block_hash_43.to_owned(),
            },
        ];
        let addresses = vec![address.to_owned()];

        let error = provider
            .fetch_logs_by_block_range(&resolved_blocks, &addresses)
            .await
            .expect_err("empty drifted range must fail post-range block hash validation");

        assert!(
            error.to_string().contains(
                "provider block hash changed after range log lookup for block number 42: expected 0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, got 0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
            ),
            "unexpected error: {error:#}"
        );
        assert_eq!(
            requests
                .lock()
                .expect("request log must not be poisoned")
                .len(),
            2
        );

        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn json_rpc_provider_rejects_mismatched_range_log_block_hash() -> Result<()> {
        let block_hash_42 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let block_hash_43 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let wrong_hash = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let address = "0x1111111111111111111111111111111111111111";
        let requests = Arc::new(Mutex::new(Vec::new()));
        let request_log = Arc::clone(&requests);

        let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
            request_log
                .lock()
                .expect("request log must not be poisoned")
                .push(body.clone());
            assert_eq!(
                body.get("method").and_then(Value::as_str),
                Some("eth_getLogs")
            );
            let filter = body
                .get("params")
                .and_then(Value::as_array)
                .and_then(|params| params.first())
                .and_then(Value::as_object)
                .expect("range log request must include a filter object");
            assert_eq!(
                filter.get("fromBlock").and_then(Value::as_str),
                Some("0x2a")
            );
            assert_eq!(filter.get("toBlock").and_then(Value::as_str), Some("0x2b"));
            assert!(!filter.contains_key("blockHash"));

            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": [rpc_log_payload(
                    "0x3333333333333333333333333333333333333333333333333333333333333333",
                    wrong_hash,
                    43,
                    0,
                    0,
                    address,
                    "0x4444444444444444444444444444444444444444444444444444444444444444",
                )]
            })
        }))
        .await?;
        let provider = JsonRpcProvider::new(&url)?;
        let resolved_blocks = vec![
            ProviderResolvedBlock {
                block_number: 42,
                block_hash: block_hash_42.to_owned(),
            },
            ProviderResolvedBlock {
                block_number: 43,
                block_hash: block_hash_43.to_owned(),
            },
        ];
        let addresses = vec![address.to_owned()];

        let error = provider
            .fetch_logs_by_block_range(&resolved_blocks, &addresses)
            .await
            .expect_err("mismatched range log block hash must fail");

        assert!(
            error.to_string().contains(
                "provider returned log 0 for block 0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb with mismatched block hash 0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
            ),
            "unexpected error: {error:#}"
        );
        assert_eq!(
            requests
                .lock()
                .expect("request log must not be poisoned")
                .len(),
            1
        );

        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn json_rpc_provider_rejects_range_logs_for_unrequested_block_numbers() -> Result<()> {
        let block_hash_42 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let block_hash_43 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let block_hash_44 = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let address = "0x1111111111111111111111111111111111111111";

        let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
            assert_eq!(
                body.get("method").and_then(Value::as_str),
                Some("eth_getLogs")
            );

            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": [rpc_log_payload(
                    "0x3333333333333333333333333333333333333333333333333333333333333333",
                    block_hash_44,
                    44,
                    0,
                    0,
                    address,
                    "0x4444444444444444444444444444444444444444444444444444444444444444",
                )]
            })
        }))
        .await?;
        let provider = JsonRpcProvider::new(&url)?;
        let resolved_blocks = vec![
            ProviderResolvedBlock {
                block_number: 42,
                block_hash: block_hash_42.to_owned(),
            },
            ProviderResolvedBlock {
                block_number: 43,
                block_hash: block_hash_43.to_owned(),
            },
        ];
        let addresses = vec![address.to_owned()];

        let error = provider
            .fetch_logs_by_block_range(&resolved_blocks, &addresses)
            .await
            .expect_err("range logs from unrequested blocks must fail");

        assert!(
            error
                .to_string()
                .contains("provider returned log 0 for unrequested block number 44"),
            "unexpected error: {error:#}"
        );

        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn json_rpc_provider_rejects_invalid_log_range_requests() -> Result<()> {
        let provider = JsonRpcProvider::new("http://127.0.0.1:1")?;
        let block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let addresses = vec!["0x1111111111111111111111111111111111111111".to_owned()];

        let error = provider
            .fetch_logs_by_block_range(
                &[
                    ProviderResolvedBlock {
                        block_number: 42,
                        block_hash: block_hash.to_owned(),
                    },
                    ProviderResolvedBlock {
                        block_number: 42,
                        block_hash: block_hash.to_owned(),
                    },
                ],
                &addresses,
            )
            .await
            .expect_err("duplicate range block numbers must fail");
        assert!(
            error
                .to_string()
                .contains("provider log range requested duplicate block number 42"),
            "unexpected error: {error:#}"
        );

        let error = provider
            .fetch_logs_by_block_range(
                &[
                    ProviderResolvedBlock {
                        block_number: 42,
                        block_hash: block_hash.to_owned(),
                    },
                    ProviderResolvedBlock {
                        block_number: 44,
                        block_hash: block_hash.to_owned(),
                    },
                ],
                &addresses,
            )
            .await
            .expect_err("non-contiguous range block numbers must fail");
        assert!(
            error
                .to_string()
                .contains("provider log range requested non-contiguous block numbers"),
            "unexpected error: {error:#}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn json_rpc_provider_rejects_mismatched_batched_exact_log_block_hash() -> Result<()> {
        let block_hash_one = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let block_hash_two = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let wrong_hash = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let requests = Arc::new(Mutex::new(Vec::new()));
        let request_log = Arc::clone(&requests);

        let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
            let batch = body.as_array().expect("logs must be requested as a batch");
            request_log
                .lock()
                .expect("request log must not be poisoned")
                .push(batch.clone());
            assert_eq!(batch.len(), 2);
            for (request, (expected_hash, expected_address)) in batch.iter().zip([
                (block_hash_one, "0x1111111111111111111111111111111111111111"),
                (block_hash_two, "0x3333333333333333333333333333333333333333"),
            ]) {
                assert_eq!(
                    request.get("method").and_then(Value::as_str),
                    Some("eth_getLogs")
                );
                let filter = request
                    .get("params")
                    .and_then(Value::as_array)
                    .and_then(|params| params.first())
                    .and_then(Value::as_object)
                    .expect("batched log request must include a filter object");
                assert_eq!(
                    filter.get("blockHash").and_then(Value::as_str),
                    Some(expected_hash)
                );
                assert_eq!(
                    filter.get("address").and_then(Value::as_array),
                    Some(&vec![Value::String(expected_address.to_owned())])
                );
                assert!(!filter.contains_key("fromBlock"));
                assert!(!filter.contains_key("toBlock"));
            }

            json!([
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": [rpc_log_payload(
                        "0x1111111111111111111111111111111111111111111111111111111111111111",
                        block_hash_one,
                        42,
                        0,
                        0,
                        "0x1111111111111111111111111111111111111111",
                        "0x2222222222222222222222222222222222222222222222222222222222222222",
                    )]
                },
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "result": [rpc_log_payload(
                        "0x3333333333333333333333333333333333333333333333333333333333333333",
                        wrong_hash,
                        43,
                        0,
                        0,
                        "0x3333333333333333333333333333333333333333",
                        "0x4444444444444444444444444444444444444444444444444444444444444444",
                    )]
                }
            ])
        }))
        .await?;
        let provider = JsonRpcProvider::new(&url)?;

        let error = provider
            .fetch_logs_by_block_hashes(&[
                ProviderBlockLogRequest {
                    block_number: 42,
                    block_hash: block_hash_one.to_owned(),
                    addresses: vec!["0x1111111111111111111111111111111111111111".to_owned()],
                },
                ProviderBlockLogRequest {
                    block_number: 43,
                    block_hash: block_hash_two.to_owned(),
                    addresses: vec!["0x3333333333333333333333333333333333333333".to_owned()],
                },
            ])
            .await
            .expect_err("mismatched batched log block hash must fail");
        assert!(
            error.to_string().contains(
                "provider returned log 0 for block 0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb with mismatched block hash 0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
            ),
            "unexpected error: {error:#}"
        );
        assert_eq!(
            requests
                .lock()
                .expect("request log must not be poisoned")
                .len(),
            1
        );

        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn json_rpc_provider_rejects_invalid_code_payloads() -> Result<()> {
        let contract_address = "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
            let method = body
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let params = body
                .get("params")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();

            let result = match method {
                "eth_getCode"
                    if params.first().and_then(Value::as_str)
                        == Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") =>
                {
                    Value::String("0x123".to_owned())
                }
                _ => panic!("unexpected RPC request: {body}"),
            };

            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": result
            })
        }))
        .await?;
        let provider = JsonRpcProvider::new(&url)?;

        let error = provider
            .fetch_code_observations_at_block(
                &[contract_address.to_owned()],
                ProviderBlockSelection::Tag(ProviderBlockTag::Latest),
            )
            .await
            .expect_err("invalid code payload must fail");
        assert!(
            error
                .to_string()
                .contains("invalid hex byte string with odd length")
        );

        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn json_rpc_provider_fetches_exact_block_bundle_with_block_scoped_receipts() -> Result<()>
    {
        let requested_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let parent_hash = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let tx_hash_one = "0x1111111111111111111111111111111111111111111111111111111111111111";
        let tx_hash_two = "0x2222222222222222222222222222222222222222222222222222222222222222";
        let log_hash = "0x3333333333333333333333333333333333333333333333333333333333333333";
        let requests = Arc::new(Mutex::new(Vec::new()));
        let request_log = Arc::clone(&requests);

        let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
            let method = body
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            request_log
                .lock()
                .expect("request log must not be poisoned")
                .push(method.to_owned());

            let params = body
                .get("params")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let result = match method {
                "eth_getBlockByHash" => {
                    assert_eq!(params.get(1), Some(&Value::Bool(true)));
                    rpc_exact_block_payload(
                        requested_hash,
                        parent_hash,
                        43,
                        Some("0x0102"),
                        vec![
                            rpc_transaction_payload(
                                tx_hash_one,
                                requested_hash,
                                43,
                                0,
                                "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                                Some("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
                            ),
                            rpc_transaction_payload(
                                tx_hash_two,
                                requested_hash,
                                43,
                                1,
                                "0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
                                None,
                            ),
                        ],
                    )
                }
                "eth_getLogs" => {
                    let filter = params
                        .first()
                        .and_then(Value::as_object)
                        .expect("log filter must be an object");
                    assert_eq!(
                        filter.get("blockHash").and_then(Value::as_str),
                        Some(requested_hash)
                    );
                    Value::Array(vec![
                        rpc_log_payload(
                            log_hash,
                            requested_hash,
                            43,
                            0,
                            0,
                            "0xDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD",
                            tx_hash_one,
                        ),
                        rpc_log_payload(
                            "0x4444444444444444444444444444444444444444444444444444444444444444",
                            requested_hash,
                            43,
                            1,
                            1,
                            "0xEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE",
                            tx_hash_two,
                        ),
                    ])
                }
                "eth_getBlockReceipts" => Value::Array(vec![
                    rpc_receipt_payload(
                        tx_hash_two,
                        requested_hash,
                        43,
                        1,
                        Some("0x9999999999999999999999999999999999999999"),
                    ),
                    rpc_receipt_payload(
                        tx_hash_one,
                        requested_hash,
                        43,
                        0,
                        Some("0x8888888888888888888888888888888888888888"),
                    ),
                ]),
                _ => panic!("unexpected RPC request: {body}"),
            };

            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": result
            })
        }))
        .await?;
        let provider = JsonRpcProvider::new(&url)?;

        let bundle = provider
            .fetch_block_bundle_by_hash(&requested_hash.to_ascii_uppercase())
            .await?;

        assert_eq!(bundle.block.block_hash, requested_hash);
        assert_eq!(bundle.block.parent_hash, Some(parent_hash.to_owned()));
        assert_eq!(bundle.transactions.len(), 2);
        assert_eq!(bundle.transactions[0].transaction_hash, tx_hash_one);
        assert_eq!(
            bundle.transactions[0].from,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(
            bundle.transactions[0].to.as_deref(),
            Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        );
        assert_eq!(bundle.transactions[1].to, None);
        assert_eq!(bundle.logs.len(), 2);
        assert_eq!(
            bundle.logs[0].address,
            "0xdddddddddddddddddddddddddddddddddddddddd"
        );
        assert_eq!(bundle.logs[0].block_hash, requested_hash);
        assert_eq!(bundle.receipts.len(), 2);
        assert_eq!(bundle.receipts[0].transaction_hash, tx_hash_one);
        assert_eq!(
            bundle.receipts[0].contract_address.as_deref(),
            Some("0x8888888888888888888888888888888888888888")
        );
        assert_eq!(bundle.receipts[1].transaction_hash, tx_hash_two);
        assert_eq!(
            bundle.receipts[1].contract_address.as_deref(),
            Some("0x9999999999999999999999999999999999999999")
        );

        let requests = requests
            .lock()
            .expect("request log must not be poisoned")
            .clone();
        assert_eq!(
            requests,
            vec![
                "eth_getBlockByHash".to_owned(),
                "eth_getLogs".to_owned(),
                "eth_getBlockReceipts".to_owned(),
            ]
        );

        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn json_rpc_provider_fetches_exact_block_bundle_with_receipt_fallback() -> Result<()> {
        let requested_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let parent_hash = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let tx_hash_one = "0x1111111111111111111111111111111111111111111111111111111111111111";
        let tx_hash_two = "0x2222222222222222222222222222222222222222222222222222222222222222";
        let requests = Arc::new(Mutex::new(Vec::new()));
        let request_log = Arc::clone(&requests);

        let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
            let method = body
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            request_log
                .lock()
                .expect("request log must not be poisoned")
                .push(method.to_owned());

            let params = body
                .get("params")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            match method {
                "eth_getBlockByHash" => {
                    assert_eq!(params.get(1), Some(&Value::Bool(true)));
                    json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": rpc_exact_block_payload(
                            requested_hash,
                            parent_hash,
                            43,
                            None,
                            vec![
                                rpc_transaction_payload(
                                    tx_hash_one,
                                    requested_hash,
                                    43,
                                    0,
                                    "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                                    Some("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"),
                                ),
                                rpc_transaction_payload(
                                    tx_hash_two,
                                    requested_hash,
                                    43,
                                    1,
                                    "0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
                                    None,
                                ),
                            ],
                        )
                    })
                }
                "eth_getLogs" => {
                    let filter = params
                        .first()
                        .and_then(Value::as_object)
                        .expect("log filter must be an object");
                    assert_eq!(
                        filter.get("blockHash").and_then(Value::as_str),
                        Some(requested_hash)
                    );
                    json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": [rpc_log_payload(
                            "0x3333333333333333333333333333333333333333333333333333333333333333",
                            requested_hash,
                            43,
                            0,
                            0,
                            "0xDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD",
                            tx_hash_one,
                        )]
                    })
                }
                "eth_getBlockReceipts" => json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {
                        "code": -32601,
                        "message": "method not found"
                    }
                }),
                "eth_getTransactionReceipt"
                    if params.first().and_then(Value::as_str) == Some(tx_hash_one) =>
                {
                    json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": rpc_receipt_payload(
                            tx_hash_one,
                            requested_hash,
                            43,
                            0,
                            Some("0x8888888888888888888888888888888888888888"),
                        )
                    })
                }
                "eth_getTransactionReceipt"
                    if params.first().and_then(Value::as_str) == Some(tx_hash_two) =>
                {
                    json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": rpc_receipt_payload(
                            tx_hash_two,
                            requested_hash,
                            43,
                            1,
                            Some("0x9999999999999999999999999999999999999999"),
                        )
                    })
                }
                _ => panic!("unexpected RPC request: {body}"),
            }
        }))
        .await?;
        let provider = JsonRpcProvider::new(&url)?;

        let bundle = provider.fetch_block_bundle_by_hash(requested_hash).await?;
        assert_eq!(bundle.block.block_hash, requested_hash);
        assert_eq!(bundle.logs.len(), 1);
        assert_eq!(bundle.receipts.len(), 2);
        assert_eq!(bundle.receipts[1].transaction_hash, tx_hash_two);
        assert_eq!(
            bundle.receipts[0].contract_address.as_deref(),
            Some("0x8888888888888888888888888888888888888888")
        );

        let requests = requests
            .lock()
            .expect("request log must not be poisoned")
            .clone();
        assert_eq!(
            requests,
            vec![
                "eth_getBlockByHash".to_owned(),
                "eth_getLogs".to_owned(),
                "eth_getBlockReceipts".to_owned(),
                "eth_getTransactionReceipt".to_owned(),
                "eth_getTransactionReceipt".to_owned(),
            ]
        );

        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn json_rpc_provider_rejects_mismatched_bundle_transaction_hashes() -> Result<()> {
        let requested_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let returned_hash = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        let tx_hash = "0x1111111111111111111111111111111111111111111111111111111111111111";
        let requests = Arc::new(Mutex::new(Vec::new()));
        let request_log = Arc::clone(&requests);

        let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
            let method = body
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            request_log
                .lock()
                .expect("request log must not be poisoned")
                .push(method.to_owned());

            let result = match method {
                "eth_getBlockByHash" => rpc_exact_block_payload(
                    requested_hash,
                    ZERO_HASH,
                    43,
                    None,
                    vec![rpc_transaction_payload(
                        tx_hash,
                        returned_hash,
                        43,
                        0,
                        "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                        None,
                    )],
                ),
                _ => panic!("unexpected RPC request: {body}"),
            };

            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": result
            })
        }))
        .await?;
        let provider = JsonRpcProvider::new(&url)?;

        let error = provider
            .fetch_block_bundle_by_hash(&requested_hash.to_ascii_uppercase())
            .await
            .expect_err("mismatched transaction block hashes must fail");
        assert!(
            error
                .to_string()
                .contains("provider returned transaction 0x1111111111111111111111111111111111111111111111111111111111111111 for block 0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa with mismatched block hash 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
        );

        let requests = requests
            .lock()
            .expect("request log must not be poisoned")
            .clone();
        assert_eq!(requests, vec!["eth_getBlockByHash".to_owned()]);

        server.abort();
        Ok(())
    }

    fn rpc_block_payload(
        hash: &str,
        parent_hash: &str,
        block_number: i64,
        logs_bloom: Option<&str>,
    ) -> Value {
        let mut payload = json!({
            "hash": hash,
            "parentHash": parent_hash,
            "number": format!("0x{block_number:x}"),
            "timestamp": format!("0x{:x}", 0x65f2d150 + block_number),
        });
        if let Some(logs_bloom) = logs_bloom {
            payload["logsBloom"] = Value::String(logs_bloom.to_owned());
        }

        payload
    }

    fn rpc_exact_block_payload(
        hash: &str,
        parent_hash: &str,
        block_number: i64,
        logs_bloom: Option<&str>,
        transactions: Vec<Value>,
    ) -> Value {
        let mut payload = rpc_block_payload(hash, parent_hash, block_number, logs_bloom);
        payload["transactions"] = Value::Array(transactions);
        payload
    }

    fn rpc_transaction_payload(
        hash: &str,
        block_hash: &str,
        block_number: i64,
        transaction_index: i64,
        from: &str,
        to: Option<&str>,
    ) -> Value {
        json!({
            "hash": hash,
            "blockHash": block_hash,
            "blockNumber": format!("0x{block_number:x}"),
            "transactionIndex": format!("0x{transaction_index:x}"),
            "from": from,
            "to": to,
        })
    }

    fn rpc_receipt_payload(
        transaction_hash: &str,
        block_hash: &str,
        block_number: i64,
        transaction_index: i64,
        contract_address: Option<&str>,
    ) -> Value {
        json!({
            "transactionHash": transaction_hash,
            "blockHash": block_hash,
            "blockNumber": format!("0x{block_number:x}"),
            "transactionIndex": format!("0x{transaction_index:x}"),
            "contractAddress": contract_address,
            "status": "0x1",
            "cumulativeGasUsed": "0x5208",
            "gasUsed": "0x5208",
            "logsBloom": "0x0102",
        })
    }

    fn rpc_log_payload(
        log_hash: &str,
        block_hash: &str,
        block_number: i64,
        transaction_index: i64,
        log_index: i64,
        address: &str,
        transaction_hash: &str,
    ) -> Value {
        json!({
            "address": address,
            "blockHash": block_hash,
            "blockNumber": format!("0x{block_number:x}"),
            "data": "0xdeadbeef",
            "logIndex": format!("0x{log_index:x}"),
            "removed": false,
            "topics": [log_hash],
            "transactionHash": transaction_hash,
            "transactionIndex": format!("0x{transaction_index:x}"),
        })
    }

    fn rpc_block_number_batch_response(body: &Value, blocks: &[(i64, &str)]) -> Option<Value> {
        let batch = body.as_array()?;
        Some(Value::Array(
            batch
                .iter()
                .map(|request| {
                    assert_eq!(
                        request.get("method").and_then(Value::as_str),
                        Some("eth_getBlockByNumber")
                    );
                    let params = request
                        .get("params")
                        .and_then(Value::as_array)
                        .expect("block-number request must include params");
                    assert_eq!(params.get(1), Some(&Value::Bool(false)));
                    let block_number = parse_hex_i64(
                        params
                            .first()
                            .and_then(Value::as_str)
                            .expect("block-number request must include a number"),
                    )
                    .expect("block-number request must include valid hex");
                    let block_hash = blocks
                        .iter()
                        .find_map(|(candidate_number, candidate_hash)| {
                            (*candidate_number == block_number).then_some(*candidate_hash)
                        })
                        .unwrap_or_else(|| {
                            panic!("unexpected block-number revalidation request: {request}")
                        });

                    json!({
                        "jsonrpc": "2.0",
                        "id": request.get("id").cloned().unwrap_or(Value::Null),
                        "result": rpc_block_payload(block_hash, ZERO_HASH, block_number, None),
                    })
                })
                .collect(),
        ))
    }

    async fn spawn_json_rpc_server(
        handler: Arc<dyn Fn(Value) -> Value + Send + Sync>,
    ) -> Result<(String, JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("failed to bind JSON-RPC test server")?;
        let address = listener
            .local_addr()
            .context("failed to read JSON-RPC test server address")?;
        let url = format!("http://{address}");

        let server = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let handler = Arc::clone(&handler);
                tokio::spawn(async move {
                    let mut buffer = Vec::new();
                    let mut header_end = None;
                    let mut content_length = 0usize;

                    loop {
                        let mut chunk = [0_u8; 4096];
                        let Ok(read) = stream.read(&mut chunk).await else {
                            return;
                        };
                        if read == 0 {
                            return;
                        }
                        buffer.extend_from_slice(&chunk[..read]);

                        if header_end.is_none()
                            && let Some(index) = find_header_end(&buffer)
                        {
                            header_end = Some(index);
                            content_length = parse_content_length(&buffer[..index]).unwrap_or(0);
                        }

                        if let Some(index) = header_end
                            && buffer.len() >= index + 4 + content_length
                        {
                            let body = &buffer[index + 4..index + 4 + content_length];
                            let request_body = serde_json::from_slice::<Value>(body).unwrap();
                            let response_body = handler(request_body).to_string();
                            let response = format!(
                                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                                response_body.len(),
                                response_body
                            );
                            let _ = stream.write_all(response.as_bytes()).await;
                            let _ = stream.shutdown().await;
                            return;
                        }
                    }
                });
            }
        });

        Ok((url, server))
    }

    fn find_header_end(buffer: &[u8]) -> Option<usize> {
        buffer.windows(4).position(|window| window == b"\r\n\r\n")
    }

    fn parse_content_length(headers: &[u8]) -> Option<usize> {
        let headers = std::str::from_utf8(headers).ok()?;
        headers.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("content-length") {
                value.trim().parse().ok()
            } else {
                None
            }
        })
    }
}
