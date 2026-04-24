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

#[allow(dead_code, reason = "staged for exact block log fetch callers")]
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

    #[allow(dead_code, reason = "staged provider helper covered by tests")]
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

    #[allow(dead_code, reason = "staged cache-fill helper covered by tests")]
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
mod tests;
