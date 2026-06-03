use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use reqwest::Url;
#[cfg(test)]
use serde_json::json;

#[cfg(test)]
use decode::parse_hex_i64;

mod block_transaction;
mod code;
mod decode;
mod logs_receipts;
mod ops;
mod payload_cache;
mod request;
mod reth_db;
mod transaction_receipts;
mod types;

pub use reth_db::RethDbProvider;
#[allow(unused_imports)]
pub use types::ProviderBlockTag;
use types::ProviderHeadHashSnapshot;
pub use types::{
    ProviderBlock, ProviderBlockBundle, ProviderBlockCodeObservationRequest,
    ProviderBlockCodeObservations, ProviderBlockLogRequest, ProviderBlockSelection,
    ProviderCodeObservation, ProviderHeadSnapshot, ProviderLog, ProviderRawPayloadCacheMetadata,
    ProviderReceipt, ProviderResolvedBlock, ProviderTransaction, ProviderTransactionReceiptBundle,
    ProviderTransactionReceiptRequest,
};

const ZERO_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";
const DEFAULT_PROVIDER_BATCH_ITEM_LIMIT: usize = 32;
const MAX_PROVIDER_BATCH_ITEM_LIMIT: usize = 256;
const PROVIDER_BATCH_ITEM_LIMIT_ENV: &str = "BIGNAME_INDEXER_JSON_RPC_BATCH_ITEM_LIMIT";
const DEFAULT_PROVIDER_BATCH_REQUEST_CONCURRENCY: usize = 1;
const MAX_PROVIDER_BATCH_REQUEST_CONCURRENCY: usize = 16;
const PROVIDER_BATCH_REQUEST_CONCURRENCY_ENV: &str = "BIGNAME_INDEXER_JSON_RPC_BATCH_CONCURRENCY";
const PROVIDER_RECEIPT_FALLBACK_URLS_ENV: &str = "BIGNAME_INDEXER_CHAIN_RPC_RECEIPT_FALLBACK_URLS";
const MAX_TRANSACTION_RECEIPT_FALLBACK: usize = 128;
const JSON_RPC_PROVIDER_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const JSON_RPC_PROVIDER_REQUEST_TIMEOUT: Duration = Duration::from_secs(45);
pub(crate) const RAW_PAYLOAD_KIND_FULL_BLOCK: &str = "full_block";
pub(crate) const RAW_PAYLOAD_KIND_BLOCK_LOGS: &str = "block_logs";
pub(crate) const RAW_PAYLOAD_KIND_BLOCK_RECEIPTS: &str = "block_receipts";
pub(crate) const JSON_RPC_PAYLOAD_CONTENT_TYPE: &str = "application/json";
pub(crate) const JSON_RPC_PAYLOAD_CONTENT_ENCODING: &str = "identity";

#[derive(Clone)]
pub struct ProviderRegistry {
    providers: BTreeMap<String, ChainProvider>,
}

impl ProviderRegistry {
    #[cfg(test)]
    pub fn from_chain_rpc_urls(entries: &[String]) -> Result<Self> {
        Self::from_sources(entries, &[])
    }

    pub fn from_sources(rpc_entries: &[String], reth_db_entries: &[String]) -> Result<Self> {
        let mut providers = BTreeMap::new();

        for entry in rpc_entries {
            let (chain, url) = parse_chain_source_entry(entry, "chain RPC", "<chain>=<url>")?;
            insert_provider(
                &mut providers,
                &chain,
                ChainProvider::JsonRpc(JsonRpcProvider::new_for_chain(&chain, &url)?),
            )?;
        }

        for entry in reth_db_entries {
            let (chain, datadir) =
                parse_chain_source_entry(entry, "chain Reth DB", "<chain>=<datadir>")?;
            if providers.contains_key(&chain) {
                bail!("duplicate provider source configuration for {chain}");
            }
            let provider = RethDbProvider::new(&chain, &datadir)?;
            providers.insert(chain, ChainProvider::RethDb(provider));
        }

        Ok(Self { providers })
    }

    pub fn provider_for(&self, chain: &str) -> Option<&ChainProvider> {
        self.providers.get(chain)
    }

    pub fn configured_chain_count(&self) -> usize {
        self.providers.len()
    }

    pub fn configured_chain_count_by_kind(&self, kind: ChainProviderKind) -> usize {
        self.providers
            .values()
            .filter(|provider| provider.kind() == kind)
            .count()
    }

    pub fn ensure_configured_chains_admitted<'a>(
        &self,
        admitted_chains: impl IntoIterator<Item = &'a str>,
    ) -> Result<()> {
        let admitted_chains = admitted_chains.into_iter().collect::<BTreeSet<_>>();
        let invalid_chains = self
            .providers
            .keys()
            .filter(|chain| !admitted_chains.contains(chain.as_str()))
            .map(String::as_str)
            .collect::<Vec<_>>();
        if invalid_chains.is_empty() {
            return Ok(());
        }

        let admitted_chain_list = if admitted_chains.is_empty() {
            "<none>".to_owned()
        } else {
            admitted_chains
                .iter()
                .copied()
                .collect::<Vec<_>>()
                .join(", ")
        };
        bail!(
            "configured provider source chains outside selected/admitted runtime chain set: {}; admitted runtime chains: {admitted_chain_list}",
            invalid_chains.join(", ")
        );
    }
}

fn parse_chain_source_entry(
    entry: &str,
    source_label: &str,
    expected_shape: &str,
) -> Result<(String, String)> {
    let (chain, value) = entry.split_once('=').with_context(|| {
        format!("invalid {source_label} source entry {entry}; expected {expected_shape}")
    })?;
    let chain = chain.trim();
    let value = value.trim();
    if chain.is_empty() || value.is_empty() {
        bail!("invalid {source_label} source entry {entry}; expected non-empty {expected_shape}");
    }

    Ok((chain.to_owned(), value.to_owned()))
}

fn insert_provider(
    providers: &mut BTreeMap<String, ChainProvider>,
    chain: &str,
    provider: ChainProvider,
) -> Result<()> {
    if providers.contains_key(chain) {
        bail!("duplicate provider source configuration for {chain}");
    }

    providers.insert(chain.to_owned(), provider);
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChainProviderKind {
    JsonRpc,
    RethDb,
}

#[derive(Clone)]
pub enum ChainProvider {
    JsonRpc(JsonRpcProvider),
    RethDb(RethDbProvider),
}

impl ChainProvider {
    pub fn kind(&self) -> ChainProviderKind {
        match self {
            Self::JsonRpc(_) => ChainProviderKind::JsonRpc,
            Self::RethDb(_) => ChainProviderKind::RethDb,
        }
    }
}

#[allow(async_fn_in_trait)]
pub(crate) trait ChainProviderOps {
    async fn fetch_chain_heads(&self) -> Result<ProviderHeadSnapshot>;

    async fn fetch_block_hashes_by_numbers(
        &self,
        block_numbers: &[i64],
    ) -> Result<Vec<ProviderResolvedBlock>>;

    async fn fetch_block_by_hash(&self, block_hash: &str) -> Result<ProviderBlock>;

    async fn fetch_block_headers_by_hashes(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlock>>;

    async fn fetch_block_bundles_by_hashes(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlockBundle>>;

    async fn fetch_block_bundles_without_logs_by_hashes(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlockBundle>>;

    async fn fetch_block_bundle_by_hash(&self, block_hash: &str) -> Result<ProviderBlockBundle>;

    async fn fetch_logs_by_block_range(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
        addresses: &[String],
    ) -> Result<BTreeMap<i64, Vec<ProviderLog>>>;

    async fn fetch_logs_by_block_range_for_topic0s_and_addresses(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
        topic0s: &[String],
        addresses: &[String],
    ) -> Result<BTreeMap<i64, Vec<ProviderLog>>>;

    async fn fetch_transaction_receipt_pairs_by_hashes(
        &self,
        requests: &[ProviderTransactionReceiptRequest],
    ) -> Result<Vec<ProviderTransactionReceiptBundle>>;

    async fn fetch_code_observations_at_block(
        &self,
        addresses: &[String],
        block: ProviderBlockSelection,
    ) -> Result<Vec<ProviderCodeObservation>>;

    async fn fetch_code_observations_at_block_hashes(
        &self,
        requests: &[ProviderBlockCodeObservationRequest],
    ) -> Result<Vec<ProviderBlockCodeObservations>>;
}

#[derive(Clone)]
pub struct JsonRpcProvider {
    endpoint: Url,
    client: reqwest::Client,
    receipt_fallback_endpoint: Option<Url>,
}

impl JsonRpcProvider {
    #[allow(dead_code)]
    pub fn new(endpoint: &str) -> Result<Self> {
        Self::new_with_receipt_fallback(endpoint, None)
    }

    pub fn new_for_chain(chain: &str, endpoint: &str) -> Result<Self> {
        Self::new_with_receipt_fallback(endpoint, receipt_fallback_endpoint_for_chain(chain)?)
    }

    fn new_with_receipt_fallback(
        endpoint: &str,
        receipt_fallback_endpoint: Option<Url>,
    ) -> Result<Self> {
        let endpoint = Url::parse(endpoint)
            .with_context(|| format!("failed to parse RPC endpoint {endpoint}"))?;
        if !matches!(endpoint.scheme(), "http" | "https") {
            bail!("unsupported RPC endpoint scheme for {endpoint}; expected http:// or https://");
        }

        let client = reqwest::Client::builder()
            .connect_timeout(JSON_RPC_PROVIDER_CONNECT_TIMEOUT)
            .timeout(JSON_RPC_PROVIDER_REQUEST_TIMEOUT)
            .build()
            .context("failed to build JSON-RPC HTTP client")?;

        Ok(Self {
            endpoint,
            client,
            receipt_fallback_endpoint,
        })
    }
}

fn receipt_fallback_endpoint_for_chain(chain: &str) -> Result<Option<Url>> {
    let Some(raw_entries) = std::env::var(PROVIDER_RECEIPT_FALLBACK_URLS_ENV).ok() else {
        return Ok(None);
    };
    let mut fallback_endpoint = None;
    for entry in raw_entries
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let (entry_chain, value) =
            parse_chain_source_entry(entry, "chain RPC receipt fallback", "<chain>=<url>")?;
        if entry_chain != chain {
            continue;
        }
        if fallback_endpoint.is_some() {
            bail!("duplicate receipt fallback provider source configuration for {chain}");
        }
        let parsed = Url::parse(&value)
            .with_context(|| format!("failed to parse receipt fallback RPC endpoint {value}"))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            bail!(
                "unsupported receipt fallback RPC endpoint scheme for {value}; expected http:// or https://"
            );
        }
        fallback_endpoint = Some(parsed);
    }

    Ok(fallback_endpoint)
}

pub(super) fn provider_batch_item_limit() -> usize {
    parse_provider_batch_item_limit(std::env::var(PROVIDER_BATCH_ITEM_LIMIT_ENV).ok().as_deref())
}

pub(super) fn provider_batch_request_concurrency() -> usize {
    parse_provider_batch_request_concurrency(
        std::env::var(PROVIDER_BATCH_REQUEST_CONCURRENCY_ENV)
            .ok()
            .as_deref(),
    )
}

fn parse_provider_batch_item_limit(value: Option<&str>) -> usize {
    value
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .map(|value| value.min(MAX_PROVIDER_BATCH_ITEM_LIMIT))
        .unwrap_or(DEFAULT_PROVIDER_BATCH_ITEM_LIMIT)
}

fn parse_provider_batch_request_concurrency(value: Option<&str>) -> usize {
    value
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .map(|value| value.min(MAX_PROVIDER_BATCH_REQUEST_CONCURRENCY))
        .unwrap_or(DEFAULT_PROVIDER_BATCH_REQUEST_CONCURRENCY)
}

#[cfg(test)]
mod tests;
