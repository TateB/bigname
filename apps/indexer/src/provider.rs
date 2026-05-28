use std::collections::{BTreeMap, BTreeSet};

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
const PROVIDER_BATCH_ITEM_LIMIT: usize = 32;
const MAX_TRANSACTION_RECEIPT_FALLBACK: usize = 128;
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
                ChainProvider::JsonRpc(JsonRpcProvider::new(&url)?),
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
}

impl JsonRpcProvider {
    pub fn new(endpoint: &str) -> Result<Self> {
        let endpoint = Url::parse(endpoint)
            .with_context(|| format!("failed to parse RPC endpoint {endpoint}"))?;
        if !matches!(endpoint.scheme(), "http" | "https") {
            bail!("unsupported RPC endpoint scheme for {endpoint}; expected http:// or https://");
        }

        let client = reqwest::Client::new();

        Ok(Self { endpoint, client })
    }
}

#[cfg(test)]
mod tests;
