use std::{collections::BTreeMap, fmt};

use anyhow::{Result, bail};

use super::super::{
    ProviderBlock, ProviderBlockBundle, ProviderBlockCodeObservationRequest,
    ProviderBlockCodeObservations, ProviderBlockSelection, ProviderCodeObservation,
    ProviderHeadSnapshot, ProviderLog, ProviderResolvedBlock, ProviderTransactionReceiptBundle,
    ProviderTransactionReceiptRequest,
};

const RETH_DB_FEATURE_ERROR: &str = "Reth DB provider support is not compiled into this \
    bigname-indexer binary; rebuild with `cargo build -p bigname-indexer --features reth-db` \
    or use a Docker image built with the bigname-indexer/reth-db feature before setting \
    --chain-reth-db-source or BIGNAME_INDEXER_CHAIN_RETH_DB_SOURCES";

#[derive(Clone)]
pub struct RethDbProvider;

impl RethDbProvider {
    pub fn new(chain: &str, datadir: &str) -> Result<Self> {
        if chain.trim().is_empty() {
            bail!("Reth DB provider chain cannot be empty");
        }
        if datadir.trim().is_empty() {
            bail!("Reth DB provider datadir cannot be empty");
        }

        bail!("{RETH_DB_FEATURE_ERROR}")
    }

    pub async fn fetch_chain_heads(&self) -> Result<ProviderHeadSnapshot> {
        unavailable()
    }

    pub async fn fetch_block_hashes_by_numbers(
        &self,
        _block_numbers: &[i64],
    ) -> Result<Vec<ProviderResolvedBlock>> {
        unavailable()
    }

    pub async fn fetch_block_by_hash(&self, _block_hash: &str) -> Result<ProviderBlock> {
        unavailable()
    }

    pub async fn fetch_block_headers_by_hashes(
        &self,
        _resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlock>> {
        unavailable()
    }

    pub async fn fetch_block_bundles_by_hashes(
        &self,
        _resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlockBundle>> {
        unavailable()
    }

    pub async fn fetch_block_bundles_without_logs_by_hashes(
        &self,
        _resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlockBundle>> {
        unavailable()
    }

    pub async fn fetch_block_bundle_by_hash(
        &self,
        _block_hash: &str,
    ) -> Result<ProviderBlockBundle> {
        unavailable()
    }

    pub async fn fetch_logs_by_block_range(
        &self,
        _resolved_blocks: &[ProviderResolvedBlock],
        _addresses: &[String],
    ) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
        unavailable()
    }

    pub async fn fetch_logs_by_block_range_for_topic0s_and_addresses(
        &self,
        _resolved_blocks: &[ProviderResolvedBlock],
        _topic0s: &[String],
        _addresses: &[String],
    ) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
        unavailable()
    }

    pub async fn fetch_transaction_receipt_pairs_by_hashes(
        &self,
        _requests: &[ProviderTransactionReceiptRequest],
    ) -> Result<Vec<ProviderTransactionReceiptBundle>> {
        unavailable()
    }

    pub async fn fetch_code_observations_at_block(
        &self,
        _addresses: &[String],
        _block: ProviderBlockSelection,
    ) -> Result<Vec<ProviderCodeObservation>> {
        unavailable()
    }

    pub async fn fetch_code_observations_at_block_hashes(
        &self,
        _requests: &[ProviderBlockCodeObservationRequest],
    ) -> Result<Vec<ProviderBlockCodeObservations>> {
        unavailable()
    }
}

impl fmt::Debug for RethDbProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RethDbProvider")
            .field("compiled", &false)
            .finish()
    }
}

fn unavailable<T>() -> Result<T> {
    bail!("{RETH_DB_FEATURE_ERROR}")
}
