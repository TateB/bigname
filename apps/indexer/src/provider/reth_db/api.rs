use std::{
    collections::BTreeMap,
    fmt,
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use anyhow::{Context, Result, bail};
use tokio::task;

use super::{
    RethDbProvider, RethDbReader,
    convert::{normalized_contiguous_resolved_blocks, normalized_resolved_blocks},
};
use crate::provider::{
    ProviderBlock, ProviderBlockBundle, ProviderBlockCodeObservationRequest,
    ProviderBlockCodeObservations, ProviderBlockSelection, ProviderCodeObservation,
    ProviderHeadSnapshot, ProviderLog, ProviderResolvedBlock, ProviderTransactionReceiptBundle,
    ProviderTransactionReceiptRequest, decode::normalize_hash,
};

impl RethDbProvider {
    pub fn new(chain: &str, datadir: &str) -> Result<Self> {
        let chain = chain.trim();
        let datadir = datadir.trim();
        if chain.is_empty() {
            bail!("Reth DB provider chain cannot be empty");
        }
        if datadir.is_empty() {
            bail!("Reth DB provider datadir cannot be empty");
        }

        Ok(Self {
            reader: Arc::new(RethDbReader {
                chain: chain.to_owned(),
                datadir: PathBuf::from(datadir),
                factory: OnceLock::new(),
            }),
        })
    }

    pub async fn fetch_chain_heads(&self) -> Result<ProviderHeadSnapshot> {
        self.blocking("fetch_chain_heads", |reader| {
            reader.fetch_chain_heads_sync()
        })
        .await
    }

    pub async fn fetch_block_hashes_by_numbers(
        &self,
        block_numbers: &[i64],
    ) -> Result<Vec<ProviderResolvedBlock>> {
        let block_numbers = block_numbers.to_vec();
        self.blocking("fetch_block_hashes_by_numbers", move |reader| {
            reader.fetch_block_hashes_by_numbers_sync(&block_numbers)
        })
        .await
    }

    pub async fn fetch_block_by_hash(&self, block_hash: &str) -> Result<ProviderBlock> {
        let block_hash = block_hash.to_owned();
        self.blocking("fetch_block_by_hash", move |reader| {
            reader.fetch_block_by_hash_sync(&block_hash)
        })
        .await
    }

    pub async fn fetch_block_headers_by_hashes(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlock>> {
        let resolved_blocks = normalized_resolved_blocks(resolved_blocks)?;
        self.blocking("fetch_block_headers_by_hashes", move |reader| {
            reader.fetch_block_headers_by_hashes_sync(&resolved_blocks)
        })
        .await
    }

    pub async fn fetch_block_bundles_by_hashes(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlockBundle>> {
        let resolved_blocks = normalized_resolved_blocks(resolved_blocks)?;
        self.blocking("fetch_block_bundles_by_hashes", move |reader| {
            reader.fetch_block_bundles_by_hashes_sync(&resolved_blocks, true)
        })
        .await
    }

    pub async fn fetch_block_bundles_without_logs_by_hashes(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlockBundle>> {
        let resolved_blocks = normalized_resolved_blocks(resolved_blocks)?;
        self.blocking(
            "fetch_block_bundles_without_logs_by_hashes",
            move |reader| reader.fetch_block_bundles_by_hashes_sync(&resolved_blocks, false),
        )
        .await
    }

    pub async fn fetch_block_bundle_by_hash(
        &self,
        block_hash: &str,
    ) -> Result<ProviderBlockBundle> {
        let block_hash = block_hash.to_owned();
        self.blocking("fetch_block_bundle_by_hash", move |reader| {
            reader.fetch_block_bundle_by_hash_sync(&block_hash, true)
        })
        .await
    }

    pub async fn fetch_logs_by_block_range(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
        addresses: &[String],
    ) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
        let resolved_blocks = normalized_contiguous_resolved_blocks(resolved_blocks)?;
        let addresses = addresses.to_vec();
        self.blocking("fetch_logs_by_block_range", move |reader| {
            reader.fetch_logs_by_block_range_sync(&resolved_blocks, &addresses)
        })
        .await
    }

    pub async fn fetch_logs_by_block_range_for_topic0s_and_addresses(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
        topic0s: &[String],
        addresses: &[String],
    ) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
        let resolved_blocks = normalized_contiguous_resolved_blocks(resolved_blocks)?;
        let topic0s = topic0s.to_vec();
        let addresses = addresses.to_vec();
        self.blocking(
            "fetch_logs_by_block_range_for_topic0s_and_addresses",
            move |reader| {
                reader.fetch_logs_by_block_range_for_topic0s_and_addresses_sync(
                    &resolved_blocks,
                    &topic0s,
                    &addresses,
                )
            },
        )
        .await
    }

    pub async fn fetch_transaction_receipt_pairs_by_hashes(
        &self,
        requests: &[ProviderTransactionReceiptRequest],
    ) -> Result<Vec<ProviderTransactionReceiptBundle>> {
        let requests = requests.to_vec();
        self.blocking("fetch_transaction_receipt_pairs_by_hashes", move |reader| {
            reader.fetch_transaction_receipt_pairs_by_hashes_sync(&requests)
        })
        .await
    }

    pub async fn fetch_code_observations_at_block(
        &self,
        addresses: &[String],
        block: ProviderBlockSelection,
    ) -> Result<Vec<ProviderCodeObservation>> {
        let addresses = addresses.to_vec();
        self.blocking("fetch_code_observations_at_block", move |reader| {
            reader.fetch_code_observations_at_block_sync(&addresses, block)
        })
        .await
    }

    pub async fn fetch_code_observations_at_block_hashes(
        &self,
        requests: &[ProviderBlockCodeObservationRequest],
    ) -> Result<Vec<ProviderBlockCodeObservations>> {
        let requests = requests
            .iter()
            .map(|request| ProviderBlockCodeObservationRequest {
                block_hash: normalize_hash(&request.block_hash),
                addresses: request.addresses.clone(),
            })
            .collect::<Vec<_>>();
        self.blocking("fetch_code_observations_at_block_hashes", move |reader| {
            reader.fetch_code_observations_at_block_hashes_sync(&requests)
        })
        .await
    }

    async fn blocking<T>(
        &self,
        operation: &'static str,
        work: impl FnOnce(Arc<RethDbReader>) -> Result<T> + Send + 'static,
    ) -> Result<T>
    where
        T: Send + 'static,
    {
        let reader = Arc::clone(&self.reader);
        task::spawn_blocking(move || work(reader))
            .await
            .with_context(|| format!("Reth DB provider task failed while running {operation}"))?
    }
}

impl fmt::Debug for RethDbProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RethDbProvider")
            .field("chain", &self.reader.chain)
            .field("datadir", &self.reader.datadir)
            .finish_non_exhaustive()
    }
}
