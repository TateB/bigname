use std::collections::BTreeMap;

use anyhow::Result;

use super::{
    ChainProvider, ChainProviderOps, JsonRpcProvider, ProviderBlock, ProviderBlockBundle,
    ProviderBlockCodeObservationRequest, ProviderBlockCodeObservations, ProviderBlockSelection,
    ProviderCodeObservation, ProviderHeadSnapshot, ProviderLog, ProviderResolvedBlock,
    ProviderTransactionReceiptBundle, ProviderTransactionReceiptRequest,
};

impl ChainProviderOps for JsonRpcProvider {
    async fn fetch_chain_heads(&self) -> Result<ProviderHeadSnapshot> {
        JsonRpcProvider::fetch_chain_heads(self).await
    }

    async fn fetch_block_hashes_by_numbers(
        &self,
        block_numbers: &[i64],
    ) -> Result<Vec<ProviderResolvedBlock>> {
        JsonRpcProvider::fetch_block_hashes_by_numbers(self, block_numbers).await
    }

    async fn fetch_block_by_hash(&self, block_hash: &str) -> Result<ProviderBlock> {
        JsonRpcProvider::fetch_block_by_hash(self, block_hash).await
    }

    async fn fetch_block_headers_by_hashes(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlock>> {
        JsonRpcProvider::fetch_block_headers_by_hashes(self, resolved_blocks).await
    }

    async fn fetch_block_bundles_by_hashes(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlockBundle>> {
        JsonRpcProvider::fetch_block_bundles_by_hashes(self, resolved_blocks).await
    }

    async fn fetch_block_bundles_without_logs_by_hashes(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlockBundle>> {
        JsonRpcProvider::fetch_block_bundles_without_logs_by_hashes(self, resolved_blocks).await
    }

    async fn fetch_block_bundle_by_hash(&self, block_hash: &str) -> Result<ProviderBlockBundle> {
        JsonRpcProvider::fetch_block_bundle_by_hash(self, block_hash).await
    }

    async fn fetch_logs_by_block_range(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
        addresses: &[String],
    ) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
        JsonRpcProvider::fetch_logs_by_block_range(self, resolved_blocks, addresses).await
    }

    async fn fetch_logs_by_block_range_for_topic0s_and_addresses(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
        topic0s: &[String],
        addresses: &[String],
    ) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
        JsonRpcProvider::fetch_logs_by_block_range_for_topic0s_and_addresses(
            self,
            resolved_blocks,
            topic0s,
            addresses,
        )
        .await
    }

    async fn fetch_transaction_receipt_pairs_by_hashes(
        &self,
        requests: &[ProviderTransactionReceiptRequest],
    ) -> Result<Vec<ProviderTransactionReceiptBundle>> {
        JsonRpcProvider::fetch_transaction_receipt_pairs_by_hashes(self, requests).await
    }

    async fn fetch_code_observations_at_block(
        &self,
        addresses: &[String],
        block: ProviderBlockSelection,
    ) -> Result<Vec<ProviderCodeObservation>> {
        JsonRpcProvider::fetch_code_observations_at_block(self, addresses, block).await
    }

    async fn fetch_code_observations_at_block_hashes(
        &self,
        requests: &[ProviderBlockCodeObservationRequest],
    ) -> Result<Vec<ProviderBlockCodeObservations>> {
        JsonRpcProvider::fetch_code_observations_at_block_hashes(self, requests).await
    }
}

impl ChainProviderOps for ChainProvider {
    async fn fetch_chain_heads(&self) -> Result<ProviderHeadSnapshot> {
        match self {
            Self::JsonRpc(provider) => provider.fetch_chain_heads().await,
            Self::RethDb(provider) => provider.fetch_chain_heads().await,
        }
    }

    async fn fetch_block_hashes_by_numbers(
        &self,
        block_numbers: &[i64],
    ) -> Result<Vec<ProviderResolvedBlock>> {
        match self {
            Self::JsonRpc(provider) => provider.fetch_block_hashes_by_numbers(block_numbers).await,
            Self::RethDb(provider) => provider.fetch_block_hashes_by_numbers(block_numbers).await,
        }
    }

    async fn fetch_block_by_hash(&self, block_hash: &str) -> Result<ProviderBlock> {
        match self {
            Self::JsonRpc(provider) => provider.fetch_block_by_hash(block_hash).await,
            Self::RethDb(provider) => provider.fetch_block_by_hash(block_hash).await,
        }
    }

    async fn fetch_block_headers_by_hashes(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlock>> {
        match self {
            Self::JsonRpc(provider) => {
                provider
                    .fetch_block_headers_by_hashes(resolved_blocks)
                    .await
            }
            Self::RethDb(provider) => {
                provider
                    .fetch_block_headers_by_hashes(resolved_blocks)
                    .await
            }
        }
    }

    async fn fetch_logs_by_block_range_for_topic0s_and_addresses(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
        topic0s: &[String],
        addresses: &[String],
    ) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
        match self {
            Self::JsonRpc(provider) => {
                provider
                    .fetch_logs_by_block_range_for_topic0s_and_addresses(
                        resolved_blocks,
                        topic0s,
                        addresses,
                    )
                    .await
            }
            Self::RethDb(provider) => {
                provider
                    .fetch_logs_by_block_range_for_topic0s_and_addresses(
                        resolved_blocks,
                        topic0s,
                        addresses,
                    )
                    .await
            }
        }
    }

    async fn fetch_block_bundles_by_hashes(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlockBundle>> {
        match self {
            Self::JsonRpc(provider) => {
                provider
                    .fetch_block_bundles_by_hashes(resolved_blocks)
                    .await
            }
            Self::RethDb(provider) => {
                provider
                    .fetch_block_bundles_by_hashes(resolved_blocks)
                    .await
            }
        }
    }

    async fn fetch_block_bundles_without_logs_by_hashes(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
    ) -> Result<Vec<ProviderBlockBundle>> {
        match self {
            Self::JsonRpc(provider) => {
                provider
                    .fetch_block_bundles_without_logs_by_hashes(resolved_blocks)
                    .await
            }
            Self::RethDb(provider) => {
                provider
                    .fetch_block_bundles_without_logs_by_hashes(resolved_blocks)
                    .await
            }
        }
    }

    async fn fetch_block_bundle_by_hash(&self, block_hash: &str) -> Result<ProviderBlockBundle> {
        match self {
            Self::JsonRpc(provider) => provider.fetch_block_bundle_by_hash(block_hash).await,
            Self::RethDb(provider) => provider.fetch_block_bundle_by_hash(block_hash).await,
        }
    }

    async fn fetch_logs_by_block_range(
        &self,
        resolved_blocks: &[ProviderResolvedBlock],
        addresses: &[String],
    ) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
        match self {
            Self::JsonRpc(provider) => {
                provider
                    .fetch_logs_by_block_range(resolved_blocks, addresses)
                    .await
            }
            Self::RethDb(provider) => {
                provider
                    .fetch_logs_by_block_range(resolved_blocks, addresses)
                    .await
            }
        }
    }

    async fn fetch_transaction_receipt_pairs_by_hashes(
        &self,
        requests: &[ProviderTransactionReceiptRequest],
    ) -> Result<Vec<ProviderTransactionReceiptBundle>> {
        match self {
            Self::JsonRpc(provider) => {
                provider
                    .fetch_transaction_receipt_pairs_by_hashes(requests)
                    .await
            }
            Self::RethDb(provider) => {
                provider
                    .fetch_transaction_receipt_pairs_by_hashes(requests)
                    .await
            }
        }
    }

    async fn fetch_code_observations_at_block(
        &self,
        addresses: &[String],
        block: ProviderBlockSelection,
    ) -> Result<Vec<ProviderCodeObservation>> {
        match self {
            Self::JsonRpc(provider) => {
                provider
                    .fetch_code_observations_at_block(addresses, block)
                    .await
            }
            Self::RethDb(provider) => {
                provider
                    .fetch_code_observations_at_block(addresses, block)
                    .await
            }
        }
    }

    async fn fetch_code_observations_at_block_hashes(
        &self,
        requests: &[ProviderBlockCodeObservationRequest],
    ) -> Result<Vec<ProviderBlockCodeObservations>> {
        match self {
            Self::JsonRpc(provider) => {
                provider
                    .fetch_code_observations_at_block_hashes(requests)
                    .await
            }
            Self::RethDb(provider) => {
                provider
                    .fetch_code_observations_at_block_hashes(requests)
                    .await
            }
        }
    }
}
