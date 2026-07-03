use std::collections::BTreeMap;

use alloy_primitives::{Address, B256};
use anyhow::{Context, Result};
use reth_ethereum::{
    provider::{BlockHashReader, BlockNumReader},
    storage::{ChainStateBlockReader, StateProvider},
};

use super::{
    EthereumRethProviderFactory, RethDbReader,
    convert::{address_hex, i64_to_u64, parse_address, parse_b256},
};
use crate::provider::{
    ProviderBlockCodeObservationRequest, ProviderBlockCodeObservations, ProviderBlockSelection,
    ProviderBlockTag, ProviderCodeObservation, decode::normalize_hash,
};

impl RethDbReader {
    pub(super) fn fetch_code_observations_at_block_sync(
        &self,
        addresses: &[String],
        block: ProviderBlockSelection,
    ) -> Result<Vec<ProviderCodeObservation>> {
        let factory = self.factory()?;
        let block_hash = self.resolve_block_selection_to_hash(&factory, block)?;
        let state = factory.history_by_block_hash(block_hash)?;
        let mut cached_observations = BTreeMap::<Address, ProviderCodeObservation>::new();
        let mut observations = Vec::with_capacity(addresses.len());

        for address in addresses {
            let parsed = parse_address(address)?;
            if let Some(observation) = cached_observations.get(&parsed) {
                observations.push(observation.clone());
                continue;
            }

            let code = state
                .account_code(&parsed)?
                .map(|bytecode| bytecode.0.original_bytes().to_vec())
                .unwrap_or_default();
            let observation = ProviderCodeObservation {
                address: address_hex(parsed),
                code,
            };
            cached_observations.insert(parsed, observation.clone());
            observations.push(observation);
        }

        Ok(observations)
    }

    pub(super) fn fetch_code_observations_at_block_hashes_sync(
        &self,
        requests: &[ProviderBlockCodeObservationRequest],
    ) -> Result<Vec<ProviderBlockCodeObservations>> {
        let mut observations = Vec::with_capacity(requests.len());

        for request in requests {
            let block_hash = normalize_hash(&request.block_hash);
            observations.push(ProviderBlockCodeObservations {
                block_hash: block_hash.clone(),
                observations: self.fetch_code_observations_at_block_sync(
                    &request.addresses,
                    ProviderBlockSelection::Hash(block_hash),
                )?,
            });
        }

        Ok(observations)
    }

    fn resolve_block_selection_to_hash(
        &self,
        factory: &EthereumRethProviderFactory,
        block: ProviderBlockSelection,
    ) -> Result<B256> {
        match block {
            ProviderBlockSelection::Number(number) => {
                let number = i64_to_u64(number, "provider block selection number")?;
                factory.block_hash(number)?.with_context(|| {
                    format!("Reth DB did not return block hash for number {number}")
                })
            }
            ProviderBlockSelection::Hash(block_hash) => parse_b256(&block_hash, "block hash"),
            ProviderBlockSelection::Tag(ProviderBlockTag::Latest) => {
                Ok(factory.chain_info()?.best_hash)
            }
            ProviderBlockSelection::Tag(ProviderBlockTag::Safe) => {
                let provider = factory.provider()?;
                let number = provider
                    .last_safe_block_number()?
                    .context("Reth DB did not return a safe block number")?;
                drop(provider);
                factory
                    .block_hash(number)?
                    .with_context(|| format!("Reth DB did not return safe block hash for {number}"))
            }
            ProviderBlockSelection::Tag(ProviderBlockTag::Finalized) => {
                let provider = factory.provider()?;
                let number = provider
                    .last_finalized_block_number()?
                    .context("Reth DB did not return a finalized block number")?;
                drop(provider);
                factory.block_hash(number)?.with_context(|| {
                    format!("Reth DB did not return finalized block hash for {number}")
                })
            }
        }
    }
}
