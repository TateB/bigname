use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::Value;

use super::{
    JsonRpcProvider, PROVIDER_BATCH_ITEM_LIMIT, ProviderBlockCodeObservationRequest,
    ProviderBlockCodeObservations, ProviderBlockSelection, ProviderCodeObservation,
    decode::{address_hex_from_str, hash_hex_from_str, parse_hex_bytes},
    request::JsonRpcBatchCall,
};

impl JsonRpcProvider {
    pub async fn fetch_code_observations_at_block(
        &self,
        addresses: &[String],
        block: ProviderBlockSelection,
    ) -> Result<Vec<ProviderCodeObservation>> {
        let block_parameter = block.json_rpc_parameter()?;
        let mut cached_observations: BTreeMap<String, ProviderCodeObservation> = BTreeMap::new();
        let mut observations = Vec::with_capacity(addresses.len());

        for address in addresses {
            let address = address_hex_from_str(address)?;
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
            let block_hash =
                hash_hex_from_str(&request.block_hash, "provider code observation block hash")?;
            let block_parameter =
                ProviderBlockSelection::Hash(block_hash.clone()).json_rpc_parameter()?;
            let addresses = request
                .addresses
                .iter()
                .map(|address| address_hex_from_str(address))
                .collect::<Result<Vec<_>>>()?;

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
}
