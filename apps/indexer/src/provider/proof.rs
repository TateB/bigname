use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use serde_json::Value;

use super::{
    JsonRpcProvider, ProviderBlockCodeHashProofRequest, ProviderBlockCodeHashProofs,
    ProviderBlockSelection, ProviderCodeHashProof,
    decode::{address_hex_from_str, hash_hex_from_str},
    provider_batch_item_limit,
    request::JsonRpcBatchCall,
};

impl JsonRpcProvider {
    pub async fn fetch_code_hash_proofs_at_block_hashes(
        &self,
        requests: &[ProviderBlockCodeHashProofRequest],
    ) -> Result<Vec<ProviderBlockCodeHashProofs>> {
        let mut normalized_requests = Vec::with_capacity(requests.len());
        let mut seen_call_keys = BTreeMap::<(String, String), ()>::new();
        let mut call_keys = Vec::<(String, String, Value)>::new();

        for request in requests {
            let block_hash =
                hash_hex_from_str(&request.block_hash, "provider code-hash proof block hash")?;
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

        let mut proofs_by_key = BTreeMap::<(String, String), String>::new();
        for chunk in call_keys.chunks(provider_batch_item_limit()) {
            let calls = chunk
                .iter()
                .map(|(_, address, block_parameter)| JsonRpcBatchCall {
                    method: "eth_getProof",
                    params: vec![
                        Value::String(address.clone()),
                        Value::Array(Vec::new()),
                        block_parameter.clone(),
                    ],
                })
                .collect::<Vec<_>>();
            let results = self.fetch_json_rpc_batch_results(calls).await?;

            for ((block_hash, address, _), result) in chunk.iter().zip(results) {
                let proof = result.with_context(|| {
                    format!(
                        "provider did not return proof for address {address} at block hash {block_hash}"
                    )
                })?;
                let proof = serde_json::from_value::<RpcAccountProof>(proof).with_context(|| {
                    format!(
                        "failed to decode eth_getProof response for address {address} at block hash {block_hash}"
                    )
                })?;
                let proof_address = address_hex_from_str(&proof.address)?;
                ensure!(
                    proof_address == *address,
                    "provider proof address {proof_address} does not match requested address {address} at block hash {block_hash}"
                );
                let code_hash = hash_hex_from_str(&proof.code_hash, "provider proof codeHash")?;
                proofs_by_key.insert((block_hash.clone(), address.clone()), code_hash);
            }
        }

        let mut proofs = Vec::with_capacity(normalized_requests.len());
        for (block_hash, addresses) in normalized_requests {
            let mut block_proofs = Vec::with_capacity(addresses.len());
            for address in addresses {
                let code_hash = proofs_by_key
                    .get(&(block_hash.clone(), address.clone()))
                    .with_context(|| {
                        format!(
                            "provider batch omitted proof for address {address} at block hash {block_hash}"
                        )
                    })?
                    .clone();
                block_proofs.push(ProviderCodeHashProof { address, code_hash });
            }

            proofs.push(ProviderBlockCodeHashProofs {
                block_hash,
                proofs: block_proofs,
            });
        }

        Ok(proofs)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcAccountProof {
    address: String,
    code_hash: String,
}
