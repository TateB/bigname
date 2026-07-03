use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use bigname_storage::RawCodeHashCorrectionCandidate;
use tracing::info;

use crate::{
    provider::{
        ChainProviderOps, JsonRpcProvider, ProviderBlockCodeHashProofRequest,
        ProviderBlockCodeObservationRequest, ProviderBlockCodeObservations,
    },
    reconciliation::keccak256_hex,
};

use super::{CorrectionSampleRow, DerivedCodeHash};

pub(super) async fn derive_code_hashes(
    reth: &(impl ChainProviderOps + ?Sized),
    rows: &[RawCodeHashCorrectionCandidate],
) -> Result<BTreeMap<(String, String), DerivedCodeHash>> {
    let requests = code_observation_requests(rows);
    let observations = reth
        .fetch_code_observations_at_block_hashes(&requests)
        .await?;
    code_observation_digests(&observations)
}

fn code_observation_requests(
    rows: &[RawCodeHashCorrectionCandidate],
) -> Vec<ProviderBlockCodeObservationRequest> {
    let mut addresses_by_block_hash = BTreeMap::<String, Vec<String>>::new();
    for row in rows {
        addresses_by_block_hash
            .entry(row.block_hash.clone())
            .or_default()
            .push(row.contract_address.clone());
    }

    addresses_by_block_hash
        .into_iter()
        .map(
            |(block_hash, addresses)| ProviderBlockCodeObservationRequest {
                block_hash,
                addresses,
            },
        )
        .collect()
}

fn code_observation_digests(
    observations: &[ProviderBlockCodeObservations],
) -> Result<BTreeMap<(String, String), DerivedCodeHash>> {
    let mut digests = BTreeMap::new();
    for block_observations in observations {
        for observation in &block_observations.observations {
            let code_byte_length = i64::try_from(observation.code.len()).with_context(|| {
                format!(
                    "code byte length {} does not fit in i64 for {} at {}",
                    observation.code.len(),
                    observation.address,
                    block_observations.block_hash
                )
            })?;
            digests.insert(
                (
                    block_observations.block_hash.clone(),
                    observation.address.clone(),
                ),
                DerivedCodeHash {
                    code_hash: keccak256_hex(&observation.code),
                    code_byte_length,
                },
            );
        }
    }
    Ok(digests)
}

pub(super) async fn verify_rpc_sample(
    rpc: &JsonRpcProvider,
    samples: &[CorrectionSampleRow],
) -> Result<()> {
    if samples.is_empty() {
        return Ok(());
    }

    let mut addresses_by_block_hash = BTreeMap::<String, Vec<String>>::new();
    for sample in samples {
        addresses_by_block_hash
            .entry(sample.block_hash.clone())
            .or_default()
            .push(sample.contract_address.clone());
    }
    let requests = addresses_by_block_hash
        .into_iter()
        .map(
            |(block_hash, addresses)| ProviderBlockCodeHashProofRequest {
                block_hash,
                addresses,
            },
        )
        .collect::<Vec<_>>();
    let proofs = rpc
        .fetch_code_hash_proofs_at_block_hashes(&requests)
        .await?;
    let mut proof_hashes = BTreeMap::<(String, String), String>::new();
    for block_proofs in proofs {
        for proof in block_proofs.proofs {
            proof_hashes.insert(
                (block_proofs.block_hash.clone(), proof.address),
                proof.code_hash,
            );
        }
    }

    for sample in samples {
        let key = (sample.block_hash.clone(), sample.contract_address.clone());
        let proof_hash = proof_hashes.get(&key).with_context(|| {
            format!(
                "RPC proof sample omitted {} at {}",
                sample.contract_address, sample.block_hash
            )
        })?;
        ensure!(
            proof_hash == &sample.rederived_code_hash,
            "Reth DB re-derived code hash {} disagrees with eth_getProof codeHash {} for raw_code_hash_id {} address {} at block {} ({})",
            sample.rederived_code_hash,
            proof_hash,
            sample.raw_code_hash_id,
            sample.contract_address,
            sample.block_number,
            sample.block_hash
        );
    }

    info!(
        service = "indexer",
        command = "repair raw-code-hashes",
        rpc_sample_count = samples.len(),
        "raw code-hash correction RPC sample verified"
    );
    Ok(())
}
