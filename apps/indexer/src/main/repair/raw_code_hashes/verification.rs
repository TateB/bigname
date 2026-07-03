use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, Result, ensure};
use bigname_storage::RawCodeHashCorrectionCandidate;
use tracing::{info, warn};

use crate::{
    provider::{
        ChainProviderOps, JsonRpcProvider, ProviderBlockCodeHashProofRequest,
        ProviderBlockCodeObservationRequest, ProviderBlockCodeObservations,
    },
    reconciliation::keccak256_hex,
};

use super::{CorrectionSampleRow, DerivedCodeHash};

pub(super) const PROOF_SPOT_CHECK_TIMEOUT_SECS: u64 = 300;

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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct ProofSpotCheckOutcome {
    pub(super) attempted_count: usize,
    pub(super) verified_count: usize,
    pub(super) timed_out: bool,
}

pub(super) async fn verify_rpc_code_sample(
    rpc: &JsonRpcProvider,
    samples: &[CorrectionSampleRow],
) -> Result<()> {
    if samples.is_empty() {
        return Ok(());
    }

    let requests = sample_code_observation_requests(samples);
    let observations = rpc
        .fetch_code_observations_at_block_hashes(&requests)
        .await?;
    let observed = code_observation_digests(&observations)?;

    for sample in samples {
        let key = (sample.block_hash.clone(), sample.contract_address.clone());
        let observed = observed.get(&key).with_context(|| {
            format!(
                "RPC code sample omitted {} at {}",
                sample.contract_address, sample.block_hash
            )
        })?;
        ensure!(
            observed.code_hash == sample.rederived_code_hash
                && observed.code_byte_length == sample.rederived_code_byte_length,
            "Reth DB re-derived code hash/length {}/{} disagrees with eth_getCode hash/length {}/{} for raw_code_hash_id {} address {} at block {} ({})",
            sample.rederived_code_hash,
            sample.rederived_code_byte_length,
            observed.code_hash,
            observed.code_byte_length,
            sample.raw_code_hash_id,
            sample.contract_address,
            sample.block_number,
            sample.block_hash
        );
    }

    info!(
        service = "indexer",
        command = "repair raw-code-hashes",
        rpc_code_sample_count = samples.len(),
        "raw code-hash correction RPC eth_getCode sample verified"
    );
    Ok(())
}

fn sample_code_observation_requests(
    samples: &[CorrectionSampleRow],
) -> Vec<ProviderBlockCodeObservationRequest> {
    let mut addresses_by_block_hash = BTreeMap::<String, Vec<String>>::new();
    for sample in samples {
        addresses_by_block_hash
            .entry(sample.block_hash.clone())
            .or_default()
            .push(sample.contract_address.clone());
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

pub(super) async fn verify_rpc_proof_spot_check(
    rpc: &JsonRpcProvider,
    samples: &[CorrectionSampleRow],
) -> Result<ProofSpotCheckOutcome> {
    if samples.is_empty() {
        info!(
            service = "indexer",
            command = "repair raw-code-hashes",
            proof_spot_check_status = "skipped",
            proof_spot_check_count = 0_usize,
            "raw code-hash correction eth_getProof spot-check skipped"
        );
        return Ok(ProofSpotCheckOutcome::default());
    }

    let result = tokio::time::timeout(
        Duration::from_secs(PROOF_SPOT_CHECK_TIMEOUT_SECS),
        verify_rpc_proof_sample(rpc, samples),
    )
    .await;

    match result {
        Err(error) => {
            warn!(
                service = "indexer",
                command = "repair raw-code-hashes",
                proof_spot_check_status = "timeout",
                proof_spot_check_count = samples.len(),
                error = %error,
                "raw code-hash correction eth_getProof spot-check unavailable; continuing after mandatory eth_getCode verification"
            );
            Ok(ProofSpotCheckOutcome {
                attempted_count: samples.len(),
                verified_count: 0,
                timed_out: true,
            })
        }
        Ok(Ok(())) => Ok(ProofSpotCheckOutcome {
            attempted_count: samples.len(),
            verified_count: samples.len(),
            timed_out: false,
        }),
        Ok(Err(error)) if is_provider_serving_error(&error) => {
            let timed_out = is_timeout_error(&error);
            warn!(
                service = "indexer",
                command = "repair raw-code-hashes",
                proof_spot_check_status = if timed_out { "timeout" } else { "provider_error" },
                proof_spot_check_count = samples.len(),
                error = %format!("{error:#}"),
                "raw code-hash correction eth_getProof spot-check unavailable; continuing after mandatory eth_getCode verification"
            );
            Ok(ProofSpotCheckOutcome {
                attempted_count: samples.len(),
                verified_count: 0,
                timed_out,
            })
        }
        Ok(Err(error)) => {
            warn!(
                service = "indexer",
                command = "repair raw-code-hashes",
                proof_spot_check_status = "disagreement",
                proof_spot_check_count = samples.len(),
                error = %format!("{error:#}"),
                "raw code-hash correction eth_getProof spot-check disagreed"
            );
            Err(error)
        }
    }
}

async fn verify_rpc_proof_sample(
    rpc: &JsonRpcProvider,
    samples: &[CorrectionSampleRow],
) -> Result<()> {
    let requests = proof_requests(samples);
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
        proof_spot_check_status = "verified",
        proof_spot_check_count = samples.len(),
        "raw code-hash correction eth_getProof spot-check verified"
    );
    Ok(())
}

fn proof_requests(samples: &[CorrectionSampleRow]) -> Vec<ProviderBlockCodeHashProofRequest> {
    let mut addresses_by_block_hash = BTreeMap::<String, Vec<String>>::new();
    for sample in samples {
        addresses_by_block_hash
            .entry(sample.block_hash.clone())
            .or_default()
            .push(sample.contract_address.clone());
    }
    addresses_by_block_hash
        .into_iter()
        .map(
            |(block_hash, addresses)| ProviderBlockCodeHashProofRequest {
                block_hash,
                addresses,
            },
        )
        .collect()
}

fn is_provider_serving_error(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    is_timeout_message(&message)
        || message.contains("json-rpc")
        || message.contains("http ")
        || message.contains("failed to send")
        || message.contains("failed to read")
        || message.contains("failed to decode eth_getproof response")
        || message.contains("provider did not return proof")
        || message.contains("provider batch omitted proof")
        || message.contains("provider request")
        || message.contains("provider returned")
}

fn is_timeout_error(error: &anyhow::Error) -> bool {
    is_timeout_message(&format!("{error:#}").to_ascii_lowercase())
}

fn is_timeout_message(message: &str) -> bool {
    message.contains("timed out")
        || message.contains("timeout")
        || message.contains("deadline")
        || message.contains("elapsed")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_proof_disagreements_are_not_provider_serving_errors() {
        let address_error = anyhow::anyhow!(
            "provider proof address 0x2222222222222222222222222222222222222222 does not match requested address 0x1111111111111111111111111111111111111111"
        );
        let hash_error = anyhow::anyhow!(
            "Reth DB re-derived code hash 0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa disagrees with eth_getProof codeHash 0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );

        assert!(!is_provider_serving_error(&address_error));
        assert!(!is_provider_serving_error(&hash_error));
    }
}
