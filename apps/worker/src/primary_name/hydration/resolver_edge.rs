use std::collections::BTreeMap;

use anyhow::{Context, Result};
use bigname_execution::{OnDemandEnsPrimaryNameError, ens_namehash_hex};
use bigname_storage::{ENS_NAMESPACE, normalize_evm_address};
use serde_json::{Value, json};
use sqlx::PgPool;

use super::super::{
    PrimaryNameLegacyReverseHydrationSummary,
    projection::primary_name_row_with_provenance_extensions,
    types::{NameClaimObservation, PrimaryNameTupleKey, ReverseClaimTuple},
};
use super::{
    COIN_TYPE_ETH, DERIVATION_KIND_LEGACY_REVERSE_RESOLVER_HYDRATION, HYDRATION_PROVENANCE_KEY,
    ResolverEdgeHydrationCandidate, ResolverEdgeHydrationTarget, ReverseNameHydrationCall,
    ReverseNameHydrationChainPosition, ReverseNameHydrationClient, ReverseNameHydrationOutcome,
    SOURCE_FAMILY_ENS_V1_REVERSE_L1, TUPLE_SOURCE_RESOLVER_EDGE_FORWARD_CONFIRMED,
    add_snapshot_status,
};

pub(super) async fn hydrate_resolver_edge_candidates(
    pool: &PgPool,
    candidates: &[ResolverEdgeHydrationCandidate],
    client: &dyn ReverseNameHydrationClient,
    summary: &mut PrimaryNameLegacyReverseHydrationSummary,
    snapshots: &mut Vec<bigname_storage::PrimaryNameCurrentSnapshot>,
) -> Result<()> {
    for candidate in candidates
        .iter()
        .filter(|candidate| candidate.hydration_target.is_none())
    {
        if let Some(existing_key) = &candidate.existing_key {
            summary.deleted_row_count += bigname_storage::delete_primary_name_current(
                pool,
                &existing_key.address,
                &existing_key.namespace,
                &existing_key.coin_type,
            )
            .await?;
        }
    }

    let calls_by_position = candidates.iter().enumerate().fold(
        BTreeMap::<(String, i64, String), Vec<(usize, ReverseNameHydrationCall)>>::new(),
        |mut by_position, (index, candidate)| {
            let Some(target) = candidate.hydration_target.as_ref() else {
                return by_position;
            };
            by_position
                .entry((
                    target.chain_id.clone(),
                    target.position.block_number,
                    target.position.block_hash.clone(),
                ))
                .or_default()
                .push((
                    index,
                    ReverseNameHydrationCall {
                        resolver_address: target.resolver_address.clone(),
                        reverse_node: target.reverse_node.clone(),
                    },
                ));
            by_position
        },
    );

    for ((chain_id, block_number, block_hash), calls_with_refs) in calls_by_position {
        let position = ReverseNameHydrationChainPosition {
            block_number,
            block_hash,
        };
        let calls = calls_with_refs
            .iter()
            .map(|(_, call)| call.clone())
            .collect::<Vec<_>>();
        summary.queried_tuple_count += calls.len();
        let outcomes = match client.hydrate(&chain_id, &position, &calls).await {
            Ok(outcomes) => outcomes,
            Err(error) => {
                summary.failed_lookup_count += calls.len();
                tracing::warn!(
                    service = "worker",
                    projection = "primary_names_current",
                    chain_id,
                    error = %format!("{error:#}"),
                    failed_lookup_count = calls.len(),
                    "legacy reverse-resolver resolver-edge hydration batch failed"
                );
                continue;
            }
        };
        if outcomes.len() != calls_with_refs.len() {
            anyhow::bail!(
                "legacy reverse-resolver resolver-edge hydration provider returned {} outcomes for {} calls on {chain_id}",
                outcomes.len(),
                calls_with_refs.len()
            );
        }

        for ((candidate_index, _), outcome) in calls_with_refs.iter().zip(outcomes) {
            let candidate = candidates.get(*candidate_index).context(
                "legacy reverse-resolver resolver-edge candidate reference is out of bounds",
            )?;
            let target = candidate.hydration_target.as_ref().context(
                "legacy reverse-resolver resolver-edge candidate is missing hydration target",
            )?;
            let raw_name = match outcome {
                ReverseNameHydrationOutcome::Success(value) => value,
                ReverseNameHydrationOutcome::NotFound => {
                    delete_existing_resolver_edge_row(pool, candidate, summary).await?;
                    continue;
                }
                ReverseNameHydrationOutcome::Failed(_) => {
                    summary.failed_lookup_count += 1;
                    continue;
                }
            };
            let Some(normalized_name) = normalize_hydrated_name(&raw_name) else {
                delete_existing_resolver_edge_row(pool, candidate, summary).await?;
                continue;
            };
            let address = match client
                .lookup_forward_address(&chain_id, &position, &normalized_name)
                .await
            {
                Ok(Some(address)) => address,
                Ok(None) => {
                    delete_existing_resolver_edge_row(pool, candidate, summary).await?;
                    continue;
                }
                Err(error) => {
                    if is_universal_resolver_non_confirmation(&error)
                        || is_new_row_offchain_non_confirmation(candidate, &error)
                    {
                        delete_existing_resolver_edge_row(pool, candidate, summary).await?;
                    } else {
                        summary.failed_lookup_count += 1;
                        tracing::warn!(
                            service = "worker",
                            projection = "primary_names_current",
                            chain_id,
                            reverse_node = target.reverse_node,
                            normalized_name,
                            error = %format!("{error:#}"),
                            "legacy reverse-resolver resolver-edge forward confirmation failed"
                        );
                    }
                    continue;
                }
            };
            let normalized_address = normalize_evm_address(&address);
            if !forward_address_matches_reverse_node(&normalized_address, &target.reverse_node)? {
                delete_existing_resolver_edge_row(pool, candidate, summary).await?;
                continue;
            }

            let tuple = resolver_edge_tuple(&normalized_address);
            let primary_claim_source =
                resolver_edge_primary_claim_source(&tuple.key, &target.reverse_node);
            let claim_observation = NameClaimObservation {
                key: tuple.key.clone(),
                raw_name: Some(raw_name),
                primary_claim_source,
            };
            let hydration_provenance = resolver_edge_hydration_provenance(target, &position);
            let snapshot = primary_name_row_with_provenance_extensions(
                &tuple,
                Some(&claim_observation),
                [(HYDRATION_PROVENANCE_KEY, hydration_provenance)],
            )?;
            add_snapshot_status(summary, &snapshot);
            snapshots.push(snapshot);
        }
    }

    Ok(())
}

async fn delete_existing_resolver_edge_row(
    pool: &PgPool,
    candidate: &ResolverEdgeHydrationCandidate,
    summary: &mut PrimaryNameLegacyReverseHydrationSummary,
) -> Result<()> {
    let Some(existing_key) = &candidate.existing_key else {
        return Ok(());
    };
    summary.deleted_row_count += bigname_storage::delete_primary_name_current(
        pool,
        &existing_key.address,
        &existing_key.namespace,
        &existing_key.coin_type,
    )
    .await?;
    Ok(())
}

fn normalize_hydrated_name(raw_name: &str) -> Option<String> {
    if raw_name.is_empty() || raw_name.chars().all(char::is_whitespace) {
        return None;
    }
    bigname_domain::normalization::normalize_name(raw_name)
        .ok()
        .map(|name| name.normalized_name)
}

fn forward_address_matches_reverse_node(address: &str, reverse_node: &str) -> Result<bool> {
    let Some(label) = address.strip_prefix("0x") else {
        return Ok(false);
    };
    if label.len() != 40 || !label.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        return Ok(false);
    }
    let expected = ens_namehash_hex(&format!("{label}.addr.reverse"))?;
    Ok(expected.eq_ignore_ascii_case(reverse_node))
}

fn is_universal_resolver_non_confirmation(error: &anyhow::Error) -> bool {
    error
        .chain()
        .filter_map(|cause| cause.downcast_ref::<OnDemandEnsPrimaryNameError>())
        .any(OnDemandEnsPrimaryNameError::is_plain_execution_revert)
}

fn is_new_row_offchain_non_confirmation(
    candidate: &ResolverEdgeHydrationCandidate,
    error: &anyhow::Error,
) -> bool {
    candidate.existing_key.is_none()
        && error
            .chain()
            .filter_map(|cause| cause.downcast_ref::<OnDemandEnsPrimaryNameError>())
            .any(OnDemandEnsPrimaryNameError::is_offchain_lookup_required)
}

fn resolver_edge_tuple(address: &str) -> ReverseClaimTuple {
    ReverseClaimTuple {
        key: PrimaryNameTupleKey {
            address: address.to_owned(),
            namespace: ENS_NAMESPACE.to_owned(),
            coin_type: COIN_TYPE_ETH.to_owned(),
        },
        claim_provenance: json!({
            "source_family": SOURCE_FAMILY_ENS_V1_REVERSE_L1,
            "derivation_kind": DERIVATION_KIND_LEGACY_REVERSE_RESOLVER_HYDRATION,
            "tuple_source": TUPLE_SOURCE_RESOLVER_EDGE_FORWARD_CONFIRMED,
        }),
    }
}

fn resolver_edge_primary_claim_source(key: &PrimaryNameTupleKey, reverse_node: &str) -> Value {
    let reverse_label = key.address.trim_start_matches("0x");
    json!({
        "address": key.address,
        "namespace": key.namespace,
        "coin_type": key.coin_type,
        "reverse_name": format!("{reverse_label}.addr.reverse"),
        "reverse_node": reverse_node,
        "tuple_source": TUPLE_SOURCE_RESOLVER_EDGE_FORWARD_CONFIRMED,
    })
}

fn resolver_edge_hydration_provenance(
    target: &ResolverEdgeHydrationTarget,
    position: &ReverseNameHydrationChainPosition,
) -> Value {
    json!({
        "source_family": SOURCE_FAMILY_ENS_V1_REVERSE_L1,
        "derivation_kind": DERIVATION_KIND_LEGACY_REVERSE_RESOLVER_HYDRATION,
        "tuple_source": TUPLE_SOURCE_RESOLVER_EDGE_FORWARD_CONFIRMED,
        "chain_id": target.chain_id,
        "resolver_address": target.resolver_address,
        "reverse_node": target.reverse_node,
        "block_number": position.block_number,
        "block_hash": position.block_hash,
        "latest_successful_call_block_number": target.latest_successful_call_block_number,
        "latest_successful_call_block_hash": target.latest_successful_call_block_hash,
        "latest_successful_call_transaction_hash": target.latest_successful_call_transaction_hash,
        "latest_successful_call_transaction_index": target.latest_successful_call_transaction_index,
    })
}
