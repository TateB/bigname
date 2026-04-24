use std::collections::HashMap;

use anyhow::Result;
use bigname_manifests::{
    ManifestCodeHashObservation, ManifestDeclaredContractDriftInput, ManifestDriftInputs,
    ManifestProxyImplementationDriftEdge,
};
use bigname_storage::NormalizedEvent;
use serde_json::json;

use super::constants::{
    DERIVATION_KIND_MANIFEST_ALERT, EVENT_KIND_MANIFEST_CODE_HASH_DRIFT_ALERT,
    EVENT_KIND_MANIFEST_PROXY_IMPLEMENTATION_ALERT,
};
use super::utils::{
    canonicality_state_from_view, code_hash_observation_key, event_identity, manifest_version_i64,
    watched_contract_source_name,
};

pub(super) fn build_code_hash_drift_alert_events(
    drift_inputs: &ManifestDriftInputs,
) -> Result<Vec<NormalizedEvent>> {
    let observations = drift_inputs
        .code_hash_observations
        .iter()
        .map(|observation| {
            (
                code_hash_observation_key(
                    &observation.chain,
                    observation.contract_instance_id,
                    &observation.address,
                ),
                observation,
            )
        })
        .collect::<HashMap<_, _>>();

    let mut events = Vec::new();
    for declared_contract in &drift_inputs.declared_contracts {
        let Some(expected_code_hash) = declared_contract.code_hash.as_ref() else {
            continue;
        };
        let Some(observation) = observations.get(&code_hash_observation_key(
            &declared_contract.chain,
            declared_contract.contract_instance_id,
            &declared_contract.declared_address,
        )) else {
            continue;
        };
        if expected_code_hash.eq_ignore_ascii_case(&observation.code_hash) {
            continue;
        }
        events.push(build_code_hash_drift_alert_event(
            declared_contract,
            observation,
            expected_code_hash,
        )?);
    }

    Ok(events)
}

fn build_code_hash_drift_alert_event(
    declared_contract: &ManifestDeclaredContractDriftInput,
    observation: &ManifestCodeHashObservation,
    expected_code_hash: &str,
) -> Result<NormalizedEvent> {
    let canonicality_state = canonicality_state_from_view(&observation.canonicality_state)?;
    let contract_instance_id = declared_contract.contract_instance_id.to_string();
    let source_manifest_id = declared_contract.manifest_id;
    let namespace = declared_contract.namespace.clone();
    let source_family = declared_contract.source_family.clone();
    let chain = declared_contract.chain.clone();
    let address = declared_contract.declared_address.clone();

    Ok(NormalizedEvent {
        event_identity: event_identity(
            "manifest_alert:code_hash_drift",
            json!([
                source_manifest_id,
                declared_contract.declaration_kind,
                declared_contract.declaration_name,
                contract_instance_id,
                address,
                expected_code_hash,
                observation.code_hash,
                observation.block_hash,
            ]),
        )?,
        namespace,
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_MANIFEST_CODE_HASH_DRIFT_ALERT.to_owned(),
        source_family,
        manifest_version: manifest_version_i64(declared_contract.manifest_version)?,
        source_manifest_id: Some(source_manifest_id),
        chain_id: Some(chain.clone()),
        block_number: Some(observation.block_number),
        block_hash: Some(observation.block_hash.clone()),
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({
            "manifest_id": source_manifest_id,
            "declaration_kind": declared_contract.declaration_kind,
            "declaration_name": declared_contract.declaration_name,
            "contract_instance_id": contract_instance_id,
            "address": address,
            "observed_block_number": observation.block_number,
            "observed_block_hash": observation.block_hash,
        }),
        derivation_kind: DERIVATION_KIND_MANIFEST_ALERT.to_owned(),
        canonicality_state,
        before_state: json!({}),
        after_state: json!({
            "alert_type": "manifest_code_hash_drift",
            "alert_status": "active",
            "chain": chain,
            "source_family": declared_contract.source_family,
            "declaration_kind": declared_contract.declaration_kind,
            "declaration_name": declared_contract.declaration_name,
            "contract_instance_id": contract_instance_id,
            "address": declared_contract.declared_address,
            "expected_code_hash": expected_code_hash,
            "observed_code_hash": observation.code_hash,
            "observed_code_byte_length": observation.code_byte_length,
            "observed_block_number": observation.block_number,
            "observed_block_hash": observation.block_hash,
            "observed_canonicality_state": observation.canonicality_state,
            "watched_source": watched_contract_source_name(observation),
            "source_manifest_id": observation.source_manifest_id,
        }),
    })
}

pub(super) fn build_proxy_implementation_alert_event(
    edge: &ManifestProxyImplementationDriftEdge,
) -> Result<NormalizedEvent> {
    let proxy_contract_instance_id = edge.proxy_contract_instance_id.to_string();
    let implementation_contract_instance_id = edge.implementation_contract_instance_id.to_string();

    Ok(NormalizedEvent {
        event_identity: event_identity(
            "manifest_alert:proxy_implementation",
            json!([
                edge.source_manifest_id,
                edge.discovery_edge_id,
                proxy_contract_instance_id,
                edge.proxy_address,
                implementation_contract_instance_id,
                edge.implementation_address,
            ]),
        )?,
        namespace: edge.namespace.clone(),
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_MANIFEST_PROXY_IMPLEMENTATION_ALERT.to_owned(),
        source_family: edge.source_family.clone(),
        manifest_version: manifest_version_i64(edge.manifest_version)?,
        source_manifest_id: Some(edge.source_manifest_id),
        chain_id: Some(edge.chain.clone()),
        block_number: None,
        block_hash: None,
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({
            "manifest_id": edge.source_manifest_id,
            "discovery_edge_id": edge.discovery_edge_id,
            "proxy_contract_instance_id": proxy_contract_instance_id,
            "implementation_contract_instance_id": implementation_contract_instance_id,
        }),
        derivation_kind: DERIVATION_KIND_MANIFEST_ALERT.to_owned(),
        canonicality_state: bigname_storage::CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "alert_type": "manifest_proxy_implementation_edge",
            "alert_status": "active",
            "chain": edge.chain,
            "source_family": edge.source_family,
            "proxy_contract_instance_id": edge.proxy_contract_instance_id.to_string(),
            "proxy_address": edge.proxy_address,
            "implementation_contract_instance_id": edge.implementation_contract_instance_id.to_string(),
            "implementation_address": edge.implementation_address,
            "declaration_name": edge.declaration_name,
            "role": edge.role,
            "proxy_kind": edge.proxy_kind,
            "admission": edge.admission,
            "active_from_block_number": edge.active_from_block_number,
            "active_to_block_number": edge.active_to_block_number,
            "provenance": edge.provenance,
        }),
    })
}
