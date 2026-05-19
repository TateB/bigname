use anyhow::{Context, Result};
use bigname_storage::NormalizedEvent;
use serde_json::json;

use super::{
    assignment::{ObservedRegistryAssignment, RegistryDiscoveryKind},
    hex_topic::{ZERO_ADDRESS, hex_string, null_if_zero_address},
    loader::ActiveRegistryEdge,
};

pub(super) fn build_registry_changed_event(
    assignment: &ObservedRegistryAssignment,
    active_edge: Option<&ActiveRegistryEdge>,
) -> Result<Option<NormalizedEvent>> {
    if assignment.to_address != ZERO_ADDRESS && active_edge.is_none() {
        return Ok(None);
    }

    let after_state = match assignment.discovery_kind {
        RegistryDiscoveryKind::Subregistry => {
            build_subregistry_after_state(assignment, active_edge)?
        }
        RegistryDiscoveryKind::Resolver => build_resolver_after_state(assignment, active_edge)?,
    };
    Ok(Some(NormalizedEvent {
        event_identity: format!(
            "{}:{}:{}:{}:{}:{}",
            assignment.discovery_kind.derivation_kind(),
            assignment.raw_log.source_manifest_id,
            assignment.raw_log.block_hash,
            assignment.raw_log.transaction_hash,
            assignment.raw_log.log_index,
            assignment.raw_log.emitting_address
        ),
        namespace: assignment.raw_log.namespace.clone(),
        logical_name_id: None,
        resource_id: None,
        event_kind: assignment.discovery_kind.event_kind().to_owned(),
        source_family: assignment.raw_log.source_family.clone(),
        manifest_version: assignment.raw_log.manifest_version,
        source_manifest_id: Some(assignment.raw_log.source_manifest_id),
        chain_id: Some(assignment.raw_log.chain_id.clone()),
        block_number: Some(assignment.raw_log.block_number),
        block_hash: Some(assignment.raw_log.block_hash.clone()),
        transaction_hash: Some(assignment.raw_log.transaction_hash.clone()),
        log_index: Some(assignment.raw_log.log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": assignment.raw_log.chain_id,
            "block_hash": assignment.raw_log.block_hash,
            "block_number": assignment.raw_log.block_number,
            "transaction_hash": assignment.raw_log.transaction_hash,
            "transaction_index": assignment.raw_log.transaction_index,
            "log_index": assignment.raw_log.log_index,
            "emitting_address": assignment.raw_log.emitting_address,
            "topic0": assignment.raw_log.topics.first().cloned(),
            "topic1": assignment.raw_log.topics.get(1).cloned(),
            "topic2": assignment.raw_log.topics.get(2).cloned(),
            "data_hex": hex_string(&assignment.raw_log.data),
        }),
        derivation_kind: assignment.discovery_kind.derivation_kind().to_owned(),
        canonicality_state: assignment.raw_log.canonicality_state,
        before_state: json!({}),
        after_state,
    }))
}

fn build_subregistry_after_state(
    assignment: &ObservedRegistryAssignment,
    active_edge: Option<&ActiveRegistryEdge>,
) -> Result<serde_json::Value> {
    let parent_node = assignment
        .parent_node
        .as_deref()
        .context("ENSv1 subregistry observation is missing provenance.parent_node")?;
    let labelhash = assignment
        .labelhash
        .as_deref()
        .context("ENSv1 subregistry observation is missing provenance.labelhash")?;
    let child_node = assignment.observation_key.as_str();
    let owner = assignment.to_address.as_str();
    let tombstone = assignment.to_address == ZERO_ADDRESS;

    Ok(json!({
        "source_event": assignment.discovery_kind.source_event(),
        "discovery_source": assignment.discovery_source,
        "edge_kind": assignment.discovery_kind.edge_kind(),
        "observation_key": assignment.observation_key,
        "parent_node": parent_node,
        "labelhash": labelhash,
        "child_node": child_node,
        "emitting_address": assignment.raw_log.emitting_address,
        "owner": owner,
        "tombstone": tombstone,
        "from_contract_instance_id": active_edge
            .map(|edge| edge.from_contract_instance_id.to_string())
            .unwrap_or_else(|| assignment.raw_log.emitting_contract_instance_id.to_string()),
        "to_contract_instance_id": active_edge.map(|edge| edge.to_contract_instance_id.to_string()),
        "active_edge": !tombstone && active_edge.is_some(),
    }))
}

fn build_resolver_after_state(
    assignment: &ObservedRegistryAssignment,
    active_edge: Option<&ActiveRegistryEdge>,
) -> Result<serde_json::Value> {
    let node = assignment
        .node
        .as_deref()
        .context("ENSv1 resolver observation is missing provenance.node")?;
    let resolver = assignment.to_address.as_str();
    let tombstone = assignment.to_address == ZERO_ADDRESS;

    Ok(json!({
        "source_event": assignment.discovery_kind.source_event(),
        "discovery_source": assignment.discovery_source,
        "edge_kind": assignment.discovery_kind.edge_kind(),
        "observation_key": assignment.observation_key,
        "node": node,
        "emitting_address": assignment.raw_log.emitting_address,
        "resolver": null_if_zero_address(resolver),
        "raw_resolver": resolver,
        "tombstone": tombstone,
        "from_contract_instance_id": active_edge
            .map(|edge| edge.from_contract_instance_id.to_string())
            .unwrap_or_else(|| assignment.raw_log.emitting_contract_instance_id.to_string()),
        "to_contract_instance_id": active_edge.map(|edge| edge.to_contract_instance_id.to_string()),
        "active_edge": !tombstone && active_edge.is_some(),
        "resolver_profile_supported": false,
        "resolver_profile_status": "unsupported",
        "resolver_profile_reason": "registry_resolver_discovery_does_not_admit_typed_resolver_profile",
    }))
}
