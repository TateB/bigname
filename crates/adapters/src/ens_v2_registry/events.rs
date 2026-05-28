use std::collections::{BTreeMap, HashMap};

use anyhow::Result;
use bigname_manifests::DiscoveryObservation;
use bigname_storage::{NormalizedEvent, SurfaceBinding, SurfaceBindingKind};
use serde_json::json;
use sqlx::types::Uuid;

use super::{
    constants::*,
    discovery::{ens_v2_resolver_discovery_source, ens_v2_subregistry_discovery_source},
    names::{
        closed_surface_binding_for_unregister, deactivate_registry_suffix, name_under_registry,
        observe_name, remember_linked_resource_state, resolve_token_key, state_for_token_mut,
    },
    normalized::normalized_event,
    types::{RegistryNameState, RegistryObservation, RegistryResourceLink},
    util::{deterministic_uuid, normalize_address, null_if_zero_address},
};

pub(super) struct RegistryObservationContext<'a> {
    pub(super) registry_suffix_by_address: &'a mut HashMap<String, String>,
    pub(super) registry_contract_by_address: &'a mut HashMap<String, Uuid>,
    pub(super) states_by_registry_token: &'a mut BTreeMap<(String, String), RegistryNameState>,
    pub(super) linked_resource_states: &'a mut BTreeMap<Uuid, RegistryNameState>,
    pub(super) closed_bindings: &'a mut BTreeMap<Uuid, SurfaceBinding>,
    pub(super) token_aliases: &'a mut HashMap<(String, String), (String, String)>,
    pub(super) observations: &'a mut Vec<DiscoveryObservation>,
    pub(super) graph_events: &'a mut Vec<NormalizedEvent>,
}

pub(super) fn apply_registry_observation(
    observation: RegistryObservation,
    context: &mut RegistryObservationContext<'_>,
) -> Result<()> {
    match observation {
        RegistryObservation::LabelRegistered {
            token_id,
            labelhash,
            label,
            owner,
            expiry,
            sender,
            reference,
        } => {
            let Some(full_name) = name_under_registry(
                &reference.emitting_address,
                &label,
                context.registry_suffix_by_address,
            ) else {
                return Ok(());
            };
            let key = (reference.emitting_address.clone(), token_id.clone());
            let Ok(name) = observe_name(&reference.namespace, &full_name, &reference, &label)
            else {
                return Ok(());
            };
            let state = RegistryNameState {
                token_id,
                labelhash,
                label,
                full_name,
                name,
                owner: Some(owner),
                expiry: Some(expiry),
                status: "registered",
                first_ref: reference.clone(),
                current_ref: reference.clone(),
                registry_address: reference.emitting_address.clone(),
                registry_contract_instance_id: reference.emitting_contract_instance_id,
                source_manifest_id: reference.source_manifest_id,
                source_family: reference.source_family.clone(),
                manifest_version: reference.manifest_version,
                resource: None,
                resolver: None,
                subregistry: None,
                binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            };
            context.graph_events.push(normalized_event(
                &reference,
                Some(state.name.logical_name_id.clone()),
                None,
                EVENT_KIND_REGISTRATION_GRANTED,
                json!({}),
                json!({
                    "source_event": "LabelRegistered",
                    "status": "registered",
                    "token_id": state.token_id,
                    "label": state.label,
                    "labelhash": state.labelhash,
                    "registrant": state.owner,
                    "expiry": expiry,
                    "sender": sender,
                    "registry_contract_instance_id": reference.emitting_contract_instance_id.to_string(),
                    "resource_pending": true,
                }),
                format!("label-registered:{}", state.token_id),
            ));
            context.states_by_registry_token.insert(key, state);
        }
        RegistryObservation::LabelReserved {
            token_id,
            labelhash,
            label,
            expiry,
            sender,
            reference,
        } => {
            let Some(full_name) = name_under_registry(
                &reference.emitting_address,
                &label,
                context.registry_suffix_by_address,
            ) else {
                return Ok(());
            };
            let key = (reference.emitting_address.clone(), token_id.clone());
            let Ok(name) = observe_name(&reference.namespace, &full_name, &reference, &label)
            else {
                return Ok(());
            };
            context.states_by_registry_token.insert(
                key,
                RegistryNameState {
                    token_id: token_id.clone(),
                    labelhash: labelhash.clone(),
                    label,
                    full_name,
                    name,
                    owner: None,
                    expiry: Some(expiry),
                    status: "reserved",
                    first_ref: reference.clone(),
                    current_ref: reference.clone(),
                    registry_address: reference.emitting_address.clone(),
                    registry_contract_instance_id: reference.emitting_contract_instance_id,
                    source_manifest_id: reference.source_manifest_id,
                    source_family: reference.source_family.clone(),
                    manifest_version: reference.manifest_version,
                    resource: None,
                    resolver: None,
                    subregistry: None,
                    binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                },
            );
            context.graph_events.push(normalized_event(
                &reference,
                None,
                None,
                EVENT_KIND_REGISTRATION_RESERVED,
                json!({}),
                json!({
                    "source_event": "LabelReserved",
                    "status": "reserved",
                    "token_id": token_id,
                    "labelhash": labelhash,
                    "expiry": expiry,
                    "sender": sender,
                }),
                format!("label-reserved:{token_id}"),
            ));
        }
        RegistryObservation::LabelUnregistered {
            token_id,
            sender,
            reference,
        } => {
            if let Some(state) = state_for_token_mut(
                context.states_by_registry_token,
                context.token_aliases,
                &reference.emitting_address,
                &token_id,
            ) {
                if let Some(binding) = closed_surface_binding_for_unregister(state, &reference) {
                    context
                        .closed_bindings
                        .insert(binding.surface_binding_id, binding);
                }
                state.status = "unregistered";
                state.current_ref = reference.clone();
                context.graph_events.push(normalized_event(
                    &reference,
                    Some(state.name.logical_name_id.clone()),
                    state.resource.as_ref().map(|link| link.resource_id),
                    EVENT_KIND_REGISTRATION_RELEASED,
                    json!({"status": "registered"}),
                    json!({
                        "source_event": "LabelUnregistered",
                        "status": "unregistered",
                        "token_id": token_id,
                        "sender": sender,
                        "registry_contract_instance_id": reference.emitting_contract_instance_id.to_string(),
                    }),
                    format!("label-unregistered:{token_id}"),
                ));
            }
        }
        RegistryObservation::ExpiryUpdated {
            token_id,
            new_expiry,
            sender,
            reference,
        } => {
            if let Some(state) = state_for_token_mut(
                context.states_by_registry_token,
                context.token_aliases,
                &reference.emitting_address,
                &token_id,
            ) {
                let before_expiry = state.expiry;
                state.expiry = Some(new_expiry);
                state.current_ref = reference.clone();
                remember_linked_resource_state(context.linked_resource_states, state);
                context.graph_events.push(normalized_event(
                    &reference,
                    Some(state.name.logical_name_id.clone()),
                    state.resource.as_ref().map(|link| link.resource_id),
                    EVENT_KIND_EXPIRY_CHANGED,
                    json!({"expiry": before_expiry}),
                    json!({
                        "source_event": "ExpiryUpdated",
                        "token_id": token_id,
                        "expiry": new_expiry,
                        "sender": sender,
                    }),
                    format!("expiry-updated:{token_id}"),
                ));
                context.graph_events.push(normalized_event(
                    &reference,
                    Some(state.name.logical_name_id.clone()),
                    state.resource.as_ref().map(|link| link.resource_id),
                    EVENT_KIND_REGISTRATION_RENEWED,
                    json!({"expiry": before_expiry}),
                    json!({
                        "source_event": "ExpiryUpdated",
                        "token_id": token_id,
                        "expiry": new_expiry,
                        "labelhash": state.labelhash,
                        "registry_contract_instance_id": reference.emitting_contract_instance_id.to_string(),
                    }),
                    format!("registration-renewed:{token_id}"),
                ));
            }
        }
        RegistryObservation::SubregistryUpdated {
            token_id,
            subregistry,
            sender,
            reference,
        } => {
            let mut logical_name_id = None;
            let mut resource_id = None;
            let mut observation_key = format!("{}:{token_id}", reference.emitting_address);
            if let Some(state) = state_for_token_mut(
                context.states_by_registry_token,
                context.token_aliases,
                &reference.emitting_address,
                &token_id,
            ) {
                let before = state.subregistry.clone();
                if before.as_deref() != Some(subregistry.as_str()) {
                    deactivate_registry_suffix(
                        context.registry_suffix_by_address,
                        before.as_deref(),
                        &state.full_name,
                    );
                }
                state.subregistry = Some(subregistry.clone());
                state.current_ref = reference.clone();
                logical_name_id = Some(state.name.logical_name_id.clone());
                resource_id = state.resource.as_ref().map(|link| link.resource_id);
                observation_key = format!("{}:{}", reference.emitting_address, state.name.namehash);
                if subregistry != ZERO_ADDRESS {
                    context
                        .registry_suffix_by_address
                        .insert(subregistry.clone(), state.full_name.clone());
                }
                remember_linked_resource_state(context.linked_resource_states, state);
                context.graph_events.push(normalized_event(
                    &reference,
                    logical_name_id.clone(),
                    resource_id,
                    EVENT_KIND_SUBREGISTRY_CHANGED,
                    json!({"subregistry": before}),
                    json!({
                        "source_event": "SubregistryUpdated",
                        "token_id": token_id,
                        "subregistry": null_if_zero_address(&subregistry),
                        "sender": sender,
                        "from_contract_instance_id": reference.emitting_contract_instance_id.to_string(),
                        "to_contract_instance_id": context.registry_contract_by_address
                            .get(&subregistry)
                            .map(ToString::to_string),
                    }),
                    format!("subregistry-updated:{token_id}"),
                ));
            }
            context.observations.push(DiscoveryObservation {
                chain: reference.chain_id.clone(),
                from_address: reference.emitting_address.clone(),
                to_address: subregistry.clone(),
                edge_kind: SUBREGISTRY_EDGE_KIND.to_owned(),
                discovery_source: ens_v2_subregistry_discovery_source(&reference.chain_id),
                active_from_block_number: Some(reference.block_number),
                active_from_block_hash: Some(reference.block_hash.clone()),
                active_to_block_number: None,
                active_to_block_hash: None,
                provenance: json!({
                    "source": "raw_log",
                    "source_event": "SubregistryUpdated",
                    "observation_key": observation_key,
                    "token_id": token_id,
                    "from_address": reference.emitting_address,
                    "to_address": subregistry,
                    "logical_name_id": logical_name_id,
                    "resource_id": resource_id.map(|value| value.to_string()),
                    "chain_id": reference.chain_id,
                    "block_hash": reference.block_hash,
                    "block_number": reference.block_number,
                    "transaction_hash": reference.transaction_hash,
                    "transaction_index": reference.transaction_index,
                    "log_index": reference.log_index,
                    "tombstone": normalize_address(&subregistry) == ZERO_ADDRESS,
                }),
            });
        }
        RegistryObservation::ResolverUpdated {
            token_id,
            resolver,
            sender,
            reference,
        } => {
            if let Some(state) = state_for_token_mut(
                context.states_by_registry_token,
                context.token_aliases,
                &reference.emitting_address,
                &token_id,
            ) {
                let before = state.resolver.clone();
                state.resolver = Some(resolver.clone());
                state.current_ref = reference.clone();
                remember_linked_resource_state(context.linked_resource_states, state);
                context.graph_events.push(normalized_event(
                    &reference,
                    Some(state.name.logical_name_id.clone()),
                    state.resource.as_ref().map(|link| link.resource_id),
                    EVENT_KIND_RESOLVER_CHANGED,
                    json!({"resolver": before}),
                    json!({
                        "source_event": "ResolverUpdated",
                        "token_id": token_id,
                        "resolver": null_if_zero_address(&resolver),
                        "sender": sender,
                    }),
                    format!("resolver-updated:{token_id}"),
                ));
                context.observations.push(DiscoveryObservation {
                    chain: reference.chain_id.clone(),
                    from_address: reference.emitting_address.clone(),
                    to_address: resolver.clone(),
                    edge_kind: RESOLVER_EDGE_KIND.to_owned(),
                    discovery_source: ens_v2_resolver_discovery_source(&reference.chain_id),
                    active_from_block_number: Some(reference.block_number),
                    active_from_block_hash: Some(reference.block_hash.clone()),
                    active_to_block_number: None,
                    active_to_block_hash: None,
                    provenance: json!({
                        "source": "raw_log",
                        "source_event": "ResolverUpdated",
                        "observation_key": format!("resolver:{}:{}", reference.emitting_address, state.name.namehash),
                        "token_id": token_id,
                        "from_address": reference.emitting_address,
                        "to_address": resolver.clone(),
                        "logical_name_id": state.name.logical_name_id,
                        "resource_id": state.resource.as_ref().map(|link| link.resource_id.to_string()),
                        "chain_id": reference.chain_id,
                        "block_hash": reference.block_hash,
                        "block_number": reference.block_number,
                        "transaction_hash": reference.transaction_hash,
                        "transaction_index": reference.transaction_index,
                        "log_index": reference.log_index,
                        "tombstone": normalize_address(&resolver) == ZERO_ADDRESS,
                    }),
                });
            }
        }
        RegistryObservation::TokenResource {
            token_id,
            upstream_resource,
            reference,
        } => {
            if let Some(state) = state_for_token_mut(
                context.states_by_registry_token,
                context.token_aliases,
                &reference.emitting_address,
                &token_id,
            ) {
                let resource_id = deterministic_uuid(&format!(
                    "ens-v2-resource:{}:{}:{}",
                    reference.chain_id, reference.emitting_contract_instance_id, upstream_resource
                ));
                let token_lineage_id = deterministic_uuid(&format!(
                    "ens-v2-token-lineage:{}:{}:{}",
                    reference.chain_id, reference.emitting_contract_instance_id, upstream_resource
                ));
                let surface_binding_id = deterministic_uuid(&format!(
                    "ens-v2-surface-binding:{}:{}:{}:{}",
                    reference.chain_id,
                    reference.emitting_contract_instance_id,
                    upstream_resource,
                    state.name.logical_name_id
                ));
                state.resource = Some(RegistryResourceLink {
                    upstream_resource,
                    observed_token_id: token_id.clone(),
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                    linked_ref: reference.clone(),
                });
                state.current_ref = reference;
                remember_linked_resource_state(context.linked_resource_states, state);
            }
        }
        RegistryObservation::TokenRegenerated {
            old_token_id,
            new_token_id,
            reference,
        } => {
            let canonical_key = resolve_token_key(
                context.token_aliases,
                &reference.emitting_address,
                &old_token_id,
            )
            .unwrap_or_else(|| (reference.emitting_address.clone(), old_token_id.clone()));
            if let Some(state) = context.states_by_registry_token.get_mut(&canonical_key) {
                let previous_token_id = state.token_id.clone();
                state.token_id = new_token_id.clone();
                state.current_ref = reference.clone();
                remember_linked_resource_state(context.linked_resource_states, state);
                context.token_aliases.insert(
                    (reference.emitting_address.clone(), old_token_id.clone()),
                    canonical_key.clone(),
                );
                context.token_aliases.insert(
                    (reference.emitting_address.clone(), new_token_id.clone()),
                    canonical_key,
                );
                context.graph_events.push(normalized_event(
                    &reference,
                    Some(state.name.logical_name_id.clone()),
                    state.resource.as_ref().map(|link| link.resource_id),
                    EVENT_KIND_TOKEN_REGENERATED,
                    json!({"token_id": previous_token_id}),
                    json!({
                        "source_event": "TokenRegenerated",
                        "old_token_id": old_token_id,
                        "new_token_id": new_token_id,
                        "resource_id": state.resource.as_ref().map(|link| link.resource_id.to_string()),
                    }),
                    format!("token-regenerated:{old_token_id}:{new_token_id}"),
                ));
            }
        }
        RegistryObservation::ParentUpdated {
            parent,
            label,
            sender,
            reference,
        } => {
            if let Some(full_name) =
                name_under_registry(&parent, &label, context.registry_suffix_by_address)
            {
                context
                    .registry_suffix_by_address
                    .insert(reference.emitting_address.clone(), full_name.clone());
                context.graph_events.push(normalized_event(
                    &reference,
                    None,
                    None,
                    EVENT_KIND_PARENT_CHANGED,
                    json!({}),
                    json!({
                        "source_event": "ParentUpdated",
                        "parent": null_if_zero_address(&parent),
                        "label": label,
                        "registry_name": full_name,
                        "sender": sender,
                        "registry_contract_instance_id": reference.emitting_contract_instance_id.to_string(),
                        "parent_contract_instance_id": context.registry_contract_by_address
                            .get(&parent)
                            .map(ToString::to_string),
                    }),
                    format!("parent-updated:{}", reference.emitting_address),
                ));
            }
        }
    }

    Ok(())
}
