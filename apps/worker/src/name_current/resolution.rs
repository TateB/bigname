use anyhow::Result;
use bigname_storage::SurfaceBindingKind;
use serde_json::{Value, json};
use uuid::Uuid;

use super::json::{format_timestamp, json_str, normalize_resolver_address};
use super::types::{
    BasenamesExecutionManifestVersion, CurrentBindingContext, NameSurfaceSeed, ProjectedFacts,
    RelevantEvent, SupportedResolutionProjection, WildcardSourceContext,
};
use super::{
    BASE_MAINNET_CHAIN_ID, BASENAMES_L1_RESOLVER_ADDRESS, BASENAMES_NAMESPACE, ENS_NAMESPACE,
    ETHEREUM_MAINNET_CHAIN_ID, EVENT_KIND_ALIAS_CHANGED, EVENT_KIND_RECORD_VERSION_CHANGED,
    EVENT_KIND_RESOLVER_CHANGED, SOURCE_FAMILY_BASENAMES_EXECUTION,
};

pub(super) fn build_supported_resolution_projection(
    name: &NameSurfaceSeed,
    current_binding: Option<&CurrentBindingContext>,
    facts: &ProjectedFacts,
    events: &[RelevantEvent],
    chain_positions: &Value,
    wildcard_source_context: Option<&WildcardSourceContext>,
    basenames_execution_manifest: Option<&BasenamesExecutionManifestVersion>,
) -> Result<Option<SupportedResolutionProjection>> {
    let Some(binding) = current_binding else {
        return Ok(None);
    };

    match name.namespace.as_str() {
        ENS_NAMESPACE => match binding.binding_kind {
            SurfaceBindingKind::ResolverAliasPath => {
                build_alias_only_supported_projection(name, binding, facts, events, chain_positions)
            }
            SurfaceBindingKind::ObservedWildcardPath => {
                build_wildcard_supported_projection(name, binding, wildcard_source_context)
            }
            _ => Ok(None),
        },
        BASENAMES_NAMESPACE => build_basenames_supported_projection(
            name,
            binding,
            facts,
            events,
            chain_positions,
            basenames_execution_manifest,
        ),
        _ => Ok(None),
    }
}

fn build_alias_only_supported_projection(
    name: &NameSurfaceSeed,
    current_binding: &CurrentBindingContext,
    facts: &ProjectedFacts,
    events: &[RelevantEvent],
    chain_positions: &Value,
) -> Result<Option<SupportedResolutionProjection>> {
    let Some(final_target) = events
        .iter()
        .rev()
        .find(|event| {
            event.event_kind == EVENT_KIND_ALIAS_CHANGED
                && event.after_state.get("active").and_then(Value::as_bool) == Some(true)
        })
        .and_then(alias_final_target_ref)
    else {
        return Ok(None);
    };
    let Some(resolver_hop) = resolver_hop_from_facts(
        &name.logical_name_id,
        &name.namespace,
        &name.normalized_name,
        &name.canonical_display_name,
        current_binding.resource_id,
        facts,
    ) else {
        return Ok(None);
    };
    let Some(boundary) = build_supported_resolution_boundary_from_chain_positions(
        chain_positions,
        &name.logical_name_id,
        current_binding.resource_id,
        None,
    ) else {
        return Ok(None);
    };

    Ok(Some(SupportedResolutionProjection {
        topology: json!({
            "registry_path": [name_ref(
                &name.logical_name_id,
                &name.namespace,
                &name.normalized_name,
                &name.canonical_display_name,
                &name.namehash,
                current_binding.resource_id,
                SurfaceBindingKind::ResolverAliasPath,
            )],
            "subregistry_path": [],
            "resolver_path": [resolver_hop],
            "wildcard": empty_wildcard_detail(),
            "alias": {
                "final_target": final_target.clone(),
                "hops": [final_target],
            },
            "version_boundaries": {
                "topology_version_boundary": boundary.clone(),
                "record_version_boundary": boundary,
            },
            "transport": empty_transport_detail(),
        }),
        manifest_versions: Vec::new(),
    }))
}

fn build_wildcard_supported_projection(
    name: &NameSurfaceSeed,
    current_binding: &CurrentBindingContext,
    wildcard_source_context: Option<&WildcardSourceContext>,
) -> Result<Option<SupportedResolutionProjection>> {
    let Some(source_context) = wildcard_source_context else {
        return Ok(None);
    };
    let Some(boundary) = build_supported_resolution_boundary_from_event(
        &source_context.logical_name_id,
        source_context.resource_id,
        &source_context.boundary_event,
    ) else {
        return Ok(None);
    };
    let Some(resolver_hop) = resolver_hop_from_event(source_context) else {
        return Ok(None);
    };
    let source = wildcard_source_ref(source_context);
    let matched_labels = source_context
        .matched_labels
        .iter()
        .map(|label| Value::String(label.clone()))
        .collect::<Vec<_>>();

    Ok(Some(SupportedResolutionProjection {
        topology: json!({
            "registry_path": [name_ref(
                &name.logical_name_id,
                &name.namespace,
                &name.normalized_name,
                &name.canonical_display_name,
                &name.namehash,
                current_binding.resource_id,
                SurfaceBindingKind::ObservedWildcardPath,
            )],
            "subregistry_path": [],
            "resolver_path": [resolver_hop],
            "wildcard": {
                "source": source,
                "matched_labels": matched_labels,
            },
            "alias": empty_alias_detail(),
            "version_boundaries": {
                "topology_version_boundary": boundary.clone(),
                "record_version_boundary": boundary,
            },
            "transport": empty_transport_detail(),
        }),
        manifest_versions: Vec::new(),
    }))
}

fn build_basenames_supported_projection(
    name: &NameSurfaceSeed,
    current_binding: &CurrentBindingContext,
    facts: &ProjectedFacts,
    events: &[RelevantEvent],
    chain_positions: &Value,
    basenames_execution_manifest: Option<&BasenamesExecutionManifestVersion>,
) -> Result<Option<SupportedResolutionProjection>> {
    if current_binding.binding_kind != SurfaceBindingKind::DeclaredRegistryPath {
        return Ok(None);
    }
    let Some(manifest) = basenames_execution_manifest else {
        return Ok(None);
    };
    if manifest.chain != ETHEREUM_MAINNET_CHAIN_ID
        || !manifest
            .contract_address
            .eq_ignore_ascii_case(BASENAMES_L1_RESOLVER_ADDRESS)
        || !chain_positions_include_chain(chain_positions, BASE_MAINNET_CHAIN_ID)
        || !chain_positions_include_chain(chain_positions, ETHEREUM_MAINNET_CHAIN_ID)
    {
        return Ok(None);
    }
    let Some(resolver_hop) = resolver_hop_from_facts(
        &name.logical_name_id,
        &name.namespace,
        &name.normalized_name,
        &name.canonical_display_name,
        current_binding.resource_id,
        facts,
    ) else {
        return Ok(None);
    };
    if facts.resolver_chain_id.as_deref() != Some(BASE_MAINNET_CHAIN_ID) {
        return Ok(None);
    }
    let Some(boundary) = build_basenames_supported_boundary(
        &name.logical_name_id,
        current_binding.resource_id,
        events,
    ) else {
        return Ok(None);
    };

    Ok(Some(SupportedResolutionProjection {
        topology: json!({
            "registry_path": [name_ref(
                &name.logical_name_id,
                &name.namespace,
                &name.normalized_name,
                &name.canonical_display_name,
                &name.namehash,
                current_binding.resource_id,
                SurfaceBindingKind::DeclaredRegistryPath,
            )],
            "subregistry_path": [],
            "resolver_path": [resolver_hop],
            "wildcard": empty_wildcard_detail(),
            "alias": empty_alias_detail(),
            "version_boundaries": {
                "topology_version_boundary": boundary.clone(),
                "record_version_boundary": boundary,
            },
            "transport": {
                "source_chain_id": BASE_MAINNET_CHAIN_ID,
                "target_chain_id": ETHEREUM_MAINNET_CHAIN_ID,
                "contract_address": BASENAMES_L1_RESOLVER_ADDRESS,
                "latest_event_kind": Value::Null,
            },
        }),
        manifest_versions: vec![basenames_execution_manifest_value(manifest)],
    }))
}

fn basenames_execution_manifest_value(manifest: &BasenamesExecutionManifestVersion) -> Value {
    json!({
        "source_family": SOURCE_FAMILY_BASENAMES_EXECUTION,
        "manifest_version": manifest.manifest_version,
        "chain": manifest.chain,
        "deployment_epoch": manifest.deployment_epoch,
    })
}

fn build_basenames_supported_boundary(
    logical_name_id: &str,
    resource_id: Uuid,
    events: &[RelevantEvent],
) -> Option<Value> {
    let boundary_anchor = events.iter().rev().find(|event| {
        event.resource_id == Some(resource_id)
            && event.chain_id.as_deref() == Some(BASE_MAINNET_CHAIN_ID)
            && matches!(
                event.event_kind.as_str(),
                EVENT_KIND_RECORD_VERSION_CHANGED | EVENT_KIND_RESOLVER_CHANGED
            )
    })?;
    let chain_position = relevant_event_chain_position(boundary_anchor)?;
    let has_pointer = boundary_anchor.event_kind == EVENT_KIND_RECORD_VERSION_CHANGED;

    Some(json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id.to_string(),
        "normalized_event_id": has_pointer.then_some(boundary_anchor.normalized_event_id),
        "event_kind": has_pointer.then_some(boundary_anchor.event_kind.clone()),
        "chain_position": chain_position,
    }))
}

fn build_supported_resolution_boundary_from_chain_positions(
    chain_positions: &Value,
    logical_name_id: &str,
    resource_id: Uuid,
    preferred_chain_id: Option<&str>,
) -> Option<Value> {
    let chain_position = preferred_chain_id
        .and_then(|chain_id| chain_position_for_chain(chain_positions, chain_id))
        .or_else(|| chain_position_slot(chain_positions, "ethereum"))
        .or_else(|| only_chain_position(chain_positions))?;

    Some(json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id.to_string(),
        "normalized_event_id": Value::Null,
        "event_kind": Value::Null,
        "chain_position": chain_position,
    }))
}

fn build_supported_resolution_boundary_from_event(
    logical_name_id: &str,
    resource_id: Uuid,
    event: &RelevantEvent,
) -> Option<Value> {
    let chain_position = relevant_event_chain_position(event)?;
    let has_pointer = event.event_kind == EVENT_KIND_RECORD_VERSION_CHANGED;

    Some(json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id.to_string(),
        "normalized_event_id": has_pointer.then_some(event.normalized_event_id),
        "event_kind": has_pointer.then_some(event.event_kind.clone()),
        "chain_position": chain_position,
    }))
}

fn alias_final_target_ref(event: &RelevantEvent) -> Option<Value> {
    let logical_name_id = json_str(&event.after_state, &["to_logical_name_id"]).or_else(|| {
        json_str(&event.after_state, &["to_name"])
            .and_then(|name| bigname_domain::normalization::normalize_name(&name).ok())
            .map(|name| format!("{ENS_NAMESPACE}:{}", name.normalized_name))
    })?;
    let normalized_name = json_str(&event.after_state, &["to_normalized_name"]).or_else(|| {
        json_str(&event.after_state, &["to_name"])
            .and_then(|name| bigname_domain::normalization::normalize_name(&name).ok())
            .map(|name| name.normalized_name)
    })?;
    let canonical_display_name = json_str(&event.after_state, &["to_canonical_display_name"])
        .or_else(|| json_str(&event.after_state, &["to_name"]))?;
    let namehash = json_str(&event.after_state, &["to_namehash"])?;
    let resource_id = json_str(&event.after_state, &["to_resource_id"])?;

    Some(json!({
        "logical_name_id": logical_name_id,
        "namespace": ENS_NAMESPACE,
        "normalized_name": normalized_name,
        "canonical_display_name": canonical_display_name,
        "namehash": namehash,
        "resource_id": resource_id,
        "binding_kind": SurfaceBindingKind::ResolverAliasPath.as_str(),
    }))
}

fn wildcard_source_ref(source_context: &WildcardSourceContext) -> Value {
    name_ref(
        &source_context.logical_name_id,
        &source_context.namespace,
        &source_context.normalized_name,
        &source_context.canonical_display_name,
        &source_context.namehash,
        source_context.resource_id,
        SurfaceBindingKind::ObservedWildcardPath,
    )
}

fn resolver_hop_from_event(source_context: &WildcardSourceContext) -> Option<Value> {
    Some(json!({
        "logical_name_id": source_context.logical_name_id,
        "namespace": source_context.namespace,
        "normalized_name": source_context.normalized_name,
        "canonical_display_name": source_context.canonical_display_name,
        "resource_id": source_context.resource_id.to_string(),
        "chain_id": source_context.resolver_event.chain_id.as_ref()?,
        "address": normalize_resolver_address(json_str(&source_context.resolver_event.after_state, &["resolver"]).as_deref())?,
        "latest_event_kind": source_context.resolver_event.event_kind,
    }))
}

fn resolver_hop_from_facts(
    logical_name_id: &str,
    namespace: &str,
    normalized_name: &str,
    canonical_display_name: &str,
    resource_id: Uuid,
    facts: &ProjectedFacts,
) -> Option<Value> {
    Some(json!({
        "logical_name_id": logical_name_id,
        "namespace": namespace,
        "normalized_name": normalized_name,
        "canonical_display_name": canonical_display_name,
        "resource_id": resource_id.to_string(),
        "chain_id": facts.resolver_chain_id.as_ref()?,
        "address": facts.resolver_address.as_ref()?,
        "latest_event_kind": facts.latest_resolver_event_kind.clone(),
    }))
}

fn name_ref(
    logical_name_id: &str,
    namespace: &str,
    normalized_name: &str,
    canonical_display_name: &str,
    namehash: &str,
    resource_id: Uuid,
    binding_kind: SurfaceBindingKind,
) -> Value {
    json!({
        "logical_name_id": logical_name_id,
        "namespace": namespace,
        "normalized_name": normalized_name,
        "canonical_display_name": canonical_display_name,
        "namehash": namehash,
        "resource_id": resource_id.to_string(),
        "binding_kind": binding_kind.as_str(),
    })
}

pub(super) fn empty_alias_detail() -> Value {
    json!({
        "final_target": Value::Null,
        "hops": [],
    })
}

pub(super) fn empty_wildcard_detail() -> Value {
    json!({
        "source": Value::Null,
        "matched_labels": [],
    })
}

pub(super) fn empty_transport_detail() -> Value {
    json!({
        "source_chain_id": Value::Null,
        "target_chain_id": Value::Null,
        "contract_address": Value::Null,
        "latest_event_kind": Value::Null,
    })
}

fn chain_positions_include_chain(chain_positions: &Value, chain_id: &str) -> bool {
    chain_position_for_chain(chain_positions, chain_id).is_some()
}

fn chain_position_for_chain(chain_positions: &Value, chain_id: &str) -> Option<Value> {
    chain_positions
        .as_object()?
        .values()
        .find(|position| {
            position
                .get("chain_id")
                .and_then(Value::as_str)
                .is_some_and(|value| value == chain_id)
        })
        .cloned()
}

fn chain_position_slot(chain_positions: &Value, slot: &str) -> Option<Value> {
    chain_positions.as_object()?.get(slot).cloned()
}

fn only_chain_position(chain_positions: &Value) -> Option<Value> {
    let positions = chain_positions.as_object()?;
    if positions.len() == 1 {
        positions.values().next().cloned()
    } else {
        None
    }
}

pub(super) fn relevant_event_chain_position(event: &RelevantEvent) -> Option<Value> {
    Some(json!({
        "chain_id": event.chain_id.as_ref()?,
        "block_number": event.block_number?,
        "block_hash": event.block_hash.as_ref()?,
        "timestamp": format_timestamp(event.block_timestamp?),
    }))
}
