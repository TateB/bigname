use std::collections::{BTreeMap, HashMap};

use anyhow::Result;
use bigname_domain::normalization::{ENS_NORMALIZER_VERSION, normalize_name};
use bigname_manifests::WatchedContractSource;
use bigname_storage::SurfaceBinding;
use serde_json::json;
use sqlx::types::Uuid;

use super::{
    constants::*,
    types::{ActiveEmitter, NameMetadata, ObservationRef, RegistryNameState},
    util::{dns_encode, event_position_timestamp, hex_string, keccak256_bytes, namehash_bytes},
};

pub(super) fn initial_registry_suffixes(emitters: &[ActiveEmitter]) -> HashMap<String, String> {
    let mut suffixes = HashMap::new();
    for emitter in emitters {
        if emitter.source_family == SOURCE_FAMILY_ENS_V2_ROOT_L1 {
            suffixes.insert(emitter.address.clone(), String::new());
        } else if emitter.source_family == SOURCE_FAMILY_ENS_V2_REGISTRY_L1
            && emitter.source != WatchedContractSource::DiscoveryEdge
        {
            suffixes.insert(emitter.address.clone(), "eth".to_owned());
        }
    }
    suffixes
}

pub(super) fn name_under_registry(
    registry_address: &str,
    label: &str,
    registry_suffix_by_address: &HashMap<String, String>,
) -> Option<String> {
    if label.is_empty() || label.contains('.') {
        return None;
    }
    let suffix = registry_suffix_by_address.get(registry_address)?;
    if suffix.is_empty() {
        Some(label.to_owned())
    } else {
        Some(format!("{label}.{suffix}"))
    }
}

pub(super) fn observe_name(
    namespace: &str,
    full_name: &str,
    _reference: &ObservationRef,
    _label: &str,
) -> Result<NameMetadata> {
    let normalized = normalize_name(full_name)?;
    let labels = normalized
        .normalized_labels
        .iter()
        .map(|label| label.as_bytes().to_vec())
        .collect::<Vec<_>>();
    let dns_encoded_name = dns_encode(&labels)?;
    let labelhashes = labels
        .iter()
        .map(|label| format!("0x{}", hex_string(keccak256_bytes(label))))
        .collect::<Vec<_>>();
    Ok(NameMetadata {
        namespace: namespace.to_owned(),
        logical_name_id: format!("{namespace}:{}", normalized.normalized_name),
        input_name: normalized.input_name,
        canonical_display_name: normalized.canonical_display_name,
        normalized_name: normalized.normalized_name,
        dns_encoded_name,
        namehash: format!("0x{}", hex_string(namehash_bytes(&labels))),
        labelhashes,
        normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
    })
}

pub(super) fn state_for_token_mut<'a>(
    states: &'a mut BTreeMap<(String, String), RegistryNameState>,
    aliases: &HashMap<(String, String), (String, String)>,
    registry: &str,
    token_id: &str,
) -> Option<&'a mut RegistryNameState> {
    let key = resolve_token_key(aliases, registry, token_id)
        .unwrap_or_else(|| (registry.to_owned(), token_id.to_owned()));
    states.get_mut(&key)
}

pub(super) fn resolve_token_key(
    aliases: &HashMap<(String, String), (String, String)>,
    registry: &str,
    token_id: &str,
) -> Option<(String, String)> {
    aliases
        .get(&(registry.to_owned(), token_id.to_owned()))
        .cloned()
}

pub(super) fn remember_linked_resource_state(
    linked_resource_states: &mut BTreeMap<Uuid, RegistryNameState>,
    state: &RegistryNameState,
) {
    if let Some(link) = state.resource.as_ref() {
        linked_resource_states.insert(link.resource_id, state.clone());
    }
}

pub(super) fn closed_surface_binding_for_unregister(
    state: &RegistryNameState,
    reference: &ObservationRef,
) -> Option<SurfaceBinding> {
    let link = state.resource.as_ref()?;
    Some(SurfaceBinding {
        surface_binding_id: link.surface_binding_id,
        logical_name_id: state.name.logical_name_id.clone(),
        resource_id: link.resource_id,
        binding_kind: state.binding_kind,
        active_from: event_position_timestamp(&link.linked_ref),
        active_to: Some(event_position_timestamp(reference)),
        chain_id: link.linked_ref.chain_id.clone(),
        block_hash: link.linked_ref.block_hash.clone(),
        block_number: link.linked_ref.block_number,
        provenance: json!({
            "adapter": DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE,
            "binding_kind": state.binding_kind.as_str(),
            "logical_name_id": state.name.logical_name_id,
            "upstream_resource": link.upstream_resource,
            "token_id": link.observed_token_id,
            "current_token_id": state.token_id,
        }),
        canonicality_state: reference.canonicality_state,
    })
}

pub(super) fn deactivate_registry_suffix(
    registry_suffix_by_address: &mut HashMap<String, String>,
    registry_address: Option<&str>,
    expected_suffix: &str,
) {
    let Some(registry_address) = registry_address else {
        return;
    };
    if registry_address == ZERO_ADDRESS {
        return;
    }
    if registry_suffix_by_address
        .get(registry_address)
        .is_some_and(|suffix| suffix == expected_suffix)
    {
        registry_suffix_by_address.remove(registry_address);
    }
}
