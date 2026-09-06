use alloy_primitives::{B256, keccak256};
use serde_json::{Value, json};

use super::super::{
    BindingClosureDraft, BindingDraft, EventDraft, Interpreted, ResourceDraft, SourcedEventBatch,
    permissions::v1_revoke_states,
};
use crate::schema_v2::{
    common::{event_time, stable_uuid},
    model::RawLogInput,
    state::{V1NameState, V1ResolverLink, V1SurfaceMaterialization},
};

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum RegistryOwnerView {
    Authentic { owner: String },
    ZeroEquivalent { reason: RegistryOwnerZeroReason },
    UnavailableUnmasked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RegistryOwnerZeroReason {
    LiteralZero,
    RegistrySelf,
}

impl RegistryOwnerZeroReason {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::LiteralZero => "literal_zero",
            Self::RegistrySelf => "registry_self",
        }
    }
}

pub(super) fn classify_registry_owner(
    owner_word: &str,
    registry_address: &str,
    body_has_unmasked_owner_word: bool,
    registry_self_is_zero: bool,
) -> RegistryOwnerView {
    if body_has_unmasked_owner_word {
        RegistryOwnerView::UnavailableUnmasked
    } else if owner_word.eq_ignore_ascii_case(ZERO_ADDRESS) {
        RegistryOwnerView::ZeroEquivalent {
            reason: RegistryOwnerZeroReason::LiteralZero,
        }
    } else if registry_self_is_zero && owner_word.eq_ignore_ascii_case(registry_address) {
        RegistryOwnerView::ZeroEquivalent {
            reason: RegistryOwnerZeroReason::RegistrySelf,
        }
    } else {
        RegistryOwnerView::Authentic {
            owner: owner_word.to_owned(),
        }
    }
}

pub(super) fn registry_fallback_handoff_kind(
    source_event: &str,
    handoff: Option<&(bool, Vec<V1ResolverLink>)>,
) -> Option<&'static str> {
    match handoff {
        Some((_, retired)) if !retired.is_empty() => Some("ResolverChanged"),
        Some((true, retired)) if retired.is_empty() && source_event == "Transfer" => {
            Some("AuthorityTransferred")
        }
        _ => None,
    }
}

pub(super) fn append_registry_fallback_handoff(
    output: &mut Interpreted,
    handoff: Option<&(bool, Vec<V1ResolverLink>)>,
    previous: Option<&V1NameState>,
    raw: &RawLogInput,
    observation: &Value,
    node: &str,
) {
    let Some((_, retired_links)) = handoff.filter(|(_, links)| !links.is_empty()) else {
        return;
    };
    let source_event = observation
        .get("source_event")
        .and_then(Value::as_str)
        .unwrap_or("RegistryOwnershipChanged");
    let mut events = retired_links.iter().map(|retired| EventDraft {
        event_kind: "ResolverChanged".to_owned(),
        logical_name_id: retired.logical_name_id.clone(),
        resource_id: retired.resource_id,
        identity_suffix: format!(
            "ResolverChanged:registry-fallback-handoff:{node}:{}:{}",
            retired
                .resource_id
                .map_or_else(|| "unlinked".to_owned(), |id| id.to_string()),
            retired.resolver_address
        ),
        explicit_before: Some(json!({"resolver":retired.resolver_address})),
        after_state: merge_observation(
            observation,
            json!({
                "state_derived":true,
                "registry_fallback_handoff":true,
                "source_event":source_event,
                "node":node,
                "resolver":ZERO_ADDRESS,
                "previous_resolver":retired.resolver_address,
                "pointer_reason":"current_registry_record_suppresses_old_fallback",
            }),
        ),
        state_scope: format!(
            "registry-fallback-handoff:{node}:resolver:{}",
            retired
                .resource_id
                .map_or_else(|| "unlinked".to_owned(), |id| id.to_string())
        ),
    });
    let declared = output
        .events
        .iter_mut()
        .find(|event| event.event_kind == "ResolverChanged")
        .expect("fallback handoff kind was declared");
    *declared = events.next().expect("retired links are nonempty");
    output.events.extend(events);

    let Some(authority) = previous.filter(|authority| {
        retired_links
            .iter()
            .any(|retired| retired.resource_id == Some(authority.resource_id))
    }) else {
        return;
    };
    let (Some(subject), Some(authority_key)) = (
        authority.owner.as_deref(),
        authority.authority_key.as_deref(),
    ) else {
        return;
    };
    let resolver = &retired_links
        .iter()
        .find(|retired| retired.resource_id == Some(authority.resource_id))
        .expect("matched authority has a retired resolver link")
        .resolver_address;
    let (before, after) = v1_revoke_states(
        subject,
        json!({
            "kind":"resolver",
            "chain_id":raw.chain_id,
            "resolver_address":resolver,
        }),
        "resolver_control",
        authority_kind(authority),
        authority_key,
        source_event,
    );
    output.events.push(EventDraft {
        event_kind: "PermissionChanged".to_owned(),
        logical_name_id: authority
            .surface_known
            .then(|| authority.logical_name_id.clone()),
        resource_id: Some(authority.resource_id),
        identity_suffix: format!(
            "PermissionChanged:registry-fallback-handoff:{node}:{}",
            resolver
        ),
        explicit_before: Some(before),
        after_state: after,
        state_scope: format!(
            "registry-fallback-handoff:{node}:resolver-control:{}",
            authority.resource_id
        ),
    });
}

pub(super) fn child_node(parent: B256, labelhash: B256) -> String {
    let mut input = [0u8; 64];
    input[..32].copy_from_slice(parent.as_slice());
    input[32..].copy_from_slice(labelhash.as_slice());
    format!("{:#x}", keccak256(input))
}

pub(super) fn append_surface_materialization(
    output: &mut Interpreted,
    authority_arm: &str,
    materialization: &V1SurfaceMaterialization,
    raw: &RawLogInput,
    source_event: &str,
) {
    append_surface_materialization_for_trigger(
        output,
        authority_arm,
        materialization,
        raw,
        source_event,
    );
}

pub(super) fn append_surface_materialization_for_trigger(
    output: &mut Interpreted,
    authority_arm: &str,
    materialization: &V1SurfaceMaterialization,
    raw: &RawLogInput,
    source_event: &str,
) {
    let (source_manifest_id, events) = match materialization {
        V1SurfaceMaterialization::RegistryAuthority {
            previous,
            promoted,
            resolver,
            source_manifest_id,
        } => {
            debug_assert_eq!(previous.resource_id, promoted.resource_id);
            output.bindings.push(BindingDraft {
                logical_name_id: promoted.logical_name_id.clone(),
                resource_id: promoted.resource_id,
                binding_kind: "declared_registry_path".to_owned(),
                authority_arm: authority_arm.to_owned(),
                surface_binding_id: promoted.authority_key.as_ref().map(|authority_key| {
                    stable_uuid(&format!(
                        "binding:{authority_key}:{}",
                        event_time(raw).unix_timestamp_nanos()
                    ))
                }),
                active_from: None,
            });
            let node = promoted
                .logical_name_id
                .split_once(':')
                .map(|(_, node)| node)
                .unwrap_or(&promoted.logical_name_id);
            let common = json!({
                "state_derived":true,
                "surface_materialization":true,
                "source_event":source_event,
                "node":node,
                "authority_kind":"registry_only",
                "authority_key":promoted.authority_key,
                "owner":promoted.owner,
                "owner_getter":promoted.owner,
                "registry_contract":promoted.registry_contract,
                "binding_kind":"declared_registry_path",
                "pointer_reason":"surface_materialization_current_resolver",
            });
            let mut events = vec![EventDraft {
                event_kind: "SurfaceBound".to_owned(),
                logical_name_id: Some(promoted.logical_name_id.clone()),
                resource_id: Some(promoted.resource_id),
                identity_suffix: format!(
                    "SurfaceBound:surface-materialization:{node}:{}",
                    promoted.resource_id
                ),
                explicit_before: Some(json!({})),
                after_state: merge_observation(
                    &common,
                    json!({"active_from":raw.block_timestamp.unix_timestamp()}),
                ),
                state_scope: format!("surface-materialization:{node}:{}", promoted.resource_id),
            }];
            if let Some(resolver) = resolver {
                let resolver_address = &resolver.resolver_address;
                events.push(EventDraft {
                    event_kind: "ResolverChanged".to_owned(),
                    logical_name_id: Some(promoted.logical_name_id.clone()),
                    resource_id: Some(promoted.resource_id),
                    identity_suffix: format!(
                        "ResolverChanged:surface-materialization:{node}:{}:{resolver_address}",
                        promoted.resource_id
                    ),
                    explicit_before: Some(json!({"resolver":Value::Null})),
                    after_state: merge_observation(
                        &common,
                        json!({
                            "resolver":resolver_address,
                            "resolver_source_role":resolver.source_role,
                        }),
                    ),
                    state_scope: format!(
                        "surface-materialization:{node}:{}:resolver",
                        promoted.resource_id
                    ),
                });
            }
            (*source_manifest_id, events)
        }
        V1SurfaceMaterialization::RegistryRead {
            anchor,
            resolver,
            source_manifest_id,
        } => {
            let node = anchor
                .logical_name_id
                .split_once(':')
                .map(|(_, node)| node)
                .unwrap_or(&anchor.logical_name_id);
            let events = resolver
                .as_ref()
                .map(|resolver| {
                    let resolver_address = &resolver.resolver_address;
                    vec![EventDraft {
                        event_kind: "ResolverChanged".to_owned(),
                        logical_name_id: Some(anchor.logical_name_id.clone()),
                        resource_id: Some(anchor.resource_id),
                        identity_suffix: format!(
                            "ResolverChanged:surface-materialization:{node}:{}:{resolver_address}",
                            anchor.resource_id
                        ),
                        explicit_before: Some(json!({"resolver":Value::Null})),
                        after_state: json!({
                            "state_derived":true,
                            "surface_materialization":true,
                            "source_event":source_event,
                            "node":node,
                            "authority_kind":"registry_only",
                            "authority_key":Value::Null,
                            "binding_kind":"declared_registry_path",
                            "pointer_reason":"surface_materialization_current_resolver",
                            "resolver":resolver_address,
                            "resolver_source_role":resolver.source_role,
                        }),
                        state_scope: format!(
                            "surface-materialization:{node}:{}:resolver",
                            anchor.resource_id
                        ),
                    }]
                })
                .unwrap_or_default();
            (*source_manifest_id, events)
        }
        V1SurfaceMaterialization::AlreadyMaterialized => return,
    };
    if !events.is_empty() {
        debug_assert!(events.iter().all(|event| !event.state_scope.is_empty()));
        output.sourced_events.push(SourcedEventBatch {
            source_manifest_id,
            events,
        });
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn append_authority_transition(
    output: &mut Interpreted,
    authority_arm: &str,
    previous: Option<&V1NameState>,
    linked: Option<&V1NameState>,
    registry_binding: Option<(String, String)>,
    raw: &RawLogInput,
    observation_state: &Value,
    resolver: Option<V1ResolverLink>,
    binding_active_from: Option<time::OffsetDateTime>,
) {
    if let Some(linked) = linked.filter(|state| state.token_lineage_id.is_none()) {
        output.resources.push(ResourceDraft {
            resource_id: linked.resource_id,
            token_lineage_id: None,
        });
    }
    if previous.map(|authority| authority.resource_id)
        == linked.map(|authority| authority.resource_id)
    {
        return;
    }
    if let Some(linked) = linked.filter(|authority| authority.surface_known) {
        output.bindings.push(BindingDraft {
            logical_name_id: linked.logical_name_id.clone(),
            resource_id: linked.resource_id,
            binding_kind: "declared_registry_path".to_owned(),
            authority_arm: authority_arm.to_owned(),
            surface_binding_id: linked.authority_key.as_ref().map(|authority_key| {
                stable_uuid(&format!(
                    "binding:{authority_key}:{}",
                    event_time(raw).unix_timestamp_nanos()
                ))
            }),
            active_from: binding_active_from,
        });
    } else if let Some(previous) = previous.filter(|authority| authority.surface_known) {
        output.binding_closures.push(BindingClosureDraft {
            logical_name_id: previous.logical_name_id.clone(),
            authority_arm: authority_arm.to_owned(),
        });
    }
    let logical_name_id = linked
        .filter(|authority| authority.surface_known || authority.token_lineage_id.is_some())
        .map(|authority| authority.logical_name_id.clone())
        .or_else(|| {
            previous
                .filter(|authority| authority.surface_known || authority.token_lineage_id.is_some())
                .map(|authority| authority.logical_name_id.clone())
        });
    let Some(logical_name_id) = logical_name_id else {
        return;
    };
    let source_event = observation_state
        .get("source_event")
        .and_then(Value::as_str)
        .unwrap_or("AuthorityTransferred");
    if let Some(previous) = previous.filter(|authority| authority.surface_known) {
        output.events.push(EventDraft {
            event_kind: "SurfaceUnbound".to_owned(),
            logical_name_id: Some(logical_name_id.clone()),
            resource_id: Some(previous.resource_id),
            identity_suffix: format!("SurfaceUnbound:{source_event}:{}", previous.resource_id),
            explicit_before: Some(json!({
                "authority_kind":authority_kind(previous),
                "authority_key":previous.authority_key,
            })),
            after_state: merge_observation(
                observation_state,
                json!({
                    "source_event":source_event,
                    "authority_kind":authority_kind(previous),
                    "authority_key":previous.authority_key,
                    "active_to":raw.block_timestamp.unix_timestamp(),
                }),
            ),
            state_scope: String::new(),
        });
    }
    if let Some(linked) = linked.filter(|authority| authority.surface_known) {
        let mut linked_observation = observation_state.clone();
        if let Some((owner, contract)) = registry_binding {
            linked_observation["owner_getter"] = json!(owner);
            linked_observation["registry_contract"] = json!(contract);
        }
        output.events.push(EventDraft {
            event_kind: "SurfaceBound".to_owned(),
            logical_name_id: Some(logical_name_id.clone()),
            resource_id: Some(linked.resource_id),
            identity_suffix: format!("SurfaceBound:{source_event}:{}", linked.resource_id),
            explicit_before: Some(json!({})),
            after_state: merge_observation(
                &linked_observation,
                json!({
                    "source_event":source_event,
                    "authority_kind":authority_kind(linked),
                    "authority_key":linked.authority_key,
                    "active_from":raw.block_timestamp.unix_timestamp(),
                    "binding_kind":"declared_registry_path",
                }),
            ),
            state_scope: String::new(),
        });
    }
    output.events.push(EventDraft {
        event_kind: "AuthorityEpochChanged".to_owned(),
        logical_name_id: Some(logical_name_id.clone()),
        resource_id: linked
            .map(|authority| authority.resource_id)
            .or_else(|| previous.map(|authority| authority.resource_id)),
        identity_suffix: format!("AuthorityEpochChanged:{source_event}:{logical_name_id}"),
        explicit_before: Some(json!({
            "authority_kind":previous.map(authority_kind),
            "authority_key":previous.and_then(|authority| authority.authority_key.clone()),
        })),
        after_state: merge_observation(
            observation_state,
            json!({
                "source_event":source_event,
                "authority_kind":linked.map(authority_kind),
                "authority_key":linked.and_then(|authority| authority.authority_key.clone()),
            }),
        ),
        state_scope: String::new(),
    });
    if let (Some(linked), Some(resolver)) = (linked, resolver) {
        output.events.push(EventDraft {
            event_kind: "ResolverChanged".to_owned(),
            logical_name_id: Some(logical_name_id),
            resource_id: Some(linked.resource_id),
            identity_suffix: format!(
                "ResolverChanged:authority:{source_event}:{}",
                resolver.resolver_address
            ),
            explicit_before: Some(json!({"resolver":Value::Null})),
            after_state: merge_observation(
                observation_state,
                json!({
                    "source_event":"AuthorityEpochChanged",
                    "resolver":resolver.resolver_address,
                    "resolver_source_role":resolver.source_role,
                }),
            ),
            state_scope: String::new(),
        });
    }
}

fn merge_observation(observation: &Value, fields: Value) -> Value {
    let mut merged = observation.clone();
    merged
        .as_object_mut()
        .expect("authority observation is an object")
        .extend(
            fields
                .as_object()
                .expect("authority boundary fields are an object")
                .clone(),
        );
    merged
}

pub(super) fn authority_kind(authority: &V1NameState) -> &'static str {
    match authority.authority_source_family.as_str() {
        "ens_v1_wrapper_l1" => "wrapper",
        "ens_v1_registrar_l1" | "basenames_base_registrar" => "registrar",
        _ => "registry_only",
    }
}
