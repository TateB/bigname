use alloy_primitives::{B256, hex, keccak256};
use alloy_sol_types::sol;
use anyhow::bail;
use serde_json::{Value, json};

use super::super::{
    EventDraft, Interpreted, NameDraft, ResourceDraft, ShadowNameDraft, ensure_declared,
    permissions::v1_grant_states,
};
use super::authority_transition::{append_authority_transition, append_surface_materialization};
use super::registry::push_permission_change;
use super::support::{events_linked, single_event};
use crate::evm_abi::{address_hex, decode_event_log, u256_word_hex};
use crate::schema_v2::{
    catalog::Selected,
    common::{admitted_label, decoded_label, stable_uuid},
    model::RawLogInput,
    state::{State, V1NameState},
};

mod identity;
use identity::{new_registrar_identity, registrar_namehash};

mod decode;
mod transfer_permissions;
mod wrapper_renewal;
use transfer_permissions::append_transfer_permissions;

mod transfer {
    use super::*;
    sol! { event Transfer(address indexed from, address indexed to, uint256 indexed tokenId); }
}

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
pub(super) fn interpret(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
    migration_enabled: bool,
) -> anyhow::Result<Interpreted> {
    match selected.event.signature.as_str() {
        "ControllerAdded(address)"
        | "ControllerRemoved(address)"
        | "NameRegistered(uint256,address,uint256)"
        | "NameRenewed(uint256,uint256)" => {
            return if migration_enabled {
                super::super::migration::interpret_base_registrar(selected, raw, state)
            } else {
                Ok(Interpreted::new())
            };
        }
        "Transfer(address,address,uint256)"
            if selected.source.source_family == "ens_v1_registrar_l1"
                && selected.emitter_role.as_deref() == Some("registrar") =>
        {
            let mut ordinary = transfer(selected, raw, state)?;
            if migration_enabled {
                let mut correlated =
                    super::super::migration::interpret_base_registrar(selected, raw, state)?;
                ordinary.append(&mut correlated);
            }
            return Ok(ordinary);
        }
        _ => {}
    }
    match selected.event.name.as_str() {
        "NameRegistered" => name_event(selected, raw, state, true),
        "NameRenewed" => name_event(selected, raw, state, false),
        "Transfer" => transfer(selected, raw, state),
        "Upgraded" => super::upgrade::interpret(selected, raw),
        name => bail!("unsupported registrar event {name}"),
    }
}

fn transfer(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    ensure_declared(selected, &["TokenControlTransferred"])?;
    let event = decode_event_log::<transfer::Transfer>(
        &raw.topics,
        &raw.data,
        "registrar Transfer log is malformed",
    )?;
    let from = address_hex(event.from);
    let to = address_hex(event.to);
    if from == ZERO_ADDRESS || to == ZERO_ADDRESS {
        return Ok(Interpreted::new());
    }
    let labelhash = B256::from(event.tokenId.to_be_bytes::<32>());
    let raw_namehash = registrar_namehash(selected, labelhash);
    let previous_active = state.v1_name(&selected.source.namespace, &raw_namehash);
    let mut wrapper_fallback = false;
    let mut fallback_active_from = None;
    if state
        .v1_registrar(&selected.source.namespace, &raw_namehash)
        .is_none()
        && state.v1_surface_materialized(&selected.source.namespace, &raw_namehash)
        && let Some(unwrapped_at) =
            state.matching_v1_unwrap_time(&selected.source.namespace, &raw_namehash, &from, raw)
        && let Some(expiry) =
            state.v1_registrar_expiry_from_wrapper(&selected.source.namespace, &raw_namehash)
    {
        let (token_lineage_id, resource_id, authority_key) =
            new_registrar_identity(selected, raw, &format!("{labelhash:#x}"));
        state.observe_v1_registrar(
            &selected.source.namespace,
            &raw_namehash,
            format!("{}:{raw_namehash}", selected.source.namespace),
            true,
            resource_id,
            token_lineage_id,
            selected.source.source_family.clone(),
            Some(selected.source.manifest_id),
            Some(format!("{labelhash:#x}")),
            Some(expiry),
            Some(from.clone()),
            authority_key,
            true,
            false,
        );
        wrapper_fallback = true;
        fallback_active_from = Some(unwrapped_at);
    }
    let Some((_, linked)) =
        state.transfer_v1_registrar_owner(&selected.source.namespace, &raw_namehash, to.clone())
    else {
        return Ok(Interpreted::new());
    };
    let mut active_after = state.converge_v1_registrar_transfer(
        &selected.source.namespace,
        &raw_namehash,
        raw.block_timestamp.unix_timestamp(),
    );
    if active_after.is_none()
        && state
            .v1_registry_owner(&selected.source.namespace, &raw_namehash)
            .is_some_and(|owner| !owner.eq_ignore_ascii_case(ZERO_ADDRESS))
    {
        let registry_owner = state
            .v1_registry_owner(&selected.source.namespace, &raw_namehash)
            .expect("checked registry owner");
        let authority = V1NameState {
            logical_name_id: linked.logical_name_id.clone(),
            surface_known: linked.surface_known,
            resource_id: stable_uuid(&format!(
                "resource:registry-only:{}:{raw_namehash}",
                raw.chain_id
            )),
            token_lineage_id: None,
            authority_source_family: if selected.source.source_family == "basenames_base_registrar"
            {
                "basenames_base_registry"
            } else {
                "ens_v1_registry_l1"
            }
            .to_owned(),
            source_manifest_id: None,
            labelhash: Some(format!("{labelhash:#x}")),
            expiry: None,
            owner: Some(registry_owner),
            registry_contract: None,
            authority_key: Some(format!("registry-only:{}:{raw_namehash}", raw.chain_id)),
            wrapper_fallback: false,
        };
        state.remember_v1_registry_authority(
            &selected.source.namespace,
            &raw_namehash,
            authority.clone(),
        );
        state.activate_v1_authority(
            &selected.source.namespace,
            &raw_namehash,
            Some(authority.clone()),
        );
        active_after = Some(authority);
    }
    let mut after = json!({
        "source_event": "Transfer",
        "to": to,
        "token_id": u256_word_hex(event.tokenId),
        "namehash": raw_namehash,
        "token_lineage_id": linked.token_lineage_id.map(|id| id.to_string()),
    });
    // A fallback-created registrar identity must be recoverable from the latest transfer row
    // alone. Until a label-bearing registrar-controller registration or renewal replaces it,
    // every transfer repeats the marker and uses that transfer's sender as the restore-time owner.
    if wrapper_fallback || linked.wrapper_fallback {
        after["fallback_from_wrapper"] = json!(true);
        after["fallback_from"] = json!(from);
        after["surface_known"] = json!(linked.surface_known);
        after["labelhash"] = json!(linked.labelhash);
        after["expiry"] = json!(linked.expiry);
        after["authority_key"] = json!(linked.authority_key);
        after["authority_source_manifest_id"] = json!(linked.source_manifest_id);
    }
    let mut output = single_event(
        "TokenControlTransferred",
        Some(linked.logical_name_id.clone()),
        Some(linked.resource_id),
        after,
    );
    output.events[0].explicit_before = Some(json!({"from": from}));
    output.resources.push(ResourceDraft {
        resource_id: linked.resource_id,
        token_lineage_id: linked.token_lineage_id,
    });
    append_transfer_permissions(
        &mut output,
        &from,
        &linked,
        previous_active.as_ref(),
        active_after.as_ref(),
        state.v1_resolver(&selected.source.namespace, &raw_namehash),
        &raw.chain_id,
    );
    let linked_resolver = state.v1_resolver_for_activation(
        &selected.source.namespace,
        &raw_namehash,
        active_after.as_ref(),
    );
    append_authority_transition(
        &mut output,
        super::authority_arm(&selected.source.namespace),
        previous_active.as_ref(),
        active_after.as_ref(),
        state.v1_registry_binding(&selected.source.namespace, &raw_namehash),
        raw,
        &json!({"source_event":"Transfer"}),
        linked_resolver,
        fallback_active_from,
    );
    Ok(output)
}

fn name_event(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
    registration: bool,
) -> anyhow::Result<Interpreted> {
    let (raw_label, explicit_labelhash, mut after) = decode::name(selected, raw)?;
    if keccak256(&raw_label) != explicit_labelhash {
        bail!(
            "{} label does not hash to its indexed label",
            selected.event.name
        );
    }
    let suffix = if selected.source.source_family == "basenames_base_registrar" {
        vec!["base".to_owned(), "eth".to_owned()]
    } else {
        vec!["eth".to_owned()]
    };
    let raw_namehash = registrar_namehash(selected, explicit_labelhash);
    let decoded_label = decoded_label(&raw_label);
    let label = admitted_label(&raw_label);
    let labels = label.map(|label| {
        std::iter::once(label)
            .chain(suffix.iter().cloned())
            .collect::<Vec<_>>()
    });
    let surface_known = labels.is_some();
    let mut raw_labels = vec![raw_label.clone()];
    raw_labels.extend(suffix.iter().map(|label| label.as_bytes().to_vec()));
    let logical_name_id = format!("{}:{raw_namehash}", selected.source.namespace);
    let previous_active = state.v1_name(&selected.source.namespace, &raw_namehash);
    let prior_registrar = state.v1_registrar(&selected.source.namespace, &raw_namehash);
    let existing = (!registration).then(|| prior_registrar.clone()).flatten();
    let synthetic_grant = !registration && existing.is_none();
    let (token_lineage_id, resource_id, authority_key) = existing
        .as_ref()
        .map(|state| {
            (
                state
                    .token_lineage_id
                    .expect("registrar authority has token lineage"),
                state.resource_id,
                None,
            )
        })
        .unwrap_or_else(|| {
            new_registrar_identity(selected, raw, &format!("{explicit_labelhash:#x}"))
        });
    let expiry = after.get("expiry").and_then(Value::as_i64);
    let event_registrant = after
        .get("registrant")
        .and_then(Value::as_str)
        .map(str::to_owned);
    // The wrapper registers itself first; the controller's later event names the wrapped user.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L297 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L656 @ ens_v1@91c966f)
    let owner = registration
        .then(|| state.v1_registry_owner(&selected.source.namespace, &raw_namehash))
        .flatten()
        .filter(|owner| !owner.eq_ignore_ascii_case(ZERO_ADDRESS))
        .or_else(|| event_registrant.clone())
        .or_else(|| existing.as_ref().and_then(|state| state.owner.clone()))
        .or_else(|| synthetic_grant.then(|| ZERO_ADDRESS.to_owned()));
    let retained_authority_key = authority_key.clone().or_else(|| {
        existing
            .as_ref()
            .and_then(|state| state.authority_key.clone())
    });
    let ens_v1_registrar = selected.source.source_family.starts_with("ens_v1_");
    let explicit_ownerless_registry = ens_v1_registrar
        && state.v1_explicit_ownerless_registry_evidence(&selected.source.namespace, &raw_namehash);
    let refresh_current_registrar = !registration
        && existing.is_some()
        && previous_active.as_ref().is_some_and(|current| {
            current.resource_id == resource_id
                && current.authority_source_family == selected.source.source_family
        });
    let make_current = refresh_current_registrar
        || (!explicit_ownerless_registry
            && previous_active.as_ref().is_none_or(|current| {
                let same_family = current.authority_source_family == selected.source.source_family;
                current.authority_source_family != "ens_v1_wrapper_l1"
                    && (registration || same_family)
            }));
    let labelhash = format!("{explicit_labelhash:#x}");
    state.observe_v1_registrar(
        &selected.source.namespace,
        &raw_namehash,
        logical_name_id.clone(),
        surface_known,
        resource_id,
        token_lineage_id,
        selected.source.source_family.clone(),
        Some(selected.source.manifest_id),
        Some(labelhash.clone()),
        expiry,
        owner.clone(),
        retained_authority_key.clone(),
        false,
        make_current,
    );
    if !ens_v1_registrar {
        state.sync_registry_surface_from_registrar(
            &selected.source.namespace,
            &raw_namehash,
            &logical_name_id,
            surface_known,
            Some(&labelhash),
        );
    }
    let surface_materialization = if surface_known && ens_v1_registrar {
        Some(state.materialize_or_sync_v1_active_surface(
            &selected.source.namespace,
            &raw_namehash,
            &logical_name_id,
            &labelhash,
        )?)
    } else {
        None
    };
    let wrapper_renewal = wrapper_renewal::event(
        selected,
        state,
        previous_active.as_ref(),
        &raw_namehash,
        expiry,
        registration,
    )?;
    let after_object = after.as_object_mut().expect("registrar state is an object");
    after_object.insert("namehash".to_owned(), Value::String(raw_namehash.clone()));
    after_object.insert("surface_known".to_owned(), Value::Bool(surface_known));
    after_object.insert(
        "raw_label_hex".to_owned(),
        Value::String(hex::encode(&raw_label)),
    );
    after_object.insert(
        "decoded_label".to_owned(),
        decoded_label.map(Value::String).unwrap_or(Value::Null),
    );
    after_object.insert(
        "labelhash".to_owned(),
        Value::String(format!("{explicit_labelhash:#x}")),
    );
    after_object.insert("token_lineage_id".to_owned(), json!(token_lineage_id));
    if explicit_ownerless_registry && refresh_current_registrar {
        after_object.insert("authority_current".to_owned(), Value::Bool(true));
    }
    if let Some(owner) = owner.as_ref() {
        after_object
            .entry("registrant")
            .or_insert_with(|| json!(owner));
    }
    if let Some(authority_key) = retained_authority_key.as_ref() {
        after_object.insert(
            "authority_kind".to_owned(),
            Value::String("registrar".to_owned()),
        );
        after_object.insert(
            "authority_key".to_owned(),
            Value::String(authority_key.clone()),
        );
    }
    let event_kinds = if registration {
        vec!["RegistrationGranted", "ExpiryChanged", "PermissionChanged"]
    } else if synthetic_grant {
        vec![
            "RegistrationGranted",
            "RegistrationRenewed",
            "ExpiryChanged",
        ]
    } else {
        vec!["RegistrationRenewed", "ExpiryChanged"]
    };
    ensure_declared(selected, &[event_kinds[0]])?;
    let mut output = events_linked(
        event_kinds,
        logical_name_id.clone(),
        resource_id,
        after.clone(),
    );
    output.events.extend(wrapper_renewal);
    if let Some(materialization) = surface_materialization.as_ref() {
        append_surface_materialization(
            &mut output,
            super::authority_arm(&selected.source.namespace),
            materialization,
            raw,
            &selected.event.name,
        );
    }
    if registration || synthetic_grant {
        if let Some(grant) = output
            .events
            .iter_mut()
            .find(|event| event.event_kind == "RegistrationGranted")
        {
            // Retain the live owner because compacted registry facts can restore after this anchor.
            grant.after_state["authority_owner"] = json!(owner);
            grant.explicit_before = Some(json!({
                "authority_kind":previous_active.as_ref().map(super::authority_transition::authority_kind),
                "registrant":prior_registrar.as_ref().and_then(|state| state.owner.clone()),
            }));
        }
        if let Some(expiry_event) = output
            .events
            .iter_mut()
            .find(|event| event.event_kind == "ExpiryChanged")
        {
            expiry_event.explicit_before = Some(json!({
                "expiry":prior_registrar.as_ref().and_then(|state| state.expiry),
            }));
        }
    }
    if !registration {
        let before_expiry = existing.as_ref().and_then(|state| state.expiry);
        for event in output.events.iter_mut().filter(|event| {
            matches!(
                event.event_kind.as_str(),
                "RegistrationRenewed" | "ExpiryChanged"
            )
        }) {
            if event.explicit_before.is_none() {
                event.explicit_before = Some(json!({"expiry":before_expiry}));
            }
        }
    }
    if registration
        && let (Some(subject), Some(authority_key), Some(permission)) = (
            after.get("registrant").and_then(Value::as_str),
            after.get("authority_key").and_then(Value::as_str),
            output
                .events
                .iter_mut()
                .find(|event| event.event_kind == "PermissionChanged"),
        )
    {
        let (before, after) = v1_grant_states(
            subject,
            json!({"kind":"resource"}),
            "resource_control",
            "registrar",
            authority_key,
            "RegistrationGranted",
        );
        permission.explicit_before = Some(before);
        permission.after_state = after;
    }
    if registration
        && let (Some(subject), Some(authority_key), Some(resolver)) = (
            after.get("registrant").and_then(Value::as_str),
            after.get("authority_key").and_then(Value::as_str),
            state.v1_resolver(&selected.source.namespace, &raw_namehash),
        )
    {
        let (before, after_state) = v1_grant_states(
            subject,
            json!({"kind":"resolver","chain_id":raw.chain_id,"resolver_address":resolver}),
            "resolver_control",
            "registrar",
            authority_key,
            "RegistrationGranted",
        );
        output.events.push(EventDraft {
            event_kind: "PermissionChanged".to_owned(),
            logical_name_id: Some(logical_name_id.clone()),
            resource_id: Some(resource_id),
            identity_suffix: format!("PermissionChanged:registration-resolver:{subject}"),
            explicit_before: Some(before),
            after_state,
            state_scope: String::new(),
        });
    }
    let active_after = state.v1_name(&selected.source.namespace, &raw_namehash);
    if registration || synthetic_grant {
        let linked_resolver = state.v1_resolver_for_activation(
            &selected.source.namespace,
            &raw_namehash,
            active_after.as_ref(),
        );
        append_authority_transition(
            &mut output,
            super::authority_arm(&selected.source.namespace),
            previous_active.as_ref(),
            active_after.as_ref(),
            state.v1_registry_binding(&selected.source.namespace, &raw_namehash),
            raw,
            &after,
            linked_resolver,
            None,
        );
    }
    if let Some(labels) = labels {
        output.names.push(NameDraft {
            labels,
            namehash: raw_namehash,
            resource_id: Some(resource_id),
            token_lineage_id: Some(token_lineage_id),
            surface_binding_id: authority_key.as_ref().map(|authority_key| {
                stable_uuid(&format!(
                    "binding:{authority_key}:{}",
                    raw.block_timestamp.unix_timestamp()
                ))
            }),
            bind: false,
            binding_kind: "declared_registry_path".to_owned(),
            authority_arm: super::authority_arm(&selected.source.namespace).to_owned(),
            source_kind: format!("{}_name", selected.event.name),
            preimage_metadata: None,
        });
    } else {
        output.shadow_names.push(ShadowNameDraft {
            raw_labels,
            namehash: raw_namehash,
            source_kind: format!("{}_name", selected.event.name),
        });
        output.resources.push(ResourceDraft {
            resource_id,
            token_lineage_id: Some(token_lineage_id),
        });
    }
    Ok(output)
}
