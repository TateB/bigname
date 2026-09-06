use super::{model::PriorEventInput, protocol::v1::unmasked_word, state::State};
use {serde_json::Value, uuid::Uuid};
#[path = "state_restore_support.rs"]
mod support;
#[path = "state_restore_v1_surface.rs"]
pub(super) mod v1_surface;
#[path = "state_restore_v1_transfer.rs"]
mod v1_transfer;
use support::{
    expiry_retirement_is_projection_only, parse_i64, parse_u32, parse_u64, raw_label,
    v1_registry_authority, v1_registry_read_anchor,
};
pub(super) fn rebuild_v2_indexes(state: &mut State) {
    state.rebuild_v2_token_indexes();
}
#[rustfmt::skip] pub(super) fn v2(state: &mut State, event: &PriorEventInput) {
    if event.source_family == "ens_v2_resolver_l1" && event.event_kind == "PreimageObserved" {
        if event
            .after_state
            .get("source_event")
            .and_then(Value::as_str)
            == Some("AliasChanged")
            && event
                .after_state
                .get("visibility_state")
                .and_then(Value::as_str)
                != Some("shadow")
            && let Some(logical_name_id) = event.logical_name_id.as_ref()
        {
            state.observe_name_surface(logical_name_id.clone());
        }
        if let (Some(resolver), Some(upstream_resource), Some(logical_name_id)) = (
            event.after_state.get("resolver").and_then(Value::as_str),
            event
                .after_state
                .get("upstream_resource")
                .and_then(Value::as_str),
            event.logical_name_id.as_ref(),
        ) {
            state.observe_name_surface(logical_name_id.clone());
            state.observe_v2_resolver_hint(
                resolver,
                upstream_resource,
                logical_name_id.clone(),
                event
                    .after_state
                    .get("selector")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({})),
            );
        }
        return;
    }
    if !matches!(
        event.source_family.as_str(),
        "ens_v2_registry_l1" | "ens_v2_root_l1"
    ) {
        return;
    }
    let Some(emitter) = event
        .state_scope
        .as_deref()
        .and_then(|scope| scope.split(':').next())
    else {
        return;
    };
    let token = event.after_state.get("token_id").and_then(Value::as_str);
    match event.event_kind.as_str() {
        "RegistrationGranted" | "RegistrationReserved" => {
            let Some(token) = token else { return };
            let raw_label = raw_label(&event.after_state);
            let expiry = event.after_state.get("expiry").and_then(parse_u64);
            let (Some(raw_label), Some(expiry)) = (raw_label, expiry) else {
                return;
            };
            state.restore_v2_registration(
                emitter,
                token,
                event
                    .after_state
                    .get("registry_contract_instance_id")
                    .and_then(Value::as_str)
                    .and_then(|value| Uuid::parse_str(value).ok()),
                &event.namespace,
                &raw_label,
                expiry,
                (event.event_kind == "RegistrationGranted").then(|| event.after_state.clone()),
            ); if event.block_timestamp.is_some_and(|timestamp| super::state::v2_expiry_is_live(Some(expiry), timestamp.unix_timestamp())) { state.clear_v2_expiry_retirement(emitter, token, false); }
            state.restore_v2_unbound_resource(emitter, token, event);
        }
        "TokenResourceLinked" => {
            let (Some(token), Some(resource_id)) = (token, event.resource_id) else {
                return;
            };
            let upstream = event
                .after_state
                .get("resource")
                .or_else(|| event.after_state.get("upstream_resource"))
                .and_then(Value::as_str)
                .unwrap_or("-")
                .to_owned();
            let lineage = event
                .after_state
                .get("token_lineage_id")
                .and_then(Value::as_str)
                .and_then(|value| Uuid::parse_str(value).ok());
            state.link_v2_resource(emitter, token, upstream, resource_id, lineage);
        }
        "TokenRegenerated" => {
            let old = event
                .after_state
                .get("old_token_id")
                .and_then(Value::as_str);
            let new = event
                .after_state
                .get("new_token_id")
                .and_then(Value::as_str);
            if let (Some(old), Some(new)) = (old, new) {
                state.restore_v2_regeneration(emitter, old, new, &event.after_state);
            }
        }
        "TokenControlTransferred" => {
            let (Some(token), Some(registrant)) =
                (token, event.after_state.get("to").and_then(Value::as_str))
            else {
                return;
            };
            state.transfer_v2_registrant(emitter, token, registrant.to_owned());
        }
        "RegistrationReleased" => {
            if expiry_retirement_is_projection_only(event)
                && let Some(token) = token
            {
                state.mark_v2_expiry_retirement(emitter, token, event.after_state.get("expiry").and_then(parse_u64).zip(event.block_timestamp.map(time::OffsetDateTime::unix_timestamp)).is_some_and(|(expiry, timestamp)| u64::try_from(timestamp).is_ok_and(|timestamp| expiry <= timestamp)));
            }
            if !matches!(
                event
                    .after_state
                    .get("terminal_reason")
                    .and_then(Value::as_str),
                Some("registry_name_binding_changed" | "registry_name_binding_expired")
            ) && let Some(token) = token
            {
                state.release_v2_token(emitter, token);
            }
        }
        "ResolverChanged" => {
            let Some(token) = token else { return };
            if expiry_retirement_is_projection_only(event) || support::missing_replacement_role(state, emitter, token, event, "resolver") {
                return;
            }
            let resolver = event
                .after_state
                .get("resolver")
                .and_then(Value::as_str)
                .map(str::to_owned);
            state.set_v2_resolver(emitter, token, resolver);
        }
        "SubregistryChanged" => {
            let Some(token) = token else { return };
            if expiry_retirement_is_projection_only(event) || support::missing_replacement_role(state, emitter, token, event, "subregistry") {
                return;
            }
            state.restore_v2_subregistry_change(emitter, token, &event.after_state);
        }
        "ExpiryChanged" => {
            let (Some(token), Some(expiry)) =
                (token, event.after_state.get("expiry").and_then(parse_u64))
            else {
                return;
            };
            state.set_v2_expiry(emitter, token, expiry); if event.block_timestamp.is_some_and(|timestamp| super::state::v2_expiry_is_live(Some(expiry), timestamp.unix_timestamp())) { state.clear_v2_expiry_retirement(emitter, token, true); }
        }
        "ParentChanged" => {
            let Some(raw_label) = raw_label(&event.after_state) else {
                return;
            };
            let parent = event
                .after_state
                .get("parent")
                .and_then(Value::as_str)
                .map(str::to_owned);
            state.set_v2_parent_claim(emitter, parent, &raw_label);
        }
        _ => {}
    }
    let displaced_regeneration_event = event.after_state.get("source_event").and_then(Value::as_str) == Some("TokenRegenerated") && (event.event_kind == "SurfaceUnbound" || event.event_kind == "RegistrationReleased" && event.after_state.get("terminal_reason").and_then(Value::as_str) == Some("registry_name_binding_changed"));
    if !displaced_regeneration_event && let (Some(token), Some(logical_name_id)) = (token, event.logical_name_id.as_deref()) { state.remember_v2_logical_name(emitter, token, logical_name_id); }
}
pub(super) fn v1(state: &mut State, event: &PriorEventInput) {
    let source_event = event
        .after_state
        .get("source_event")
        .and_then(Value::as_str);
    if event.source_family == "ens_v2_migration_l1"
        && source_event == Some("NameRenewed")
        && let (Some(namehash), Some(expiry)) = (
            event.after_state.get("namehash").and_then(Value::as_str),
            event.after_state.get("wrapper_expiry").and_then(parse_u64),
        )
    {
        state.restore_v1_correlated_wrapper_expiry(&event.namespace, namehash, expiry);
    }
    if !(event.source_family.starts_with("ens_v1_")
        || event.source_family.starts_with("basenames_"))
    {
        return;
    }
    v1_surface::restore_preimage(state, event);
    if matches!(source_event, Some("NewOwner" | "Transfer"))
        && let Some(namehash) = event
            .after_state
            .get("child_node")
            .or_else(|| event.after_state.get("node"))
            .and_then(Value::as_str)
    {
        if unmasked_word::body_has_unmasked_owner_word(&event.after_state) {
            state.forget_v1_registry_owner(&event.namespace, namehash);
        } else if let Some(owner) = event.after_state.get("owner").and_then(Value::as_str) {
            let owner_getter = event
                .after_state
                .get("owner_getter")
                .and_then(Value::as_str)
                .unwrap_or(owner);
            let owner_getter_reason = event
                .after_state
                .get("owner_getter_reason")
                .and_then(Value::as_str)
                .map(str::to_owned);
            state.set_v1_registry_owner_views(
                &event.namespace,
                namehash,
                owner.to_owned(),
                owner_getter.to_owned(),
                owner_getter_reason,
            );
            let anchor = v1_registry_read_anchor(event, namehash);
            state.remember_v1_registry_read_anchor(&event.namespace, namehash, anchor.clone());
            if !owner_getter.eq_ignore_ascii_case("0x0000000000000000000000000000000000000000") {
                state.remember_v1_registry_authority(
                    &event.namespace,
                    namehash,
                    v1_registry_authority(event, namehash, owner_getter, &anchor),
                );
            }
        }
    }
    if matches!(source_event, Some("NewOwner" | "Transfer")) {
        let node = event
            .after_state
            .get(if source_event == Some("NewOwner") {
                "child_node"
            } else {
                "node"
            })
            .and_then(Value::as_str);
        if event
            .after_state
            .get("emitter_role")
            .and_then(Value::as_str)
            == Some("registry")
            && let Some(node) = node
        {
            let _ = state.mark_v1_migrated(&event.namespace, node);
        }
    }
    if source_event == Some("NewResolver")
        && let Some(namehash) = event.after_state.get("node").and_then(Value::as_str)
    {
        let registry_resource_id = super::common::stable_uuid(&format!(
            "resource:registry-only:{}:{namehash}",
            event.chain_id
        ));
        let already_registry_linked = state
            .v1_resolver_link(&event.namespace, namehash)
            .is_some_and(|link| link.resource_id == Some(registry_resource_id));
        let resolver = event
            .after_state
            .get("resolver")
            .and_then(Value::as_str)
            .filter(|resolver| {
                !resolver.eq_ignore_ascii_case("0x0000000000000000000000000000000000000000")
            })
            .map(str::to_owned);
        if !(already_registry_linked && event.resource_id != Some(registry_resource_id)) {
            state.set_v1_resolver_link(
                &event.namespace,
                namehash,
                resolver,
                event.resource_id,
                event.logical_name_id.clone(),
                event
                    .after_state
                    .get("emitter_role")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            );
        }
    }
    if event.event_kind == "ResolverChanged"
        && let (Some(namehash), Some(resolver), Some(resource_id)) = (
            event.after_state["child_node"]
                .as_str()
                .or_else(|| event.after_state["node"].as_str())
                .or_else(|| event.after_state["namehash"].as_str()),
            event.after_state.get("resolver").and_then(Value::as_str),
            event.resource_id,
        )
    {
        if let Some(source_role) = event
            .after_state
            .get("resolver_source_role")
            .and_then(Value::as_str)
        {
            state.restore_v1_resolver_linked_resource(
                &event.namespace,
                namehash,
                resolver,
                resource_id,
                event.logical_name_id.clone(),
                source_role,
            );
        } else {
            state.remember_v1_resolver_linked_resource(
                &event.namespace,
                namehash,
                resolver,
                resource_id,
                event.logical_name_id.clone(),
            );
        }
    }
    if event.source_family == "ens_v1_wrapper_l1"
        && event.event_kind == "PermissionScopeChanged"
        && let (Some(namehash), Some(fuses), Some(expiry)) = (
            event.after_state.get("node").and_then(Value::as_str),
            event.after_state.get("fuses").and_then(parse_u32),
            event.after_state.get("expiry").and_then(parse_u64),
        )
    {
        state.restore_v1_wrapper_data(&event.namespace, namehash, fuses, expiry);
    }
    if source_event == Some("ExpiryExtended")
        && event.event_kind == "ExpiryChanged"
        && let (Some(namehash), Some(expiry)) = (
            event.after_state.get("node").and_then(Value::as_str),
            event.after_state.get("expiry").and_then(parse_u64),
        )
    {
        state.update_v1_wrapper_expiry(&event.namespace, namehash, expiry);
    }
    if event.source_family == "ens_v1_registrar_l1"
        && event.event_kind == "ExpiryChanged"
        && event
            .after_state
            .get("authority_kind")
            .and_then(Value::as_str)
            == Some("wrapper")
        && let (Some(namehash), Some(expiry)) = (
            event.after_state.get("node").and_then(Value::as_str),
            event.after_state.get("expiry").and_then(parse_u64),
        )
    {
        state.update_v1_wrapper_expiry(&event.namespace, namehash, expiry);
    }
    if event.event_kind == "AuthorityTransferred"
        && matches!(source_event, Some("NewOwner" | "Transfer"))
        && let Some(namehash) = event
            .after_state
            .get("child_node")
            .or_else(|| event.after_state.get("node"))
            .and_then(Value::as_str)
    {
        match event
            .after_state
            .get("authority_kind")
            .and_then(Value::as_str)
        {
            Some("registrar") => {
                // Load-bearing assumption for masked writes, whose live arm deliberately
                // leaves the registrar untouched: state.rs keeps the v1_registrars snapshot
                // in sync with v1_names while registrar authority is current, which is what
                // makes this reactivate an equivalent replay.
                if let Some(owner) = event.after_state.get("owner").and_then(Value::as_str) {
                    state.reactivate_v1_registrar_for_owner(
                        &event.namespace,
                        namehash,
                        owner,
                        event
                            .block_timestamp
                            .map(|timestamp| timestamp.unix_timestamp())
                            .unwrap_or(i64::MIN),
                    );
                }
                return;
            }
            None => {
                if state
                    .v1_name(&event.namespace, namehash)
                    .is_some_and(|authority| authority.token_lineage_id.is_none())
                {
                    state.release_v1_name(&event.namespace, namehash);
                }
                return;
            }
            _ => {}
        }
    }
    if event.event_kind == "AuthorityTransferred"
        && event
            .after_state
            .get("authority_kind")
            .and_then(Value::as_str)
            == Some("registry_only")
        && let (Some(_resource_id), Some(namehash)) = (
            event.resource_id,
            event
                .after_state
                .get("child_node")
                .or_else(|| event.after_state.get("node"))
                .and_then(Value::as_str),
        )
    {
        state.activate_retained_v1_registry_authority(&event.namespace, namehash);
        return;
    }
    if event.event_kind == "RegistrationReleased"
        && source_event == Some("RegistrationReleased")
        && let Some(namehash) = event.after_state.get("namehash").and_then(Value::as_str)
    {
        state.restore_v1_registration_release(&event.namespace, namehash);
        return;
    }
    let (Some(logical_name_id), Some(resource_id)) =
        (event.logical_name_id.as_ref(), event.resource_id)
    else {
        return;
    };
    let lineage = event
        .after_state
        .get("token_lineage_id")
        .and_then(Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok());
    let expiry = event.after_state.get("expiry").and_then(parse_i64);
    if v1_surface::restore_registrar(
        state,
        event,
        source_event,
        logical_name_id,
        resource_id,
        lineage,
        expiry,
    ) {
        return;
    }
    match source_event {
        Some("NameWrapped") if event.event_kind == "TokenControlTransferred" => {
            let Some(namehash) = event.after_state.get("node").and_then(Value::as_str) else {
                return;
            };
            state.observe_v1_name(
                &event.namespace,
                namehash,
                logical_name_id.clone(),
                event
                    .after_state
                    .get("surface_known")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                resource_id,
                lineage,
                event.source_family.clone(),
                expiry,
                event
                    .after_state
                    .get("to")
                    .or_else(|| event.after_state.get("owner"))
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                event
                    .after_state
                    .get("authority_key")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            );
        }
        Some("NameUnwrapped") => {
            let Some(namehash) = event.after_state.get("node").and_then(Value::as_str) else {
                return;
            };
            state.release_v1_name(&event.namespace, namehash);
            if event.after_state.get("reactivated_resource_id").is_some() {
                let at = event
                    .after_state
                    .get("unwrapped_at")
                    .and_then(Value::as_i64)
                    .unwrap_or(i64::MIN);
                state.reactivate_v1_registrar(&event.namespace, namehash, at);
            }
        }
        _ => {}
    }

    v1_transfer::restore(state, event);
}
