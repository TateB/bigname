use uuid::Uuid;

use crate::schema_v2::{common::hash_hex, model::PriorEventInput, state::State};

fn materialize_or_sync_retained_registry(
    state: &mut State,
    event: &PriorEventInput,
    namehash: &str,
    logical_name_id: &str,
    labelhash: &str,
) {
    if let Err(error) = state.materialize_or_sync_v1_active_surface(
        &event.namespace,
        namehash,
        logical_name_id,
        labelhash,
    ) {
        state.record_restore_error(error);
    }
}

pub(in crate::schema_v2) fn restore_preimage(state: &mut State, event: &PriorEventInput) {
    if event.event_kind != "PreimageObserved" || event.logical_name_id.is_none() {
        return;
    }
    let Some(namehash) = event
        .after_state
        .get("namehash")
        .and_then(serde_json::Value::as_str)
    else {
        return;
    };
    if event
        .after_state
        .get("visibility_state")
        .and_then(serde_json::Value::as_str)
        == Some("shadow")
    {
        state.observe_v1_surface(&event.namespace, namehash);
    } else if !event.source_family.starts_with("ens_v1_") {
        state.observe_v1_active_surface(&event.namespace, namehash);
    } else {
        let logical_name_id = event
            .logical_name_id
            .as_deref()
            .unwrap_or_else(|| unreachable!("checked above"));
        let labelhash = event
            .after_state
            .get("labelhash")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .or_else(|| {
                event
                    .after_state
                    .get("raw_labels")
                    .and_then(serde_json::Value::as_array)
                    .and_then(|labels| labels.first())
                    .and_then(serde_json::Value::as_str)
                    .map(|label| hash_hex(label.as_bytes()))
            })
            .unwrap_or_default();
        if let Err(error) = state.materialize_v1_active_surface(
            &event.namespace,
            namehash,
            logical_name_id,
            &labelhash,
        ) {
            state.record_restore_error(error);
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn restore_registrar(
    state: &mut State,
    event: &PriorEventInput,
    source_event: Option<&str>,
    logical_name_id: &str,
    resource_id: Uuid,
    lineage: Option<Uuid>,
    expiry: Option<i64>,
) -> bool {
    match source_event {
        Some("NameRegistered" | "NameRenewed") if event.event_kind == "RegistrationGranted" => {
            let (Some(namehash), Some(lineage)) = (
                event
                    .after_state
                    .get("namehash")
                    .and_then(serde_json::Value::as_str),
                lineage,
            ) else {
                return true;
            };
            let registration = source_event == Some("NameRegistered");
            let current = state.v1_name(&event.namespace, namehash);
            let retained_authority_owner = event
                .after_state
                .get("authority_owner")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            let registration_registry_owner = registration
                .then(|| state.v1_registry_owner(&event.namespace, namehash))
                .flatten()
                .filter(|owner| {
                    !owner.eq_ignore_ascii_case("0x0000000000000000000000000000000000000000")
                });
            let event_registrant = event
                .after_state
                .get("registrant")
                .or_else(|| event.after_state.get("owner"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            let registrar_owner = retained_authority_owner
                .or(registration_registry_owner)
                .or(event_registrant);
            let ens_v1_ownerless = event.source_family.starts_with("ens_v1_")
                && state.v1_explicit_ownerless_registry_evidence(&event.namespace, namehash);
            let make_current = !ens_v1_ownerless
                && current.is_none_or(|current| {
                    let same_family = current.authority_source_family == event.source_family;
                    current.authority_source_family != "ens_v1_wrapper_l1"
                        && (registration || same_family)
                });
            let surface_known = event
                .after_state
                .get("surface_known")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(true);
            let registrar_labelhash = event
                .after_state
                .get("labelhash")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            let labelhash = registrar_labelhash.clone().unwrap_or_default();
            state.observe_v1_registrar(
                &event.namespace,
                namehash,
                logical_name_id.to_owned(),
                surface_known,
                resource_id,
                lineage,
                event.source_family.clone(),
                event.source_manifest_id,
                registrar_labelhash.clone(),
                expiry,
                registrar_owner,
                event
                    .after_state
                    .get("authority_key")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                false,
                make_current,
            );
            if !event.source_family.starts_with("ens_v1_") {
                state.sync_registry_surface_from_registrar(
                    &event.namespace,
                    namehash,
                    logical_name_id,
                    surface_known,
                    registrar_labelhash.as_deref(),
                );
            }
            if surface_known && event.source_family.starts_with("ens_v1_") {
                materialize_or_sync_retained_registry(
                    state,
                    event,
                    namehash,
                    logical_name_id,
                    &labelhash,
                );
            }
            true
        }
        Some("NameRenewed") if event.event_kind == "RegistrationRenewed" => {
            let (Some(namehash), Some(lineage)) = (
                event
                    .after_state
                    .get("namehash")
                    .and_then(serde_json::Value::as_str),
                lineage,
            ) else {
                return true;
            };
            let ens_v1_ownerless = event.source_family.starts_with("ens_v1_")
                && state.v1_explicit_ownerless_registry_evidence(&event.namespace, namehash);
            let current = state.v1_name(&event.namespace, namehash);
            let refresh_current_registrar = current.as_ref().is_some_and(|current| {
                current.resource_id == resource_id
                    && current.authority_source_family == event.source_family
            });
            let retained_current_registrar = event
                .after_state
                .get("authority_current")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            let make_current = retained_current_registrar
                || refresh_current_registrar
                || (!ens_v1_ownerless
                    && current.is_none_or(|current| {
                        current.authority_source_family == event.source_family
                    }));
            let retained = state.v1_registrar(&event.namespace, namehash);
            let surface_known = event
                .after_state
                .get("surface_known")
                .and_then(serde_json::Value::as_bool)
                .or_else(|| retained.as_ref().map(|state| state.surface_known))
                .unwrap_or(true);
            let registrar_labelhash = event
                .after_state
                .get("labelhash")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
                .or_else(|| retained.as_ref().and_then(|state| state.labelhash.clone()));
            let labelhash = registrar_labelhash.clone().unwrap_or_default();
            state.observe_v1_registrar(
                &event.namespace,
                namehash,
                logical_name_id.to_owned(),
                surface_known,
                resource_id,
                lineage,
                event.source_family.clone(),
                event.source_manifest_id,
                registrar_labelhash.clone(),
                expiry,
                event
                    .after_state
                    .get("registrant")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| retained.as_ref().and_then(|state| state.owner.clone())),
                event
                    .after_state
                    .get("authority_key")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| {
                        retained
                            .as_ref()
                            .and_then(|state| state.authority_key.clone())
                    }),
                false,
                make_current,
            );
            if !event.source_family.starts_with("ens_v1_") {
                state.sync_registry_surface_from_registrar(
                    &event.namespace,
                    namehash,
                    logical_name_id,
                    surface_known,
                    registrar_labelhash.as_deref(),
                );
            }
            if surface_known && event.source_family.starts_with("ens_v1_") {
                materialize_or_sync_retained_registry(
                    state,
                    event,
                    namehash,
                    logical_name_id,
                    &labelhash,
                );
            }
            true
        }
        _ => false,
    }
}
