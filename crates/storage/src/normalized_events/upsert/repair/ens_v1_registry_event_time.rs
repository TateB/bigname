use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::Postgres;

use super::super::super::types::NormalizedEvent;
use super::super::{normalized_event_identity_differences, serialize_jsonb_value};

pub(crate) async fn repair_ens_v1_unwrapped_authority_registry_event_time_resource_ids(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Result<HashSet<String>> {
    let mut event_identities = Vec::new();
    let mut old_resource_ids = Vec::new();
    let mut new_resource_ids = Vec::new();
    let mut logical_name_ids = Vec::new();
    let mut block_numbers = Vec::new();
    let mut block_hashes = Vec::new();
    let mut transaction_hashes = Vec::new();
    let mut log_indexes = Vec::new();
    let mut event_kinds = Vec::new();
    let mut old_before_states = Vec::new();
    let mut new_before_states = Vec::new();
    let mut old_after_states = Vec::new();
    let mut new_after_states = Vec::new();
    let mut registration_resource_ids = Vec::new();
    let mut registration_block_hashes = Vec::new();
    let mut registration_transaction_hashes = Vec::new();
    let mut registration_log_indexes = Vec::new();

    for event in events {
        if event.event_kind != "RegistrationGranted" {
            continue;
        }
        let (Some(resource_id), Some(block_hash), Some(transaction_hash), Some(log_index)) = (
            event.resource_id,
            event.block_hash.as_ref(),
            event.transaction_hash.as_ref(),
            event.log_index,
        ) else {
            continue;
        };
        registration_resource_ids.push(resource_id);
        registration_block_hashes.push(block_hash.clone());
        registration_transaction_hashes.push(transaction_hash.clone());
        registration_log_indexes.push(log_index);
    }

    for event in events {
        let Some(existing) = existing_by_identity.get(&event.event_identity) else {
            continue;
        };
        if !ens_v1_unwrapped_authority_registry_event_time_resource_id_repair_allowed(
            existing,
            event,
            &normalized_event_identity_differences(existing, event),
        ) {
            continue;
        }
        let Some(old_resource_id) = existing.resource_id else {
            continue;
        };
        let new_resource_id = event.resource_id;
        let (Some(logical_name_id), Some(block_number)) =
            (existing.logical_name_id.as_ref(), existing.block_number)
        else {
            continue;
        };

        event_identities.push(event.event_identity.clone());
        old_resource_ids.push(old_resource_id);
        new_resource_ids.push(new_resource_id);
        logical_name_ids.push(logical_name_id.clone());
        block_numbers.push(block_number);
        block_hashes.push(
            existing
                .block_hash
                .clone()
                .or_else(|| event.block_hash.clone())
                .unwrap_or_default(),
        );
        transaction_hashes.push(
            existing
                .transaction_hash
                .clone()
                .or_else(|| event.transaction_hash.clone())
                .unwrap_or_default(),
        );
        log_indexes.push(existing.log_index.or(event.log_index).unwrap_or(-1));
        event_kinds.push(event.event_kind.clone());
        old_before_states.push(serialize_jsonb_value(
            &existing.before_state,
            "failed to serialize existing ENSv1 registry event-time before_state",
        )?);
        new_before_states.push(serialize_jsonb_value(
            &event.before_state,
            "failed to serialize repaired ENSv1 registry event-time before_state",
        )?);
        old_after_states.push(serialize_jsonb_value(
            &existing.after_state,
            "failed to serialize existing ENSv1 registry event-time after_state",
        )?);
        new_after_states.push(serialize_jsonb_value(
            &event.after_state,
            "failed to serialize repaired ENSv1 registry event-time after_state",
        )?);
    }

    if event_identities.is_empty() {
        return Ok(HashSet::new());
    }

    let repaired = sqlx::query_scalar::<_, String>(
        include_str!("ens_v1_registry_event_time_resource_id.sql"),
    )
    .bind(&event_identities)
    .bind(&old_resource_ids)
    .bind(&new_resource_ids)
    .bind(&logical_name_ids)
    .bind(&block_numbers)
    .bind(&block_hashes)
    .bind(&transaction_hashes)
    .bind(&log_indexes)
    .bind(&event_kinds)
    .bind(&old_before_states)
    .bind(&new_before_states)
    .bind(&old_after_states)
    .bind(&new_after_states)
    .bind(&registration_resource_ids)
    .bind(&registration_block_hashes)
    .bind(&registration_transaction_hashes)
    .bind(&registration_log_indexes)
    .fetch_all(&mut **executor)
    .await
    .context(
        "failed to repair ENSv1 unwrapped-authority event-time registry normalized-event resource_id",
    )?;

    let repaired = repaired.into_iter().collect::<HashSet<_>>();
    let rejected = (0..event_identities.len())
        .filter(|index| !repaired.contains(event_identities[*index].as_str()))
        .map(|index| {
            let event_identity = &event_identities[index];
            let old_resource_id = old_resource_ids[index];
            let new_resource_id = new_resource_ids[index];
            let new_resource_id = new_resource_id
                .map(|resource_id| resource_id.to_string())
                .unwrap_or_else(|| "NULL".to_owned());
            format!(
                "{event_identity} (old_resource_id={old_resource_id}, new_resource_id={new_resource_id}, logical_name_id={}, event_kind={}, old_before_state={}, new_before_state={}, old_after_state={}, new_after_state={})",
                logical_name_ids[index],
                event_kinds[index],
                old_before_states[index],
                new_before_states[index],
                old_after_states[index],
                new_after_states[index]
            )
        })
        .collect::<Vec<_>>();
    if !rejected.is_empty() {
        bail!(
            "ENSv1 registry event-time resource_id repair rejected invalid resource anchors for events: {}",
            rejected.join(", ")
        );
    }

    Ok(repaired)
}

pub(crate) async fn repair_ens_v1_unwrapped_authority_registry_event_time_before_states(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Result<HashSet<String>> {
    let mut event_identities = Vec::new();
    let mut resource_ids = Vec::new();
    let mut logical_name_ids = Vec::new();
    let mut event_kinds = Vec::new();
    let mut old_before_states = Vec::new();
    let mut new_before_states = Vec::new();
    let mut after_states = Vec::new();

    for event in events {
        let Some(existing) = existing_by_identity.get(&event.event_identity) else {
            continue;
        };
        if !ens_v1_unwrapped_authority_registry_event_time_before_state_repair_allowed(
            existing,
            event,
            &normalized_event_identity_differences(existing, event),
        ) {
            continue;
        }
        let (Some(resource_id), Some(logical_name_id)) =
            (existing.resource_id, existing.logical_name_id.as_ref())
        else {
            continue;
        };

        event_identities.push(event.event_identity.clone());
        resource_ids.push(resource_id);
        logical_name_ids.push(logical_name_id.clone());
        event_kinds.push(event.event_kind.clone());
        old_before_states.push(serialize_jsonb_value(
            &existing.before_state,
            "failed to serialize existing ENSv1 registry event-time before_state",
        )?);
        new_before_states.push(serialize_jsonb_value(
            &event.before_state,
            "failed to serialize repaired ENSv1 registry event-time before_state",
        )?);
        after_states.push(serialize_jsonb_value(
            &event.after_state,
            "failed to serialize ENSv1 registry event-time after_state",
        )?);
    }

    if event_identities.is_empty() {
        return Ok(HashSet::new());
    }

    let repaired = sqlx::query_scalar::<_, String>(
        include_str!("ens_v1_registry_event_time_before_state.sql"),
    )
    .bind(&event_identities)
    .bind(&resource_ids)
    .bind(&logical_name_ids)
    .bind(&event_kinds)
    .bind(&old_before_states)
    .bind(&new_before_states)
    .bind(&after_states)
    .fetch_all(&mut **executor)
    .await
    .context(
        "failed to repair ENSv1 unwrapped-authority event-time registry normalized-event before_state",
    )?;

    let repaired = repaired.into_iter().collect::<HashSet<_>>();
    let rejected = event_identities
        .iter()
        .zip(resource_ids.iter())
        .filter(|(event_identity, _)| !repaired.contains(event_identity.as_str()))
        .map(|(event_identity, resource_id)| {
            format!("{event_identity} (resource_id={resource_id})")
        })
        .collect::<Vec<_>>();
    if !rejected.is_empty() {
        bail!(
            "ENSv1 registry event-time before_state repair rejected invalid resource anchors for events: {}",
            rejected.join(", ")
        );
    }

    Ok(repaired)
}

pub(crate) fn ens_v1_unwrapped_authority_registry_event_time_resource_id_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    if !registry_event_time_repair_differences_allowed(differing_fields) {
        return false;
    }
    if existing.resource_id.is_none()
        || existing.logical_name_id.is_none()
        || incoming.logical_name_id.is_none()
        || existing.logical_name_id != incoming.logical_name_id
        || existing.block_number.is_none()
        || existing.namespace != "ens"
        || existing.chain_id.as_deref() != Some("ethereum-mainnet")
        || existing.derivation_kind != "ens_v1_unwrapped_authority"
        || !matches!(
            existing.source_family.as_str(),
            "ens_v1_registry_l1" | "ens_v1_registrar_l1" | "ens_v1_resolver_l1"
        )
        || !matches!(
            existing.event_kind.as_str(),
            "ResolverChanged"
                | "RecordChanged"
                | "RecordVersionChanged"
                | "PermissionChanged"
                | "AuthorityTransferred"
        )
    {
        return false;
    }

    if differing_fields.len() == 1 {
        return true;
    }

    if existing.event_kind == "AuthorityTransferred" {
        return authority_transfer_state_repair_allowed(
            &existing.before_state,
            &incoming.before_state,
            &existing.after_state,
            &incoming.after_state,
        );
    }

    if existing.event_kind == "RecordVersionChanged" {
        return record_version_state_repair_allowed(
            &existing.before_state,
            &incoming.before_state,
            &existing.after_state,
            &incoming.after_state,
        );
    }

    if existing.event_kind == "RecordChanged" {
        return record_changed_text_value_state_repair_allowed(
            &existing.before_state,
            &incoming.before_state,
            &existing.after_state,
            &incoming.after_state,
        );
    }

    existing.event_kind == "PermissionChanged"
        && permission_state_authority_repair_allowed(&existing.before_state, &incoming.before_state)
        && permission_state_authority_repair_allowed(&existing.after_state, &incoming.after_state)
}

pub(crate) fn ens_v1_unwrapped_authority_registry_event_time_before_state_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    if !matches!(differing_fields, ["before_state"]) {
        return false;
    }
    if existing.resource_id.is_none()
        || existing.resource_id != incoming.resource_id
        || existing.logical_name_id.is_none()
        || incoming.logical_name_id.is_none()
        || existing.logical_name_id != incoming.logical_name_id
        || existing.namespace != "ens"
        || existing.chain_id.as_deref() != Some("ethereum-mainnet")
        || existing.derivation_kind != "ens_v1_unwrapped_authority"
    {
        return false;
    }

    if existing.event_kind == "AuthorityTransferred" {
        return existing.source_family == "ens_v1_registry_l1"
            && authority_transfer_state_repair_allowed(
                &existing.before_state,
                &incoming.before_state,
                &existing.after_state,
                &incoming.after_state,
            );
    }

    existing.event_kind == "RecordVersionChanged"
        && existing.source_family == "ens_v1_resolver_l1"
        && record_version_state_repair_allowed(
            &existing.before_state,
            &incoming.before_state,
            &existing.after_state,
            &incoming.after_state,
        )
}

fn registry_event_time_repair_differences_allowed(differing_fields: &[&'static str]) -> bool {
    matches!(
        differing_fields,
        ["resource_id"]
            | ["resource_id", "before_state"]
            | ["resource_id", "after_state"]
            | ["resource_id", "before_state", "after_state"]
    )
}

fn authority_transfer_state_repair_allowed(
    existing_before_state: &Value,
    incoming_before_state: &Value,
    existing_after_state: &Value,
    incoming_after_state: &Value,
) -> bool {
    if existing_after_state != incoming_after_state {
        return false;
    }

    if !authority_transfer_owner_transition_allowed(existing_before_state, incoming_before_state) {
        return false;
    }

    let mut existing_without_owner = existing_before_state.clone();
    if let Some(object) = existing_without_owner.as_object_mut() {
        object.remove("owner");
    }
    let mut incoming_without_owner = incoming_before_state.clone();
    if let Some(object) = incoming_without_owner.as_object_mut() {
        object.remove("owner");
    }

    existing_without_owner == incoming_without_owner
}

fn authority_transfer_owner_transition_allowed(
    existing_before_state: &Value,
    incoming_before_state: &Value,
) -> bool {
    matches!(
        (
            owner_state(existing_before_state),
            owner_state(incoming_before_state)
        ),
        (Some(OwnerState::Known), Some(OwnerState::Known))
            | (Some(OwnerState::Known), Some(OwnerState::Null))
            | (Some(OwnerState::Null), Some(OwnerState::Known))
    )
}

enum OwnerState {
    Known,
    Null,
}

fn owner_state(value: &Value) -> Option<OwnerState> {
    match value.get("owner")? {
        Value::Null => Some(OwnerState::Null),
        Value::String(owner) if !owner.trim().is_empty() => Some(OwnerState::Known),
        _ => None,
    }
}

fn record_version_state_repair_allowed(
    existing_before_state: &Value,
    incoming_before_state: &Value,
    existing_after_state: &Value,
    incoming_after_state: &Value,
) -> bool {
    if existing_after_state != incoming_after_state {
        return false;
    }

    let Some(after_version) = incoming_after_state
        .get("record_version")
        .and_then(Value::as_i64)
    else {
        return false;
    };

    let mut existing_without_version = existing_before_state.clone();
    if let Some(object) = existing_without_version.as_object_mut() {
        object.remove("record_version");
    }
    let mut incoming_without_version = incoming_before_state.clone();
    if let Some(object) = incoming_without_version.as_object_mut() {
        object.remove("record_version");
    }

    if existing_without_version != incoming_without_version {
        return false;
    }

    let existing_version = existing_before_state.get("record_version");
    let incoming_version = incoming_before_state.get("record_version");
    let previous_version = match (existing_version, incoming_version) {
        (Some(Value::Null), Some(value)) => value.as_i64(),
        (Some(value), Some(Value::Null)) => value.as_i64(),
        _ => None,
    };

    previous_version.and_then(|version| version.checked_add(1)) == Some(after_version)
}

fn record_changed_text_value_state_repair_allowed(
    existing_before_state: &Value,
    incoming_before_state: &Value,
    existing_after_state: &Value,
    incoming_after_state: &Value,
) -> bool {
    if existing_before_state != incoming_before_state
        || !selector_text_record_state(existing_after_state)
        || !selector_text_record_state(incoming_after_state)
    {
        return false;
    }

    let existing_value = existing_after_state.get("value").and_then(Value::as_str);
    let incoming_value = incoming_after_state.get("value").and_then(Value::as_str);
    if existing_value.is_some() == incoming_value.is_some() {
        return false;
    }

    let mut existing_without_value = existing_after_state.clone();
    if let Some(object) = existing_without_value.as_object_mut() {
        object.remove("value");
    }
    let mut incoming_without_value = incoming_after_state.clone();
    if let Some(object) = incoming_without_value.as_object_mut() {
        object.remove("value");
    }

    existing_without_value == incoming_without_value
}

fn selector_text_record_state(state: &Value) -> bool {
    let Some(record_family) = state.get("record_family").and_then(Value::as_str) else {
        return false;
    };
    let Some(record_key) = state.get("record_key").and_then(Value::as_str) else {
        return false;
    };
    let Some(selector_key) = state.get("selector_key").and_then(Value::as_str) else {
        return false;
    };

    record_family == "text" && record_key.starts_with("text:") && !selector_key.is_empty()
}

fn permission_state_authority_repair_allowed(
    existing_state: &Value,
    incoming_state: &Value,
) -> bool {
    if existing_state == incoming_state {
        return true;
    }

    if permission_state_without_authority_sources(existing_state)
        != permission_state_without_authority_sources(incoming_state)
    {
        return false;
    }

    let grant_source_repair_allowed = authority_source_unchanged_or_repaired(
        existing_state.get("grant_source"),
        incoming_state.get("grant_source"),
    );
    let revocation_source_repair_allowed = authority_source_unchanged_or_repaired(
        existing_state.get("revocation_source"),
        incoming_state.get("revocation_source"),
    );

    grant_source_repair_allowed && revocation_source_repair_allowed
}

fn permission_state_without_authority_sources(state: &Value) -> Value {
    let mut value = state.clone();
    if let Some(object) = value.as_object_mut() {
        object.remove("grant_source");
        object.remove("revocation_source");
    }
    value
}

fn authority_source_unchanged_or_repaired(
    existing_source: Option<&Value>,
    incoming_source: Option<&Value>,
) -> bool {
    existing_source == incoming_source
        || authority_source_transition_allowed(existing_source, incoming_source)
}

fn authority_source_transition_allowed(
    existing_source: Option<&Value>,
    incoming_source: Option<&Value>,
) -> bool {
    let (Some(existing_source), Some(incoming_source)) = (existing_source, incoming_source) else {
        return false;
    };
    existing_source.get("kind").and_then(Value::as_str) == Some("ens_v1_authority")
        && incoming_source.get("kind").and_then(Value::as_str) == Some("ens_v1_authority")
        && existing_source
            .get("authority_kind")
            .and_then(Value::as_str)
            .is_some_and(|authority_kind| {
                matches!(authority_kind, "registrar" | "wrapper" | "registry_only")
            })
        && incoming_source
            .get("authority_kind")
            .and_then(Value::as_str)
            .is_some_and(|incoming_authority_kind| {
                let existing_authority_kind = existing_source
                    .get("authority_kind")
                    .and_then(Value::as_str);
                incoming_authority_kind == "registry_only"
                    || (existing_authority_kind == Some("registry_only")
                        && incoming_authority_kind == "registrar")
            })
        && existing_source
            .get("authority_key")
            .and_then(Value::as_str)
            .is_some_and(|authority_key| !authority_key.is_empty())
        && incoming_source
            .get("authority_key")
            .and_then(Value::as_str)
            .is_some_and(|authority_key| !authority_key.is_empty())
        && existing_source
            .get("source_event_kind")
            .and_then(Value::as_str)
            .is_some_and(|source_event_kind| !source_event_kind.is_empty())
        && existing_source
            .get("source_event_kind")
            .and_then(Value::as_str)
            == incoming_source
                .get("source_event_kind")
                .and_then(Value::as_str)
}
