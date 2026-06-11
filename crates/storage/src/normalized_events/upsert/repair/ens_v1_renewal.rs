use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::Postgres;

use super::super::super::types::NormalizedEvent;
use super::super::{normalized_event_identity_differences, serialize_jsonb_value};

pub(crate) async fn repair_ens_v1_unwrapped_authority_renewal_resource_ids(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Result<HashSet<String>> {
    let mut event_identities = Vec::new();
    let mut old_resource_ids = Vec::new();
    let mut new_resource_ids = Vec::new();
    let mut logical_name_ids = Vec::new();
    let mut min_block_numbers = Vec::new();
    let mut labelhashes = Vec::new();
    let mut old_before_states = Vec::new();
    let mut new_before_states = Vec::new();
    let mut after_states = Vec::new();

    for event in events {
        let Some(existing) = existing_by_identity.get(&event.event_identity) else {
            continue;
        };
        if !ens_v1_unwrapped_authority_renewal_resource_id_repair_allowed(
            existing,
            event,
            &normalized_event_identity_differences(existing, event),
        ) {
            continue;
        }
        let (Some(old_resource_id), Some(new_resource_id)) =
            (existing.resource_id, event.resource_id)
        else {
            continue;
        };
        let (Some(logical_name_id), Some(min_block_number)) =
            (existing.logical_name_id.as_ref(), existing.block_number)
        else {
            continue;
        };
        let labelhash = existing
            .after_state
            .get("labelhash")
            .and_then(|value| value.as_str())
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_default();
        let old_before_state = serialize_jsonb_value(
            &existing.before_state,
            "failed to serialize existing ENSv1 renewal before_state",
        )?;
        let new_before_state = serialize_jsonb_value(
            &event.before_state,
            "failed to serialize repaired ENSv1 renewal before_state",
        )?;
        let after_state = serialize_jsonb_value(
            &event.after_state,
            "failed to serialize ENSv1 renewal after_state",
        )?;

        event_identities.push(event.event_identity.clone());
        old_resource_ids.push(old_resource_id);
        new_resource_ids.push(new_resource_id);
        logical_name_ids.push(logical_name_id.clone());
        min_block_numbers.push(min_block_number);
        labelhashes.push(labelhash.to_ascii_lowercase());
        old_before_states.push(old_before_state);
        new_before_states.push(new_before_state);
        after_states.push(after_state);
    }

    if event_identities.is_empty() {
        return Ok(HashSet::new());
    }

    let repaired = sqlx::query_scalar::<_, String>(include_str!("ens_v1_renewal_resource_id.sql"))
        .bind(&event_identities)
        .bind(&old_resource_ids)
        .bind(&new_resource_ids)
        .bind(&logical_name_ids)
        .bind(&min_block_numbers)
        .bind(&labelhashes)
        .bind(&old_before_states)
        .bind(&new_before_states)
        .bind(&after_states)
        .fetch_all(&mut **executor)
        .await
        .context(
            "failed to repair ENSv1 unwrapped-authority renewal normalized-event resource_id",
        )?;

    let repaired = repaired.into_iter().collect::<HashSet<_>>();
    let rejected = event_identities
        .iter()
        .zip(old_resource_ids.iter())
        .zip(new_resource_ids.iter())
        .zip(logical_name_ids.iter())
        .zip(labelhashes.iter())
        .zip(old_before_states.iter())
        .zip(new_before_states.iter())
        .filter(|((((((event_identity, _), _), _), _), _), _)| {
            !repaired.contains(event_identity.as_str())
        })
        .map(
            |((((((event_identity, old_resource_id), new_resource_id), logical_name_id), labelhash), old_before_state), new_before_state)| {
                format!(
                    "{event_identity} (old_resource_id={old_resource_id}, new_resource_id={new_resource_id}, logical_name_id={logical_name_id}, labelhash={labelhash}, old_before_state={old_before_state}, new_before_state={new_before_state})"
                )
            },
        )
        .collect::<Vec<_>>();
    if !rejected.is_empty() {
        bail!(
            "ENSv1 renewal resource_id repair rejected invalid resource anchors for events: {}",
            rejected.join(", ")
        );
    }

    Ok(repaired)
}

pub(crate) async fn repair_ens_v1_unwrapped_authority_registration_release_before_states(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Result<HashSet<String>> {
    let mut event_identities = Vec::new();
    let mut resource_ids = Vec::new();
    let mut logical_name_ids = Vec::new();
    let mut old_before_states = Vec::new();
    let mut new_before_states = Vec::new();
    let mut after_states = Vec::new();

    for event in events {
        let Some(existing) = existing_by_identity.get(&event.event_identity) else {
            continue;
        };
        if !ens_v1_unwrapped_authority_registration_release_before_state_repair_allowed(
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
        old_before_states.push(serialize_jsonb_value(
            &existing.before_state,
            "failed to serialize existing ENSv1 registration release before_state",
        )?);
        new_before_states.push(serialize_jsonb_value(
            &event.before_state,
            "failed to serialize repaired ENSv1 registration release before_state",
        )?);
        after_states.push(serialize_jsonb_value(
            &event.after_state,
            "failed to serialize ENSv1 registration release after_state",
        )?);
    }

    if event_identities.is_empty() {
        return Ok(HashSet::new());
    }

    let repaired = sqlx::query_scalar::<_, String>(include_str!(
        "ens_v1_registration_release_before_state.sql"
    ))
    .bind(&event_identities)
    .bind(&resource_ids)
    .bind(&logical_name_ids)
    .bind(&old_before_states)
    .bind(&new_before_states)
    .bind(&after_states)
    .fetch_all(&mut **executor)
    .await
    .context("failed to repair ENSv1 unwrapped-authority registration release before_state")?;

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
            "ENSv1 registration release before_state repair rejected invalid resource anchors for events: {}",
            rejected.join(", ")
        );
    }

    Ok(repaired)
}

pub(crate) async fn repair_ens_v1_unwrapped_authority_renewal_before_states(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Result<HashSet<String>> {
    let mut event_identities = Vec::new();
    let mut resource_ids = Vec::new();
    let mut logical_name_ids = Vec::new();
    let mut block_numbers = Vec::new();
    let mut log_indexes = Vec::new();
    let mut old_before_states = Vec::new();
    let mut new_before_states = Vec::new();
    let mut after_states = Vec::new();

    for event in events {
        let Some(existing) = existing_by_identity.get(&event.event_identity) else {
            continue;
        };
        if !ens_v1_unwrapped_authority_renewal_before_state_repair_allowed(
            existing,
            event,
            &normalized_event_identity_differences(existing, event),
        ) {
            continue;
        }
        let (Some(resource_id), Some(logical_name_id), Some(block_number), Some(log_index)) = (
            existing.resource_id,
            existing.logical_name_id.as_ref(),
            existing.block_number,
            existing.log_index,
        ) else {
            continue;
        };

        event_identities.push(event.event_identity.clone());
        resource_ids.push(resource_id);
        logical_name_ids.push(logical_name_id.clone());
        block_numbers.push(block_number);
        log_indexes.push(log_index);
        old_before_states.push(serialize_jsonb_value(
            &existing.before_state,
            "failed to serialize existing ENSv1 renewal before_state",
        )?);
        new_before_states.push(serialize_jsonb_value(
            &event.before_state,
            "failed to serialize repaired ENSv1 renewal before_state",
        )?);
        after_states.push(serialize_jsonb_value(
            &event.after_state,
            "failed to serialize ENSv1 renewal after_state",
        )?);
    }

    if event_identities.is_empty() {
        return Ok(HashSet::new());
    }

    let repaired = sqlx::query_scalar::<_, String>(include_str!("ens_v1_renewal_before_state.sql"))
        .bind(&event_identities)
        .bind(&resource_ids)
        .bind(&logical_name_ids)
        .bind(&block_numbers)
        .bind(&log_indexes)
        .bind(&old_before_states)
        .bind(&new_before_states)
        .bind(&after_states)
        .fetch_all(&mut **executor)
        .await
        .context("failed to repair ENSv1 unwrapped-authority renewal before_state")?;

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
            "ENSv1 renewal before_state repair rejected invalid resource anchors for events: {}",
            rejected.join(", ")
        );
    }

    Ok(repaired)
}

pub(crate) fn ens_v1_unwrapped_authority_renewal_resource_id_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    if !renewal_resource_id_repair_differences_allowed(differing_fields) {
        return false;
    }
    if existing.resource_id.is_none()
        || incoming.resource_id.is_none()
        || existing.logical_name_id.is_none()
        || incoming.logical_name_id.is_none()
        || existing.block_number.is_none()
        || existing
            .after_state
            .get("expiry")
            .and_then(Value::as_i64)
            .is_none()
        || existing.namespace != "ens"
        || existing.chain_id.as_deref() != Some("ethereum-mainnet")
        || existing.source_family != "ens_v1_registrar_l1"
        || existing.derivation_kind != "ens_v1_unwrapped_authority"
        || !matches!(
            existing.event_kind.as_str(),
            "ExpiryChanged" | "RegistrationRenewed"
        )
    {
        return false;
    }

    existing.logical_name_id == incoming.logical_name_id
        && existing.after_state == incoming.after_state
        && (existing.before_state == incoming.before_state
            || renewal_before_state_expiry_repair_allowed(
                &existing.before_state,
                &incoming.before_state,
                &incoming.after_state,
            ))
}

pub(crate) fn ens_v1_unwrapped_authority_renewal_before_state_repair_allowed(
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
        || existing.block_number.is_none()
        || existing.log_index.is_none()
        || existing.namespace != "ens"
        || existing.chain_id.as_deref() != Some("ethereum-mainnet")
        || existing.source_family != "ens_v1_registrar_l1"
        || existing.derivation_kind != "ens_v1_unwrapped_authority"
        || !matches!(
            existing.event_kind.as_str(),
            "ExpiryChanged" | "RegistrationRenewed"
        )
        || existing.after_state != incoming.after_state
    {
        return false;
    }

    renewal_same_resource_before_state_expiry_repair_allowed(
        &existing.before_state,
        &incoming.before_state,
        &incoming.after_state,
    )
}

fn renewal_resource_id_repair_differences_allowed(differing_fields: &[&'static str]) -> bool {
    matches!(
        differing_fields,
        ["resource_id"] | ["resource_id", "before_state"]
    )
}

fn renewal_before_state_expiry_repair_allowed(
    existing_before_state: &Value,
    incoming_before_state: &Value,
    after_state: &Value,
) -> bool {
    let Some(existing_expiry) = existing_before_state.get("expiry").and_then(Value::as_i64) else {
        return false;
    };
    let Some(incoming_expiry) = incoming_before_state.get("expiry").and_then(Value::as_i64) else {
        return false;
    };
    let Some(after_expiry) = after_state.get("expiry").and_then(Value::as_i64) else {
        return false;
    };
    if existing_expiry != after_expiry || incoming_expiry == existing_expiry {
        return false;
    }
    let mut existing_without_expiry = existing_before_state.clone();
    let Some(existing_object) = existing_without_expiry.as_object_mut() else {
        return false;
    };
    existing_object.remove("expiry");

    let mut incoming_without_expiry = incoming_before_state.clone();
    let Some(incoming_object) = incoming_without_expiry.as_object_mut() else {
        return false;
    };
    incoming_object.remove("expiry");

    existing_without_expiry == incoming_without_expiry
}

fn renewal_same_resource_before_state_expiry_repair_allowed(
    existing_before_state: &Value,
    incoming_before_state: &Value,
    after_state: &Value,
) -> bool {
    let Some(existing_expiry) = existing_before_state.get("expiry").and_then(Value::as_i64) else {
        return false;
    };
    let Some(incoming_expiry) = incoming_before_state.get("expiry").and_then(Value::as_i64) else {
        return false;
    };
    let Some(after_expiry) = after_state.get("expiry").and_then(Value::as_i64) else {
        return false;
    };
    if incoming_expiry == existing_expiry || incoming_expiry == after_expiry {
        return false;
    }
    let mut existing_without_expiry = existing_before_state.clone();
    let Some(existing_object) = existing_without_expiry.as_object_mut() else {
        return false;
    };
    existing_object.remove("expiry");

    let mut incoming_without_expiry = incoming_before_state.clone();
    let Some(incoming_object) = incoming_without_expiry.as_object_mut() else {
        return false;
    };
    incoming_object.remove("expiry");

    existing_without_expiry == incoming_without_expiry
}

pub(crate) fn ens_v1_unwrapped_authority_registration_release_before_state_repair_allowed(
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
        || existing.source_family != "ens_v1_registrar_l1"
        || existing.derivation_kind != "ens_v1_unwrapped_authority"
        || existing.event_kind != "RegistrationReleased"
        || existing.after_state != incoming.after_state
    {
        return false;
    }

    registration_release_before_state_repair_allowed(
        &existing.before_state,
        &incoming.before_state,
        &incoming.after_state,
    )
}

fn registration_release_before_state_repair_allowed(
    existing_before_state: &Value,
    incoming_before_state: &Value,
    after_state: &Value,
) -> bool {
    if !after_state.get("released_at").is_some_and(Value::is_number)
        || !after_state
            .get("labelhash")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty())
    {
        return false;
    }

    let Some(existing_registrant) = existing_before_state
        .get("registrant")
        .and_then(Value::as_str)
    else {
        return false;
    };
    let Some(incoming_registrant) = incoming_before_state
        .get("registrant")
        .and_then(Value::as_str)
    else {
        return false;
    };
    if existing_registrant.trim().is_empty()
        || incoming_registrant.trim().is_empty()
        || existing_registrant.eq_ignore_ascii_case(incoming_registrant)
    {
        return false;
    }

    let Some(existing_expiry) = existing_before_state.get("expiry").and_then(Value::as_i64) else {
        return false;
    };
    let Some(incoming_expiry) = incoming_before_state.get("expiry").and_then(Value::as_i64) else {
        return false;
    };
    if existing_expiry != incoming_expiry {
        return false;
    }

    let mut existing_without_registrant = existing_before_state.clone();
    if let Some(object) = existing_without_registrant.as_object_mut() {
        object.remove("registrant");
    }
    let mut incoming_without_registrant = incoming_before_state.clone();
    if let Some(object) = incoming_without_registrant.as_object_mut() {
        object.remove("registrant");
    }

    existing_without_registrant == incoming_without_registrant
}
