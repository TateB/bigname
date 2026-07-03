use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use sqlx::Postgres;

use super::super::super::types::NormalizedEvent;
use super::super::{normalized_event_identity_differences, serialize_jsonb_value};
use super::ens_v1_registry_event_time_state::{
    authority_transfer_state_repair_allowed, permission_state_authority_repair_allowed,
    record_changed_text_value_state_repair_allowed, record_version_state_repair_allowed,
    registry_only_authority_state_resource_repair_allowed,
};

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

pub(crate) async fn supersede_basenames_registry_boundary_derivation_change_events(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
) -> Result<usize> {
    let mut event_identities = Vec::new();
    let mut resource_ids = Vec::new();
    let mut logical_name_ids = Vec::new();
    let mut block_numbers = Vec::new();
    let mut block_hashes = Vec::new();
    let mut event_kinds = Vec::new();
    let mut raw_fact_refs = Vec::new();
    let mut before_states = Vec::new();
    let mut after_states = Vec::new();
    let mut manifest_versions = Vec::new();
    let mut source_manifest_ids = Vec::new();
    let mut source_families = Vec::new();

    for event in events {
        if !basenames_registry_boundary_derivation_change_candidate(event) {
            continue;
        }
        let (Some(resource_id), Some(logical_name_id), Some(block_number), Some(block_hash)) = (
            event.resource_id,
            event.logical_name_id.as_ref(),
            event.block_number,
            event.block_hash.as_ref(),
        ) else {
            continue;
        };

        event_identities.push(event.event_identity.clone());
        resource_ids.push(resource_id);
        logical_name_ids.push(logical_name_id.clone());
        block_numbers.push(block_number);
        block_hashes.push(block_hash.clone());
        event_kinds.push(event.event_kind.clone());
        raw_fact_refs.push(serialize_jsonb_value(
            &event.raw_fact_ref,
            "failed to serialize Basenames registry boundary supersession raw_fact_ref",
        )?);
        before_states.push(serialize_jsonb_value(
            &event.before_state,
            "failed to serialize Basenames registry boundary supersession before_state",
        )?);
        after_states.push(serialize_jsonb_value(
            &event.after_state,
            "failed to serialize Basenames registry boundary supersession after_state",
        )?);
        manifest_versions.push(event.manifest_version);
        source_manifest_ids.push(event.source_manifest_id);
        source_families.push(event.source_family.clone());
    }

    if event_identities.is_empty() {
        return Ok(0);
    }

    let repair_results =
        sqlx::query_scalar::<_, String>(include_str!("ens_v1_registry_boundary_supersession.sql"))
            .bind(&event_identities)
            .bind(&resource_ids)
            .bind(&logical_name_ids)
            .bind(&block_numbers)
            .bind(&block_hashes)
            .bind(&event_kinds)
            .bind(&raw_fact_refs)
            .bind(&before_states)
            .bind(&after_states)
            .bind(&manifest_versions)
            .bind(&source_manifest_ids)
            .bind(&source_families)
            .fetch_all(&mut **executor)
            .await
            .context("failed to supersede Basenames registry boundary derivation-change events")?;

    let mut superseded = 0usize;
    let mut manifest_rejected = Vec::new();
    let mut resource_rejected = Vec::new();
    let mut state_rejected = Vec::new();
    for result in repair_results {
        if result.strip_prefix("superseded:").is_some() {
            superseded += 1;
        } else if let Some(rejection) = result.strip_prefix("manifest_mismatch:") {
            manifest_rejected.push(rejection.to_owned());
        } else if let Some(rejection) = result.strip_prefix("resource_mismatch:") {
            resource_rejected.push(rejection.to_owned());
        } else if let Some(rejection) = result.strip_prefix("state_mismatch:") {
            state_rejected.push(rejection.to_owned());
        } else {
            bail!("unexpected Basenames registry boundary supersession result: {result}");
        }
    }
    let mut rejection_summaries = Vec::new();
    if !manifest_rejected.is_empty() {
        rejection_summaries.push(format!(
            "manifest metadata mismatches: {}",
            manifest_rejected.join(", ")
        ));
    }
    if !resource_rejected.is_empty() {
        rejection_summaries.push(format!(
            "resource/provenance mismatches: {}",
            resource_rejected.join(", ")
        ));
    }
    if !state_rejected.is_empty() {
        rejection_summaries.push(format!(
            "state verification mismatches: {}",
            state_rejected.join(", ")
        ));
    }
    if !rejection_summaries.is_empty() {
        bail!(
            "Basenames registry boundary derivation-change supersession rejected {}",
            rejection_summaries.join("; ")
        );
    }

    Ok(superseded)
}

fn basenames_registry_boundary_derivation_change_candidate(event: &NormalizedEvent) -> bool {
    let base_boundary_event = event.namespace == "basenames"
        && event.chain_id.as_deref() == Some("base-mainnet")
        && event.derivation_kind == "ens_v1_unwrapped_authority"
        && event.transaction_hash.is_none()
        && event.log_index.is_none()
        && event.logical_name_id.is_some()
        && event.resource_id.is_some()
        && event.block_number.is_some()
        && event.block_hash.is_some()
        && event
            .raw_fact_ref
            .get("kind")
            .and_then(|value| value.as_str())
            == Some("raw_block");

    base_boundary_event
        && ((event.source_family == "basenames_base_registry"
            && (matches!(
                event.event_kind.as_str(),
                "AuthorityEpochChanged" | "PermissionChanged" | "SurfaceBound" | "SurfaceUnbound"
            ) || (event.event_kind == "ResolverChanged"
                && event
                    .after_state
                    .get("source_event")
                    .and_then(|value| value.as_str())
                    == Some("AuthorityEpochChanged"))))
            || (event.source_family == "basenames_base_registrar"
                && event.event_kind == "AuthorityEpochChanged"))
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
        || existing.derivation_kind != "ens_v1_unwrapped_authority"
        || !registry_event_time_resource_repair_source_allowed(existing)
        || (basenames_registry_boundary_derivation_change_candidate(existing)
            && matches!(
                existing.event_kind.as_str(),
                "PermissionChanged" | "ResolverChanged"
            ))
        || !matches!(
            existing.event_kind.as_str(),
            "ResolverChanged"
                | "RecordChanged"
                | "RecordVersionChanged"
                | "PermissionChanged"
                | "AuthorityTransferred"
                | "AuthorityEpochChanged"
                | "SurfaceBound"
                | "SurfaceUnbound"
        )
    {
        return false;
    }

    if differing_fields.len() == 1
        && !matches!(
            existing.event_kind.as_str(),
            "AuthorityEpochChanged" | "SurfaceBound" | "SurfaceUnbound"
        )
    {
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

    if matches!(
        existing.event_kind.as_str(),
        "AuthorityEpochChanged" | "SurfaceBound" | "SurfaceUnbound"
    ) {
        return registry_only_authority_state_resource_repair_allowed(
            existing.event_kind.as_str(),
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

fn registry_event_time_resource_repair_source_allowed(existing: &NormalizedEvent) -> bool {
    (existing.namespace == "ens"
        && existing.chain_id.as_deref() == Some("ethereum-mainnet")
        && matches!(
            existing.source_family.as_str(),
            "ens_v1_registry_l1" | "ens_v1_registrar_l1" | "ens_v1_resolver_l1"
        ))
        || (existing.namespace == "basenames"
            && existing.chain_id.as_deref() == Some("base-mainnet")
            && existing.source_family == "basenames_base_registry"
            && matches!(
                existing.event_kind.as_str(),
                "AuthorityTransferred" | "PermissionChanged" | "ResolverChanged"
            ))
}

pub(crate) fn ens_v1_unwrapped_authority_registry_event_time_before_state_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    if !matches!(differing_fields, ["before_state"]) {
        return false;
    }
    let source_allowed = matches!(
        (
            existing.namespace.as_str(),
            existing.chain_id.as_deref(),
            existing.source_family.as_str(),
            existing.event_kind.as_str(),
        ),
        (
            "ens",
            Some("ethereum-mainnet"),
            "ens_v1_registry_l1",
            "AuthorityTransferred"
        ) | (
            "ens",
            Some("ethereum-mainnet"),
            "ens_v1_resolver_l1",
            "RecordVersionChanged"
        ) | (
            "basenames",
            Some("base-mainnet"),
            "basenames_base_registry",
            "AuthorityTransferred"
        )
    );
    if existing.resource_id.is_none()
        || existing.resource_id != incoming.resource_id
        || existing.logical_name_id.is_none()
        || incoming.logical_name_id.is_none()
        || existing.logical_name_id != incoming.logical_name_id
        || existing.derivation_kind != "ens_v1_unwrapped_authority"
        || !source_allowed
    {
        return false;
    }
    if existing.event_kind == "AuthorityTransferred" {
        return authority_transfer_state_repair_allowed(
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
