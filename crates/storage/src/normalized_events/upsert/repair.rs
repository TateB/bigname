use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use sqlx::Postgres;
use uuid::Uuid;

use super::super::types::NormalizedEvent;
use super::{normalized_event_identity_differences, serialize_jsonb_value};

#[path = "repair/ens_v1_authority_epoch_registry_owner.rs"]
mod ens_v1_authority_epoch_registry_owner;
#[path = "repair/ens_v1_authority_epoch_resolver_boundary.rs"]
mod ens_v1_authority_epoch_resolver_boundary;
#[path = "repair/ens_v1_registry_event_time.rs"]
mod ens_v1_registry_event_time;
#[path = "repair/ens_v1_registry_event_time_null_resource.rs"]
mod ens_v1_registry_event_time_null_resource;
#[path = "repair/ens_v1_registry_event_time_state.rs"]
mod ens_v1_registry_event_time_state;
#[path = "repair/ens_v1_registry_resolver_before_state.rs"]
mod ens_v1_registry_resolver_before_state;
#[path = "repair/ens_v1_registry_resolver_observation_key.rs"]
mod ens_v1_registry_resolver_observation_key;
#[path = "repair/ens_v1_renewal.rs"]
mod ens_v1_renewal;
#[path = "repair/ens_v1_reverse_resolver_before_state.rs"]
mod ens_v1_reverse_resolver_before_state;
#[path = "repair/ens_v1_same_tx_registration_setup.rs"]
mod ens_v1_same_tx_registration_setup;
#[path = "repair/ens_v1_wrapper_token_before_state.rs"]
mod ens_v1_wrapper_token_before_state;

pub(super) use ens_v1_authority_epoch_registry_owner::{
    ens_v1_authority_epoch_registry_owner_after_state_repair_allowed,
    repair_ens_v1_authority_epoch_registry_owner_after_states,
};
pub(super) use ens_v1_authority_epoch_resolver_boundary::{
    ens_v1_authority_epoch_resolver_boundary_after_state_repair_allowed,
    repair_ens_v1_authority_epoch_resolver_boundary_after_states,
};
pub(super) use ens_v1_registry_event_time::{
    ens_v1_unwrapped_authority_registry_event_time_before_state_repair_allowed,
    ens_v1_unwrapped_authority_registry_event_time_resource_id_repair_allowed,
    repair_ens_v1_unwrapped_authority_registry_event_time_before_states,
    repair_ens_v1_unwrapped_authority_registry_event_time_resource_ids,
    supersede_basenames_registry_boundary_derivation_change_events,
};
pub(super) use ens_v1_registry_event_time_null_resource::{
    ens_v1_unwrapped_authority_registry_event_time_null_resource_id_repair_allowed,
    repair_ens_v1_unwrapped_authority_registry_event_time_null_resource_ids,
};
pub(super) use ens_v1_registry_resolver_before_state::{
    ens_v1_registry_resolver_before_state_repair_allowed,
    repair_ens_v1_registry_resolver_before_states,
};
pub(super) use ens_v1_registry_resolver_observation_key::{
    ens_v1_registry_resolver_observation_key_after_state_repair_allowed,
    repair_ens_v1_registry_resolver_observation_key_after_states,
};
pub(super) use ens_v1_renewal::{
    ens_v1_unwrapped_authority_registration_release_before_state_repair_allowed,
    ens_v1_unwrapped_authority_renewal_before_state_repair_allowed,
    ens_v1_unwrapped_authority_renewal_resource_id_repair_allowed,
    repair_ens_v1_unwrapped_authority_registration_release_before_states,
    repair_ens_v1_unwrapped_authority_renewal_before_states,
    repair_ens_v1_unwrapped_authority_renewal_resource_ids,
};
pub(super) use ens_v1_reverse_resolver_before_state::{
    ens_v1_reverse_resolver_before_state_repair_allowed,
    repair_ens_v1_reverse_resolver_before_states,
};
pub(super) use ens_v1_same_tx_registration_setup::{
    ens_v1_same_tx_registration_setup_before_state_repair_allowed,
    repair_ens_v1_same_tx_registration_setup_before_states,
};
pub(super) use ens_v1_wrapper_token_before_state::{
    ens_v1_wrapper_token_before_state_repair_allowed, repair_ens_v1_wrapper_token_before_states,
};

pub(super) async fn repair_after_state_conflicts(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Result<usize> {
    let basenames_primary_claim_source =
        repair_basenames_primary_claim_source_after_states(executor, events, existing_by_identity)
            .await?;
    let resolver_observation_key = repair_ens_v1_registry_resolver_observation_key_after_states(
        executor,
        events,
        existing_by_identity,
    )
    .await?;
    let authority_epoch_registry_owner = repair_ens_v1_authority_epoch_registry_owner_after_states(
        executor,
        events,
        existing_by_identity,
    )
    .await?;
    let authority_epoch_resolver_boundary =
        repair_ens_v1_authority_epoch_resolver_boundary_after_states(
            executor,
            events,
            existing_by_identity,
        )
        .await?;
    let same_tx_registration_setup = repair_ens_v1_same_tx_registration_setup_before_states(
        executor,
        events,
        existing_by_identity,
    )
    .await?;
    let wrapper_token_before_state =
        repair_ens_v1_wrapper_token_before_states(executor, events, existing_by_identity).await?;
    let registry_event_time_before_state =
        repair_ens_v1_unwrapped_authority_registry_event_time_before_states(
            executor,
            events,
            existing_by_identity,
        )
        .await?;
    let reverse_resolver_before_state =
        repair_ens_v1_reverse_resolver_before_states(executor, events, existing_by_identity)
            .await?;
    let registry_resolver_before_state =
        repair_ens_v1_registry_resolver_before_states(executor, events, existing_by_identity)
            .await?;
    let renewal_before_state = repair_ens_v1_unwrapped_authority_renewal_before_states(
        executor,
        events,
        existing_by_identity,
    )
    .await?;
    let registration_release_before_state =
        repair_ens_v1_unwrapped_authority_registration_release_before_states(
            executor,
            events,
            existing_by_identity,
        )
        .await?;

    Ok(basenames_primary_claim_source.len()
        + resolver_observation_key.len()
        + authority_epoch_registry_owner.len()
        + authority_epoch_resolver_boundary.len()
        + same_tx_registration_setup.len()
        + wrapper_token_before_state.len()
        + registry_event_time_before_state.len()
        + reverse_resolver_before_state.len()
        + registry_resolver_before_state.len()
        + renewal_before_state.len()
        + registration_release_before_state.len())
}

pub(super) async fn repair_resource_id_conflicts(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Result<usize> {
    let renewal_resource_ids = repair_ens_v1_unwrapped_authority_renewal_resource_ids(
        executor,
        events,
        existing_by_identity,
    )
    .await?;
    let registry_event_time_resource_ids =
        repair_ens_v1_unwrapped_authority_registry_event_time_resource_ids(
            executor,
            events,
            existing_by_identity,
        )
        .await?;
    let registry_event_time_null_resource_ids =
        repair_ens_v1_unwrapped_authority_registry_event_time_null_resource_ids(
            executor,
            events,
            existing_by_identity,
        )
        .await?;

    Ok(renewal_resource_ids.len()
        + registry_event_time_resource_ids.len()
        + registry_event_time_null_resource_ids.len())
}

pub(super) fn normalized_event_identity_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    basenames_primary_claim_source_after_state_repair_allowed(existing, incoming, differing_fields)
        || ens_v1_registry_resolver_observation_key_after_state_repair_allowed(
            existing,
            incoming,
            differing_fields,
        )
        || ens_v1_authority_epoch_registry_owner_after_state_repair_allowed(
            existing,
            incoming,
            differing_fields,
        )
        || ens_v1_authority_epoch_resolver_boundary_after_state_repair_allowed(
            existing,
            incoming,
            differing_fields,
        )
        || ens_v1_same_tx_registration_setup_before_state_repair_allowed(
            existing,
            incoming,
            differing_fields,
        )
        || ens_v1_wrapper_token_before_state_repair_allowed(existing, incoming, differing_fields)
        || ens_v1_unwrapped_authority_registry_event_time_before_state_repair_allowed(
            existing,
            incoming,
            differing_fields,
        )
        || ens_v1_reverse_resolver_before_state_repair_allowed(existing, incoming, differing_fields)
        || ens_v1_registry_resolver_before_state_repair_allowed(
            existing,
            incoming,
            differing_fields,
        )
        || ens_v1_unwrapped_authority_registration_release_before_state_repair_allowed(
            existing,
            incoming,
            differing_fields,
        )
        || ens_v1_unwrapped_authority_renewal_before_state_repair_allowed(
            existing,
            incoming,
            differing_fields,
        )
        || ens_v1_unwrapped_authority_renewal_resource_id_repair_allowed(
            existing,
            incoming,
            differing_fields,
        )
        || ens_v1_unwrapped_authority_registry_event_time_resource_id_repair_allowed(
            existing,
            incoming,
            differing_fields,
        )
        || ens_v1_unwrapped_authority_registry_event_time_null_resource_id_repair_allowed(
            existing,
            incoming,
            differing_fields,
        )
        || ens_v1_unwrapped_authority_boundary_manifest_metadata_mismatch_allowed(
            existing,
            incoming,
            differing_fields,
        )
}

pub(super) async fn repair_basenames_primary_claim_source_after_states(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Result<HashSet<String>> {
    let mut event_identities = Vec::new();
    let mut old_after_states = Vec::new();
    let mut new_after_states = Vec::new();

    for event in events {
        let Some(existing) = existing_by_identity.get(&event.event_identity) else {
            continue;
        };
        if !basenames_primary_claim_source_after_state_repair_allowed(
            existing,
            event,
            &normalized_event_identity_differences(existing, event),
        ) {
            continue;
        }

        event_identities.push(event.event_identity.clone());
        old_after_states.push(serialize_jsonb_value(
            &existing.after_state,
            "failed to serialize existing normalized-event after_state",
        )?);
        new_after_states.push(serialize_jsonb_value(
            &event.after_state,
            "failed to serialize repaired normalized-event after_state",
        )?);
    }

    if event_identities.is_empty() {
        return Ok(HashSet::new());
    }

    let repaired = sqlx::query_scalar::<_, String>(
        r#"
        WITH input AS (
            SELECT *
            FROM unnest(
                $1::TEXT[],
                $2::TEXT[],
                $3::TEXT[]
            ) AS input(
                event_identity,
                old_after_state,
                new_after_state
            )
        ),
        updated AS (
            UPDATE normalized_events event
            SET
                after_state = input.new_after_state::JSONB,
                observed_at = now()
            FROM input
            WHERE event.event_identity = input.event_identity
              AND event.after_state IS NOT DISTINCT FROM input.old_after_state::JSONB
            RETURNING
                event.event_identity,
                event.normalized_event_id,
                event.canonicality_state,
                input.old_after_state,
                input.new_after_state
        ),
        queued_changes AS (
            INSERT INTO projection_normalized_event_changes (
                normalized_event_id,
                changed_at,
                change_kind,
                canonicality_state
            )
            SELECT
                normalized_event_id,
                now(),
                'canonicality_update',
                canonicality_state
            FROM updated
            RETURNING
                change_id,
                normalized_event_id,
                changed_at
        ),
        candidate_primary_keys AS (
            SELECT
                'primary_names_current'::TEXT AS projection,
                lower(claim_source ->> 'address')
                    || ':' || COALESCE(claim_source ->> 'namespace', 'basenames')
                    || ':' || (claim_source ->> 'coin_type') AS projection_key,
                jsonb_build_object(
                    'address', lower(claim_source ->> 'address'),
                    'namespace', COALESCE(claim_source ->> 'namespace', 'basenames'),
                    'coin_type', claim_source ->> 'coin_type'
                ) AS key_payload,
                updated.normalized_event_id,
                queued_changes.change_id,
                queued_changes.changed_at
            FROM updated
            JOIN queued_changes
              ON queued_changes.normalized_event_id = updated.normalized_event_id
            CROSS JOIN LATERAL (
                VALUES
                    (updated.old_after_state::JSONB -> 'primary_claim_source'),
                    (updated.new_after_state::JSONB -> 'primary_claim_source')
            ) AS tuple(claim_source)
            WHERE claim_source ->> 'address' IS NOT NULL
              AND claim_source ->> 'address' <> ''
              AND COALESCE(claim_source ->> 'namespace', 'basenames') IS NOT NULL
              AND COALESCE(claim_source ->> 'namespace', 'basenames') <> ''
              AND claim_source ->> 'coin_type' IS NOT NULL
              AND claim_source ->> 'coin_type' <> ''
        ),
        queued_primary_invalidations AS (
            INSERT INTO projection_invalidations (
                projection,
                projection_key,
                key_payload,
                first_change_id,
                last_change_id,
                first_normalized_event_id,
                last_normalized_event_id,
                last_changed_at,
                invalidated_at
            )
            SELECT
                projection,
                projection_key,
                key_payload,
                MIN(change_id),
                MAX(change_id),
                MIN(normalized_event_id),
                MAX(normalized_event_id),
                MAX(changed_at),
                now()
            FROM candidate_primary_keys
            WHERE projection_key IS NOT NULL
              AND btrim(projection_key) <> ''
            GROUP BY projection, projection_key, key_payload
            ON CONFLICT (projection, projection_key)
            DO UPDATE SET
                key_payload = EXCLUDED.key_payload,
                generation = projection_invalidations.generation + 1,
                first_change_id = LEAST(
                    projection_invalidations.first_change_id,
                    EXCLUDED.first_change_id
                ),
                last_change_id = GREATEST(
                    projection_invalidations.last_change_id,
                    EXCLUDED.last_change_id
                ),
                first_normalized_event_id = LEAST(
                    projection_invalidations.first_normalized_event_id,
                    EXCLUDED.first_normalized_event_id
                ),
                last_normalized_event_id = GREATEST(
                    projection_invalidations.last_normalized_event_id,
                    EXCLUDED.last_normalized_event_id
                ),
                last_changed_at = GREATEST(
                    projection_invalidations.last_changed_at,
                    EXCLUDED.last_changed_at
                ),
                invalidated_at = EXCLUDED.invalidated_at,
                claim_token = NULL,
                claimed_at = NULL,
                last_failure_reason = NULL,
                last_failure_at = NULL
            RETURNING projection_key
        )
        SELECT event_identity
        FROM updated
        "#,
    )
    .bind(&event_identities)
    .bind(&old_after_states)
    .bind(&new_after_states)
    .fetch_all(&mut **executor)
    .await
    .context("failed to repair Basenames primary-claim normalized-event after_state")?;

    Ok(repaired.into_iter().collect())
}

pub(super) fn basenames_primary_claim_source_after_state_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    if differing_fields.len() != 1 || differing_fields[0] != "after_state" {
        return false;
    }
    if existing.namespace != "basenames"
        || existing.source_family != "basenames_base_primary"
        || existing.chain_id.as_deref() != Some("base-mainnet")
        || existing.derivation_kind != "ens_v1_reverse_claim"
        || existing.event_kind != "RecordChanged"
        || existing.logical_name_id.is_some()
        || existing.resource_id.is_some()
        || existing
            .after_state
            .get("record_key")
            .and_then(|value| value.as_str())
            != Some("name")
    {
        return false;
    }

    if after_state_without_primary_claim_source(&existing.after_state)
        != after_state_without_primary_claim_source(&incoming.after_state)
    {
        return false;
    }

    let Some(existing_source) = existing.after_state.get("primary_claim_source") else {
        return false;
    };
    let Some(incoming_source) = incoming.after_state.get("primary_claim_source") else {
        return false;
    };

    primary_claim_source_required_text(existing_source, &["coin_type"]) == Some("60")
        && primary_claim_source_required_text(
            existing_source,
            &["claim_provenance", "emitting_address"],
        ) == Some("0x79ea96012eea67a83431f1701b3dff7e37f9e282")
        && primary_claim_source_uuid(
            existing_source,
            &["claim_provenance", "contract_instance_id"],
        )
        .is_some()
        && primary_claim_source_required_text(incoming_source, &["coin_type"]) == Some("2147492101")
        && primary_claim_source_required_text(
            incoming_source,
            &["claim_provenance", "emitting_address"],
        ) == Some("0x0000000000d8e504002cc26e3ec46d81971c1664")
        && primary_claim_source_uuid(
            incoming_source,
            &["claim_provenance", "contract_instance_id"],
        )
        .is_some()
        && primary_claim_source_required_text_matches(
            existing_source,
            incoming_source,
            &["address"],
        )
        && primary_claim_source_required_text_matches(
            existing_source,
            incoming_source,
            &["namespace"],
        )
        && primary_claim_source_required_text_matches(
            existing_source,
            incoming_source,
            &["reverse_node"],
        )
        && primary_claim_source_required_text_matches(
            existing_source,
            incoming_source,
            &["reverse_name"],
        )
        && primary_claim_source_required_text_matches(
            existing_source,
            incoming_source,
            &["claim_provenance", "source_family"],
        )
        && primary_claim_source_required_text_matches(
            existing_source,
            incoming_source,
            &["claim_provenance", "contract_role"],
        )
}

pub(super) fn ens_v1_unwrapped_authority_boundary_manifest_metadata_mismatch_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    if differing_fields != ["manifest_version", "source_manifest_id"] {
        return false;
    }
    existing.namespace == "ens"
        && incoming.namespace == "ens"
        && existing.chain_id.as_deref() == Some("ethereum-mainnet")
        && incoming.chain_id.as_deref() == Some("ethereum-mainnet")
        && existing.derivation_kind == "ens_v1_unwrapped_authority"
        && incoming.derivation_kind == "ens_v1_unwrapped_authority"
        && existing.source_family == "ens_v1_registry_l1"
        && incoming.source_family == "ens_v1_registry_l1"
        && matches!(
            existing.event_kind.as_str(),
            "AuthorityEpochChanged" | "ResolverChanged" | "SurfaceBound" | "SurfaceUnbound"
        )
        && existing.event_kind == incoming.event_kind
        && existing.transaction_hash.is_none()
        && incoming.transaction_hash.is_none()
        && existing.log_index.is_none()
        && incoming.log_index.is_none()
        && existing.source_manifest_id.is_some()
        && incoming.source_manifest_id.is_none()
        && incoming.manifest_version == 1
}

fn after_state_without_primary_claim_source(
    value: &serde_json::Value,
) -> Option<serde_json::Value> {
    let mut object = value.as_object()?.clone();
    object.remove("primary_claim_source");
    Some(serde_json::Value::Object(object))
}

fn primary_claim_source_text<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_str()
}

fn primary_claim_source_required_text<'a>(
    value: &'a serde_json::Value,
    path: &[&str],
) -> Option<&'a str> {
    primary_claim_source_text(value, path).filter(|value| !value.trim().is_empty())
}

fn primary_claim_source_required_text_matches(
    existing: &serde_json::Value,
    incoming: &serde_json::Value,
    path: &[&str],
) -> bool {
    match (
        primary_claim_source_required_text(existing, path),
        primary_claim_source_required_text(incoming, path),
    ) {
        (Some(existing), Some(incoming)) => existing == incoming,
        _ => false,
    }
}

fn primary_claim_source_uuid(value: &serde_json::Value, path: &[&str]) -> Option<Uuid> {
    let uuid: Uuid = primary_claim_source_required_text(value, path)?
        .parse()
        .ok()?;
    if uuid.is_nil() { None } else { Some(uuid) }
}
