use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::Postgres;

use super::super::super::types::NormalizedEvent;
use super::super::{normalized_event_identity_differences, serialize_jsonb_value};

pub(crate) async fn repair_ens_v1_same_tx_registration_setup_before_states(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Result<HashSet<String>> {
    let mut event_identities = Vec::new();
    let mut old_before_states = Vec::new();
    let mut new_before_states = Vec::new();
    let mut resource_ids = Vec::new();
    let mut logical_name_ids = Vec::new();
    let mut block_hashes = Vec::new();
    let mut transaction_hashes = Vec::new();
    let mut log_indexes = Vec::new();
    let mut event_kinds = Vec::new();

    for event in events {
        let Some(existing) = existing_by_identity.get(&event.event_identity) else {
            continue;
        };
        if !ens_v1_same_tx_registration_setup_before_state_repair_allowed(
            existing,
            event,
            &normalized_event_identity_differences(existing, event),
        ) {
            continue;
        }
        let (
            Some(resource_id),
            Some(logical_name_id),
            Some(block_hash),
            Some(transaction_hash),
            Some(log_index),
        ) = (
            event.resource_id,
            event.logical_name_id.as_ref(),
            event.block_hash.as_ref(),
            event.transaction_hash.as_ref(),
            event.log_index,
        )
        else {
            continue;
        };

        event_identities.push(event.event_identity.clone());
        old_before_states.push(serialize_jsonb_value(
            &existing.before_state,
            "failed to serialize existing same-transaction registration before_state",
        )?);
        new_before_states.push(serialize_jsonb_value(
            &event.before_state,
            "failed to serialize repaired same-transaction registration before_state",
        )?);
        resource_ids.push(resource_id);
        logical_name_ids.push(logical_name_id.clone());
        block_hashes.push(block_hash.clone());
        transaction_hashes.push(transaction_hash.clone());
        log_indexes.push(log_index);
        event_kinds.push(event.event_kind.clone());
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
                $3::TEXT[],
                $4::UUID[],
                $5::TEXT[],
                $6::TEXT[],
                $7::TEXT[],
                $8::BIGINT[],
                $9::TEXT[]
            ) AS input(
                event_identity,
                old_before_state,
                new_before_state,
                registrar_resource_id,
                logical_name_id,
                block_hash,
                transaction_hash,
                registration_log_index,
                event_kind
            )
        ),
        verified_registration AS (
            SELECT
                input.*,
                event.normalized_event_id,
                event.namespace,
                event.chain_id,
                event.source_family,
                event.resource_id,
                event.canonicality_state
            FROM input
            JOIN normalized_events event
              ON event.event_identity = input.event_identity
             AND event.resource_id = input.registrar_resource_id
             AND event.logical_name_id = input.logical_name_id
             AND event.block_hash = input.block_hash
             AND event.transaction_hash = input.transaction_hash
             AND event.log_index = input.registration_log_index
             AND event.event_kind = input.event_kind
             AND event.derivation_kind = 'ens_v1_unwrapped_authority'
              AND event.before_state IS NOT DISTINCT FROM input.old_before_state::JSONB
              AND event.after_state->>'authority_kind' = 'registrar'
              AND COALESCE(event.after_state->>'authority_key', '') <> ''
              AND COALESCE(event.after_state->>'labelhash', '') <> ''
            JOIN resources registrar_resource
              ON registrar_resource.resource_id = event.resource_id
             AND registrar_resource.chain_id = event.chain_id
             AND registrar_resource.canonicality_state IN (
                 'canonical'::canonicality_state,
                 'safe'::canonicality_state,
                 'finalized'::canonicality_state
             )
             AND registrar_resource.provenance->>'authority_kind' = 'registrar'
             AND registrar_resource.provenance->>'logical_name_id' = event.logical_name_id
             AND registrar_resource.provenance->>'authority_key' =
                 event.after_state->>'authority_key'
             AND lower(registrar_resource.provenance->>'labelhash') =
                 lower(event.after_state->>'labelhash')
            WHERE (
                    event.namespace = 'ens'
                AND event.chain_id = 'ethereum-mainnet'
                AND event.source_family = 'ens_v1_registrar_l1'
                AND input.event_kind = 'RegistrationGranted'
                AND input.old_before_state::JSONB->>'authority_kind' = 'registry_only'
                AND input.new_before_state::JSONB->>'authority_kind' IS NULL
                AND COALESCE(input.new_before_state::JSONB->>'authority_key', '') = ''
            )
               OR (
                    event.namespace = 'basenames'
                AND event.chain_id = 'base-mainnet'
                AND event.source_family = 'basenames_base_registrar'
                AND input.event_kind = 'RegistrationGranted'
                AND input.old_before_state::JSONB->>'authority_kind' = 'registry_only'
                AND COALESCE(input.old_before_state::JSONB->>'authority_key', '') = ''
                AND input.new_before_state::JSONB->>'authority_kind' IS NULL
                AND COALESCE(input.new_before_state::JSONB->>'authority_key', '') = ''
            )
        ),
        updated_registration AS (
            UPDATE normalized_events event
            SET
                before_state = registration.new_before_state::JSONB,
                observed_at = now()
            FROM verified_registration registration
            WHERE event.normalized_event_id = registration.normalized_event_id
            RETURNING
                event.event_identity,
                event.normalized_event_id,
                registration.namespace,
                registration.chain_id,
                event.logical_name_id,
                event.resource_id,
                event.canonicality_state,
                registration.block_hash,
                registration.transaction_hash,
                registration.registration_log_index
        ),
        stale_setup_events AS (
            SELECT
                stale.normalized_event_id,
                stale.logical_name_id,
                stale.resource_id,
                stale.canonicality_state
            FROM updated_registration registration
            JOIN normalized_events stale
              ON stale.namespace = registration.namespace
             AND stale.chain_id = registration.chain_id
             AND stale.derivation_kind = 'ens_v1_unwrapped_authority'
             AND stale.logical_name_id = registration.logical_name_id
             AND stale.resource_id <> registration.resource_id
             AND stale.block_hash = registration.block_hash
             AND stale.canonicality_state IN (
                 'canonical'::canonicality_state,
                 'safe'::canonicality_state,
                 'finalized'::canonicality_state
            )
            JOIN resources stale_resource
              ON stale_resource.resource_id = stale.resource_id
             AND stale_resource.chain_id = registration.chain_id
             AND stale_resource.provenance->>'authority_kind' = 'registry_only'
             AND (
                    registration.namespace = 'ens'
                 OR (
                        registration.namespace = 'basenames'
                    AND stale_resource.provenance->>'authority_key' = concat(
                        'registry-only:',
                        stale_resource.chain_id,
                        ':',
                        stale_resource.provenance->>'labelhash'
                    )
                 )
             )
            WHERE (
                    stale.transaction_hash = registration.transaction_hash
                AND stale.log_index IS NOT NULL
                AND stale.log_index < registration.registration_log_index
                AND stale.event_kind IN ('AuthorityTransferred', 'PermissionChanged')
            )
               OR (
                    stale.transaction_hash IS NULL
                AND stale.event_kind IN (
                    'AuthorityEpochChanged',
                    'ResolverChanged',
                    'SurfaceBound',
                    'SurfaceUnbound'
                )
            )
        ),
        orphaned_setup_events AS (
            UPDATE normalized_events event
            SET
                canonicality_state = 'orphaned'::canonicality_state,
                observed_at = now()
            FROM stale_setup_events stale
            WHERE event.normalized_event_id = stale.normalized_event_id
            RETURNING
                event.normalized_event_id,
                event.logical_name_id,
                event.resource_id,
                stale.canonicality_state
        ),
        changed_events AS (
            SELECT
                normalized_event_id,
                logical_name_id,
                resource_id,
                canonicality_state
            FROM updated_registration

            UNION ALL

            SELECT
                normalized_event_id,
                logical_name_id,
                resource_id,
                canonicality_state
            FROM orphaned_setup_events
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
            FROM changed_events
            RETURNING
                change_id,
                normalized_event_id,
                changed_at
        ),
        candidate_keys AS (
            SELECT
                'name_current'::TEXT AS projection,
                changed.logical_name_id AS projection_key,
                jsonb_build_object('logical_name_id', changed.logical_name_id) AS key_payload,
                changed.normalized_event_id,
                queued.change_id,
                queued.changed_at
            FROM changed_events changed
            JOIN queued_changes queued
              ON queued.normalized_event_id = changed.normalized_event_id
            WHERE changed.logical_name_id IS NOT NULL

            UNION ALL

            SELECT
                'permissions_current'::TEXT AS projection,
                changed.resource_id::TEXT AS projection_key,
                jsonb_build_object('resource_id', changed.resource_id::TEXT) AS key_payload,
                changed.normalized_event_id,
                queued.change_id,
                queued.changed_at
            FROM changed_events changed
            JOIN queued_changes queued
              ON queued.normalized_event_id = changed.normalized_event_id
            WHERE changed.resource_id IS NOT NULL
        ),
        queued_invalidations AS (
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
            FROM candidate_keys
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
        FROM updated_registration
        "#,
    )
    .bind(&event_identities)
    .bind(&old_before_states)
    .bind(&new_before_states)
    .bind(&resource_ids)
    .bind(&logical_name_ids)
    .bind(&block_hashes)
    .bind(&transaction_hashes)
    .bind(&log_indexes)
    .bind(&event_kinds)
    .fetch_all(&mut **executor)
    .await
    .context("failed to repair ENSv1 same-transaction registration setup before_state")?;

    let repaired = repaired.into_iter().collect::<HashSet<_>>();
    let rejected = event_identities
        .iter()
        .filter(|event_identity| !repaired.contains(event_identity.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !rejected.is_empty() {
        anyhow::bail!(
            "ENSv1 same-transaction registration setup before_state repair rejected invalid anchors for events: {}",
            rejected.join(", ")
        );
    }

    Ok(repaired)
}

pub(crate) fn ens_v1_same_tx_registration_setup_before_state_repair_allowed(
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
        ),
        ("ens", Some("ethereum-mainnet"), "ens_v1_registrar_l1")
            | (
                "basenames",
                Some("base-mainnet"),
                "basenames_base_registrar"
            )
    );
    if existing.derivation_kind != "ens_v1_unwrapped_authority"
        || !source_allowed
        || existing.event_kind != "RegistrationGranted"
        || existing.logical_name_id != incoming.logical_name_id
        || existing.resource_id != incoming.resource_id
        || existing.block_hash != incoming.block_hash
        || existing.transaction_hash != incoming.transaction_hash
        || existing.log_index != incoming.log_index
        || existing.after_state != incoming.after_state
    {
        return false;
    }

    before_state_without_authority(&existing.before_state)
        == before_state_without_authority(&incoming.before_state)
        && existing
            .before_state
            .get("authority_kind")
            .and_then(Value::as_str)
            == Some("registry_only")
        && incoming_authority_shape_allowed(&incoming.before_state)
        && existing_registry_only_authority_shape_allowed(
            existing.namespace.as_str(),
            existing.chain_id.as_deref(),
            &existing.before_state,
        )
        && registry_only_authority_key_shape_allowed(
            incoming.chain_id.as_deref(),
            &incoming.before_state,
        )
}

fn before_state_without_authority(before_state: &Value) -> Value {
    let mut value = before_state.clone();
    if let Some(object) = value.as_object_mut() {
        object.remove("authority_kind");
        object.remove("authority_key");
    }
    value
}

fn incoming_authority_shape_allowed(before_state: &Value) -> bool {
    before_state
        .get("authority_kind")
        .is_none_or(|value| value.is_null())
        && before_state.get("authority_key").is_none_or(|value| {
            value.is_null() || value.as_str().is_some_and(|value| value.is_empty())
        })
}

fn existing_registry_only_authority_shape_allowed(
    namespace: &str,
    chain_id: Option<&str>,
    before_state: &Value,
) -> bool {
    if namespace == "basenames" && chain_id == Some("base-mainnet") {
        return before_state.get("authority_key").is_none_or(|value| {
            value.is_null() || value.as_str().is_some_and(|value| value.is_empty())
        });
    }

    registry_only_authority_key_shape_allowed(chain_id, before_state)
}

fn registry_only_authority_key_shape_allowed(chain_id: Option<&str>, before_state: &Value) -> bool {
    let Some(authority_key) = before_state.get("authority_key") else {
        return true;
    };
    if authority_key.is_null() {
        return true;
    }
    let (Some(chain_id), Some(authority_key)) = (chain_id, authority_key.as_str()) else {
        return false;
    };
    authority_key.starts_with(&format!("registry-only:{chain_id}:"))
}
