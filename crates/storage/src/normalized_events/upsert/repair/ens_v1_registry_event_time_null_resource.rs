use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::Postgres;

use super::super::super::types::NormalizedEvent;
use super::super::{normalized_event_identity_differences, serialize_jsonb_value};

pub(crate) async fn repair_ens_v1_unwrapped_authority_registry_event_time_null_resource_ids(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Result<HashSet<String>> {
    let mut event_identities = Vec::new();
    let mut new_resource_ids = Vec::new();
    let mut logical_name_ids = Vec::new();
    let mut block_numbers = Vec::new();
    let mut event_kinds = Vec::new();
    let mut old_before_states = Vec::new();
    let mut new_before_states = Vec::new();
    let mut old_after_states = Vec::new();
    let mut new_after_states = Vec::new();

    for event in events {
        let Some(existing) = existing_by_identity.get(&event.event_identity) else {
            continue;
        };
        if !ens_v1_unwrapped_authority_registry_event_time_null_resource_id_repair_allowed(
            existing,
            event,
            &normalized_event_identity_differences(existing, event),
        ) {
            continue;
        }
        let Some(new_resource_id) = event.resource_id else {
            continue;
        };
        let (Some(logical_name_id), Some(block_number)) =
            (existing.logical_name_id.as_ref(), existing.block_number)
        else {
            continue;
        };

        event_identities.push(event.event_identity.clone());
        new_resource_ids.push(new_resource_id);
        logical_name_ids.push(logical_name_id.clone());
        block_numbers.push(block_number);
        event_kinds.push(event.event_kind.clone());
        old_before_states.push(serialize_jsonb_value(
            &existing.before_state,
            "failed to serialize existing ENSv1 registry event-time null-resource before_state",
        )?);
        new_before_states.push(serialize_jsonb_value(
            &event.before_state,
            "failed to serialize repaired ENSv1 registry event-time null-resource before_state",
        )?);
        old_after_states.push(serialize_jsonb_value(
            &existing.after_state,
            "failed to serialize existing ENSv1 registry event-time null-resource after_state",
        )?);
        new_after_states.push(serialize_jsonb_value(
            &event.after_state,
            "failed to serialize repaired ENSv1 registry event-time null-resource after_state",
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
                $2::UUID[],
                $3::TEXT[],
                $4::BIGINT[],
                $5::TEXT[],
                $6::TEXT[],
                $7::TEXT[],
                $8::TEXT[],
                $9::TEXT[]
            ) AS input(
                event_identity,
                new_resource_id,
                logical_name_id,
                block_number,
                event_kind,
                old_before_state,
                new_before_state,
                old_after_state,
                new_after_state
            )
        ),
        repair_map AS (
            SELECT input.*
            FROM input
            JOIN resources new_resource
              ON new_resource.resource_id = input.new_resource_id
             AND new_resource.chain_id = 'ethereum-mainnet'
             AND new_resource.canonicality_state IN (
                 'canonical'::canonicality_state,
                 'safe'::canonicality_state,
                 'finalized'::canonicality_state
             )
             AND new_resource.provenance->>'logical_name_id' = input.logical_name_id
             AND new_resource.provenance->>'authority_kind' IN (
                 'registrar',
                 'wrapper',
                 'registry_only'
             )
            WHERE (
                    input.old_before_state::JSONB IS NOT DISTINCT FROM
                        input.new_before_state::JSONB
                    AND input.old_after_state::JSONB IS NOT DISTINCT FROM
                        input.new_after_state::JSONB
                )
               OR (
                    input.event_kind = 'ResolverChanged'
                    AND input.old_after_state::JSONB IS NOT DISTINCT FROM
                        input.new_after_state::JSONB
                    AND input.old_before_state::JSONB - 'resolver' =
                        input.new_before_state::JSONB - 'resolver'
                    AND input.old_before_state::JSONB ? 'resolver'
                    AND input.new_before_state::JSONB ? 'resolver'
                    AND input.old_before_state::JSONB -> 'resolver' = 'null'::JSONB
                    AND input.new_before_state::JSONB ->> 'resolver' ~ '^0x[0-9a-f]{40}$'
                    AND input.new_after_state::JSONB ->> 'namehash' ~ '^0x[0-9a-f]{64}$'
                    AND input.new_after_state::JSONB ->> 'resolver' ~ '^0x[0-9a-f]{40}$'
                )
        ),
        updated AS (
            UPDATE normalized_events event
            SET
                resource_id = repair.new_resource_id,
                before_state = repair.new_before_state::JSONB,
                observed_at = now()
            FROM repair_map repair
            WHERE event.event_identity = repair.event_identity
              AND event.resource_id IS NULL
              AND event.before_state IS NOT DISTINCT FROM repair.old_before_state::JSONB
              AND event.after_state IS NOT DISTINCT FROM repair.old_after_state::JSONB
            RETURNING
                event.event_identity,
                event.normalized_event_id,
                event.canonicality_state,
                event.event_kind,
                repair.new_resource_id
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
        affected_resource_keys AS (
            SELECT
                'permissions_current'::TEXT AS projection,
                new_resource_id::TEXT AS projection_key,
                jsonb_build_object('resource_id', new_resource_id::TEXT) AS key_payload
            FROM updated
            WHERE event_kind IN ('AuthorityTransferred', 'PermissionChanged')

            UNION ALL

            SELECT
                'record_inventory_current'::TEXT AS projection,
                new_resource_id::TEXT AS projection_key,
                jsonb_build_object('resource_id', new_resource_id::TEXT) AS key_payload
            FROM updated
            WHERE event_kind IN (
                'ResolverChanged',
                'RecordChanged',
                'RecordVersionChanged'
            )
        ),
        queued_resource_invalidations AS (
            INSERT INTO projection_invalidations (
                projection,
                projection_key,
                key_payload,
                last_changed_at,
                invalidated_at
            )
            SELECT
                projection,
                projection_key,
                key_payload,
                now(),
                now()
            FROM affected_resource_keys
            WHERE projection_key IS NOT NULL
              AND btrim(projection_key) <> ''
            GROUP BY projection, projection_key, key_payload
            ON CONFLICT (projection, projection_key)
            DO UPDATE SET
                key_payload = EXCLUDED.key_payload,
                generation = projection_invalidations.generation + 1,
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
        SELECT input.event_identity
        FROM input
        JOIN updated
          ON updated.event_identity = input.event_identity
        "#,
    )
    .bind(&event_identities)
    .bind(&new_resource_ids)
    .bind(&logical_name_ids)
    .bind(&block_numbers)
    .bind(&event_kinds)
    .bind(&old_before_states)
    .bind(&new_before_states)
    .bind(&old_after_states)
    .bind(&new_after_states)
    .fetch_all(&mut **executor)
    .await
    .context(
        "failed to repair ENSv1 unwrapped-authority event-time null normalized-event resource_id",
    )?;

    let repaired = repaired.into_iter().collect::<HashSet<_>>();
    let rejected = event_identities
        .iter()
        .zip(new_resource_ids.iter())
        .filter(|(event_identity, _)| !repaired.contains(event_identity.as_str()))
        .map(|(event_identity, new_resource_id)| {
            format!("{event_identity} (new_resource_id={new_resource_id})")
        })
        .collect::<Vec<_>>();
    if !rejected.is_empty() {
        bail!(
            "ENSv1 registry event-time null resource_id repair rejected invalid resource anchors for events: {}",
            rejected.join(", ")
        );
    }

    Ok(repaired)
}

pub(crate) fn ens_v1_unwrapped_authority_registry_event_time_null_resource_id_repair_allowed(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
    differing_fields: &[&'static str],
) -> bool {
    if !matches!(
        differing_fields,
        ["resource_id"] | ["resource_id", "before_state"]
    ) {
        return false;
    }
    if existing.resource_id.is_some()
        || incoming.resource_id.is_none()
        || existing.logical_name_id.is_none()
        || incoming.logical_name_id.is_none()
        || existing.logical_name_id != incoming.logical_name_id
        || existing.block_number.is_none()
        || existing.namespace != "ens"
        || existing.chain_id.as_deref() != Some("ethereum-mainnet")
        || existing.derivation_kind != "ens_v1_unwrapped_authority"
        || !matches!(
            existing.source_family.as_str(),
            "ens_v1_registry_l1" | "ens_v1_resolver_l1"
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

    if existing.before_state == incoming.before_state
        && existing.after_state == incoming.after_state
    {
        return true;
    }

    matches!(differing_fields, ["resource_id", "before_state"])
        && existing.event_kind == "ResolverChanged"
        && existing.source_family == "ens_v1_registry_l1"
        && existing.raw_fact_ref.get("kind").and_then(Value::as_str) == Some("raw_log")
        && existing.after_state == incoming.after_state
        && registry_resolver_after_state_allowed(&incoming.after_state)
        && before_state_resolver_transition_allowed(&existing.before_state, &incoming.before_state)
}

fn registry_resolver_after_state_allowed(value: &Value) -> bool {
    required_json_text(value, "namehash").is_some_and(is_lower_hex_hash)
        && required_json_text(value, "resolver").is_some_and(is_lower_hex_address)
}

fn before_state_resolver_transition_allowed(existing: &Value, incoming: &Value) -> bool {
    if before_state_without_resolver(existing) != before_state_without_resolver(incoming) {
        return false;
    }

    match (existing.get("resolver"), incoming.get("resolver")) {
        (Some(Value::Null), Some(Value::String(incoming_resolver))) => {
            is_lower_hex_address(incoming_resolver)
        }
        _ => false,
    }
}

fn before_state_without_resolver(value: &Value) -> Option<Value> {
    let mut object = value.as_object()?.clone();
    object.remove("resolver");
    Some(Value::Object(object))
}

fn required_json_text<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key)?.as_str().filter(|value| !value.is_empty())
}

fn is_lower_hex_address(value: &str) -> bool {
    value.len() == 42
        && value.starts_with("0x")
        && value
            .as_bytes()
            .iter()
            .skip(2)
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn is_lower_hex_hash(value: &str) -> bool {
    value.len() == 66
        && value.starts_with("0x")
        && value
            .as_bytes()
            .iter()
            .skip(2)
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}
