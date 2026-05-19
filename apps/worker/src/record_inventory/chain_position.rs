use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::{PgPool, Row, types::time::OffsetDateTime};

pub(super) use crate::projection_json::format_timestamp;

use super::{
    constants::*,
    types::{ChainPositionCandidate, RelevantEvent},
};

pub(super) async fn load_basenames_transport_chain_positions(
    pool: &PgPool,
    events: &[RelevantEvent],
) -> Result<Vec<ChainPositionCandidate>> {
    let Some(base_boundary) = events.iter().rev().find(|event| {
        event
            .logical_name_id
            .split_once(':')
            .map(|(namespace, _)| namespace)
            == Some(BASENAMES_NAMESPACE)
            && event.chain_id == BASE_MAINNET_CHAIN_ID
    }) else {
        return Ok(Vec::new());
    };

    let Some(upper_bound) = base_boundary.block_timestamp else {
        return Ok(Vec::new());
    };

    if !basenames_execution_transport_is_active(pool).await? {
        return Ok(Vec::new());
    }

    let row = sqlx::query(&format!(
        r#"
        SELECT
            chain_id,
            block_number,
            block_hash,
            block_timestamp
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_timestamp <= $2
          AND canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY block_timestamp DESC, block_number DESC, block_hash DESC
        LIMIT 1
        "#
    ))
    .bind(ETHEREUM_MAINNET_CHAIN_ID)
    .bind(upper_bound)
    .fetch_optional(pool)
    .await
    .context("failed to load Basenames Ethereum transport chain position")?;

    row.map(|row| {
        let chain_id = row
            .try_get::<String, _>("chain_id")
            .context("missing Basenames transport chain_id")?;
        let timestamp = row
            .try_get::<OffsetDateTime, _>("block_timestamp")
            .context("missing Basenames transport block_timestamp")?;
        Ok(ChainPositionCandidate {
            slot: chain_slot(&chain_id),
            chain_id,
            block_number: row
                .try_get("block_number")
                .context("missing Basenames transport block_number")?,
            block_hash: row
                .try_get("block_hash")
                .context("missing Basenames transport block_hash")?,
            timestamp: format_timestamp(timestamp),
        })
    })
    .transpose()
    .map(|candidate| candidate.into_iter().collect())
}

async fn basenames_execution_transport_is_active(pool: &PgPool) -> Result<bool> {
    sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM manifest_versions mv
            JOIN manifest_capability_flags mcf
              ON mcf.manifest_id = mv.manifest_id
             AND mcf.capability_name = $1
             AND mcf.status = 'supported'::capability_support_status
            JOIN manifest_contract_instances mci
              ON mci.manifest_id = mv.manifest_id
             AND mci.declaration_kind = 'contract'
             AND mci.role = 'l1_resolver'
             AND lower(mci.declared_address) = lower($6)
            WHERE mv.namespace = $2
              AND mv.source_family = $3
              AND mv.chain = $4
              AND mv.deployment_epoch = $5
              AND mv.rollout_status = 'active'::manifest_rollout_status
        )
        "#,
    )
    .bind(VERIFIED_RESOLUTION_CAPABILITY)
    .bind(BASENAMES_NAMESPACE)
    .bind(SOURCE_FAMILY_BASENAMES_EXECUTION)
    .bind(ETHEREUM_MAINNET_CHAIN_ID)
    .bind(BASENAMES_V1_DEPLOYMENT_EPOCH)
    .bind(BASENAMES_L1_RESOLVER_ADDRESS)
    .fetch_one(pool)
    .await
    .context("failed to load active basenames_execution transport admission for record_inventory_current")
}

pub(super) fn collect_chain_position_events(
    boundary_anchor: &RelevantEvent,
    provenance_events: &[RelevantEvent],
) -> Vec<RelevantEvent> {
    let mut events = provenance_events.to_vec();
    if !events
        .iter()
        .any(|event| event.normalized_event_id == boundary_anchor.normalized_event_id)
    {
        events.push(boundary_anchor.clone());
    }
    events
}

pub(super) fn build_record_version_boundary(
    event: &RelevantEvent,
    has_boundary_pointer: bool,
) -> Result<Value> {
    Ok(json!({
        "logical_name_id": event.logical_name_id,
        "resource_id": event.resource_id,
        "normalized_event_id": has_boundary_pointer.then_some(event.normalized_event_id),
        "event_kind": has_boundary_pointer.then_some(event.event_kind.clone()),
        "chain_position": chain_position_value(event)?,
    }))
}

pub(super) fn chain_position_value(event: &RelevantEvent) -> Result<Value> {
    let timestamp = event
        .block_timestamp
        .context("record event must have a chain_lineage timestamp for chain_position")?;
    Ok(json!({
        "chain_id": event.chain_id,
        "block_number": event.block_number,
        "block_hash": event.block_hash,
        "timestamp": format_timestamp(timestamp),
    }))
}

pub(super) fn build_chain_positions(
    events: &[RelevantEvent],
    supplemental_candidates: Vec<ChainPositionCandidate>,
) -> Value {
    let mut chain_positions = BTreeMap::<String, ChainPositionCandidate>::new();

    for event in events {
        let Some(timestamp) = event.block_timestamp else {
            continue;
        };
        let candidate = ChainPositionCandidate {
            slot: chain_slot(&event.chain_id),
            chain_id: event.chain_id.clone(),
            block_number: event.block_number,
            block_hash: event.block_hash.clone(),
            timestamp: format_timestamp(timestamp),
        };

        push_chain_position_candidate(&mut chain_positions, candidate);
    }

    for candidate in supplemental_candidates {
        push_chain_position_candidate(&mut chain_positions, candidate);
    }

    json!(
        chain_positions
            .into_iter()
            .map(|(slot, candidate)| {
                (
                    slot,
                    json!({
                        "chain_id": candidate.chain_id,
                        "block_number": candidate.block_number,
                        "block_hash": candidate.block_hash,
                        "timestamp": candidate.timestamp,
                    }),
                )
            })
            .collect::<serde_json::Map<String, Value>>()
    )
}

fn push_chain_position_candidate(
    chain_positions: &mut BTreeMap<String, ChainPositionCandidate>,
    candidate: ChainPositionCandidate,
) {
    match chain_positions.get(&candidate.slot) {
        Some(existing)
            if existing.block_number > candidate.block_number
                || (existing.block_number == candidate.block_number
                    && existing.block_hash >= candidate.block_hash) => {}
        _ => {
            chain_positions.insert(candidate.slot.clone(), candidate);
        }
    }
}

fn chain_slot(chain_id: &str) -> String {
    match chain_id {
        ETHEREUM_MAINNET_CHAIN_ID => "ethereum".to_owned(),
        BASE_MAINNET_CHAIN_ID => "base".to_owned(),
        _ => chain_id.to_owned(),
    }
}
