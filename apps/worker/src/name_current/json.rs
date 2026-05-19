use std::collections::BTreeSet;

use anyhow::Result;
use bigname_storage::{HistoryEvent, normalize_evm_address};
use serde_json::{Map, Value, json};
use sqlx::types::time::OffsetDateTime;

use crate::projection_json::dedupe_json_values;

use super::types::{
    HistoryHeads, HistoryPointer, ProjectedFacts, RelevantEvent, WildcardSourceContext,
};
use super::{NAME_CURRENT_DERIVATION_KIND, RECORD_INVENTORY_UNSUPPORTED_REASON, ZERO_ADDRESS};

pub(super) use crate::projection_json::{format_timestamp, json_i64, json_str};

pub(super) fn build_declared_summary(facts: ProjectedFacts, topology: Option<Value>) -> Value {
    let surface_head = facts
        .surface_head
        .as_ref()
        .map(history_pointer_json)
        .unwrap_or(Value::Null);
    let resource_head = facts
        .resource_head
        .as_ref()
        .map(history_pointer_json)
        .unwrap_or(Value::Null);

    let mut summary = Map::new();
    summary.insert(
        "registration".to_owned(),
        json!({
            "status": facts.registration_status,
            "authority_kind": facts.authority_kind,
            "authority_key": facts.authority_key,
            "registrant": facts.registrant,
            "expiry": facts.expiry,
            "released_at": facts.released_at,
            "latest_event_kind": facts.latest_registration_event_kind,
        }),
    );
    summary.insert(
        "control".to_owned(),
        json!({
            "status": facts.control_status_substrate,
            "expiry": format_unix_timestamp_value(facts.control_expiry_substrate),
            "registrant": facts.registrant,
            "registry_owner": facts.registry_owner,
            "latest_event_kind": facts.latest_control_event_kind,
        }),
    );
    summary.insert(
        "resolver".to_owned(),
        json!({
            "chain_id": facts.resolver_chain_id,
            "address": facts.resolver_address,
            "latest_event_kind": facts.latest_resolver_event_kind,
        }),
    );
    summary.insert(
        "record_inventory".to_owned(),
        json!({
            "status": "unsupported",
            "unsupported_reason": RECORD_INVENTORY_UNSUPPORTED_REASON,
        }),
    );
    summary.insert(
        "history".to_owned(),
        json!({
            "surface_head": surface_head,
            "resource_head": resource_head,
        }),
    );
    if let Some(topology) = topology {
        summary.insert("topology".to_owned(), topology);
    }

    Value::Object(summary)
}

pub(super) fn build_provenance(
    events: &[RelevantEvent],
    history_heads: &HistoryHeads,
    wildcard_source_context: Option<&WildcardSourceContext>,
    supplemental_manifest_versions: &[Value],
) -> Result<Value> {
    let mut normalized_event_ids = Vec::new();
    let mut seen_normalized_event_ids = BTreeSet::new();
    for normalized_event_id in events
        .iter()
        .map(|event| event.normalized_event_id)
        .chain(
            wildcard_source_context
                .into_iter()
                .flat_map(WildcardSourceContext::events)
                .map(|event| event.normalized_event_id),
        )
        .chain(history_heads.iter().map(|event| event.normalized_event_id))
    {
        if seen_normalized_event_ids.insert(normalized_event_id) {
            normalized_event_ids.push(normalized_event_id);
        }
    }

    let raw_fact_refs = dedupe_json_values(
        events
            .iter()
            .map(|event| event.raw_fact_ref.clone())
            .chain(
                wildcard_source_context
                    .into_iter()
                    .flat_map(WildcardSourceContext::events)
                    .map(|event| event.raw_fact_ref.clone()),
            )
            .chain(history_heads.iter().map(|event| event.raw_fact_ref.clone())),
    )?;
    let manifest_versions = dedupe_json_values(
        events
            .iter()
            .map(event_manifest_version)
            .chain(
                wildcard_source_context
                    .into_iter()
                    .flat_map(WildcardSourceContext::events)
                    .map(event_manifest_version),
            )
            .chain(history_heads.iter().map(history_manifest_version)),
    )?;
    let manifest_versions = dedupe_json_values(
        manifest_versions
            .into_iter()
            .chain(supplemental_manifest_versions.iter().cloned()),
    )?;

    Ok(json!({
        "normalized_event_ids": normalized_event_ids,
        "raw_fact_refs": raw_fact_refs,
        "manifest_versions": manifest_versions,
        "execution_trace_id": Value::Null,
        "derivation_kind": NAME_CURRENT_DERIVATION_KIND,
    }))
}

fn format_unix_timestamp_value(timestamp: Option<i64>) -> Value {
    match timestamp {
        Some(timestamp) => OffsetDateTime::from_unix_timestamp(timestamp)
            .map(format_timestamp)
            .map(Value::String)
            .unwrap_or_else(|_| Value::Number(timestamp.into())),
        None => Value::Null,
    }
}

fn event_manifest_version(event: &RelevantEvent) -> Value {
    json!({
        "source_manifest_id": event.source_manifest_id,
        "source_family": event.source_family,
        "manifest_version": event.manifest_version,
    })
}

fn history_manifest_version(event: &HistoryEvent) -> Value {
    json!({
        "source_manifest_id": event.source_manifest_id,
        "source_family": event.source_family,
        "manifest_version": event.manifest_version,
    })
}

pub(super) fn normalize_resolver_address(value: Option<&str>) -> Option<String> {
    let normalized = normalize_evm_address(value?.trim());
    if normalized.is_empty() || normalized == ZERO_ADDRESS {
        None
    } else {
        Some(normalized)
    }
}

pub(super) fn history_pointer_from_event(event: &HistoryEvent) -> HistoryPointer {
    HistoryPointer {
        normalized_event_id: event.normalized_event_id,
        event_kind: event.event_kind.clone(),
        chain_position: history_pointer_chain_position(event),
    }
}

fn history_pointer_chain_position(event: &HistoryEvent) -> Value {
    match (
        event.chain_id.as_ref(),
        event.block_number,
        event.block_hash.as_ref(),
        event.block_timestamp,
    ) {
        (Some(chain_id), Some(block_number), Some(block_hash), Some(timestamp)) => json!({
            "chain_id": chain_id,
            "block_number": block_number,
            "block_hash": block_hash,
            "timestamp": format_timestamp(timestamp),
        }),
        _ => Value::Null,
    }
}

pub(super) fn history_pointer_json(pointer: &HistoryPointer) -> Value {
    json!({
        "normalized_event_id": pointer.normalized_event_id,
        "event_kind": pointer.event_kind,
        "chain_position": pointer.chain_position,
    })
}
