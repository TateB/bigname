use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use bigname_storage::CanonicalityState;
use serde_json::{Value, json};

use crate::projection_json::{dedupe_json_values, projection_coverage};

use super::{
    chain_position::chain_position_value,
    constants::*,
    types::{RecordSelector, RelevantEvent},
};

pub(super) fn build_selectors(
    record_change_events: &[&RelevantEvent],
) -> Result<BTreeMap<String, RecordSelector>> {
    let mut selectors = BTreeMap::new();

    for event in record_change_events {
        let selector = parse_record_selector(event)?;
        if is_supported_selector(&selector) {
            selectors.insert(selector.record_key.clone(), selector);
        }
    }

    Ok(selectors)
}

pub(super) fn build_explicit_gaps(selectors: &BTreeMap<String, RecordSelector>) -> Vec<Value> {
    let mut gaps = Vec::new();
    let has_text = selectors
        .values()
        .any(|selector| selector.record_family == SUPPORTED_TEXT_RECORD_FAMILY);
    let has_native_addr = selectors.contains_key(&supported_native_addr_record_key());

    if !has_native_addr {
        gaps.push(gap_value(
            &supported_native_addr_record_key(),
            SUPPORTED_ADDR_RECORD_FAMILY,
            Some(SUPPORTED_NATIVE_ADDR_SELECTOR_KEY),
        ));
    }
    if !has_text {
        gaps.push(gap_value(
            SUPPORTED_TEXT_RECORD_KEY,
            SUPPORTED_TEXT_RECORD_FAMILY,
            None,
        ));
    }

    gaps.sort_by(|left, right| {
        left["record_key"]
            .as_str()
            .cmp(&right["record_key"].as_str())
    });
    gaps
}

pub(super) fn build_unsupported_families(
    record_change_events: &[&RelevantEvent],
) -> Result<Vec<Value>> {
    let mut families = BTreeSet::new();

    for event in record_change_events {
        let selector = parse_record_selector(event)?;
        if !is_supported_selector(&selector) {
            families.insert(selector.record_family);
        }
    }

    Ok(families
        .into_iter()
        .map(|record_family| {
            json!({
                "record_family": record_family,
                "unsupported_reason": UNSUPPORTED_FAMILY_REASON,
            })
        })
        .collect())
}

pub(super) fn build_entries(
    record_change_events: &[&RelevantEvent],
    selectors: &BTreeMap<String, RecordSelector>,
) -> Result<Vec<Value>> {
    let mut entries = Vec::new();
    for selector in selectors.values() {
        let mut latest_value = None;
        for event in record_change_events.iter().rev() {
            if parse_record_selector(event)? == *selector {
                latest_value = event
                    .after_state
                    .as_object()
                    .and_then(|object| object.get("value"))
                    .cloned();
                break;
            }
        }

        let entry = latest_value
            .map(|value| {
                json!({
                    "record_key": selector.record_key,
                    "record_family": selector.record_family,
                    "selector_key": selector.selector_key,
                    "status": "success",
                    "value": value,
                })
            })
            .unwrap_or_else(|| {
                json!({
                    "record_key": selector.record_key,
                    "record_family": selector.record_family,
                    "selector_key": selector.selector_key,
                    "status": "unsupported",
                    "unsupported_reason": CACHE_UNSUPPORTED_REASON_VALUE_NOT_RETAINED,
                })
            });
        entries.push(entry);
    }

    entries.sort_by(|left, right| {
        left["record_key"]
            .as_str()
            .cmp(&right["record_key"].as_str())
    });
    Ok(entries)
}

pub(super) fn build_last_change(event: &RelevantEvent) -> Result<Value> {
    Ok(json!({
        "normalized_event_id": event.normalized_event_id,
        "event_kind": event.event_kind,
        "chain_position": chain_position_value(event)?,
    }))
}

pub(super) fn gap_value(
    record_key: &str,
    record_family: &str,
    selector_key: Option<&str>,
) -> Value {
    json!({
        "record_key": record_key,
        "record_family": record_family,
        "selector_key": selector_key,
        "gap_reason": GAP_REASON_NOT_OBSERVED,
    })
}

pub(super) fn resolver_family_status_value(record_family: &str, unsupported_reason: &str) -> Value {
    json!({
        "record_family": record_family,
        "unsupported_reason": unsupported_reason,
    })
}

fn is_supported_selector(selector: &RecordSelector) -> bool {
    match selector.record_family.as_str() {
        SUPPORTED_TEXT_RECORD_FAMILY => selector
            .selector_key
            .as_ref()
            .map(|selector_key| selector.record_key == format!("text:{selector_key}"))
            .unwrap_or_else(|| selector.record_key == SUPPORTED_TEXT_RECORD_KEY),
        SUPPORTED_ADDR_RECORD_FAMILY => selector
            .selector_key
            .as_ref()
            .is_some_and(|selector_key| selector.record_key == format!("addr:{selector_key}")),
        _ => false,
    }
}

fn parse_record_selector(event: &RelevantEvent) -> Result<RecordSelector> {
    let object = event
        .after_state
        .as_object()
        .context("record event after_state must be an object")?;
    let record_key = object
        .get("record_key")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .context("record event after_state.record_key must be a non-empty string")?
        .to_owned();
    let record_family = object
        .get("record_family")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .context("record event after_state.record_family must be a non-empty string")?
        .to_owned();
    let selector_key = match object.get("selector_key") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) if !value.trim().is_empty() => Some(value.clone()),
        Some(_) => {
            anyhow::bail!(
                "record event after_state.selector_key must be null or a non-empty string"
            )
        }
    };

    let expected_record_key = selector_key
        .as_ref()
        .map(|selector_key| format!("{record_family}:{selector_key}"))
        .unwrap_or_else(|| record_family.clone());
    if record_key != expected_record_key {
        anyhow::bail!(
            "record event selector identity mismatch: record_key {} must match {}",
            record_key,
            expected_record_key
        );
    }

    Ok(RecordSelector {
        record_key,
        record_family,
        selector_key,
    })
}

pub(super) fn build_provenance(events: &[RelevantEvent]) -> Result<Value> {
    let normalized_event_ids = events
        .iter()
        .map(|event| Value::Number(event.normalized_event_id.into()))
        .collect::<Vec<_>>();
    let raw_fact_refs = dedupe_json_values(events.iter().map(|event| event.raw_fact_ref.clone()))?;
    let manifest_versions = dedupe_json_values(events.iter().map(|event| {
        json!({
            "source_manifest_id": event.source_manifest_id,
            "source_family": event.source_family,
            "manifest_version": event.manifest_version,
        })
    }))?;

    Ok(json!({
        "normalized_event_ids": dedupe_json_values(normalized_event_ids)?,
        "raw_fact_refs": raw_fact_refs,
        "manifest_versions": manifest_versions,
        "execution_trace_id": Value::Null,
        "derivation_kind": RECORD_INVENTORY_CURRENT_DERIVATION_KIND,
    }))
}

pub(super) fn build_coverage(events: &[RelevantEvent]) -> Value {
    let source_classes_considered = events
        .iter()
        .map(|event| event.source_family.clone())
        .collect::<BTreeSet<_>>();

    projection_coverage(
        "full",
        "authoritative",
        source_classes_considered,
        None,
        RECORD_INVENTORY_ENUMERATION_BASIS,
    )
}

pub(super) fn build_canonicality_summary(events: &[RelevantEvent]) -> Value {
    let status = weakest_canonicality(events.iter().map(|event| event.canonicality_state))
        .unwrap_or(CanonicalityState::Canonical);

    let mut chain_states = BTreeMap::<String, CanonicalityState>::new();
    for event in events {
        let replacement = chain_states
            .get(&event.chain_id)
            .map(|current| {
                canonicality_rank(event.canonicality_state) < canonicality_rank(*current)
            })
            .unwrap_or(true);
        if replacement {
            chain_states.insert(event.chain_id.clone(), event.canonicality_state);
        }
    }

    json!({
        "status": status.as_str(),
        "chains": chain_states
            .into_iter()
            .map(|(chain_id, state)| (chain_id, Value::String(state.as_str().to_owned())))
            .collect::<serde_json::Map<String, Value>>(),
    })
}

fn weakest_canonicality(
    states: impl IntoIterator<Item = CanonicalityState>,
) -> Option<CanonicalityState> {
    states
        .into_iter()
        .min_by_key(|state| canonicality_rank(*state))
}

fn canonicality_rank(state: CanonicalityState) -> u8 {
    match state {
        CanonicalityState::Canonical => 0,
        CanonicalityState::Safe => 1,
        CanonicalityState::Finalized => 2,
        CanonicalityState::Observed => 3,
        CanonicalityState::Orphaned => 4,
    }
}

fn supported_native_addr_record_key() -> String {
    format!("{SUPPORTED_ADDR_RECORD_FAMILY}:{SUPPORTED_NATIVE_ADDR_SELECTOR_KEY}")
}
