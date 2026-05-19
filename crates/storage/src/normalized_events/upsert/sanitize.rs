use anyhow::{Context, Result};

use crate::normalized_events::types::NormalizedEvent;

pub(super) fn jsonb_safe_normalized_event(event: &NormalizedEvent) -> NormalizedEvent {
    let mut safe_event = event.clone();
    safe_event.event_identity = postgres_text_safe(&safe_event.event_identity);
    safe_event.namespace = postgres_text_safe(&safe_event.namespace);
    safe_event.logical_name_id = safe_event
        .logical_name_id
        .as_deref()
        .map(postgres_text_safe);
    safe_event.event_kind = postgres_text_safe(&safe_event.event_kind);
    safe_event.source_family = postgres_text_safe(&safe_event.source_family);
    safe_event.chain_id = safe_event.chain_id.as_deref().map(postgres_text_safe);
    safe_event.block_hash = safe_event.block_hash.as_deref().map(postgres_text_safe);
    safe_event.transaction_hash = safe_event
        .transaction_hash
        .as_deref()
        .map(postgres_text_safe);
    safe_event.derivation_kind = postgres_text_safe(&safe_event.derivation_kind);
    if json_value_contains_nul(&safe_event.raw_fact_ref) {
        safe_event.raw_fact_ref = jsonb_safe_value(&safe_event.raw_fact_ref);
    }
    if json_value_contains_nul(&safe_event.before_state) {
        safe_event.before_state = jsonb_safe_value(&safe_event.before_state);
    }
    if json_value_contains_nul(&safe_event.after_state) {
        safe_event.after_state = jsonb_safe_value(&safe_event.after_state);
    }
    safe_event
}

pub fn serialize_jsonb_value(value: &serde_json::Value, context: &'static str) -> Result<String> {
    if json_value_contains_nul(value) {
        return serde_json::to_string(&jsonb_safe_value(value)).context(context);
    }

    serde_json::to_string(value).context(context)
}

fn postgres_text_safe(text: &str) -> String {
    text.replace('\0', "\\u0000")
}

fn json_value_contains_nul(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(text) => text.contains('\0'),
        serde_json::Value::Array(items) => items.iter().any(json_value_contains_nul),
        serde_json::Value::Object(fields) => fields
            .iter()
            .any(|(key, value)| key.contains('\0') || json_value_contains_nul(value)),
        _ => false,
    }
}

fn jsonb_safe_value(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(text) => serde_json::Value::String(text.replace('\0', "\\u0000")),
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(jsonb_safe_value).collect())
        }
        serde_json::Value::Object(fields) => serde_json::Value::Object(
            fields
                .iter()
                .map(|(key, value)| (postgres_text_safe(key), jsonb_safe_value(value)))
                .collect(),
        ),
        _ => value.clone(),
    }
}
