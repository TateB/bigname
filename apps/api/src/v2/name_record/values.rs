use bigname_storage::{NameCurrentRow, SelectedSnapshot};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;

use crate::{
    direct_json_field, record_json_path, record_json_string_at_paths,
    record_network_from_chain_positions,
    v2::{chains::slug_to_numeric, format_timestamp},
};

pub(super) fn json_chain_id(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(value) => value.parse::<u64>().ok().or_else(|| slug_to_numeric(value)),
        _ => None,
    }
}

pub(super) fn response_chain_id(selected_snapshot: &SelectedSnapshot) -> Option<u64> {
    selected_snapshot
        .chain_positions
        .as_map()
        .values()
        .find_map(|position| slug_to_numeric(&position.chain_id))
}

pub(super) fn network(row: &NameCurrentRow) -> String {
    network_from_parts(&row.namespace, &row.chain_positions)
}

pub(in crate::v2) fn network_from_parts(namespace: &str, chain_positions: &Value) -> String {
    record_network_from_chain_positions(namespace, chain_positions, direct_json_field)
}

pub(in crate::v2) fn chain_id_from_positions(chain_positions: &Value) -> Option<u64> {
    chain_positions
        .as_object()
        .into_iter()
        .flatten()
        .find_map(|(_, value)| {
            value
                .get("chain_id")
                .and_then(value_to_string)
                .and_then(|value| slug_to_numeric(&value))
        })
}

pub(super) fn object_field<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    value.get(key).filter(|value| value.is_object())
}

pub(in crate::v2) fn json_string_at_paths(value: &Value, paths: &[&[&str]]) -> Option<String> {
    record_json_string_at_paths(value, paths, direct_json_field)
}

pub(super) fn json_address_at_paths(value: &Value, paths: &[&[&str]]) -> Option<String> {
    json_string_at_paths(value, paths).map(|value| value.to_ascii_lowercase())
}

pub(super) fn json_timestamp_at_paths(value: &Value, paths: &[&[&str]]) -> Option<String> {
    for path in paths {
        let Some(value) = record_json_path(value, path, direct_json_field) else {
            continue;
        };
        match value {
            Value::String(value) if !value.trim().is_empty() => return Some(value.clone()),
            Value::Number(number) => {
                if let Some(timestamp) = number.as_i64().and_then(format_unix_timestamp) {
                    return Some(timestamp);
                }
            }
            _ => {}
        }
    }
    None
}

pub(in crate::v2) fn string_field(value: Option<&Value>) -> Option<String> {
    value.and_then(value_to_string)
}

pub(in crate::v2) fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

pub(super) fn json_value_present(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::String(value) => !value.trim().is_empty(),
        _ => true,
    }
}

fn format_unix_timestamp(timestamp: i64) -> Option<String> {
    let value = OffsetDateTime::from_unix_timestamp(timestamp).ok()?;
    Some(format_timestamp(value))
}
