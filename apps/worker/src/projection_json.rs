use std::collections::BTreeSet;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::types::time::{OffsetDateTime, UtcOffset};

pub(crate) fn format_timestamp(value: OffsetDateTime) -> String {
    let value = value.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        value.year(),
        u8::from(value.month()),
        value.day(),
        value.hour(),
        value.minute(),
        value.second(),
    )
}

pub(crate) fn json_str(value: &Value, path: &[&str]) -> Option<String> {
    path.iter()
        .try_fold(value, |current, key| current.get(key))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

pub(crate) fn json_i64(value: &Value, path: &[&str]) -> Option<i64> {
    path.iter()
        .try_fold(value, |current, key| current.get(key))
        .and_then(Value::as_i64)
}

pub(crate) fn dedupe_json_values(values: impl IntoIterator<Item = Value>) -> Result<Vec<Value>> {
    let mut seen = BTreeSet::new();
    let mut deduped = Vec::new();

    for value in values {
        let key = serde_json::to_string(&value).context("failed to serialize JSON value")?;
        if seen.insert(key) {
            deduped.push(value);
        }
    }

    Ok(deduped)
}

pub(crate) fn json_array_field(value: &Value, field: &str) -> Vec<Value> {
    value
        .get(field)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

pub(crate) fn json_string_array_field(value: &Value, field: &str) -> Vec<String> {
    value
        .get(field)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

pub(crate) fn unsupported_summary(unsupported_reason: &str) -> Value {
    json!({
        "status": "unsupported",
        "unsupported_reason": unsupported_reason,
    })
}

pub(crate) fn projection_coverage(
    status: &str,
    exhaustiveness: &str,
    source_classes: impl IntoIterator<Item = String>,
    unsupported_reason: Option<&str>,
    enumeration_basis: &str,
) -> Value {
    let source_classes = source_classes
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    json!({
        "status": status,
        "exhaustiveness": exhaustiveness,
        "source_classes_considered": source_classes,
        "unsupported_reason": unsupported_reason
            .map(|reason| Value::String(reason.to_owned()))
            .unwrap_or(Value::Null),
        "enumeration_basis": enumeration_basis,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::json_i64;

    #[test]
    fn json_i64_reads_real_timestamps_and_drops_the_no_expiry_sentinel() {
        // Decode stays faithful: the raw on-chain `uint64` expiry lands in the event JSON, including
        // ENSv2's `type(uint64).max` "reserved forever / no expiry" sentinel. The projection reads it
        // here; an unrepresentable value is treated as absent (`None`) — so a permanent name projects
        // to a `null` expiry rather than a fabricated far-future date.
        assert_eq!(
            json_i64(&json!({ "expiry": 1_900_000_000 }), &["expiry"]),
            Some(1_900_000_000)
        );
        assert_eq!(json_i64(&json!({ "expiry": u64::MAX }), &["expiry"]), None);
        assert_eq!(json_i64(&json!({}), &["expiry"]), None);
    }
}
