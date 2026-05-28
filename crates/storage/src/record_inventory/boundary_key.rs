use std::fmt::Write as _;

use anyhow::{Context, Result, bail};
use serde_json::{Map, Value};
use uuid::Uuid;

pub(crate) fn record_version_boundary_storage_key(
    record_version_boundary: &Value,
    expected_resource_id: Uuid,
) -> Result<String> {
    let boundary =
        decode_record_version_boundary(record_version_boundary, Some(expected_resource_id))
            .context("record_inventory_current record_version_boundary key derivation failed")?;

    let mut key = String::new();
    append_key_part(&mut key, &boundary.logical_name_id);
    append_key_part(&mut key, &boundary.resource_id.to_string());
    append_key_part(
        &mut key,
        &boundary
            .normalized_event_id
            .map(|value| value.to_string())
            .unwrap_or_default(),
    );
    append_key_part(&mut key, boundary.event_kind.as_deref().unwrap_or_default());
    append_key_part(&mut key, &boundary.chain_position.chain_id);
    append_key_part(&mut key, &boundary.chain_position.block_number.to_string());
    append_key_part(&mut key, &boundary.chain_position.block_hash);
    append_key_part(&mut key, &boundary.chain_position.timestamp);
    Ok(key)
}

fn append_key_part(buffer: &mut String, value: &str) {
    write!(buffer, "{}:{value};", value.len()).expect("string write to key buffer must succeed");
}

pub(super) fn decode_record_version_boundary(
    value: &Value,
    expected_resource_id: Option<Uuid>,
) -> Result<RecordVersionBoundaryParts> {
    let object = value
        .as_object()
        .with_context(|| "record_version_boundary must be a JSON object".to_owned())?;
    let logical_name_id =
        required_string_field(object, "logical_name_id", "record_version_boundary")?.to_owned();
    let resource_id = Uuid::parse_str(required_string_field(
        object,
        "resource_id",
        "record_version_boundary",
    )?)
    .context("record_version_boundary resource_id must be a UUID")?;
    if let Some(expected_resource_id) = expected_resource_id
        && resource_id != expected_resource_id
    {
        bail!(
            "record_version_boundary resource_id {} does not match storage key resource_id {}",
            resource_id,
            expected_resource_id
        );
    }

    let normalized_event_id = match object.get("normalized_event_id") {
        Some(Value::Null) => None,
        Some(value) => Some(value.as_i64().filter(|value| *value > 0).with_context(|| {
            "record_version_boundary normalized_event_id must be null or positive integer"
                .to_owned()
        })?),
        None => bail!("record_version_boundary must include normalized_event_id"),
    };
    let event_kind = match object.get("event_kind") {
        Some(Value::Null) => None,
        Some(Value::String(value)) if !value.trim().is_empty() => Some(value.clone()),
        Some(_) => {
            bail!("record_version_boundary event_kind must be null or non-empty string");
        }
        None => bail!("record_version_boundary must include event_kind"),
    };
    if normalized_event_id.is_some() != event_kind.is_some() {
        bail!(
            "record_version_boundary normalized_event_id and event_kind must both be present or both be null"
        );
    }

    let chain_position = decode_chain_position(
        object
            .get("chain_position")
            .with_context(|| "record_version_boundary must include chain_position".to_owned())?,
        "record_version_boundary.chain_position",
    )?;

    Ok(RecordVersionBoundaryParts {
        logical_name_id,
        resource_id,
        normalized_event_id,
        event_kind,
        chain_position,
    })
}

pub(super) fn decode_chain_position(value: &Value, context: &str) -> Result<ChainPositionParts> {
    let object = value
        .as_object()
        .with_context(|| format!("{context} must be a JSON object"))?;
    let chain_id = required_string_field(object, "chain_id", context)?.to_owned();
    let block_number = object
        .get("block_number")
        .and_then(Value::as_i64)
        .filter(|value| *value >= 0)
        .with_context(|| format!("{context} must include non-negative integer block_number"))?;
    let block_hash = required_string_field(object, "block_hash", context)?.to_owned();
    let timestamp = required_string_field(object, "timestamp", context)?.to_owned();
    Ok(ChainPositionParts {
        chain_id,
        block_number,
        block_hash,
        timestamp,
    })
}

fn required_string_field<'a>(
    object: &'a Map<String, Value>,
    field_name: &str,
    context: &str,
) -> Result<&'a str> {
    object
        .get(field_name)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("{context} must include non-empty string field {field_name}"))
}

#[derive(Clone, Debug)]
pub(super) struct RecordVersionBoundaryParts {
    logical_name_id: String,
    resource_id: Uuid,
    normalized_event_id: Option<i64>,
    event_kind: Option<String>,
    chain_position: ChainPositionParts,
}

#[derive(Clone, Debug)]
pub(super) struct ChainPositionParts {
    chain_id: String,
    block_number: i64,
    block_hash: String,
    timestamp: String,
}
