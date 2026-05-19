use std::fmt::Write as _;

use anyhow::{Context, Result, bail};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::evm_primitives::normalize_evm_b256;

pub(super) fn decode_requested_chain_positions(
    value: &Value,
    request_key: &str,
) -> Result<Vec<RequestedChainPositionParts>> {
    let items = value.as_array().with_context(|| {
        format!(
            "execution outcome cache key for request_key {request_key} requested_chain_positions must be a JSON array"
        )
    })?;
    if items.is_empty() {
        bail!(
            "execution outcome cache key for request_key {request_key} requested_chain_positions must not be empty"
        );
    }

    let mut positions = Vec::with_capacity(items.len());
    let mut seen_chain_ids = std::collections::BTreeSet::new();
    for (index, item) in items.iter().enumerate() {
        let object = item.as_object().with_context(|| {
            format!(
                "execution outcome cache key for request_key {request_key} requested_chain_positions[{index}] must be a JSON object"
            )
        })?;
        let position = RequestedChainPositionParts::from_object(object, request_key, index)?;
        if !seen_chain_ids.insert(position.chain_id.clone()) {
            bail!(
                "execution outcome cache key for request_key {request_key} requested_chain_positions must not repeat chain_id {}",
                position.chain_id
            );
        }
        positions.push(position);
    }

    positions.sort_by(|left, right| {
        left.chain_id
            .cmp(&right.chain_id)
            .then(left.block_number.cmp(&right.block_number))
            .then(left.block_hash.cmp(&right.block_hash))
    });
    Ok(positions)
}

pub(super) fn decode_manifest_versions(
    value: &Value,
    request_key: &str,
) -> Result<Vec<ManifestVersionParts>> {
    let items = value.as_array().with_context(|| {
        format!(
            "execution outcome cache key for request_key {request_key} manifest_versions must be a JSON array"
        )
    })?;
    if items.is_empty() {
        bail!(
            "execution outcome cache key for request_key {request_key} manifest_versions must not be empty"
        );
    }

    let mut manifest_versions = Vec::with_capacity(items.len());
    let mut seen = std::collections::BTreeSet::new();
    for (index, item) in items.iter().enumerate() {
        let object = item.as_object().with_context(|| {
            format!(
                "execution outcome cache key for request_key {request_key} manifest_versions[{index}] must be a JSON object"
            )
        })?;
        let manifest_version = ManifestVersionParts::from_object(object, request_key, index)?;
        if !seen.insert(manifest_version.identity_key()) {
            bail!(
                "execution outcome cache key for request_key {request_key} manifest_versions must not repeat the same manifest identity"
            );
        }
        manifest_versions.push(manifest_version);
    }

    manifest_versions.sort_by_key(ManifestVersionParts::identity_key);
    Ok(manifest_versions)
}

pub(super) fn decode_version_boundary(
    value: &Value,
    field_name: &str,
    request_key: &str,
) -> Result<VersionBoundaryParts> {
    let object = value.as_object().with_context(|| {
        format!(
            "execution outcome cache key for request_key {request_key} {field_name} must be a JSON object"
        )
    })?;
    let logical_name_id =
        required_string_field(object, "logical_name_id", field_name, request_key)?.to_owned();
    let resource_id = Uuid::parse_str(required_string_field(
        object,
        "resource_id",
        field_name,
        request_key,
    )?)
    .with_context(|| {
        format!(
            "execution outcome cache key for request_key {request_key} {field_name}.resource_id must be a UUID"
        )
    })?;
    let normalized_event_id = match object.get("normalized_event_id") {
        Some(Value::Null) => None,
        Some(value) => Some(value.as_i64().filter(|value| *value > 0).with_context(|| {
            format!(
                "execution outcome cache key for request_key {request_key} {field_name}.normalized_event_id must be null or positive integer"
            )
        })?),
        None => bail!(
            "execution outcome cache key for request_key {request_key} {field_name} must include normalized_event_id"
        ),
    };
    let event_kind = match object.get("event_kind") {
        Some(Value::Null) => None,
        Some(Value::String(value)) if !value.trim().is_empty() => Some(value.clone()),
        Some(_) => bail!(
            "execution outcome cache key for request_key {request_key} {field_name}.event_kind must be null or non-empty string"
        ),
        None => bail!(
            "execution outcome cache key for request_key {request_key} {field_name} must include event_kind"
        ),
    };
    if normalized_event_id.is_some() != event_kind.is_some() {
        bail!(
            "execution outcome cache key for request_key {request_key} {field_name} normalized_event_id and event_kind must both be present or both be null"
        );
    }
    let chain_position = decode_chain_position(
        object.get("chain_position").with_context(|| {
            format!(
                "execution outcome cache key for request_key {request_key} {field_name} must include chain_position"
            )
        })?,
        &format!("{field_name}.chain_position"),
        request_key,
    )?;

    Ok(VersionBoundaryParts {
        logical_name_id,
        resource_id,
        normalized_event_id,
        event_kind,
        chain_position,
    })
}

pub(super) fn append_version_boundary_key_parts(
    buffer: &mut String,
    boundary: &VersionBoundaryParts,
) {
    append_key_part(buffer, &boundary.logical_name_id);
    append_key_part(buffer, &boundary.resource_id.to_string());
    append_key_part(
        buffer,
        &boundary
            .normalized_event_id
            .map(|value| value.to_string())
            .unwrap_or_default(),
    );
    append_key_part(buffer, boundary.event_kind.as_deref().unwrap_or_default());
    append_key_part(buffer, &boundary.chain_position.chain_id);
    append_key_part(buffer, &boundary.chain_position.block_number.to_string());
    append_key_part(buffer, &boundary.chain_position.block_hash);
    append_key_part(buffer, &boundary.chain_position.timestamp);
}

pub(super) fn manifest_version_identity_key(
    source_manifest_id: Option<i64>,
    source_family: Option<&str>,
    manifest_version: i64,
) -> String {
    let mut key = String::new();
    append_key_part(
        &mut key,
        &source_manifest_id
            .map(|value| value.to_string())
            .unwrap_or_default(),
    );
    append_key_part(&mut key, source_family.unwrap_or_default());
    append_key_part(&mut key, &manifest_version.to_string());
    key
}

pub(super) fn append_key_part(buffer: &mut String, value: &str) {
    write!(buffer, "{}:{value};", value.len()).expect("string write to key buffer must succeed");
}

fn required_string_field<'a>(
    object: &'a Map<String, Value>,
    field_name: &str,
    context: &str,
    request_key: &str,
) -> Result<&'a str> {
    object
        .get(field_name)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .with_context(|| {
            format!(
                "execution outcome cache key for request_key {request_key} {context} must include non-empty string field {field_name}"
            )
        })
}

fn optional_string_field<'a>(
    object: &'a Map<String, Value>,
    field_name: &str,
    context: &str,
    request_key: &str,
) -> Result<Option<&'a str>> {
    match object.get(field_name) {
        Some(Value::Null) | None => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value)),
        Some(_) => bail!(
            "execution outcome cache key for request_key {request_key} {context}.{field_name} must be null or non-empty string"
        ),
    }
}

fn decode_chain_position(
    value: &Value,
    context: &str,
    request_key: &str,
) -> Result<ChainPositionParts> {
    let object = value.as_object().with_context(|| {
        format!(
            "execution outcome cache key for request_key {request_key} {context} must be a JSON object"
        )
    })?;
    let chain_id = required_string_field(object, "chain_id", context, request_key)?.to_owned();
    let block_number = object
        .get("block_number")
        .and_then(Value::as_i64)
        .filter(|value| *value >= 0)
        .with_context(|| {
            format!(
                "execution outcome cache key for request_key {request_key} {context} must include non-negative integer block_number"
            )
        })?;
    let block_hash = normalize_evm_b256(required_string_field(
        object,
        "block_hash",
        context,
        request_key,
    )?);
    let timestamp = required_string_field(object, "timestamp", context, request_key)?.to_owned();

    Ok(ChainPositionParts {
        chain_id,
        block_number,
        block_hash,
        timestamp,
    })
}

#[derive(Clone, Debug)]
pub(super) struct RequestedChainPositionParts {
    pub(super) chain_id: String,
    pub(super) block_number: i64,
    pub(super) block_hash: String,
}

impl RequestedChainPositionParts {
    fn from_object(object: &Map<String, Value>, request_key: &str, index: usize) -> Result<Self> {
        let context = format!("requested_chain_positions[{index}]");
        let chain_id = required_string_field(object, "chain_id", &context, request_key)?.to_owned();
        let block_number = object
            .get("block_number")
            .and_then(Value::as_i64)
            .filter(|value| *value >= 0)
            .with_context(|| {
                format!(
                    "execution outcome cache key for request_key {request_key} {context} must include non-negative integer block_number"
                )
            })?;
        let block_hash = normalize_evm_b256(required_string_field(
            object,
            "block_hash",
            &context,
            request_key,
        )?);

        Ok(Self {
            chain_id,
            block_number,
            block_hash,
        })
    }

    pub(super) fn to_value(&self) -> Value {
        serde_json::json!({
            "chain_id": self.chain_id,
            "block_number": self.block_number,
            "block_hash": self.block_hash,
        })
    }
}

#[derive(Clone, Debug)]
pub(super) struct ManifestVersionParts {
    pub(super) source_manifest_id: Option<i64>,
    pub(super) source_family: Option<String>,
    pub(super) manifest_version: i64,
}

impl ManifestVersionParts {
    fn from_object(object: &Map<String, Value>, request_key: &str, index: usize) -> Result<Self> {
        let context = format!("manifest_versions[{index}]");
        let source_manifest_id = match object.get("source_manifest_id") {
            Some(Value::Null) | None => None,
            Some(value) => Some(value.as_i64().filter(|value| *value > 0).with_context(|| {
                format!(
                    "execution outcome cache key for request_key {request_key} {context}.source_manifest_id must be null or positive integer"
                )
            })?),
        };
        let source_family = optional_string_field(object, "source_family", &context, request_key)?
            .map(str::to_owned);
        if source_manifest_id.is_none() && source_family.is_none() {
            bail!(
                "execution outcome cache key for request_key {request_key} {context} must include source_manifest_id or source_family"
            );
        }
        let manifest_version = object
            .get("manifest_version")
            .and_then(Value::as_i64)
            .filter(|value| *value > 0)
            .with_context(|| {
                format!(
                    "execution outcome cache key for request_key {request_key} {context} must include positive integer manifest_version"
                )
            })?;

        Ok(Self {
            source_manifest_id,
            source_family,
            manifest_version,
        })
    }

    pub(super) fn identity_key(&self) -> String {
        manifest_version_identity_key(
            self.source_manifest_id,
            self.source_family.as_deref(),
            self.manifest_version,
        )
    }

    pub(super) fn to_value(&self) -> Value {
        let mut object = Map::new();
        if let Some(source_manifest_id) = self.source_manifest_id {
            object.insert(
                "source_manifest_id".to_owned(),
                Value::Number(source_manifest_id.into()),
            );
        }
        if let Some(source_family) = &self.source_family {
            object.insert(
                "source_family".to_owned(),
                Value::String(source_family.clone()),
            );
        }
        object.insert(
            "manifest_version".to_owned(),
            Value::Number(self.manifest_version.into()),
        );
        Value::Object(object)
    }
}

#[derive(Clone, Debug)]
pub(super) struct VersionBoundaryParts {
    pub(super) logical_name_id: String,
    pub(super) resource_id: Uuid,
    pub(super) normalized_event_id: Option<i64>,
    pub(super) event_kind: Option<String>,
    pub(super) chain_position: ChainPositionParts,
}

impl VersionBoundaryParts {
    pub(super) fn to_value(&self) -> Value {
        serde_json::json!({
            "logical_name_id": self.logical_name_id.clone(),
            "resource_id": self.resource_id.to_string(),
            "normalized_event_id": self.normalized_event_id,
            "event_kind": self.event_kind.clone(),
            "chain_position": self.chain_position.to_value(),
        })
    }
}

#[derive(Clone, Debug)]
pub(super) struct ChainPositionParts {
    pub(super) chain_id: String,
    pub(super) block_number: i64,
    pub(super) block_hash: String,
    pub(super) timestamp: String,
}

impl ChainPositionParts {
    fn to_value(&self) -> Value {
        serde_json::json!({
            "chain_id": self.chain_id.clone(),
            "block_number": self.block_number,
            "block_hash": self.block_hash.clone(),
            "timestamp": self.timestamp.clone(),
        })
    }
}
