use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use bigname_storage::{PermissionScope, normalize_evm_address};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::projection_json::dedupe_json_values;

use super::types::RelevantEvent;
use super::{PERMISSIONS_CURRENT_DERIVATION_KIND, PERMISSIONS_ENUMERATION_BASIS};

pub(super) fn parse_scope(state: &Value) -> Result<PermissionScope> {
    let scope = state
        .get("scope")
        .and_then(Value::as_object)
        .context("PermissionChanged after_state.scope must be an object")?;
    let kind = scope
        .get("kind")
        .and_then(Value::as_str)
        .context("PermissionChanged after_state.scope.kind must be a string")?;

    match kind {
        "root" => Ok(PermissionScope::Root),
        "registry" => Ok(PermissionScope::Registry),
        "resource" => Ok(PermissionScope::Resource),
        "resolver" => Ok(PermissionScope::Resolver {
            chain_id: scope
                .get("chain_id")
                .and_then(Value::as_str)
                .context("resolver scope must include chain_id")?
                .to_owned(),
            resolver_address: normalize_evm_address(
                scope
                    .get("resolver_address")
                    .and_then(Value::as_str)
                    .context("resolver scope must include resolver_address")?,
            ),
        }),
        "record_manager" => Ok(PermissionScope::RecordManager {
            chain_id: scope
                .get("chain_id")
                .and_then(Value::as_str)
                .context("record_manager scope must include chain_id")?
                .to_owned(),
            manager_address: normalize_evm_address(
                scope
                    .get("manager_address")
                    .and_then(Value::as_str)
                    .context("record_manager scope must include manager_address")?,
            ),
        }),
        "migration_derived" => Ok(PermissionScope::MigrationDerived {
            predecessor_resource_id: Uuid::parse_str(
                scope
                    .get("predecessor_resource_id")
                    .and_then(Value::as_str)
                    .context("migration_derived scope must include predecessor_resource_id")?,
            )
            .context("migration_derived scope predecessor_resource_id must be a UUID")?,
        }),
        "transport_derived" => Ok(PermissionScope::TransportDerived {
            transport: scope
                .get("transport")
                .and_then(Value::as_str)
                .context("transport_derived scope must include transport")?
                .to_owned(),
        }),
        _ => bail!("unsupported PermissionChanged scope kind {kind}"),
    }
}

pub(super) fn json_text(value: &Value, path: &[&str]) -> Result<String> {
    let mut current = value;
    for segment in path {
        current = current
            .get(*segment)
            .with_context(|| format!("missing PermissionChanged field {}", path.join(".")))?;
    }

    current.as_str().map(str::to_owned).with_context(|| {
        format!(
            "PermissionChanged field {} must be a string",
            path.join(".")
        )
    })
}

pub(super) fn json_string_array(value: &Value, path: &[&str]) -> Result<Vec<String>> {
    let mut current = value;
    for segment in path {
        current = current
            .get(*segment)
            .with_context(|| format!("missing PermissionChanged field {}", path.join(".")))?;
    }

    current
        .as_array()
        .with_context(|| {
            format!(
                "PermissionChanged field {} must be an array",
                path.join(".")
            )
        })?
        .iter()
        .map(|item| {
            item.as_str().map(str::to_owned).with_context(|| {
                format!(
                    "PermissionChanged field {} must contain strings",
                    path.join(".")
                )
            })
        })
        .collect()
}

pub(super) fn json_object_or_default(value: &Value, field: &str) -> Value {
    match value.get(field) {
        Some(Value::Object(_)) => value[field].clone(),
        _ => json!({}),
    }
}

pub(super) fn json_optional_object(value: &Value, field: &str) -> Option<Value> {
    match value.get(field) {
        Some(Value::Object(_)) => Some(value[field].clone()),
        _ => None,
    }
}

pub(super) fn build_provenance(events: &[&RelevantEvent]) -> Result<Value> {
    let normalized_event_ids = events
        .iter()
        .map(|event| event.normalized_event_id)
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
        "normalized_event_ids": normalized_event_ids,
        "raw_fact_refs": raw_fact_refs,
        "manifest_versions": manifest_versions,
        "execution_trace_id": Value::Null,
        "derivation_kind": PERMISSIONS_CURRENT_DERIVATION_KIND,
    }))
}

pub(super) fn build_coverage(events: &[&RelevantEvent]) -> Value {
    let source_classes_considered = events
        .iter()
        .map(|event| event.source_family.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(Value::String)
        .collect::<Vec<_>>();

    json!({
        "status": "full",
        "exhaustiveness": "authoritative",
        "source_classes_considered": source_classes_considered,
        "unsupported_reason": Value::Null,
        "enumeration_basis": PERMISSIONS_ENUMERATION_BASIS,
    })
}
