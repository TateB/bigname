use anyhow::{Context, Result, bail};
use bigname_storage::{ExecutionOutcome, ExecutionTrace};
use serde_json::Value;

use crate::json_helpers::{
    ensure_absent, ensure_only_allowed_fields, optional_nonempty_string_field,
    required_coin_type_field, required_nonempty_string_field, required_object, required_string,
};
use crate::validation::normalize_address;

use super::context::verified_primary_context_label;
use super::name_ref::validate_verified_primary_name_ref;
use super::{VerifiedPrimaryNameSection, VerifiedPrimaryNameStatus, VerifiedPrimaryNameTuple};

pub(super) fn extract_verified_primary_tuple(
    trace: &ExecutionTrace,
) -> Result<VerifiedPrimaryNameTuple> {
    let trace_namespace = trace.namespace.as_str();
    let context = verified_primary_context_label(trace_namespace)?;
    let request_metadata = required_object(
        Some(&trace.request_metadata),
        &format!("{context} trace.request_metadata"),
    )?;
    let normalized_address = required_string(
        request_metadata,
        "normalized_address",
        &format!("{context} trace.request_metadata"),
    )?
    .to_owned();
    if normalized_address != normalize_address(&normalized_address) {
        bail!("{context} trace.request_metadata.normalized_address must already be lowercase");
    }

    let coin_type = required_coin_type_field(
        request_metadata,
        "coin_type",
        &format!("{context} trace.request_metadata"),
    )?;
    let namespace = if let Some(namespace) = optional_nonempty_string_field(
        request_metadata,
        "namespace",
        &format!("{context} trace.request_metadata"),
    )? {
        if namespace != trace_namespace {
            bail!(
                "{context} trace.request_metadata.namespace must be {}",
                trace_namespace
            );
        }
        namespace.to_owned()
    } else {
        trace.namespace.clone()
    };

    Ok(VerifiedPrimaryNameTuple {
        namespace,
        normalized_address,
        coin_type,
    })
}

pub(super) fn validate_verified_primary_cache_identity(
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
    tuple: &VerifiedPrimaryNameTuple,
) -> Result<()> {
    let context = verified_primary_context_label(&tuple.namespace)?;
    let metadata_context = format!("{context} trace.request_metadata");
    let request_metadata = required_object(Some(&trace.request_metadata), &metadata_context)?;
    let cache_identity_context = format!("{metadata_context}.cache_identity");
    let cache_identity = required_object(
        request_metadata.get("cache_identity"),
        &cache_identity_context,
    )?;
    ensure_only_allowed_fields(
        cache_identity,
        &[
            "requested_chain_positions",
            "manifest_versions",
            "topology_version_boundary",
            "record_version_boundary",
        ],
        &cache_identity_context,
    )?;

    let expected_fields = [
        (
            "requested_chain_positions",
            &outcome.cache_key.requested_chain_positions,
        ),
        ("manifest_versions", &outcome.cache_key.manifest_versions),
        (
            "topology_version_boundary",
            &outcome.cache_key.topology_version_boundary,
        ),
        (
            "record_version_boundary",
            &outcome.cache_key.record_version_boundary,
        ),
    ];
    for (field, expected) in expected_fields {
        let field_context = format!("{cache_identity_context}.{field}");
        let actual = cache_identity
            .get(field)
            .with_context(|| format!("{field_context} must be set"))?;
        if actual != expected {
            bail!("{field_context} must match cache_key.{field}");
        }
    }

    Ok(())
}

pub(super) fn extract_verified_primary_name_section(
    payload: Option<&Value>,
    context: &str,
    namespace: &str,
) -> Result<VerifiedPrimaryNameSection> {
    let payload = required_object(payload, context)?;
    ensure_only_allowed_fields(payload, &["verified_primary_name"], context)?;

    let section_context = format!("{context}.verified_primary_name");
    let section = required_object(payload.get("verified_primary_name"), &section_context)?;
    ensure_only_allowed_fields(
        section,
        &["status", "name", "failure_reason"],
        &section_context,
    )?;

    let status = match required_string(section, "status", &section_context)? {
        "success" => {
            validate_verified_primary_name_ref(
                section.get("name"),
                &format!("{section_context}.name"),
                namespace,
            )?;
            ensure_absent(section, "failure_reason", &section_context)?;
            VerifiedPrimaryNameStatus::Success
        }
        "not_found" => {
            ensure_absent(section, "name", &section_context)?;
            optional_nonempty_string_field(section, "failure_reason", &section_context)?;
            VerifiedPrimaryNameStatus::NotFound
        }
        "mismatch" => {
            validate_verified_primary_name_ref(
                section.get("name"),
                &format!("{section_context}.name"),
                namespace,
            )?;
            optional_nonempty_string_field(section, "failure_reason", &section_context)?;
            VerifiedPrimaryNameStatus::Mismatch
        }
        "invalid_name" => {
            ensure_absent(section, "name", &section_context)?;
            optional_nonempty_string_field(section, "failure_reason", &section_context)?;
            VerifiedPrimaryNameStatus::InvalidName
        }
        "execution_failed" => {
            ensure_absent(section, "name", &section_context)?;
            required_nonempty_string_field(section, "failure_reason", &section_context)?;
            VerifiedPrimaryNameStatus::ExecutionFailed
        }
        status => bail!(
            "verified-primary only supports success, not_found, mismatch, invalid_name, and execution_failed; found {status}"
        ),
    };

    Ok(VerifiedPrimaryNameSection {
        section: Value::Object(section.clone()),
        status,
    })
}
