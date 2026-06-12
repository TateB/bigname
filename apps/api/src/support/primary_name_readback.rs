use super::*;

pub(super) fn manifest_versions_contain_source_family(
    manifest_versions: &JsonValue,
    expected_source_family: &str,
    context: &str,
) -> Result<bool> {
    let manifest_versions = manifest_versions
        .as_array()
        .with_context(|| format!("{context} must be a JSON array"))?;

    for (index, manifest_version) in manifest_versions.iter().enumerate() {
        let manifest_version = manifest_version
            .as_object()
            .with_context(|| format!("{context}[{index}] must be a JSON object"))?;
        if manifest_version
            .get("source_family")
            .and_then(JsonValue::as_str)
            .is_some_and(|source_family| source_family == expected_source_family)
        {
            return Ok(true);
        }
    }

    Ok(false)
}

pub(super) fn ensure_persisted_primary_name_execution_source_family(
    outcome: &ExecutionOutcome,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> ApiResult<()> {
    let expected_source_family =
        persisted_primary_name_execution_source_family(address, namespace, coin_type)?;

    let includes_expected_source_family = manifest_versions_contain_source_family(
        &outcome.cache_key.manifest_versions,
        expected_source_family,
        "persisted verified primary-name cache_key.manifest_versions",
    )
    .map_err(|load_error| {
        error!(
            service = "api",
            address = %address,
            namespace = %namespace,
            coin_type = %coin_type,
            execution_trace_id = %outcome.execution_trace_id,
            error = ?load_error,
            manifest_versions = ?outcome.cache_key.manifest_versions,
            "persisted verified primary-name manifest_versions malformed"
        );
        ApiError::internal_error(format!(
            "persisted verified primary-name provenance mismatch for address {address}"
        ))
    })?;

    if !includes_expected_source_family {
        error!(
            service = "api",
            address = %address,
            namespace = %namespace,
            coin_type = %coin_type,
            execution_trace_id = %outcome.execution_trace_id,
            expected_source_family = %expected_source_family,
            manifest_versions = ?outcome.cache_key.manifest_versions,
            "persisted verified primary-name execution source-family mismatch"
        );
        return Err(ApiError::internal_error(format!(
            "persisted verified primary-name provenance mismatch for address {address}"
        )));
    }

    Ok(())
}

pub(super) fn persisted_verified_primary_name_cache_identity_is_current(
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> ApiResult<bool> {
    if !trace_cache_identity_matches_outcome(trace, outcome, address, namespace, coin_type)? {
        return Ok(false);
    }

    Ok(true)
}

fn persisted_primary_name_execution_source_family(
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> ApiResult<&'static str> {
    match namespace {
        "ens" => Ok("ens_execution"),
        "basenames" => Ok("basenames_execution"),
        _ => {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                "persisted verified primary-name namespace unsupported for execution source-family check"
            );
            Err(ApiError::internal_error(format!(
                "persisted verified primary-name provenance mismatch for address {address}"
            )))
        }
    }
}

fn warn_cache_miss(
    trace: &ExecutionTrace,
    address: &str,
    namespace: &str,
    coin_type: &str,
    reason: &str,
) {
    warn!(
        service = "api",
        address = %address,
        namespace = %namespace,
        coin_type = %coin_type,
        execution_trace_id = %trace.execution_trace_id,
        reason = %reason,
        "persisted verified primary-name cache identity mismatch"
    );
}

fn trace_cache_identity_matches_outcome(
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> ApiResult<bool> {
    let expected_request_key = primary_name_verified_request_key(namespace, address, coin_type);
    if trace.request_type != VERIFIED_PRIMARY_NAME_REQUEST_TYPE
        || trace.namespace != namespace
        || trace.request_key != expected_request_key
    {
        warn_cache_miss(trace, address, namespace, coin_type, "trace tuple identity");
        return Ok(false);
    }

    let Some(trace_metadata) = trace.request_metadata.as_object() else {
        warn_cache_miss(trace, address, namespace, coin_type, "trace metadata");
        return Ok(false);
    };
    let metadata_address = trace_metadata
        .get("normalized_address")
        .and_then(JsonValue::as_str);
    let metadata_namespace = trace_metadata.get("namespace").and_then(JsonValue::as_str);
    let metadata_coin_type = trace_metadata.get("coin_type").and_then(JsonValue::as_str);
    if metadata_address != Some(address)
        || metadata_namespace != Some(namespace)
        || metadata_coin_type != Some(coin_type)
    {
        warn_cache_miss(trace, address, namespace, coin_type, "trace metadata tuple");
        return Ok(false);
    }

    let Some(cache_identity) = trace_metadata.get("cache_identity").and_then(JsonValue::as_object)
    else {
        warn_cache_miss(trace, address, namespace, coin_type, "trace cache_identity");
        return Ok(false);
    };

    if trace.chain_context.get("requested_positions")
        != Some(&outcome.cache_key.requested_chain_positions)
    {
        warn_cache_miss(trace, address, namespace, coin_type, "requested positions");
        return Ok(false);
    }

    if trace.manifest_context.get("manifest_versions") != Some(&outcome.cache_key.manifest_versions)
    {
        warn_cache_miss(trace, address, namespace, coin_type, "trace manifest versions");
        return Ok(false);
    }

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
        if cache_identity.get(field) != Some(expected) {
            warn_cache_miss(trace, address, namespace, coin_type, field);
            return Ok(false);
        }
    }

    Ok(true)
}

pub(super) fn persisted_verified_primary_name_section(
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> ApiResult<JsonValue> {
    let request_key = primary_name_verified_request_key(namespace, address, coin_type);
    if trace.request_type != VERIFIED_PRIMARY_NAME_REQUEST_TYPE
        || trace.namespace != namespace
        || trace.request_key != request_key
    {
        error!(
            service = "api",
            address = %address,
            namespace = %namespace,
            coin_type = %coin_type,
            request_type = %trace.request_type,
            trace_namespace = %trace.namespace,
            trace_request_key = %trace.request_key,
            "persisted verified primary-name trace identity mismatch"
        );
        return Err(ApiError::internal_error(format!(
            "persisted verified primary-name trace identity mismatch for address {address}"
        )));
    }

    let trace_metadata = trace.request_metadata.as_object().ok_or_else(|| {
        error!(
            service = "api",
            address = %address,
            namespace = %namespace,
            coin_type = %coin_type,
            execution_trace_id = %trace.execution_trace_id,
            "persisted verified primary-name trace metadata missing"
        );
        ApiError::internal_error(format!(
            "persisted verified primary-name trace metadata missing for address {address}"
        ))
    })?;

    let metadata_address = trace_metadata
        .get("normalized_address")
        .and_then(JsonValue::as_str);
    let metadata_namespace = trace_metadata.get("namespace").and_then(JsonValue::as_str);
    let metadata_coin_type = trace_metadata.get("coin_type").and_then(JsonValue::as_str);
    if metadata_address != Some(address)
        || metadata_namespace != Some(namespace)
        || metadata_coin_type != Some(coin_type)
    {
        error!(
            service = "api",
            address = %address,
            namespace = %namespace,
            coin_type = %coin_type,
            execution_trace_id = %trace.execution_trace_id,
            metadata = ?trace.request_metadata,
            "persisted verified primary-name trace tuple mismatch"
        );
        return Err(ApiError::internal_error(format!(
            "persisted verified primary-name trace tuple mismatch for address {address}"
        )));
    }

    ensure_persisted_primary_name_execution_source_family(outcome, address, namespace, coin_type)?;

    let verified_primary_name = extract_persisted_verified_primary_name_section(
        outcome.outcome_payload.as_ref(),
        "persisted verified primary-name outcome_payload",
        namespace,
    )
    .map_err(|load_error| {
        error!(
            service = "api",
            address = %address,
            namespace = %namespace,
            coin_type = %coin_type,
            execution_trace_id = %trace.execution_trace_id,
            error = ?load_error,
            "persisted verified primary-name outcome section invalid"
        );
        ApiError::internal_error(format!(
            "persisted verified primary-name payload mismatch for address {address}"
        ))
    })?
    .ok_or_else(|| {
        error!(
            service = "api",
            address = %address,
            namespace = %namespace,
            coin_type = %coin_type,
            execution_trace_id = %trace.execution_trace_id,
            "persisted verified primary-name outcome section missing"
        );
        ApiError::internal_error(format!(
            "persisted verified primary-name outcome missing for address {address}"
        ))
    })?;

    let status = verified_primary_name
        .get("status")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                execution_trace_id = %trace.execution_trace_id,
                "persisted verified primary-name status missing"
            );
            ApiError::internal_error(format!(
                "persisted verified primary-name status missing for address {address}"
            ))
        })?;

    if status == "execution_failed" {
        if trace.final_payload.is_some()
            || !outcome
                .failure_payload
                .as_ref()
                .is_some_and(JsonValue::is_object)
            || !trace
                .failure_payload
                .as_ref()
                .is_some_and(JsonValue::is_object)
        {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                execution_trace_id = %trace.execution_trace_id,
                "persisted verified primary-name execution_failed payload mismatch"
            );
            return Err(ApiError::internal_error(format!(
                "persisted verified primary-name payload mismatch for address {address}"
            )));
        }
    } else {
        let trace_verified_primary_name = extract_persisted_verified_primary_name_section(
            trace.final_payload.as_ref(),
            "persisted verified primary-name trace.final_payload",
            namespace,
        )
        .map_err(|load_error| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                execution_trace_id = %trace.execution_trace_id,
                error = ?load_error,
                "persisted verified primary-name trace final payload invalid"
            );
            ApiError::internal_error(format!(
                "persisted verified primary-name payload mismatch for address {address}"
            ))
        })?;
        if trace.failure_payload.is_some()
            || outcome.failure_payload.is_some()
            || trace_verified_primary_name.as_ref() != Some(&verified_primary_name)
        {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                execution_trace_id = %trace.execution_trace_id,
                "persisted verified primary-name final payload mismatch"
            );
            return Err(ApiError::internal_error(format!(
                "persisted verified primary-name payload mismatch for address {address}"
            )));
        }
    }

    Ok(verified_primary_name)
}

pub(super) fn extract_persisted_verified_primary_name_section(
    payload: Option<&JsonValue>,
    context: &str,
    namespace: &str,
) -> Result<Option<JsonValue>> {
    let Some(payload) = payload else {
        return Ok(None);
    };
    let payload = payload
        .as_object()
        .with_context(|| format!("{context} must be a JSON object"))?;
    ensure_allowed_json_fields(payload, &["verified_primary_name"], context)?;

    let section_context = format!("{context}.verified_primary_name");
    let section = payload
        .get("verified_primary_name")
        .and_then(JsonValue::as_object)
        .with_context(|| format!("{section_context} must be a JSON object"))?;
    ensure_allowed_json_fields(
        section,
        &["status", "name", "failure_reason"],
        &section_context,
    )?;

    match required_json_string_field(section, "status", &section_context)? {
        "success" => {
            validate_persisted_verified_primary_name_ref(
                section.get("name"),
                &format!("{section_context}.name"),
                namespace,
            )?;
            ensure_json_field_absent(section, "failure_reason", &section_context)?;
        }
        "not_found" => {
            ensure_json_field_absent(section, "name", &section_context)?;
            optional_nonempty_json_string_field(section, "failure_reason", &section_context)?;
        }
        "mismatch" => {
            validate_persisted_verified_primary_name_ref(
                section.get("name"),
                &format!("{section_context}.name"),
                namespace,
            )?;
            optional_nonempty_json_string_field(section, "failure_reason", &section_context)?;
        }
        "invalid_name" => {
            ensure_json_field_absent(section, "name", &section_context)?;
            optional_nonempty_json_string_field(section, "failure_reason", &section_context)?;
        }
        "execution_failed" => {
            ensure_json_field_absent(section, "name", &section_context)?;
            required_json_string_field(section, "failure_reason", &section_context)?;
        }
        status => {
            bail!(
                "{section_context} only supports success, not_found, mismatch, invalid_name, and execution_failed; found {status}"
            );
        }
    }

    Ok(Some(JsonValue::Object(section.clone())))
}

pub(super) fn validate_persisted_verified_primary_name_ref(
    value: Option<&JsonValue>,
    context: &str,
    expected_namespace: &str,
) -> Result<()> {
    let name = value
        .and_then(JsonValue::as_object)
        .with_context(|| format!("{context} must be a JSON object"))?;
    ensure_allowed_json_fields(
        name,
        &[
            "logical_name_id",
            "namespace",
            "normalized_name",
            "canonical_display_name",
            "namehash",
            "resource_id",
            "binding_kind",
        ],
        context,
    )?;

    let logical_name_id = required_json_string_field(name, "logical_name_id", context)?;
    let namespace = required_json_string_field(name, "namespace", context)?;
    let normalized_name = required_json_string_field(name, "normalized_name", context)?;
    required_json_string_field(name, "canonical_display_name", context)?;
    required_json_string_field(name, "namehash", context)?;
    optional_nonempty_json_string_field(name, "resource_id", context)?;
    optional_nonempty_json_string_field(name, "binding_kind", context)?;

    if namespace != expected_namespace {
        bail!("{context}.namespace must be {expected_namespace}");
    }
    if logical_name_id != format!("{expected_namespace}:{normalized_name}") {
        bail!(
            "{context}.logical_name_id {logical_name_id} does not match normalized_name {normalized_name}"
        );
    }

    Ok(())
}
