use super::topology::{build_resolution_resolver_hop, projected_resolution_resolver_path};

pub(super) fn build_resolution_execution_summary(
    row: &NameCurrentRow,
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
) -> Result<JsonValue> {
    if trace.request_type != VERIFIED_RESOLUTION_REQUEST_TYPE
        || outcome.request_type != VERIFIED_RESOLUTION_REQUEST_TYPE
    {
        bail!(
            "persisted execution explain requires request_type {VERIFIED_RESOLUTION_REQUEST_TYPE}"
        );
    }

    let mut execution = empty_object();
    insert_string_field(
        &mut execution,
        "execution_trace_id",
        trace.execution_trace_id.to_string(),
    );
    insert_value_field(
        &mut execution,
        "selected_entrypoint",
        build_resolution_selected_entrypoint(trace),
    );
    insert_value_field(
        &mut execution,
        "resolver_discovery_path",
        build_resolution_execution_resolver_discovery_path(row, trace),
    );
    insert_value_field(
        &mut execution,
        "wildcard",
        build_resolution_execution_wildcard(trace),
    );
    insert_value_field(
        &mut execution,
        "alias",
        build_resolution_execution_alias(trace),
    );
    insert_value_field(
        &mut execution,
        "steps",
        JsonValue::Array(
            trace
                .steps
                .iter()
                .map(build_execution_step_summary)
                .collect(),
        ),
    );
    insert_string_field(
        &mut execution,
        "finished_at",
        format_timestamp(trace.finished_at.unwrap_or(outcome.finished_at)),
    );

    Ok(execution)
}

fn build_resolution_selected_entrypoint(trace: &ExecutionTrace) -> JsonValue {
    let source_family = provenance_field(&trace.manifest_context, "manifest_versions")
        .and_then(JsonValue::as_array)
        .and_then(|items| {
            items
                .iter()
                .find_map(|item| string_field(provenance_field(item, "source_family")))
        });
    let role =
        string_field(provenance_field(&trace.request_metadata, "entrypoint")).or_else(|| {
            trace
                .steps
                .iter()
                .find_map(|step| string_field(provenance_field(&step.step_payload, "entrypoint")))
        });
    let contract_call = trace
        .contracts_called
        .as_array()
        .and_then(|items| items.iter().find(|item| item.is_object()));

    let chain_id = string_field(contract_call.and_then(|item| provenance_field(item, "chain_id")));
    let contract_address = string_field(provenance_field(
        &trace.request_metadata,
        "contract_address",
    ))
    .or_else(|| {
        trace
            .steps
            .iter()
            .find_map(|step| string_field(provenance_field(&step.step_payload, "resolver")))
    })
    .or_else(|| {
        string_field(contract_call.and_then(|item| provenance_field(item, "contract_address")))
    });

    let mut selected_entrypoint = empty_object();
    insert_nullable_string_field(&mut selected_entrypoint, "source_family", source_family);
    insert_nullable_string_field(&mut selected_entrypoint, "role", role);
    insert_nullable_string_field(&mut selected_entrypoint, "chain_id", chain_id);
    insert_nullable_string_field(
        &mut selected_entrypoint,
        "contract_address",
        contract_address,
    );
    selected_entrypoint
}

fn build_resolution_execution_resolver_discovery_path(
    row: &NameCurrentRow,
    trace: &ExecutionTrace,
) -> JsonValue {
    if let Some(resolver_path) = projected_resolution_resolver_path(&row.declared_summary) {
        return resolver_path;
    }

    let declared_resolver = provenance_field(&row.declared_summary, "resolver");
    let chain_id = trace
        .contracts_called
        .as_array()
        .and_then(|items| items.iter().find(|item| item.is_object()))
        .and_then(|item| string_field(provenance_field(item, "chain_id")))
        .or_else(|| {
            string_field(declared_resolver.and_then(|value| provenance_field(value, "chain_id")))
        });
    let address = trace
        .steps
        .iter()
        .find_map(|step| string_field(provenance_field(&step.step_payload, "resolver")))
        .or_else(|| {
            string_field(declared_resolver.and_then(|value| provenance_field(value, "address")))
        });
    let latest_event_kind = string_field(
        declared_resolver.and_then(|value| provenance_field(value, "latest_event_kind")),
    );

    JsonValue::Array(vec![build_resolution_resolver_hop(
        row,
        chain_id,
        address,
        latest_event_kind,
    )])
}

fn build_resolution_execution_wildcard(trace: &ExecutionTrace) -> JsonValue {
    persisted_trace_detail_object(trace, "wildcard").unwrap_or_else(|| {
        json!({
            "source": null,
            "matched_labels": [],
        })
    })
}

fn build_resolution_execution_alias(trace: &ExecutionTrace) -> JsonValue {
    persisted_trace_detail_object(trace, "alias").unwrap_or_else(|| {
        json!({
            "final_target": null,
            "hops": [],
        })
    })
}

fn build_execution_step_summary(step: &bigname_storage::ExecutionTraceStep) -> JsonValue {
    let mut summary = empty_object();
    insert_value_field(
        &mut summary,
        "step_index",
        JsonValue::Number(step.step_index.into()),
    );
    insert_string_field(&mut summary, "step_kind", step.step_kind.clone());
    insert_nullable_string_field(&mut summary, "input_digest", step.input_digest.clone());
    insert_nullable_string_field(&mut summary, "output_digest", step.output_digest.clone());
    insert_value_field(
        &mut summary,
        "latency",
        step.latency_ms
            .map(|value| JsonValue::Number(value.into()))
            .unwrap_or(JsonValue::Null),
    );
    insert_value_field(
        &mut summary,
        "canonicality_dependency",
        ensure_object(&step.canonicality_dependency),
    );
    summary
}
fn persisted_trace_detail_object(trace: &ExecutionTrace, key: &str) -> Option<JsonValue> {
    provenance_field(&trace.request_metadata, key)
        .filter(|value| value.is_object())
        .cloned()
        .or_else(|| {
            trace
                .steps
                .iter()
                .find_map(|step| {
                    provenance_field(&step.step_payload, key).filter(|value| value.is_object())
                })
                .cloned()
        })
}
