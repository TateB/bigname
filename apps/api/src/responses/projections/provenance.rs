pub(super) fn build_name_provenance(provenance: &JsonValue) -> JsonValue {
    let mut normalized = empty_object();
    insert_value_field(
        &mut normalized,
        "normalized_event_ids",
        array_value_strings(provenance_field(provenance, "normalized_event_ids")),
    );
    insert_value_field(
        &mut normalized,
        "raw_fact_refs",
        array_or_empty(provenance_field(provenance, "raw_fact_refs")),
    );
    insert_value_field(
        &mut normalized,
        "manifest_versions",
        array_or_empty(provenance_field(provenance, "manifest_versions")),
    );
    if let Some(execution_trace_id) = string_field(provenance_field(provenance, "execution_trace_id"))
    {
        insert_string_field(&mut normalized, "execution_trace_id", execution_trace_id);
    }
    insert_string_field(
        &mut normalized,
        "derivation_kind",
        string_field(provenance_field(provenance, "derivation_kind"))
            .unwrap_or_else(|| "declared".to_owned()),
    );
    normalized
}

pub(super) fn build_name_provenance_with_execution_trace(
    provenance: &JsonValue,
    execution_trace_id: Option<Uuid>,
) -> JsonValue {
    let mut normalized = build_name_provenance(provenance);
    if let Some(execution_trace_id) = execution_trace_id
        .map(|value| value.to_string())
        .or_else(|| string_field(provenance_field(provenance, "execution_trace_id")))
    {
        insert_string_field(&mut normalized, "execution_trace_id", execution_trace_id);
    }
    normalized
}
