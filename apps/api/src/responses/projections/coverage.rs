pub(super) fn build_name_coverage(coverage: &JsonValue) -> JsonValue {
    let mut normalized = empty_object();
    insert_string_field(
        &mut normalized,
        "status",
        string_field(provenance_field(coverage, "status"))
            .unwrap_or_else(|| "unsupported".to_owned()),
    );
    insert_string_field(
        &mut normalized,
        "exhaustiveness",
        string_field(provenance_field(coverage, "exhaustiveness"))
            .unwrap_or_else(|| "not_applicable".to_owned()),
    );
    insert_value_field(
        &mut normalized,
        "source_classes_considered",
        array_or_empty(provenance_field(coverage, "source_classes_considered")),
    );
    insert_string_field(
        &mut normalized,
        "enumeration_basis",
        string_field(provenance_field(coverage, "enumeration_basis"))
            .unwrap_or_else(|| "exact_name".to_owned()),
    );
    insert_nullable_string_field(
        &mut normalized,
        "unsupported_reason",
        string_field(provenance_field(coverage, "unsupported_reason")),
    );
    normalized
}

pub(super) fn build_name_coverage_declared_state(coverage: &JsonValue) -> JsonValue {
    let mut declared_state = empty_object();
    insert_string_field(
        &mut declared_state,
        "status",
        string_field(provenance_field(coverage, "status"))
            .unwrap_or_else(|| "unsupported".to_owned()),
    );
    insert_string_field(
        &mut declared_state,
        "exhaustiveness",
        string_field(provenance_field(coverage, "exhaustiveness"))
            .unwrap_or_else(|| "not_applicable".to_owned()),
    );
    insert_value_field(
        &mut declared_state,
        "source_classes_considered",
        array_or_empty(provenance_field(coverage, "source_classes_considered")),
    );
    insert_string_field(
        &mut declared_state,
        "enumeration_basis",
        string_field(provenance_field(coverage, "enumeration_basis"))
            .unwrap_or_else(|| "exact_name".to_owned()),
    );
    insert_nullable_string_field(
        &mut declared_state,
        "unsupported_reason",
        string_field(provenance_field(coverage, "unsupported_reason")),
    );
    declared_state
}
