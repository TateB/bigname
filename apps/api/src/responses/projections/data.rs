pub(super) fn build_address_name_expansion_facts(row: &NameCurrentRow) -> AddressNameExpansionFacts {
    AddressNameExpansionFacts {
        status: supported_summary_field(
            provenance_field(&row.declared_summary, "control"),
            "status",
        ),
        expiry: supported_summary_field(
            provenance_field(&row.declared_summary, "control"),
            "expiry",
        ),
        record_count: supported_summary_field(
            provenance_field(&row.declared_summary, "record_inventory"),
            "count",
        ),
    }
}

pub(super) fn build_name_data(row: &NameCurrentRow) -> JsonValue {
    let mut data = empty_object();
    insert_string_field(&mut data, "logical_name_id", row.logical_name_id.clone());
    insert_string_field(&mut data, "namespace", row.namespace.clone());
    insert_string_field(&mut data, "normalized_name", row.normalized_name.clone());
    insert_string_field(
        &mut data,
        "canonical_display_name",
        row.canonical_display_name.clone(),
    );
    insert_string_field(&mut data, "namehash", row.namehash.clone());
    insert_optional_string_field(
        &mut data,
        "resource_id",
        row.resource_id.map(|value| value.to_string()),
    );
    insert_optional_string_field(
        &mut data,
        "token_lineage_id",
        row.token_lineage_id.map(|value| value.to_string()),
    );
    insert_optional_string_field(
        &mut data,
        "binding_kind",
        row.binding_kind.map(|value| value.as_str().to_owned()),
    );
    data
}
