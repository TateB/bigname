pub(super) fn build_name_declared_state(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> JsonValue {
    let mut declared_state = empty_object();
    insert_value_field(
        &mut declared_state,
        "registration",
        declared_summary_section(
            &row.declared_summary,
            "registration",
            "declared registration summary is not yet projected",
        ),
    );
    insert_value_field(
        &mut declared_state,
        "authority",
        declared_authority_section(row),
    );
    insert_value_field(
        &mut declared_state,
        "control",
        declared_name_control_section(&row.declared_summary),
    );
    insert_value_field(
        &mut declared_state,
        "resolver",
        declared_summary_section(
            &row.declared_summary,
            "resolver",
            "declared resolver summary is not yet projected",
        ),
    );
    insert_value_field(
        &mut declared_state,
        "record_inventory",
        build_record_inventory_section_for_name(
            row,
            record_inventory_row,
            "declared record inventory summary is not yet projected",
        ),
    );
    insert_value_field(
        &mut declared_state,
        "history",
        declared_summary_section(
            &row.declared_summary,
            "history",
            "declared history pointers are not yet projected",
        ),
    );
    declared_state
}

pub(super) fn build_name_surface_binding_explain_declared_state(
    row: &NameCurrentRow,
) -> JsonValue {
    let mut declared_state = empty_object();
    insert_value_field(
        &mut declared_state,
        "surface_binding",
        build_name_surface_binding_explain_summary(row),
    );
    insert_value_field(
        &mut declared_state,
        "history",
        declared_summary_section(
            &row.declared_summary,
            "history",
            "declared history pointers are not yet projected",
        ),
    );
    declared_state
}

pub(super) fn build_name_authority_control_explain_declared_state(
    row: &NameCurrentRow,
) -> JsonValue {
    let mut declared_state = empty_object();
    insert_value_field(
        &mut declared_state,
        "authority",
        declared_authority_section(row),
    );
    insert_value_field(
        &mut declared_state,
        "control",
        declared_name_control_section(&row.declared_summary),
    );
    declared_state
}

fn build_name_surface_binding_explain_summary(row: &NameCurrentRow) -> JsonValue {
    let has_binding_summary = row.surface_binding_id.is_some() || row.binding_kind.is_some();
    if !has_binding_summary {
        return unsupported_section("declared surface binding summary is not yet projected");
    }

    let mut surface_binding = empty_object();
    insert_optional_string_field(
        &mut surface_binding,
        "surface_binding_id",
        row.surface_binding_id.map(|value| value.to_string()),
    );
    insert_optional_string_field(
        &mut surface_binding,
        "binding_kind",
        row.binding_kind.map(|value| value.as_str().to_owned()),
    );
    surface_binding
}

fn declared_authority_section(row: &NameCurrentRow) -> JsonValue {
    if let Some(section) =
        provenance_field(&row.declared_summary, "authority").filter(|value| value.is_object())
    {
        return section.clone();
    }

    let has_binding_summary =
        row.resource_id.is_some() || row.token_lineage_id.is_some() || row.binding_kind.is_some();
    if !has_binding_summary {
        return unsupported_section("declared authority summary is not yet projected");
    }

    let mut authority = empty_object();
    insert_optional_string_field(
        &mut authority,
        "resource_id",
        row.resource_id.map(|value| value.to_string()),
    );
    insert_optional_string_field(
        &mut authority,
        "token_lineage_id",
        row.token_lineage_id.map(|value| value.to_string()),
    );
    insert_optional_string_field(
        &mut authority,
        "binding_kind",
        row.binding_kind.map(|value| value.as_str().to_owned()),
    );
    authority
}

fn declared_name_control_section(summary: &JsonValue) -> JsonValue {
    let Some(section) = provenance_field(summary, "control").filter(|value| value.is_object())
    else {
        return unsupported_section("declared control summary is not yet projected");
    };

    if summary_is_unsupported(Some(section)) {
        return section.clone();
    }

    let mut control = empty_object();
    insert_value_field(
        &mut control,
        "registrant",
        provenance_field(section, "registrant")
            .cloned()
            .unwrap_or(JsonValue::Null),
    );
    insert_value_field(
        &mut control,
        "registry_owner",
        provenance_field(section, "registry_owner")
            .cloned()
            .unwrap_or(JsonValue::Null),
    );
    insert_value_field(
        &mut control,
        "latest_event_kind",
        provenance_field(section, "latest_event_kind")
            .cloned()
            .unwrap_or(JsonValue::Null),
    );
    control
}

fn declared_summary_section(summary: &JsonValue, key: &str, unsupported_reason: &str) -> JsonValue {
    provenance_field(summary, key)
        .filter(|value| value.is_object())
        .cloned()
        .unwrap_or_else(|| unsupported_section(unsupported_reason))
}
