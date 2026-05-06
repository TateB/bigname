#[allow(clippy::too_many_arguments)]
fn build_compact_children_response(
    summary: &bigname_storage::ChildrenCurrentSummary,
    page_rows: &[ChildrenCurrentRow],
    parent_normalized_name: &str,
    child_labelhashes: &BTreeMap<String, Option<String>>,
    child_name_rows: &BTreeMap<String, NameCurrentRow>,
    child_summaries: &BTreeMap<String, bigname_storage::ChildrenCurrentSummary>,
    include_counts: bool,
    meta: MetaMode,
    page: HistoryPageResponse,
) -> JsonValue {
    let counts_supported = !include_counts
        || page_rows
            .iter()
            .all(|row| child_summaries.contains_key(&row.child_logical_name_id));
    let mut response = empty_object();
    insert_value_field(
        &mut response,
        "data",
        JsonValue::Array(
            page_rows
                .iter()
                .map(|row| {
                    build_compact_child_item(
                        row,
                        parent_normalized_name,
                        child_labelhashes,
                        child_name_rows.get(&row.child_logical_name_id),
                        child_summaries.get(&row.child_logical_name_id),
                        include_counts,
                    )
                })
                .collect(),
        ),
    );
    insert_value_field(
        &mut response,
        "page",
        serde_json::to_value(page).expect("children page response must serialize"),
    );
    if meta != MetaMode::None {
        insert_value_field(
            &mut response,
            "meta",
            build_compact_children_meta(summary, include_counts, counts_supported, meta),
        );
    }
    response
}

fn build_compact_child_item(
    row: &ChildrenCurrentRow,
    parent_normalized_name: &str,
    child_labelhashes: &BTreeMap<String, Option<String>>,
    child_name_row: Option<&NameCurrentRow>,
    child_summary: Option<&bigname_storage::ChildrenCurrentSummary>,
    include_counts: bool,
) -> JsonValue {
    let mut value = empty_object();
    insert_string_field(&mut value, "name", row.canonical_display_name.clone());
    insert_string_field(&mut value, "normalized_name", row.normalized_name.clone());
    insert_string_field(
        &mut value,
        "label_name",
        compact_child_label_name(&row.normalized_name, parent_normalized_name),
    );
    insert_optional_string_field(
        &mut value,
        "labelhash",
        child_labelhashes
            .get(&row.child_logical_name_id)
            .cloned()
            .flatten(),
    );
    insert_string_field(&mut value, "namehash", row.namehash.clone());
    insert_optional_string_field(
        &mut value,
        "owner",
        child_name_row.and_then(compact_child_owner),
    );
    insert_optional_string_field(
        &mut value,
        "registrant",
        child_name_row.and_then(compact_child_registrant),
    );
    if include_counts {
        insert_value_field(
            &mut value,
            "subname_count",
            child_summary
                .map(|summary| JsonValue::Number(summary.child_count.into()))
                .unwrap_or(JsonValue::Null),
        );
    }
    value
}

fn build_compact_children_meta(
    summary: &bigname_storage::ChildrenCurrentSummary,
    include_counts: bool,
    counts_supported: bool,
    meta: MetaMode,
) -> JsonValue {
    let unsupported_fields = if include_counts && !counts_supported {
        vec!["subname_count".to_owned()]
    } else {
        Vec::new()
    };
    let mut value = compact_meta_object(
        "supported",
        Some(u64::try_from(summary.child_count).unwrap_or_default()),
        unsupported_fields,
        std::iter::empty(),
    );

    if meta == MetaMode::Full {
        insert_value_field(
            &mut value,
            "provenance",
            build_collection_provenance_from_inputs(&summary.provenance_inputs, "declared"),
        );
        insert_value_field(
            &mut value,
            "coverage",
            serde_json::to_value(CoverageResponse {
                status: "full".to_owned(),
                exhaustiveness: "authoritative".to_owned(),
                source_classes_considered: vec!["declared".to_owned()],
                enumeration_basis: "declared_direct_children".to_owned(),
                unsupported_reason: None,
            })
            .expect("children coverage response must serialize"),
        );
        insert_value_field(
            &mut value,
            "chain_positions",
            build_chain_positions_from_values(summary.chain_positions.iter()),
        );
        insert_string_field(
            &mut value,
            "consistency",
            collection_consistency(summary.canonicality_summaries.iter()).to_owned(),
        );
        insert_string_field(
            &mut value,
            "last_updated",
            summary
                .last_recomputed_at
                .map(format_timestamp)
                .unwrap_or_else(|| format_timestamp(OffsetDateTime::now_utc())),
        );
    }

    value
}

fn compact_child_label_name(normalized_name: &str, parent_normalized_name: &str) -> String {
    let suffix = format!(".{parent_normalized_name}");
    if let Some(label) = normalized_name
        .strip_suffix(&suffix)
        .filter(|label| !label.is_empty() && !label.contains('.'))
    {
        return label.to_owned();
    }

    normalized_name
        .split('.')
        .next()
        .unwrap_or(normalized_name)
        .to_owned()
}

fn compact_child_owner(row: &NameCurrentRow) -> Option<String> {
    compact_child_declared_summary_string(&row.declared_summary, "control", "registry_owner")
        .or_else(|| compact_child_declared_summary_string(&row.declared_summary, "control", "owner"))
        .map(|value| value.to_ascii_lowercase())
}

fn compact_child_registrant(row: &NameCurrentRow) -> Option<String> {
    compact_child_declared_summary_string(&row.declared_summary, "control", "registrant")
        .or_else(|| {
            compact_child_declared_summary_string(&row.declared_summary, "registration", "registrant")
        })
        .map(|value| value.to_ascii_lowercase())
}

fn compact_child_declared_summary_string(
    summary: &JsonValue,
    section: &str,
    field: &str,
) -> Option<String> {
    provenance_field(summary, section)
        .and_then(|section| provenance_field(section, field))
        .and_then(value_to_string)
        .filter(|value| !value.trim().is_empty())
}
