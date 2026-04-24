pub(super) fn build_record_inventory_section(
    row: Option<&RecordInventoryCurrentRow>,
    unsupported_reason: &str,
) -> JsonValue {
    row.map(build_record_inventory_state)
        .unwrap_or_else(|| unsupported_section(unsupported_reason))
}

pub(super) fn build_record_cache_section(
    row: Option<&RecordInventoryCurrentRow>,
    records: &[ResolutionRecordKey],
    unsupported_reason: &str,
) -> JsonValue {
    row.map(|row| build_record_cache_state(row, records))
        .unwrap_or_else(|| unsupported_section(unsupported_reason))
}

fn build_record_inventory_state(row: &RecordInventoryCurrentRow) -> JsonValue {
    let mut record_inventory = empty_object();
    insert_value_field(
        &mut record_inventory,
        "record_version_boundary",
        row.record_version_boundary.clone(),
    );
    insert_value_field(
        &mut record_inventory,
        "enumeration_basis",
        ensure_object(&row.enumeration_basis),
    );
    insert_value_field(
        &mut record_inventory,
        "selectors",
        array_or_empty(Some(&row.selectors)),
    );
    insert_value_field(
        &mut record_inventory,
        "explicit_gaps",
        array_or_empty(Some(&row.explicit_gaps)),
    );
    insert_value_field(
        &mut record_inventory,
        "unsupported_families",
        array_or_empty(Some(&row.unsupported_families)),
    );
    insert_value_field(
        &mut record_inventory,
        "last_change",
        row.last_change.clone().unwrap_or(JsonValue::Null),
    );
    record_inventory
}

fn build_record_cache_state(
    row: &RecordInventoryCurrentRow,
    records: &[ResolutionRecordKey],
) -> JsonValue {
    let mut record_cache = empty_object();
    insert_value_field(
        &mut record_cache,
        "record_version_boundary",
        row.record_version_boundary.clone(),
    );
    insert_value_field(
        &mut record_cache,
        "entries",
        build_record_cache_entries(row, records),
    );
    record_cache
}

fn build_record_cache_entries(
    row: &RecordInventoryCurrentRow,
    records: &[ResolutionRecordKey],
) -> JsonValue {
    let entry_lookup = row
        .entries
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            string_field(provenance_field(entry, "record_key"))
                .map(|record_key| (record_key, entry))
        })
        .map(|(record_key, entry)| (record_key, entry.clone()))
        .collect::<BTreeMap<_, _>>();
    let unsupported_family_lookup = row
        .unsupported_families
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|family| {
            Some((
                string_field(provenance_field(family, "record_family"))?,
                string_field(provenance_field(family, "unsupported_reason"))?,
            ))
        })
        .collect::<BTreeMap<_, _>>();
    let cacheable_selector_lookup = row
        .selectors
        .as_array()
        .into_iter()
        .flatten()
        .filter(|selector| {
            provenance_field(selector, "cacheable").and_then(JsonValue::as_bool) == Some(true)
        })
        .filter_map(|selector| string_field(provenance_field(selector, "record_key")))
        .collect::<BTreeSet<_>>();

    if records.is_empty() {
        return JsonValue::Array(
            row.selectors
                .as_array()
                .into_iter()
                .flatten()
                .filter(|selector| {
                    provenance_field(selector, "cacheable").and_then(JsonValue::as_bool)
                        == Some(true)
                })
                .filter_map(|selector| string_field(provenance_field(selector, "record_key")))
                .filter_map(|record_key| {
                    parse_resolution_record_key(&record_key).map(|record| {
                        entry_lookup
                            .get(&record_key)
                            .cloned()
                            .unwrap_or_else(|| {
                                build_missing_record_cache_entry(
                                    &record,
                                    &unsupported_family_lookup,
                                    &cacheable_selector_lookup,
                                )
                            })
                    })
                })
                .collect(),
        );
    }

    JsonValue::Array(
        records
            .iter()
            .map(|record| {
                entry_lookup
                    .get(&record.record_key)
                    .cloned()
                    .unwrap_or_else(|| {
                        build_missing_record_cache_entry(
                            record,
                            &unsupported_family_lookup,
                            &cacheable_selector_lookup,
                        )
                    })
            })
            .collect(),
    )
}

fn phase_unsupported_record_family_reason(record_family: &str) -> Option<&'static str> {
    match record_family {
        "abi" | "pubkey" => Some("record_family_not_supported_in_phase6_projection"),
        _ => None,
    }
}

fn build_missing_record_cache_entry(
    record: &ResolutionRecordKey,
    unsupported_family_lookup: &BTreeMap<String, String>,
    cacheable_selector_lookup: &BTreeSet<String>,
) -> JsonValue {
    let mut entry = empty_object();
    insert_string_field(&mut entry, "record_key", record.record_key.clone());
    insert_string_field(&mut entry, "record_family", record.record_family.clone());
    insert_nullable_string_field(&mut entry, "selector_key", record.selector_key.clone());

    if let Some(unsupported_reason) = unsupported_family_lookup
        .get(&record.record_family)
        .cloned()
        .or_else(|| {
            phase_unsupported_record_family_reason(&record.record_family).map(str::to_owned)
        })
    {
        insert_string_field(&mut entry, "status", "unsupported".to_owned());
        insert_string_field(&mut entry, "unsupported_reason", unsupported_reason);
    } else if cacheable_selector_lookup.contains(&record.record_key) {
        insert_string_field(&mut entry, "status", "unsupported".to_owned());
        insert_string_field(
            &mut entry,
            "unsupported_reason",
            "value_not_retained_in_normalized_events".to_owned(),
        );
    } else {
        insert_string_field(&mut entry, "status", "not_found".to_owned());
    }

    entry
}
