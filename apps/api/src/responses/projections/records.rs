pub(super) fn build_record_inventory_section_for_name(
    name_row: &NameCurrentRow,
    row: Option<&RecordInventoryCurrentRow>,
    unsupported_reason: &str,
) -> JsonValue {
    if let Some(boundary) = fallback_no_declared_resolver_boundary(name_row) {
        return build_no_declared_resolver_inventory_state(boundary);
    }

    if let Some(row) = row {
        return build_record_inventory_state(row);
    }

    unsupported_section(unsupported_reason)
}

pub(super) fn build_record_cache_section_for_name(
    name_row: &NameCurrentRow,
    row: Option<&RecordInventoryCurrentRow>,
    records: &[ResolutionRecordKey],
    unsupported_reason: &str,
) -> JsonValue {
    if let Some(boundary) = fallback_no_declared_resolver_boundary(name_row) {
        return build_no_declared_resolver_cache_state(boundary, records);
    }

    if let Some(row) = row {
        return build_record_cache_state(row, records);
    }

    unsupported_section(unsupported_reason)
}

#[cfg(test)]
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
        canonical_record_inventory_items(&row.selectors),
    );
    insert_value_field(
        &mut record_inventory,
        "explicit_gaps",
        canonical_record_inventory_items(&row.explicit_gaps),
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

fn canonical_record_inventory_items(value: &JsonValue) -> JsonValue {
    let mut items = BTreeMap::new();
    for item in value.as_array().into_iter().flatten() {
        let Some((record_key, item)) = canonical_record_cache_item(item) else {
            continue;
        };
        items.entry(record_key).or_insert(item);
    }
    JsonValue::Array(
        items
            .into_values()
            .collect(),
    )
}

fn build_no_declared_resolver_inventory_state(record_version_boundary: JsonValue) -> JsonValue {
    let mut record_inventory = empty_object();
    insert_value_field(
        &mut record_inventory,
        "record_version_boundary",
        record_version_boundary,
    );
    insert_value_field(
        &mut record_inventory,
        "enumeration_basis",
        json!({
            "observed_selectors": false,
            "capability_declared_families": true,
            "globally_enumerable": false,
        }),
    );
    insert_value_field(&mut record_inventory, "selectors", JsonValue::Array(Vec::new()));
    insert_value_field(
        &mut record_inventory,
        "explicit_gaps",
        JsonValue::Array(Vec::new()),
    );
    insert_value_field(
        &mut record_inventory,
        "unsupported_families",
        JsonValue::Array(Vec::new()),
    );
    insert_value_field(&mut record_inventory, "last_change", JsonValue::Null);
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

fn build_no_declared_resolver_cache_state(
    record_version_boundary: JsonValue,
    records: &[ResolutionRecordKey],
) -> JsonValue {
    let mut record_cache = empty_object();
    insert_value_field(
        &mut record_cache,
        "record_version_boundary",
        record_version_boundary,
    );
    insert_value_field(
        &mut record_cache,
        "entries",
        JsonValue::Array(
            records
                .iter()
                .map(build_no_declared_resolver_cache_entry)
                .collect(),
        ),
    );
    record_cache
}

fn build_no_declared_resolver_cache_entry(record: &ResolutionRecordKey) -> JsonValue {
    let mut entry = empty_object();
    insert_string_field(&mut entry, "record_key", record.record_key.clone());
    insert_string_field(&mut entry, "record_family", record.record_family.clone());
    insert_nullable_string_field(&mut entry, "selector_key", record.selector_key.clone());
    insert_string_field(&mut entry, "status", "not_found".to_owned());
    entry
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
        .filter_map(canonical_record_cache_item)
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
        .filter_map(|selector| {
            let record_key = string_field(provenance_field(selector, "record_key"))?;
            parse_resolution_record_key(&record_key).map(|record| record.record_key)
        })
        .collect::<BTreeSet<_>>();

    if records.is_empty() {
        let mut entries = BTreeMap::new();
        for selector in row.selectors.as_array().into_iter().flatten().filter(|selector| {
            provenance_field(selector, "cacheable").and_then(JsonValue::as_bool) == Some(true)
        }) {
            let Some(record_key) = string_field(provenance_field(selector, "record_key")) else {
                continue;
            };
            let Some(record) = parse_resolution_record_key(&record_key) else {
                continue;
            };
            let record_key = record.record_key.clone();
            entries.entry(record_key).or_insert_with(|| {
                entry_lookup
                    .get(&record.record_key)
                    .cloned()
                    .unwrap_or_else(|| {
                        build_missing_record_cache_entry(
                            &record,
                            &unsupported_family_lookup,
                            &cacheable_selector_lookup,
                        )
                    })
            });
        }
        return JsonValue::Array(entries.into_values().collect());
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

fn canonical_record_cache_item(entry: &JsonValue) -> Option<(String, JsonValue)> {
    let record_key = string_field(provenance_field(entry, "record_key"))?;
    let record = parse_resolution_record_key(&record_key)?;
    let mut entry = entry.clone();
    if let Some(object) = entry.as_object_mut() {
        object.insert(
            "record_key".to_owned(),
            JsonValue::String(record.record_key.clone()),
        );
        object.insert(
            "record_family".to_owned(),
            JsonValue::String(record.record_family.clone()),
        );
        object.insert(
            "selector_key".to_owned(),
            record.selector_key.clone().map_or(JsonValue::Null, JsonValue::String),
        );
    }
    Some((record.record_key, entry))
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

fn fallback_no_declared_resolver_boundary(name_row: &NameCurrentRow) -> Option<JsonValue> {
    name_has_terminal_no_declared_resolver(name_row)
        .then(|| bigname_storage::resolution_record_version_boundary(name_row, None))
        .flatten()
}

fn name_has_terminal_no_declared_resolver(name_row: &NameCurrentRow) -> bool {
    let Some(resolver_summary) = provenance_field(&name_row.declared_summary, "resolver")
        .filter(|value| value.is_object())
    else {
        return false;
    };
    if string_field(provenance_field(resolver_summary, "status"))
        .is_some_and(|status| status == "unsupported")
    {
        return false;
    }

    string_field(provenance_field(resolver_summary, "chain_id")).is_none()
        && string_field(provenance_field(resolver_summary, "address")).is_none()
}
