fn compact_record_inventory_lookup(
    row: Option<&RecordInventoryCurrentRow>,
) -> CompactRecordInventoryLookup {
    let Some(row) = row else {
        return CompactRecordInventoryLookup::default();
    };

    CompactRecordInventoryLookup {
        selectors: compact_record_items_by_key(&row.selectors),
        explicit_gaps: compact_record_items_by_key(&row.explicit_gaps),
        unsupported_families: row
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
            .collect(),
    }
}

fn compact_record_items_by_key(value: &JsonValue) -> BTreeMap<String, JsonValue> {
    value
        .as_array()
        .map_or_else(BTreeMap::new, |items| {
            compact_record_items_by_key_slice(items)
        })
}

fn compact_record_items_by_key_slice(items: &[JsonValue]) -> BTreeMap<String, JsonValue> {
    items
        .iter()
        .filter_map(|item| {
            let record_key = string_field(provenance_field(item, "record_key"))?;
            let record = parse_resolution_record_key(&record_key)?;
            Some((record.record_key, item.clone()))
        })
        .collect()
}

fn compact_record_payload(
    record: &ResolutionRecordKey,
    value_entry: Option<&JsonValue>,
    inventory_lookup: &CompactRecordInventoryLookup,
) -> JsonValue {
    let payload = value_entry
        .cloned()
        .unwrap_or_else(|| compact_record_payload_from_inventory(record, inventory_lookup));
    compact_record_public_payload(payload)
}

fn compact_record_payload_from_inventory(
    record: &ResolutionRecordKey,
    inventory_lookup: &CompactRecordInventoryLookup,
) -> JsonValue {
    let inventory = compact_inventory_status(record, inventory_lookup);
    if string_field(provenance_field(&inventory, "status")).as_deref()
        == Some("unsupported_family")
    {
        let mut entry = empty_object();
        insert_string_field(&mut entry, "status", "unsupported".to_owned());
        if let Some(reason) = string_field(provenance_field(&inventory, "unsupported_reason")) {
            insert_string_field(&mut entry, "unsupported_reason", reason);
        }
        return entry;
    }
    if string_field(provenance_field(&inventory, "status")).as_deref() == Some("explicit_gap") {
        let mut entry = empty_object();
        insert_string_field(&mut entry, "status", "not_found".to_owned());
        if let Some(reason) = string_field(provenance_field(&inventory, "gap_reason")) {
            insert_string_field(&mut entry, "failure_reason", reason);
        }
        return entry;
    }

    compact_synthetic_record_entry(record, "not_found", None)
}

fn compact_record_public_payload(payload: JsonValue) -> JsonValue {
    let mut public = empty_object();
    let status = string_field(provenance_field(&payload, "status"))
        .unwrap_or_else(|| "unsupported".to_owned());
    insert_string_field(&mut public, "status", status.clone());
    if status == "success"
        && let Some(value) = provenance_field(&payload, "value").cloned()
    {
        insert_value_field(&mut public, "value", compact_record_value(value));
    }
    if let Some(reason) = string_field(provenance_field(&payload, "failure_reason")) {
        insert_string_field(&mut public, "failure_reason", reason);
    }
    if let Some(reason) = string_field(provenance_field(&payload, "unsupported_reason")) {
        insert_string_field(&mut public, "unsupported_reason", reason);
    }
    public
}

fn compact_synthetic_record_entry(
    record: &ResolutionRecordKey,
    status: &str,
    unsupported_reason: Option<&str>,
) -> JsonValue {
    let mut entry = empty_object();
    insert_string_field(&mut entry, "record_key", record.record_key.clone());
    insert_string_field(&mut entry, "record_family", record.record_family.clone());
    insert_nullable_string_field(&mut entry, "selector_key", record.selector_key.clone());
    insert_string_field(&mut entry, "status", status.to_owned());
    if let Some(unsupported_reason) = unsupported_reason {
        insert_string_field(
            &mut entry,
            "unsupported_reason",
            unsupported_reason.to_owned(),
        );
    }
    entry
}

fn compact_record_value(value: JsonValue) -> JsonValue {
    provenance_field(&value, "value").cloned().unwrap_or(value)
}

fn compact_inventory_status(
    record: &ResolutionRecordKey,
    inventory_lookup: &CompactRecordInventoryLookup,
) -> JsonValue {
    if let Some(selector) = inventory_lookup.selectors.get(&record.record_key) {
        return json!({
            "status": "known",
            "cacheable": provenance_field(selector, "cacheable")
                .and_then(JsonValue::as_bool)
                .unwrap_or(false),
        });
    }
    if let Some(gap) = inventory_lookup.explicit_gaps.get(&record.record_key) {
        return json!({
            "status": "explicit_gap",
            "gap_reason": string_field(provenance_field(gap, "gap_reason")),
        });
    }
    if let Some(reason) = inventory_lookup
        .unsupported_families
        .get(&record.record_family)
    {
        return json!({
            "status": "unsupported_family",
            "unsupported_reason": reason,
        });
    }

    json!({ "status": "unknown" })
}

fn compact_inventory_source(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    inventory_lookup: &CompactRecordInventoryLookup,
) -> JsonValue {
    let Some(record_inventory_row) = record_inventory_row else {
        if compact_name_has_terminal_no_declared_resolver(row) {
            return json!({
                "status": "supported",
                "known_selector_count": 0,
                "explicit_gaps": [],
                "unsupported_families": [],
            });
        }
        return unsupported_section(COMPACT_RECORDS_DECLARED_INVENTORY_UNSUPPORTED_REASON);
    };

    json!({
        "status": "supported",
        "coverage_status": string_field(provenance_field(&record_inventory_row.coverage, "status")),
        "known_selector_count": inventory_lookup.selectors.len(),
        "explicit_gaps": record_inventory_row.explicit_gaps,
        "unsupported_families": record_inventory_row.unsupported_families,
    })
}
