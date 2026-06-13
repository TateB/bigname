fn compact_requested_records(
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    request: &CompactNameRecordsRequest,
) -> Vec<ResolutionRecordKey> {
    let mut records = Vec::new();
    let mut seen = BTreeSet::new();
    for text_key in compact_requested_text_keys(record_inventory_row, request) {
        compact_push_record_key(&mut records, &mut seen, &format!("text:{text_key}"));
    }
    if request.avatar {
        compact_push_record_key(&mut records, &mut seen, "avatar");
    }
    if request.content_hash {
        compact_push_record_key(&mut records, &mut seen, "contenthash");
    }
    for coin_type in compact_requested_coin_types(record_inventory_row, request) {
        compact_push_record_key(&mut records, &mut seen, &format!("addr:{coin_type}"));
    }
    records
}

const COMPACT_BASIC_TEXT_KEYS: &[&str] = &["description", "url", "email"];

const COMPACT_BASIC_COIN_TYPES: &[&str] = &["60"];

fn compact_requested_text_keys(
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    request: &CompactNameRecordsRequest,
) -> Vec<String> {
    let mut keys = request.texts.clone();
    let mut seen = keys.iter().cloned().collect::<BTreeSet<_>>();
    if compact_should_include_known_or_basic_texts(request) {
        for key in compact_known_text_keys_from_inventory(record_inventory_row) {
            if seen.insert(key.clone()) {
                keys.push(key);
            }
        }
        if compact_should_probe_basic_selector_fallbacks(record_inventory_row, request) {
            for key in COMPACT_BASIC_TEXT_KEYS {
                if seen.insert((*key).to_owned()) {
                    keys.push((*key).to_owned());
                }
            }
        }
    }
    keys
}

fn compact_should_include_known_or_basic_texts(request: &CompactNameRecordsRequest) -> bool {
    request.default_profile_include
        && request.include.known_text_keys
        && matches!(
            request.mode,
            CompactNameRecordsMode::Auto
                | CompactNameRecordsMode::Verified
                | CompactNameRecordsMode::Both
        )
}

fn compact_should_probe_basic_selector_fallbacks(
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    request: &CompactNameRecordsRequest,
) -> bool {
    matches!(
        request.mode,
        CompactNameRecordsMode::Auto | CompactNameRecordsMode::Verified | CompactNameRecordsMode::Both
    ) && !compact_has_public_declared_record_selectors(record_inventory_row)
}

fn compact_has_public_declared_record_selectors(
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> bool {
    record_inventory_row
        .and_then(|row| row.selectors.as_array())
        .is_some_and(|selectors| {
            selectors.iter().any(|selector| {
                string_field(provenance_field(selector, "record_key"))
                    .and_then(|record_key| parse_resolution_record_key(&record_key))
                    .is_some()
            })
        })
}

fn compact_push_record_key(
    records: &mut Vec<ResolutionRecordKey>,
    seen: &mut BTreeSet<String>,
    record_key: &str,
) {
    let record = parse_resolution_record_key(record_key)
        .expect("compact record request builder must produce valid record selectors");
    if !seen.insert(record.record_key.clone()) {
        return;
    }
    records.push(record);
}

fn compact_verified_record_cache_entries(
    records: &[ResolutionRecordKey],
    verified_outcome: Option<&ExecutionOutcome>,
) -> BTreeMap<String, JsonValue> {
    let verified_queries = verified_outcome
        .and_then(|outcome| outcome.outcome_payload.as_ref())
        .and_then(|payload| provenance_field(payload, "verified_queries"))
        .map(compact_record_items_by_key)
        .unwrap_or_default();

    records
        .iter()
        .map(|record| {
            let entry = verified_queries
                .get(&record.record_key)
                .map(|query| compact_verified_record_entry(record, query))
                .unwrap_or_else(|| {
                    compact_synthetic_record_entry(
                        record,
                        "unsupported",
                        Some(COMPACT_RECORDS_VERIFIED_UNSUPPORTED_REASON),
                    )
                });
            (record.record_key.clone(), entry)
        })
        .collect()
}

fn compact_verified_record_entry(record: &ResolutionRecordKey, query: &JsonValue) -> JsonValue {
    let status = string_field(provenance_field(query, "status")).unwrap_or_else(|| {
        "unsupported".to_owned()
    });
    let mut entry = compact_synthetic_record_entry(
        record,
        verified_compact_status(&status),
        verified_compact_unsupported_reason(query, &status).as_deref(),
    );
    if status == "success"
        && let Some(value) = provenance_field(query, "value").cloned()
    {
        insert_value_field(&mut entry, "value", value);
    }
    if status == "not_found"
        && let Some(failure_reason) = string_field(provenance_field(query, "failure_reason"))
    {
        insert_string_field(&mut entry, "failure_reason", failure_reason);
    }
    entry
}

fn verified_compact_status(status: &str) -> &str {
    match status {
        "success" | "not_found" => status,
        _ => "unsupported",
    }
}

fn verified_compact_unsupported_reason(query: &JsonValue, status: &str) -> Option<String> {
    match status {
        "success" | "not_found" => None,
        "unsupported" => string_field(provenance_field(query, "unsupported_reason"))
            .or_else(|| Some(COMPACT_RECORDS_VERIFIED_UNSUPPORTED_REASON.to_owned())),
        "execution_failed" => string_field(provenance_field(query, "failure_reason"))
            .or_else(|| Some("verified_execution_failed".to_owned())),
        _ => Some(COMPACT_RECORDS_VERIFIED_UNSUPPORTED_REASON.to_owned()),
    }
}

fn compact_declared_record_cache_entries(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    records: &[ResolutionRecordKey],
) -> BTreeMap<String, JsonValue> {
    let record_cache = build_record_cache_section_for_name(
        row,
        record_inventory_row,
        records,
        COMPACT_RECORDS_DECLARED_CACHE_UNSUPPORTED_REASON,
    );
    let Some(entries) = provenance_field(&record_cache, "entries").and_then(JsonValue::as_array)
    else {
        return records
            .iter()
            .map(|record| {
                (
                    record.record_key.clone(),
                    compact_synthetic_record_entry(
                        record,
                        "unsupported",
                        Some(COMPACT_RECORDS_DECLARED_CACHE_UNSUPPORTED_REASON),
                    ),
                )
            })
            .collect();
    };

    let mut value_entries = compact_record_items_by_key_slice(entries);

    add_declared_compact_alias_entries(record_inventory_row, records, &mut value_entries);
    value_entries
}

fn add_declared_compact_alias_entries(
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    records: &[ResolutionRecordKey],
    value_entries: &mut BTreeMap<String, JsonValue>,
) {
    if !records.iter().any(|record| record.record_key == "avatar") {
        return;
    }
    if compact_value_entry_is_success(value_entries.get("avatar")) {
        return;
    }

    let Some(text_avatar_entry) = record_inventory_row
        .and_then(|row| row.entries.as_array())
        .into_iter()
        .flatten()
        .find(|entry| {
            string_field(provenance_field(entry, "record_key")).as_deref() == Some("text:avatar")
        })
    else {
        return;
    };
    value_entries.insert("avatar".to_owned(), text_avatar_entry.clone());
}

fn compact_resolver_address(row: &NameCurrentRow) -> JsonValue {
    provenance_field(&row.declared_summary, "resolver")
        .and_then(|resolver| provenance_field(resolver, "address"))
        .and_then(|value| match value {
            JsonValue::String(address) => Some(JsonValue::String(address.clone())),
            JsonValue::Null => Some(JsonValue::Null),
            _ => None,
        })
        .unwrap_or(JsonValue::Null)
}

fn compact_text_records(
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    request: &CompactNameRecordsRequest,
    value_entries: &BTreeMap<String, JsonValue>,
    inventory_lookup: &CompactRecordInventoryLookup,
) -> JsonValue {
    let mut text_records = JsonMap::new();
    for text_key in compact_requested_text_keys(record_inventory_row, request) {
        let record_key = format!("text:{text_key}");
        let record = parse_resolution_record_key(&record_key)
            .expect("compact text selector must be a valid record key");
        let value_entry = value_entries.get(&record_key);
        if compact_is_unrequested_basic_text_selector_fallback(record_inventory_row, request, &text_key)
            && !compact_value_entry_is_success(value_entry)
        {
            continue;
        }
        text_records.insert(
            text_key,
            compact_record_payload(&record, value_entry, inventory_lookup),
        );
    }
    JsonValue::Object(text_records)
}

fn compact_is_unrequested_basic_text_selector_fallback(
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    request: &CompactNameRecordsRequest,
    text_key: &str,
) -> bool {
    compact_should_probe_basic_selector_fallbacks(record_inventory_row, request)
        && COMPACT_BASIC_TEXT_KEYS.contains(&text_key)
        && !request.texts.iter().any(|requested| requested == text_key)
}

fn compact_value_entry_is_success(value_entry: Option<&JsonValue>) -> bool {
    value_entry
        .and_then(|entry| provenance_field(entry, "status"))
        .and_then(JsonValue::as_str)
        .is_some_and(|status| status == "success")
}

fn compact_known_text_keys(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    request: &CompactNameRecordsRequest,
    inventory_lookup: &CompactRecordInventoryLookup,
) -> JsonValue {
    if !request.known_text_keys {
        return JsonValue::Null;
    }
    if record_inventory_row.is_none() && !compact_name_has_terminal_no_declared_resolver(row) {
        if request.meta == MetaMode::None {
            return JsonValue::Null;
        }
        return unsupported_section(COMPACT_RECORDS_DECLARED_INVENTORY_UNSUPPORTED_REASON);
    }
    if let Some(reason) = inventory_lookup.unsupported_families.get("text") {
        if request.meta == MetaMode::None {
            return JsonValue::Null;
        }
        return unsupported_section(reason);
    }

    let keys = inventory_lookup
        .selectors
        .values()
        .filter(|selector| {
            string_field(provenance_field(selector, "record_family")).as_deref() == Some("text")
        })
        .filter_map(|selector| string_field(provenance_field(selector, "selector_key")))
        .map(JsonValue::String)
        .collect::<Vec<_>>();

    json!({
        "status": "supported",
        "keys": keys,
    })
}

fn compact_optional_record(
    requested: bool,
    record_key: &str,
    value_entries: &BTreeMap<String, JsonValue>,
    inventory_lookup: &CompactRecordInventoryLookup,
) -> JsonValue {
    if !requested {
        return JsonValue::Null;
    }
    let record = parse_resolution_record_key(record_key)
        .expect("compact optional selector must be a valid record key");
    compact_record_payload(
        &record,
        value_entries.get(record_key),
        inventory_lookup,
    )
}

fn compact_coin_addresses(
    request: &CompactNameRecordsRequest,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    value_entries: &BTreeMap<String, JsonValue>,
    inventory_lookup: &CompactRecordInventoryLookup,
) -> JsonValue {
    let mut coin_addresses = JsonMap::new();
    for coin_type in compact_requested_coin_types(record_inventory_row, request) {
        let record_key = format!("addr:{coin_type}");
        let record = parse_resolution_record_key(&record_key)
            .expect("compact coin selector must be a valid record key");
        let coin_type = record
            .selector_key
            .clone()
            .expect("compact addr selector must include coin type");
        if coin_addresses.contains_key(&coin_type) {
            continue;
        }
        coin_addresses.insert(
            coin_type,
            compact_record_payload(
                &record,
                value_entries.get(&record.record_key),
                inventory_lookup,
            ),
        );
    }
    JsonValue::Object(coin_addresses)
}

fn compact_requested_coin_types(
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    request: &CompactNameRecordsRequest,
) -> Vec<String> {
    if !request.coin_types.is_empty() {
        return request.coin_types.clone();
    }
    if !request.include.coins {
        return Vec::new();
    }

    let known_coin_types = compact_known_coin_types(record_inventory_row);
    if !known_coin_types.is_empty() {
        return known_coin_types;
    }
    if compact_should_probe_basic_selector_fallbacks(record_inventory_row, request) {
        return COMPACT_BASIC_COIN_TYPES
            .iter()
            .map(|coin_type| (*coin_type).to_owned())
            .collect();
    }

    Vec::new()
}

fn compact_known_coin_types(
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Vec<String> {
    compact_known_selector_keys(record_inventory_row, "addr")
}

fn compact_known_text_keys_from_inventory(
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Vec<String> {
    compact_known_selector_keys(record_inventory_row, "text")
}

fn compact_known_selector_keys(
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    record_family: &str,
) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut keys = Vec::new();
    for selector_key in record_inventory_row
        .and_then(|row| row.selectors.as_array())
        .into_iter()
        .flatten()
        .filter_map(|selector| {
            let record_key = string_field(provenance_field(selector, "record_key"))?;
            let record = parse_resolution_record_key(&record_key)?;
            (record.record_family == record_family).then_some(record.selector_key)?
        })
    {
        if seen.insert(selector_key.clone()) {
            keys.push(selector_key);
        }
    }
    keys
}
