fn compact_value_source(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    request: &CompactNameRecordsRequest,
    value_source: CompactNameRecordsValueSource,
    verified_outcome: Option<&ExecutionOutcome>,
) -> JsonValue {
    let declared_status = if request.mode.includes_declared() {
        if record_inventory_row.is_some() || compact_name_has_terminal_no_declared_resolver(row) {
            "supported"
        } else {
            "unsupported"
        }
    } else {
        "not_requested"
    };
    let verified_requested = value_source == CompactNameRecordsValueSource::Verified
        || matches!(
            request.mode,
            CompactNameRecordsMode::Verified | CompactNameRecordsMode::Both
        );
    let verified_status = if verified_requested {
        compact_verified_status(verified_outcome)
    } else {
        "not_requested"
    };
    let mut value_source = json!({
        "mode": compact_resolution_mode_label(request.mode),
        "declared_status": declared_status,
        "source": compact_value_source_label(value_source),
    });

    if declared_status == "unsupported" {
        insert_string_field(
            &mut value_source,
            "declared_unsupported_reason",
            COMPACT_RECORDS_DECLARED_CACHE_UNSUPPORTED_REASON.to_owned(),
        );
    }
    if verified_requested {
        insert_string_field(&mut value_source, "verified_status", verified_status.to_owned());
        if verified_status != "supported" {
            insert_string_field(
                &mut value_source,
                "verified_unsupported_reason",
                COMPACT_RECORDS_VERIFIED_UNSUPPORTED_REASON.to_owned(),
            );
        }
    }

    value_source
}

fn compact_value_source_label(value_source: CompactNameRecordsValueSource) -> &'static str {
    match value_source {
        CompactNameRecordsValueSource::Declared => "record_inventory_current",
        CompactNameRecordsValueSource::Verified => "verified_resolution",
    }
}

fn compact_verified_status(verified_outcome: Option<&ExecutionOutcome>) -> &'static str {
    if verified_outcome.is_some() {
        "supported"
    } else {
        "unsupported"
    }
}

fn compact_verified_records_summary(
    requested_records: &[ResolutionRecordKey],
    verified_outcome: Option<&ExecutionOutcome>,
) -> JsonValue {
    let entries = compact_verified_record_cache_entries(requested_records, verified_outcome)
        .into_values()
        .collect::<Vec<_>>();
    json!({
        "status": compact_verified_status(verified_outcome),
        "entries": entries,
    })
}

fn compact_name_records_meta(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    request: &CompactNameRecordsRequest,
    value_source: CompactNameRecordsValueSource,
    verified_outcome: Option<&ExecutionOutcome>,
) -> JsonValue {
    let unsupported_fields = compact_name_records_unsupported_fields(
        row,
        record_inventory_row,
        request,
        value_source,
        verified_outcome,
    );
    let support_status = if unsupported_fields.is_empty() {
        "supported"
    } else if record_inventory_row.is_none()
        && !compact_name_has_terminal_no_declared_resolver(row)
        && !request.mode.includes_declared()
    {
        "unsupported"
    } else {
        "partial"
    };

    let mut meta = compact_meta_object(support_status, None, unsupported_fields, Vec::new());
    insert_value_field(
        &mut meta,
        "value_source",
        compact_value_source(row, record_inventory_row, request, value_source, verified_outcome),
    );
    if request.meta == MetaMode::Full {
        insert_value_field(
            &mut meta,
            "inventory_source",
            compact_inventory_source(
                row,
                record_inventory_row,
                &compact_record_inventory_lookup(record_inventory_row),
            ),
        );
        insert_value_field(&mut meta, "coverage", build_name_coverage(&row.coverage));
        insert_value_field(&mut meta, "chain_positions", ensure_object(&row.chain_positions));
        insert_string_field(
            &mut meta,
            "consistency",
            canonicality_consistency(&row.canonicality_summary).to_owned(),
        );
        insert_string_field(
            &mut meta,
            "last_updated",
            format_timestamp(row.last_recomputed_at),
        );
        insert_value_field(&mut meta, "provenance", build_name_provenance(&row.provenance));
    }

    meta
}

fn compact_name_records_unsupported_fields(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    _request: &CompactNameRecordsRequest,
    value_source: CompactNameRecordsValueSource,
    verified_outcome: Option<&ExecutionOutcome>,
) -> Vec<String> {
    let mut fields = BTreeSet::new();
    if record_inventory_row.is_none() && !compact_name_has_terminal_no_declared_resolver(row) {
        fields.insert("record_inventory");
        fields.insert("record_cache");
    }
    if value_source == CompactNameRecordsValueSource::Verified && verified_outcome.is_none() {
        fields.insert("verified_records");
    }

    fields.into_iter().map(str::to_owned).collect()
}

fn compact_resolution_mode_label(mode: CompactNameRecordsMode) -> &'static str {
    mode.label()
}

fn compact_declared_records_are_authoritative(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> bool {
    if compact_name_has_terminal_no_declared_resolver(row) {
        return true;
    }
    let Some(record_inventory_row) = record_inventory_row else {
        return false;
    };
    string_field(provenance_field(&record_inventory_row.coverage, "unsupported_reason")).is_none()
        && string_field(provenance_field(&record_inventory_row.coverage, "status"))
            .is_some_and(|status| status == "full")
}

fn compact_declared_records_satisfy_request(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    requested_records: &[ResolutionRecordKey],
) -> bool {
    if compact_name_has_terminal_no_declared_resolver(row) {
        return true;
    }
    if !compact_declared_records_are_authoritative(row, record_inventory_row) {
        return false;
    }
    if requested_records.is_empty() {
        return true;
    }

    let Some(record_inventory_row) = record_inventory_row else {
        return false;
    };
    let value_entries = compact_record_items_by_key(&record_inventory_row.entries);
    requested_records.iter().all(|record| {
        compact_declared_record_entry_satisfies(&record.record_key, &value_entries)
            || record.record_key == "avatar"
                && compact_declared_record_entry_satisfies("text:avatar", &value_entries)
    })
}

fn compact_declared_record_entry_satisfies(
    record_key: &str,
    value_entries: &BTreeMap<String, JsonValue>,
) -> bool {
    value_entries
        .get(record_key)
        .and_then(|entry| provenance_field(entry, "status"))
        .and_then(JsonValue::as_str)
        .is_some_and(|status| matches!(status, "success" | "not_found"))
}

fn compact_name_has_terminal_no_declared_resolver(row: &NameCurrentRow) -> bool {
    let Some(resolver_summary) = provenance_field(&row.declared_summary, "resolver")
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
