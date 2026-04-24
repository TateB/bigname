use super::{
    execution_summary::build_resolution_execution_summary,
    readback::{
        persisted_verified_queries_by_record_key, reordered_persisted_verified_queries,
        supported_resolution_verified_readback_records,
    },
    topology::{build_resolution_topology, classify_supported_resolution_topology},
};

impl bigname_storage::VerifiedResolutionRecord for ResolutionRecordKey {
    fn record_key(&self) -> &str {
        &self.record_key
    }

    fn record_family(&self) -> &str {
        &self.record_family
    }

    fn selector_key(&self) -> Option<&str> {
        self.selector_key.as_deref()
    }
}

pub(super) fn build_resolution_declared_state(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    records: &[ResolutionRecordKey],
) -> JsonValue {
    let mut declared_state = empty_object();
    let topology = build_resolution_topology(row, record_inventory_row);
    let mut record_cache = build_record_cache_section(
        record_inventory_row,
        records,
        "declared resolution record cache is not yet projected",
    );
    if classify_supported_resolution_topology(&row.namespace, &row.logical_name_id, &topology)
        == Some(bigname_storage::VerifiedResolutionPathClass::BasenamesTransportDirect)
    {
        mark_basenames_transport_direct_unretained_record_cache_values(&mut record_cache);
    }
    insert_value_field(&mut declared_state, "topology", topology);
    insert_value_field(
        &mut declared_state,
        "record_inventory",
        build_record_inventory_section(
            record_inventory_row,
            "declared resolution record inventory is not yet projected",
        ),
    );
    insert_value_field(&mut declared_state, "record_cache", record_cache);
    declared_state
}

fn mark_basenames_transport_direct_unretained_record_cache_values(record_cache: &mut JsonValue) {
    let Some(entries) = record_cache
        .as_object_mut()
        .and_then(|object| object.get_mut("entries"))
        .and_then(JsonValue::as_array_mut)
    else {
        return;
    };

    for entry in entries {
        let is_missing_numeric_addr = string_field(provenance_field(entry, "status"))
            .is_some_and(|status| status == "not_found")
            && string_field(provenance_field(entry, "record_family"))
                .is_some_and(|family| family == "addr")
            && string_field(provenance_field(entry, "selector_key"))
                .is_some_and(|selector| selector.as_bytes().iter().all(u8::is_ascii_digit));
        if !is_missing_numeric_addr {
            continue;
        }

        insert_string_field(entry, "status", "unsupported".to_owned());
        insert_string_field(
            entry,
            "unsupported_reason",
            "value_not_retained_in_normalized_events".to_owned(),
        );
    }
}

pub(super) fn build_resolution_verified_state(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    persisted_outcome: Option<&ExecutionOutcome>,
) -> Result<JsonValue> {
    let mut verified_state = empty_object();
    let persisted_queries_by_record_key = persisted_outcome
        .map(|outcome| -> Result<BTreeMap<String, JsonValue>> {
            let supported_records = supported_resolution_verified_readback_records(row, records);
            let persisted_queries = persisted_verified_queries_by_record_key(outcome)?;
            Ok(supported_records
                .into_iter()
                .filter_map(|record| {
                    persisted_queries
                        .get(&record.record_key)
                        .cloned()
                        .map(|query| (record.record_key, query))
                })
                .collect::<BTreeMap<_, _>>())
        })
        .transpose()?
        .unwrap_or_default();
    insert_value_field(
        &mut verified_state,
        "verified_queries",
        JsonValue::Array(
            records
                .iter()
                .map(|record| {
                    persisted_queries_by_record_key
                        .get(&record.record_key)
                        .cloned()
                        .unwrap_or_else(|| build_resolution_verified_query(record))
                })
                .collect(),
        ),
    );
    Ok(verified_state)
}

pub(super) fn build_resolution_execution_explain_verified_state(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
) -> Result<JsonValue> {
    let mut verified_state = empty_object();
    insert_value_field(
        &mut verified_state,
        "execution",
        build_resolution_execution_summary(row, trace, outcome)?,
    );
    insert_value_field(
        &mut verified_state,
        "verified_queries",
        reordered_persisted_verified_queries(outcome, records)?,
    );
    Ok(verified_state)
}

fn build_resolution_verified_query(record: &ResolutionRecordKey) -> JsonValue {
    let mut query = empty_object();
    insert_string_field(&mut query, "record_key", record.record_key.clone());
    insert_string_field(&mut query, "status", "unsupported".to_owned());
    insert_string_field(
        &mut query,
        "unsupported_reason",
        "verified resolution entrypoint is not yet supported".to_owned(),
    );
    query
}
