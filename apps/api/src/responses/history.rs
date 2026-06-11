fn build_history_item(row: &HistoryEvent) -> JsonValue {
    let mut value = empty_object();
    insert_string_field(
        &mut value,
        "normalized_event_id",
        row.normalized_event_id.to_string(),
    );
    insert_string_field(&mut value, "event_identity", row.event_identity.clone());
    insert_string_field(&mut value, "namespace", row.namespace.clone());
    insert_optional_string_field(&mut value, "logical_name_id", row.logical_name_id.clone());
    insert_optional_string_field(
        &mut value,
        "resource_id",
        row.resource_id.map(|resource_id| resource_id.to_string()),
    );
    insert_string_field(&mut value, "event_kind", row.event_kind.clone());
    insert_string_field(&mut value, "source_family", row.source_family.clone());
    insert_value_field(
        &mut value,
        "manifest_version",
        JsonValue::Number(row.manifest_version.into()),
    );
    insert_value_field(
        &mut value,
        "source_manifest_id",
        row.source_manifest_id
            .map(|source_manifest_id| JsonValue::Number(source_manifest_id.into()))
            .unwrap_or(JsonValue::Null),
    );
    insert_value_field(
        &mut value,
        "chain_position",
        build_history_chain_position(row),
    );
    insert_nullable_string_field(&mut value, "transaction_hash", row.transaction_hash.clone());
    insert_value_field(
        &mut value,
        "log_index",
        row.log_index
            .map(|log_index| JsonValue::Number(log_index.into()))
            .unwrap_or(JsonValue::Null),
    );
    insert_value_field(&mut value, "raw_fact_ref", row.raw_fact_ref.clone());
    insert_string_field(&mut value, "derivation_kind", row.derivation_kind.clone());
    insert_string_field(
        &mut value,
        "canonicality_state",
        row.canonicality_state.as_str().to_owned(),
    );
    insert_value_field(&mut value, "before_state", row.before_state.clone());
    insert_value_field(&mut value, "after_state", row.after_state.clone());
    insert_value_field(&mut value, "provenance", ensure_object(&row.provenance));
    insert_value_field(&mut value, "coverage", build_name_coverage(&row.coverage));
    value
}

fn build_history_provenance(rows: &[HistoryEvent]) -> JsonValue {
    let mut value = empty_object();
    insert_value_field(
        &mut value,
        "normalized_event_ids",
        JsonValue::Array(
            rows.iter()
                .map(|row| JsonValue::String(row.normalized_event_id.to_string()))
                .collect(),
        ),
    );
    insert_value_field(
        &mut value,
        "raw_fact_refs",
        dedupe_json_values(rows.iter().map(|row| row.raw_fact_ref.clone())),
    );
    insert_value_field(
        &mut value,
        "manifest_versions",
        dedupe_json_values(rows.iter().map(history_manifest_version)),
    );
    // History provenance is a route-level summary; if execution-backed rows are
    // later admitted, the first non-null trace id is the representative id.
    if let Some(execution_trace_id) = rows
        .iter()
        .filter_map(|row| string_field(provenance_field(&row.provenance, "execution_trace_id")))
        .next()
    {
        insert_string_field(&mut value, "execution_trace_id", execution_trace_id);
    }
    insert_string_field(
        &mut value,
        "derivation_kind",
        "normalized_event_history".to_owned(),
    );
    value
}

fn build_history_coverage(scope: HistoryScope) -> CoverageResponse {
    CoverageResponse {
        status: "full".to_owned(),
        exhaustiveness: "authoritative".to_owned(),
        source_classes_considered: vec!["normalized_events".to_owned()],
        enumeration_basis: format!(
            "canonical normalized-event history for the requested {} scope",
            scope.as_str()
        ),
        unsupported_reason: None,
    }
}

fn build_history_chain_positions(rows: &[HistoryEvent]) -> JsonValue {
    let mut chain_positions = BTreeMap::<String, ChainPositionResponse>::new();
    for row in rows {
        let (Some(chain_id), Some(block_number), Some(block_hash), Some(timestamp)) = (
            row.chain_id.as_ref(),
            row.block_number,
            row.block_hash.as_ref(),
            row.block_timestamp,
        ) else {
            continue;
        };

        let key = chain_position_key(chain_id);
        let candidate = ChainPositionResponse {
            chain_id: chain_id.clone(),
            block_number,
            block_hash: block_hash.clone(),
            timestamp: format_timestamp(timestamp),
        };
        merge_chain_position(&mut chain_positions, key, candidate);
    }

    serde_json::to_value(chain_positions).expect("history chain positions must serialize")
}

fn chain_position_from_value(value: &JsonValue) -> Option<ChainPositionResponse> {
    Some(ChainPositionResponse {
        chain_id: string_field(provenance_field(value, "chain_id"))?,
        block_number: provenance_field(value, "block_number")?.as_i64()?,
        block_hash: string_field(provenance_field(value, "block_hash"))?,
        timestamp: string_field(provenance_field(value, "timestamp"))?,
    })
}

fn merge_chain_position(
    chain_positions: &mut BTreeMap<String, ChainPositionResponse>,
    key: String,
    candidate: ChainPositionResponse,
) {
    match chain_positions.get(&key) {
        Some(existing)
            if existing.block_number > candidate.block_number
                || (existing.block_number == candidate.block_number
                    && existing.block_hash >= candidate.block_hash) => {}
        _ => {
            chain_positions.insert(key, candidate);
        }
    }
}

fn build_history_chain_position(row: &HistoryEvent) -> JsonValue {
    match (
        row.chain_id.as_ref(),
        row.block_number,
        row.block_hash.as_ref(),
        row.block_timestamp,
    ) {
        (Some(chain_id), Some(block_number), Some(block_hash), Some(timestamp)) => json!({
            "chain_id": chain_id,
            "block_number": block_number,
            "block_hash": block_hash,
            "timestamp": format_timestamp(timestamp),
        }),
        _ => JsonValue::Null,
    }
}
