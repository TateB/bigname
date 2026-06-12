pub(crate) type CompactEventsResponse = JsonValue;

fn build_history_route_response(
    summary: Option<&HistorySummary>,
    page_rows: &[HistoryEvent],
    scope: HistoryScope,
    page: HistoryPageResponse,
    view: ResponseView,
    meta: MetaMode,
) -> JsonValue {
    match view {
        ResponseView::Full => {
            serde_json::to_value(build_history_response(
                summary.expect("full history response requires a full history summary"),
                page_rows,
                scope,
                page,
            ))
                .expect("history response must serialize")
        }
        ResponseView::Compact => build_compact_events_response(summary, page_rows, page, meta, scope),
    }
}

fn build_compact_events_response(
    summary: Option<&HistorySummary>,
    page_rows: &[HistoryEvent],
    page: HistoryPageResponse,
    meta: MetaMode,
    scope: HistoryScope,
) -> JsonValue {
    let mut value = empty_object();
    insert_value_field(
        &mut value,
        "data",
        JsonValue::Array(page_rows.iter().map(build_compact_history_event).collect()),
    );
    insert_value_field(
        &mut value,
        "page",
        serde_json::to_value(page).expect("history page response must serialize"),
    );
    if meta != MetaMode::None {
        insert_value_field(
            &mut value,
            "meta",
            build_compact_events_meta(
                summary.expect("compact history metadata requires a history summary"),
                meta,
                scope,
            ),
        );
    }
    value
}

fn build_compact_history_event(row: &HistoryEvent) -> JsonValue {
    let mut value = empty_object();
    insert_string_field(&mut value, "type", compact_event_type(&row.event_kind));
    insert_optional_string_field(&mut value, "name", compact_event_name(row));
    insert_string_field(&mut value, "namespace", row.namespace.clone());
    insert_optional_string_field(
        &mut value,
        "resource_id",
        row.resource_id.map(|resource_id| resource_id.to_string()),
    );
    insert_value_field(
        &mut value,
        "block_number",
        row.block_number
            .map(|block_number| JsonValue::Number(block_number.into()))
            .unwrap_or(JsonValue::Null),
    );
    insert_value_field(
        &mut value,
        "timestamp",
        row.block_timestamp
            .map(format_timestamp)
            .map(JsonValue::String)
            .unwrap_or(JsonValue::Null),
    );
    insert_nullable_string_field(&mut value, "transaction_hash", row.transaction_hash.clone());
    insert_value_field(
        &mut value,
        "log_index",
        row.log_index
            .map(|log_index| JsonValue::Number(log_index.into()))
            .unwrap_or(JsonValue::Null),
    );
    insert_value_field(&mut value, "data", compact_event_data(row));
    value
}

fn build_compact_events_meta(
    summary: &HistorySummary,
    meta: MetaMode,
    scope: HistoryScope,
) -> JsonValue {
    let mut value = compact_meta_object(
        "supported",
        Some(summary.total_count),
        Vec::<String>::new(),
        Vec::<String>::new(),
    );

    if meta == MetaMode::Full {
        insert_value_field(&mut value, "provenance", build_history_provenance(summary));
        insert_value_field(
            &mut value,
            "coverage",
            serde_json::to_value(build_history_coverage(scope))
                .expect("history coverage must serialize"),
        );
        insert_value_field(
            &mut value,
            "chain_positions",
            build_history_chain_positions(summary),
        );
        insert_string_field(&mut value, "consistency", "head".to_owned());
        insert_string_field(&mut value, "last_updated", history_last_updated(summary));
    }

    value
}

fn compact_event_name(row: &HistoryEvent) -> Option<String> {
    row.logical_name_id
        .as_deref()
        .and_then(|logical_name_id| logical_name_id.split_once(':').map(|(_, name)| name))
        .map(str::to_owned)
}

fn compact_event_type(event_kind: &str) -> String {
    match event_kind {
        "RegistrationGranted" | "LabelRegistered" => "registration".to_owned(),
        "TokenControlTransferred" => "transfer".to_owned(),
        "AuthorityTransferred" => "authority".to_owned(),
        "ResolverChanged" => "resolver".to_owned(),
        "RecordChanged" | "RecordVersionChanged" => "record".to_owned(),
        "ReverseChanged" => "primary_name".to_owned(),
        "PermissionChanged" | "PermissionScopeChanged" | "RolesChanged" | "EACRolesChanged" => {
            "permission".to_owned()
        }
        other => other.to_owned(),
    }
}

fn compact_event_data(row: &HistoryEvent) -> JsonValue {
    let before = compact_state_payload(&row.before_state);
    let after = compact_state_payload(&row.after_state);
    let before_empty = json_object_is_empty(&before);
    let after_empty = json_object_is_empty(&after);

    match (before_empty, after_empty) {
        (true, true) => empty_object(),
        (true, false) => after,
        (false, true) => {
            let mut value = empty_object();
            insert_value_field(&mut value, "before", before);
            value
        }
        (false, false) => {
            let mut value = empty_object();
            insert_value_field(&mut value, "before", before);
            insert_value_field(&mut value, "after", after);
            value
        }
    }
}

fn compact_state_payload(value: &JsonValue) -> JsonValue {
    let Some(object) = value.as_object() else {
        return value.clone();
    };

    let mut compact = object.clone();
    compact.remove("provenance");
    compact.remove("coverage");
    JsonValue::Object(compact)
}

fn json_object_is_empty(value: &JsonValue) -> bool {
    value.as_object().is_some_and(serde_json::Map::is_empty)
}
