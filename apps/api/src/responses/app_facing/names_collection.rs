pub(crate) type CompactNamesResponse = JsonValue;

pub(crate) fn build_compact_names_response(
    rows: &[bigname_storage::NameCurrentListRow],
    page: HistoryPageResponse,
    meta: Option<JsonValue>,
) -> CompactNamesResponse {
    let mut response = empty_object();
    insert_value_field(
        &mut response,
        "data",
        JsonValue::Array(rows.iter().map(build_compact_domain_summary).collect()),
    );
    insert_value_field(&mut response, "page", json!(page));
    if let Some(meta) = meta {
        insert_value_field(&mut response, "meta", meta);
    }
    response
}

fn build_compact_domain_summary(row: &bigname_storage::NameCurrentListRow) -> JsonValue {
    let mut value = empty_object();
    insert_string_field(&mut value, "namespace", row.row.namespace.clone());
    insert_string_field(&mut value, "name", row.row.canonical_display_name.clone());
    insert_string_field(
        &mut value,
        "normalized_name",
        row.row.normalized_name.clone(),
    );
    insert_string_field(&mut value, "namehash", row.row.namehash.clone());
    insert_string_if_present(&mut value, "labelhash", row.labelhash.clone());
    insert_string_if_present(&mut value, "token_id", row.token_id.clone());
    insert_string_if_present(&mut value, "owner", row.owner.clone());
    insert_string_if_present(&mut value, "registrant", row.registrant.clone());
    insert_timestamp_if_present(&mut value, "created_at", row.created_at);
    insert_timestamp_if_present(&mut value, "registration_date", row.registration_date);
    insert_timestamp_if_present(&mut value, "expiry_date", row.expiry_date);
    insert_string_if_present(&mut value, "resolver_address", row.resolver_address.clone());
    value
}

fn insert_string_if_present(object: &mut JsonValue, key: &str, value: Option<String>) {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        insert_string_field(object, key, value);
    }
}

fn insert_timestamp_if_present(object: &mut JsonValue, key: &str, value: Option<OffsetDateTime>) {
    if let Some(value) = value {
        insert_string_field(object, key, format_timestamp(value));
    }
}
