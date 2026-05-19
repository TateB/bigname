pub(crate) type CompactRolesResponse = JsonValue;
pub(crate) type ResourceLookupResponse = JsonValue;

pub(crate) fn build_compact_roles_response(
    rows: &[PermissionsCurrentRow],
    associated_names: &BTreeMap<Uuid, String>,
    summary: &bigname_storage::PermissionsCurrentFullFilterSummary,
    page: HistoryPageResponse,
    meta_mode: MetaMode,
) -> CompactRolesResponse {
    build_roles_response(
        rows,
        associated_names,
        summary.row_count.max(0) as u64,
        page,
        meta_mode,
    )
}

pub(crate) fn build_empty_compact_roles_response(
    page: HistoryPageResponse,
    meta_mode: MetaMode,
) -> CompactRolesResponse {
    build_roles_response(&[], &BTreeMap::new(), 0, page, meta_mode)
}

pub(crate) fn build_resource_lookup_response(
    row: &NameCurrentRow,
    resource_id: Uuid,
    meta_mode: MetaMode,
) -> ResourceLookupResponse {
    let mut data = empty_object();
    insert_string_field(&mut data, "namespace", row.namespace.clone());
    insert_string_field(&mut data, "name", row.canonical_display_name.clone());
    insert_string_field(&mut data, "normalized_name", row.normalized_name.clone());
    insert_string_field(&mut data, "resource_id", resource_id.to_string());
    insert_value_field(&mut data, "resource_hex", JsonValue::Null);

    let mut response = empty_object();
    insert_value_field(&mut response, "data", data);
    if meta_mode != MetaMode::None {
        insert_value_field(
            &mut response,
            "meta",
            compact_meta_object(
                "supported",
                None,
                ["resource_hex"].into_iter().map(str::to_owned),
                std::iter::empty(),
            ),
        );
    }
    response
}

fn build_roles_response(
    rows: &[PermissionsCurrentRow],
    associated_names: &BTreeMap<Uuid, String>,
    total_count: u64,
    page: HistoryPageResponse,
    meta_mode: MetaMode,
) -> CompactRolesResponse {
    let data = rows
        .iter()
        .map(|row| build_role_row(row, associated_names.get(&row.resource_id)))
        .collect::<Vec<_>>();

    let mut response = empty_object();
    insert_value_field(&mut response, "data", JsonValue::Array(data));
    insert_value_field(
        &mut response,
        "page",
        serde_json::to_value(page).expect("roles page response must serialize"),
    );
    if meta_mode != MetaMode::None {
        insert_value_field(
            &mut response,
            "meta",
            compact_meta_object(
                "supported",
                Some(total_count),
                ["resource_hex", "role_bitmap"].into_iter().map(str::to_owned),
                std::iter::empty(),
            ),
        );
    }
    response
}

fn build_role_row(row: &PermissionsCurrentRow, name: Option<&String>) -> JsonValue {
    let mut value = empty_object();
    insert_string_field(&mut value, "account", row.subject.clone());
    insert_value_field(&mut value, "resource_hex", JsonValue::Null);
    insert_string_field(&mut value, "resource_id", row.resource_id.to_string());
    insert_nullable_string_field(
        &mut value,
        "name",
        name.cloned(),
    );
    // permissions_current currently exposes post-scope powers, not a raw role bitmap.
    insert_value_field(&mut value, "role_bitmap", JsonValue::Null);
    insert_value_field(&mut value, "effective_powers", row.effective_powers.clone());
    value
}
