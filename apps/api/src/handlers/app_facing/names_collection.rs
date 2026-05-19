use super::*;

pub(super) async fn names(
    Query(query): Query<NamesQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<CompactNamesResponse>> {
    let parsed = parse_names_request(query)?;
    let cursor_spec = parsed.cursor_spec();
    let storage_cursor =
        names_storage_cursor(&parsed.pagination, &cursor_spec, parsed.sort, parsed.order)?;

    let storage_page = bigname_storage::load_name_current_list_page(
        &state.pool,
        &parsed.filter,
        parsed.sort,
        parsed.order,
        storage_cursor.as_ref(),
        storage_page_size(&parsed.pagination),
    )
    .await
    .map_err(|load_error| {
        error!(
            service = "api",
            filter = ?parsed.filter,
            sort = parsed.sort.as_str(),
            order = parsed.order.as_str(),
            error = ?load_error,
            "failed to load app-facing compact names page"
        );
        ApiError::internal_error("failed to load compact names collection")
    })?;
    let page = page_response_from_storage_cursor(
        &parsed.pagination,
        &cursor_spec,
        storage_page
            .next_cursor
            .as_ref()
            .map(|cursor| names_cursor_item(cursor, parsed.sort)),
    );
    let meta = parsed.meta.include_summary().then(|| {
        compact_meta_object(
            "partial",
            parsed.include.total_count.then_some(storage_page.total_count),
            parsed.unsupported_fields,
            Vec::new(),
        )
    });

    Ok(Json(build_compact_names_response(
        &storage_page.rows,
        page,
        meta,
    )))
}

pub(super) async fn address_names_count(
    Path(address): Path<String>,
    Query(query): Query<AddressNamesCountQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<AddressNamesCountResponse>> {
    let address = parse_address_filter_value("address", &address)?;
    let namespace = parse_address_names_namespace(query.namespace.as_deref())?;
    let relation = parse_app_relation(query.relation.as_deref())?;
    let prefix = parse_optional_nonempty_query_value(query.prefix, "prefix")?;
    let contains = parse_optional_nonempty_query_value(query.contains, "contains")?;
    let contains_nocase =
        parse_optional_nonempty_query_value(query.contains_nocase, "contains_nocase")?;
    let resolver = parse_optional_address_filter("resolver", query.resolver.as_deref())?;

    let count_filter = bigname_storage::AddressNamesCurrentCountFilter {
        address: address.clone(),
        namespace: namespace.clone(),
        relation,
        prefix,
        contains,
        contains_nocase,
        resolver,
    };
    let count =
        bigname_storage::count_address_names_current_for_app_filter(&state.pool, &count_filter)
            .await
            .map_err(|load_error| {
                error!(
                    service = "api",
                    filter = ?count_filter,
                    error = ?load_error,
                    "failed to count app-facing address names"
                );
                ApiError::internal_error("failed to count address names")
            })?;

    Ok(Json(build_address_names_count_response(
        &address,
        namespace.as_deref(),
        relation.as_str(),
        count,
    )))
}

include!("names_collection_support.rs");
