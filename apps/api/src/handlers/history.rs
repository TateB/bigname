use super::*;

pub(super) async fn address_history(
    Path(address): Path<String>,
    Query(query): Query<AddressHistoryQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<JsonValue>> {
    let namespace = parse_address_names_namespace(query.namespace.as_deref())?;
    let relation = parse_address_name_relation(query.relation.as_deref())?;
    let scope = parse_history_scope(query.scope.as_deref())?;
    let view = parse_response_view(query.view.as_deref(), ResponseView::Full)?;
    let meta = parse_meta_mode(query.meta.as_deref(), MetaMode::Summary)?;
    let pagination = parse_pagination(query.cursor.as_deref(), query.page_size)?;
    let normalized_address = parse_evm_address(&address, "address")?;

    let mut filters = BTreeMap::new();
    filters.insert("scope".to_owned(), scope.as_str().to_owned());
    if let Some(namespace) = namespace.as_ref() {
        filters.insert("namespace".to_owned(), namespace.clone());
    }
    if let Some(relation) = relation {
        filters.insert("relation".to_owned(), relation.as_str().to_owned());
    }
    let cursor_spec = CursorSpec {
        route: "/v1/history/addresses/{address}",
        anchor: normalized_address.clone(),
        sort: "chain_position_desc",
        filters,
    };
    let storage_cursor = history_storage_cursor(&pagination, &cursor_spec)?;

    let storage_page = load_address_history_page(
        &state.pool,
        &normalized_address,
        namespace.as_deref(),
        relation,
        scope,
        true,
        storage_cursor.as_ref(),
        storage_page_size(&pagination),
        history_route_summary_mode(view, meta),
    )
    .await
    .map_err(|load_error| {
        if load_error
            .downcast_ref::<bigname_storage::InvalidHistoryCursor>()
            .is_some()
        {
            return invalid_cursor_error();
        }

        error!(
            service = "api",
            address = %normalized_address,
            namespace = ?namespace,
            relation = relation.map(|value| value.as_str()),
            scope = scope.as_str(),
            error = ?load_error,
            "failed to load address history"
        );
        ApiError::internal_error(format!(
            "failed to load history for address {normalized_address}"
        ))
    })?;
    let page = page_response_from_storage_cursor(
        &pagination,
        &cursor_spec,
        storage_page.next_cursor.as_ref().map(history_cursor_item),
    );

    Ok(Json(build_history_route_response(
        storage_page.summary.as_ref(),
        &storage_page.rows,
        scope,
        page,
        view,
        meta,
    )))
}

pub(super) async fn name_history(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<HistoryQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<JsonValue>> {
    let name = parse_exact_name_path_name(&namespace, &name)?;

    let scope = parse_history_scope(query.scope.as_deref())?;
    let view = parse_response_view(query.view.as_deref(), ResponseView::Full)?;
    let meta = parse_meta_mode(query.meta.as_deref(), MetaMode::Summary)?;
    let pagination = parse_pagination(query.cursor.as_deref(), query.page_size)?;
    let logical_name_id = format!("{namespace}:{name}");
    let surface = load_name_surface(&state.pool, &logical_name_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                name = %name,
                logical_name_id = %logical_name_id,
                error = ?load_error,
                "failed to load name surface for history route"
            );
            ApiError::internal_error(format!(
                "failed to load history for name {namespace}/{name}"
            ))
        })?;

    let Some(_surface) = surface else {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!("name {name} was not found in namespace {namespace}"),
        });
    };

    let resource_ids = resource_ids_for_name(&state.pool, &logical_name_id).await?;
    let mut filters = BTreeMap::new();
    filters.insert("scope".to_owned(), scope.as_str().to_owned());
    let cursor_spec = CursorSpec {
        route: "/v1/history/names/{namespace}/{name}",
        anchor: logical_name_id.clone(),
        sort: "chain_position_desc",
        filters,
    };
    let storage_cursor = history_storage_cursor(&pagination, &cursor_spec)?;

    let storage_page = load_name_history_page(
        &state.pool,
        &logical_name_id,
        &resource_ids,
        scope,
        true,
        storage_cursor.as_ref(),
        storage_page_size(&pagination),
        history_route_summary_mode(view, meta),
    )
    .await
    .map_err(|load_error| {
        if load_error
            .downcast_ref::<bigname_storage::InvalidHistoryCursor>()
            .is_some()
        {
            return invalid_cursor_error();
        }

        error!(
            service = "api",
            namespace = %namespace,
            name = %name,
            logical_name_id = %logical_name_id,
            resource_ids = ?resource_ids,
            scope = scope.as_str(),
            error = ?load_error,
            "failed to load name history"
        );
        ApiError::internal_error(format!(
            "failed to load history for name {namespace}/{name}"
        ))
    })?;
    let page = page_response_from_storage_cursor(
        &pagination,
        &cursor_spec,
        storage_page.next_cursor.as_ref().map(history_cursor_item),
    );

    Ok(Json(build_history_route_response(
        storage_page.summary.as_ref(),
        &storage_page.rows,
        scope,
        page,
        view,
        meta,
    )))
}

pub(super) async fn resource_history(
    Path(resource_id): Path<String>,
    Query(query): Query<HistoryQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<JsonValue>> {
    let scope = parse_history_scope(query.scope.as_deref())?;
    let view = parse_response_view(query.view.as_deref(), ResponseView::Full)?;
    let meta = parse_meta_mode(query.meta.as_deref(), MetaMode::Summary)?;
    let pagination = parse_pagination(query.cursor.as_deref(), query.page_size)?;
    let resource_id = Uuid::parse_str(&resource_id).map_err(|_| ApiError {
        status: StatusCode::BAD_REQUEST,
        code: "invalid_input",
        message: "resource_id must be a UUID".to_owned(),
    })?;

    let resource = load_resource(&state.pool, resource_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                resource_id = %resource_id,
                error = ?load_error,
                "failed to load resource for history route"
            );
            ApiError::internal_error(format!("failed to load history for resource {resource_id}"))
        })?;

    let Some(_resource) = resource else {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!("resource {resource_id} was not found"),
        });
    };

    let logical_name_ids = logical_name_ids_for_resource(&state.pool, resource_id).await?;
    let mut filters = BTreeMap::new();
    filters.insert("scope".to_owned(), scope.as_str().to_owned());
    let cursor_spec = CursorSpec {
        route: "/v1/history/resources/{resource_id}",
        anchor: resource_id.to_string(),
        sort: "chain_position_desc",
        filters,
    };
    let storage_cursor = history_storage_cursor(&pagination, &cursor_spec)?;

    let storage_page = load_resource_history_page(
        &state.pool,
        resource_id,
        &logical_name_ids,
        scope,
        true,
        storage_cursor.as_ref(),
        storage_page_size(&pagination),
        history_route_summary_mode(view, meta),
    )
    .await
    .map_err(|load_error| {
        if load_error
            .downcast_ref::<bigname_storage::InvalidHistoryCursor>()
            .is_some()
        {
            return invalid_cursor_error();
        }

        error!(
            service = "api",
            resource_id = %resource_id,
            logical_name_ids = ?logical_name_ids,
            scope = scope.as_str(),
            error = ?load_error,
            "failed to load resource history"
        );
        ApiError::internal_error(format!("failed to load history for resource {resource_id}"))
    })?;
    let page = page_response_from_storage_cursor(
        &pagination,
        &cursor_spec,
        storage_page.next_cursor.as_ref().map(history_cursor_item),
    );

    Ok(Json(build_history_route_response(
        storage_page.summary.as_ref(),
        &storage_page.rows,
        scope,
        page,
        view,
        meta,
    )))
}
