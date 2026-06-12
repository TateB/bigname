use super::*;

pub(super) async fn roles(
    Query(query): Query<RolesQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<CompactRolesResponse>> {
    parse_compact_only_response_view(
        query.view.as_deref(),
        "view=full is not supported for compact roles",
    )?;
    let meta_mode = parse_meta_mode(query.meta.as_deref(), MetaMode::Summary)?;
    reject_role_bitmap_filter(query.role_bitmap.as_deref())?;

    let account = parse_roles_account(query.account.as_deref());
    let requested_resource_id = parse_optional_roles_resource_id(query.resource_id.as_deref())?;
    let name_filter = parse_roles_name_filter(query.namespace.as_deref(), query.name.as_deref())?;
    if account.is_none() && requested_resource_id.is_none() && name_filter.is_none() {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "at least one of account, resource_id, or namespace+name is required"
                .to_owned(),
        });
    }

    let resolved_name_row = match name_filter.as_ref() {
        Some((namespace, name)) => Some(load_resource_lookup_row(&state.pool, namespace, name).await?),
        None => None,
    };
    let resolved_resource_id = resolved_name_row
        .as_ref()
        .map(resource_id_from_name_current)
        .transpose()?;
    let effective_resource_id = requested_resource_id.or(resolved_resource_id);
    let name_resource_mismatch = requested_resource_id
        .zip(resolved_resource_id)
        .is_some_and(|(requested, resolved)| requested != resolved);

    let pagination = parse_pagination(query.cursor.as_deref(), query.page_size)?;
    let cursor_spec = roles_cursor_spec(
        "/v1/roles",
        "roles".to_owned(),
        "account_resource_scope_asc",
        account.as_deref(),
        requested_resource_id,
        name_filter.as_ref(),
    );
    let storage_cursor = roles_storage_cursor(&pagination, &cursor_spec)?;

    if name_resource_mismatch {
        if storage_cursor.is_some() {
            return Err(invalid_cursor_error());
        }
        let page = page_response_from_storage_cursor(&pagination, &cursor_spec, None);
        return Ok(Json(build_empty_compact_roles_response(page, meta_mode)));
    }

    ensure_permissions_current_projection_available(&state.pool, "/v1/roles").await?;

    if let Some(cursor) = storage_cursor.as_ref() {
        ensure_roles_cursor_exists(
            &state.pool,
            account.as_deref(),
            effective_resource_id,
            cursor,
            "/v1/roles",
        )
        .await?;
    }

    let storage_page = load_roles_page(
        &state.pool,
        account.as_deref(),
        effective_resource_id,
        storage_cursor.as_ref(),
        storage_page_size(&pagination),
        "/v1/roles",
    )
    .await?;
    let page = page_response_from_storage_cursor(
        &pagination,
        &cursor_spec,
        storage_page.next_cursor.as_ref().map(roles_cursor_item),
    );
    let associated_names =
        load_associated_role_names(&state.pool, &storage_page.rows, resolved_name_row.as_ref())
            .await?;

    Ok(Json(build_compact_roles_response(
        &storage_page.rows,
        &associated_names,
        &storage_page.summary,
        page,
        meta_mode,
    )))
}

pub(super) async fn name_roles(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<NameRolesQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<CompactRolesResponse>> {
    let name = parse_exact_name_path_name(&namespace, &name)?;
    parse_compact_only_response_view(
        query.view.as_deref(),
        "view=full is not supported for compact name roles",
    )?;
    let meta_mode = parse_meta_mode(query.meta.as_deref(), MetaMode::Summary)?;
    reject_role_bitmap_filter(query.role_bitmap.as_deref())?;

    let account = parse_roles_account(query.account.as_deref());
    let name_row = load_resource_lookup_row(&state.pool, &namespace, &name).await?;
    let resource_id = resource_id_from_name_current(&name_row)?;
    let pagination = parse_pagination(query.cursor.as_deref(), query.page_size)?;
    let logical_name_id = format!("{namespace}:{name}");
    let cursor_spec = roles_cursor_spec(
        "/v1/names/{namespace}/{name}/roles",
        logical_name_id,
        "account_scope_asc",
        account.as_deref(),
        None,
        None,
    );
    let storage_cursor = roles_storage_cursor(&pagination, &cursor_spec)?;
    ensure_permissions_current_projection_available(
        &state.pool,
        "/v1/names/{namespace}/{name}/roles",
    )
    .await?;

    if let Some(cursor) = storage_cursor.as_ref() {
        ensure_roles_cursor_exists(
            &state.pool,
            account.as_deref(),
            Some(resource_id),
            cursor,
            "/v1/names/{namespace}/{name}/roles",
        )
        .await?;
    }

    let storage_page = load_roles_page(
        &state.pool,
        account.as_deref(),
        Some(resource_id),
        storage_cursor.as_ref(),
        storage_page_size(&pagination),
        "/v1/names/{namespace}/{name}/roles",
    )
    .await?;
    let page = page_response_from_storage_cursor(
        &pagination,
        &cursor_spec,
        storage_page.next_cursor.as_ref().map(roles_cursor_item),
    );
    let associated_names =
        load_associated_role_names(&state.pool, &storage_page.rows, Some(&name_row)).await?;

    Ok(Json(build_compact_roles_response(
        &storage_page.rows,
        &associated_names,
        &storage_page.summary,
        page,
        meta_mode,
    )))
}

pub(super) async fn resource_lookup(
    Query(query): Query<ResourceLookupQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<ResourceLookupResponse>> {
    parse_compact_only_response_view(
        query.view.as_deref(),
        "view=full is not supported for resource lookup",
    )?;
    let meta_mode = parse_meta_mode(query.meta.as_deref(), MetaMode::Summary)?;
    let (namespace, name) =
        parse_required_resource_lookup_name(query.namespace.as_deref(), query.name.as_deref())?;
    let row = load_resource_lookup_row(&state.pool, &namespace, &name).await?;
    let resource_id = resource_id_from_name_current(&row)?;

    Ok(Json(build_resource_lookup_response(
        &row,
        resource_id,
        meta_mode,
    )))
}

include!("roles_support.rs");
