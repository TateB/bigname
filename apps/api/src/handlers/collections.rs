use super::*;

pub(super) async fn address_names(
    Path(address): Path<String>,
    Query(query): Query<AddressNamesQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<AddressNamesResponse>> {
    let namespace = parse_address_names_namespace(query.namespace.as_deref())?;
    let relation = parse_address_name_relation(query.relation.as_deref())?;
    let dedupe_by = parse_address_names_dedupe_by(query.dedupe_by.as_deref())?;
    let include = parse_address_names_include(query.include.as_deref())?;
    let pagination = parse_pagination(query.cursor.as_deref(), query.page_size)?;
    let normalized_address = parse_evm_address(&address, "address")?;

    let mut filters = BTreeMap::new();
    if let Some(namespace) = namespace.as_ref() {
        filters.insert("namespace".to_owned(), namespace.clone());
    }
    if let Some(relation) = relation {
        filters.insert("relation".to_owned(), relation.as_str().to_owned());
    }
    filters.insert(
        "dedupe_by".to_owned(),
        address_names_dedupe_label(dedupe_by).to_owned(),
    );
    let cursor_spec = CursorSpec {
        route: "/v1/addresses/{address}/names",
        anchor: normalized_address.clone(),
        sort: "display_name_asc",
        filters,
    };
    let storage_cursor = address_names_storage_cursor(&pagination, &cursor_spec)?;
    let storage_page = bigname_storage::load_address_names_current_page(
        &state.pool,
        &normalized_address,
        namespace.as_deref(),
        relation,
        dedupe_by,
        storage_cursor.as_ref(),
        storage_page_size(&pagination),
    )
    .await
    .map_err(|load_error| {
        if storage_cursor.is_some()
            && load_error
                .to_string()
                .contains("page cursor does not match a grouped entry")
        {
            return invalid_cursor_error();
        }

        error!(
            service = "api",
            address = %normalized_address,
            namespace = ?namespace,
            relation = relation.map(|value| value.as_str()),
            dedupe_by = address_names_dedupe_label(dedupe_by),
            error = ?load_error,
            "failed to load address_names_current rows"
        );
        ApiError::internal_error(format!(
            "failed to load current address-name collection for address {normalized_address}"
        ))
    })?;
    let page = page_response_from_storage_cursor(
        &pagination,
        &cursor_spec,
        storage_page
            .next_cursor
            .as_ref()
            .map(address_names_cursor_item),
    );
    let response = if include.role_summary {
        build_address_names_response_with_role_summary(
            &state.pool,
            &storage_page.entries,
            &storage_page.summary,
            page,
        )
        .await?
    } else {
        let data = storage_page
            .entries
            .iter()
            .map(build_address_name_item)
            .collect();
        build_address_names_response_from_summary(
            &storage_page.summary,
            data,
            AddressNamesResponseSupplement::default(),
            page,
        )
    };

    Ok(Json(response))
}

pub(super) async fn name_children(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<ChildrenQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<JsonValue>> {
    let name = parse_exact_name_path_name(&namespace, &name)?;

    let include_counts = parse_children_query(&query)?;
    let view = parse_response_view(query.view.as_deref(), ResponseView::Compact)?;
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
                "failed to load name surface for children route"
            );
            ApiError::internal_error(format!(
                "failed to load child collection for name {namespace}/{name}"
            ))
        })?;

    let Some(surface) = surface else {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!("name {name} was not found in namespace {namespace}"),
        });
    };

    let cursor_spec = CursorSpec {
        route: "/v1/names/{namespace}/{name}/children",
        anchor: logical_name_id.clone(),
        sort: "display_name_asc",
        filters: BTreeMap::new(),
    };
    let storage_cursor = children_storage_cursor(&pagination, &cursor_spec)?;
    if let Some(cursor) = storage_cursor.as_ref() {
        ensure_children_cursor_exists(&state.pool, &logical_name_id, cursor).await?;
    }

    let storage_page = bigname_storage::load_children_current_page(
        &state.pool,
        &logical_name_id,
        storage_cursor.as_ref(),
        storage_page_size(&pagination),
    )
    .await
    .map_err(|load_error| {
        error!(
            service = "api",
            namespace = %namespace,
            name = %name,
            logical_name_id = %logical_name_id,
            error = ?load_error,
            "failed to load children_current rows"
        );
        ApiError::internal_error(format!(
            "failed to load child collection for name {namespace}/{name}"
        ))
    })?;
    let page = page_response_from_storage_cursor(
        &pagination,
        &cursor_spec,
        storage_page.next_cursor.as_ref().map(children_cursor_item),
    );

    match view {
        ResponseView::Full => serde_json::to_value(build_children_response_from_summary(
            &storage_page.summary,
            &storage_page.rows,
            include_counts,
            page,
        ))
        .map(Json)
        .map_err(|serialize_error| {
            error!(
                service = "api",
                namespace = %namespace,
                name = %name,
                logical_name_id = %logical_name_id,
                error = ?serialize_error,
                "failed to serialize full children response"
            );
            ApiError::internal_error(format!(
                "failed to serialize child collection for name {namespace}/{name}"
            ))
        }),
        ResponseView::Compact => {
            let child_logical_name_ids = storage_page
                .rows
                .iter()
                .map(|row| row.child_logical_name_id.clone())
                .collect::<Vec<_>>();
            let child_name_rows =
                bigname_storage::load_name_current_by_logical_name_ids(
                    &state.pool,
                    &child_logical_name_ids,
                )
                .await
                .map_err(|load_error| {
                    error!(
                        service = "api",
                        namespace = %namespace,
                        name = %name,
                        logical_name_id = %logical_name_id,
                        error = ?load_error,
                        "failed to batch load name_current rows for compact children route"
                    );
                    ApiError::internal_error(format!(
                        "failed to load compact child collection for name {namespace}/{name}"
                    ))
                })?;
            let child_summaries = if include_counts {
                let summaries = bigname_storage::load_children_current_summaries(
                    &state.pool,
                    &child_logical_name_ids,
                )
                .await
                .map_err(|load_error| {
                    error!(
                        service = "api",
                        namespace = %namespace,
                        name = %name,
                        logical_name_id = %logical_name_id,
                        error = ?load_error,
                        "failed to batch load children_current summaries for compact children route"
                    );
                    ApiError::internal_error(format!(
                        "failed to load compact child counts for name {namespace}/{name}"
                    ))
                })?;

                summaries
                    .into_iter()
                    .map(|summary| (summary.parent_logical_name_id.clone(), summary))
                    .collect()
            } else {
                BTreeMap::new()
            };
            let mut child_surface_labelhashes = BTreeMap::new();
            let mut child_surface_ids = BTreeSet::new();
            for row in storage_page
                .rows
                .iter()
                .filter(|row| include_counts || row.labelhash.is_none())
            {
                let child_surface = load_name_surface(&state.pool, &row.child_logical_name_id)
                    .await
                    .map_err(|load_error| {
                        error!(
                            service = "api",
                            namespace = %namespace,
                            name = %name,
                            logical_name_id = %logical_name_id,
                            child_logical_name_id = %row.child_logical_name_id,
                            error = ?load_error,
                            "failed to load child name surface for compact children labelhash fallback"
                        );
                        ApiError::internal_error(format!(
                            "failed to load compact child collection for name {namespace}/{name}"
                        ))
                    })?;
                if let Some(surface) = child_surface {
                    child_surface_ids.insert(row.child_logical_name_id.clone());
                    if row.labelhash.is_none() {
                        child_surface_labelhashes.insert(
                            row.child_logical_name_id.clone(),
                            surface.labelhashes.into_iter().next(),
                        );
                    }
                } else if row.labelhash.is_none() {
                    child_surface_labelhashes.insert(row.child_logical_name_id.clone(), None);
                }
            }
            Ok(Json(build_compact_children_response(
                &storage_page.summary,
                &storage_page.rows,
                &surface.normalized_name,
                &child_surface_ids,
                &child_surface_labelhashes,
                &child_name_rows,
                &child_summaries,
                include_counts,
                meta,
                page,
            )))
        }
    }
}

pub(super) async fn resource_permissions(
    Path(resource_id): Path<String>,
    Query(query): Query<PermissionsQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<ResourcePermissionsResponse>> {
    let resource_id = Uuid::parse_str(&resource_id).map_err(|_| ApiError {
        status: StatusCode::BAD_REQUEST,
        code: "invalid_input",
        message: "resource_id must be a UUID".to_owned(),
    })?;
    let subject = parse_permissions_subject(query.subject.as_deref());
    let scope = parse_permission_scope_filter(query.scope.as_deref())?;
    let pagination = parse_pagination(query.cursor.as_deref(), query.page_size)?;

    let mut filters = BTreeMap::new();
    if let Some(subject) = subject.as_ref() {
        filters.insert("subject".to_owned(), subject.clone());
    }
    if let Some(scope) = scope.as_ref() {
        filters.insert("scope".to_owned(), scope.storage_key());
    }
    let cursor_spec = CursorSpec {
        route: "/v1/resources/{resource_id}/permissions",
        anchor: resource_id.to_string(),
        sort: "subject_scope_asc",
        filters,
    };
    let storage_cursor = permissions_storage_cursor(&pagination, &cursor_spec)?;
    if let Some(cursor) = storage_cursor.as_ref() {
        ensure_permissions_cursor_exists(
            &state.pool,
            resource_id,
            subject.as_deref(),
            scope.as_ref(),
            cursor,
        )
        .await?;
    }

    let storage_page = bigname_storage::load_permissions_current_page(
        &state.pool,
        resource_id,
        subject.as_deref(),
        scope.as_ref(),
        storage_cursor.as_ref(),
        storage_page_size(&pagination),
    )
    .await
    .map_err(|load_error| {
        error!(
            service = "api",
            resource_id = %resource_id,
            subject = ?subject,
            scope = scope.as_ref().map(PermissionScope::storage_key),
            error = ?load_error,
            "failed to load permissions_current rows"
        );
        ApiError::internal_error(format!(
            "failed to load permissions for resource {resource_id}"
        ))
    })?;
    let page = page_response_from_storage_cursor(
        &pagination,
        &cursor_spec,
        storage_page
            .next_cursor
            .as_ref()
            .map(permissions_cursor_item),
    );

    Ok(Json(build_resource_permissions_response_from_summary(
        &storage_page.summary,
        &storage_page.rows,
        page,
    )))
}

async fn build_address_names_response_with_role_summary(
    pool: &PgPool,
    page_entries: &[AddressNameCurrentEntry],
    collection_summary: &bigname_storage::AddressNamesCurrentSummary,
    page: HistoryPageResponse,
) -> ApiResult<AddressNamesResponse> {
    let logical_name_ids = page_entries
        .iter()
        .map(|entry| entry.logical_name_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let resource_ids = page_entries
        .iter()
        .map(|entry| entry.resource_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    let name_rows = bigname_storage::load_name_current_by_logical_name_ids(pool, &logical_name_ids)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                logical_name_ids = ?logical_name_ids,
                error = ?load_error,
                "failed to batch load name_current rows for address role summary expansion"
            );
            ApiError::internal_error(
                "failed to load current projections for address role summary expansion",
            )
        })?;
    let permissions_by_resource = bigname_storage::load_permissions_current_by_resource_ids(
        pool,
        &resource_ids,
    )
    .await
    .map_err(|load_error| {
        error!(
            service = "api",
            resource_ids = ?resource_ids,
            error = ?load_error,
            "failed to batch load permissions_current rows for address role summary expansion"
        );
        ApiError::internal_error("failed to load permissions for address role summary expansion")
    })?;
    let children_summaries = bigname_storage::load_children_current_summaries(
        pool,
        &logical_name_ids,
    )
    .await
    .map_err(|load_error| {
        error!(
            service = "api",
            logical_name_ids = ?logical_name_ids,
            error = ?load_error,
            "failed to batch load children_current summaries for address role summary expansion"
        );
        ApiError::internal_error(
            "failed to load child summaries for address role summary expansion",
        )
    })?;
    let children_summaries = children_summaries
        .into_iter()
        .map(|summary| (summary.parent_logical_name_id.clone(), summary))
        .collect::<BTreeMap<_, _>>();

    let mut data = Vec::with_capacity(page_entries.len());
    let mut supplement = AddressNamesResponseSupplement::default();

    for entry in page_entries {
        let name_row = name_rows.get(&entry.logical_name_id);
        let permissions = permissions_by_resource
            .get(&entry.resource_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let children_summary = children_summaries.get(&entry.logical_name_id);
        let child_count = children_summary
            .map(|summary| u64::try_from(summary.child_count).unwrap_or_default())
            .unwrap_or_default();

        if let Some(row) = name_row {
            supplement.push_name_current(row);
        }
        supplement.push_permissions(permissions);
        if let Some(summary) = children_summary {
            supplement.push_children_summary(summary);
        }
        data.push(build_address_name_item_with_role_summary(
            entry,
            name_row,
            permissions,
            child_count,
        ));
    }

    Ok(build_address_names_response_from_summary(
        collection_summary,
        data,
        supplement,
        page,
    ))
}
