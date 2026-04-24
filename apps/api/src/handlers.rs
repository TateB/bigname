async fn health(State(state): State<AppState>) -> (StatusCode, Json<HealthResponse>) {
    let database_reachable = sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&state.pool)
        .await;

    let (http_status, status, database) = match database_reachable {
        Ok(_) => (
            StatusCode::OK,
            "ready",
            HealthDatabaseResponse {
                status: "reachable",
                reachable: true,
                check: "select_1",
                error: None,
            },
        ),
        Err(readiness_error) => {
            warn!(
                service = "api",
                phase = state.phase,
                error = ?readiness_error,
                "database readiness probe failed"
            );
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "degraded",
                HealthDatabaseResponse {
                    status: "unreachable",
                    reachable: false,
                    check: "select_1",
                    error: Some("database readiness query failed"),
                },
            )
        }
    };

    (
        http_status,
        Json(HealthResponse {
            service: "api",
            phase: state.phase,
            status,
            process: HealthProcessResponse { status: "running" },
            database,
        }),
    )
}

async fn namespace_metadata(
    Path(namespace): Path<String>,
    State(state): State<AppState>,
) -> ApiResult<Json<NamespaceMetadataResponse>> {
    ensure_public_namespace(&namespace)?;

    let snapshot = load_namespace_manifest_snapshot(&state.pool, &namespace)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                error = ?load_error,
                "failed to load namespace metadata"
            );
            ApiError::internal_error(format!(
                "failed to load namespace metadata for namespace {namespace}"
            ))
        })?;

    Ok(Json(build_namespace_metadata_response(namespace, snapshot)))
}

async fn namespace_manifests(
    Path(namespace): Path<String>,
    State(state): State<AppState>,
) -> ApiResult<Json<NamespaceManifestsResponse>> {
    ensure_public_namespace(&namespace)?;

    let snapshot = load_namespace_manifest_snapshot(&state.pool, &namespace)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                error = ?load_error,
                "failed to load manifest snapshot for namespace"
            );
            ApiError::internal_error(format!(
                "failed to load manifest snapshot for namespace {namespace}"
            ))
        })?;

    Ok(Json(build_namespace_manifests_response(
        namespace, snapshot,
    )))
}

async fn name_current(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<ExactNameSnapshotQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<NameResponse>> {
    ensure_public_namespace(&namespace)?;

    let selected_snapshot = resolve_exact_name_selected_snapshot(
        &state.pool,
        &namespace,
        ExactNameSnapshotSelector::from(&query),
        false,
    )
    .await
    .map_err(|error| {
        map_internal_api_error(
            error,
            format!("failed to load current projection for name {namespace}/{name}"),
        )
    })?;
    let row =
        load_name_current_for_selected_snapshot(&state.pool, &namespace, &name, &selected_snapshot)
            .await
            .map_err(|error| {
                map_internal_api_error(
                    error,
                    format!("failed to load current projection for name {namespace}/{name}"),
                )
            })?;

    let record_inventory_current =
        load_supported_record_inventory_current_for_snapshot(&state.pool, &row, &selected_snapshot)
            .await
            .map_err(snapshot_selection_api_error)?;

    Ok(Json(build_name_response(
        row,
        record_inventory_current.as_ref(),
        &selected_snapshot,
    )))
}

async fn coverage_current(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<ExactNameSnapshotQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<NameResponse>> {
    ensure_public_namespace(&namespace)?;

    let selected_snapshot = resolve_exact_name_selected_snapshot(
        &state.pool,
        &namespace,
        ExactNameSnapshotSelector::from(&query),
        false,
    )
    .await
    .map_err(|error| {
        map_internal_api_error(
            error,
            format!("failed to load current projection for name {namespace}/{name}"),
        )
    })?;
    let row =
        load_name_current_for_selected_snapshot(&state.pool, &namespace, &name, &selected_snapshot)
            .await
            .map_err(|error| {
                map_internal_api_error(
                    error,
                    format!("failed to load current projection for name {namespace}/{name}"),
                )
            })?;

    Ok(Json(build_name_coverage_response(row, &selected_snapshot)))
}

async fn explain_surface_binding_current(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<ExactNameSnapshotQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<NameResponse>> {
    ensure_public_namespace(&namespace)?;

    let selected_snapshot = resolve_exact_name_selected_snapshot(
        &state.pool,
        &namespace,
        ExactNameSnapshotSelector::from(&query),
        false,
    )
    .await
    .map_err(|error| {
        map_internal_api_error(
            error,
            format!("failed to load surface-binding explain projection for name {namespace}/{name}"),
        )
    })?;
    let row =
        load_name_current_for_selected_snapshot(&state.pool, &namespace, &name, &selected_snapshot)
            .await
            .map_err(|error| {
                map_internal_api_error(
                    error,
                    format!(
                        "failed to load surface-binding explain projection for name {namespace}/{name}"
                    ),
                )
            })?;

    Ok(Json(build_name_surface_binding_explain_response(
        row,
        &selected_snapshot,
    )))
}

async fn explain_authority_control_current(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<ExactNameSnapshotQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<NameResponse>> {
    ensure_public_namespace(&namespace)?;

    let selected_snapshot = resolve_exact_name_selected_snapshot(
        &state.pool,
        &namespace,
        ExactNameSnapshotSelector::from(&query),
        false,
    )
    .await
    .map_err(|error| {
        map_internal_api_error(
            error,
            format!("failed to load authority-control explain projection for name {namespace}/{name}"),
        )
    })?;
    let row =
        load_name_current_for_selected_snapshot(&state.pool, &namespace, &name, &selected_snapshot)
            .await
            .map_err(|error| {
                map_internal_api_error(
                    error,
                    format!(
                        "failed to load authority-control explain projection for name {namespace}/{name}"
                    ),
                )
            })?;

    Ok(Json(build_name_authority_control_explain_response(
        row,
        &selected_snapshot,
    )))
}

async fn explain_resolution_execution_current(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<ResolutionExecutionExplainQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<ResolutionResponse>> {
    ensure_public_namespace(&namespace)?;

    let records = parse_resolution_record_keys(query.records.as_deref(), ResolutionMode::Verified)?;
    let logical_name_id = format!("{namespace}:{name}");
    let row = load_name_current(&state.pool, &logical_name_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                name = %name,
                logical_name_id = %logical_name_id,
                records = ?records,
                error = ?load_error,
                "failed to load exact-name current projection for resolution execution explain route"
            );
            ApiError::internal_error(format!(
                "failed to load resolution execution explain projection for name {namespace}/{name}"
            ))
        })?;

    let Some(row) = row else {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!("name {name} was not found in namespace {namespace}"),
        });
    };

    let record_inventory_current = load_supported_record_inventory_current(&state.pool, &row)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                name = %name,
                logical_name_id = %logical_name_id,
                records = ?records,
                error = ?load_error,
                "failed to load declared record inventory for resolution execution explain route"
            );
            ApiError::internal_error(format!(
                "failed to load resolution execution explain projection for name {namespace}/{name}"
            ))
        })?;

    if resolution_verified_support_boundary(&row, record_inventory_current.as_ref()).is_none() {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!(
                "persisted resolution execution explain was not found for name {name} in namespace {namespace}"
            ),
        });
    }

    let cache_key_records = resolution_execution_cache_lookup_records(&row, &records);
    let cache_key = build_resolution_execution_cache_key(
        &row,
        &cache_key_records,
        record_inventory_current.as_ref(),
        row.chain_positions.clone(),
    )
    .map_err(|cache_key_error| {
        error!(
            service = "api",
            namespace = %namespace,
            name = %name,
            logical_name_id = %logical_name_id,
            records = ?records,
            error = ?cache_key_error,
            "failed to derive persisted execution cache key for resolution execution explain route"
        );
        ApiError::internal_error(format!(
            "failed to load resolution execution explain projection for name {namespace}/{name}"
        ))
    })?;

    let outcome = load_execution_outcome(&state.pool, &cache_key)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                name = %name,
                logical_name_id = %logical_name_id,
                records = ?records,
                error = ?load_error,
                "failed to load persisted execution outcome for resolution execution explain route"
            );
            ApiError::internal_error(format!(
                "failed to load resolution execution explain projection for name {namespace}/{name}"
            ))
        })?;

    let Some(outcome) = outcome else {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!(
                "persisted resolution execution explain was not found for name {name} in namespace {namespace}"
            ),
        });
    };

    let trace = load_execution_trace(&state.pool, outcome.execution_trace_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                name = %name,
                logical_name_id = %logical_name_id,
                execution_trace_id = %outcome.execution_trace_id,
                error = ?load_error,
                "failed to load persisted execution trace for resolution execution explain route"
            );
            ApiError::internal_error(format!(
                "failed to load resolution execution explain projection for name {namespace}/{name}"
            ))
        })?;

    let Some(trace) = trace else {
        return Err(ApiError::internal_error(format!(
            "failed to load resolution execution explain projection for name {namespace}/{name}"
        )));
    };

    let response = build_resolution_execution_explain_response(row, &records, &trace, &outcome)
        .map_err(|build_error| {
            error!(
                service = "api",
                namespace = %namespace,
                name = %name,
                logical_name_id = %logical_name_id,
                execution_trace_id = %outcome.execution_trace_id,
                error = ?build_error,
                "failed to build resolution execution explain response"
            );
            ApiError::internal_error(format!(
                "failed to load resolution execution explain projection for name {namespace}/{name}"
            ))
        })?;

    Ok(Json(response))
}

async fn resolution_current(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<ResolutionQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<ResolutionResponse>> {
    ensure_public_namespace(&namespace)?;

    Ok(Json(
        resolution_response_for_name(&state.pool, &namespace, &name, query).await?,
    ))
}

async fn resolve_current(
    Path(name): Path<String>,
    Query(query): Query<InferredResolutionQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<ResolutionResponse>> {
    let namespace = infer_resolution_namespace(&name);
    let query = ResolutionQuery {
        mode: query.mode,
        records: query.records,
        ..ResolutionQuery::default()
    };

    Ok(Json(
        resolution_response_for_name(&state.pool, namespace, &name, query).await?,
    ))
}

async fn resolution_response_for_name(
    pool: &PgPool,
    namespace: &str,
    name: &str,
    query: ResolutionQuery,
) -> ApiResult<ResolutionResponse> {
    let mode = parse_resolution_mode(query.mode.as_deref())?;
    let records = parse_resolution_record_keys(query.records.as_deref(), mode)?;
    let selected_snapshot = resolve_exact_name_selected_snapshot(
        pool,
        namespace,
        ExactNameSnapshotSelector::from(&query),
        namespace == BASENAMES_NAMESPACE && mode.includes_verified(),
    )
    .await?;
    let row = load_name_current_for_selected_snapshot(pool, namespace, name, &selected_snapshot)
        .await?;

    let record_inventory_current = if mode.includes_declared() || mode.includes_verified() {
        load_resolution_record_inventory_current_for_snapshot(pool, &row, mode, &selected_snapshot)
            .await
            .map_err(snapshot_selection_api_error)?
    } else {
        None
    };

    let persisted_verified_outcome = if mode.includes_verified() {
        load_resolution_verified_outcome(
            pool,
            &row,
            &records,
            record_inventory_current.as_ref(),
            &selected_snapshot,
        )
        .await
        .map_err(snapshot_selection_api_error)?
    } else {
        None
    };

    let logical_name_id = row.logical_name_id.clone();
    build_resolution_response(
        row,
        mode,
        &records,
        record_inventory_current.as_ref(),
        persisted_verified_outcome.as_ref(),
        &selected_snapshot,
    )
    .map_err(|build_error| {
        error!(
            service = "api",
            namespace = %namespace,
            name = %name,
            logical_name_id = %logical_name_id,
            mode = ?mode,
            records = ?records,
            error = ?build_error,
            "failed to build resolution response"
        );
        ApiError::internal_error(format!(
            "failed to load resolution projection for name {namespace}/{name}"
        ))
    })
}

async fn load_resolution_record_inventory_current_for_snapshot(
    pool: &PgPool,
    row: &NameCurrentRow,
    mode: ResolutionMode,
    selected_snapshot: &SelectedSnapshot,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    match load_supported_record_inventory_current_for_snapshot(pool, row, selected_snapshot).await {
        Ok(Some(record_inventory_row)) => Ok(Some(record_inventory_row)),
        Ok(None) => load_record_inventory_current_matching_selected_snapshot(
            pool,
            row,
            selected_snapshot,
        )
        .await,
        Err(error) if error.kind() == SnapshotSelectionErrorKind::Stale => {
            if let Some(record_inventory_row) =
                load_explicit_unsupported_record_inventory_current(pool, row).await?
            {
                return Ok(Some(record_inventory_row));
            }

            if mode.includes_verified() && resolution_verified_support_boundary(row, None).is_none()
            {
                return Ok(None);
            }

            Err(error)
        }
        Err(error) => Err(error),
    }
}

async fn load_record_inventory_current_matching_selected_snapshot(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    let Some((resource_id, _)) = record_inventory_lookup_key(row) else {
        return Ok(None);
    };

    let rows = sqlx::query(
        r#"
        SELECT ric.record_version_boundary, ric.chain_positions
        FROM record_inventory_current ric
        JOIN resources resource
          ON resource.resource_id = ric.resource_id
        WHERE ric.resource_id = $1
          AND resource.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        "#,
    )
    .bind(resource_id)
    .fetch_all(pool)
    .await
    .map_err(|error| {
        SnapshotSelectionError::internal(format!(
            "failed to load record_inventory_current rows for resource_id {resource_id}: {error}"
        ))
    })?;

    let mut matching_boundaries = Vec::new();
    for candidate in rows {
        let chain_positions = candidate
            .try_get::<JsonValue, _>("chain_positions")
            .map_err(|error| {
                SnapshotSelectionError::internal(format!(
                    "record_inventory_current lookup for resource_id {resource_id} returned a row without chain_positions: {error}"
                ))
            })?;
        let projected = ChainPositions::from_value(&chain_positions).map_err(|error| {
            SnapshotSelectionError::stale(format!(
                "record_inventory_current projection has unusable chain_positions: {}",
                error.message()
            ))
        })?;
        if !selected_snapshot
            .chain_positions
            .equivalent_by_chain_id(&projected)
        {
            continue;
        }

        let record_version_boundary =
            candidate
                .try_get::<JsonValue, _>("record_version_boundary")
                .map_err(|error| {
                    SnapshotSelectionError::internal(format!(
                        "record_inventory_current lookup for resource_id {resource_id} returned a row without record_version_boundary: {error}"
                    ))
                })?;
        matching_boundaries.push(record_version_boundary);
    }

    let Some(record_version_boundary) = matching_boundaries.pop() else {
        return Ok(None);
    };
    if !matching_boundaries.is_empty() {
        return Err(SnapshotSelectionError::internal(format!(
            "record_inventory_current lookup for resource_id {resource_id} found multiple projection rows for the selected snapshot"
        )));
    }

    match load_record_inventory_current_for_snapshot(
        pool,
        resource_id,
        &record_version_boundary,
        &selected_snapshot.chain_positions,
    )
    .await?
    {
        SnapshotProjectionRead::Found(record_inventory_row) => Ok(Some(record_inventory_row)),
        SnapshotProjectionRead::NotFound => Err(SnapshotSelectionError::internal(format!(
            "matched record_inventory_current boundary for resource_id {resource_id} but the projection row was not loadable"
        ))),
    }
}

async fn load_explicit_unsupported_record_inventory_current(
    pool: &PgPool,
    row: &NameCurrentRow,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    let Some((resource_id, record_version_boundary)) = record_inventory_lookup_key(row) else {
        return Ok(None);
    };

    if let Some(record_inventory_row) =
        load_record_inventory_current(pool, resource_id, &record_version_boundary)
            .await
            .map_err(|error| {
                SnapshotSelectionError::internal(format!(
                    "failed to load record_inventory_current row for resource_id {resource_id}: {error}"
                ))
            })?
    {
        return Ok(record_inventory_current_has_explicit_unsupported_coverage(
            &record_inventory_row,
        )
        .then_some(record_inventory_row));
    }

    if record_version_boundary_has_pointer(&record_version_boundary) {
        return Ok(None);
    }

    let Some(persisted_boundary) =
        find_supported_record_inventory_boundary(pool, resource_id, &record_version_boundary)
            .await
            .map_err(|error| {
                SnapshotSelectionError::internal(format!(
                    "failed to locate supported record_inventory_current boundary for resource_id {resource_id}: {error}"
                ))
            })?
    else {
        return Ok(None);
    };

    let record_inventory_row =
        load_record_inventory_current(pool, resource_id, &persisted_boundary)
            .await
            .map_err(|error| {
                SnapshotSelectionError::internal(format!(
                    "failed to load record_inventory_current row for resource_id {resource_id}: {error}"
                ))
            })?
            .ok_or_else(|| {
                SnapshotSelectionError::internal(format!(
                    "matched record_inventory_current boundary for resource_id {resource_id} but the projection row was not loadable"
                ))
            })?;

    Ok(record_inventory_current_has_explicit_unsupported_coverage(&record_inventory_row)
        .then_some(record_inventory_row))
}

fn record_inventory_current_has_explicit_unsupported_coverage(
    row: &RecordInventoryCurrentRow,
) -> bool {
    string_field(provenance_field(&row.coverage, "unsupported_reason")).is_some()
}

fn infer_resolution_namespace(name: &str) -> &'static str {
    if name == "base.eth" {
        return "ens";
    }

    if name
        .strip_suffix(".base.eth")
        .is_some_and(|prefix| !prefix.is_empty())
    {
        "basenames"
    } else {
        "ens"
    }
}

async fn primary_names(
    Path(address): Path<String>,
    Query(query): Query<PrimaryNameQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<PrimaryNameResponse>> {
    let address = parse_primary_name_address(&address)?;
    let namespace = parse_primary_name_namespace(query.namespace.as_deref())?;
    let coin_type = parse_primary_name_coin_type(query.coin_type.as_deref())?;
    let mode = parse_resolution_mode(query.mode.as_deref())?;
    let lookup_state =
        load_primary_name_lookup_state(&state.pool, &address, &namespace, &coin_type, mode).await?;

    Ok(Json(build_primary_name_response(
        address,
        namespace,
        coin_type,
        mode,
        &lookup_state,
    )))
}

async fn resolver_current(
    Path((chain_id, resolver_address)): Path<(String, String)>,
    State(state): State<AppState>,
) -> ApiResult<Json<ResolverResponse>> {
    let normalized_address = normalize_address(&resolver_address);
    let row = load_resolver_current(&state.pool, &chain_id, &normalized_address)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                chain_id = %chain_id,
                resolver_address = %normalized_address,
                error = ?load_error,
                "failed to load resolver_current projection"
            );
            ApiError::internal_error(format!(
                "failed to load resolver projection for chain_id {chain_id} resolver_address {normalized_address}"
            ))
        })?;

    let Some(row) = row else {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!("resolver {normalized_address} was not found on chain {chain_id}"),
        });
    };

    Ok(Json(build_resolver_response(row)))
}

async fn address_names(
    Path(address): Path<String>,
    Query(query): Query<AddressNamesQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<AddressNamesResponse>> {
    let namespace = parse_address_names_namespace(query.namespace.as_deref())?;
    let relation = parse_address_name_relation(query.relation.as_deref())?;
    let dedupe_by = parse_address_names_dedupe_by(query.dedupe_by.as_deref())?;
    let include = parse_address_names_include(query.include.as_deref())?;
    let pagination = parse_pagination(query.cursor.as_deref(), query.page_size)?;
    let normalized_address = normalize_address(&address);

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

async fn name_children(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<ChildrenQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<ChildrenResponse>> {
    ensure_public_namespace(&namespace)?;

    let include_counts = parse_children_query(&query)?;
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

    let Some(_surface) = surface else {
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

    Ok(Json(build_children_response_from_summary(
        &storage_page.summary,
        &storage_page.rows,
        include_counts,
        page,
    )))
}

async fn address_history(
    Path(address): Path<String>,
    Query(query): Query<AddressHistoryQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<HistoryResponse>> {
    let namespace = parse_address_names_namespace(query.namespace.as_deref())?;
    let relation = parse_address_name_relation(query.relation.as_deref())?;
    let scope = parse_history_scope(query.scope.as_deref())?;
    let pagination = parse_pagination(query.cursor.as_deref(), query.page_size)?;
    let normalized_address = normalize_address(&address);

    let rows = load_address_history(
        &state.pool,
        &normalized_address,
        namespace.as_deref(),
        relation,
        scope,
        true,
    )
    .await
    .map_err(|load_error| {
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

    let mut filters = BTreeMap::new();
    filters.insert("scope".to_owned(), scope.as_str().to_owned());
    if let Some(namespace) = namespace.as_ref() {
        filters.insert("namespace".to_owned(), namespace.clone());
    }
    if let Some(relation) = relation {
        filters.insert("relation".to_owned(), relation.as_str().to_owned());
    }
    let page = paginate_window(
        &rows,
        &pagination,
        &CursorSpec {
            route: "/v1/history/addresses/{address}",
            anchor: normalized_address.clone(),
            sort: "chain_position_desc",
            filters,
        },
        history_cursor_fields,
    )?;

    Ok(Json(build_history_response(
        &rows,
        &rows[page.start..page.end],
        scope,
        page.page,
    )))
}

async fn name_history(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<HistoryQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<HistoryResponse>> {
    ensure_public_namespace(&namespace)?;

    let scope = parse_history_scope(query.scope.as_deref())?;
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
    let rows = load_name_history(&state.pool, &logical_name_id, &resource_ids, scope, true)
        .await
        .map_err(|load_error| {
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

    let mut filters = BTreeMap::new();
    filters.insert("scope".to_owned(), scope.as_str().to_owned());
    let page = paginate_window(
        &rows,
        &pagination,
        &CursorSpec {
            route: "/v1/history/names/{namespace}/{name}",
            anchor: logical_name_id,
            sort: "chain_position_desc",
            filters,
        },
        history_cursor_fields,
    )?;

    Ok(Json(build_history_response(
        &rows,
        &rows[page.start..page.end],
        scope,
        page.page,
    )))
}

async fn resource_history(
    Path(resource_id): Path<String>,
    Query(query): Query<HistoryQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<HistoryResponse>> {
    let scope = parse_history_scope(query.scope.as_deref())?;
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
    let rows = load_resource_history(&state.pool, resource_id, &logical_name_ids, scope, true)
        .await
        .map_err(|load_error| {
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

    let mut filters = BTreeMap::new();
    filters.insert("scope".to_owned(), scope.as_str().to_owned());
    let page = paginate_window(
        &rows,
        &pagination,
        &CursorSpec {
            route: "/v1/history/resources/{resource_id}",
            anchor: resource_id.to_string(),
            sort: "chain_position_desc",
            filters,
        },
        history_cursor_fields,
    )?;

    Ok(Json(build_history_response(
        &rows,
        &rows[page.start..page.end],
        scope,
        page.page,
    )))
}

async fn resource_permissions(
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
