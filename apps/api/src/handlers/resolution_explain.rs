pub(super) async fn explain_resolution_execution_current(
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
