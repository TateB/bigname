use super::*;

include!("resolution_explain.rs");

#[cfg(test)]
pub(crate) use super::record_inventory_chain_positions_match_selected_snapshot;

pub(super) async fn resolution_current(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<ResolutionQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<ResolutionResponse>> {
    ensure_public_namespace(&namespace)?;

    Ok(Json(
        resolution_response_for_name(&state, &namespace, &name, query).await?,
    ))
}

pub(super) async fn resolve_current(
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
        resolution_response_for_name(&state, namespace, &name, query).await?,
    ))
}

async fn resolution_response_for_name(
    state: &AppState,
    namespace: &str,
    name: &str,
    query: ResolutionQuery,
) -> ApiResult<ResolutionResponse> {
    let read = load_resolution_records_read(state, namespace, name, query).await?;
    let ResolutionRecordsRead {
        row,
        mode,
        records,
        selected_snapshot,
        record_inventory_current,
        persisted_verified_outcome,
    } = read;
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
