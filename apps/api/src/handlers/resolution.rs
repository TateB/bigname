use super::*;

include!("resolution_explain.rs");

#[cfg(test)]
pub(crate) use super::record_inventory_chain_positions_match_selected_snapshot;

pub(super) async fn name_profile(
    Path(name): Path<String>,
    Query(query): Query<NameProfileQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<ResolutionResponse>> {
    let parsed = normalize_inferred_route_name(&name).map_err(route_name_normalization_api_error)?;
    let namespace = parsed.namespace;
    let normalized_name = parsed.normalized_name;

    let meta = parse_meta_mode(query.meta.as_deref(), MetaMode::Summary)?;
    let include_provenance = meta == MetaMode::Full;
    let read = load_name_profile_records_read(&state, namespace, &normalized_name, query).await?;
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
        include_provenance,
    )
    .map(Json)
    .map_err(|build_error| {
        error!(
            service = "api",
            namespace = %namespace,
            name = %normalized_name,
            logical_name_id = %logical_name_id,
            mode = ?mode,
            records = ?records,
            error = ?build_error,
            "failed to build name profile response"
        );
        ApiError::internal_error(format!(
            "failed to load profile projection for name {namespace}/{normalized_name}"
        ))
    })
}
