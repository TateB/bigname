use super::*;

pub(super) async fn resolver_overview(
    Path((chain_id, resolver_address)): Path<(String, String)>,
    Query(query): Query<ResolverOverviewQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<CompactResolverOverviewResponse>> {
    parse_compact_only_response_view(
        query.view.as_deref(),
        "view=full is not supported for compact resolver overview",
    )?;
    let meta = parse_meta_mode(query.meta.as_deref(), MetaMode::Summary)?;
    let include = parse_resolver_overview_include(query.include.as_deref())?;
    let normalized_address = normalize_address(&resolver_address);
    let row = load_resolver_current(&state.pool, &chain_id, &normalized_address)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                chain_id = %chain_id,
                resolver_address = %normalized_address,
                error = ?load_error,
                "failed to load resolver_current projection for compact overview"
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

    Ok(Json(build_compact_resolver_overview_response(
        row, include, meta,
    )))
}
