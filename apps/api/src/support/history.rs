use super::*;

pub(super) async fn resource_ids_for_name(
    pool: &PgPool,
    logical_name_id: &str,
) -> ApiResult<Vec<Uuid>> {
    let bindings = load_surface_bindings_by_logical_name_id(pool, logical_name_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                logical_name_id = %logical_name_id,
                error = ?load_error,
                "failed to load surface bindings for name history"
            );
            ApiError::internal_error(format!(
                "failed to load history bindings for logical name {logical_name_id}"
            ))
        })?;

    Ok(bindings
        .into_iter()
        .map(|binding| binding.resource_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

pub(super) async fn logical_name_ids_for_resource(
    pool: &PgPool,
    resource_id: Uuid,
) -> ApiResult<Vec<String>> {
    let bindings = load_surface_bindings_by_resource_id(pool, resource_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                resource_id = %resource_id,
                error = ?load_error,
                "failed to load surface bindings for resource history"
            );
            ApiError::internal_error(format!(
                "failed to load history bindings for resource {resource_id}"
            ))
        })?;

    Ok(bindings
        .into_iter()
        .map(|binding| binding.logical_name_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

pub(super) fn chain_position_key(chain_id: &str) -> String {
    match chain_id {
        "ethereum-mainnet" => "ethereum".to_owned(),
        "base-mainnet" => "base".to_owned(),
        other => other.to_owned(),
    }
}

pub(super) fn compact_history_summary_mode(meta: MetaMode) -> HistorySummaryMode {
    match meta {
        MetaMode::None => HistorySummaryMode::None,
        MetaMode::Summary => HistorySummaryMode::Count,
        MetaMode::Full => HistorySummaryMode::Full,
    }
}

pub(super) fn history_route_summary_mode(view: ResponseView, meta: MetaMode) -> HistorySummaryMode {
    match view {
        ResponseView::Full => HistorySummaryMode::Full,
        ResponseView::Compact => compact_history_summary_mode(meta),
    }
}
