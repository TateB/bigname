use super::*;

pub(super) async fn name_current(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<ExactNameSnapshotQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<NameResponse>> {
    ensure_public_namespace(&namespace)?;

    let ExactNameInventoryRead {
        row,
        record_inventory_current,
        selected_snapshot,
    } = load_exact_name_inventory_read(
        &state.pool,
        &namespace,
        &name,
        ExactNameSnapshotSelector::from(&query),
    )
    .await?;

    Ok(Json(build_name_response(
        row,
        record_inventory_current.as_ref(),
        &selected_snapshot,
    )))
}

pub(super) async fn coverage_current(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<ExactNameSnapshotQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<NameResponse>> {
    ensure_public_namespace(&namespace)?;

    let ExactNameRead {
        row,
        selected_snapshot,
    } = load_exact_name_read(
        &state.pool,
        &namespace,
        &name,
        ExactNameSnapshotSelector::from(&query),
        "current",
    )
    .await?;

    Ok(Json(build_name_coverage_response(row, &selected_snapshot)))
}

pub(super) async fn explain_surface_binding_current(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<ExactNameSnapshotQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<NameResponse>> {
    ensure_public_namespace(&namespace)?;

    let ExactNameRead {
        row,
        selected_snapshot,
    } = load_exact_name_read(
        &state.pool,
        &namespace,
        &name,
        ExactNameSnapshotSelector::from(&query),
        "surface-binding explain",
    )
    .await?;

    Ok(Json(build_name_surface_binding_explain_response(
        row,
        &selected_snapshot,
    )))
}

pub(super) async fn explain_authority_control_current(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<ExactNameSnapshotQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<NameResponse>> {
    ensure_public_namespace(&namespace)?;

    let ExactNameRead {
        row,
        selected_snapshot,
    } = load_exact_name_read(
        &state.pool,
        &namespace,
        &name,
        ExactNameSnapshotSelector::from(&query),
        "authority-control explain",
    )
    .await?;

    Ok(Json(build_name_authority_control_explain_response(
        row,
        &selected_snapshot,
    )))
}
