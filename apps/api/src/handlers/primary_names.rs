use super::*;

pub(super) async fn primary_names(
    Path(address): Path<String>,
    Query(query): Query<PrimaryNameQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<PrimaryNameResponse>> {
    let address = parse_primary_name_address(&address)?;
    let namespace = parse_primary_name_namespace(query.namespace.as_deref().or(Some("ens")))?;
    let coin_type = parse_primary_name_coin_type(query.coin_type.as_deref().or(Some("60")))?;
    let mode = parse_resolution_mode(query.mode.as_deref())?;
    let mut lookup_state =
        load_primary_name_lookup_state(&state.pool, &address, &namespace, &coin_type, mode).await?;
    if (mode.includes_declared() || mode.includes_verified())
        && matches!(lookup_state.tuple_state, PrimaryNameTupleState::TupleMissing)
    {
        lookup_state.on_demand_claim =
            load_on_demand_primary_name_claim(&state, &address, &namespace, &coin_type).await?;
    }
    if mode.includes_verified()
        && matches!(lookup_state.tuple_state, PrimaryNameTupleState::TupleMissing)
        && let OnDemandPrimaryNameClaimState::Found(claim) = &lookup_state.on_demand_claim
    {
        lookup_state.on_demand_verified = load_on_demand_primary_name_verification(
            &state, &address, &namespace, &coin_type, claim,
        )
        .await?;
    }

    Ok(Json(build_primary_name_response(
        address,
        namespace,
        coin_type,
        mode,
        &lookup_state,
    )))
}
