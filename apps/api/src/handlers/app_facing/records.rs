use super::*;

pub(super) async fn name_records(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<NameRecordsQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<CompactNameRecordsResponse>> {
    let name = parse_exact_name_path_name(&namespace, &name)?;

    Ok(Json(
        compact_name_records_response_for_name(
            &state,
            &namespace,
            &name,
            query,
            CompactNameRecordsDefaultMode::Declared,
        )
        .await?,
    ))
}

include!("records_warmup.rs");

async fn compact_name_records_response_for_name(
    state: &AppState,
    namespace: &str,
    name: &str,
    query: NameRecordsQuery,
    default_mode: CompactNameRecordsDefaultMode,
) -> ApiResult<CompactNameRecordsResponse> {
    let (request, read) =
        load_compact_records_read(state, namespace, name, query, default_mode).await?;

    Ok(build_compact_name_records_response(
        &read.row,
        read.record_inventory_current.as_ref(),
        &read.records,
        &request,
        read.value_source,
        read.verified_outcome.as_ref(),
    ))
}
