mod support;

use axum::extract::rejection::JsonRejection;

use super::*;
use support::*;

pub(super) async fn identity_name(
    Path(name): Path<String>,
    State(state): State<AppState>,
) -> ApiResult<Json<IdentityNameResponse>> {
    let lookup = match parse_identity_name_lookup(&name) {
        Ok(lookup) => lookup,
        Err(error) => {
            tracing::debug!(
                service = "api",
                name = %name,
                error = %error.message,
                "identity forward input could not be normalized"
            );
            return Ok(Json(IdentityNameResponse {
                status: "unnormalizable_input".to_owned(),
                record: None,
            }));
        }
    };
    let records = bigname_storage::load_identity_records_by_names(
        &state.pool,
        std::slice::from_ref(&lookup.logical_name_id),
    )
    .await
    .map_err(|load_error| {
        error!(
            service = "api",
            name = %name,
            logical_name_id = %lookup.logical_name_id,
            error = ?load_error,
            "failed to load identity forward record"
        );
        ApiError::internal_error("failed to load identity name record")
    })?;

    Ok(Json(build_identity_name_response_with_normalization(
        records.first(),
        lookup.corrected_input_normalization,
    )))
}

pub(super) async fn identity_names_batch(
    State(state): State<AppState>,
    body: std::result::Result<Json<ForwardIdentityBatchInput>, JsonRejection>,
) -> ApiResult<Json<ForwardIdentityBatchResponse>> {
    let body = parse_identity_json_body(body)?;
    ensure_identity_batch_limit(body.names.len())?;

    let lookups = body
        .names
        .iter()
        .map(|name| parse_identity_name_lookup(name))
        .collect::<Vec<_>>();
    let logical_name_ids = lookups
        .iter()
        .filter_map(|lookup| lookup.as_ref().ok().map(|lookup| lookup.logical_name_id.clone()))
        .collect::<Vec<_>>();
    let records = bigname_storage::load_identity_records_by_names(&state.pool, &logical_name_ids)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                input_count = body.names.len(),
                error = ?load_error,
                "failed to load identity forward batch"
            );
            ApiError::internal_error("failed to load identity name batch")
        })?
        .into_iter()
        .map(|record| (record.row.logical_name_id.clone(), record))
        .collect::<BTreeMap<_, _>>();

    let results = body
        .names
        .iter()
        .zip(lookups.iter())
        .map(|(name, lookup)| {
            let input = ForwardIdentityBatchResultInput { name: name.clone() };
            let lookup = match lookup {
                Ok(lookup) => lookup,
                Err(_) => {
                    return ForwardIdentityBatchResult {
                        input,
                        record: None,
                        status: "unnormalizable_input".to_owned(),
                    };
                }
            };

            match records.get(&lookup.logical_name_id) {
                Some(record) => {
                    let record = build_name_record_response_with_normalization(
                        record,
                        lookup.corrected_input_normalization,
                    );
                    ForwardIdentityBatchResult {
                        input,
                        status: record.status.clone(),
                        record: Some(record),
                    }
                }
                None => ForwardIdentityBatchResult {
                    input,
                    record: None,
                    status: "not_found".to_owned(),
                },
            }
        })
        .collect();

    Ok(Json(ForwardIdentityBatchResponse { results }))
}

pub(super) async fn identity_address_names(
    Path(address): Path<String>,
    Query(query): Query<ReverseIdentityQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<ReverseNamesResponse>> {
    let address = parse_primary_name_address(&address)?;
    let coin_type = parse_identity_coin_type(query.coin_type.as_deref())?;
    let roles = parse_identity_roles(query.roles.as_deref())?;
    let pagination = parse_identity_pagination(query.page_cursor.as_deref(), query.page_size)?;
    let cursor_spec = reverse_identity_cursor_spec(&address, coin_type, roles);
    let storage_input = reverse_storage_input(&address, coin_type, roles, &pagination, &cursor_spec)?;
    let fallback_input = storage_input.clone();

    let group = bigname_storage::load_reverse_identity_records(&state.pool, &[storage_input])
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                address = %address,
                coin_type = coin_type,
                roles = roles.as_str(),
                error = ?load_error,
                "failed to load reverse identity records"
            );
            ApiError::internal_error("failed to load reverse identity records")
        })?
        .into_iter()
        .next()
        .unwrap_or_else(|| bigname_storage::ReverseIdentityGroup {
            input: fallback_input,
            entries: Vec::new(),
            total_count: Some(0),
            has_more: false,
        });
    let (records, pagination) = render_reverse_identity_page(
        group.entries,
        &address,
        coin_type,
        roles,
        group.total_count,
        group.has_more,
    )?;

    Ok(Json(ReverseNamesResponse {
        input: ReverseNamesInputResponse {
            address,
            coin_type,
            roles: roles.as_str().to_owned(),
        },
        records,
        pagination,
    }))
}

pub(super) async fn identity_address_names_batch(
    State(state): State<AppState>,
    body: std::result::Result<Json<ReverseIdentityBatchInput>, JsonRejection>,
) -> ApiResult<Json<ReverseIdentityBatchResponse>> {
    let body = parse_identity_json_body(body)?;
    ensure_identity_batch_limit(body.inputs.len())?;

    let requests = body
        .inputs
        .iter()
        .map(parse_reverse_batch_item)
        .collect::<ApiResult<Vec<_>>>()?;
    let storage_inputs = deduped_reverse_storage_inputs(&requests)?;
    let groups = bigname_storage::load_reverse_identity_records(&state.pool, &storage_inputs)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                input_count = body.inputs.len(),
                error = ?load_error,
                "failed to load reverse identity batch"
            );
            ApiError::internal_error("failed to load reverse identity batch")
        })?
        .into_iter()
        .map(|group| (reverse_identity_group_key(&group), group))
        .collect::<BTreeMap<_, _>>();

    let mut results = Vec::with_capacity(requests.len());
    for request in requests {
        let group = groups
            .get(&ReverseIdentityStorageKey::from(&request))
            .cloned()
            .unwrap_or_else(|| empty_reverse_identity_group(&request));
        let (records, pagination) = render_reverse_identity_page(
            group.entries,
            &request.address,
            request.coin_type,
            request.roles,
            group.total_count,
            group.has_more,
        )?;
        let status = reverse_batch_status(&records);
        results.push(ReverseIdentityBatchResult {
            input: ReverseNamesInputResponse {
                address: request.address,
                coin_type: request.coin_type,
                roles: request.roles.as_str().to_owned(),
            },
            records,
            pagination,
            status,
        });
    }

    Ok(Json(ReverseIdentityBatchResponse { results }))
}

pub(super) async fn indexing_status(
    State(state): State<AppState>,
) -> ApiResult<Json<IndexingStatusResponse>> {
    let read = bigname_storage::load_indexing_status(&state.pool)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                error = ?load_error,
                "failed to load indexing status"
            );
            ApiError::internal_error("failed to load indexing status")
        })?;

    Ok(Json(build_indexing_status_response(&read)))
}
