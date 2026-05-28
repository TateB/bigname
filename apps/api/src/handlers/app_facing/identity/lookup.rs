use axum::extract::rejection::JsonRejection;

use super::support::*;
use super::*;

struct ParsedNameLookup {
    index: usize,
    id: String,
    input_name: String,
    lookup: Option<IdentityNameLookup>,
    normalization: Option<NormalizationInfo>,
}

struct ParsedAddressLookup {
    index: usize,
    id: String,
    address: String,
    coin_type: u64,
    roles: IdentityRoles,
    page_size: u64,
    page_cursor: Option<String>,
}

pub(crate) async fn identity_lookup(
    State(state): State<AppState>,
    body: std::result::Result<Json<IdentityLookupInput>, JsonRejection>,
) -> ApiResult<Json<IdentityLookupResponse>> {
    let body = parse_identity_json_body(body)?;
    ensure_identity_batch_limit(body.inputs.len())?;
    let profile = parse_identity_lookup_profile(body.profile.as_deref())?;
    let namespace = parse_identity_lookup_namespace(body.namespace.as_deref())?;

    let mut name_inputs = Vec::new();
    let mut address_inputs = Vec::new();
    for (index, item) in body.inputs.iter().enumerate() {
        let kind = item.kind.trim().to_ascii_lowercase();
        if item.id.trim().is_empty() {
            return Err(ApiError {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_input",
                message: "identity lookup input id must not be empty".to_owned(),
            });
        }

        match kind.as_str() {
            "name" => {
                let input_name = item.name.clone().ok_or_else(|| ApiError {
                    status: StatusCode::BAD_REQUEST,
                    code: "invalid_input",
                    message: "name identity lookup inputs require name".to_owned(),
                })?;
                let (lookup, normalization) =
                    match parse_identity_name_lookup_with_namespace(&input_name, namespace) {
                        Ok(lookup) => {
                            let normalization = lookup
                                .corrected_input_normalization
                                .then(|| NormalizationInfo {
                                    changed: true,
                                    input_name: input_name.clone(),
                                    reason: "case_normalized".to_owned(),
                                });
                            (Some(lookup), normalization)
                        }
                        Err(_) => (
                            None,
                            Some(NormalizationInfo {
                                changed: false,
                                input_name: input_name.clone(),
                                reason: "invalid_normalized_name".to_owned(),
                            }),
                        ),
                    };
                name_inputs.push(ParsedNameLookup {
                    index,
                    id: item.id.clone(),
                    input_name,
                    lookup,
                    normalization,
                });
            }
            "address" => {
                if namespace.is_some() {
                    return Err(ApiError {
                        status: StatusCode::BAD_REQUEST,
                        code: "invalid_input",
                        message: "explicit namespace filters are not supported for address identity lookup inputs; omit namespace or use public".to_owned(),
                    });
                }
                let input_address = item.address.as_deref().ok_or_else(|| ApiError {
                    status: StatusCode::BAD_REQUEST,
                    code: "invalid_input",
                    message: "address identity lookup inputs require address".to_owned(),
                })?;
                let address = parse_primary_name_address(input_address)?;
                let coin_type = item.coin_type.ok_or_else(|| ApiError {
                    status: StatusCode::BAD_REQUEST,
                    code: "invalid_input",
                    message: "coin_type is required for address identity lookup".to_owned(),
                })?;
                let roles = parse_identity_lookup_roles(item.roles.as_deref())?;
                let pagination = parse_identity_pagination_with_default(
                    item.cursor.as_deref(),
                    item.page_size,
                    1,
                )?;
                let cursor_spec = reverse_identity_cursor_spec(&address, coin_type, roles);
                let page_cursor = native_reverse_identity_cursor(&pagination, &cursor_spec)?;
                address_inputs.push(ParsedAddressLookup {
                    index,
                    id: item.id.clone(),
                    address,
                    coin_type,
                    roles,
                    page_size: pagination.page_size,
                    page_cursor,
                });
            }
            _ => {
                return Err(ApiError {
                    status: StatusCode::BAD_REQUEST,
                    code: "invalid_input",
                    message: "identity lookup kind must be one of: name, address".to_owned(),
                });
            }
        }
    }

    let mut results = vec![None; body.inputs.len()];
    render_name_lookup_results(&state, profile, &name_inputs, &mut results).await?;
    match profile {
        IdentityLookupProfile::Feed => {
            render_feed_lookup_results(&state, &address_inputs, &mut results).await?;
        }
        IdentityLookupProfile::Detail | IdentityLookupProfile::Shadow => {
            render_detail_lookup_results(&state, &address_inputs, &mut results).await?;
        }
    }

    let results = results
        .into_iter()
        .map(|result| result.expect("every parsed identity lookup input must render a result"))
        .collect();
    Ok(Json(IdentityLookupResponse { results }))
}

async fn render_name_lookup_results(
    state: &AppState,
    profile: IdentityLookupProfile,
    inputs: &[ParsedNameLookup],
    results: &mut [Option<IdentityLookupResult>],
) -> ApiResult<()> {
    let logical_name_ids = inputs
        .iter()
        .filter_map(|input| {
            input
                .lookup
                .as_ref()
                .map(|lookup| lookup.logical_name_id.clone())
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let records = load_native_name_records(state, profile, &logical_name_ids).await?;

    for input in inputs {
        let (status, record) = match input.lookup.as_ref() {
            None => ("unnormalizable_input".to_owned(), None),
            Some(lookup) => match records.get(&lookup.logical_name_id) {
                Some(record) => {
                    let record = match profile {
                        IdentityLookupProfile::Feed => {
                            build_native_identity_name_feed_record_response(record)
                        }
                        IdentityLookupProfile::Detail | IdentityLookupProfile::Shadow => {
                            build_native_identity_name_record_response(record)
                        }
                    };
                    (record.status.clone(), Some(record))
                }
                None => ("not_found".to_owned(), None),
            },
        };
        results[input.index] = Some(IdentityLookupResult {
            id: input.id.clone(),
            kind: "name".to_owned(),
            status,
            input: IdentityLookupResultInput {
                name: Some(input.input_name.clone()),
                address: None,
                coin_type: None,
                roles: None,
            },
            normalization: input.normalization.clone(),
            record: Some(record),
            records: None,
            page: None,
        });
    }

    Ok(())
}

async fn load_native_name_records(
    state: &AppState,
    profile: IdentityLookupProfile,
    logical_name_ids: &[String],
) -> ApiResult<BTreeMap<String, bigname_storage::IdentityNameRecordRow>> {
    let records = match profile {
        IdentityLookupProfile::Feed => {
            bigname_storage::load_identity_name_feed_records_by_names(
                &state.pool,
                logical_name_ids,
            )
            .await
        }
        IdentityLookupProfile::Detail | IdentityLookupProfile::Shadow => {
            bigname_storage::load_identity_records_by_names(&state.pool, logical_name_ids).await
        }
    }
    .map_err(|load_error| {
        error!(
            service = "api",
            input_count = logical_name_ids.len(),
            profile = ?profile,
            error = ?load_error,
            "failed to load native identity name lookup"
        );
        ApiError::internal_error("failed to load identity name lookup")
    })?;

    Ok(records
        .into_iter()
        .map(|record| (record.row.logical_name_id.clone(), record))
        .collect())
}

async fn render_feed_lookup_results(
    state: &AppState,
    inputs: &[ParsedAddressLookup],
    results: &mut [Option<IdentityLookupResult>],
) -> ApiResult<()> {
    let storage_inputs = inputs
        .iter()
        .map(|input| {
            (
                feed_lookup_key(input),
                bigname_storage::ReverseIdentityFeedInput {
                    address: input.address.clone(),
                    coin_type: input.coin_type.to_string(),
                    roles: input.roles.storage_roles(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect::<Vec<_>>();
    let groups = bigname_storage::load_reverse_identity_feed_records(&state.pool, &storage_inputs)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                input_count = inputs.len(),
                error = ?load_error,
                "failed to load native identity feed lookup"
            );
            ApiError::internal_error("failed to load identity feed lookup")
        })?
        .into_iter()
        .map(|group| (feed_group_key(&group), group))
        .collect::<BTreeMap<_, _>>();

    for input in inputs {
        let group = groups.get(&feed_lookup_key(input));
        let records = group
            .and_then(|group| group.record.as_ref())
            .map(build_native_identity_feed_record_response)
            .into_iter()
            .collect::<Vec<_>>();
        let status = native_address_lookup_status(&records);
        results[input.index] = Some(address_lookup_result(
            input,
            records,
            IdentityLookupPageResponse {
                next_cursor: None,
                total_count: Some(group.map(|group| group.total_count).unwrap_or(0)),
                has_more: false,
            },
            status,
        ));
    }

    Ok(())
}

async fn render_detail_lookup_results(
    state: &AppState,
    inputs: &[ParsedAddressLookup],
    results: &mut [Option<IdentityLookupResult>],
) -> ApiResult<()> {
    let requests = inputs
        .iter()
        .map(|input| ReverseIdentityRequestKey {
            address: input.address.clone(),
            coin_type: input.coin_type,
            roles: input.roles,
            page_size: input.page_size,
            page_cursor: input.page_cursor.clone(),
        })
        .collect::<Vec<_>>();
    let storage_inputs = deduped_reverse_storage_inputs(&requests)?;
    let groups = bigname_storage::load_reverse_identity_records(&state.pool, &storage_inputs)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                input_count = inputs.len(),
                error = ?load_error,
                "failed to load native identity detail lookup"
            );
            ApiError::internal_error("failed to load identity detail lookup")
        })?
        .into_iter()
        .map(|group| (reverse_identity_group_key(&group), group))
        .collect::<BTreeMap<_, _>>();

    for input in inputs {
        let request_key = ReverseIdentityRequestKey {
            address: input.address.clone(),
            coin_type: input.coin_type,
            roles: input.roles,
            page_size: input.page_size,
            page_cursor: input.page_cursor.clone(),
        };
        let group = groups
            .get(&ReverseIdentityStorageKey::from(&request_key))
            .cloned()
            .unwrap_or_else(|| empty_reverse_identity_group_from_native(input));
        let (records, page) = native_reverse_identity_page(
            group.entries,
            &input.address,
            input.coin_type,
            input.roles,
            group.total_count,
            group.has_more,
        );
        let status = native_address_lookup_status(&records);
        results[input.index] = Some(address_lookup_result(input, records, page, status));
    }

    Ok(())
}

fn feed_lookup_key(input: &ParsedAddressLookup) -> (String, u64, IdentityRoles) {
    (input.address.clone(), input.coin_type, input.roles)
}

fn feed_group_key(
    group: &bigname_storage::ReverseIdentityFeedGroup,
) -> (String, u64, IdentityRoles) {
    (
        group.input.address.clone(),
        group.input.coin_type.parse::<u64>().unwrap_or_default(),
        IdentityRoles::from_storage(group.input.roles),
    )
}

fn native_reverse_identity_page(
    mut entries: Vec<bigname_storage::ReverseIdentityRecordRow>,
    address: &str,
    coin_type: u64,
    roles: IdentityRoles,
    total_count: Option<u64>,
    has_more: bool,
) -> (Vec<NativeIdentityRecordResponse>, IdentityLookupPageResponse) {
    entries.sort_by(reverse_identity_sort);
    let cursor_spec = reverse_identity_cursor_spec(address, coin_type, roles);
    let next_cursor = if has_more {
        entries
            .last()
            .map(reverse_identity_cursor_item)
            .map(|item| encode_cursor(&cursor_spec.envelope(item)))
    } else {
        None
    };
    let records = entries
        .iter()
        .map(build_native_reverse_identity_record_response)
        .collect::<Vec<_>>();
    (
        records,
        IdentityLookupPageResponse {
            next_cursor,
            total_count,
            has_more,
        },
    )
}

fn native_reverse_identity_cursor(
    pagination: &PaginationRequest,
    cursor_spec: &CursorSpec,
) -> ApiResult<Option<String>> {
    let Some(cursor) = pagination.cursor.as_deref() else {
        return Ok(None);
    };

    let decoded = decode_cursor(cursor)?;
    validate_cursor(cursor_spec, &decoded)?;
    Ok(Some(encode_cursor(&decoded)))
}

fn empty_reverse_identity_group_from_native(
    request: &ParsedAddressLookup,
) -> bigname_storage::ReverseIdentityGroup {
    bigname_storage::ReverseIdentityGroup {
        input: bigname_storage::ReverseIdentityStorageInput {
            address: request.address.clone(),
            coin_type: request.coin_type.to_string(),
            roles: request.roles.storage_roles(),
            page_size: request.page_size as i64,
            cursor: None,
        },
        entries: Vec::new(),
        total_count: Some(0),
        has_more: false,
    }
}

fn address_lookup_result(
    input: &ParsedAddressLookup,
    records: Vec<NativeIdentityRecordResponse>,
    page: IdentityLookupPageResponse,
    status: String,
) -> IdentityLookupResult {
    IdentityLookupResult {
        id: input.id.clone(),
        kind: "address".to_owned(),
        status,
        input: IdentityLookupResultInput {
            name: None,
            address: Some(input.address.clone()),
            coin_type: Some(input.coin_type),
            roles: Some(native_identity_roles(input.roles)),
        },
        normalization: None,
        record: None,
        records: Some(records),
        page: Some(page),
    }
}

fn native_address_lookup_status(records: &[NativeIdentityRecordResponse]) -> String {
    if records.iter().any(|record| record.status == "stale") {
        return "stale".to_owned();
    }
    if !records.is_empty() && records.iter().all(|record| record.status == "unsupported") {
        return "unsupported".to_owned();
    }
    "success".to_owned()
}
