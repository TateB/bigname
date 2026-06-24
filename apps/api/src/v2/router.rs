use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use bigname_storage::{
    RecordInventoryCurrentRow, SelectedSnapshot, SnapshotSelectionErrorKind, SnapshotSelectionScope,
};

use crate::{
    ApiError, AppState, ExactNameSnapshotSelector,
    handler_resolution_on_demand::load_or_execute_resolution_verified_outcome,
    load_name_current_for_selected_snapshot, load_supported_record_inventory_current_for_snapshot,
    map_internal_api_error, normalize_inferred_route_name, parse_resolution_record_key,
    snapshot_selection_api_error,
};

use super::{
    Envelope, MAX_PAGE_SIZE, Meta, NameRecord, NameRecords, Page, QueryParams, RequestSource,
    Source, Status, Subname, V2Error, V2Result, VerifiedRecordLookup, as_of_meta,
    build_auto_name_records, build_indexed_name_records, build_name_record, build_subname,
    build_verified_name_records, decode, default_requested_records, encode, encode_at_token,
    indexed_records_requiring_verified_fallback, resolve_v2_snapshot, subname_cursor_payload,
    subname_storage_cursor, validate_product_record,
};

const MAX_RECORD_KEYS: usize = MAX_PAGE_SIZE as usize;

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/v2/lookup", post(not_implemented))
        .route("/v2/status", get(not_implemented))
        .route("/v2/names/{name}", get(get_name_record))
        .route("/v2/names/{name}/records", get(get_name_records))
        .route("/v2/names/{name}/subnames", get(get_subnames))
        .route("/v2/names/{name}/history", get(not_implemented))
        .route("/v2/permissions", get(not_implemented))
        .route("/v2/addresses/{address}/names", get(not_implemented))
        .route("/v2/addresses/{address}/primary-name", get(not_implemented))
        .route("/v2/addresses/{address}/history", get(not_implemented))
        .route("/v2/search", get(not_implemented))
        .route("/v2/events", get(not_implemented))
        .route("/v2/resolvers/{chain_id}/{address}", get(not_implemented))
        .route("/v2/namespaces/{namespace}", get(not_implemented))
        .route(
            "/v2/diagnostics/names/{name}/coverage",
            get(not_implemented),
        )
        .route("/v2/diagnostics/names/{name}/binding", get(not_implemented))
        .route(
            "/v2/diagnostics/names/{name}/authority",
            get(not_implemented),
        )
        .route("/v2/diagnostics/names/{name}/records", get(not_implemented))
        .route(
            "/v2/diagnostics/names/{name}/execution",
            get(not_implemented),
        )
        .route(
            "/v2/diagnostics/namespaces/{namespace}/manifests",
            get(not_implemented),
        )
        .route("/v2/diagnostics/events", get(not_implemented))
}

async fn not_implemented() -> StatusCode {
    StatusCode::NOT_IMPLEMENTED
}

async fn get_subnames(
    Path(input_name): Path<String>,
    params: QueryParams,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<Subname>>>> {
    let normalized = normalize_inferred_route_name(&input_name)
        .map_err(|error| V2Error::invalid_input(error.message))?;
    let namespace = params
        .namespace
        .clone()
        .unwrap_or_else(|| normalized.namespace.to_owned());
    let include_counts = subnames_include_counts(&params.include)?;

    let scope = v2_exact_name_snapshot_scope(&state, &namespace).await?;
    let selected_snapshot =
        resolve_v2_snapshot(&state.pool, &scope, params.at.as_ref(), params.finality).await?;
    let parent = load_name_current_for_selected_snapshot(
        &state.pool,
        &namespace,
        &normalized.normalized_name,
        &selected_snapshot,
    )
    .await
    .map_err(|error| {
        api_error_to_v2(map_internal_api_error(
            error,
            format!(
                "failed to load subnames for {}/{}",
                namespace, normalized.normalized_name
            ),
        ))
    })?;

    let snapshot_token = encode_at_token(&selected_snapshot);
    let storage_cursor = params
        .cursor
        .as_deref()
        .map(|cursor| {
            let payload = decode(cursor)?;
            subname_storage_cursor(
                &payload,
                &namespace,
                &parent.logical_name_id,
                &snapshot_token,
            )
        })
        .transpose()?;

    let storage_page = bigname_storage::load_children_current_page(
        &state.pool,
        &parent.logical_name_id,
        storage_cursor.as_ref(),
        params.page_size,
    )
    .await
    .map_err(|_| {
        V2Error::internal_error(format!(
            "failed to load subnames for {}/{}",
            namespace, normalized.normalized_name
        ))
    })?;

    let child_logical_name_ids = storage_page
        .rows
        .iter()
        .map(|row| row.child_logical_name_id.clone())
        .collect::<Vec<_>>();
    let child_name_rows = bigname_storage::load_name_current_by_logical_name_ids(
        &state.pool,
        &child_logical_name_ids,
    )
    .await
    .map_err(|_| {
        V2Error::internal_error(format!(
            "failed to load subname registration summaries for {}/{}",
            namespace, normalized.normalized_name
        ))
    })?;
    let child_summaries = if include_counts {
        bigname_storage::load_children_current_summaries(&state.pool, &child_logical_name_ids)
            .await
            .map_err(|_| {
                V2Error::internal_error(format!(
                    "failed to load subname counts for {}/{}",
                    namespace, normalized.normalized_name
                ))
            })?
            .into_iter()
            .map(|summary| (summary.parent_logical_name_id.clone(), summary))
            .collect()
    } else {
        std::collections::BTreeMap::new()
    };

    let next_cursor = storage_page.next_cursor.as_ref().map(|cursor| {
        encode(&subname_cursor_payload(
            cursor,
            &namespace,
            &parent.logical_name_id,
            &snapshot_token,
        ))
    });
    let has_more = next_cursor.is_some();
    let data = storage_page
        .rows
        .iter()
        .map(|row| {
            build_subname(
                row,
                child_name_rows.get(&row.child_logical_name_id),
                child_summaries.get(&row.child_logical_name_id),
                include_counts,
            )
        })
        .collect();
    let meta = Meta {
        as_of: Some(as_of_meta(&selected_snapshot)?),
        ..Meta::default()
    };

    Ok(Json(Envelope {
        data,
        page: Some(Page {
            cursor: params.cursor.clone(),
            next_cursor,
            page_size: params.page_size,
            total_count: None,
            has_more,
        }),
        meta,
    }))
}

async fn get_name_records(
    Path(input_name): Path<String>,
    params: QueryParams,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<NameRecords>>> {
    let normalized = normalize_inferred_route_name(&input_name)
        .map_err(|error| V2Error::invalid_input(error.message))?;
    let namespace = params
        .namespace
        .clone()
        .unwrap_or_else(|| normalized.namespace.to_owned());
    let requested_records = parse_record_keys(params.keys.as_deref())?;
    let include_inventory = records_include_inventory(&params.include)?;

    let scope = v2_exact_name_snapshot_scope(&state, &namespace).await?;
    let selected_snapshot =
        resolve_v2_snapshot(&state.pool, &scope, params.at.as_ref(), params.finality).await?;
    let row = load_name_current_for_selected_snapshot(
        &state.pool,
        &namespace,
        &normalized.normalized_name,
        &selected_snapshot,
    )
    .await
    .map_err(|error| {
        api_error_to_v2(map_internal_api_error(
            error,
            format!(
                "failed to load name records for {}/{}",
                namespace, normalized.normalized_name
            ),
        ))
    })?;

    let record_inventory =
        load_supported_record_inventory_current_for_snapshot(&state.pool, &row, &selected_snapshot)
            .await
            .map_err(|error| api_error_to_v2(snapshot_selection_api_error(error)))?;
    let default_records;
    let requested_records = match requested_records.as_deref() {
        Some(records) => Some(records),
        None if params.source == RequestSource::Verified => {
            default_records = default_requested_records(record_inventory.as_ref());
            Some(default_records.as_slice())
        }
        None => None,
    };

    let (route_source, data) = match params.source {
        RequestSource::Indexed => (
            Source::Indexed,
            build_indexed_name_records(
                &row,
                record_inventory.as_ref(),
                requested_records,
                include_inventory,
            ),
        ),
        RequestSource::Verified => {
            let verified_lookup = load_verified_record_lookup(
                &state,
                &row,
                record_inventory.as_ref(),
                requested_records.unwrap_or_default(),
                &selected_snapshot,
            )
            .await?;
            (
                Source::Verified,
                build_verified_name_records(
                    &row,
                    record_inventory.as_ref(),
                    requested_records,
                    verified_lookup,
                    include_inventory,
                )?,
            )
        }
        RequestSource::Auto => {
            let records = requested_records.unwrap_or_default();
            if records.is_empty() {
                (
                    Source::Indexed,
                    build_indexed_name_records(
                        &row,
                        record_inventory.as_ref(),
                        requested_records,
                        include_inventory,
                    ),
                )
            } else {
                let fallback_records = indexed_records_requiring_verified_fallback(
                    &row,
                    record_inventory.as_ref(),
                    records,
                );
                let verified_lookup = load_verified_record_lookup(
                    &state,
                    &row,
                    record_inventory.as_ref(),
                    &fallback_records,
                    &selected_snapshot,
                )
                .await?;
                build_auto_name_records(
                    &row,
                    record_inventory.as_ref(),
                    records,
                    verified_lookup,
                    include_inventory,
                )?
            }
        }
    };

    let meta = Meta {
        as_of: Some(as_of_meta(&selected_snapshot)?),
        source: Some(route_source),
        ..Meta::default()
    };

    Ok(Json(Envelope {
        data,
        page: None,
        meta,
    }))
}

async fn get_name_record(
    Path(input_name): Path<String>,
    params: QueryParams,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<NameRecord>>> {
    let normalized = normalize_inferred_route_name(&input_name)
        .map_err(|error| V2Error::invalid_input(error.message))?;
    let namespace = params
        .namespace
        .clone()
        .unwrap_or_else(|| normalized.namespace.to_owned());
    let route_source = route_source(params.source)?;

    let scope = v2_exact_name_snapshot_scope(&state, &namespace).await?;
    let selected_snapshot =
        resolve_v2_snapshot(&state.pool, &scope, params.at.as_ref(), params.finality).await?;
    let row = load_name_current_for_selected_snapshot(
        &state.pool,
        &namespace,
        &normalized.normalized_name,
        &selected_snapshot,
    )
    .await
    .map_err(|error| {
        api_error_to_v2(map_internal_api_error(
            error,
            format!(
                "failed to load name profile for {}/{}",
                namespace, normalized.normalized_name
            ),
        ))
    })?;

    let record_inventory =
        load_supported_record_inventory_current_for_snapshot(&state.pool, &row, &selected_snapshot)
            .await
            .map_err(|error| api_error_to_v2(snapshot_selection_api_error(error)))?;
    let chain_id = response_chain_id(&selected_snapshot);
    let mut data = build_name_record(
        &row,
        record_inventory.as_ref(),
        chain_id,
        if route_source == Source::Verified {
            Status::Failed
        } else {
            Status::Ok
        },
    );
    if route_source == Source::Verified {
        mark_unserved_verified_fields(&mut data);
    }
    let meta = Meta {
        as_of: Some(as_of_meta(&selected_snapshot)?),
        source: Some(route_source),
        ..Meta::default()
    };

    Ok(Json(Envelope {
        data,
        page: None,
        meta,
    }))
}

async fn load_verified_record_lookup(
    state: &AppState,
    row: &bigname_storage::NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
    records: &[crate::ResolutionRecordKey],
    selected_snapshot: &SelectedSnapshot,
) -> V2Result<Option<VerifiedRecordLookup>> {
    if records.is_empty() {
        return Ok(None);
    }

    match load_or_execute_resolution_verified_outcome(
        state,
        row,
        records,
        record_inventory,
        selected_snapshot,
        false,
        true,
    )
    .await
    {
        Ok(Some(outcome)) => Ok(Some(VerifiedRecordLookup::Found(Box::new(outcome)))),
        Ok(None) => Ok(Some(VerifiedRecordLookup::NotSupported)),
        Err(error) if error.kind() == SnapshotSelectionErrorKind::Stale => Ok(Some(
            VerifiedRecordLookup::Stale(error.message().to_owned()),
        )),
        Err(error) => Err(api_error_to_v2(snapshot_selection_api_error(error))),
    }
}

fn parse_record_keys(keys: Option<&str>) -> V2Result<Option<Vec<crate::ResolutionRecordKey>>> {
    let Some(keys) = keys.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    let mut parsed = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for key in keys.split(',').map(str::trim) {
        if parsed.len() >= MAX_RECORD_KEYS {
            return Err(V2Error::invalid_input(format!(
                "keys must contain at most {MAX_RECORD_KEYS} record keys"
            )));
        }
        if key.is_empty() {
            return Err(V2Error::invalid_input(
                "keys must be a comma-separated record-key list",
            ));
        }
        let record = parse_resolution_record_key(key)
            .and_then(validate_product_record)
            .ok_or_else(|| {
                V2Error::invalid_input(
                    "keys must contain only addr:<coin_type>, text:<key>, avatar, or contenthash",
                )
            })?;
        if !seen.insert(record.record_key.clone()) {
            return Err(V2Error::invalid_input(
                "keys must not contain duplicate record keys",
            ));
        }
        parsed.push(record);
    }

    Ok(Some(parsed))
}

fn records_include_inventory(include: &[String]) -> V2Result<bool> {
    let mut include_inventory = false;
    for value in include {
        match value.as_str() {
            "inventory" => include_inventory = true,
            _ => {
                return Err(V2Error::invalid_input(
                    "include must contain only inventory",
                ));
            }
        }
    }
    Ok(include_inventory)
}

fn subnames_include_counts(include: &[String]) -> V2Result<bool> {
    let mut include_counts = false;
    for value in include {
        match value.as_str() {
            "counts" => include_counts = true,
            _ => return Err(V2Error::invalid_input("include must contain only counts")),
        }
    }
    Ok(include_counts)
}

fn mark_unserved_verified_fields(record: &mut NameRecord) {
    for field in [
        "addresses",
        "content_hash",
        "primary_address",
        "text_records",
    ] {
        if !record.unsupported_fields.iter().any(|value| value == field) {
            record.unsupported_fields.push(field.to_owned());
        }
    }
    record.unsupported_fields.sort();
}

fn route_source(source: RequestSource) -> V2Result<Source> {
    match source {
        RequestSource::Indexed => Ok(Source::Indexed),
        RequestSource::Verified => Ok(Source::Verified),
        RequestSource::Auto => Err(V2Error::invalid_input(
            "source must be one of: indexed, verified",
        )),
    }
}

async fn v2_exact_name_snapshot_scope(
    state: &AppState,
    namespace: &str,
) -> V2Result<SnapshotSelectionScope> {
    crate::exact_name_snapshot_scope(
        &state.pool,
        namespace,
        ExactNameSnapshotSelector::default(),
        false,
    )
    .await
    .map_err(api_error_to_v2)
}

fn response_chain_id(selected_snapshot: &SelectedSnapshot) -> Option<u64> {
    selected_snapshot
        .chain_positions
        .as_map()
        .values()
        .find_map(|position| super::slug_to_numeric(&position.chain_id))
}

fn api_error_to_v2(error: ApiError) -> V2Error {
    match error.code {
        "invalid_input" => V2Error::invalid_input(error.message),
        "not_found" => V2Error::not_found(error.message),
        "unsupported" => V2Error::unsupported(error.message),
        "stale" => V2Error::stale(error.message),
        "conflict" => V2Error::conflict(error.message),
        _ => V2Error::internal_error(error.message),
    }
}
