use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use bigname_storage::{SelectedSnapshot, SnapshotSelectionScope};

use crate::{
    ApiError, AppState, ExactNameSnapshotSelector, load_name_current_for_selected_snapshot,
    load_supported_record_inventory_current_for_snapshot, map_internal_api_error,
    normalize_inferred_route_name, snapshot_selection_api_error,
};

use super::{
    Envelope, Meta, NameRecord, QueryParams, RequestSource, Source, Status, V2Error, V2Result,
    as_of_meta, build_name_record, resolve_v2_snapshot,
};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/v2/lookup", post(not_implemented))
        .route("/v2/status", get(not_implemented))
        .route("/v2/names/{name}", get(get_name_record))
        .route("/v2/names/{name}/records", get(not_implemented))
        .route("/v2/names/{name}/subnames", get(not_implemented))
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
