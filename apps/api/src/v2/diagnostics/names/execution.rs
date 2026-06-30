use axum::{
    Json,
    extract::{FromRequestParts, Path, Query, State},
    http::request::Parts,
};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use tracing::error;

use crate::{
    AppState, build_resolution_execution_cache_key, build_resolution_execution_diagnostic_data,
    load_supported_record_inventory_current_for_snapshot, parse_resolution_record_key,
    resolution_execution_cache_lookup_records, resolution_verified_support_boundary,
    snapshot_selection_api_error,
};

use super::super::super::{MAX_PAGE_SIZE, validate_product_record};
use super::{
    Envelope, QueryParams, RawQueryParams, V2Error, V2Result, api_error_to_v2, diagnostic_envelope,
    resolve_diagnostic_name_with_resolution_auxiliary,
};

const MAX_EXECUTION_KEYS: usize = MAX_PAGE_SIZE as usize;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct RawDiagnosticNameExecutionQueryParams {
    at: Option<String>,
    finality: Option<String>,
    namespace: Option<String>,
    keys: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DiagnosticNameExecutionQueryParams {
    inner: QueryParams,
}

impl<S> FromRequestParts<S> for DiagnosticNameExecutionQueryParams
where
    S: Send + Sync,
{
    type Rejection = V2Error;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Query(raw) =
            Query::<RawDiagnosticNameExecutionQueryParams>::from_request_parts(parts, state)
                .await
                .map_err(|_| V2Error::invalid_input("query parameters are invalid"))?;
        Self::try_from(raw)
    }
}

impl TryFrom<RawDiagnosticNameExecutionQueryParams> for DiagnosticNameExecutionQueryParams {
    type Error = V2Error;

    fn try_from(raw: RawDiagnosticNameExecutionQueryParams) -> Result<Self, Self::Error> {
        Ok(Self {
            inner: QueryParams::try_from(RawQueryParams {
                at: raw.at,
                finality: raw.finality,
                namespace: raw.namespace,
                keys: raw.keys,
                ..RawQueryParams::default()
            })?,
        })
    }
}

pub(crate) async fn get_name_execution_diagnostic(
    Path(input_name): Path<String>,
    params: DiagnosticNameExecutionQueryParams,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<JsonValue>>> {
    let mut params = params.inner;
    params.name = Some(input_name);
    let records = parse_execution_keys(params.keys.as_deref())?;
    let (row, selected_snapshot) =
        resolve_diagnostic_name_with_resolution_auxiliary(&state, &params, true).await?;

    let record_inventory_current = match load_supported_record_inventory_current_for_snapshot(
        &state.pool,
        &row,
        &selected_snapshot,
    )
    .await
    {
        Ok(record_inventory_current) => record_inventory_current,
        Err(load_error) => {
            error!(
                service = "api",
                logical_name_id = %row.logical_name_id,
                records = ?records,
                error = ?load_error,
                "failed to load declared record inventory for v2 resolution execution diagnostic"
            );
            return Err(api_error_to_v2(snapshot_selection_api_error(load_error)));
        }
    };

    if resolution_verified_support_boundary(&row, record_inventory_current.as_ref()).is_none() {
        return Err(missing_execution_artifact_error(
            &row.namespace,
            &row.normalized_name,
        ));
    }

    let cache_key_records = resolution_execution_cache_lookup_records(&row, &records);
    let cache_key = build_resolution_execution_cache_key(
        &row,
        &cache_key_records,
        record_inventory_current.as_ref(),
        selected_snapshot.chain_positions_value(),
    )
    .map_err(|cache_key_error| {
        error!(
            service = "api",
            logical_name_id = %row.logical_name_id,
            records = ?records,
            error = ?cache_key_error,
            "failed to derive persisted execution request key for v2 resolution execution diagnostic"
        );
        V2Error::internal_error(format!(
            "failed to load resolution execution diagnostic for name {}",
            row.normalized_name
        ))
    })?;

    let outcome = bigname_storage::load_resolution_execution_outcome_at_snapshot(
        &state.pool,
        &cache_key,
        &selected_snapshot.chain_positions,
    )
    .await
    .map_err(|load_error| {
        error!(
            service = "api",
            logical_name_id = %row.logical_name_id,
            request_key = %cache_key.request_key,
            records = ?records,
            error = ?load_error,
            "failed to load persisted execution outcome for v2 resolution execution diagnostic"
        );
        V2Error::internal_error(format!(
            "failed to load resolution execution diagnostic for name {}",
            row.normalized_name
        ))
    })?;

    let Some(outcome) = outcome else {
        return Err(missing_execution_artifact_error(
            &row.namespace,
            &row.normalized_name,
        ));
    };

    let trace = bigname_storage::load_execution_trace(&state.pool, outcome.execution_trace_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                logical_name_id = %row.logical_name_id,
                execution_trace_id = %outcome.execution_trace_id,
                error = ?load_error,
                "failed to load persisted execution trace for v2 resolution execution diagnostic"
            );
            V2Error::internal_error(format!(
                "failed to load resolution execution diagnostic for name {}",
                row.normalized_name
            ))
        })?;

    let Some(trace) = trace else {
        error!(
            service = "api",
            logical_name_id = %row.logical_name_id,
            execution_trace_id = %outcome.execution_trace_id,
            "persisted execution outcome references a missing trace for v2 resolution execution diagnostic"
        );
        return Err(V2Error::internal_error(format!(
            "failed to load resolution execution diagnostic for name {}",
            row.normalized_name
        )));
    };

    let data = build_resolution_execution_diagnostic_data(&row, &records, &trace, &outcome)
        .map_err(|build_error| {
            error!(
                service = "api",
                execution_trace_id = %outcome.execution_trace_id,
                error = ?build_error,
                "failed to build v2 resolution execution diagnostic response"
            );
            V2Error::internal_error("failed to build resolution execution diagnostic")
        })?;

    diagnostic_envelope(data, &selected_snapshot)
}

fn parse_execution_keys(keys: Option<&str>) -> V2Result<Vec<crate::ResolutionRecordKey>> {
    let Some(keys) = keys.map(str::trim).filter(|value| !value.is_empty()) else {
        return Err(V2Error::invalid_input("keys is required"));
    };

    let mut parsed = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for key in keys.split(',').map(str::trim) {
        if parsed.len() >= MAX_EXECUTION_KEYS {
            return Err(V2Error::invalid_input(format!(
                "keys must contain at most {MAX_EXECUTION_KEYS} record keys"
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

    Ok(parsed)
}

fn missing_execution_artifact_error(namespace: &str, name: &str) -> V2Error {
    V2Error::not_found(format!(
        "persisted resolution execution diagnostic was not found for name {name} in namespace {namespace}"
    ))
}
