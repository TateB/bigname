use axum::{
    Json,
    extract::{FromRequestParts, Path, State},
    http::request::Parts,
};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use tracing::error;

use crate::{
    AppState, build_resolution_execution_cache_key, build_resolution_execution_diagnostic_data,
    load_record_inventory_current_matching_selected_snapshot,
    load_supported_record_inventory_current_for_snapshot, parse_resolution_record_key,
    resolution_execution_cache_lookup_records, resolution_verified_support_boundary,
    snapshot_selection_api_error, validate_loaded_resolution_verified_outcome,
};

use super::super::super::{MAX_PAGE_SIZE, validate_product_record};
use super::{
    Envelope, QueryParams, RawQueryParams, SnapshotReadResource, V2Error, V2Result,
    api_error_to_v2_for_resource, apply_diagnostics_dictionary_names, diagnostic_envelope,
    resolve_diagnostic_name_with_resolution_auxiliary,
};

const MAX_EXECUTION_KEYS: usize = MAX_PAGE_SIZE as usize;
const DIAGNOSTIC_NAME_EXECUTION_QUERY_PARAMS: &[&str] = &["namespace", "at", "finality", "keys"];

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
        let raw = super::super::super::parse_raw_query_params_with_allowlist::<
            RawDiagnosticNameExecutionQueryParams,
            S,
        >(parts, state, DIAGNOSTIC_NAME_EXECUTION_QUERY_PARAMS)
        .await?;
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

    let record_inventory_current = match load_record_inventory_current_for_execution_diagnostic(
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
            return Err(api_error_to_v2_for_resource(
                snapshot_selection_api_error(load_error),
                SnapshotReadResource::DiagnosticData,
            ));
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

    let mut outcome = bigname_storage::load_resolution_execution_outcome_at_snapshot(
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

    if outcome.is_none() && cache_key_records != records {
        let full_selector_cache_key = build_resolution_execution_cache_key(
            &row,
            &records,
            record_inventory_current.as_ref(),
            selected_snapshot.chain_positions_value(),
        )
        .map_err(|cache_key_error| {
            error!(
                service = "api",
                logical_name_id = %row.logical_name_id,
                records = ?records,
                error = ?cache_key_error,
                "failed to derive full-selector persisted execution request key for v2 resolution execution diagnostic"
            );
            V2Error::internal_error(format!(
                "failed to load resolution execution diagnostic for name {}",
                row.normalized_name
            ))
        })?;

        outcome = bigname_storage::load_resolution_execution_outcome_at_snapshot(
            &state.pool,
            &full_selector_cache_key,
            &selected_snapshot.chain_positions,
        )
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                logical_name_id = %row.logical_name_id,
                request_key = %full_selector_cache_key.request_key,
                records = ?records,
                error = ?load_error,
                "failed to load full-selector persisted execution outcome for v2 resolution execution diagnostic"
            );
            V2Error::internal_error(format!(
                "failed to load resolution execution diagnostic for name {}",
                row.normalized_name
            ))
        })?;
    }

    let Some(outcome) = outcome else {
        return Err(missing_execution_artifact_error(
            &row.namespace,
            &row.normalized_name,
        ));
    };
    if let Err(validation_error) =
        validate_loaded_resolution_verified_outcome(&row, &records, &outcome)
    {
        if validation_error.kind() == bigname_storage::SnapshotSelectionErrorKind::Stale {
            return Err(missing_execution_artifact_error(
                &row.namespace,
                &row.normalized_name,
            ));
        }

        error!(
            service = "api",
            logical_name_id = %row.logical_name_id,
            execution_trace_id = %outcome.execution_trace_id,
            error = ?validation_error,
            "persisted execution outcome failed coverage validation for v2 resolution execution diagnostic"
        );
        return Err(V2Error::internal_error(format!(
            "failed to load resolution execution diagnostic for name {}",
            row.normalized_name
        )));
    }

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

    let mut data = build_resolution_execution_diagnostic_data(&row, &records, &trace, &outcome)
        .map_err(|build_error| {
            error!(
                service = "api",
                execution_trace_id = %outcome.execution_trace_id,
                error = ?build_error,
                "failed to build v2 resolution execution diagnostic response"
            );
            V2Error::internal_error("failed to build resolution execution diagnostic")
        })?;
    prepare_execution_diagnostic_dictionary_names(&mut data)?;
    apply_diagnostics_dictionary_names(&mut data)?;

    diagnostic_envelope(data, &selected_snapshot)
}

fn prepare_execution_diagnostic_dictionary_names(value: &mut JsonValue) -> V2Result<()> {
    match value {
        JsonValue::Object(object) => {
            remove_redundant_logical_name_id_from_name_ref(object)?;
            for child in object.values_mut() {
                prepare_execution_diagnostic_dictionary_names(child)?;
            }
        }
        JsonValue::Array(items) => {
            for child in items {
                prepare_execution_diagnostic_dictionary_names(child)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn remove_redundant_logical_name_id_from_name_ref(
    object: &mut serde_json::Map<String, JsonValue>,
) -> V2Result<()> {
    if !object.contains_key("logical_name_id")
        || !object.contains_key("namespace")
        || !object.contains_key("normalized_name")
    {
        return Ok(());
    }

    let logical_name_id = object
        .get("logical_name_id")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| V2Error::internal_error("failed to map diagnostics dictionary names"))?;
    let Some((namespace, name)) = logical_name_id.split_once(':') else {
        return Err(V2Error::internal_error(
            "failed to map diagnostics dictionary names",
        ));
    };
    let existing_namespace = object
        .get("namespace")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| V2Error::internal_error("failed to map diagnostics dictionary names"))?;
    let existing_name = object
        .get("normalized_name")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| V2Error::internal_error("failed to map diagnostics dictionary names"))?;
    if namespace.is_empty()
        || name.is_empty()
        || existing_namespace != namespace
        || existing_name != name
    {
        return Err(V2Error::internal_error(
            "failed to map diagnostics dictionary names",
        ));
    }

    object.remove("logical_name_id");
    Ok(())
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

async fn load_record_inventory_current_for_execution_diagnostic(
    pool: &sqlx::PgPool,
    row: &bigname_storage::NameCurrentRow,
    selected_snapshot: &bigname_storage::SelectedSnapshot,
) -> std::result::Result<
    Option<bigname_storage::RecordInventoryCurrentRow>,
    bigname_storage::SnapshotSelectionError,
> {
    let allow_selected_superset = row.namespace == bigname_storage::BASENAMES_NAMESPACE;
    match load_supported_record_inventory_current_for_snapshot(pool, row, selected_snapshot).await {
        Ok(Some(record_inventory_current)) => Ok(Some(record_inventory_current)),
        Ok(None) if allow_selected_superset => {
            load_record_inventory_current_matching_selected_snapshot(
                pool,
                row,
                selected_snapshot,
                true,
            )
            .await
        }
        Ok(None) => Ok(None),
        Err(load_error)
            if allow_selected_superset
                && load_error.kind() == bigname_storage::SnapshotSelectionErrorKind::Stale =>
        {
            load_record_inventory_current_matching_selected_snapshot(
                pool,
                row,
                selected_snapshot,
                true,
            )
            .await
        }
        Err(load_error) => Err(load_error),
    }
}

fn missing_execution_artifact_error(namespace: &str, name: &str) -> V2Error {
    V2Error::not_found(format!(
        "persisted resolution execution diagnostic was not found for name {name} in namespace {namespace}"
    ))
}
