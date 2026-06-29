use std::collections::BTreeMap;

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_storage::{RecordInventoryCurrentRow, SelectedSnapshot, SnapshotSelectionErrorKind};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    AppState, ExecutionOutcome,
    handler_resolution_on_demand::load_or_execute_resolution_verified_outcome,
    load_name_current_for_selected_snapshot, load_supported_record_inventory_current_for_snapshot,
    map_internal_api_error, normalize_inferred_route_name, parse_resolution_record_key,
    snapshot_selection_api_error,
};

use super::{
    Envelope, MAX_PAGE_SIZE, Meta, QueryParams, RequestSource, Resolver, Source, Status, V2Error,
    V2Result, api_error_to_v2, as_of_meta, default_requested_records,
    name_records_inventory::RecordInventory, resolve_v2_snapshot, v2_exact_name_snapshot_scope,
    validate_product_record,
};

mod build;
pub(crate) use build::{
    build_auto_name_records, build_indexed_name_records, build_verified_name_records,
    indexed_records_requiring_verified_fallback,
};

const MAX_RECORD_KEYS: usize = MAX_PAGE_SIZE as usize;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct NameRecords {
    pub(crate) resolver: Option<Resolver>,
    pub(crate) addresses: BTreeMap<String, String>,
    pub(crate) text_records: BTreeMap<String, String>,
    pub(crate) content_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) records: Option<BTreeMap<String, RecordAnswer>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) inventory: Option<RecordInventory>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct RecordAnswer {
    pub(crate) status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) value: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) unsupported_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) failure_reason: Option<String>,
}

pub(crate) enum VerifiedRecordLookup {
    Found(Box<ExecutionOutcome>),
    Stale(String),
    NotSupported,
}

pub(crate) async fn get_name_records(
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
