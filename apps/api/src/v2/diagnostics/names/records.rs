use std::collections::BTreeMap;

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_storage::{NameCurrentRow, RecordInventoryCurrentRow, SelectedSnapshot};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::{
    AppState, ResolutionRecordKey, load_supported_record_inventory_current_for_snapshot,
    responses::{build_record_cache_section_for_name, build_record_inventory_section_for_name},
    snapshot_selection_api_error,
};

use super::{
    DiagnosticNameQueryParams, Envelope, Meta, V2Result, as_of_meta, bind_diagnostic_path_name,
    resolve_diagnostic_name,
};

use crate::v2::{
    RecordAnswer, Source, api_error_to_v2, build_indexed_name_records, build_verified_name_records,
    default_requested_records, load_ephemeral_verified_record_lookup,
};

const RECORD_INVENTORY_UNSUPPORTED_REASON: &str =
    "declared record inventory summary is not yet projected";
const RECORD_CACHE_UNSUPPORTED_REASON: &str = "declared record cache is not yet projected";

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct NameRecordsDiagnostic {
    pub(crate) record_inventory: JsonValue,
    pub(crate) record_cache: JsonValue,
    pub(crate) value_sources: BTreeMap<String, Vec<RecordValueSource>>,
    pub(crate) comparison: BTreeMap<String, RecordComparison>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct RecordComparison {
    pub(crate) indexed: RecordAnswer,
    pub(crate) verified: RecordAnswer,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct RecordValueSource {
    pub(crate) source: Source,
    pub(crate) status: crate::v2::Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) value: Option<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) unsupported_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) failure_reason: Option<String>,
}

pub(crate) async fn get_name_records_diagnostic(
    Path(input_name): Path<String>,
    params: DiagnosticNameQueryParams,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<NameRecordsDiagnostic>>> {
    let params = bind_diagnostic_path_name(input_name, params);
    let (row, selected_snapshot) = resolve_diagnostic_name(&state, &params).await?;
    let record_inventory =
        load_diagnostic_record_inventory_current(&state, &row, &selected_snapshot).await?;
    let records = default_requested_records(record_inventory.as_ref());
    let data = build_name_records_diagnostic(
        &state,
        &row,
        record_inventory.as_ref(),
        &records,
        &selected_snapshot,
    )
    .await?;

    Ok(Json(Envelope {
        data,
        page: None,
        meta: Meta {
            as_of: Some(as_of_meta(&selected_snapshot)?),
            ..Meta::default()
        },
    }))
}

async fn build_name_records_diagnostic(
    state: &AppState,
    row: &NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
    records: &[ResolutionRecordKey],
    selected_snapshot: &SelectedSnapshot,
) -> V2Result<NameRecordsDiagnostic> {
    let indexed = build_indexed_name_records(row, record_inventory, Some(records), false);
    let verified_lookup = load_ephemeral_verified_record_lookup(
        state,
        row,
        record_inventory,
        records,
        selected_snapshot,
    )
    .await?;
    let verified =
        build_verified_name_records(row, record_inventory, Some(records), verified_lookup, false)?;
    let indexed_records = indexed.records.unwrap_or_default();
    let verified_records = verified.records.unwrap_or_default();
    let comparison = build_record_comparison(records, &indexed_records, &verified_records);
    let value_sources = build_value_sources(&comparison);

    Ok(NameRecordsDiagnostic {
        record_inventory: build_record_inventory_section_for_name(
            row,
            record_inventory,
            RECORD_INVENTORY_UNSUPPORTED_REASON,
        ),
        record_cache: build_record_cache_section_for_name(
            row,
            record_inventory,
            &[],
            RECORD_CACHE_UNSUPPORTED_REASON,
        ),
        value_sources,
        comparison,
    })
}

async fn load_diagnostic_record_inventory_current(
    state: &AppState,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> V2Result<Option<RecordInventoryCurrentRow>> {
    load_supported_record_inventory_current_for_snapshot(&state.pool, row, selected_snapshot)
        .await
        .map_err(|error| api_error_to_v2(snapshot_selection_api_error(error)))
}

fn build_record_comparison(
    records: &[ResolutionRecordKey],
    indexed_records: &BTreeMap<String, RecordAnswer>,
    verified_records: &BTreeMap<String, RecordAnswer>,
) -> BTreeMap<String, RecordComparison> {
    records
        .iter()
        .filter_map(|record| {
            Some((
                record.record_key.clone(),
                RecordComparison {
                    indexed: indexed_records.get(&record.record_key)?.clone(),
                    verified: verified_records.get(&record.record_key)?.clone(),
                },
            ))
        })
        .collect()
}

fn build_value_sources(
    comparison: &BTreeMap<String, RecordComparison>,
) -> BTreeMap<String, Vec<RecordValueSource>> {
    comparison
        .iter()
        .map(|(record_key, comparison)| {
            (
                record_key.clone(),
                vec![
                    value_source(Source::Indexed, &comparison.indexed),
                    value_source(Source::Verified, &comparison.verified),
                ],
            )
        })
        .collect()
}

fn value_source(source: Source, answer: &RecordAnswer) -> RecordValueSource {
    RecordValueSource {
        source,
        status: answer.status,
        value: answer.value.clone(),
        unsupported_reason: answer.unsupported_reason.clone(),
        failure_reason: answer.failure_reason.clone(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::v2::Status;

    #[test]
    fn value_sources_preserve_source_order_and_status_detail() {
        let mut comparison = BTreeMap::new();
        comparison.insert(
            "addr:60".to_owned(),
            RecordComparison {
                indexed: RecordAnswer {
                    status: Status::Ok,
                    value: Some(json!("0x0000000000000000000000000000000000000abc")),
                    unsupported_reason: None,
                    failure_reason: None,
                },
                verified: RecordAnswer {
                    status: Status::Stale,
                    value: None,
                    unsupported_reason: None,
                    failure_reason: Some("rpc_not_configured".to_owned()),
                },
            },
        );

        assert_eq!(
            serde_json::to_value(build_value_sources(&comparison)).expect("must serialize"),
            json!({
                "addr:60": [
                    {
                        "source": "indexed",
                        "status": "ok",
                        "value": "0x0000000000000000000000000000000000000abc"
                    },
                    {
                        "source": "verified",
                        "status": "stale",
                        "failure_reason": "rpc_not_configured"
                    }
                ]
            })
        );
    }
}
