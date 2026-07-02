use std::collections::BTreeMap;

use axum::{
    Json,
    extract::{FromRequestParts, Path, State},
    http::request::Parts,
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
    Envelope, Meta, QueryParams, RawQueryParams, V2Error, V2Result,
    apply_diagnostics_dictionary_names, as_of_meta, resolve_diagnostic_name,
};

use crate::v2::{
    RecordAnswer, Source, api_error_to_v2, build_indexed_name_records, build_verified_name_records,
    default_requested_records, load_ephemeral_verified_record_lookup,
    load_persisted_verified_record_lookup, parse_raw_query_params_with_allowlist,
    parse_record_keys,
};

pub(crate) const DIAGNOSTIC_RECORDS_DEFAULT_COMPARISON_LIMIT: usize = 16;
pub(crate) const DIAGNOSTIC_RECORDS_VERIFIED_LOOKUP_CONCURRENCY: usize = 4;
const RECORD_INVENTORY_UNSUPPORTED_REASON: &str =
    "declared record inventory summary is not yet projected";
const RECORD_CACHE_UNSUPPORTED_REASON: &str = "declared record cache is not yet projected";
const COMPARISON_DEFAULT_LIMIT_GAP_REASON: &str = "diagnostics_comparison_default_limit_exceeded";
const DIAGNOSTIC_NAME_RECORDS_QUERY_PARAMS: &[&str] = &["namespace", "at", "finality", "keys"];

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct RawDiagnosticNameRecordsQueryParams {
    at: Option<String>,
    finality: Option<String>,
    namespace: Option<String>,
    keys: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DiagnosticNameRecordsQueryParams {
    inner: QueryParams,
}

impl<S> FromRequestParts<S> for DiagnosticNameRecordsQueryParams
where
    S: Send + Sync,
{
    type Rejection = V2Error;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let raw = parse_raw_query_params_with_allowlist::<RawDiagnosticNameRecordsQueryParams, S>(
            parts,
            state,
            DIAGNOSTIC_NAME_RECORDS_QUERY_PARAMS,
        )
        .await?;
        Self::try_from(raw)
    }
}

impl TryFrom<RawDiagnosticNameRecordsQueryParams> for DiagnosticNameRecordsQueryParams {
    type Error = V2Error;

    fn try_from(raw: RawDiagnosticNameRecordsQueryParams) -> Result<Self, Self::Error> {
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

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct NameRecordsDiagnostic {
    pub(crate) record_inventory: JsonValue,
    pub(crate) record_cache: JsonValue,
    pub(crate) value_sources: BTreeMap<String, Vec<RecordValueSource>>,
    pub(crate) comparison: BTreeMap<String, RecordComparison>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) comparison_explicit_gaps: Vec<RecordComparisonGap>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct RecordComparison {
    pub(crate) indexed: RecordAnswer,
    pub(crate) verified: RecordAnswer,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct RecordComparisonGap {
    pub(crate) record_key: String,
    pub(crate) record_family: String,
    pub(crate) selector_key: Option<String>,
    pub(crate) gap_reason: String,
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
    params: DiagnosticNameRecordsQueryParams,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<NameRecordsDiagnostic>>> {
    let params = bind_diagnostic_records_path_name(input_name, params);
    let requested_records = parse_record_keys(params.keys.as_deref())?;
    let (row, selected_snapshot) = resolve_diagnostic_name(&state, &params).await?;
    let record_inventory =
        load_diagnostic_record_inventory_current(&state, &row, &selected_snapshot).await?;
    let comparison_scope =
        comparison_scope(record_inventory.as_ref(), requested_records.as_deref());
    let data = build_name_records_diagnostic(
        &state,
        &row,
        record_inventory.as_ref(),
        &comparison_scope.records,
        comparison_scope.explicit_gaps,
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

fn bind_diagnostic_records_path_name(
    input_name: String,
    mut params: DiagnosticNameRecordsQueryParams,
) -> QueryParams {
    params.inner.name = Some(input_name);
    params.inner
}

async fn build_name_records_diagnostic(
    state: &AppState,
    row: &NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
    records: &[ResolutionRecordKey],
    comparison_explicit_gaps: Vec<RecordComparisonGap>,
    selected_snapshot: &SelectedSnapshot,
) -> V2Result<NameRecordsDiagnostic> {
    let indexed = build_indexed_name_records(row, record_inventory, Some(records), false)?;
    let verified_records = build_bounded_ephemeral_verified_record_answers(
        state,
        row,
        record_inventory,
        records,
        selected_snapshot,
    )
    .await?;
    let indexed_records = indexed.records.unwrap_or_default();
    let comparison = build_record_comparison(records, &indexed_records, &verified_records);
    let value_sources = build_value_sources(&comparison);

    let mut record_inventory_section = build_record_inventory_section_for_name(
        row,
        record_inventory,
        RECORD_INVENTORY_UNSUPPORTED_REASON,
    );
    let mut record_cache_section = build_record_cache_section_for_name(
        row,
        record_inventory,
        &[],
        RECORD_CACHE_UNSUPPORTED_REASON,
    );
    apply_diagnostics_dictionary_names(&mut record_inventory_section)?;
    apply_diagnostics_dictionary_names(&mut record_cache_section)?;

    Ok(NameRecordsDiagnostic {
        record_inventory: record_inventory_section,
        record_cache: record_cache_section,
        value_sources,
        comparison,
        comparison_explicit_gaps,
    })
}

async fn build_bounded_ephemeral_verified_record_answers(
    state: &AppState,
    row: &NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
    records: &[ResolutionRecordKey],
    selected_snapshot: &SelectedSnapshot,
) -> V2Result<BTreeMap<String, RecordAnswer>> {
    if let Some(verified_lookup) = load_persisted_verified_record_lookup(
        state,
        row,
        record_inventory,
        records,
        selected_snapshot,
    )
    .await?
    {
        let verified = build_verified_name_records(
            row,
            record_inventory,
            Some(records),
            Some(verified_lookup),
            false,
        )?;
        return Ok(verified.records.unwrap_or_default());
    }

    let mut answers = BTreeMap::new();
    for chunk in records.chunks(DIAGNOSTIC_RECORDS_VERIFIED_LOOKUP_CONCURRENCY) {
        let verified_lookup = load_ephemeral_verified_record_lookup(
            state,
            row,
            record_inventory,
            chunk,
            selected_snapshot,
        )
        .await?;
        let verified = build_verified_name_records(
            row,
            record_inventory,
            Some(chunk),
            verified_lookup,
            false,
        )?;
        answers.extend(verified.records.unwrap_or_default());
    }
    Ok(answers)
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct DiagnosticComparisonScope {
    records: Vec<ResolutionRecordKey>,
    explicit_gaps: Vec<RecordComparisonGap>,
}

fn comparison_scope(
    record_inventory: Option<&RecordInventoryCurrentRow>,
    requested_records: Option<&[ResolutionRecordKey]>,
) -> DiagnosticComparisonScope {
    let Some(requested_records) = requested_records else {
        let mut records = default_requested_records(record_inventory);
        let truncated_records = if records.len() > DIAGNOSTIC_RECORDS_DEFAULT_COMPARISON_LIMIT {
            records.split_off(DIAGNOSTIC_RECORDS_DEFAULT_COMPARISON_LIMIT)
        } else {
            Vec::new()
        };
        return DiagnosticComparisonScope {
            records,
            explicit_gaps: truncated_records.iter().map(comparison_limit_gap).collect(),
        };
    };

    DiagnosticComparisonScope {
        records: requested_records.to_vec(),
        explicit_gaps: Vec::new(),
    }
}

fn comparison_limit_gap(record: &ResolutionRecordKey) -> RecordComparisonGap {
    RecordComparisonGap {
        record_key: record.record_key.clone(),
        record_family: record.record_family.clone(),
        selector_key: record.selector_key.clone(),
        gap_reason: COMPARISON_DEFAULT_LIMIT_GAP_REASON.to_owned(),
    }
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
    use sqlx::types::{Uuid, time::OffsetDateTime};

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

    #[test]
    fn default_comparison_scope_caps_records_and_lists_explicit_gaps() {
        let inventory = comparison_inventory(18);

        let scope = comparison_scope(Some(&inventory), None);

        assert_eq!(
            scope.records.len(),
            DIAGNOSTIC_RECORDS_DEFAULT_COMPARISON_LIMIT
        );
        assert_eq!(scope.records[0].record_key, "text:key00");
        assert_eq!(scope.records[15].record_key, "text:key15");
        assert_eq!(
            scope.explicit_gaps,
            vec![
                RecordComparisonGap {
                    record_key: "text:key16".to_owned(),
                    record_family: "text".to_owned(),
                    selector_key: Some("key16".to_owned()),
                    gap_reason: COMPARISON_DEFAULT_LIMIT_GAP_REASON.to_owned(),
                },
                RecordComparisonGap {
                    record_key: "text:key17".to_owned(),
                    record_family: "text".to_owned(),
                    selector_key: Some("key17".to_owned()),
                    gap_reason: COMPARISON_DEFAULT_LIMIT_GAP_REASON.to_owned(),
                },
            ]
        );
    }

    #[test]
    fn default_comparison_scope_has_no_gap_at_or_below_limit() {
        let inventory = comparison_inventory(DIAGNOSTIC_RECORDS_DEFAULT_COMPARISON_LIMIT);

        let scope = comparison_scope(Some(&inventory), None);

        assert_eq!(
            scope.records.len(),
            DIAGNOSTIC_RECORDS_DEFAULT_COMPARISON_LIMIT
        );
        assert!(scope.explicit_gaps.is_empty());
    }

    #[test]
    fn requested_comparison_scope_is_not_default_capped() {
        let records = (0..18)
            .map(text_record)
            .collect::<Vec<ResolutionRecordKey>>();

        let scope = comparison_scope(None, Some(&records));

        assert_eq!(scope.records.len(), 18);
        assert!(scope.explicit_gaps.is_empty());
    }

    fn comparison_inventory(record_count: usize) -> RecordInventoryCurrentRow {
        RecordInventoryCurrentRow {
            resource_id: Uuid::from_u128(0x2200),
            record_version_boundary: json!({}),
            enumeration_basis: json!({}),
            selectors: JsonValue::Array((0..record_count).map(text_record_item).collect()),
            explicit_gaps: json!([]),
            unsupported_families: json!([]),
            last_change: None,
            entries: json!([]),
            provenance: json!({}),
            coverage: json!({}),
            chain_positions: json!({}),
            canonicality_summary: json!({}),
            manifest_version: 1,
            last_recomputed_at: OffsetDateTime::from_unix_timestamp(1_717_171_719)
                .expect("test timestamp must be valid"),
        }
    }

    fn text_record(index: usize) -> ResolutionRecordKey {
        ResolutionRecordKey {
            record_key: format!("text:key{index:02}"),
            record_family: "text".to_owned(),
            selector_key: Some(format!("key{index:02}")),
        }
    }

    fn text_record_item(index: usize) -> JsonValue {
        let record = text_record(index);
        json!({
            "record_key": record.record_key,
            "record_family": record.record_family,
            "selector_key": record.selector_key,
            "cacheable": true
        })
    }
}
