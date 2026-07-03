use axum::{Json, extract::State};
use bigname_storage::{HistoryEvent as StorageHistoryEvent, HistorySummaryMode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::AppState;

use super::cursor::invalid_cursor_error;
use super::events::{parse_events_filter, resolve_events_namespace};
use super::{
    Envelope, Page, QueryParamAllowlist, SnapshotReadResource, StrictQueryParams, V2Error,
    V2Result, decode, encode, encode_at_token, events_cursor_payload, events_storage_cursor,
    format_timestamp, resolve_v2_snapshot_for, snapshot_meta, v2_exact_name_snapshot_scope,
};

pub(crate) struct DiagnosticEventsQueryParams;

impl QueryParamAllowlist for DiagnosticEventsQueryParams {
    const ALLOWED: &'static [&'static str] = &[
        "namespace",
        "name",
        "address",
        "registration_id",
        "type",
        "from_block",
        "to_block",
        "at",
        "finality",
        "cursor",
        "page_size",
    ];
}

pub(crate) type DiagnosticEventsQuery = StrictQueryParams<DiagnosticEventsQueryParams>;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct DiagnosticEvent {
    pub(crate) normalized_event_id: String,
    pub(crate) event_identity: String,
    pub(crate) namespace: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) registration_id: Option<String>,
    pub(crate) event_kind: String,
    pub(crate) source_family: String,
    pub(crate) manifest_version: i64,
    pub(crate) source_manifest_id: Option<i64>,
    pub(crate) chain_position: Value,
    pub(crate) transaction_hash: Option<String>,
    pub(crate) log_index: Option<i64>,
    pub(crate) raw_fact_ref: Value,
    pub(crate) derivation_kind: String,
    pub(crate) canonicality_state: String,
    pub(crate) before_state: Value,
    pub(crate) after_state: Value,
    pub(crate) provenance: Value,
    pub(crate) coverage: Value,
}

/// Raw diagnostics twin of `/v2/events`; filtering and cursor anchoring match
/// the product route, but rows are emitted without product event type mapping.
pub(crate) async fn get_diagnostic_events(
    params: DiagnosticEventsQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<DiagnosticEvent>>>> {
    let params = params.into_inner();
    let namespace = resolve_events_namespace(&params)?;
    let parsed = parse_events_filter(&params, &namespace)?;

    let scope = v2_exact_name_snapshot_scope(&state, &namespace, params.at.as_ref()).await?;
    let selected_snapshot = resolve_v2_snapshot_for(
        &state.pool,
        &scope,
        params.at.as_ref(),
        params.finality,
        SnapshotReadResource::DiagnosticData,
    )
    .await?;
    let snapshot_token = encode_at_token(&selected_snapshot);
    let storage_cursor = params
        .cursor
        .as_deref()
        .map(|cursor| {
            let payload = decode(cursor)?;
            events_storage_cursor(&payload, &parsed.cursor_filters, &snapshot_token)
        })
        .transpose()?;

    let storage_page = bigname_storage::load_event_history_page(
        &state.pool,
        parsed.storage_filter,
        true,
        storage_cursor.as_ref(),
        params.page_size,
        HistorySummaryMode::None,
    )
    .await
    .map_err(|error| {
        if error
            .downcast_ref::<bigname_storage::InvalidHistoryCursor>()
            .is_some()
        {
            invalid_cursor_error()
        } else {
            V2Error::internal_error("failed to load diagnostic events")
        }
    })?;

    let next_cursor = storage_page.next_cursor.as_ref().map(|cursor| {
        encode(&events_cursor_payload(
            cursor,
            &parsed.cursor_filters,
            &snapshot_token,
        ))
    });
    let has_more = next_cursor.is_some();
    let data = storage_page
        .rows
        .iter()
        .map(build_diagnostic_event)
        .collect();
    let meta = snapshot_meta(&selected_snapshot)?;

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

pub(crate) fn build_diagnostic_event(row: &StorageHistoryEvent) -> DiagnosticEvent {
    DiagnosticEvent {
        normalized_event_id: row.normalized_event_id.to_string(),
        event_identity: row.event_identity.clone(),
        namespace: row.namespace.clone(),
        name: event_name(row),
        registration_id: row.resource_id.map(|resource_id| resource_id.to_string()),
        event_kind: row.event_kind.clone(),
        source_family: row.source_family.clone(),
        manifest_version: row.manifest_version,
        source_manifest_id: row.source_manifest_id,
        chain_position: build_chain_position(row),
        transaction_hash: row.transaction_hash.clone(),
        log_index: row.log_index,
        raw_fact_ref: row.raw_fact_ref.clone(),
        derivation_kind: row.derivation_kind.clone(),
        canonicality_state: row.canonicality_state.as_str().to_owned(),
        before_state: row.before_state.clone(),
        after_state: row.after_state.clone(),
        provenance: ensure_object(&row.provenance),
        coverage: build_coverage(&row.coverage),
    }
}

fn event_name(row: &StorageHistoryEvent) -> Option<String> {
    row.logical_name_id
        .as_deref()
        .and_then(|logical_name_id| logical_name_id.split_once(':').map(|(_, name)| name.trim()))
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
}

fn build_chain_position(row: &StorageHistoryEvent) -> Value {
    match (
        row.chain_id.as_ref(),
        row.block_number,
        row.block_hash.as_ref(),
        row.block_timestamp,
    ) {
        (Some(chain_id), Some(block_number), Some(block_hash), Some(timestamp)) => json!({
            "chain_id": chain_id,
            "block_number": block_number,
            "block_hash": block_hash,
            "timestamp": format_timestamp(timestamp),
        }),
        _ => Value::Null,
    }
}

fn build_coverage(coverage: &Value) -> Value {
    json!({
        "status": str_field(coverage.get("status")).unwrap_or_else(|| "unsupported".to_owned()),
        "exhaustiveness": str_field(coverage.get("exhaustiveness"))
            .unwrap_or_else(|| "not_applicable".to_owned()),
        "source_classes_considered": coverage
            .get("source_classes_considered")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
        "enumeration_basis": str_field(coverage.get("enumeration_basis"))
            .unwrap_or_else(|| "exact_name".to_owned()),
        "unsupported_reason": str_field(coverage.get("unsupported_reason")),
    })
}

fn ensure_object(value: &Value) -> Value {
    if value.is_object() {
        value.clone()
    } else {
        json!({})
    }
}

fn str_field(value: Option<&Value>) -> Option<String> {
    value.and_then(Value::as_str).map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use bigname_storage::CanonicalityState;
    use serde_json::json;
    use sqlx::types::{Uuid, time::OffsetDateTime};

    use super::*;

    #[test]
    fn build_diagnostic_event_emits_raw_history_fields_without_type_filtering() {
        let row = StorageHistoryEvent {
            normalized_event_id: 12,
            event_identity: "diag:surface-bound".to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: Some("ens:alice.eth".to_owned()),
            resource_id: Some(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()),
            event_kind: "SurfaceBound".to_owned(),
            source_family: "ens_v1_registry_l1".to_owned(),
            manifest_version: 7,
            source_manifest_id: Some(99),
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_number: Some(123),
            block_hash: Some("0xblock".to_owned()),
            block_timestamp: Some(OffsetDateTime::from_unix_timestamp(1_700_000_123).unwrap()),
            transaction_hash: Some("0xtx".to_owned()),
            log_index: Some(4),
            raw_fact_ref: json!({"kind": "raw_log"}),
            derivation_kind: "direct".to_owned(),
            canonicality_state: CanonicalityState::Canonical,
            before_state: json!({"before": true}),
            after_state: json!({"after": true}),
            provenance: json!({"source": "test"}),
            coverage: json!({
                "status": "full",
                "exhaustiveness": "authoritative",
                "source_classes_considered": ["normalized_events"],
                "enumeration_basis": "event-history",
                "unsupported_reason": null,
            }),
        };

        let event = build_diagnostic_event(&row);

        assert_eq!(event.normalized_event_id, "12");
        assert_eq!(event.event_identity, "diag:surface-bound");
        assert_eq!(event.name, Some("alice.eth".to_owned()));
        assert_eq!(
            event.registration_id,
            Some("550e8400-e29b-41d4-a716-446655440000".to_owned())
        );
        assert_eq!(event.event_kind, "SurfaceBound");
        assert_eq!(event.derivation_kind, "direct");
        assert_eq!(event.canonicality_state, "canonical");
        assert_eq!(event.before_state, json!({"before": true}));
        assert_eq!(event.after_state, json!({"after": true}));
        assert_eq!(event.provenance, json!({"source": "test"}));
        assert_eq!(
            event.chain_position,
            json!({
                "chain_id": "ethereum-mainnet",
                "block_number": 123,
                "block_hash": "0xblock",
                "timestamp": "2023-11-14T22:15:23Z",
            })
        );
        assert_eq!(
            event.coverage,
            json!({
                "status": "full",
                "exhaustiveness": "authoritative",
                "source_classes_considered": ["normalized_events"],
                "enumeration_basis": "event-history",
                "unsupported_reason": null,
            })
        );
    }
}
