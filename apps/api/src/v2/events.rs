use std::collections::BTreeMap;

use axum::{Json, extract::State};
use bigname_storage::{
    EventHistoryAddressFilter, EventHistoryFilter, HistoryCursor,
    HistoryEvent as StorageHistoryEvent, HistorySummaryMode,
};
use serde::{Deserialize, Serialize};
use sqlx::types::Uuid;

use crate::{AppState, normalize_inferred_route_name};

use super::{
    CursorPayload, Envelope, HistoryEventType, Meta, Page, QueryParamAllowlist, QueryParams,
    StrictQueryParams, V2Error, V2Result, as_of_meta, decode, encode, encode_at_token,
    format_timestamp, history_event_type, relation_to_storage, resolve_v2_snapshot,
    v2_exact_name_snapshot_scope,
};

const EVENTS_SORT: &str = "chain_position_desc";
const NAMESPACE_FILTER_KEY: &str = "namespace";
const NAME_FILTER_KEY: &str = "name";
const ADDRESS_FILTER_KEY: &str = "address";
const REGISTRATION_ID_FILTER_KEY: &str = "registration_id";
const TYPE_FILTER_KEY: &str = "type";
const RELATION_FILTER_KEY: &str = "relation";
const FROM_BLOCK_FILTER_KEY: &str = "from_block";
const TO_BLOCK_FILTER_KEY: &str = "to_block";
const NORMALIZED_EVENT_ID_CURSOR_KEY: &str = "normalized_event_id";
const EVENT_IDENTITY_CURSOR_KEY: &str = "event_identity";

pub(crate) struct EventsQueryParams;

impl QueryParamAllowlist for EventsQueryParams {
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

pub(crate) type EventsQuery = StrictQueryParams<EventsQueryParams>;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct Event {
    #[serde(rename = "type")]
    pub(crate) event_type: HistoryEventType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    pub(crate) namespace: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) registration_id: Option<String>,
    pub(crate) block_number: Option<i64>,
    pub(crate) timestamp: Option<String>,
    pub(crate) transaction_hash: Option<String>,
    pub(crate) log_index: Option<i64>,
}

#[derive(Debug)]
pub(crate) struct ParsedEventsFilter {
    pub(crate) storage_filter: EventHistoryFilter,
    pub(crate) cursor_filters: BTreeMap<String, String>,
}

/// `namespace` defaults to the name's inferred namespace when `name` is provided
/// and `namespace` is omitted; otherwise defaults to `ens`.
pub(crate) async fn get_events(
    params: EventsQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<Event>>>> {
    let params = params.into_inner();
    let namespace = resolve_events_namespace(&params)?;
    let parsed = parse_events_filter(&params, &namespace)?;

    let scope = v2_exact_name_snapshot_scope(&state, &namespace, params.at.as_ref()).await?;
    let selected_snapshot =
        resolve_v2_snapshot(&state.pool, &scope, params.at.as_ref(), params.finality).await?;
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
            invalid_events_cursor()
        } else {
            V2Error::internal_error("failed to load events")
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
    let data = storage_page.rows.iter().filter_map(build_event).collect();
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

pub(crate) fn build_event(row: &StorageHistoryEvent) -> Option<Event> {
    let event_type = history_event_type(&row.event_kind)?;

    Some(Event {
        event_type,
        name: event_name(row),
        namespace: row.namespace.clone(),
        registration_id: row.resource_id.map(|resource_id| resource_id.to_string()),
        block_number: row.block_number,
        timestamp: row.block_timestamp.map(format_timestamp),
        transaction_hash: row.transaction_hash.clone(),
        log_index: row.log_index,
    })
}

pub(crate) fn events_cursor_payload(
    cursor: &HistoryCursor,
    filters: &BTreeMap<String, String>,
    snapshot_token: &str,
) -> CursorPayload {
    CursorPayload::new(
        EVENTS_SORT,
        filters.clone(),
        BTreeMap::from([
            (
                NORMALIZED_EVENT_ID_CURSOR_KEY.to_owned(),
                cursor.normalized_event_id.to_string(),
            ),
            (
                EVENT_IDENTITY_CURSOR_KEY.to_owned(),
                cursor.event_identity.clone(),
            ),
        ]),
        Some(snapshot_token.to_owned()),
    )
}

pub(crate) fn events_storage_cursor(
    payload: &CursorPayload,
    expected_filters: &BTreeMap<String, String>,
    snapshot_token: &str,
) -> V2Result<HistoryCursor> {
    if payload.sort != EVENTS_SORT {
        return Err(invalid_events_cursor());
    }
    if payload.snapshot.as_deref() != Some(snapshot_token) {
        return Err(invalid_events_cursor());
    }
    if &payload.filters != expected_filters {
        return Err(invalid_events_cursor());
    }
    if payload.last_item.len() != 2 {
        return Err(invalid_events_cursor());
    }

    let normalized_event_id = cursor_value(payload, NORMALIZED_EVENT_ID_CURSOR_KEY)?
        .parse::<i64>()
        .map_err(|_| invalid_events_cursor())?;
    let event_identity = cursor_value(payload, EVENT_IDENTITY_CURSOR_KEY)?;

    Ok(HistoryCursor {
        normalized_event_id,
        event_identity,
    })
}

pub(crate) fn resolve_events_namespace(params: &QueryParams) -> V2Result<String> {
    match (params.namespace.as_deref(), params.name.as_deref()) {
        (Some(namespace), _) => Ok(namespace.to_owned()),
        (None, Some(name)) => normalize_inferred_route_name(name)
            .map(|normalized| normalized.namespace.to_owned())
            .map_err(|error| V2Error::invalid_input(error.message)),
        (None, None) => Ok("ens".to_owned()),
    }
}

pub(crate) fn parse_events_filter(
    params: &QueryParams,
    namespace: &str,
) -> V2Result<ParsedEventsFilter> {
    if params.relation.is_some() && params.address.is_none() {
        return Err(V2Error::invalid_input("relation requires address"));
    }
    if matches!(
        (params.from_block, params.to_block),
        (Some(from_block), Some(to_block)) if from_block > to_block
    ) {
        return Err(V2Error::invalid_input(
            "from_block must be less than or equal to to_block",
        ));
    }

    let logical_name_id = params
        .name
        .as_deref()
        .map(|name| {
            normalize_inferred_route_name(name)
                .map(|normalized| format!("{namespace}:{}", normalized.normalized_name))
                .map_err(|error| V2Error::invalid_input(error.message))
        })
        .transpose()?;
    let resource_id = params
        .registration_id
        .as_deref()
        .map(|registration_id| {
            Uuid::parse_str(registration_id)
                .map_err(|_| V2Error::invalid_input("registration_id must be a UUID"))
        })
        .transpose()?;
    let event_kinds = params
        .event_type
        .map(|event_type| {
            event_type
                .storage_event_kinds()
                .iter()
                .map(|kind| (*kind).to_owned())
                .collect()
        })
        .unwrap_or_default();

    let mut cursor_filters =
        BTreeMap::from([(NAMESPACE_FILTER_KEY.to_owned(), namespace.to_owned())]);
    if let Some(logical_name_id) = logical_name_id.as_ref() {
        cursor_filters.insert(NAME_FILTER_KEY.to_owned(), logical_name_id.clone());
    }
    if let Some(address) = params.address.as_ref() {
        cursor_filters.insert(ADDRESS_FILTER_KEY.to_owned(), address.clone());
    }
    if let Some(registration_id) = params.registration_id.as_ref() {
        cursor_filters.insert(
            REGISTRATION_ID_FILTER_KEY.to_owned(),
            registration_id.clone(),
        );
    }
    if let Some(event_type) = params.event_type {
        cursor_filters.insert(TYPE_FILTER_KEY.to_owned(), event_type.as_str().to_owned());
    }
    if let Some(relation) = params.relation {
        cursor_filters.insert(RELATION_FILTER_KEY.to_owned(), relation.as_str().to_owned());
    }
    if let Some(from_block) = params.from_block {
        cursor_filters.insert(FROM_BLOCK_FILTER_KEY.to_owned(), from_block.to_string());
    }
    if let Some(to_block) = params.to_block {
        cursor_filters.insert(TO_BLOCK_FILTER_KEY.to_owned(), to_block.to_string());
    }

    Ok(ParsedEventsFilter {
        storage_filter: EventHistoryFilter {
            namespace: Some(namespace.to_owned()),
            logical_name_id,
            resource_id,
            address: params
                .address
                .as_ref()
                .map(|address| EventHistoryAddressFilter {
                    address: address.clone(),
                    relation: params.relation.map(relation_to_storage),
                }),
            event_kinds,
            from_block: params.from_block,
            to_block: params.to_block,
        },
        cursor_filters,
    })
}

fn event_name(row: &StorageHistoryEvent) -> Option<String> {
    row.logical_name_id
        .as_deref()
        .and_then(|logical_name_id| logical_name_id.split_once(':').map(|(_, name)| name.trim()))
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
}

fn cursor_value(payload: &CursorPayload, key: &str) -> V2Result<String> {
    payload
        .last_item
        .get(key)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(invalid_events_cursor)
}

fn invalid_events_cursor() -> V2Error {
    V2Error::invalid_input("cursor must be a valid pagination cursor")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bigname_storage::CanonicalityState;
    use serde_json::json;

    use super::*;
    use crate::v2::{ErrorCode, RawQueryParams};

    const ADDRESS: &str = "0x00000000000000000000000000000000000000aa";
    const REGISTRATION_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

    fn sample_cursor() -> HistoryCursor {
        HistoryCursor {
            normalized_event_id: 42,
            event_identity: "event:42".to_owned(),
        }
    }

    fn sample_filters() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("namespace".to_owned(), "ens".to_owned()),
            ("type".to_owned(), "registration".to_owned()),
            ("from_block".to_owned(), "10".to_owned()),
        ])
    }

    fn storage_event(event_kind: &str, logical_name_id: Option<&str>) -> StorageHistoryEvent {
        StorageHistoryEvent {
            normalized_event_id: 1,
            event_identity: "event:1".to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: logical_name_id.map(str::to_owned),
            resource_id: Some(Uuid::parse_str(REGISTRATION_ID).expect("uuid literal must parse")),
            event_kind: event_kind.to_owned(),
            source_family: "ens_v1".to_owned(),
            manifest_version: 1,
            source_manifest_id: Some(1),
            chain_id: Some("eip155:1".to_owned()),
            block_number: Some(100),
            block_hash: Some("0xblock".to_owned()),
            block_timestamp: None,
            transaction_hash: Some("0xtx".to_owned()),
            log_index: Some(5),
            raw_fact_ref: json!({}),
            derivation_kind: "direct".to_owned(),
            canonicality_state: CanonicalityState::Canonical,
            before_state: json!({}),
            after_state: json!({}),
            provenance: json!({}),
            coverage: json!({}),
        }
    }

    #[test]
    fn resolve_events_namespace_uses_explicit_inferred_or_global_default() {
        let params = QueryParams::try_from(RawQueryParams {
            namespace: Some("ens".to_owned()),
            name: Some("alice.base.eth".to_owned()),
            ..RawQueryParams::default()
        })
        .expect("params must parse");
        assert_eq!(resolve_events_namespace(&params).expect("namespace"), "ens");

        let params = QueryParams::try_from(RawQueryParams {
            name: Some("alice.base.eth".to_owned()),
            ..RawQueryParams::default()
        })
        .expect("params must parse");
        assert_eq!(
            resolve_events_namespace(&params).expect("namespace"),
            "basenames"
        );

        let params = QueryParams::try_from(RawQueryParams {
            name: Some("alice.eth".to_owned()),
            ..RawQueryParams::default()
        })
        .expect("params must parse");
        assert_eq!(resolve_events_namespace(&params).expect("namespace"), "ens");

        let params = QueryParams::try_from(RawQueryParams::default()).expect("params must parse");
        assert_eq!(resolve_events_namespace(&params).expect("namespace"), "ens");

        let params = QueryParams::try_from(RawQueryParams {
            name: Some("bad name.eth".to_owned()),
            ..RawQueryParams::default()
        })
        .expect("params must parse");
        let error = resolve_events_namespace(&params).expect_err("invalid name must fail");
        assert_eq!(error.code(), ErrorCode::InvalidInput);
    }

    #[test]
    fn events_cursor_payload_round_trips_storage_cursor() {
        let cursor = sample_cursor();
        let filters = sample_filters();
        let payload = events_cursor_payload(&cursor, &filters, "snapshot-1");

        assert_eq!(payload.filters, filters);
        assert_eq!(
            events_storage_cursor(&payload, &sample_filters(), "snapshot-1")
                .expect("cursor must decode"),
            cursor
        );
    }

    #[test]
    fn events_cursor_rejects_wrong_sort_snapshot_or_filters() {
        let cursor = sample_cursor();
        let filters = sample_filters();

        let mut payload = events_cursor_payload(&cursor, &filters, "snapshot-1");
        payload.sort = "name".to_owned();
        assert!(events_storage_cursor(&payload, &filters, "snapshot-1").is_err());

        let mut payload = events_cursor_payload(&cursor, &filters, "snapshot-1");
        payload.snapshot = Some("snapshot-2".to_owned());
        assert!(events_storage_cursor(&payload, &filters, "snapshot-1").is_err());

        let mut payload = events_cursor_payload(&cursor, &filters, "snapshot-1");
        payload
            .filters
            .insert("to_block".to_owned(), "20".to_owned());
        assert!(events_storage_cursor(&payload, &filters, "snapshot-1").is_err());

        let mut payload = events_cursor_payload(&cursor, &filters, "snapshot-1");
        payload.filters.remove("namespace");
        assert!(events_storage_cursor(&payload, &filters, "snapshot-1").is_err());

        let payload = events_cursor_payload(&cursor, &filters, "snapshot-1");
        assert!(
            events_storage_cursor(
                &payload,
                &BTreeMap::from([("namespace".to_owned(), "ens".to_owned())]),
                "snapshot-1",
            )
            .is_err()
        );
    }

    #[test]
    fn build_event_derives_name_and_drops_non_product_kinds() {
        let event = build_event(&storage_event("RegistrationGranted", Some("ens:alice.eth")))
            .expect("product event must build");

        assert_eq!(event.event_type, HistoryEventType::Registration);
        assert_eq!(event.name, Some("alice.eth".to_owned()));
        assert_eq!(event.namespace, "ens");
        assert_eq!(event.registration_id, Some(REGISTRATION_ID.to_owned()));
        assert_eq!(event.block_number, Some(100));
        assert_eq!(event.transaction_hash, Some("0xtx".to_owned()));
        assert_eq!(event.log_index, Some(5));

        let event = build_event(&storage_event("RecordChanged", None))
            .expect("product event without name must build");
        assert_eq!(event.name, None);

        assert!(build_event(&storage_event("SurfaceBound", Some("ens:alice.eth"))).is_none());
    }

    #[test]
    fn events_filter_rejects_relation_without_address_and_invalid_block_range() {
        let params = QueryParams::try_from(RawQueryParams {
            relation: Some("owner".to_owned()),
            ..RawQueryParams::default()
        })
        .expect("relation parses globally");
        let error =
            parse_events_filter(&params, "ens").expect_err("relation requires address for events");
        assert_eq!(error.code(), ErrorCode::InvalidInput);

        let params = QueryParams::try_from(RawQueryParams {
            from_block: Some("20".to_owned()),
            to_block: Some("10".to_owned()),
            ..RawQueryParams::default()
        })
        .expect("block bounds parse globally");
        let error = parse_events_filter(&params, "ens").expect_err("bad block range must fail");
        assert_eq!(error.code(), ErrorCode::InvalidInput);
    }

    #[test]
    fn events_filter_builds_storage_filter_and_cursor_filters() {
        let params = QueryParams::try_from(RawQueryParams {
            namespace: Some("basenames".to_owned()),
            event_type: Some("permission".to_owned()),
            name: Some(" Alice.base.eth ".to_owned()),
            registration_id: Some(REGISTRATION_ID.to_owned()),
            address: Some(ADDRESS.to_owned()),
            relation: Some("manager".to_owned()),
            from_block: Some("10".to_owned()),
            to_block: Some("20".to_owned()),
            ..RawQueryParams::default()
        })
        .expect("filters must parse globally");

        let parsed = parse_events_filter(&params, "basenames").expect("filter must build");

        assert_eq!(
            parsed.cursor_filters,
            BTreeMap::from([
                ("address".to_owned(), ADDRESS.to_owned()),
                ("from_block".to_owned(), "10".to_owned()),
                ("name".to_owned(), "basenames:alice.base.eth".to_owned()),
                ("namespace".to_owned(), "basenames".to_owned()),
                ("registration_id".to_owned(), REGISTRATION_ID.to_owned()),
                ("relation".to_owned(), "manager".to_owned()),
                ("to_block".to_owned(), "20".to_owned()),
                ("type".to_owned(), "permission".to_owned()),
            ])
        );
        assert_eq!(
            parsed.storage_filter.event_kinds,
            vec![
                "PermissionChanged".to_owned(),
                "PermissionScopeChanged".to_owned(),
                "RolesChanged".to_owned(),
                "EACRolesChanged".to_owned(),
            ]
        );
        assert_eq!(
            parsed.storage_filter.logical_name_id,
            Some("basenames:alice.base.eth".to_owned())
        );
        assert_eq!(parsed.storage_filter.from_block, Some(10));
        assert_eq!(parsed.storage_filter.to_block, Some(20));
        assert_eq!(
            parsed
                .storage_filter
                .address
                .as_ref()
                .expect("address filter must exist")
                .relation,
            Some(bigname_storage::AddressNameRelation::EffectiveController)
        );
    }
}
