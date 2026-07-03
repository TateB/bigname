use std::collections::BTreeMap;

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_storage::{HistoryCursor, HistorySummaryMode};

use crate::AppState;

use super::address_names::relation_set_to_storage;
use super::cursor::{cursor_value, invalid_cursor_error};
use super::{
    CursorPayload, Envelope, Event, HistoryScope, Page, QueryParamAllowlist, RelationSet,
    SnapshotReadResource, StrictQueryParams, V2Error, V2Result, api_error_to_v2, build_event,
    decode, encode, encode_at_token, history_storage_scope, resolve_v2_snapshot_for, snapshot_meta,
    v2_exact_name_snapshot_scope,
};

const ADDRESS_HISTORY_SORT: &str = "chain_position_desc";
const ADDRESS_FILTER_KEY: &str = "address";
const NAMESPACE_FILTER_KEY: &str = "namespace";
const RELATION_FILTER_KEY: &str = "relation";
const SCOPE_FILTER_KEY: &str = "scope";
const NORMALIZED_EVENT_ID_CURSOR_KEY: &str = "normalized_event_id";
const EVENT_IDENTITY_CURSOR_KEY: &str = "event_identity";

pub(crate) struct AddressHistoryQueryParams;

impl QueryParamAllowlist for AddressHistoryQueryParams {
    const ALLOWED: &'static [&'static str] = &[
        "namespace",
        "at",
        "finality",
        "relation",
        "scope",
        "cursor",
        "page_size",
    ];
}

pub(crate) type AddressHistoryQuery = StrictQueryParams<AddressHistoryQueryParams>;

pub(crate) async fn get_address_history(
    Path(address): Path<String>,
    params: AddressHistoryQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<Event>>>> {
    let params = params.into_inner();
    let normalized_address =
        crate::parse_evm_address(&address, "address").map_err(api_error_to_v2)?;
    let namespace = params.namespace.clone().unwrap_or_else(|| "ens".to_owned());
    let storage_relations = params
        .relation
        .as_ref()
        .map(relation_set_to_storage)
        .unwrap_or_default();
    let storage_relations = (!storage_relations.is_empty()).then_some(storage_relations.as_slice());
    let storage_scope = history_storage_scope(params.scope);

    let scope = v2_exact_name_snapshot_scope(&state, &namespace, params.at.as_ref()).await?;
    let selected_snapshot = resolve_v2_snapshot_for(
        &state.pool,
        &scope,
        params.at.as_ref(),
        params.finality,
        SnapshotReadResource::AddressHistory,
    )
    .await?;
    let snapshot_token = encode_at_token(&selected_snapshot);
    let cursor_binding = AddressHistoryCursorBinding {
        address: &normalized_address,
        namespace: &namespace,
        relation: params.relation.as_ref(),
        scope: params.scope,
        snapshot_token: &snapshot_token,
    };
    let storage_cursor = params
        .cursor
        .as_deref()
        .map(|cursor| {
            let payload = decode(cursor)?;
            address_history_storage_cursor(&payload, &cursor_binding)
        })
        .transpose()?;

    let storage_page = bigname_storage::load_address_history_page_for_relations(
        &state.pool,
        &normalized_address,
        Some(&namespace),
        storage_relations,
        storage_scope,
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
            V2Error::internal_error("failed to load address history")
        }
    })?;

    let next_cursor = storage_page
        .next_cursor
        .as_ref()
        .map(|cursor| encode(&address_history_cursor_payload(cursor, &cursor_binding)));
    let has_more = next_cursor.is_some();
    let data = storage_page.rows.iter().filter_map(build_event).collect();
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

#[derive(Clone, Debug)]
pub(crate) struct AddressHistoryCursorBinding<'a> {
    pub(crate) address: &'a str,
    pub(crate) namespace: &'a str,
    pub(crate) relation: Option<&'a RelationSet>,
    pub(crate) scope: HistoryScope,
    pub(crate) snapshot_token: &'a str,
}

pub(crate) fn address_history_cursor_payload(
    cursor: &HistoryCursor,
    binding: &AddressHistoryCursorBinding<'_>,
) -> CursorPayload {
    CursorPayload::new(
        ADDRESS_HISTORY_SORT,
        address_history_cursor_filters(binding),
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
        Some(binding.snapshot_token.to_owned()),
    )
}

pub(crate) fn address_history_storage_cursor(
    payload: &CursorPayload,
    binding: &AddressHistoryCursorBinding<'_>,
) -> V2Result<HistoryCursor> {
    if payload.sort != ADDRESS_HISTORY_SORT {
        return Err(invalid_cursor_error());
    }
    if payload.snapshot.as_deref() != Some(binding.snapshot_token) {
        return Err(invalid_cursor_error());
    }
    if payload.filters != address_history_cursor_filters(binding) {
        return Err(invalid_cursor_error());
    }
    if payload.last_item.len() != 2 {
        return Err(invalid_cursor_error());
    }

    let normalized_event_id = cursor_value(
        payload,
        NORMALIZED_EVENT_ID_CURSOR_KEY,
        invalid_cursor_error,
    )?
    .parse::<i64>()
    .map_err(|_| invalid_cursor_error())?;
    let event_identity = cursor_value(payload, EVENT_IDENTITY_CURSOR_KEY, invalid_cursor_error)?;

    Ok(HistoryCursor {
        normalized_event_id,
        event_identity,
    })
}

fn address_history_cursor_filters(
    binding: &AddressHistoryCursorBinding<'_>,
) -> BTreeMap<String, String> {
    let mut filters = BTreeMap::from([
        (ADDRESS_FILTER_KEY.to_owned(), binding.address.to_owned()),
        (
            NAMESPACE_FILTER_KEY.to_owned(),
            binding.namespace.to_owned(),
        ),
        (
            SCOPE_FILTER_KEY.to_owned(),
            binding.scope.as_str().to_owned(),
        ),
    ]);
    if let Some(relation) = binding.relation {
        filters.insert(RELATION_FILTER_KEY.to_owned(), relation.canonical_value());
    }
    filters
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::Relation;

    const ADDRESS: &str = "0x00000000000000000000000000000000000000aa";
    const OTHER_ADDRESS: &str = "0x00000000000000000000000000000000000000bb";

    fn sample_cursor() -> HistoryCursor {
        HistoryCursor {
            normalized_event_id: 42,
            event_identity: "event:42".to_owned(),
        }
    }

    fn sample_binding() -> AddressHistoryCursorBinding<'static> {
        let relation = Box::leak(Box::new(RelationSet::from(Relation::Manager)));
        AddressHistoryCursorBinding {
            address: ADDRESS,
            namespace: "ens",
            relation: Some(relation),
            scope: HistoryScope::Both,
            snapshot_token: "snapshot-1",
        }
    }

    #[test]
    fn address_history_cursor_payload_round_trips_storage_cursor() {
        let cursor = sample_cursor();
        let binding = sample_binding();
        let payload = address_history_cursor_payload(&cursor, &binding);

        assert_eq!(
            payload.filters,
            BTreeMap::from([
                ("address".to_owned(), ADDRESS.to_owned()),
                ("namespace".to_owned(), "ens".to_owned()),
                ("relation".to_owned(), "manager".to_owned()),
                ("scope".to_owned(), "both".to_owned()),
            ])
        );
        assert_eq!(
            address_history_storage_cursor(&payload, &binding).expect("cursor must decode"),
            cursor
        );
    }

    #[test]
    fn address_history_cursor_omits_unset_relation_filter() {
        let cursor = sample_cursor();
        let binding = AddressHistoryCursorBinding {
            relation: None,
            ..sample_binding()
        };
        let payload = address_history_cursor_payload(&cursor, &binding);

        assert!(!payload.filters.contains_key("relation"));
        assert_eq!(
            address_history_storage_cursor(&payload, &binding).expect("cursor must decode"),
            cursor
        );
    }

    #[test]
    fn address_history_cursor_rejects_wrong_sort_snapshot_or_filters() {
        let cursor = sample_cursor();
        let binding = sample_binding();

        let mut payload = address_history_cursor_payload(&cursor, &binding);
        payload.sort = "name".to_owned();
        assert!(address_history_storage_cursor(&payload, &binding).is_err());

        let mut payload = address_history_cursor_payload(&cursor, &binding);
        payload.snapshot = Some("snapshot-2".to_owned());
        assert!(address_history_storage_cursor(&payload, &binding).is_err());

        let mut payload = address_history_cursor_payload(&cursor, &binding);
        payload
            .filters
            .insert("address".to_owned(), OTHER_ADDRESS.to_owned());
        assert!(address_history_storage_cursor(&payload, &binding).is_err());

        let mut payload = address_history_cursor_payload(&cursor, &binding);
        payload
            .filters
            .insert("scope".to_owned(), "name".to_owned());
        assert!(address_history_storage_cursor(&payload, &binding).is_err());

        let mut payload = address_history_cursor_payload(&cursor, &binding);
        payload.filters.remove("relation");
        assert!(address_history_storage_cursor(&payload, &binding).is_err());
    }
}
