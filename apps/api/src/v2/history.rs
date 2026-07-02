use std::collections::BTreeMap;

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_storage::{
    HistoryCursor, HistoryEvent as StorageHistoryEvent, HistorySummaryMode, SnapshotAt,
    SnapshotSelectionScope,
};
use serde::{Deserialize, Serialize};
use sqlx::types::time::{OffsetDateTime, UtcOffset};

use crate::{
    ApiError, AppState, ExactNameSnapshotSelector, load_name_current_for_selected_snapshot,
    map_internal_api_error, normalize_inferred_route_name,
};

use super::{
    AtSelector, CursorPayload, Envelope, HistoryEventType, HistoryScope, Meta, Page,
    QueryParamAllowlist, QueryParams, StrictQueryParams, V2Error, V2Result, as_of_meta, decode,
    decode_at_token, encode, encode_at_token, resolve_v2_snapshot,
};

const HISTORY_SORT: &str = "chain_position_desc";
const NAMESPACE_FILTER_KEY: &str = "namespace";
const NAME_FILTER_KEY: &str = "name";
const SCOPE_FILTER_KEY: &str = "scope";
const NORMALIZED_EVENT_ID_CURSOR_KEY: &str = "normalized_event_id";
const EVENT_IDENTITY_CURSOR_KEY: &str = "event_identity";

pub(crate) struct HistoryQueryParams;

impl QueryParamAllowlist for HistoryQueryParams {
    const ALLOWED: &'static [&'static str] = &[
        "namespace",
        "at",
        "finality",
        "scope",
        "cursor",
        "page_size",
    ];
}

pub(crate) type HistoryQuery = StrictQueryParams<HistoryQueryParams>;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct HistoryEvent {
    #[serde(rename = "type")]
    pub(crate) event_type: HistoryEventType,
    pub(crate) name: String,
    pub(crate) namespace: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) registration_id: Option<String>,
    pub(crate) block_number: Option<i64>,
    pub(crate) timestamp: Option<String>,
    pub(crate) transaction_hash: Option<String>,
    pub(crate) log_index: Option<i64>,
}

pub(crate) async fn get_history(
    Path(input_name): Path<String>,
    params: HistoryQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<HistoryEvent>>>> {
    let params = params.into_inner();
    let normalized = normalize_inferred_route_name(&input_name)
        .map_err(|error| V2Error::invalid_input(error.message))?;
    let namespace = params
        .namespace
        .clone()
        .unwrap_or_else(|| normalized.namespace.to_owned());

    let scope = v2_exact_name_snapshot_scope(&state, &namespace, params.at.as_ref()).await?;
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
                "failed to load history for {}/{}",
                namespace, normalized.normalized_name
            ),
        ))
    })?;

    let resource_ids = if matches!(params.scope, HistoryScope::Name) {
        Vec::new()
    } else {
        crate::resource_ids_for_name(&state.pool, &parent.logical_name_id)
            .await
            .map_err(api_error_to_v2)?
    };
    let storage_scope = history_storage_scope(params.scope);
    let snapshot_token = encode_at_token(&selected_snapshot);
    let storage_cursor = params
        .cursor
        .as_deref()
        .map(|cursor| {
            let payload = decode(cursor)?;
            history_storage_cursor(
                &payload,
                &namespace,
                &parent.logical_name_id,
                params.scope,
                &snapshot_token,
            )
        })
        .transpose()?;

    let storage_page = bigname_storage::load_name_history_page(
        &state.pool,
        &parent.logical_name_id,
        &resource_ids,
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
            V2Error::invalid_input("cursor must be a valid pagination cursor")
        } else {
            V2Error::internal_error(format!(
                "failed to load history for {}/{}",
                namespace, normalized.normalized_name
            ))
        }
    })?;

    let next_cursor = storage_page.next_cursor.as_ref().map(|cursor| {
        encode(&history_cursor_payload(
            cursor,
            &namespace,
            &parent.logical_name_id,
            params.scope,
            &snapshot_token,
        ))
    });
    let has_more = next_cursor.is_some();
    let data = storage_page
        .rows
        .iter()
        .filter_map(|row| build_history_event(row, &normalized.normalized_name))
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

pub(crate) fn build_history_event(
    row: &StorageHistoryEvent,
    anchor_name: &str,
) -> Option<HistoryEvent> {
    let event_type = history_event_type(&row.event_kind)?;

    Some(HistoryEvent {
        event_type,
        name: history_event_name(row, anchor_name),
        namespace: row.namespace.clone(),
        registration_id: row.resource_id.map(|resource_id| resource_id.to_string()),
        block_number: row.block_number,
        timestamp: row.block_timestamp.map(format_timestamp),
        transaction_hash: row.transaction_hash.clone(),
        log_index: row.log_index,
    })
}

pub(crate) fn history_event_type(event_kind: &str) -> Option<HistoryEventType> {
    match event_kind {
        "RegistrationGranted" | "LabelRegistered" => Some(HistoryEventType::Registration),
        "RegistrationRenewed" => Some(HistoryEventType::Renewal),
        "RegistrationReleased" => Some(HistoryEventType::Release),
        "ExpiryChanged" => Some(HistoryEventType::Expiry),
        "TokenControlTransferred" => Some(HistoryEventType::Transfer),
        "AuthorityTransferred" | "AuthorityEpochChanged" => Some(HistoryEventType::Authority),
        "ResolverChanged" => Some(HistoryEventType::Resolver),
        "RecordChanged" | "RecordVersionChanged" => Some(HistoryEventType::Record),
        "ReverseChanged" => Some(HistoryEventType::PrimaryName),
        "PermissionChanged" | "PermissionScopeChanged" | "RolesChanged" | "EACRolesChanged" => {
            Some(HistoryEventType::Permission)
        }
        _ => None,
    }
}

pub(crate) fn history_cursor_payload(
    cursor: &HistoryCursor,
    namespace: &str,
    parent_logical_name_id: &str,
    scope: HistoryScope,
    snapshot_token: &str,
) -> CursorPayload {
    CursorPayload::new(
        HISTORY_SORT,
        BTreeMap::from([
            (NAMESPACE_FILTER_KEY.to_owned(), namespace.to_owned()),
            (
                NAME_FILTER_KEY.to_owned(),
                parent_logical_name_id.to_owned(),
            ),
            (SCOPE_FILTER_KEY.to_owned(), scope.as_str().to_owned()),
        ]),
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

pub(crate) fn history_storage_cursor(
    payload: &CursorPayload,
    namespace: &str,
    parent_logical_name_id: &str,
    scope: HistoryScope,
    snapshot_token: &str,
) -> V2Result<HistoryCursor> {
    if payload.sort != HISTORY_SORT {
        return Err(invalid_history_cursor());
    }
    if payload.snapshot.as_deref() != Some(snapshot_token) {
        return Err(invalid_history_cursor());
    }
    if payload.filters.len() != 3
        || payload
            .filters
            .get(NAMESPACE_FILTER_KEY)
            .map(String::as_str)
            != Some(namespace)
        || payload.filters.get(NAME_FILTER_KEY).map(String::as_str) != Some(parent_logical_name_id)
        || payload.filters.get(SCOPE_FILTER_KEY).map(String::as_str) != Some(scope.as_str())
    {
        return Err(invalid_history_cursor());
    }
    if payload.last_item.len() != 2 {
        return Err(invalid_history_cursor());
    }

    let normalized_event_id = cursor_value(payload, NORMALIZED_EVENT_ID_CURSOR_KEY)?
        .parse::<i64>()
        .map_err(|_| invalid_history_cursor())?;
    let event_identity = cursor_value(payload, EVENT_IDENTITY_CURSOR_KEY)?;

    Ok(HistoryCursor {
        normalized_event_id,
        event_identity,
    })
}

fn history_event_name(row: &StorageHistoryEvent, anchor_name: &str) -> String {
    row.logical_name_id
        .as_deref()
        .and_then(|logical_name_id| logical_name_id.split_once(':').map(|(_, name)| name))
        .filter(|name| !name.trim().is_empty())
        .unwrap_or(anchor_name)
        .to_owned()
}

pub(crate) fn history_storage_scope(scope: HistoryScope) -> bigname_storage::HistoryScope {
    match scope {
        HistoryScope::Name => bigname_storage::HistoryScope::Surface,
        HistoryScope::Registration => bigname_storage::HistoryScope::Resource,
        HistoryScope::Both => bigname_storage::HistoryScope::Both,
    }
}

pub(crate) async fn v2_exact_name_snapshot_scope(
    state: &AppState,
    namespace: &str,
    at: Option<&AtSelector>,
) -> V2Result<SnapshotSelectionScope> {
    v2_exact_name_snapshot_scope_with_resolution_auxiliary(state, namespace, at, false).await
}

pub(crate) async fn v2_exact_name_snapshot_scope_with_resolution_auxiliary(
    state: &AppState,
    namespace: &str,
    at: Option<&AtSelector>,
    include_resolution_auxiliary: bool,
) -> V2Result<SnapshotSelectionScope> {
    let at_positions = at.map(v2_snapshot_scope_at_selector).transpose()?.flatten();
    let selector = at_positions
        .as_deref()
        .map(ExactNameSnapshotSelector::from_at)
        .unwrap_or_default();

    crate::exact_name_snapshot_scope(
        &state.pool,
        namespace,
        selector,
        include_resolution_auxiliary,
    )
    .await
    .map_err(api_error_to_v2)
}

fn v2_snapshot_scope_at_selector(at: &AtSelector) -> V2Result<Option<String>> {
    match at {
        AtSelector::Timestamp(_) => Ok(None),
        AtSelector::SnapshotToken(token) => {
            let SnapshotAt::ResolvedPositions(chain_positions) = decode_at_token(token)? else {
                return Ok(None);
            };
            Ok(Some(chain_positions.to_value().to_string()))
        }
    }
}

pub(crate) fn api_error_to_v2(error: ApiError) -> V2Error {
    match error.code {
        "invalid_input" => V2Error::invalid_input(error.message),
        "not_found" => V2Error::not_found(error.message),
        "unsupported" => V2Error::unsupported(error.message),
        "stale" => V2Error::stale(error.message),
        "conflict" => V2Error::conflict(error.message),
        _ => V2Error::internal_error(error.message),
    }
}

fn cursor_value(payload: &CursorPayload, key: &str) -> V2Result<String> {
    payload
        .last_item
        .get(key)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(invalid_history_cursor)
}

fn invalid_history_cursor() -> V2Error {
    V2Error::invalid_input("cursor must be a valid pagination cursor")
}

pub(crate) fn format_timestamp(value: OffsetDateTime) -> String {
    let value = value.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        value.year(),
        value.month() as u8,
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_cursor_payload_round_trips_storage_cursor() {
        let cursor = HistoryCursor {
            normalized_event_id: 42,
            event_identity: "event:42".to_owned(),
        };
        let payload = history_cursor_payload(
            &cursor,
            "ens",
            "ens:parent.eth",
            HistoryScope::Both,
            "snapshot-1",
        );

        assert_eq!(
            payload.filters,
            BTreeMap::from([
                ("namespace".to_owned(), "ens".to_owned()),
                ("name".to_owned(), "ens:parent.eth".to_owned()),
                ("scope".to_owned(), "both".to_owned()),
            ])
        );

        assert_eq!(
            history_storage_cursor(
                &payload,
                "ens",
                "ens:parent.eth",
                HistoryScope::Both,
                "snapshot-1"
            )
            .expect("cursor must decode"),
            cursor
        );
    }

    #[test]
    fn history_cursor_rejects_wrong_sort_filter_scope_or_snapshot() {
        let cursor = HistoryCursor {
            normalized_event_id: 42,
            event_identity: "event:42".to_owned(),
        };

        let mut payload = history_cursor_payload(
            &cursor,
            "ens",
            "ens:parent.eth",
            HistoryScope::Both,
            "snapshot-1",
        );
        payload.sort = "wrong".to_owned();
        assert!(
            history_storage_cursor(
                &payload,
                "ens",
                "ens:parent.eth",
                HistoryScope::Both,
                "snapshot-1"
            )
            .is_err()
        );

        let mut payload = history_cursor_payload(
            &cursor,
            "ens",
            "ens:parent.eth",
            HistoryScope::Both,
            "snapshot-1",
        );
        payload
            .filters
            .insert("name".to_owned(), "ens:other.eth".to_owned());
        assert!(
            history_storage_cursor(
                &payload,
                "ens",
                "ens:parent.eth",
                HistoryScope::Both,
                "snapshot-1"
            )
            .is_err()
        );

        let payload = history_cursor_payload(
            &cursor,
            "ens",
            "ens:parent.eth",
            HistoryScope::Name,
            "snapshot-1",
        );
        assert!(
            history_storage_cursor(
                &payload,
                "ens",
                "ens:parent.eth",
                HistoryScope::Both,
                "snapshot-1"
            )
            .is_err()
        );

        let mut payload = history_cursor_payload(
            &cursor,
            "ens",
            "ens:parent.eth",
            HistoryScope::Both,
            "snapshot-1",
        );
        payload.snapshot = Some("snapshot-2".to_owned());
        assert!(
            history_storage_cursor(
                &payload,
                "ens",
                "ens:parent.eth",
                HistoryScope::Both,
                "snapshot-1"
            )
            .is_err()
        );
    }

    #[test]
    fn history_event_type_filters_non_product_kinds() {
        assert_eq!(
            history_event_type("RegistrationRenewed"),
            Some(HistoryEventType::Renewal)
        );
        assert_eq!(
            history_event_type("RegistrationReleased"),
            Some(HistoryEventType::Release)
        );
        assert_eq!(
            history_event_type("ExpiryChanged"),
            Some(HistoryEventType::Expiry)
        );
        assert_eq!(
            history_event_type("AuthorityEpochChanged"),
            Some(HistoryEventType::Authority)
        );
        assert_eq!(history_event_type("SurfaceBound"), None);
        assert_eq!(history_event_type("PreimageObserved"), None);
    }
}
