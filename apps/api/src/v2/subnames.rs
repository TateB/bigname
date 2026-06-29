use std::collections::BTreeMap;

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_storage::{
    ChildrenCurrentKeysetCursor, ChildrenCurrentRow, ChildrenCurrentSummary, NameCurrentRow,
};
use serde::{Deserialize, Serialize};

use crate::{
    AppState, load_name_current_for_selected_snapshot, map_internal_api_error,
    normalize_inferred_route_name,
};

use super::{
    CursorPayload, Envelope, Meta, Page, QueryParams, RegistrationStatus, V2Error, V2Result,
    api_error_to_v2, as_of_meta, decode, encode, encode_at_token,
    name_record::name_registration_fields, resolve_v2_snapshot, v2_exact_name_snapshot_scope,
};

const SUBNAMES_SORT: &str = "display_name_asc";
const DISPLAY_NAME_CURSOR_KEY: &str = "display_name";
const CHILD_LOGICAL_NAME_ID_CURSOR_KEY: &str = "child_logical_name_id";
const NAMESPACE_FILTER_KEY: &str = "namespace";
const PARENT_FILTER_KEY: &str = "parent";

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct Subname {
    pub(crate) name: String,
    pub(crate) display_name: String,
    pub(crate) namespace: String,
    pub(crate) namehash: String,
    pub(crate) labelhash: Option<String>,
    pub(crate) owner: Option<String>,
    pub(crate) registrant: Option<String>,
    pub(crate) registration_status: RegistrationStatus,
    pub(crate) registered_at: Option<String>,
    pub(crate) created_at: Option<String>,
    pub(crate) expires_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) subname_count: Option<u64>,
}

pub(crate) async fn get_subnames(
    Path(input_name): Path<String>,
    params: QueryParams,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<Vec<Subname>>>> {
    let normalized = normalize_inferred_route_name(&input_name)
        .map_err(|error| V2Error::invalid_input(error.message))?;
    let namespace = params
        .namespace
        .clone()
        .unwrap_or_else(|| normalized.namespace.to_owned());
    let include_counts = subnames_include_counts(&params.include)?;

    let scope = v2_exact_name_snapshot_scope(&state, &namespace).await?;
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
                "failed to load subnames for {}/{}",
                namespace, normalized.normalized_name
            ),
        ))
    })?;

    let snapshot_token = encode_at_token(&selected_snapshot);
    let storage_cursor = params
        .cursor
        .as_deref()
        .map(|cursor| {
            let payload = decode(cursor)?;
            subname_storage_cursor(
                &payload,
                &namespace,
                &parent.logical_name_id,
                &snapshot_token,
            )
        })
        .transpose()?;

    let storage_page = bigname_storage::load_children_current_page(
        &state.pool,
        &parent.logical_name_id,
        storage_cursor.as_ref(),
        params.page_size,
    )
    .await
    .map_err(|_| {
        V2Error::internal_error(format!(
            "failed to load subnames for {}/{}",
            namespace, normalized.normalized_name
        ))
    })?;

    let child_logical_name_ids = storage_page
        .rows
        .iter()
        .map(|row| row.child_logical_name_id.clone())
        .collect::<Vec<_>>();
    let child_name_rows = bigname_storage::load_name_current_by_logical_name_ids(
        &state.pool,
        &child_logical_name_ids,
    )
    .await
    .map_err(|_| {
        V2Error::internal_error(format!(
            "failed to load subname registration summaries for {}/{}",
            namespace, normalized.normalized_name
        ))
    })?;
    let child_summaries = if include_counts {
        bigname_storage::load_children_current_summaries(&state.pool, &child_logical_name_ids)
            .await
            .map_err(|_| {
                V2Error::internal_error(format!(
                    "failed to load subname counts for {}/{}",
                    namespace, normalized.normalized_name
                ))
            })?
            .into_iter()
            .map(|summary| (summary.parent_logical_name_id.clone(), summary))
            .collect()
    } else {
        std::collections::BTreeMap::new()
    };

    let next_cursor = storage_page.next_cursor.as_ref().map(|cursor| {
        encode(&subname_cursor_payload(
            cursor,
            &namespace,
            &parent.logical_name_id,
            &snapshot_token,
        ))
    });
    let has_more = next_cursor.is_some();
    let data = storage_page
        .rows
        .iter()
        .map(|row| {
            build_subname(
                row,
                child_name_rows.get(&row.child_logical_name_id),
                child_summaries.get(&row.child_logical_name_id),
                include_counts,
            )
        })
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

pub(crate) fn build_subname(
    row: &ChildrenCurrentRow,
    name_row: Option<&NameCurrentRow>,
    summary: Option<&ChildrenCurrentSummary>,
    include_counts: bool,
) -> Subname {
    let registration = name_registration_fields(name_row, &row.namespace);
    let (owner, registrant) = if name_row.is_some() {
        (registration.owner, registration.registrant)
    } else {
        (row.owner.clone(), row.registrant.clone())
    };

    Subname {
        name: row.normalized_name.clone(),
        display_name: row.canonical_display_name.clone(),
        namespace: row.namespace.clone(),
        namehash: row.namehash.clone(),
        labelhash: row.labelhash.clone(),
        owner,
        registrant,
        registration_status: registration.registration_status,
        registered_at: registration.registered_at,
        created_at: registration.created_at,
        expires_at: registration.expires_at,
        subname_count: include_counts.then(|| {
            summary
                .and_then(|summary| u64::try_from(summary.child_count).ok())
                .unwrap_or_default()
        }),
    }
}

pub(crate) fn subname_cursor_payload(
    cursor: &ChildrenCurrentKeysetCursor,
    namespace: &str,
    parent_logical_name_id: &str,
    snapshot_token: &str,
) -> CursorPayload {
    CursorPayload::new(
        SUBNAMES_SORT,
        BTreeMap::from([
            (NAMESPACE_FILTER_KEY.to_owned(), namespace.to_owned()),
            (
                PARENT_FILTER_KEY.to_owned(),
                parent_logical_name_id.to_owned(),
            ),
        ]),
        BTreeMap::from([
            (
                DISPLAY_NAME_CURSOR_KEY.to_owned(),
                cursor.canonical_display_name.clone(),
            ),
            (
                CHILD_LOGICAL_NAME_ID_CURSOR_KEY.to_owned(),
                cursor.child_logical_name_id.clone(),
            ),
        ]),
        Some(snapshot_token.to_owned()),
    )
}

pub(crate) fn subname_storage_cursor(
    payload: &CursorPayload,
    namespace: &str,
    parent_logical_name_id: &str,
    snapshot_token: &str,
) -> V2Result<ChildrenCurrentKeysetCursor> {
    if payload.sort != SUBNAMES_SORT {
        return Err(invalid_subname_cursor());
    }
    if payload.snapshot.as_deref() != Some(snapshot_token) {
        return Err(invalid_subname_cursor());
    }
    if payload.filters.len() != 2
        || payload
            .filters
            .get(NAMESPACE_FILTER_KEY)
            .map(String::as_str)
            != Some(namespace)
        || payload.filters.get(PARENT_FILTER_KEY).map(String::as_str)
            != Some(parent_logical_name_id)
    {
        return Err(invalid_subname_cursor());
    }
    if payload.last_item.len() != 2 {
        return Err(invalid_subname_cursor());
    }

    let canonical_display_name = cursor_value(payload, DISPLAY_NAME_CURSOR_KEY)?;
    let child_logical_name_id = cursor_value(payload, CHILD_LOGICAL_NAME_ID_CURSOR_KEY)?;

    Ok(ChildrenCurrentKeysetCursor {
        canonical_display_name,
        child_logical_name_id,
    })
}

fn cursor_value(payload: &CursorPayload, key: &str) -> V2Result<String> {
    payload
        .last_item
        .get(key)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(invalid_subname_cursor)
}

fn invalid_subname_cursor() -> V2Error {
    V2Error::invalid_input("cursor must be a valid pagination cursor")
}

fn subnames_include_counts(include: &[String]) -> V2Result<bool> {
    let mut include_counts = false;
    for value in include {
        match value.as_str() {
            "counts" => include_counts = true,
            _ => return Err(V2Error::invalid_input("include must contain only counts")),
        }
    }
    Ok(include_counts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subname_cursor_payload_round_trips_storage_cursor() {
        let cursor = ChildrenCurrentKeysetCursor {
            canonical_display_name: "alice.eth".to_owned(),
            child_logical_name_id: "ens:alice.eth".to_owned(),
        };
        let payload = subname_cursor_payload(&cursor, "ens", "ens:parent.eth", "snapshot-1");

        assert_eq!(
            payload.filters,
            BTreeMap::from([
                ("namespace".to_owned(), "ens".to_owned()),
                ("parent".to_owned(), "ens:parent.eth".to_owned()),
            ])
        );

        assert_eq!(
            subname_storage_cursor(&payload, "ens", "ens:parent.eth", "snapshot-1")
                .expect("cursor must decode"),
            cursor
        );
    }

    #[test]
    fn subname_cursor_rejects_wrong_sort_filter_or_snapshot() {
        let cursor = ChildrenCurrentKeysetCursor {
            canonical_display_name: "alice.eth".to_owned(),
            child_logical_name_id: "ens:alice.eth".to_owned(),
        };
        let mut payload = subname_cursor_payload(&cursor, "ens", "ens:parent.eth", "snapshot-1");

        payload.sort = "wrong".to_owned();
        assert!(subname_storage_cursor(&payload, "ens", "ens:parent.eth", "snapshot-1").is_err());

        let mut payload = subname_cursor_payload(&cursor, "ens", "ens:parent.eth", "snapshot-1");
        payload
            .filters
            .insert("namespace".to_owned(), "basenames".to_owned());
        assert!(subname_storage_cursor(&payload, "ens", "ens:parent.eth", "snapshot-1").is_err());

        let mut payload = subname_cursor_payload(&cursor, "ens", "ens:parent.eth", "snapshot-1");
        payload.snapshot = Some("snapshot-2".to_owned());
        assert!(subname_storage_cursor(&payload, "ens", "ens:parent.eth", "snapshot-1").is_err());
    }

    #[test]
    fn subname_cursor_rejects_wrong_parent_filter() {
        let cursor = ChildrenCurrentKeysetCursor {
            canonical_display_name: "alice.eth".to_owned(),
            child_logical_name_id: "ens:alice.eth".to_owned(),
        };
        let payload = subname_cursor_payload(&cursor, "ens", "ens:parent-a.eth", "snapshot-1");

        assert!(subname_storage_cursor(&payload, "ens", "ens:parent-b.eth", "snapshot-1").is_err());
    }
}
