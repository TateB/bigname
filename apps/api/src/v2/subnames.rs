use std::collections::BTreeMap;

use bigname_storage::{
    ChildrenCurrentKeysetCursor, ChildrenCurrentRow, ChildrenCurrentSummary, NameCurrentRow,
};
use serde::{Deserialize, Serialize};

use super::{
    CursorPayload, RegistrationStatus, V2Error, V2Result, name_record::name_registration_fields,
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
