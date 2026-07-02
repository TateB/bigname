use bigname_storage::{NameCurrentListCursor, NameCurrentListCursorValue};

use crate::v2::{CursorPayload, V2Error, V2Result};

const CHAIN_ID_FILTER_KEY: &str = "chain_id";
const RESOLVER_FILTER_KEY: &str = "resolver";
const NAMESPACE_FILTER_KEY: &str = "namespace";
const SORT_VALUE_CURSOR_KEY: &str = "sort_value";
const CURSOR_NAMESPACE_KEY: &str = "namespace";
const NORMALIZED_NAME_CURSOR_KEY: &str = "normalized_name";
const NAMEHASH_CURSOR_KEY: &str = "namehash";
const NONE_FILTER_VALUE: &str = "";

#[derive(Clone, Copy, Debug)]
pub(crate) struct BoundNamesCursorBinding<'a> {
    pub(crate) chain_id: u64,
    pub(crate) resolver_address: &'a str,
    pub(crate) namespace: Option<&'a str>,
    pub(crate) sort: &'a str,
    pub(crate) snapshot_token: &'a str,
}

pub(crate) fn bound_names_cursor_payload(
    cursor: &NameCurrentListCursor,
    binding: &BoundNamesCursorBinding<'_>,
) -> CursorPayload {
    CursorPayload::new(
        binding.sort,
        std::collections::BTreeMap::from([
            (CHAIN_ID_FILTER_KEY.to_owned(), binding.chain_id.to_string()),
            (
                RESOLVER_FILTER_KEY.to_owned(),
                binding.resolver_address.to_owned(),
            ),
            (
                NAMESPACE_FILTER_KEY.to_owned(),
                option_filter(binding.namespace),
            ),
        ]),
        std::collections::BTreeMap::from([
            (SORT_VALUE_CURSOR_KEY.to_owned(), cursor_sort_value(cursor)),
            (CURSOR_NAMESPACE_KEY.to_owned(), cursor.namespace.clone()),
            (
                NORMALIZED_NAME_CURSOR_KEY.to_owned(),
                cursor.normalized_name.clone(),
            ),
            (NAMEHASH_CURSOR_KEY.to_owned(), cursor.namehash.clone()),
        ]),
        Some(binding.snapshot_token.to_owned()),
    )
}

pub(crate) fn bound_names_storage_cursor(
    payload: &CursorPayload,
    binding: &BoundNamesCursorBinding<'_>,
) -> V2Result<NameCurrentListCursor> {
    let expected_chain_id = binding.chain_id.to_string();
    let expected_namespace = option_filter(binding.namespace);
    if payload.sort != binding.sort {
        return Err(invalid_bound_names_cursor());
    }
    if payload.snapshot.as_deref() != Some(binding.snapshot_token) {
        return Err(invalid_bound_names_cursor());
    }
    if payload.filters.len() != 3
        || payload.filters.get(CHAIN_ID_FILTER_KEY).map(String::as_str)
            != Some(expected_chain_id.as_str())
        || payload.filters.get(RESOLVER_FILTER_KEY).map(String::as_str)
            != Some(binding.resolver_address)
        || payload
            .filters
            .get(NAMESPACE_FILTER_KEY)
            .map(String::as_str)
            != Some(expected_namespace.as_str())
    {
        return Err(invalid_bound_names_cursor());
    }
    if payload.last_item.len() != 4 {
        return Err(invalid_bound_names_cursor());
    }

    Ok(NameCurrentListCursor {
        sort_value: NameCurrentListCursorValue::Name(cursor_nonempty_value(
            payload,
            SORT_VALUE_CURSOR_KEY,
        )?),
        namespace: cursor_nonempty_value(payload, CURSOR_NAMESPACE_KEY)?,
        normalized_name: cursor_nonempty_value(payload, NORMALIZED_NAME_CURSOR_KEY)?,
        namehash: cursor_nonempty_value(payload, NAMEHASH_CURSOR_KEY)?,
    })
}

fn cursor_sort_value(cursor: &NameCurrentListCursor) -> String {
    match &cursor.sort_value {
        NameCurrentListCursorValue::Name(value) => value.clone(),
        NameCurrentListCursorValue::Timestamp(_) => String::new(),
    }
}

fn cursor_nonempty_value(payload: &CursorPayload, key: &str) -> V2Result<String> {
    payload
        .last_item
        .get(key)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(invalid_bound_names_cursor)
}

fn invalid_bound_names_cursor() -> V2Error {
    V2Error::invalid_input("cursor must be a valid pagination cursor")
}

fn option_filter(value: Option<&str>) -> String {
    value.unwrap_or(NONE_FILTER_VALUE).to_owned()
}
