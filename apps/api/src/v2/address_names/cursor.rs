use std::collections::BTreeMap;

use bigname_storage::{AddressNamesCurrentSortedCursor, AddressNamesCurrentSortedCursorValue};
use sqlx::types::Uuid;

use crate::v2::{
    AddressNamesDedupe, AddressNamesSort, CursorPayload, RelationSet, SortOrder, V2Result,
    cursor::{cursor_value, invalid_cursor_error},
    format_timestamp,
};

pub(crate) const ADDRESS_FILTER_KEY: &str = "address";
const NAMESPACE_FILTER_KEY: &str = "namespace";
const RELATION_FILTER_KEY: &str = "relation";
const DEDUPE_FILTER_KEY: &str = "dedupe";
const Q_FILTER_KEY: &str = "q";
pub(crate) const ORDER_FILTER_KEY: &str = "order";
pub(crate) const SORT_KIND_CURSOR_KEY: &str = "sort_kind";
pub(crate) const SORT_VALUE_CURSOR_KEY: &str = "sort_value";
const LOGICAL_NAME_ID_CURSOR_KEY: &str = "logical_name_id";
const RESOURCE_ID_CURSOR_KEY: &str = "resource_id";
pub(crate) const SORT_KIND_NAME: &str = "name";
pub(crate) const SORT_KIND_TIMESTAMP_NULL: &str = "timestamp_null";
pub(crate) const SORT_KIND_TIMESTAMP_VALUE: &str = "timestamp_value";
const NONE_FILTER_VALUE: &str = "";

#[derive(Clone, Debug)]
pub(crate) struct AddressNamesCursorBinding<'a> {
    pub(crate) address: &'a str,
    pub(crate) namespace: Option<&'a str>,
    pub(crate) relation: Option<&'a RelationSet>,
    pub(crate) dedupe: AddressNamesDedupe,
    pub(crate) q: Option<&'a str>,
    pub(crate) sort: AddressNamesSort,
    pub(crate) order: SortOrder,
    pub(crate) snapshot_token: &'a str,
}

pub(crate) fn address_names_cursor_payload(
    cursor: &AddressNamesCurrentSortedCursor,
    binding: &AddressNamesCursorBinding<'_>,
) -> CursorPayload {
    CursorPayload::new(
        binding.sort.as_str(),
        BTreeMap::from([
            (ADDRESS_FILTER_KEY.to_owned(), binding.address.to_owned()),
            (
                NAMESPACE_FILTER_KEY.to_owned(),
                option_filter(binding.namespace),
            ),
            (
                RELATION_FILTER_KEY.to_owned(),
                relation_filter_value(binding.relation),
            ),
            (
                DEDUPE_FILTER_KEY.to_owned(),
                binding.dedupe.as_str().to_owned(),
            ),
            (Q_FILTER_KEY.to_owned(), option_filter(binding.q)),
            (
                ORDER_FILTER_KEY.to_owned(),
                binding.order.as_str().to_owned(),
            ),
        ]),
        cursor_last_item(cursor),
        Some(binding.snapshot_token.to_owned()),
    )
}

pub(crate) fn address_names_storage_cursor(
    payload: &CursorPayload,
    binding: &AddressNamesCursorBinding<'_>,
) -> V2Result<AddressNamesCurrentSortedCursor> {
    if payload.sort != binding.sort.as_str() {
        return Err(invalid_cursor_error());
    }
    if payload.snapshot.as_deref() != Some(binding.snapshot_token) {
        return Err(invalid_cursor_error());
    }
    if payload.filters.len() != 6
        || payload.filters.get(ADDRESS_FILTER_KEY).map(String::as_str) != Some(binding.address)
        || payload
            .filters
            .get(NAMESPACE_FILTER_KEY)
            .map(String::as_str)
            != Some(option_filter(binding.namespace).as_str())
        || payload.filters.get(RELATION_FILTER_KEY).map(String::as_str)
            != Some(relation_filter_value(binding.relation).as_str())
        || payload.filters.get(DEDUPE_FILTER_KEY).map(String::as_str)
            != Some(binding.dedupe.as_str())
        || payload.filters.get(Q_FILTER_KEY).map(String::as_str)
            != Some(option_filter(binding.q).as_str())
        || payload.filters.get(ORDER_FILTER_KEY).map(String::as_str) != Some(binding.order.as_str())
    {
        return Err(invalid_cursor_error());
    }
    if payload.last_item.len() != 4 {
        return Err(invalid_cursor_error());
    }

    let sort_value = cursor_sort_value(payload, binding.sort)?;
    let logical_name_id = cursor_value(payload, LOGICAL_NAME_ID_CURSOR_KEY, invalid_cursor_error)?;
    let resource_id = Uuid::parse_str(&cursor_value(
        payload,
        RESOURCE_ID_CURSOR_KEY,
        invalid_cursor_error,
    )?)
    .map_err(|_| invalid_cursor_error())?;

    Ok(AddressNamesCurrentSortedCursor {
        sort_value,
        logical_name_id,
        resource_id,
    })
}

fn cursor_last_item(cursor: &AddressNamesCurrentSortedCursor) -> BTreeMap<String, String> {
    let (sort_kind, sort_value) = match &cursor.sort_value {
        AddressNamesCurrentSortedCursorValue::Name(value) => {
            (SORT_KIND_NAME.to_owned(), value.clone())
        }
        AddressNamesCurrentSortedCursorValue::Timestamp(None) => {
            (SORT_KIND_TIMESTAMP_NULL.to_owned(), String::new())
        }
        AddressNamesCurrentSortedCursorValue::Timestamp(Some(value)) => (
            SORT_KIND_TIMESTAMP_VALUE.to_owned(),
            format_timestamp(*value),
        ),
    };

    BTreeMap::from([
        (SORT_KIND_CURSOR_KEY.to_owned(), sort_kind),
        (SORT_VALUE_CURSOR_KEY.to_owned(), sort_value),
        (
            LOGICAL_NAME_ID_CURSOR_KEY.to_owned(),
            cursor.logical_name_id.clone(),
        ),
        (
            RESOURCE_ID_CURSOR_KEY.to_owned(),
            cursor.resource_id.to_string(),
        ),
    ])
}

fn cursor_sort_value(
    payload: &CursorPayload,
    sort: AddressNamesSort,
) -> V2Result<AddressNamesCurrentSortedCursorValue> {
    let sort_kind = cursor_value(payload, SORT_KIND_CURSOR_KEY, invalid_cursor_error)?;
    let sort_value = payload
        .last_item
        .get(SORT_VALUE_CURSOR_KEY)
        .cloned()
        .ok_or_else(invalid_cursor_error)?;

    match (sort, sort_kind.as_str()) {
        (AddressNamesSort::Name, SORT_KIND_NAME) if !sort_value.trim().is_empty() => {
            Ok(AddressNamesCurrentSortedCursorValue::Name(sort_value))
        }
        (
            AddressNamesSort::ExpiresAt | AddressNamesSort::RegisteredAt,
            SORT_KIND_TIMESTAMP_NULL,
        ) if sort_value.is_empty() => Ok(AddressNamesCurrentSortedCursorValue::Timestamp(None)),
        (
            AddressNamesSort::ExpiresAt | AddressNamesSort::RegisteredAt,
            SORT_KIND_TIMESTAMP_VALUE,
        ) if !sort_value.trim().is_empty() => {
            let value = bigname_storage::parse_rfc3339_utc_timestamp(&sort_value)
                .map_err(|_| invalid_cursor_error())?;
            Ok(AddressNamesCurrentSortedCursorValue::Timestamp(Some(value)))
        }
        _ => Err(invalid_cursor_error()),
    }
}

fn option_filter(value: Option<&str>) -> String {
    value.unwrap_or(NONE_FILTER_VALUE).to_owned()
}

fn relation_filter_value(value: Option<&RelationSet>) -> String {
    value
        .map(RelationSet::canonical_value)
        .unwrap_or_else(|| NONE_FILTER_VALUE.to_owned())
}
