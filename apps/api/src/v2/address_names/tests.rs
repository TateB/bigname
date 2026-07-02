use super::*;
use crate::v2::decode;
use bigname_storage::{AddressNamesCurrentSortedCursor, AddressNamesCurrentSortedCursorValue};
use sqlx::types::Uuid;

fn binding(sort: AddressNamesSort) -> AddressNamesCursorBinding<'static> {
    let relation = Box::leak(Box::new(RelationSet::from(Relation::Owner)));
    AddressNamesCursorBinding {
        address: "0x00000000000000000000000000000000000000aa",
        namespace: Some("ens"),
        relation: Some(relation),
        dedupe: AddressNamesDedupe::Name,
        q: Some("al"),
        sort,
        order: SortOrder::Asc,
        snapshot_token: "snapshot-1",
    }
}

#[test]
fn address_names_cursor_payload_round_trips_name_cursor() {
    let cursor = AddressNamesCurrentSortedCursor {
        sort_value: AddressNamesCurrentSortedCursorValue::Name("Alice.eth".to_owned()),
        logical_name_id: "ens:alice.eth".to_owned(),
        resource_id: Uuid::from_u128(0x1234),
    };
    let payload = address_names_cursor_payload(&cursor, &binding(AddressNamesSort::Name));

    assert_eq!(
        address_names_storage_cursor(&payload, &binding(AddressNamesSort::Name))
            .expect("cursor must decode"),
        cursor
    );
    assert_eq!(payload.last_item[SORT_KIND_CURSOR_KEY], SORT_KIND_NAME);
}

#[test]
fn address_names_cursor_payload_distinguishes_timestamp_null_and_value() {
    let null_cursor = AddressNamesCurrentSortedCursor {
        sort_value: AddressNamesCurrentSortedCursorValue::Timestamp(None),
        logical_name_id: "ens:missing-expiry.eth".to_owned(),
        resource_id: Uuid::from_u128(0x1235),
    };
    let null_payload =
        address_names_cursor_payload(&null_cursor, &binding(AddressNamesSort::ExpiresAt));
    assert_eq!(
        null_payload.last_item[SORT_KIND_CURSOR_KEY],
        SORT_KIND_TIMESTAMP_NULL
    );
    assert_eq!(null_payload.last_item[SORT_VALUE_CURSOR_KEY], "");
    assert_eq!(
        address_names_storage_cursor(&null_payload, &binding(AddressNamesSort::ExpiresAt))
            .expect("null timestamp cursor must decode"),
        null_cursor
    );

    let value = bigname_storage::parse_rfc3339_utc_timestamp("2027-01-02T03:04:05Z")
        .expect("timestamp must parse");
    let value_cursor = AddressNamesCurrentSortedCursor {
        sort_value: AddressNamesCurrentSortedCursorValue::Timestamp(Some(value)),
        logical_name_id: "ens:alice.eth".to_owned(),
        resource_id: Uuid::from_u128(0x1236),
    };
    let value_payload =
        address_names_cursor_payload(&value_cursor, &binding(AddressNamesSort::ExpiresAt));
    assert_eq!(
        value_payload.last_item[SORT_KIND_CURSOR_KEY],
        SORT_KIND_TIMESTAMP_VALUE
    );
    assert_eq!(
        value_payload.last_item[SORT_VALUE_CURSOR_KEY],
        "2027-01-02T03:04:05Z"
    );
    assert_eq!(
        address_names_storage_cursor(&value_payload, &binding(AddressNamesSort::ExpiresAt))
            .expect("timestamp cursor must decode"),
        value_cursor
    );
}

#[test]
fn address_names_cursor_rejects_cross_sort_filter_order_or_snapshot() {
    let cursor = AddressNamesCurrentSortedCursor {
        sort_value: AddressNamesCurrentSortedCursorValue::Name("Alice.eth".to_owned()),
        logical_name_id: "ens:alice.eth".to_owned(),
        resource_id: Uuid::from_u128(0x1234),
    };

    let payload = address_names_cursor_payload(&cursor, &binding(AddressNamesSort::Name));
    assert!(address_names_storage_cursor(&payload, &binding(AddressNamesSort::ExpiresAt)).is_err());

    let mut payload = address_names_cursor_payload(&cursor, &binding(AddressNamesSort::Name));
    payload.filters.insert(
        ADDRESS_FILTER_KEY.to_owned(),
        "0x00000000000000000000000000000000000000bb".to_owned(),
    );
    assert!(address_names_storage_cursor(&payload, &binding(AddressNamesSort::Name)).is_err());

    let mut payload = address_names_cursor_payload(&cursor, &binding(AddressNamesSort::Name));
    payload
        .filters
        .insert(ORDER_FILTER_KEY.to_owned(), "desc".to_owned());
    assert!(address_names_storage_cursor(&payload, &binding(AddressNamesSort::Name)).is_err());

    let mut payload = address_names_cursor_payload(&cursor, &binding(AddressNamesSort::Name));
    payload.snapshot = Some("snapshot-2".to_owned());
    assert!(address_names_storage_cursor(&payload, &binding(AddressNamesSort::Name)).is_err());
}

#[test]
fn address_names_cursor_rejects_cross_timestamp_sort_reuse() {
    let cursor = AddressNamesCurrentSortedCursor {
        sort_value: AddressNamesCurrentSortedCursorValue::Timestamp(Some(
            bigname_storage::parse_rfc3339_utc_timestamp("2027-01-02T03:04:05Z")
                .expect("timestamp must parse"),
        )),
        logical_name_id: "ens:alice.eth".to_owned(),
        resource_id: Uuid::from_u128(0x1234),
    };
    let payload = address_names_cursor_payload(&cursor, &binding(AddressNamesSort::ExpiresAt));

    assert!(
        address_names_storage_cursor(&payload, &binding(AddressNamesSort::RegisteredAt)).is_err()
    );
}

#[test]
fn address_names_cursor_token_decodes_to_bound_payload() {
    let cursor = AddressNamesCurrentSortedCursor {
        sort_value: AddressNamesCurrentSortedCursorValue::Name("Alice.eth".to_owned()),
        logical_name_id: "ens:alice.eth".to_owned(),
        resource_id: Uuid::from_u128(0x1234),
    };
    let payload = address_names_cursor_payload(&cursor, &binding(AddressNamesSort::Name));
    let encoded = crate::v2::encode(&payload);

    assert_eq!(
        decode(&encoded).expect("encoded cursor must decode"),
        payload
    );
}
