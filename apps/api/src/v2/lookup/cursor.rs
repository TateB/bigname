use std::collections::BTreeMap;

use bigname_storage::{ReverseIdentityCursor, ReverseIdentityRecordRow};

use crate::v2::{CursorPayload, Relation, RelationSet, V2Error, V2Result};

const SORT: &str = "primary_relation_name_namespace_namehash_asc";
const NONE_FILTER_VALUE: &str = "any";

const ADDRESS_FILTER: &str = "address";
const COIN_TYPE_FILTER: &str = "coin_type";
const RELATION_FILTER: &str = "relation";

const IS_PRIMARY_CURSOR: &str = "is_primary";
const ROLE_RANK_CURSOR: &str = "role_rank";
const NORMALIZED_NAME_CURSOR: &str = "normalized_name";
const NAMESPACE_CURSOR: &str = "namespace";
const NAMEHASH_CURSOR: &str = "namehash";

#[derive(Clone, Debug)]
pub(super) struct LookupReverseCursorBinding<'a> {
    pub(super) address: &'a str,
    pub(super) coin_type: u64,
    pub(super) relation: Option<&'a RelationSet>,
}

pub(super) fn lookup_reverse_cursor_payload(
    record: &ReverseIdentityRecordRow,
    binding: &LookupReverseCursorBinding<'_>,
) -> CursorPayload {
    CursorPayload::new(
        SORT,
        cursor_filters(binding),
        reverse_identity_cursor_item(record),
        None,
    )
}

pub(super) fn lookup_reverse_storage_cursor(
    payload: &CursorPayload,
    binding: &LookupReverseCursorBinding<'_>,
) -> V2Result<ReverseIdentityCursor> {
    if payload.sort != SORT {
        return Err(invalid_lookup_cursor());
    }
    if payload.filters != cursor_filters(binding) {
        return Err(invalid_lookup_cursor());
    }
    if payload.last_item.len() != 5 {
        return Err(invalid_lookup_cursor());
    }

    Ok(ReverseIdentityCursor {
        is_primary: cursor_value(payload, IS_PRIMARY_CURSOR)?
            .parse::<bool>()
            .map_err(|_| invalid_lookup_cursor())?,
        role_rank: cursor_value(payload, ROLE_RANK_CURSOR)?
            .parse::<i16>()
            .map_err(|_| invalid_lookup_cursor())?,
        normalized_name: cursor_value(payload, NORMALIZED_NAME_CURSOR)?,
        namespace: cursor_value(payload, NAMESPACE_CURSOR)?,
        namehash: cursor_value(payload, NAMEHASH_CURSOR)?,
    })
}

pub(super) fn reverse_identity_sort(
    left: &ReverseIdentityRecordRow,
    right: &ReverseIdentityRecordRow,
) -> std::cmp::Ordering {
    (
        !reverse_identity_is_primary(left),
        reverse_identity_role_rank(left),
        &left.name_record.row.normalized_name,
        &left.name_record.row.namespace,
        &left.name_record.row.namehash,
    )
        .cmp(&(
            !reverse_identity_is_primary(right),
            reverse_identity_role_rank(right),
            &right.name_record.row.normalized_name,
            &right.name_record.row.namespace,
            &right.name_record.row.namehash,
        ))
}

pub(super) fn reverse_identity_storage_cursor(
    record: &ReverseIdentityRecordRow,
) -> ReverseIdentityCursor {
    ReverseIdentityCursor {
        is_primary: reverse_identity_is_primary(record),
        role_rank: reverse_identity_role_rank(record).into(),
        normalized_name: record.name_record.row.normalized_name.clone(),
        namespace: record.name_record.row.namespace.clone(),
        namehash: record.name_record.row.namehash.clone(),
    }
}

pub(super) fn reverse_identity_is_primary(record: &ReverseIdentityRecordRow) -> bool {
    record.primary_name.as_ref().is_some_and(|primary| {
        primary.claim_status == bigname_storage::PrimaryNameClaimStatus::Success
            && primary.normalized_claim_name.as_deref()
                == Some(record.name_record.row.normalized_name.as_str())
    })
}

fn reverse_identity_role_rank(record: &ReverseIdentityRecordRow) -> u8 {
    if record.relation_facets.iter().any(|relation| {
        matches!(
            relation,
            bigname_storage::AddressNameRelation::TokenHolder
                | bigname_storage::AddressNameRelation::Registrant
        )
    }) {
        0
    } else {
        1
    }
}

fn reverse_identity_cursor_item(record: &ReverseIdentityRecordRow) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            IS_PRIMARY_CURSOR.to_owned(),
            reverse_identity_is_primary(record).to_string(),
        ),
        (
            ROLE_RANK_CURSOR.to_owned(),
            reverse_identity_role_rank(record).to_string(),
        ),
        (
            NORMALIZED_NAME_CURSOR.to_owned(),
            record.name_record.row.normalized_name.clone(),
        ),
        (
            NAMESPACE_CURSOR.to_owned(),
            record.name_record.row.namespace.clone(),
        ),
        (
            NAMEHASH_CURSOR.to_owned(),
            record.name_record.row.namehash.clone(),
        ),
    ])
}

fn cursor_filters(binding: &LookupReverseCursorBinding<'_>) -> BTreeMap<String, String> {
    BTreeMap::from([
        (ADDRESS_FILTER.to_owned(), binding.address.to_owned()),
        (COIN_TYPE_FILTER.to_owned(), binding.coin_type.to_string()),
        (
            RELATION_FILTER.to_owned(),
            binding
                .relation
                .map(RelationSet::canonical_value)
                .unwrap_or_else(|| NONE_FILTER_VALUE.to_owned()),
        ),
    ])
}

fn cursor_value(payload: &CursorPayload, key: &str) -> V2Result<String> {
    payload
        .last_item
        .get(key)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(invalid_lookup_cursor)
}

fn invalid_lookup_cursor() -> V2Error {
    V2Error::invalid_input("cursor must match this lookup input")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::{decode, encode};
    use sqlx::types::{Uuid, time::OffsetDateTime};

    #[test]
    fn lookup_reverse_cursor_binds_query_filters_without_snapshot() {
        let binding = LookupReverseCursorBinding {
            address: "0x0000000000000000000000000000000000000abc",
            coin_type: 60,
            relation: Some(&RelationSet::from(Relation::Owner)),
        };
        let mut payload = CursorPayload::new(
            SORT,
            cursor_filters(&binding),
            BTreeMap::from([
                (IS_PRIMARY_CURSOR.to_owned(), "true".to_owned()),
                (ROLE_RANK_CURSOR.to_owned(), "0".to_owned()),
                (NORMALIZED_NAME_CURSOR.to_owned(), "alice.eth".to_owned()),
                (NAMESPACE_CURSOR.to_owned(), "ens".to_owned()),
                (NAMEHASH_CURSOR.to_owned(), "namehash:alice.eth".to_owned()),
            ]),
            Some("head-a".to_owned()),
        );
        assert!(lookup_reverse_storage_cursor(&payload, &binding).is_ok());

        payload.snapshot = Some("head-b".to_owned());
        assert!(lookup_reverse_storage_cursor(&payload, &binding).is_ok());

        payload.snapshot = None;
        assert!(lookup_reverse_storage_cursor(&payload, &binding).is_ok());

        payload
            .filters
            .insert(ADDRESS_FILTER.to_owned(), "0xother".to_owned());
        assert!(lookup_reverse_storage_cursor(&payload, &binding).is_err());

        let minted = lookup_reverse_cursor_payload(&reverse_record(), &binding);
        assert!(minted.snapshot.is_none());

        let encoded = encode(&minted);
        let decoded = decode(&encoded).expect("cursor payload must decode");
        assert!(decoded.snapshot.is_none());
    }

    fn reverse_record() -> ReverseIdentityRecordRow {
        ReverseIdentityRecordRow {
            name_record: bigname_storage::IdentityNameRecordRow {
                row: bigname_storage::IdentityNameCurrentRow {
                    logical_name_id: "ens:alice.eth".to_owned(),
                    namespace: "ens".to_owned(),
                    canonical_display_name: "Alice.eth".to_owned(),
                    normalized_name: "alice.eth".to_owned(),
                    namehash: "namehash:alice.eth".to_owned(),
                    labelhash: None,
                    labelhash_count: None,
                    resource_id: Some(Uuid::from_u128(0x5a0301)),
                    record_inventory_boundary_key: None,
                    declared_summary: serde_json::json!({}),
                    coverage: serde_json::json!({}),
                    chain_positions: serde_json::json!({}),
                    last_recomputed_at: OffsetDateTime::from_unix_timestamp(1)
                        .expect("test timestamp must be valid"),
                },
                record_inventory_current: None,
                relations: Vec::new(),
            },
            relation_facets: vec![bigname_storage::AddressNameRelation::TokenHolder],
            primary_name: None,
            requested_coin_type: "60".to_owned(),
        }
    }
}
