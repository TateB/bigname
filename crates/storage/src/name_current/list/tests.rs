use serde_json::json;
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

use super::*;
use crate::SurfaceBindingKind;

#[test]
fn name_current_list_cursor_uses_sort_specific_value() {
    let row = NameCurrentListRow {
        row: NameCurrentRow {
            logical_name_id: "ens:alice.eth".to_owned(),
            namespace: "ens".to_owned(),
            canonical_display_name: "Alice.eth".to_owned(),
            normalized_name: "alice.eth".to_owned(),
            namehash: "namehash:alice.eth".to_owned(),
            surface_binding_id: Some(Uuid::from_u128(1)),
            resource_id: Some(Uuid::from_u128(2)),
            token_lineage_id: Some(Uuid::from_u128(3)),
            binding_kind: Some(SurfaceBindingKind::DeclaredRegistryPath),
            declared_summary: json!({}),
            provenance: json!({}),
            coverage: json!({}),
            chain_positions: json!({}),
            canonicality_summary: json!({}),
            manifest_version: 1,
            last_recomputed_at: timestamp(1_717_171_717),
        },
        labelhash: None,
        token_id: None,
        owner: None,
        registrant: None,
        created_at: Some(timestamp(1_717_171_700)),
        registration_date: Some(timestamp(1_717_171_701)),
        expiry_date: Some(timestamp(1_900_000_000)),
        resolver_address: None,
    };

    assert_eq!(
        name_current_list_cursor_from_row(&row, NameCurrentListSort::Name).sort_value,
        NameCurrentListCursorValue::Name("Alice.eth".to_owned())
    );
    assert_eq!(
        name_current_list_cursor_from_row(&row, NameCurrentListSort::ExpiryDate).sort_value,
        NameCurrentListCursorValue::Timestamp(Some(timestamp(1_900_000_000)))
    );
}

#[test]
fn name_current_list_like_filters_escape_wildcards() {
    assert_eq!(escape_like_pattern(r"al%_ice\eth"), r"al\%\_ice\\eth");
}

fn timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
}
