use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

use super::*;
use crate::{
    AddressNameCurrentRow, AddressNameRelation, CanonicalityState, NameSurface, Resource,
    SurfaceBinding, SurfaceBindingKind, TokenLineage, upsert_address_names_current_rows,
    upsert_name_current_rows, upsert_name_surfaces, upsert_resources, upsert_surface_bindings,
    upsert_token_lineages,
};

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

// --- DB-backed list / count integration tests -------------------------------

async fn test_database() -> Result<TestDatabase> {
    TestDatabase::create_migrated(
        TestDatabaseConfig::new("bigname_storage_name_current_list_test")
            .admin_database("postgres")
            .pool_max_connections(5)
            .parse_context("failed to parse database URL for name_current list tests")
            .admin_connect_context("failed to connect admin pool for name_current list tests")
            .pool_connect_context("failed to connect name_current list test pool"),
        &crate::MIGRATOR,
        "failed to apply migrations for name_current list tests",
    )
    .await
}

#[derive(Clone, Copy)]
struct Refs {
    token_lineage: Uuid,
    resource: Uuid,
    surface_binding: Uuid,
}

fn refs(index: u128) -> Refs {
    Refs {
        token_lineage: Uuid::from_u128(0x0001_0000 + index),
        resource: Uuid::from_u128(0x0002_0000 + index),
        surface_binding: Uuid::from_u128(0x0003_0000 + index),
    }
}

fn token_lineage(token_lineage_id: Uuid) -> TokenLineage {
    TokenLineage {
        token_lineage_id,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: format!("0xlineage{}", token_lineage_id.simple()),
        block_number: 21_000_000,
        provenance: json!({"source": "name_current_list_test", "anchor": "token_lineage"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn resource(resource_id: Uuid, token_lineage_id: Uuid) -> Resource {
    Resource {
        resource_id,
        token_lineage_id: Some(token_lineage_id),
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: format!("0xresource{}", resource_id.simple()),
        block_number: 21_000_001,
        provenance: json!({"source": "name_current_list_test", "anchor": "resource"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn name_surface(logical_name_id: &str, display_name: &str, namehash: &str) -> NameSurface {
    NameSurface {
        logical_name_id: logical_name_id.to_owned(),
        namespace: namespace_from_logical_name_id(logical_name_id).to_owned(),
        input_name: display_name.to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: display_name.to_owned(),
        dns_encoded_name: display_name.as_bytes().to_vec(),
        namehash: namehash.to_owned(),
        labelhashes: vec![format!("labelhash:{display_name}")],
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: format!("0xsurface:{display_name}"),
        block_number: 21_000_002,
        provenance: json!({"source": "name_current_list_test", "anchor": "surface"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn surface_binding(
    surface_binding_id: Uuid,
    logical_name_id: &str,
    resource_id: Uuid,
) -> SurfaceBinding {
    SurfaceBinding {
        surface_binding_id,
        logical_name_id: logical_name_id.to_owned(),
        resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: timestamp(1_717_171_700),
        active_to: None,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: format!("0xbinding{}", surface_binding_id.simple()),
        block_number: 21_000_003,
        provenance: json!({"source": "name_current_list_test", "anchor": "binding"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn name_current_row(
    logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    declared_summary: Value,
    refs: Refs,
) -> NameCurrentRow {
    NameCurrentRow {
        logical_name_id: logical_name_id.to_owned(),
        namespace: namespace_from_logical_name_id(logical_name_id).to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: display_name.to_owned(),
        namehash: namehash.to_owned(),
        surface_binding_id: Some(refs.surface_binding),
        resource_id: Some(refs.resource),
        token_lineage_id: Some(refs.token_lineage),
        binding_kind: Some(SurfaceBindingKind::DeclaredRegistryPath),
        declared_summary,
        provenance: json!({
            "normalized_event_ids": [101, 102],
            "raw_fact_refs": [{"kind": "log", "chain_id": "ethereum-mainnet", "block_hash": "0xabc"}],
            "manifest_versions": [{"source_manifest_id": 7, "manifest_version": 3}],
            "execution_trace_id": null,
            "derivation_kind": "projection_apply"
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ensv1_registry_path"],
            "unsupported_reason": null,
            "enumeration_basis": "exact_name"
        }),
        chain_positions: json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-mainnet": "finalized"
            }
        }),
        manifest_version: 3,
        last_recomputed_at: timestamp(1_717_171_717),
    }
}

fn namespace_from_logical_name_id(logical_name_id: &str) -> &str {
    logical_name_id
        .split_once(':')
        .map(|(namespace, _)| namespace)
        .unwrap_or("ens")
}

fn address_name_current_row(
    address: &str,
    logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    relation: AddressNameRelation,
    refs: Refs,
) -> AddressNameCurrentRow {
    AddressNameCurrentRow {
        address: address.to_owned(),
        logical_name_id: logical_name_id.to_owned(),
        relation,
        namespace: "ens".to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: display_name.to_owned(),
        namehash: namehash.to_owned(),
        surface_binding_id: refs.surface_binding,
        resource_id: refs.resource,
        token_lineage_id: Some(refs.token_lineage),
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        provenance: json!({
            "normalized_event_ids": [1],
            "derivation_kind": "address_names_current_rebuild"
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "enumeration_basis": "address_collection"
        }),
        chain_positions: json!({
            "ethereum": {
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_003,
                "block_hash": "0xbinding",
                "timestamp": "2026-04-17T00:00:03Z"
            }
        }),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": {
                "ethereum-mainnet": "finalized"
            }
        }),
        manifest_version: 3,
        last_recomputed_at: timestamp(1_717_171_717),
    }
}

/// Seed the binding-reference graph (token lineage, resource, surface, binding) plus the
/// `name_current` projection row for a single name. All supporting rows are finalized so the
/// readability filters in the list/count CTEs admit them.
async fn seed_name(
    database: &TestDatabase,
    logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    declared_summary: Value,
    refs: Refs,
) -> Result<()> {
    upsert_token_lineages(database.pool(), &[token_lineage(refs.token_lineage)]).await?;
    upsert_resources(
        database.pool(),
        &[resource(refs.resource, refs.token_lineage)],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[name_surface(logical_name_id, display_name, namehash)],
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &[surface_binding(
            refs.surface_binding,
            logical_name_id,
            refs.resource,
        )],
    )
    .await?;
    upsert_name_current_rows(
        database.pool(),
        &[name_current_row(
            logical_name_id,
            display_name,
            namehash,
            declared_summary,
            refs,
        )],
    )
    .await?;
    Ok(())
}

/// Attach an address→name relation row (reusing the already-seeded binding references) so the
/// address-membership CTE can resolve the name for `owner` / `owner_in` style filters.
async fn seed_owner(
    database: &TestDatabase,
    address: &str,
    logical_name_id: &str,
    display_name: &str,
    namehash: &str,
    relation: AddressNameRelation,
    refs: Refs,
) -> Result<()> {
    upsert_address_names_current_rows(
        database.pool(),
        &[address_name_current_row(
            address,
            logical_name_id,
            display_name,
            namehash,
            relation,
            refs,
        )],
    )
    .await?;
    Ok(())
}

fn registered_summary(authority_kind: &str, owner: &str) -> Value {
    json!({
        "registration": {
            "status": "active",
            "authority_kind": authority_kind
        },
        "control": {
            "owner": owner
        },
        "resolver": {
            "address": "0x0000000000000000000000000000000000000abc"
        }
    })
}

fn names(rows: &[NameCurrentListRow]) -> Vec<String> {
    rows.iter()
        .map(|row| row.row.canonical_display_name.clone())
        .collect()
}

#[tokio::test]
async fn by_namehash_returns_derived_row_and_none_for_unknown() -> Result<()> {
    let database = test_database().await?;
    let owner = "0x000000000000000000000000000000000000a11ce";
    seed_name(
        &database,
        "ens:alice.eth",
        "alice.eth",
        "0xABCDEF0123",
        registered_summary("ens_v2_registry", owner),
        refs(1),
    )
    .await?;

    let found = load_name_current_list_row_by_namehash(database.pool(), "0xabcdef0123")
        .await?
        .expect("namehash lookup must find the seeded name regardless of case");
    assert_eq!(found.row.canonical_display_name, "alice.eth");
    assert_eq!(found.owner.as_deref(), Some(owner));
    assert_eq!(
        found.resolver_address.as_deref(),
        Some("0x0000000000000000000000000000000000000abc")
    );

    assert!(
        load_name_current_list_row_by_namehash(database.pool(), "0xdeadbeef")
            .await?
            .is_none(),
        "unknown namehash must return None"
    );

    database.cleanup().await
}

#[tokio::test]
async fn by_name_returns_derived_row_and_none_for_unknown() -> Result<()> {
    let database = test_database().await?;
    let owner = "0x000000000000000000000000000000000000a11ce";
    seed_name(
        &database,
        "ens:alice.eth",
        "alice.eth",
        "0xABCDEF0123",
        registered_summary("ens_v2_registry", owner),
        refs(1),
    )
    .await?;

    let found = load_name_current_list_row_by_name(database.pool(), "ens", "alice.eth")
        .await?
        .expect("name lookup must find the seeded name");
    assert_eq!(found.row.canonical_display_name, "alice.eth");
    assert_eq!(found.row.namehash, "0xABCDEF0123");
    assert_eq!(found.owner.as_deref(), Some(owner));

    assert!(
        load_name_current_list_row_by_name(database.pool(), "ens", "bob.eth")
            .await?
            .is_none(),
        "unknown name must return None"
    );
    assert!(
        load_name_current_list_row_by_name(database.pool(), "other", "alice.eth")
            .await?
            .is_none(),
        "name lookup is namespace-scoped"
    );
    // The id's shape never reroutes the lookup: a namehash passed as the name matches nothing in
    // the name column (the resolver falls back to the namehash loader for that case).
    assert!(
        load_name_current_list_row_by_name(database.pool(), "ens", "0xABCDEF0123")
            .await?
            .is_none(),
        "a namehash string must not match the name column"
    );

    database.cleanup().await
}

#[tokio::test]
async fn offset_windows_are_stable_and_disjoint() -> Result<()> {
    let database = test_database().await?;
    seed_name(
        &database,
        "ens:alice.eth",
        "alice.eth",
        "0x01",
        registered_summary("registrar", "0x01"),
        refs(1),
    )
    .await?;
    seed_name(
        &database,
        "ens:bob.eth",
        "bob.eth",
        "0x02",
        registered_summary("registrar", "0x02"),
        refs(2),
    )
    .await?;
    seed_name(
        &database,
        "ens:carol.eth",
        "carol.eth",
        "0x03",
        registered_summary("registrar", "0x03"),
        refs(3),
    )
    .await?;

    let filter = NameCurrentListFilter {
        namespace: Some("ens".to_owned()),
        ..NameCurrentListFilter::default()
    };

    let first = load_name_current_list_page_offset(
        database.pool(),
        &filter,
        NameCurrentListSort::Name,
        NameCurrentListOrder::Asc,
        2,
        0,
    )
    .await?;
    let second = load_name_current_list_page_offset(
        database.pool(),
        &filter,
        NameCurrentListSort::Name,
        NameCurrentListOrder::Asc,
        2,
        2,
    )
    .await?;

    assert_eq!(names(&first), vec!["alice.eth", "bob.eth"]);
    assert_eq!(names(&second), vec!["carol.eth"]);

    let full = load_name_current_list_page_offset(
        database.pool(),
        &filter,
        NameCurrentListSort::Name,
        NameCurrentListOrder::Asc,
        10,
        0,
    )
    .await?;
    assert_eq!(names(&full), vec!["alice.eth", "bob.eth", "carol.eth"]);

    database.cleanup().await
}

#[tokio::test]
async fn namespaces_filter_pages_across_namespace_boundary_with_prefix() -> Result<()> {
    let database = test_database().await?;
    seed_name(
        &database,
        "ens:aa.eth",
        "aa.eth",
        "0x01",
        registered_summary("registrar", "0x01"),
        refs(1),
    )
    .await?;
    seed_name(
        &database,
        "basenames:ab.base.eth",
        "ab.base.eth",
        "0x02",
        registered_summary("registrar", "0x02"),
        refs(2),
    )
    .await?;
    seed_name(
        &database,
        "ens:ac.eth",
        "ac.eth",
        "0x03",
        registered_summary("registrar", "0x03"),
        refs(3),
    )
    .await?;
    seed_name(
        &database,
        "internal:ad.internal",
        "ad.internal",
        "0x04",
        registered_summary("registrar", "0x04"),
        refs(4),
    )
    .await?;

    let filter = NameCurrentListFilter {
        namespace: Some("internal".to_owned()),
        namespaces: Some(vec!["ens".to_owned(), "basenames".to_owned()]),
        prefix: Some("a".to_owned()),
        ..NameCurrentListFilter::default()
    };

    let first = load_name_current_list_page(
        database.pool(),
        &filter,
        NameCurrentListSort::Name,
        NameCurrentListOrder::Asc,
        None,
        1,
        false,
    )
    .await?;
    assert_eq!(names(&first.rows), vec!["aa.eth"]);
    assert_eq!(first.rows[0].row.namespace.as_str(), "ens");
    let first_cursor = first.next_cursor.clone().expect("first page must continue");

    let second = load_name_current_list_page(
        database.pool(),
        &filter,
        NameCurrentListSort::Name,
        NameCurrentListOrder::Asc,
        Some(&first_cursor),
        1,
        false,
    )
    .await?;
    assert_eq!(names(&second.rows), vec!["ab.base.eth"]);
    assert_eq!(second.rows[0].row.namespace.as_str(), "basenames");
    let second_cursor = second
        .next_cursor
        .clone()
        .expect("second page must continue");

    let third = load_name_current_list_page(
        database.pool(),
        &filter,
        NameCurrentListSort::Name,
        NameCurrentListOrder::Asc,
        Some(&second_cursor),
        1,
        false,
    )
    .await?;
    assert_eq!(names(&third.rows), vec!["ac.eth"]);
    assert_eq!(third.rows[0].row.namespace.as_str(), "ens");
    assert!(third.next_cursor.is_none());

    database.cleanup().await
}

#[tokio::test]
async fn owner_in_unions_multiple_owners() -> Result<()> {
    let database = test_database().await?;
    let owner_a = "0x000000000000000000000000000000000000000a";
    let owner_b = "0x000000000000000000000000000000000000000b";

    seed_name(
        &database,
        "ens:alice.eth",
        "alice.eth",
        "0x01",
        registered_summary("ens_v2_registry", owner_a),
        refs(1),
    )
    .await?;
    seed_owner(
        &database,
        owner_a,
        "ens:alice.eth",
        "alice.eth",
        "0x01",
        AddressNameRelation::TokenHolder,
        refs(1),
    )
    .await?;
    seed_name(
        &database,
        "ens:bob.eth",
        "bob.eth",
        "0x02",
        registered_summary("ens_v2_registry", owner_a),
        refs(2),
    )
    .await?;
    seed_owner(
        &database,
        owner_a,
        "ens:bob.eth",
        "bob.eth",
        "0x02",
        AddressNameRelation::TokenHolder,
        refs(2),
    )
    .await?;
    seed_name(
        &database,
        "ens:carol.eth",
        "carol.eth",
        "0x03",
        registered_summary("ens_v2_registry", owner_b),
        refs(3),
    )
    .await?;
    seed_owner(
        &database,
        owner_b,
        "ens:carol.eth",
        "carol.eth",
        "0x03",
        AddressNameRelation::TokenHolder,
        refs(3),
    )
    .await?;

    let union_filter = NameCurrentListFilter {
        namespace: Some("ens".to_owned()),
        address: Some(NameCurrentAddressFilter {
            address: owner_a.to_owned(),
            relation: NameCurrentAddressRelationFilter::Relation(AddressNameRelation::TokenHolder),
            addresses: Some(vec![owner_a.to_owned(), owner_b.to_owned()]),
        }),
        ..NameCurrentListFilter::default()
    };
    let union_rows = load_name_current_list_page_offset(
        database.pool(),
        &union_filter,
        NameCurrentListSort::Name,
        NameCurrentListOrder::Asc,
        10,
        0,
    )
    .await?;
    assert_eq!(
        names(&union_rows),
        vec!["alice.eth", "bob.eth", "carol.eth"]
    );

    let single_filter = NameCurrentListFilter {
        namespace: Some("ens".to_owned()),
        address: Some(NameCurrentAddressFilter {
            address: owner_a.to_owned(),
            relation: NameCurrentAddressRelationFilter::Relation(AddressNameRelation::TokenHolder),
            addresses: None,
        }),
        ..NameCurrentListFilter::default()
    };
    let single_rows = load_name_current_list_page_offset(
        database.pool(),
        &single_filter,
        NameCurrentListSort::Name,
        NameCurrentListOrder::Asc,
        10,
        0,
    )
    .await?;
    assert_eq!(names(&single_rows), vec!["alice.eth", "bob.eth"]);

    database.cleanup().await
}

#[tokio::test]
async fn is_migrated_counts_only_v2_registry_rows() -> Result<()> {
    let database = test_database().await?;
    seed_name(
        &database,
        "ens:alice.eth",
        "alice.eth",
        "0x01",
        registered_summary("ens_v2_registry", "0x01"),
        refs(1),
    )
    .await?;
    seed_name(
        &database,
        "ens:bob.eth",
        "bob.eth",
        "0x02",
        registered_summary("registrar", "0x02"),
        refs(2),
    )
    .await?;

    let all = NameCurrentListFilter {
        namespace: Some("ens".to_owned()),
        ..NameCurrentListFilter::default()
    };
    assert_eq!(count_name_current_list(database.pool(), &all).await?, 2);

    let migrated = NameCurrentListFilter {
        namespace: Some("ens".to_owned()),
        is_migrated: Some(true),
        ..NameCurrentListFilter::default()
    };
    assert_eq!(
        count_name_current_list(database.pool(), &migrated).await?,
        1
    );

    let migrated_rows = load_name_current_list_page_offset(
        database.pool(),
        &migrated,
        NameCurrentListSort::Name,
        NameCurrentListOrder::Asc,
        10,
        0,
    )
    .await?;
    assert_eq!(names(&migrated_rows), vec!["alice.eth"]);

    database.cleanup().await
}
