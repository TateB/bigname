use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;

use super::*;
use crate::{
    CanonicalityState, ChainPositions, NameSurface, Resource, SnapshotProjectionRead,
    SnapshotSelectionErrorKind, SurfaceBinding, TokenLineage, upsert_name_surfaces,
    upsert_resources, upsert_surface_bindings, upsert_token_lineages,
};

async fn test_database() -> Result<TestDatabase> {
    TestDatabase::create_migrated(
        TestDatabaseConfig::new("bigname_storage_name_current_test")
            .admin_database("postgres")
            .pool_max_connections(5)
            .parse_context("failed to parse database URL for name_current tests")
            .admin_connect_context("failed to connect admin pool for name_current tests")
            .pool_connect_context("failed to connect name_current test pool"),
        &crate::MIGRATOR,
        "failed to apply migrations for name_current tests",
    )
    .await
}

fn timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
}

fn token_lineage(token_lineage_id: Uuid) -> TokenLineage {
    TokenLineage {
        token_lineage_id,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xlineage".to_owned(),
        block_number: 21_000_000,
        provenance: json!({"source": "name_current_test", "anchor": "token_lineage"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn resource(resource_id: Uuid, token_lineage_id: Option<Uuid>) -> Resource {
    Resource {
        resource_id,
        token_lineage_id,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xresource".to_owned(),
        block_number: 21_000_001,
        provenance: json!({"source": "name_current_test", "anchor": "resource"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn name_surface(logical_name_id: &str, display_name: &str) -> NameSurface {
    NameSurface {
        logical_name_id: logical_name_id.to_owned(),
        namespace: "ens".to_owned(),
        input_name: display_name.to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: display_name.to_owned(),
        dns_encoded_name: display_name.as_bytes().to_vec(),
        namehash: format!("namehash:{display_name}"),
        labelhashes: vec![format!("labelhash:{display_name}")],
        normalizer_version: "ensip15@2026-04-16".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xsurface".to_owned(),
        block_number: 21_000_002,
        provenance: json!({"source": "name_current_test", "anchor": "surface"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn surface_binding(
    surface_binding_id: Uuid,
    logical_name_id: &str,
    resource_id: Uuid,
    active_from: OffsetDateTime,
    active_to: Option<OffsetDateTime>,
    block_hash: &str,
    block_number: i64,
) -> SurfaceBinding {
    SurfaceBinding {
        surface_binding_id,
        logical_name_id: logical_name_id.to_owned(),
        resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from,
        active_to,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        provenance: json!({"source": "name_current_test", "anchor": "binding"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

async fn seed_binding_references(
    database: &TestDatabase,
    logical_name_id: &str,
    display_name: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
) -> Result<()> {
    upsert_token_lineages(database.pool(), &[token_lineage(token_lineage_id)]).await?;
    upsert_resources(
        database.pool(),
        &[resource(resource_id, Some(token_lineage_id))],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[name_surface(logical_name_id, display_name)],
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &[surface_binding(
            surface_binding_id,
            logical_name_id,
            resource_id,
            timestamp(1_717_171_700),
            None,
            "0xbinding",
            21_000_003,
        )],
    )
    .await?;
    Ok(())
}

async fn orphan_resource(database: &TestDatabase, resource_id: Uuid) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE resources
        SET canonicality_state = 'orphaned'::canonicality_state
        WHERE resource_id = $1
        "#,
    )
    .bind(resource_id)
    .execute(database.pool())
    .await?;
    Ok(())
}

fn name_current_row(
    logical_name_id: &str,
    surface_binding_id: Uuid,
    resource_id: Uuid,
    token_lineage_id: Uuid,
) -> NameCurrentRow {
    NameCurrentRow {
        logical_name_id: logical_name_id.to_owned(),
        namespace: "ens".to_owned(),
        canonical_display_name: "alice.eth".to_owned(),
        normalized_name: "alice.eth".to_owned(),
        namehash: "namehash:alice.eth".to_owned(),
        surface_binding_id: Some(surface_binding_id),
        resource_id: Some(resource_id),
        token_lineage_id: Some(token_lineage_id),
        binding_kind: Some(SurfaceBindingKind::DeclaredRegistryPath),
        declared_summary: json!({
            "registration": {
                "status": "active",
                "authority_kind": "registrar"
            },
            "resolver": {
                "address": "0x0000000000000000000000000000000000000abc"
            }
        }),
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

#[tokio::test]
async fn name_current_upserts_and_loads_exact_name_projection() -> Result<()> {
    let database = test_database().await?;
    let logical_name_id = "ens:alice.eth";
    let token_lineage_id = Uuid::from_u128(0x1100);
    let resource_id = Uuid::from_u128(0x2200);
    let surface_binding_id = Uuid::from_u128(0x3300);

    seed_binding_references(
        &database,
        logical_name_id,
        "alice.eth",
        resource_id,
        token_lineage_id,
        surface_binding_id,
    )
    .await?;

    let expected = name_current_row(
        logical_name_id,
        surface_binding_id,
        resource_id,
        token_lineage_id,
    );
    let inserted =
        upsert_name_current_rows(database.pool(), std::slice::from_ref(&expected)).await?;
    assert_eq!(inserted, vec![expected.clone()]);

    let loaded = load_name_current(database.pool(), logical_name_id).await?;
    assert_eq!(loaded, Some(expected));

    database.cleanup().await
}

#[tokio::test]
async fn name_current_snapshot_read_fails_stale_on_position_mismatch() -> Result<()> {
    let database = test_database().await?;
    let logical_name_id = "ens:alice.eth";
    let token_lineage_id = Uuid::from_u128(0x1110);
    let resource_id = Uuid::from_u128(0x2220);
    let surface_binding_id = Uuid::from_u128(0x3330);

    seed_binding_references(
        &database,
        logical_name_id,
        "alice.eth",
        resource_id,
        token_lineage_id,
        surface_binding_id,
    )
    .await?;

    let expected = name_current_row(
        logical_name_id,
        surface_binding_id,
        resource_id,
        token_lineage_id,
    );
    upsert_name_current_rows(database.pool(), std::slice::from_ref(&expected)).await?;

    let selected = ChainPositions::from_value(&expected.chain_positions)?;
    assert_eq!(
        load_name_current_for_snapshot(database.pool(), logical_name_id, &selected).await?,
        SnapshotProjectionRead::Found(expected)
    );

    let stale_selected = ChainPositions::from_value(&json!({
        "ethereum": {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_004,
            "block_hash": "0xnewer",
            "timestamp": "2026-04-17T00:00:04Z"
        }
    }))?;
    let error = load_name_current_for_snapshot(database.pool(), logical_name_id, &stale_selected)
        .await
        .expect_err("mismatched selected snapshot must be stale");
    assert_eq!(error.kind(), SnapshotSelectionErrorKind::Stale);

    database.cleanup().await
}

#[tokio::test]
async fn name_current_batch_loads_found_rows_by_logical_name_id() -> Result<()> {
    let database = test_database().await?;
    let alice_logical_name_id = "ens:alice.eth";
    let bob_logical_name_id = "ens:bob.eth";

    seed_binding_references(
        &database,
        alice_logical_name_id,
        "alice.eth",
        Uuid::from_u128(0x9200),
        Uuid::from_u128(0x9100),
        Uuid::from_u128(0x9300),
    )
    .await?;
    seed_binding_references(
        &database,
        bob_logical_name_id,
        "bob.eth",
        Uuid::from_u128(0xa200),
        Uuid::from_u128(0xa100),
        Uuid::from_u128(0xa300),
    )
    .await?;

    let alice = name_current_row(
        alice_logical_name_id,
        Uuid::from_u128(0x9300),
        Uuid::from_u128(0x9200),
        Uuid::from_u128(0x9100),
    );
    let mut bob = name_current_row(
        bob_logical_name_id,
        Uuid::from_u128(0xa300),
        Uuid::from_u128(0xa200),
        Uuid::from_u128(0xa100),
    );
    bob.canonical_display_name = "bob.eth".to_owned();
    bob.normalized_name = "bob.eth".to_owned();
    bob.namehash = "namehash:bob.eth".to_owned();

    upsert_name_current_rows(database.pool(), &[alice.clone(), bob.clone()]).await?;

    let requested = vec![
        bob_logical_name_id.to_owned(),
        "ens:missing.eth".to_owned(),
        alice_logical_name_id.to_owned(),
        bob_logical_name_id.to_owned(),
    ];
    let loaded = load_name_current_by_logical_name_ids(database.pool(), &requested).await?;

    assert_eq!(loaded.len(), 2);
    assert_eq!(
        loaded.keys().cloned().collect::<Vec<_>>(),
        vec![
            alice_logical_name_id.to_owned(),
            bob_logical_name_id.to_owned()
        ]
    );
    assert_eq!(loaded.get(alice_logical_name_id), Some(&alice));
    assert_eq!(loaded.get(bob_logical_name_id), Some(&bob));
    assert!(!loaded.contains_key("ens:missing.eth"));
    assert_eq!(
        NameCurrentRow::load_by_logical_name_ids(database.pool(), &requested).await?,
        loaded
    );

    database.cleanup().await
}

#[tokio::test]
async fn name_current_excludes_rows_with_orphaned_backing_resources() -> Result<()> {
    let database = test_database().await?;
    let logical_name_id = "ens:alice.eth";
    let token_lineage_id = Uuid::from_u128(0xb100);
    let resource_id = Uuid::from_u128(0xb200);
    let surface_binding_id = Uuid::from_u128(0xb300);

    seed_binding_references(
        &database,
        logical_name_id,
        "alice.eth",
        resource_id,
        token_lineage_id,
        surface_binding_id,
    )
    .await?;
    upsert_name_current_rows(
        database.pool(),
        &[name_current_row(
            logical_name_id,
            surface_binding_id,
            resource_id,
            token_lineage_id,
        )],
    )
    .await?;

    orphan_resource(&database, resource_id).await?;

    assert_eq!(
        load_name_current(database.pool(), logical_name_id).await?,
        None
    );

    let loaded =
        load_name_current_by_logical_name_ids(database.pool(), &[logical_name_id.to_owned()])
            .await?;
    assert!(loaded.is_empty());
    assert_eq!(
        NameCurrentRow::load_by_logical_name_ids(database.pool(), &[logical_name_id.to_owned()])
            .await?,
        loaded
    );

    database.cleanup().await
}

#[tokio::test]
async fn name_current_upsert_replaces_existing_projection_row() -> Result<()> {
    let database = test_database().await?;
    let logical_name_id = "ens:alice.eth";
    let first_token_lineage_id = Uuid::from_u128(0x4100);
    let first_resource_id = Uuid::from_u128(0x4200);
    let first_surface_binding_id = Uuid::from_u128(0x4300);

    seed_binding_references(
        &database,
        logical_name_id,
        "alice.eth",
        first_resource_id,
        first_token_lineage_id,
        first_surface_binding_id,
    )
    .await?;

    let first = name_current_row(
        logical_name_id,
        first_surface_binding_id,
        first_resource_id,
        first_token_lineage_id,
    );
    upsert_name_current_rows(database.pool(), std::slice::from_ref(&first)).await?;

    let mut replacement = name_current_row(
        logical_name_id,
        first_surface_binding_id,
        first_resource_id,
        first_token_lineage_id,
    );
    replacement.declared_summary = json!({
        "registration": {
            "status": "wrapped",
            "authority_kind": "wrapper"
        }
    });
    replacement.coverage = json!({
        "status": "partial",
        "exhaustiveness": "authoritative",
        "source_classes_considered": ["ensv1_registry_path", "wrapped_name"],
        "unsupported_reason": null,
        "enumeration_basis": "exact_name"
    });
    replacement.manifest_version = 4;

    let updated =
        upsert_name_current_rows(database.pool(), std::slice::from_ref(&replacement)).await?;
    assert_eq!(updated, vec![replacement.clone()]);
    assert_eq!(
        load_name_current(database.pool(), logical_name_id).await?,
        Some(replacement)
    );

    database.cleanup().await
}

#[tokio::test]
async fn name_current_delete_and_clear_support_rebuild_workflows() -> Result<()> {
    let database = test_database().await?;
    let first_logical_name_id = "ens:alice.eth";
    let second_logical_name_id = "ens:bob.eth";

    seed_binding_references(
        &database,
        first_logical_name_id,
        "alice.eth",
        Uuid::from_u128(0x6200),
        Uuid::from_u128(0x6100),
        Uuid::from_u128(0x6300),
    )
    .await?;
    seed_binding_references(
        &database,
        second_logical_name_id,
        "bob.eth",
        Uuid::from_u128(0x7200),
        Uuid::from_u128(0x7100),
        Uuid::from_u128(0x7300),
    )
    .await?;

    let first = name_current_row(
        first_logical_name_id,
        Uuid::from_u128(0x6300),
        Uuid::from_u128(0x6200),
        Uuid::from_u128(0x6100),
    );
    let mut second = name_current_row(
        second_logical_name_id,
        Uuid::from_u128(0x7300),
        Uuid::from_u128(0x7200),
        Uuid::from_u128(0x7100),
    );
    second.canonical_display_name = "bob.eth".to_owned();
    second.normalized_name = "bob.eth".to_owned();
    second.namehash = "namehash:bob.eth".to_owned();
    second.chain_positions = json!({
        "ethereum": {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_004,
            "block_hash": "0xbbbb",
            "timestamp": "2026-04-17T00:00:04Z"
        }
    });

    upsert_name_current_rows(database.pool(), &[first, second]).await?;

    assert_eq!(
        delete_name_current(database.pool(), first_logical_name_id).await?,
        1
    );
    assert_eq!(
        load_name_current(database.pool(), first_logical_name_id).await?,
        None
    );

    assert_eq!(clear_name_current(database.pool()).await?, 1);
    assert_eq!(
        load_name_current(database.pool(), second_logical_name_id).await?,
        None
    );

    database.cleanup().await
}

#[tokio::test]
async fn name_current_rejects_partial_binding_refs() -> Result<()> {
    let database = test_database().await?;
    let logical_name_id = "ens:alice.eth";

    upsert_name_surfaces(
        database.pool(),
        &[name_surface(logical_name_id, "alice.eth")],
    )
    .await?;

    let invalid = NameCurrentRow {
        logical_name_id: logical_name_id.to_owned(),
        namespace: "ens".to_owned(),
        canonical_display_name: "alice.eth".to_owned(),
        normalized_name: "alice.eth".to_owned(),
        namehash: "namehash:alice.eth".to_owned(),
        surface_binding_id: None,
        resource_id: Some(Uuid::from_u128(0x8200)),
        token_lineage_id: None,
        binding_kind: Some(SurfaceBindingKind::DeclaredRegistryPath),
        declared_summary: json!({}),
        provenance: json!({}),
        coverage: json!({}),
        chain_positions: json!({}),
        canonicality_summary: json!({}),
        manifest_version: 1,
        last_recomputed_at: timestamp(1_717_171_800),
    };

    let error = upsert_name_current_rows(database.pool(), &[invalid])
        .await
        .expect_err("partial binding refs must be rejected");
    assert!(
        error
            .to_string()
            .contains("must provide surface_binding_id, resource_id, and binding_kind together"),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}
