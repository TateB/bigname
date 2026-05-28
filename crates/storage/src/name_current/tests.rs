use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

use super::*;
use crate::{
    CanonicalityState, ChainLineageBlock, ChainPositions, NameSurface, NormalizedEvent, Resource,
    SnapshotProjectionRead, SnapshotSelectionErrorKind, SurfaceBinding, SurfaceBindingKind,
    TokenLineage, upsert_chain_lineage_blocks, upsert_name_surfaces, upsert_normalized_events,
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
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
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

fn normalized_event(
    logical_name_id: &str,
    resource_id: Uuid,
    block_hash: &str,
    block_number: i64,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!("name-current-test:{logical_name_id}:{block_number}"),
        namespace: "ens".to_owned(),
        logical_name_id: Some(logical_name_id.to_owned()),
        resource_id: Some(resource_id),
        event_kind: "ResolverChanged".to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("ethereum-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(block_hash.to_owned()),
        transaction_hash: Some(format!("0xtx{block_number}")),
        log_index: Some(0),
        raw_fact_ref: json!({}),
        derivation_kind: "name_current_test".to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({}),
    }
}

fn lineage_block(
    block_hash: &str,
    parent_hash: Option<&str>,
    block_number: i64,
) -> ChainLineageBlock {
    ChainLineageBlock {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: parent_hash.map(str::to_owned),
        block_number,
        block_timestamp: timestamp(1_776_384_000 + (block_number - 21_000_000)),
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
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
async fn name_current_snapshot_read_covers_later_snapshot_until_new_input() -> Result<()> {
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
    upsert_chain_lineage_blocks(
        database.pool(),
        &[
            lineage_block("0xbinding", None, 21_000_003),
            lineage_block("0xnewer", Some("0xbinding"), 21_000_004),
        ],
    )
    .await?;

    let selected = ChainPositions::from_value(&expected.chain_positions)?;
    assert_eq!(
        load_name_current_for_snapshot(database.pool(), logical_name_id, &selected).await?,
        SnapshotProjectionRead::Found(expected.clone())
    );

    let stale_selected = ChainPositions::from_value(&json!({
        "ethereum": {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_004,
            "block_hash": "0xnewer",
            "timestamp": "2026-04-17T00:00:04Z"
        }
    }))?;
    assert_eq!(
        load_name_current_for_snapshot(database.pool(), logical_name_id, &stale_selected).await?,
        SnapshotProjectionRead::Found(expected.clone())
    );

    upsert_normalized_events(
        database.pool(),
        &[normalized_event(
            logical_name_id,
            resource_id,
            "0xnewer",
            21_000_004,
        )],
    )
    .await?;

    let error = load_name_current_for_snapshot(database.pool(), logical_name_id, &stale_selected)
        .await
        .expect_err("newer selected snapshot with unreplayed input must be stale");
    assert_eq!(error.kind(), SnapshotSelectionErrorKind::Stale);

    database.cleanup().await
}

#[tokio::test]
async fn name_current_snapshot_read_rejects_later_snapshot_when_projected_block_is_noncanonical()
-> Result<()> {
    let database = test_database().await?;
    let logical_name_id = "ens:alice.eth";
    let token_lineage_id = Uuid::from_u128(0x1120);
    let resource_id = Uuid::from_u128(0x2230);
    let surface_binding_id = Uuid::from_u128(0x3340);

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
    upsert_chain_lineage_blocks(
        database.pool(),
        &[
            lineage_block("0xbinding", None, 21_000_003),
            lineage_block("0xnewer", Some("0xbinding"), 21_000_004),
        ],
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE chain_lineage
        SET canonicality_state = 'orphaned'::canonicality_state
        WHERE chain_id = 'ethereum-mainnet'
          AND block_hash = '0xbinding'
        "#,
    )
    .execute(database.pool())
    .await?;

    let later_selected = ChainPositions::from_value(&json!({
        "ethereum": {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_004,
            "block_hash": "0xnewer",
            "timestamp": "2026-04-17T00:00:04Z"
        }
    }))?;

    let error = load_name_current_for_snapshot(database.pool(), logical_name_id, &later_selected)
        .await
        .expect_err("later selected snapshot from a noncanonical projection block must be stale");
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
async fn name_current_replacement_rolls_back_when_one_row_is_invalid() -> Result<()> {
    let database = test_database().await?;
    let first_logical_name_id = "ens:alice.eth";
    let second_logical_name_id = "ens:bob.eth";

    seed_binding_references(
        &database,
        first_logical_name_id,
        "alice.eth",
        Uuid::from_u128(0x5210),
        Uuid::from_u128(0x5110),
        Uuid::from_u128(0x5310),
    )
    .await?;
    seed_binding_references(
        &database,
        second_logical_name_id,
        "bob.eth",
        Uuid::from_u128(0x5220),
        Uuid::from_u128(0x5120),
        Uuid::from_u128(0x5320),
    )
    .await?;

    let first = name_current_row(
        first_logical_name_id,
        Uuid::from_u128(0x5310),
        Uuid::from_u128(0x5210),
        Uuid::from_u128(0x5110),
    );
    let mut second = name_current_row(
        second_logical_name_id,
        Uuid::from_u128(0x5320),
        Uuid::from_u128(0x5220),
        Uuid::from_u128(0x5120),
    );
    second.canonical_display_name = "bob.eth".to_owned();
    second.normalized_name = "bob.eth".to_owned();
    second.namehash = "namehash:bob.eth".to_owned();
    upsert_name_current_rows(database.pool(), &[first.clone(), second.clone()]).await?;

    let mut replacement = first.clone();
    replacement.declared_summary = json!({"status": "replacement"});
    let mut invalid = second.clone();
    invalid.manifest_version = 0;

    replace_name_current_rows(
        database.pool(),
        &[replacement, invalid],
        &[
            first_logical_name_id.to_owned(),
            second_logical_name_id.to_owned(),
        ],
    )
    .await
    .expect_err("invalid replacement row must roll back the replacement transaction");

    assert_eq!(
        load_name_current(database.pool(), first_logical_name_id).await?,
        Some(first)
    );
    assert_eq!(
        load_name_current(database.pool(), second_logical_name_id).await?,
        Some(second)
    );

    database.cleanup().await
}

#[tokio::test]
async fn name_current_replacement_rejects_duplicate_logical_name_ids() -> Result<()> {
    let database = test_database().await?;
    let logical_name_id = "ens:alice.eth";

    seed_binding_references(
        &database,
        logical_name_id,
        "alice.eth",
        Uuid::from_u128(0x9210),
        Uuid::from_u128(0x9110),
        Uuid::from_u128(0x9310),
    )
    .await?;

    let existing = name_current_row(
        logical_name_id,
        Uuid::from_u128(0x9310),
        Uuid::from_u128(0x9210),
        Uuid::from_u128(0x9110),
    );
    upsert_name_current_rows(database.pool(), std::slice::from_ref(&existing)).await?;

    let mut first_replacement = existing.clone();
    first_replacement.declared_summary = json!({"status": "first"});
    let mut second_replacement = existing.clone();
    second_replacement.declared_summary = json!({"status": "second"});
    second_replacement.manifest_version = 2;

    replace_name_current_rows(
        database.pool(),
        &[first_replacement, second_replacement],
        &[logical_name_id.to_owned()],
    )
    .await
    .expect_err("duplicate replacement logical_name_id must fail closed");

    assert_eq!(
        load_name_current(database.pool(), logical_name_id).await?,
        Some(existing)
    );

    database.cleanup().await
}

#[tokio::test]
async fn name_current_replacement_updates_last_recomputed_at_only() -> Result<()> {
    let database = test_database().await?;
    let logical_name_id = "ens:alice.eth";

    seed_binding_references(
        &database,
        logical_name_id,
        "alice.eth",
        Uuid::from_u128(0x9610),
        Uuid::from_u128(0x9510),
        Uuid::from_u128(0x9710),
    )
    .await?;

    let existing = name_current_row(
        logical_name_id,
        Uuid::from_u128(0x9710),
        Uuid::from_u128(0x9610),
        Uuid::from_u128(0x9510),
    );
    upsert_name_current_rows(database.pool(), std::slice::from_ref(&existing)).await?;

    let mut replacement = existing.clone();
    replacement.last_recomputed_at = timestamp(1_817_171_717);

    let upserted = replace_name_current_rows(
        database.pool(),
        std::slice::from_ref(&replacement),
        &[logical_name_id.to_owned()],
    )
    .await?;
    assert_eq!(upserted, (1, 0));
    assert_eq!(
        load_name_current(database.pool(), logical_name_id).await?,
        Some(replacement)
    );

    database.cleanup().await
}

#[tokio::test]
async fn name_current_replacement_stages_batches_before_atomic_publish() -> Result<()> {
    let database = test_database().await?;
    let first_logical_name_id = "ens:alice.eth";
    let second_logical_name_id = "ens:bob.eth";
    let stale_logical_name_id = "ens:carol.eth";

    seed_binding_references(
        &database,
        first_logical_name_id,
        "alice.eth",
        Uuid::from_u128(0x9110),
        Uuid::from_u128(0x9010),
        Uuid::from_u128(0x9210),
    )
    .await?;
    seed_binding_references(
        &database,
        second_logical_name_id,
        "bob.eth",
        Uuid::from_u128(0x9120),
        Uuid::from_u128(0x9020),
        Uuid::from_u128(0x9220),
    )
    .await?;
    seed_binding_references(
        &database,
        stale_logical_name_id,
        "carol.eth",
        Uuid::from_u128(0x9130),
        Uuid::from_u128(0x9030),
        Uuid::from_u128(0x9230),
    )
    .await?;

    let first = name_current_row(
        first_logical_name_id,
        Uuid::from_u128(0x9210),
        Uuid::from_u128(0x9110),
        Uuid::from_u128(0x9010),
    );
    let mut stale = name_current_row(
        stale_logical_name_id,
        Uuid::from_u128(0x9230),
        Uuid::from_u128(0x9130),
        Uuid::from_u128(0x9030),
    );
    stale.canonical_display_name = "carol.eth".to_owned();
    stale.normalized_name = "carol.eth".to_owned();
    stale.namehash = "namehash:carol.eth".to_owned();
    upsert_name_current_rows(database.pool(), &[first.clone(), stale.clone()]).await?;

    let mut replacement_first = first.clone();
    replacement_first.declared_summary = json!({"status": "replacement"});
    let mut replacement_second = name_current_row(
        second_logical_name_id,
        Uuid::from_u128(0x9220),
        Uuid::from_u128(0x9120),
        Uuid::from_u128(0x9020),
    );
    replacement_second.canonical_display_name = "bob.eth".to_owned();
    replacement_second.normalized_name = "bob.eth".to_owned();
    replacement_second.namehash = "namehash:bob.eth".to_owned();

    let mut replacement = NameCurrentReplacement::begin(database.pool()).await?;
    replacement
        .stage_rows(std::slice::from_ref(&replacement_first))
        .await?;
    assert_eq!(replacement.staged_row_count(), 1);
    assert_eq!(
        load_name_current(database.pool(), first_logical_name_id).await?,
        Some(first)
    );

    replacement
        .stage_rows(std::slice::from_ref(&replacement_second))
        .await?;
    assert_eq!(replacement.staged_row_count(), 2);
    let (upserted_row_count, deleted_row_count) = replacement.publish().await?;

    assert_eq!(upserted_row_count, 2);
    assert_eq!(deleted_row_count, 1);
    assert_eq!(
        load_name_current(database.pool(), first_logical_name_id).await?,
        Some(replacement_first)
    );
    assert_eq!(
        load_name_current(database.pool(), second_logical_name_id).await?,
        Some(replacement_second)
    );
    assert_eq!(
        load_name_current(database.pool(), stale_logical_name_id).await?,
        None
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
