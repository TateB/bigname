use anyhow::{Context, Result};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{Row, types::time::OffsetDateTime};
use uuid::Uuid;

use super::{derive::derive_normalized_event_invalidations, has_primary_hydration_blocking_work};

async fn test_database() -> Result<TestDatabase> {
    TestDatabase::create_migrated(
        TestDatabaseConfig::new("bigname_worker_projection_apply_test")
            .admin_database("postgres")
            .pool_max_connections(5)
            .parse_context("failed to parse database URL for worker projection apply tests")
            .admin_connect_context("failed to connect admin pool for worker projection apply tests")
            .pool_connect_context("failed to connect worker projection apply test pool"),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for worker projection apply tests",
    )
    .await
}

#[tokio::test]
async fn derives_key_scoped_invalidations_from_normalized_event_changes() -> Result<()> {
    let database = test_database().await?;
    let resource_id = Uuid::new_v4();
    let observed_at = timestamp(1_800_000_000);

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:name-resolver",
            namespace: "ens",
            logical_name_id: Some("ens:alice.eth"),
            resource_id: Some(resource_id),
            event_kind: "ResolverChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(10),
            block_hash: Some("0xblock10"),
            before_state: json!({
                "resolver": "0x0000000000000000000000000000000000000bbb"
            }),
            after_state: json!({
                "resolver": "0x0000000000000000000000000000000000000aaa"
            }),
            observed_at,
        },
    )
    .await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:permission",
            namespace: "ens",
            logical_name_id: Some("ens:alice.eth"),
            resource_id: Some(resource_id),
            event_kind: "PermissionChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(11),
            block_hash: Some("0xblock11"),
            before_state: json!({}),
            after_state: json!({
                "scope": {
                    "kind": "resolver",
                    "chain_id": "ethereum-mainnet",
                    "resolver_address": "0x0000000000000000000000000000000000000ccc"
                }
            }),
            observed_at,
        },
    )
    .await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:resource-permission",
            namespace: "ens",
            logical_name_id: Some("ens:alice.eth"),
            resource_id: Some(resource_id),
            event_kind: "PermissionChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(11),
            block_hash: Some("0xblock11"),
            before_state: json!({
                "scope": {
                    "kind": "resource"
                },
                "subject": "0x0000000000000000000000000000000000000aa1"
            }),
            after_state: json!({
                "scope": {
                    "kind": "resource"
                },
                "subject": "0x0000000000000000000000000000000000000aa2"
            }),
            observed_at,
        },
    )
    .await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:address",
            namespace: "ens",
            logical_name_id: Some("ens:alice.eth"),
            resource_id: Some(resource_id),
            event_kind: "RegistrationGranted",
            source_family: "ens_v1_registrar_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(12),
            block_hash: Some("0xblock12"),
            before_state: json!({}),
            after_state: json!({
                "registrant": "0x0000000000000000000000000000000000000ddd"
            }),
            observed_at,
        },
    )
    .await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:primary",
            namespace: "ens",
            logical_name_id: None,
            resource_id: None,
            event_kind: "ReverseChanged",
            source_family: "ens_v1_reverse_l1",
            derivation_kind: "ens_v1_reverse_claim",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(13),
            block_hash: Some("0xblock13"),
            before_state: json!({}),
            after_state: json!({
                "address": "0x0000000000000000000000000000000000000eee",
                "namespace": "ens",
                "coin_type": "60"
            }),
            observed_at,
        },
    )
    .await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:primary-resolver",
            namespace: "ens",
            logical_name_id: None,
            resource_id: None,
            event_kind: "ResolverChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(14),
            block_hash: Some("0xblock14"),
            before_state: json!({}),
            after_state: json!({
                "resolver": "0x0000000000000000000000000000000000000abc",
                "primary_claim_source": {
                    "address": "0x0000000000000000000000000000000000000fff",
                    "namespace": "ens",
                    "coin_type": "60",
                    "reverse_node": "0xreverse"
                }
            }),
            observed_at,
        },
    )
    .await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:children",
            namespace: "ens",
            logical_name_id: Some("ens:parent.eth"),
            resource_id: None,
            event_kind: "SubregistryChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_subregistry_changed",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(15),
            block_hash: Some("0xblock15"),
            before_state: json!({}),
            after_state: json!({}),
            observed_at,
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 7);
    assert!(summary.enqueued_invalidation_count >= 10);

    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(&invalidations, "name_current", "ens:alice.eth"));
    assert!(has_key(
        &invalidations,
        "children_current",
        "ens:parent.eth"
    ));
    assert!(has_key(
        &invalidations,
        "permissions_current",
        &resource_id.to_string()
    ));
    assert!(has_key(
        &invalidations,
        "record_inventory_current",
        &resource_id.to_string()
    ));
    assert!(has_key(
        &invalidations,
        "resolver_current",
        "ethereum-mainnet:0x0000000000000000000000000000000000000aaa"
    ));
    assert!(has_key(
        &invalidations,
        "resolver_current",
        "ethereum-mainnet:0x0000000000000000000000000000000000000bbb"
    ));
    assert!(has_key(
        &invalidations,
        "resolver_current",
        "ethereum-mainnet:0x0000000000000000000000000000000000000ccc"
    ));
    assert!(has_key(
        &invalidations,
        "address_names_current",
        "0x0000000000000000000000000000000000000ddd:ens:alice.eth"
    ));
    assert!(has_key(
        &invalidations,
        "address_names_current",
        "0x0000000000000000000000000000000000000aa1:ens:alice.eth"
    ));
    assert!(has_key(
        &invalidations,
        "address_names_current",
        "0x0000000000000000000000000000000000000aa2:ens:alice.eth"
    ));
    assert_eq!(
        load_invalidation_payload(
            &database,
            "address_names_current",
            "0x0000000000000000000000000000000000000ddd:ens:alice.eth"
        )
        .await?,
        json!({
            "address": "0x0000000000000000000000000000000000000ddd",
            "logical_name_id": "ens:alice.eth"
        })
    );
    assert!(has_key(
        &invalidations,
        "primary_names_current",
        "0x0000000000000000000000000000000000000eee:ens:60"
    ));
    assert!(has_key(
        &invalidations,
        "primary_names_current",
        "0x0000000000000000000000000000000000000fff:ens:60"
    ));

    sqlx::query(
        r#"
        UPDATE normalized_events
        SET canonicality_state = 'orphaned'::canonicality_state,
            observed_at = $2
        WHERE event_identity = $1
        "#,
    )
    .bind("projection-apply:name-resolver")
    .bind(timestamp(1_800_000_100))
    .execute(database.pool())
    .await
    .context("failed to orphan normalized event")?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let generation = invalidation_generation(&database, "name_current", "ens:alice.eth").await?;
    assert_eq!(generation, 1);

    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear generated projection invalidations")?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:resource-permission-revoked",
            namespace: "ens",
            logical_name_id: Some("ens:alice.eth"),
            resource_id: Some(resource_id),
            event_kind: "PermissionChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(15),
            block_hash: Some("0xblock15"),
            before_state: json!({
                "scope": {
                    "kind": "resource"
                },
                "subject": "0x0000000000000000000000000000000000000aa2",
                "effective_powers": ["resource_control"]
            }),
            after_state: json!({
                "scope": {
                    "kind": "resource"
                },
                "subject": "0x0000000000000000000000000000000000000aa2",
                "effective_powers": []
            }),
            observed_at,
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(
        &invalidations,
        "address_names_current",
        "0x0000000000000000000000000000000000000aa2:ens:alice.eth"
    ));
    assert!(has_key(
        &invalidations,
        "address_names_current",
        "0x0000000000000000000000000000000000000ddd:ens:alice.eth"
    ));

    database.cleanup().await
}

#[tokio::test]
async fn address_names_permission_changes_invalidate_existing_authority_owner() -> Result<()> {
    let database = test_database().await?;
    let resource_id = Uuid::new_v4();
    let observed_at = timestamp(1_800_000_000);

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:authority-owner-before-permission",
            namespace: "ens",
            logical_name_id: Some("ens:controller.eth"),
            resource_id: Some(resource_id),
            event_kind: "AuthorityTransferred",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(20),
            block_hash: Some("0xblock20"),
            before_state: json!({}),
            after_state: json!({
                "owner": "0x0000000000000000000000000000000000000a00"
            }),
            observed_at,
        },
    )
    .await?;
    derive_normalized_event_invalidations(database.pool(), 100).await?;
    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear authority-owner invalidations")?;

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:permission-overrides-authority-owner",
            namespace: "ens",
            logical_name_id: Some("ens:controller.eth"),
            resource_id: Some(resource_id),
            event_kind: "PermissionChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(21),
            block_hash: Some("0xblock21"),
            before_state: json!({
                "scope": {
                    "kind": "resource"
                },
                "subject": "0x0000000000000000000000000000000000000b00",
                "effective_powers": []
            }),
            after_state: json!({
                "scope": {
                    "kind": "resource"
                },
                "subject": "0x0000000000000000000000000000000000000b00",
                "effective_powers": ["resource_control"]
            }),
            observed_at,
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(
        &invalidations,
        "address_names_current",
        "0x0000000000000000000000000000000000000a00:ens:controller.eth"
    ));
    assert!(has_key(
        &invalidations,
        "address_names_current",
        "0x0000000000000000000000000000000000000b00:ens:controller.eth"
    ));

    database.cleanup().await
}

#[tokio::test]
async fn address_names_permission_scope_changes_invalidate_controller_and_fallback_addresses()
-> Result<()> {
    let database = test_database().await?;
    let resource_id = Uuid::new_v4();
    let observed_at = timestamp(1_800_000_000);

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:fuse-address-registrant",
            namespace: "ens",
            logical_name_id: Some("ens:fused-controller.eth"),
            resource_id: Some(resource_id),
            event_kind: "RegistrationGranted",
            source_family: "ens_v1_registrar_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(30),
            block_hash: Some("0xfuse30"),
            before_state: json!({}),
            after_state: json!({
                "registrant": "0x0000000000000000000000000000000000000bbb"
            }),
            observed_at,
        },
    )
    .await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:fuse-address-controller",
            namespace: "ens",
            logical_name_id: Some("ens:fused-controller.eth"),
            resource_id: Some(resource_id),
            event_kind: "PermissionChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(31),
            block_hash: Some("0xfuse31"),
            before_state: json!({}),
            after_state: json!({
                "scope": {
                    "kind": "resource"
                },
                "subject": "0x0000000000000000000000000000000000000ccc",
                "effective_powers": ["resource_control"]
            }),
            observed_at,
        },
    )
    .await?;
    derive_normalized_event_invalidations(database.pool(), 100).await?;
    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear setup invalidations")?;

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:fuse-address-scope",
            namespace: "ens",
            logical_name_id: Some("ens:fused-controller.eth"),
            resource_id: Some(resource_id),
            event_kind: "PermissionScopeChanged",
            source_family: "ens_v1_wrapper_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(32),
            block_hash: Some("0xfuse32"),
            before_state: json!({}),
            after_state: json!({
                "scope": {
                    "kind": "resource"
                },
                "fuses": 8
            }),
            observed_at,
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(
        &invalidations,
        "address_names_current",
        "0x0000000000000000000000000000000000000ccc:ens:fused-controller.eth"
    ));
    assert!(has_key(
        &invalidations,
        "address_names_current",
        "0x0000000000000000000000000000000000000bbb:ens:fused-controller.eth"
    ));

    database.cleanup().await
}

#[tokio::test]
async fn resolver_current_resource_scope_fuse_changes_invalidate_resolver_permission_keys()
-> Result<()> {
    let database = test_database().await?;
    let resource_id = Uuid::new_v4();
    let observed_at = timestamp(1_800_000_000);

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:fuse-resolver-permission",
            namespace: "ens",
            logical_name_id: Some("ens:fused-resolver.eth"),
            resource_id: Some(resource_id),
            event_kind: "PermissionChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(40),
            block_hash: Some("0xfuse40"),
            before_state: json!({}),
            after_state: json!({
                "scope": {
                    "kind": "resolver",
                    "chain_id": "ethereum-mainnet",
                    "resolver_address": "0x0000000000000000000000000000000000000ccc"
                },
                "subject": "0x0000000000000000000000000000000000000aaa",
                "effective_powers": ["resolver_control"]
            }),
            observed_at,
        },
    )
    .await?;
    derive_normalized_event_invalidations(database.pool(), 100).await?;
    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear setup invalidations")?;

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:fuse-resolver-scope",
            namespace: "ens",
            logical_name_id: Some("ens:fused-resolver.eth"),
            resource_id: Some(resource_id),
            event_kind: "PermissionScopeChanged",
            source_family: "ens_v1_wrapper_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(41),
            block_hash: Some("0xfuse41"),
            before_state: json!({}),
            after_state: json!({
                "scope": {
                    "kind": "resource"
                },
                "fuses": 8
            }),
            observed_at,
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(
        &invalidations,
        "resolver_current",
        "ethereum-mainnet:0x0000000000000000000000000000000000000ccc"
    ));

    database.cleanup().await
}

#[tokio::test]
async fn manifest_events_enqueue_manifest_sensitive_projection_keys() -> Result<()> {
    let database = test_database().await?;
    let resource_id = Uuid::new_v4();
    let observed_at = timestamp(1_800_000_000);

    insert_name_surface(&database, "ens:manifest.eth", "manifest.eth").await?;
    insert_resource(&database, resource_id).await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:manifest-record",
            namespace: "ens",
            logical_name_id: Some("ens:manifest.eth"),
            resource_id: Some(resource_id),
            event_kind: "RecordChanged",
            source_family: "ens_v1_resolver_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(20),
            block_hash: Some("0xmanifest20"),
            before_state: json!({}),
            after_state: json!({
                "record_key": "text:email",
                "record_family": "text",
                "selector_key": "email"
            }),
            observed_at,
        },
    )
    .await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:manifest-resolver",
            namespace: "ens",
            logical_name_id: Some("ens:manifest.eth"),
            resource_id: Some(resource_id),
            event_kind: "ResolverChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(21),
            block_hash: Some("0xmanifest21"),
            before_state: json!({}),
            after_state: json!({
                "resolver": "0x0000000000000000000000000000000000000abc"
            }),
            observed_at,
        },
    )
    .await?;
    derive_normalized_event_invalidations(database.pool(), 100).await?;
    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear setup invalidations")?;

    for (index, event_kind) in [
        "SourceManifestUpdated",
        "CapabilityChanged",
        "ProxyImplementationChanged",
    ]
    .into_iter()
    .enumerate()
    {
        sqlx::query("DELETE FROM projection_invalidations")
            .execute(database.pool())
            .await
            .context("failed to clear manifest invalidations")?;
        let event_identity = format!("projection-apply:manifest-{event_kind}");
        insert_event(
            &database,
            EventSeed {
                event_identity: &event_identity,
                namespace: "ens",
                logical_name_id: None,
                resource_id: None,
                event_kind,
                source_family: "ens_v1_registry_l1",
                derivation_kind: "manifest_sync",
                chain_id: Some("ethereum-mainnet"),
                block_number: None,
                block_hash: None,
                before_state: json!({}),
                after_state: json!({
                    "manifest_version": 2 + index as i64,
                    "normalizer_version": "test"
                }),
                observed_at: timestamp(1_800_000_100 + index as i64),
            },
        )
        .await?;

        let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
        assert_eq!(summary.scanned_event_count, 1);
        let invalidations = load_invalidations(&database).await?;
        assert!(has_key(&invalidations, "name_current", "ens:manifest.eth"));
        assert!(has_key(
            &invalidations,
            "record_inventory_current",
            &resource_id.to_string()
        ));
        assert!(has_key(
            &invalidations,
            "resolver_current",
            "ethereum-mainnet:0x0000000000000000000000000000000000000abc"
        ));
    }

    database.cleanup().await
}

#[tokio::test]
async fn generation_bump_releases_in_flight_claim_for_serialized_reapply() -> Result<()> {
    let database = test_database().await?;
    let resource_id = Uuid::new_v4();
    let claim_token = Uuid::new_v4();

    insert_resource(&database, resource_id).await?;
    sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload,
            claim_token,
            claimed_at
        )
        VALUES (
            'permissions_current',
            $1,
            jsonb_build_object('resource_id', $1),
            $2,
            now()
        )
        "#,
    )
    .bind(resource_id.to_string())
    .bind(claim_token)
    .execute(database.pool())
    .await
    .context("failed to seed claimed projection invalidation")?;

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:claimed-permission-change",
            namespace: "ens",
            logical_name_id: Some("ens:claimed.eth"),
            resource_id: Some(resource_id),
            event_kind: "PermissionChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(22),
            block_hash: Some("0xclaimed22"),
            before_state: json!({}),
            after_state: json!({
                "scope": {
                    "kind": "resource"
                },
                "subject": "0x0000000000000000000000000000000000000abc",
                "effective_powers": ["resource_control"]
            }),
            observed_at: timestamp(1_800_000_200),
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);

    let (generation, retained_claim_token): (i64, Option<Uuid>) = sqlx::query_as(
        r#"
        SELECT generation, claim_token
        FROM projection_invalidations
        WHERE projection = 'permissions_current'
          AND projection_key = $1
        "#,
    )
    .bind(resource_id.to_string())
    .fetch_one(database.pool())
    .await
    .context("failed to load bumped projection invalidation")?;
    assert_eq!(generation, 1);
    assert_eq!(retained_claim_token, None);

    database.cleanup().await
}

#[tokio::test]
async fn derives_record_inventory_cross_resource_invalidations_for_logical_name_dependencies()
-> Result<()> {
    let database = test_database().await?;
    let predecessor_resource_id = Uuid::new_v4();
    let current_resource_id = Uuid::new_v4();
    let observed_at = timestamp(1_800_000_000);

    insert_resource(&database, predecessor_resource_id).await?;
    insert_resource(&database, current_resource_id).await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:current-resolver",
            namespace: "ens",
            logical_name_id: Some("ens:carry.eth"),
            resource_id: Some(current_resource_id),
            event_kind: "ResolverChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(21),
            block_hash: Some("0xcarry21"),
            before_state: json!({}),
            after_state: json!({
                "resolver": "0x0000000000000000000000000000000000000aaa"
            }),
            observed_at,
        },
    )
    .await?;
    derive_normalized_event_invalidations(database.pool(), 100).await?;
    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear initial projection invalidations")?;

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:predecessor-resolver-boundary",
            namespace: "ens",
            logical_name_id: Some("ens:carry.eth"),
            resource_id: Some(predecessor_resource_id),
            event_kind: "ResolverChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(19),
            block_hash: Some("0xcarry19"),
            before_state: json!({}),
            after_state: json!({
                "resolver": "0x0000000000000000000000000000000000000bbb"
            }),
            observed_at,
        },
    )
    .await?;
    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:predecessor-record",
            namespace: "ens",
            logical_name_id: Some("ens:carry.eth"),
            resource_id: Some(predecessor_resource_id),
            event_kind: "RecordChanged",
            source_family: "ens_v1_resolver_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(20),
            block_hash: Some("0xcarry20"),
            before_state: json!({}),
            after_state: json!({
                "record_key": "text:email",
                "record_family": "text",
                "selector_key": "email"
            }),
            observed_at,
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 2);
    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(
        &invalidations,
        "record_inventory_current",
        &predecessor_resource_id.to_string()
    ));
    assert!(has_key(
        &invalidations,
        "record_inventory_current",
        &current_resource_id.to_string()
    ));

    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear cross-resource projection invalidations")?;
    sqlx::query(
        r#"
        UPDATE normalized_events
        SET resource_id = NULL
        WHERE event_identity = $1
        "#,
    )
    .bind("projection-apply:predecessor-resolver-boundary")
    .execute(database.pool())
    .await
    .context("failed to unbind predecessor resolver event")?;
    sqlx::query(
        r#"
        INSERT INTO projection_normalized_event_changes (
            normalized_event_id,
            changed_at,
            change_kind,
            canonicality_state
        )
        SELECT
            normalized_event_id,
            $2,
            'canonicality_update',
            canonicality_state
        FROM normalized_events
        WHERE event_identity = $1
        "#,
    )
    .bind("projection-apply:predecessor-resolver-boundary")
    .bind(timestamp(1_800_000_100))
    .execute(database.pool())
    .await
    .context("failed to log predecessor resolver repair change")?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(
        &invalidations,
        "record_inventory_current",
        &current_resource_id.to_string()
    ));

    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear repaired resolver invalidations")?;
    sqlx::query(
        r#"
        UPDATE normalized_events
        SET canonicality_state = 'orphaned'::canonicality_state,
            observed_at = $2
        WHERE event_identity = $1
        "#,
    )
    .bind("projection-apply:predecessor-record")
    .bind(timestamp(1_800_000_200))
    .execute(database.pool())
    .await
    .context("failed to orphan predecessor record event")?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 1);
    let invalidations = load_invalidations(&database).await?;
    assert!(has_key(
        &invalidations,
        "record_inventory_current",
        &current_resource_id.to_string()
    ));

    database.cleanup().await
}

#[tokio::test]
async fn primary_hydration_blocking_work_ignores_unrelated_retry_delayed_failures() -> Result<()> {
    let database = test_database().await?;

    insert_failed_invalidation(
        &database,
        "name_current",
        "ens:retry-delayed.eth",
        "1 second",
    )
    .await?;
    assert!(
        !has_primary_hydration_blocking_work(database.pool()).await?,
        "recently failed unrelated invalidations must not starve primary hydration"
    );

    insert_failed_invalidation(&database, "name_current", "ens:claimable.eth", "2 minutes").await?;
    assert!(
        has_primary_hydration_blocking_work(database.pool()).await?,
        "failed invalidations become pending once the retry delay expires"
    );

    database.cleanup().await
}

#[tokio::test]
async fn primary_hydration_blocking_work_detects_primary_retry_delayed_failures() -> Result<()> {
    let database = test_database().await?;

    insert_failed_invalidation(
        &database,
        "primary_names_current",
        "0x0000000000000000000000000000000000000aaa:ens:60",
        "1 second",
    )
    .await?;
    assert!(
        has_primary_hydration_blocking_work(database.pool()).await?,
        "recently failed primary_names_current invalidations can affect hydration inputs"
    );

    database.cleanup().await
}

#[tokio::test]
async fn primary_hydration_blocking_work_ignores_dead_lettered_invalidations() -> Result<()> {
    let database = test_database().await?;

    sqlx::query(
        r#"
        INSERT INTO projection_invalidation_dead_letters (
            projection,
            projection_key,
            key_payload,
            attempt_count,
            generation,
            last_changed_at,
            invalidated_at,
            last_failure_reason,
            last_failure_at,
            dead_lettered_at
        )
        VALUES (
            'primary_names_current',
            '0x0000000000000000000000000000000000000aaa:ens:60',
            jsonb_build_object(
                'address', '0x0000000000000000000000000000000000000aaa',
                'namespace', 'ens',
                'coin_type', '60'
            ),
            5,
            0,
            now(),
            now(),
            'poisoned test invalidation',
            now(),
            now() - '10 minutes'::INTERVAL
        )
        "#,
    )
    .execute(database.pool())
    .await
    .context("failed to seed dead-lettered primary_names_current invalidation")?;

    assert!(
        !has_primary_hydration_blocking_work(database.pool()).await?,
        "dead-lettered invalidations are operator-visible terminal failures, not claimable blocking work"
    );

    database.cleanup().await
}

#[tokio::test]
async fn primary_hydration_blocking_work_detects_claimable_invalidations_and_cursor_lag()
-> Result<()> {
    let database = test_database().await?;
    assert!(!has_primary_hydration_blocking_work(database.pool()).await?);

    insert_invalidation(&database, "name_current", "ens:pending.eth").await?;
    assert!(has_primary_hydration_blocking_work(database.pool()).await?);

    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear pending invalidation")?;
    assert!(!has_primary_hydration_blocking_work(database.pool()).await?);

    insert_event(
        &database,
        EventSeed {
            event_identity: "projection-apply:pending-work-cursor-lag",
            namespace: "ens",
            logical_name_id: Some("ens:cursor-lag.eth"),
            resource_id: Some(Uuid::new_v4()),
            event_kind: "ResolverChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_unwrapped_authority",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(30),
            block_hash: Some("0xcursor30"),
            before_state: json!({}),
            after_state: json!({
                "resolver": "0x0000000000000000000000000000000000000aaa"
            }),
            observed_at: timestamp(1_800_000_300),
        },
    )
    .await?;
    assert!(has_primary_hydration_blocking_work(database.pool()).await?);

    database.cleanup().await
}

#[tokio::test]
async fn primary_hydration_blocking_work_detects_active_claimed_invalidations() -> Result<()> {
    let database = test_database().await?;

    insert_claimed_invalidation(&database, "name_current", "ens:in-flight.eth", "1 minute").await?;
    assert!(
        has_primary_hydration_blocking_work(database.pool()).await?,
        "freshly claimed invalidations are active apply work and must block hydration"
    );

    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear active claimed invalidation")?;
    insert_claimed_invalidation(
        &database,
        "name_current",
        "ens:stale-claim.eth",
        "10 minutes",
    )
    .await?;
    assert!(
        has_primary_hydration_blocking_work(database.pool()).await?,
        "expired claims are claimable apply work and must block hydration"
    );

    database.cleanup().await
}

fn has_key(invalidations: &[(String, String)], projection: &str, projection_key: &str) -> bool {
    invalidations
        .iter()
        .any(|(candidate_projection, candidate_key)| {
            candidate_projection == projection && candidate_key == projection_key
        })
}

async fn load_invalidations(database: &TestDatabase) -> Result<Vec<(String, String)>> {
    let rows = sqlx::query(
        r#"
        SELECT projection, projection_key
        FROM projection_invalidations
        ORDER BY projection, projection_key
        "#,
    )
    .fetch_all(database.pool())
    .await
    .context("failed to load projection invalidations")?;

    rows.into_iter()
        .map(|row| Ok((row.try_get("projection")?, row.try_get("projection_key")?)))
        .collect()
}

async fn invalidation_generation(
    database: &TestDatabase,
    projection: &str,
    projection_key: &str,
) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT generation
        FROM projection_invalidations
        WHERE projection = $1
          AND projection_key = $2
        "#,
    )
    .bind(projection)
    .bind(projection_key)
    .fetch_one(database.pool())
    .await
    .context("failed to load projection invalidation generation")
}

async fn load_invalidation_payload(
    database: &TestDatabase,
    projection: &str,
    projection_key: &str,
) -> Result<Value> {
    sqlx::query_scalar::<_, Value>(
        r#"
        SELECT key_payload
        FROM projection_invalidations
        WHERE projection = $1
          AND projection_key = $2
        "#,
    )
    .bind(projection)
    .bind(projection_key)
    .fetch_one(database.pool())
    .await
    .context("failed to load projection invalidation payload")
}

async fn insert_resource(database: &TestDatabase, resource_id: Uuid) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO resources (
            resource_id,
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        )
        VALUES (
            $1,
            'ethereum-mainnet',
            $2,
            1,
            'finalized'
        )
        "#,
    )
    .bind(resource_id)
    .bind(format!("0x{}", resource_id.simple()))
    .execute(database.pool())
    .await
    .context("failed to insert projection apply test resource")?;
    Ok(())
}

async fn insert_name_surface(
    database: &TestDatabase,
    logical_name_id: &str,
    normalized_name: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO name_surfaces (
            logical_name_id,
            namespace,
            input_name,
            canonical_display_name,
            normalized_name,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version,
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        )
        VALUES (
            $1,
            'ens',
            $2,
            $2,
            $2,
            '\x00'::bytea,
            $3,
            ARRAY[]::TEXT[],
            'test',
            'ethereum-mainnet',
            '0xsurface',
            1,
            'finalized'::canonicality_state
        )
        "#,
    )
    .bind(logical_name_id)
    .bind(normalized_name)
    .bind(format!("0x{name}", name = normalized_name.replace('.', "")))
    .execute(database.pool())
    .await
    .context("failed to insert projection apply test name surface")?;
    Ok(())
}

async fn insert_invalidation(
    database: &TestDatabase,
    projection: &str,
    projection_key: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload
        )
        VALUES ($1, $2, '{}'::jsonb)
        "#,
    )
    .bind(projection)
    .bind(projection_key)
    .execute(database.pool())
    .await
    .context("failed to insert projection invalidation")?;

    Ok(())
}

async fn insert_failed_invalidation(
    database: &TestDatabase,
    projection: &str,
    projection_key: &str,
    failure_age: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload,
            last_failure_at
        )
        VALUES ($1, $2, '{}'::jsonb, now() - $3::INTERVAL)
        "#,
    )
    .bind(projection)
    .bind(projection_key)
    .bind(failure_age)
    .execute(database.pool())
    .await
    .context("failed to insert failed projection invalidation")?;

    Ok(())
}

async fn insert_claimed_invalidation(
    database: &TestDatabase,
    projection: &str,
    projection_key: &str,
    claim_age: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload,
            claim_token,
            claimed_at
        )
        VALUES ($1, $2, '{}'::jsonb, $3, now() - $4::INTERVAL)
        "#,
    )
    .bind(projection)
    .bind(projection_key)
    .bind(Uuid::new_v4())
    .bind(claim_age)
    .execute(database.pool())
    .await
    .context("failed to insert claimed projection invalidation")?;

    Ok(())
}

struct EventSeed<'a> {
    event_identity: &'a str,
    namespace: &'a str,
    logical_name_id: Option<&'a str>,
    resource_id: Option<Uuid>,
    event_kind: &'a str,
    source_family: &'a str,
    derivation_kind: &'a str,
    chain_id: Option<&'a str>,
    block_number: Option<i64>,
    block_hash: Option<&'a str>,
    before_state: Value,
    after_state: Value,
    observed_at: OffsetDateTime,
}

async fn insert_event(database: &TestDatabase, event: EventSeed<'_>) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            logical_name_id,
            resource_id,
            event_kind,
            source_family,
            manifest_version,
            chain_id,
            block_number,
            block_hash,
            raw_fact_ref,
            derivation_kind,
            canonicality_state,
            before_state,
            after_state,
            observed_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, 1, $7, $8, $9,
            '{}'::jsonb, $10, 'canonical'::canonicality_state, $11, $12, $13
        )
        "#,
    )
    .bind(event.event_identity)
    .bind(event.namespace)
    .bind(event.logical_name_id)
    .bind(event.resource_id)
    .bind(event.event_kind)
    .bind(event.source_family)
    .bind(event.chain_id)
    .bind(event.block_number)
    .bind(event.block_hash)
    .bind(event.derivation_kind)
    .bind(event.before_state)
    .bind(event.after_state)
    .bind(event.observed_at)
    .execute(database.pool())
    .await
    .with_context(|| format!("failed to insert normalized event {}", event.event_identity))?;

    Ok(())
}

fn timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).expect("fixed timestamp must be valid")
}
