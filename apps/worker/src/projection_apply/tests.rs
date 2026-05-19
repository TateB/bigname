use anyhow::{Context, Result};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{Row, types::time::OffsetDateTime};
use uuid::Uuid;

use super::derive::derive_normalized_event_invalidations;

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
            event_identity: "projection-apply:children",
            namespace: "ens",
            logical_name_id: Some("ens:parent.eth"),
            resource_id: None,
            event_kind: "SubregistryChanged",
            source_family: "ens_v1_registry_l1",
            derivation_kind: "ens_v1_subregistry_changed",
            chain_id: Some("ethereum-mainnet"),
            block_number: Some(14),
            block_hash: Some("0xblock14"),
            before_state: json!({}),
            after_state: json!({}),
            observed_at,
        },
    )
    .await?;

    let summary = derive_normalized_event_invalidations(database.pool(), 100).await?;
    assert_eq!(summary.scanned_event_count, 5);
    assert!(summary.enqueued_invalidation_count >= 8);

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
        "0x0000000000000000000000000000000000000ddd"
    ));
    assert!(has_key(
        &invalidations,
        "primary_names_current",
        "0x0000000000000000000000000000000000000eee:ens:60"
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
