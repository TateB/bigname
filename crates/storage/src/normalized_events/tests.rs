use std::{
    collections::BTreeMap,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use serde_json::json;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

use super::*;
use crate::{RawBlock, default_database_url, upsert_raw_blocks};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

struct TestDatabase {
    admin_pool: PgPool,
    pool: PgPool,
    database_name: String,
}

impl TestDatabase {
    async fn new() -> Result<Self> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for normalized-event tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!("bn_st_ne_{}_{}_{}", std::process::id(), sequence, unique);

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for normalized-event tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect normalized-event test pool")?;

        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for normalized-event tests")?;

        Ok(Self {
            admin_pool,
            pool,
            database_name,
        })
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn cleanup(self) -> Result<()> {
        self.pool.close().await;
        sqlx::query(&format!(
            r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
            self.database_name
        ))
        .execute(&self.admin_pool)
        .await
        .with_context(|| format!("failed to drop test database {}", self.database_name))?;
        self.admin_pool.close().await;
        Ok(())
    }
}

fn normalized_event(
    event_identity: &str,
    event_kind: &str,
    state: CanonicalityState,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: event_kind.to_owned(),
        source_family: "ens_v2_registry_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("ethereum-mainnet".to_owned()),
        block_number: None,
        block_hash: None,
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({}),
        derivation_kind: "manifest_sync".to_owned(),
        canonicality_state: state,
        before_state: json!({}),
        after_state: json!({"key": event_identity}),
    }
}

#[tokio::test]
async fn upserts_and_loads_normalized_events() -> Result<()> {
    let database = TestDatabase::new().await?;

    let inserted = upsert_normalized_events(
        database.pool(),
        &[
            normalized_event(
                "manifest:1:source_manifest",
                "SourceManifestUpdated",
                CanonicalityState::Finalized,
            ),
            normalized_event(
                "manifest:1:capability:verified_resolution",
                "CapabilityChanged",
                CanonicalityState::Finalized,
            ),
        ],
    )
    .await?;
    assert_eq!(inserted.len(), 2);

    let loaded = load_normalized_events_by_namespace(database.pool(), "ens").await?;
    assert_eq!(loaded, inserted);

    let counts = load_normalized_event_counts_by_kind(database.pool(), "ens").await?;
    assert_eq!(
        counts,
        BTreeMap::from([
            ("CapabilityChanged".to_owned(), 1_usize),
            ("SourceManifestUpdated".to_owned(), 1_usize),
        ])
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_upsert_rejects_identity_mismatch() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_normalized_events(
        database.pool(),
        &[normalized_event(
            "manifest:1:source_manifest",
            "SourceManifestUpdated",
            CanonicalityState::Finalized,
        )],
    )
    .await?;

    let mut conflicting = normalized_event(
        "manifest:1:source_manifest",
        "SourceManifestUpdated",
        CanonicalityState::Finalized,
    );
    conflicting.after_state = json!({"key": "different"});
    let error = upsert_normalized_events(database.pool(), &[conflicting])
        .await
        .expect_err("normalized-event identity mismatch must fail");

    assert!(
        error
            .to_string()
            .contains("normalized event identity mismatch for event manifest:1:source_manifest"),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_upsert_rejects_resource_id_change() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut event = normalized_event(
        "raw-log:resolver-changed",
        "ResolverChanged",
        CanonicalityState::Finalized,
    );
    event.logical_name_id = Some("ens:alice.eth".to_owned());
    event.resource_id = Some(Uuid::from_u128(0x100));
    upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;

    event.resource_id = Some(Uuid::from_u128(0x200));
    let error = upsert_normalized_events(database.pool(), std::slice::from_ref(&event))
        .await
        .expect_err("concrete resource-id changes must fail");

    assert!(
        error
            .to_string()
            .contains("normalized event identity mismatch for event raw-log:resolver-changed"),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_upsert_rejects_token_transfer_before_state_change() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut event = normalized_event(
        "raw-log:token-control-transferred",
        "TokenControlTransferred",
        CanonicalityState::Finalized,
    );
    event.logical_name_id = Some("ens:alice.eth".to_owned());
    event.source_family = "ens_v1_registrar_l1".to_owned();
    event.derivation_kind = "ens_v1_unwrapped_authority".to_owned();
    event.before_state = json!({
        "from": "0x0000000000000000000000000000000000000001",
    });
    event.after_state = json!({
        "labelhash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "to": "0x0000000000000000000000000000000000000002",
    });

    upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;

    event.before_state = json!({
        "from": "0x0000000000000000000000000000000000000003",
    });
    let error = upsert_normalized_events(database.pool(), std::slice::from_ref(&event))
        .await
        .expect_err("concrete token-transfer before-state changes must fail");

    assert!(
        error.to_string().contains(
            "normalized event identity mismatch for event raw-log:token-control-transferred"
        ),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_upsert_promotes_canonicality() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_normalized_events(
        database.pool(),
        &[normalized_event(
            "manifest:1:source_manifest",
            "SourceManifestUpdated",
            CanonicalityState::Canonical,
        )],
    )
    .await?;

    let promoted = upsert_normalized_events(
        database.pool(),
        &[normalized_event(
            "manifest:1:source_manifest",
            "SourceManifestUpdated",
            CanonicalityState::Finalized,
        )],
    )
    .await?;

    assert_eq!(promoted.len(), 1);
    assert_eq!(promoted[0].canonicality_state, CanonicalityState::Finalized);

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_upsert_skips_unchanged_conflicts() -> Result<()> {
    let database = TestDatabase::new().await?;
    let event = normalized_event(
        "manifest:1:unchanged",
        "SourceManifestUpdated",
        CanonicalityState::Finalized,
    );

    upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;

    let anchored_observed_at = sqlx::types::time::OffsetDateTime::from_unix_timestamp(946_684_800)?;
    sqlx::query("UPDATE normalized_events SET observed_at = $1 WHERE event_identity = $2")
        .bind(anchored_observed_at)
        .bind(&event.event_identity)
        .execute(database.pool())
        .await?;

    let snapshots = upsert_normalized_events(database.pool(), std::slice::from_ref(&event)).await?;
    assert_eq!(
        snapshots[0].canonicality_state,
        CanonicalityState::Finalized
    );

    let observed_at = sqlx::query_scalar::<_, sqlx::types::time::OffsetDateTime>(
        "SELECT observed_at FROM normalized_events WHERE event_identity = $1",
    )
    .bind(&event.event_identity)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(observed_at, anchored_observed_at);

    database.cleanup().await
}

#[tokio::test]
async fn normalized_event_upsert_escapes_nul_bytes_for_jsonb() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut event = normalized_event(
        "manifest:1:nul-byte",
        "CapabilityChanged",
        CanonicalityState::Finalized,
    );
    event.logical_name_id = Some("name\0with-nul".to_owned());
    event.after_state = json!({
        "record": "before\0after",
        "key\0with-nul": "value",
        "nested": ["left\0right"],
    });

    let inserted = upsert_normalized_events(database.pool(), &[event]).await?;
    assert_eq!(
        inserted[0].logical_name_id.as_deref(),
        Some("name\\u0000with-nul")
    );
    assert_eq!(
        inserted[0].after_state,
        json!({
            "record": "before\\u0000after",
            "key\\u0000with-nul": "value",
            "nested": ["left\\u0000right"],
        })
    );

    let loaded = load_normalized_events_by_namespace(database.pool(), "ens").await?;
    assert_eq!(loaded, inserted);

    database.cleanup().await
}

#[tokio::test]
async fn orphan_range_marks_block_derived_normalized_events_orphaned() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_raw_blocks(
        database.pool(),
        &[
            RawBlock {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x001".to_owned(),
                parent_hash: None,
                block_number: 1,
                block_timestamp: sqlx::types::time::OffsetDateTime::UNIX_EPOCH,
                logs_bloom: None,
                transactions_root: None,
                receipts_root: None,
                state_root: None,
                canonicality_state: CanonicalityState::Canonical,
            },
            RawBlock {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x002".to_owned(),
                parent_hash: Some("0x001".to_owned()),
                block_number: 2,
                block_timestamp: sqlx::types::time::OffsetDateTime::UNIX_EPOCH,
                logs_bloom: None,
                transactions_root: None,
                receipts_root: None,
                state_root: None,
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    upsert_normalized_events(
        database.pool(),
        &[
            NormalizedEvent {
                chain_id: Some("ethereum-mainnet".to_owned()),
                block_number: Some(1),
                block_hash: Some("0x001".to_owned()),
                transaction_hash: Some("0xtx1".to_owned()),
                log_index: Some(0),
                event_identity: "preimage:0x001:0".to_owned(),
                event_kind: "PreimageObserved".to_owned(),
                ..normalized_event(
                    "preimage:0x001:0",
                    "PreimageObserved",
                    CanonicalityState::Canonical,
                )
            },
            NormalizedEvent {
                chain_id: Some("ethereum-mainnet".to_owned()),
                block_number: Some(2),
                block_hash: Some("0x002".to_owned()),
                transaction_hash: Some("0xtx2".to_owned()),
                log_index: Some(1),
                event_identity: "preimage:0x002:1".to_owned(),
                event_kind: "PreimageObserved".to_owned(),
                ..normalized_event(
                    "preimage:0x002:1",
                    "PreimageObserved",
                    CanonicalityState::Finalized,
                )
            },
        ],
    )
    .await?;

    let orphaned_count = mark_block_derived_normalized_events_range_orphaned(
        database.pool(),
        "ethereum-mainnet",
        "0x002",
        Some("0x001"),
    )
    .await?;
    assert_eq!(orphaned_count, 1);

    let events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].canonicality_state, CanonicalityState::Canonical);
    assert_eq!(events[1].canonicality_state, CanonicalityState::Orphaned);

    database.cleanup().await
}
