use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use serde_json::json;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::time::OffsetDateTime,
};
use tokio::time::sleep;

use super::*;
use crate::{
    RawBlock, default_database_url, mark_raw_block_facts_range_orphaned, upsert_raw_blocks,
};

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
            .context("failed to parse database URL for raw call snapshot tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bn_st_raw_call_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for raw call snapshot tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect raw call snapshot test pool")?;

        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for raw call snapshot tests")?;

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

fn raw_call_snapshot(request_hash: &str, state: CanonicalityState) -> RawCallSnapshot {
    RawCallSnapshot {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: "0xaaa".to_owned(),
        block_number: 101,
        request_hash: request_hash.to_owned(),
        request_payload: json!({
            "to": "0x0000000000000000000000000000000000000001",
            "data": format!("0xcall-{request_hash}")
        }),
        response_hash: format!("0xresponse-{request_hash}"),
        response_payload: json!({
            "result": format!("0xresult-{request_hash}")
        }),
        canonicality_state: state,
    }
}

fn raw_block(block_hash: &str, parent_hash: &str, block_number: i64) -> RawBlock {
    RawBlock {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: Some(parent_hash.to_owned()),
        block_number,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_000 + block_number)
            .expect("valid block timestamp"),
        logs_bloom: None,
        transactions_root: Some(format!("0xtxroot-{block_hash}")),
        receipts_root: Some(format!("0xreceipts-{block_hash}")),
        state_root: Some(format!("0xstate-{block_hash}")),
        canonicality_state: CanonicalityState::Canonical,
    }
}

async fn load_observed_at(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
    request_hash: &str,
) -> Result<OffsetDateTime> {
    sqlx::query_scalar(
        r#"
        SELECT observed_at
        FROM raw_call_snapshots
        WHERE chain_id = $1
          AND block_hash = $2
          AND request_hash = $3
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(request_hash)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load observed_at for raw call snapshot chain {chain_id} block {block_hash} request {request_hash}"
        )
    })
}

#[tokio::test]
async fn upserts_and_loads_raw_call_snapshots_by_exact_block_identity() -> Result<()> {
    let database = TestDatabase::new().await?;

    let mut transaction = database.pool().begin().await?;
    upsert_raw_call_snapshots_in_transaction(
        &mut transaction,
        &[
            raw_call_snapshot("0xreq-b", CanonicalityState::Canonical),
            raw_call_snapshot("0xreq-a", CanonicalityState::Observed),
            RawCallSnapshot {
                block_hash: "0xbbb".to_owned(),
                block_number: 102,
                request_hash: "0xreq-c".to_owned(),
                ..raw_call_snapshot("0xreq-c", CanonicalityState::Safe)
            },
        ],
    )
    .await?;
    transaction.commit().await?;

    let loaded =
        load_raw_call_snapshots_by_block_hash(database.pool(), "eth-mainnet", "0xaaa").await?;

    assert_eq!(
        loaded,
        vec![
            raw_call_snapshot("0xreq-a", CanonicalityState::Observed),
            raw_call_snapshot("0xreq-b", CanonicalityState::Canonical),
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn load_by_block_hash_includes_orphaned_raw_call_snapshots() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block("0x001", "0x000", 1),
            raw_block("0x002", "0x001", 2),
        ],
    )
    .await?;
    upsert_raw_call_snapshots(
        database.pool(),
        &[
            RawCallSnapshot {
                block_hash: "0x001".to_owned(),
                block_number: 1,
                request_hash: "0xreq-001".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                ..raw_call_snapshot("0xreq-001", CanonicalityState::Canonical)
            },
            RawCallSnapshot {
                block_hash: "0x002".to_owned(),
                block_number: 2,
                request_hash: "0xreq-002".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                ..raw_call_snapshot("0xreq-002", CanonicalityState::Canonical)
            },
        ],
    )
    .await?;

    let counts =
        mark_raw_block_facts_range_orphaned(database.pool(), "eth-mainnet", "0x002", Some("0x001"))
            .await?;
    assert_eq!(counts.call_snapshot_count, 1);

    let orphaned =
        load_raw_call_snapshots_by_block_hash(database.pool(), "eth-mainnet", "0x002").await?;
    assert_eq!(
        orphaned,
        vec![RawCallSnapshot {
            block_hash: "0x002".to_owned(),
            block_number: 2,
            request_hash: "0xreq-002".to_owned(),
            canonicality_state: CanonicalityState::Orphaned,
            ..raw_call_snapshot("0xreq-002", CanonicalityState::Canonical)
        }]
    );

    let canonical =
        load_raw_call_snapshots_by_block_hash(database.pool(), "eth-mainnet", "0x001").await?;
    assert_eq!(
        canonical,
        vec![RawCallSnapshot {
            block_hash: "0x001".to_owned(),
            block_number: 1,
            request_hash: "0xreq-001".to_owned(),
            canonicality_state: CanonicalityState::Canonical,
            ..raw_call_snapshot("0xreq-001", CanonicalityState::Canonical)
        }]
    );

    database.cleanup().await
}

#[tokio::test]
async fn raw_call_snapshot_upsert_promotes_and_reobserves() -> Result<()> {
    let database = TestDatabase::new().await?;

    let inserted = upsert_raw_call_snapshots(
        database.pool(),
        &[raw_call_snapshot("0xreq-a", CanonicalityState::Observed)],
    )
    .await?;
    assert_eq!(inserted[0].canonicality_state, CanonicalityState::Observed);

    let observed_at_before =
        load_observed_at(database.pool(), "eth-mainnet", "0xaaa", "0xreq-a").await?;

    sleep(Duration::from_millis(5)).await;

    let promoted = upsert_raw_call_snapshots(
        database.pool(),
        &[raw_call_snapshot("0xreq-a", CanonicalityState::Canonical)],
    )
    .await?;
    assert_eq!(promoted[0].canonicality_state, CanonicalityState::Canonical);

    let observed_at_after_promotion =
        load_observed_at(database.pool(), "eth-mainnet", "0xaaa", "0xreq-a").await?;
    assert!(observed_at_after_promotion > observed_at_before);

    sleep(Duration::from_millis(5)).await;

    let reobserved = upsert_raw_call_snapshots(
        database.pool(),
        &[raw_call_snapshot("0xreq-a", CanonicalityState::Observed)],
    )
    .await?;
    assert_eq!(
        reobserved[0].canonicality_state,
        CanonicalityState::Canonical
    );

    let observed_at_after_reobservation =
        load_observed_at(database.pool(), "eth-mainnet", "0xaaa", "0xreq-a").await?;
    assert!(observed_at_after_reobservation > observed_at_after_promotion);

    database.cleanup().await
}

#[tokio::test]
async fn raw_call_snapshot_upsert_rejects_identity_mismatch() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_raw_call_snapshots(
        database.pool(),
        &[raw_call_snapshot("0xreq-a", CanonicalityState::Canonical)],
    )
    .await?;

    let mut conflicting = raw_call_snapshot("0xreq-a", CanonicalityState::Observed);
    conflicting.response_hash = "0xresponse-conflict".to_owned();
    let error = upsert_raw_call_snapshots(database.pool(), &[conflicting])
        .await
        .expect_err("immutable raw call snapshot identity mismatch must fail");

    assert!(
        error.to_string().contains(
            "raw call snapshot identity mismatch for chain eth-mainnet block 0xaaa request 0xreq-a"
        ),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}
