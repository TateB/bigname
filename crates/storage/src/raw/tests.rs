use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::time::OffsetDateTime,
};

use super::*;
use crate::{
    CanonicalityState, ChainLineageBlock, RawLog, default_database_url,
    upsert_chain_lineage_blocks, upsert_raw_logs,
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
            .context("failed to parse database URL for raw block tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_storage_raw_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for raw block tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect raw block test pool")?;

        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for raw block tests")?;

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

fn raw_block(state: CanonicalityState) -> RawBlock {
    RawBlock {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: "0xaaa".to_owned(),
        parent_hash: Some("0x000".to_owned()),
        block_number: 101,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_101)
            .expect("timestamp must be valid"),
        logs_bloom: Some(vec![0xaa]),
        transactions_root: Some("0xbbb".to_owned()),
        receipts_root: Some("0xccc".to_owned()),
        state_root: Some("0xddd".to_owned()),
        canonicality_state: state,
    }
}

fn lineage_block(
    block_hash: &str,
    parent_hash: Option<&str>,
    block_number: i64,
    state: CanonicalityState,
) -> ChainLineageBlock {
    ChainLineageBlock {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: parent_hash.map(ToOwned::to_owned),
        block_number,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_001_000 + block_number)
            .expect("timestamp must be valid"),
        logs_bloom: Some(vec![block_number as u8]),
        transactions_root: Some(format!("0xtxroot-{block_hash}")),
        receipts_root: Some(format!("0xreceipts-{block_hash}")),
        state_root: Some(format!("0xstate-{block_hash}")),
        canonicality_state: state,
    }
}

fn raw_log_at(
    block_hash: &str,
    block_number: i64,
    transaction_index: i64,
    log_index: i64,
    state: CanonicalityState,
) -> RawLog {
    RawLog {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xtx-{block_hash}-{transaction_index}"),
        transaction_index,
        log_index,
        emitting_address: "0x0000000000000000000000000000000000000003".to_owned(),
        topics: vec![format!("0xtopic-{block_hash}-{log_index}")],
        data: vec![block_number as u8, transaction_index as u8, log_index as u8],
        canonicality_state: state,
    }
}

#[tokio::test]
async fn upserts_and_loads_raw_blocks() -> Result<()> {
    let database = TestDatabase::new().await?;

    let blocks =
        upsert_raw_blocks(database.pool(), &[raw_block(CanonicalityState::Canonical)]).await?;
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].canonicality_state, CanonicalityState::Canonical);
    assert_eq!(
        load_raw_block(database.pool(), "eth-mainnet", "0xaaa").await?,
        Some(blocks[0].clone())
    );

    database.cleanup().await
}

#[tokio::test]
async fn reobserving_orphaned_raw_block_revives_observed_state() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_raw_blocks(database.pool(), &[raw_block(CanonicalityState::Orphaned)]).await?;
    let refreshed =
        upsert_raw_blocks(database.pool(), &[raw_block(CanonicalityState::Observed)]).await?;

    assert_eq!(refreshed[0].canonicality_state, CanonicalityState::Observed);

    database.cleanup().await
}

#[tokio::test]
async fn rejects_mismatched_immutable_raw_block_identity() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_raw_blocks(database.pool(), &[raw_block(CanonicalityState::Canonical)]).await?;

    let mut conflicting = raw_block(CanonicalityState::Observed);
    conflicting.transactions_root = Some("0xconflict".to_owned());
    let error = upsert_raw_blocks(database.pool(), &[conflicting])
        .await
        .expect_err("immutable raw block identity mismatch must fail");

    assert!(
        error
            .to_string()
            .contains("raw block identity mismatch for chain eth-mainnet block 0xaaa"),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn orphan_range_stops_before_requested_ancestor() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_raw_blocks(
        database.pool(),
        &[
            RawBlock {
                chain_id: "eth-mainnet".to_owned(),
                block_hash: "0x001".to_owned(),
                parent_hash: Some("0x000".to_owned()),
                block_number: 1,
                block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_001)
                    .expect("timestamp must be valid"),
                logs_bloom: None,
                transactions_root: None,
                receipts_root: None,
                state_root: None,
                canonicality_state: CanonicalityState::Canonical,
            },
            RawBlock {
                chain_id: "eth-mainnet".to_owned(),
                block_hash: "0x002".to_owned(),
                parent_hash: Some("0x001".to_owned()),
                block_number: 2,
                block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_002)
                    .expect("timestamp must be valid"),
                logs_bloom: None,
                transactions_root: None,
                receipts_root: None,
                state_root: None,
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let orphaned =
        mark_raw_block_range_orphaned(database.pool(), "eth-mainnet", "0x002", Some("0x001"))
            .await?;
    assert_eq!(orphaned.len(), 1);
    assert_eq!(orphaned[0].block_hash, "0x002");
    assert_eq!(orphaned[0].canonicality_state, CanonicalityState::Orphaned);

    let ancestor = load_raw_block(database.pool(), "eth-mainnet", "0x001")
        .await?
        .expect("ancestor raw block must still exist");
    assert_eq!(ancestor.canonicality_state, CanonicalityState::Canonical);

    database.cleanup().await
}

#[tokio::test]
async fn raw_log_replay_inputs_filter_to_canonical_states_in_stable_order() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_chain_lineage_blocks(
        database.pool(),
        &[
            lineage_block("0x200", Some("0x102"), 200, CanonicalityState::Safe),
            lineage_block("0x103", Some("0x102"), 103, CanonicalityState::Observed),
            lineage_block("0x100", None, 100, CanonicalityState::Canonical),
            lineage_block("0x104", Some("0x103"), 104, CanonicalityState::Orphaned),
            lineage_block("0x102", Some("0x101"), 102, CanonicalityState::Finalized),
            lineage_block("0x101", Some("0x100"), 101, CanonicalityState::Safe),
            lineage_block("0x1ff", Some("0x102"), 200, CanonicalityState::Canonical),
        ],
    )
    .await?;

    upsert_raw_logs(
        database.pool(),
        &[
            raw_log_at("0x200", 200, 2, 4, CanonicalityState::Canonical),
            raw_log_at("0x103", 103, 0, 0, CanonicalityState::Canonical),
            raw_log_at("0x100", 100, 1, 2, CanonicalityState::Canonical),
            raw_log_at("0x102", 102, 0, 0, CanonicalityState::Orphaned),
            raw_log_at("0x100", 100, 0, 0, CanonicalityState::Safe),
            raw_log_at("0x104", 104, 0, 0, CanonicalityState::Finalized),
            raw_log_at("0x1ff", 200, 0, 1, CanonicalityState::Finalized),
            raw_log_at("0x101", 101, 0, 0, CanonicalityState::Finalized),
            raw_log_at("0x101", 101, 1, 9, CanonicalityState::Observed),
        ],
    )
    .await?;

    let range_inputs =
        list_canonical_raw_log_replay_inputs(database.pool(), "eth-mainnet", 100, 200).await?;
    let hash_inputs = list_canonical_raw_log_replay_inputs_for_block_hashes(
        database.pool(),
        "eth-mainnet",
        &[
            "0x200".to_owned(),
            "0x103".to_owned(),
            "0x100".to_owned(),
            "0x200".to_owned(),
            "0x104".to_owned(),
            "0x1ff".to_owned(),
            "0x102".to_owned(),
            "0x101".to_owned(),
        ],
    )
    .await?;

    let expected = vec![
        (
            100,
            "0x100",
            0,
            0,
            CanonicalityState::Canonical,
            CanonicalityState::Safe,
        ),
        (
            100,
            "0x100",
            1,
            2,
            CanonicalityState::Canonical,
            CanonicalityState::Canonical,
        ),
        (
            101,
            "0x101",
            0,
            0,
            CanonicalityState::Safe,
            CanonicalityState::Finalized,
        ),
        (
            200,
            "0x1ff",
            0,
            1,
            CanonicalityState::Canonical,
            CanonicalityState::Finalized,
        ),
        (
            200,
            "0x200",
            2,
            4,
            CanonicalityState::Safe,
            CanonicalityState::Canonical,
        ),
    ];

    for inputs in [&range_inputs, &hash_inputs] {
        assert_eq!(
            inputs
                .iter()
                .map(|input| (
                    input.block_number,
                    input.block_hash.as_str(),
                    input.transaction_index,
                    input.log_index,
                    input.lineage_canonicality_state,
                    input.raw_canonicality_state,
                ))
                .collect::<Vec<_>>(),
            expected
        );
    }

    database.cleanup().await
}
