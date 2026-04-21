use std::{
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

use super::*;
use crate::{
    RawBlock, RawCallSnapshot, RawCodeHash, default_database_url, upsert_raw_blocks,
    upsert_raw_call_snapshots, upsert_raw_code_hashes,
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
            .context("failed to parse database URL for raw child fact tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_storage_raw_child_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for raw child fact tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect raw child fact test pool")?;

        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for raw child fact tests")?;

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

fn raw_block(block_hash: &str, parent_hash: &str, block_number: i64) -> RawBlock {
    RawBlock {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: Some(parent_hash.to_owned()),
        block_number,
        block_timestamp: sqlx::types::time::OffsetDateTime::from_unix_timestamp(
            1_700_000_000 + block_number,
        )
        .expect("timestamp must be valid"),
        logs_bloom: Some(vec![block_number as u8]),
        transactions_root: Some(format!("0xtxroot{block_number:02x}")),
        receipts_root: Some(format!("0xrcroot{block_number:02x}")),
        state_root: Some(format!("0xstroot{block_number:02x}")),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn raw_transaction(state: CanonicalityState) -> RawTransaction {
    RawTransaction {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: "0xaaa".to_owned(),
        block_number: 101,
        transaction_hash: "0xtxaaa".to_owned(),
        transaction_index: 0,
        from_address: "0x0000000000000000000000000000000000000001".to_owned(),
        to_address: Some("0x0000000000000000000000000000000000000002".to_owned()),
        canonicality_state: state,
    }
}

fn raw_receipt(state: CanonicalityState) -> RawReceipt {
    RawReceipt {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: "0xaaa".to_owned(),
        block_number: 101,
        transaction_hash: "0xtxaaa".to_owned(),
        transaction_index: 0,
        contract_address: None,
        status: Some(true),
        gas_used: Some(21_000),
        cumulative_gas_used: Some(21_000),
        logs_bloom: Some(vec![0xaa]),
        canonicality_state: state,
    }
}

fn raw_log(state: CanonicalityState) -> RawLog {
    RawLog {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: "0xaaa".to_owned(),
        block_number: 101,
        transaction_hash: "0xtxaaa".to_owned(),
        transaction_index: 0,
        log_index: 0,
        emitting_address: "0x0000000000000000000000000000000000000003".to_owned(),
        topics: vec!["0xtopic0".to_owned(), "0xtopic1".to_owned()],
        data: vec![0xde, 0xad, 0xbe, 0xef],
        canonicality_state: state,
    }
}

fn raw_call_snapshot(
    block_hash: &str,
    block_number: i64,
    request_hash: &str,
    state: CanonicalityState,
) -> RawCallSnapshot {
    RawCallSnapshot {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
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

#[tokio::test]
async fn upserts_raw_transactions_receipts_and_logs() -> Result<()> {
    let database = TestDatabase::new().await?;

    let transactions = upsert_raw_transactions(
        database.pool(),
        &[raw_transaction(CanonicalityState::Canonical)],
    )
    .await?;
    let receipts = upsert_raw_receipts(
        database.pool(),
        &[raw_receipt(CanonicalityState::Canonical)],
    )
    .await?;
    let logs = upsert_raw_logs(database.pool(), &[raw_log(CanonicalityState::Canonical)]).await?;

    assert_eq!(transactions.len(), 1);
    assert_eq!(
        transactions[0].canonicality_state,
        CanonicalityState::Canonical
    );
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].canonicality_state, CanonicalityState::Canonical);
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].canonicality_state, CanonicalityState::Canonical);

    let promoted_transactions = upsert_raw_transactions(
        database.pool(),
        &[raw_transaction(CanonicalityState::Finalized)],
    )
    .await?;
    let promoted_receipts = upsert_raw_receipts(
        database.pool(),
        &[raw_receipt(CanonicalityState::Finalized)],
    )
    .await?;
    let promoted_logs =
        upsert_raw_logs(database.pool(), &[raw_log(CanonicalityState::Finalized)]).await?;

    assert_eq!(
        promoted_transactions[0].canonicality_state,
        CanonicalityState::Finalized
    );
    assert_eq!(
        promoted_receipts[0].canonicality_state,
        CanonicalityState::Finalized
    );
    assert_eq!(
        promoted_logs[0].canonicality_state,
        CanonicalityState::Finalized
    );

    database.cleanup().await
}

#[tokio::test]
async fn rejects_mismatched_raw_transaction_identity() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_raw_transactions(
        database.pool(),
        &[raw_transaction(CanonicalityState::Canonical)],
    )
    .await?;

    let mut conflicting = raw_transaction(CanonicalityState::Observed);
    conflicting.from_address = "0x0000000000000000000000000000000000000009".to_owned();
    let error = upsert_raw_transactions(database.pool(), &[conflicting])
        .await
        .expect_err("immutable raw transaction identity mismatch must fail");

    assert!(
        error.to_string().contains(
            "raw transaction identity mismatch for chain eth-mainnet block 0xaaa index 0"
        ),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn rejects_mismatched_raw_receipt_and_log_identity() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_raw_receipts(
        database.pool(),
        &[raw_receipt(CanonicalityState::Canonical)],
    )
    .await?;
    upsert_raw_logs(database.pool(), &[raw_log(CanonicalityState::Canonical)]).await?;

    let mut conflicting_receipt = raw_receipt(CanonicalityState::Observed);
    conflicting_receipt.gas_used = Some(42_000);
    let receipt_error = upsert_raw_receipts(database.pool(), &[conflicting_receipt])
        .await
        .expect_err("immutable raw receipt identity mismatch must fail");

    assert!(
        receipt_error
            .to_string()
            .contains("raw receipt identity mismatch for chain eth-mainnet block 0xaaa index 0"),
        "unexpected error: {receipt_error:#}"
    );

    let mut conflicting_log = raw_log(CanonicalityState::Observed);
    conflicting_log.data = vec![0xca, 0xfe];
    let log_error = upsert_raw_logs(database.pool(), &[conflicting_log])
        .await
        .expect_err("immutable raw log identity mismatch must fail");

    assert!(
        log_error
            .to_string()
            .contains("raw log identity mismatch for chain eth-mainnet block 0xaaa log 0"),
        "unexpected error: {log_error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn orphan_range_marks_raw_block_children_orphaned() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block("0x001", "0x000", 1),
            raw_block("0x002", "0x001", 2),
        ],
    )
    .await?;

    upsert_raw_transactions(
        database.pool(),
        &[RawTransaction {
            block_hash: "0x002".to_owned(),
            block_number: 2,
            transaction_hash: "0xtx002".to_owned(),
            canonicality_state: CanonicalityState::Canonical,
            ..raw_transaction(CanonicalityState::Canonical)
        }],
    )
    .await?;
    upsert_raw_receipts(
        database.pool(),
        &[RawReceipt {
            block_hash: "0x002".to_owned(),
            block_number: 2,
            transaction_hash: "0xtx002".to_owned(),
            canonicality_state: CanonicalityState::Canonical,
            ..raw_receipt(CanonicalityState::Canonical)
        }],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[RawLog {
            block_hash: "0x002".to_owned(),
            block_number: 2,
            transaction_hash: "0xtx002".to_owned(),
            canonicality_state: CanonicalityState::Canonical,
            ..raw_log(CanonicalityState::Canonical)
        }],
    )
    .await?;
    upsert_raw_code_hashes(
        database.pool(),
        &[RawCodeHash {
            chain_id: "eth-mainnet".to_owned(),
            block_hash: "0x002".to_owned(),
            block_number: 2,
            contract_address: "0x00000000000000000000000000000000000000aa".to_owned(),
            code_hash: "0x1234".to_owned(),
            code_byte_length: 32,
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_raw_call_snapshots(
        database.pool(),
        &[
            raw_call_snapshot("0x002", 2, "0xreq-002", CanonicalityState::Canonical),
            raw_call_snapshot("0x001", 1, "0xreq-001", CanonicalityState::Canonical),
        ],
    )
    .await?;

    let counts =
        mark_raw_block_facts_range_orphaned(database.pool(), "eth-mainnet", "0x002", Some("0x001"))
            .await?;
    assert_eq!(
        counts,
        RawFactOrphanCounts {
            block_count: 1,
            code_hash_count: 1,
            transaction_count: 1,
            receipt_count: 1,
            log_count: 1,
            call_snapshot_count: 1,
        }
    );

    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_hash = '0x002'"
        )
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_transactions WHERE block_hash = '0x002'"
        )
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_receipts WHERE block_hash = '0x002'"
        )
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_logs WHERE block_hash = '0x002'"
        )
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_hash = '0x002'"
        )
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_call_snapshots WHERE block_hash = '0x002' AND request_hash = '0xreq-002'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_hash = '0x001'"
        )
        .fetch_one(database.pool())
        .await?,
        "canonical".to_owned()
    );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_call_snapshots WHERE block_hash = '0x001' AND request_hash = '0xreq-001'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );

    database.cleanup().await
}
