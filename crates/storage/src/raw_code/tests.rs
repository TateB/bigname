use std::{
    collections::BTreeMap,
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
use crate::{ChainLineageBlock, default_database_url, upsert_chain_lineage_blocks};

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
            .context("failed to parse database URL for raw code-hash tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_storage_raw_code_hash_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for raw code-hash tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect raw code-hash test pool")?;

        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for raw code-hash tests")?;

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

fn raw_code_hash(address: &str, state: CanonicalityState) -> RawCodeHash {
    RawCodeHash {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: "0xaaa".to_owned(),
        block_number: 101,
        contract_address: address.to_owned(),
        code_hash: "0x1234".to_owned(),
        code_byte_length: 32,
        canonicality_state: state,
    }
}

fn lineage_block(
    block_hash: &str,
    block_number: i64,
    state: CanonicalityState,
) -> ChainLineageBlock {
    ChainLineageBlock {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: None,
        block_number,
        block_timestamp: OffsetDateTime::UNIX_EPOCH,
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
        canonicality_state: state,
    }
}

#[tokio::test]
async fn upserts_and_promotes_raw_code_hashes() -> Result<()> {
    let database = TestDatabase::new().await?;

    let inserted = upsert_raw_code_hashes(
        database.pool(),
        &[raw_code_hash("0x0001", CanonicalityState::Canonical)],
    )
    .await?;
    assert_eq!(inserted.len(), 1);
    assert_eq!(inserted[0].canonicality_state, CanonicalityState::Canonical);

    let promoted = upsert_raw_code_hashes(
        database.pool(),
        &[raw_code_hash("0x0001", CanonicalityState::Finalized)],
    )
    .await?;
    assert_eq!(promoted.len(), 1);
    assert_eq!(promoted[0].canonicality_state, CanonicalityState::Finalized);

    database.cleanup().await
}

#[tokio::test]
async fn bulk_upserts_and_promotes_raw_code_hashes() -> Result<()> {
    let database = TestDatabase::new().await?;
    let code_hashes = (0_i64..150)
        .map(|index| RawCodeHash {
            block_hash: format!("0xblock{index:064x}"),
            block_number: index,
            contract_address: format!("0x{index:040x}"),
            ..raw_code_hash("0x0001", CanonicalityState::Canonical)
        })
        .collect::<Vec<_>>();

    let inserted = upsert_raw_code_hashes(database.pool(), &code_hashes).await?;

    assert_eq!(inserted.len(), code_hashes.len());
    assert!(
        inserted
            .iter()
            .all(|code_hash| code_hash.canonicality_state == CanonicalityState::Canonical)
    );

    let promoted_code_hashes = code_hashes
        .iter()
        .cloned()
        .map(|mut code_hash| {
            code_hash.canonicality_state = CanonicalityState::Finalized;
            code_hash
        })
        .collect::<Vec<_>>();
    let promoted = upsert_raw_code_hashes(database.pool(), &promoted_code_hashes).await?;

    assert_eq!(promoted.len(), promoted_code_hashes.len());
    assert!(
        promoted
            .iter()
            .all(|code_hash| code_hash.canonicality_state == CanonicalityState::Finalized)
    );

    database.cleanup().await
}

#[tokio::test]
async fn raw_code_hash_upsert_rejects_identity_mismatch() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_raw_code_hashes(
        database.pool(),
        &[raw_code_hash("0x0001", CanonicalityState::Canonical)],
    )
    .await?;

    let mut conflicting = raw_code_hash("0x0001", CanonicalityState::Observed);
    conflicting.code_hash = "0xffff".to_owned();
    let error = upsert_raw_code_hashes(database.pool(), &[conflicting])
        .await
        .expect_err("immutable raw code-hash identity mismatch must fail");

    assert!(
        error.to_string().contains(
            "raw code-hash identity mismatch for chain eth-mainnet block 0xaaa contract 0x0001"
        ),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn raw_code_hash_count_lookup_groups_by_block() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_raw_code_hashes(
        database.pool(),
        &[
            raw_code_hash("0x0001", CanonicalityState::Canonical),
            raw_code_hash("0x0002", CanonicalityState::Canonical),
            RawCodeHash {
                block_hash: "0xbbb".to_owned(),
                block_number: 102,
                contract_address: "0x0003".to_owned(),
                ..raw_code_hash("0x0003", CanonicalityState::Safe)
            },
        ],
    )
    .await?;

    let counts = load_raw_code_hash_counts_by_block_hashes(
        database.pool(),
        "eth-mainnet",
        &["0xaaa".to_owned(), "0xbbb".to_owned(), "0xccc".to_owned()],
    )
    .await?;

    assert_eq!(
        counts,
        BTreeMap::from([("0xaaa".to_owned(), 2_usize), ("0xbbb".to_owned(), 1_usize),])
    );

    database.cleanup().await
}

#[tokio::test]
async fn raw_code_hash_correction_selection_skips_orphaned_and_missing_lineage() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_raw_code_hashes(
        database.pool(),
        &[
            RawCodeHash {
                block_hash: "0xaaa1".to_owned(),
                block_number: 101,
                contract_address: "0x0001".to_owned(),
                ..raw_code_hash("0x0001", CanonicalityState::Canonical)
            },
            RawCodeHash {
                block_hash: "0xaaa2".to_owned(),
                block_number: 102,
                contract_address: "0x0002".to_owned(),
                ..raw_code_hash("0x0002", CanonicalityState::Canonical)
            },
            RawCodeHash {
                block_hash: "0xaaa3".to_owned(),
                block_number: 103,
                contract_address: "0x0003".to_owned(),
                ..raw_code_hash("0x0003", CanonicalityState::Canonical)
            },
        ],
    )
    .await?;
    upsert_chain_lineage_blocks(
        database.pool(),
        &[
            lineage_block("0xaaa1", 101, CanonicalityState::Canonical),
            lineage_block("0xaaa2", 102, CanonicalityState::Orphaned),
        ],
    )
    .await?;

    let observed_from = OffsetDateTime::UNIX_EPOCH;
    let observed_before = OffsetDateTime::from_unix_timestamp(4_102_444_800)
        .context("failed to construct raw code-hash correction test upper bound")?;
    let candidate_count = count_raw_code_hash_correction_candidates(
        database.pool(),
        "eth-mainnet",
        observed_from,
        observed_before,
    )
    .await?;
    let skipped_count = count_raw_code_hash_correction_orphaned_skips(
        database.pool(),
        "eth-mainnet",
        observed_from,
        observed_before,
    )
    .await?;
    let page = load_raw_code_hash_correction_page(
        database.pool(),
        "eth-mainnet",
        observed_from,
        observed_before,
        0,
        10,
    )
    .await?;
    let variants = load_raw_code_hash_address_variants(
        database.pool(),
        "eth-mainnet",
        observed_from,
        observed_before,
    )
    .await?;

    assert_eq!(candidate_count, 1);
    assert_eq!(skipped_count, 2);
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].block_hash, "0xaaa1");
    assert_eq!(variants.len(), 1);
    assert!(variants.contains_key("0x0001"));

    database.cleanup().await
}

#[tokio::test]
async fn raw_code_hash_correction_updates_only_hash_and_length() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_raw_code_hashes(
        database.pool(),
        &[raw_code_hash("0x0001", CanonicalityState::Canonical)],
    )
    .await?;
    let row_id = sqlx::query_scalar::<_, i64>(
        "SELECT raw_code_hash_id FROM raw_code_hashes WHERE contract_address = '0x0001'",
    )
    .fetch_one(database.pool())
    .await?;
    let before = sqlx::query_as::<_, (String, i64, String, sqlx::types::time::OffsetDateTime)>(
        r#"
        SELECT
            code_hash,
            code_byte_length,
            canonicality_state::TEXT,
            observed_at
        FROM raw_code_hashes
        WHERE raw_code_hash_id = $1
        "#,
    )
    .bind(row_id)
    .fetch_one(database.pool())
    .await?;

    let outcome = apply_raw_code_hash_corrections(
        database.pool(),
        &[RawCodeHashCorrectionUpdate {
            raw_code_hash_id: row_id,
            stored_code_hash: "0x1234".to_owned(),
            stored_code_byte_length: 32,
            corrected_code_hash: "0xabcd".to_owned(),
            corrected_code_byte_length: 17,
        }],
    )
    .await?;

    assert_eq!(
        outcome,
        RawCodeHashCorrectionBatchOutcome {
            requested_count: 1,
            corrected_count: 1,
            already_correct_count: 0,
            conflicting_count: 0,
        }
    );
    let after = sqlx::query_as::<_, (String, i64, String, sqlx::types::time::OffsetDateTime)>(
        r#"
        SELECT
            code_hash,
            code_byte_length,
            canonicality_state::TEXT,
            observed_at
        FROM raw_code_hashes
        WHERE raw_code_hash_id = $1
        "#,
    )
    .bind(row_id)
    .fetch_one(database.pool())
    .await?;

    assert_eq!(before.0, "0x1234");
    assert_eq!(after.0, "0xabcd");
    assert_eq!(after.1, 17);
    assert_eq!(after.2, before.2);
    assert_eq!(after.3, before.3);

    let rerun = apply_raw_code_hash_corrections(
        database.pool(),
        &[RawCodeHashCorrectionUpdate {
            raw_code_hash_id: row_id,
            stored_code_hash: "0x1234".to_owned(),
            stored_code_byte_length: 32,
            corrected_code_hash: "0xabcd".to_owned(),
            corrected_code_byte_length: 17,
        }],
    )
    .await?;

    assert_eq!(
        rerun,
        RawCodeHashCorrectionBatchOutcome {
            requested_count: 1,
            corrected_count: 0,
            already_correct_count: 1,
            conflicting_count: 0,
        }
    );

    database.cleanup().await
}

#[tokio::test]
async fn raw_code_hash_correction_refuses_conflicting_current_value() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_raw_code_hashes(
        database.pool(),
        &[raw_code_hash("0x0001", CanonicalityState::Canonical)],
    )
    .await?;
    let row_id = sqlx::query_scalar::<_, i64>(
        "SELECT raw_code_hash_id FROM raw_code_hashes WHERE contract_address = '0x0001'",
    )
    .fetch_one(database.pool())
    .await?;
    sqlx::query("UPDATE raw_code_hashes SET code_hash = '0xbeef' WHERE raw_code_hash_id = $1")
        .bind(row_id)
        .execute(database.pool())
        .await?;

    let error = apply_raw_code_hash_corrections(
        database.pool(),
        &[RawCodeHashCorrectionUpdate {
            raw_code_hash_id: row_id,
            stored_code_hash: "0x1234".to_owned(),
            stored_code_byte_length: 32,
            corrected_code_hash: "0xabcd".to_owned(),
            corrected_code_byte_length: 17,
        }],
    )
    .await
    .expect_err("conflicting current value must fail closed");

    assert!(
        error.to_string().contains("1 conflicting rows"),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}
