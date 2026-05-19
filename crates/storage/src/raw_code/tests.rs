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
};

use super::*;
use crate::default_database_url;

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
