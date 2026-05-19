use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use sqlx::types::time::OffsetDateTime;
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
            .context("failed to parse database URL for storage lineage integration tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_storage_lineage_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for storage lineage integration tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect storage lineage integration test pool")?;

        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for storage lineage integration tests")?;

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

fn block(
    chain_id: &str,
    block_hash: &str,
    parent_hash: Option<&str>,
    block_number: i64,
    block_timestamp: OffsetDateTime,
    canonicality_state: CanonicalityState,
) -> ChainLineageBlock {
    ChainLineageBlock {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: parent_hash.map(str::to_owned),
        block_number,
        block_timestamp,
        logs_bloom: Some(vec![block_number as u8]),
        transactions_root: Some(format!("0xtx{:02x}", block_number)),
        receipts_root: Some(format!("0xrc{:02x}", block_number)),
        state_root: Some(format!("0xst{:02x}", block_number)),
        canonicality_state,
    }
}

fn timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
}

#[tokio::test]
async fn upserts_and_loads_lineage_blocks() -> Result<()> {
    let database = TestDatabase::new().await?;
    let timestamp = timestamp(1_717_171_717);

    let blocks = upsert_chain_lineage_blocks(
        database.pool(),
        &[block(
            "eth-mainnet",
            "0xaaa",
            Some("0x999"),
            10,
            timestamp,
            CanonicalityState::Observed,
        )],
    )
    .await?;

    assert_eq!(blocks.len(), 1);
    assert_eq!(
        load_chain_lineage_block(database.pool(), "eth-mainnet", "0xaaa").await?,
        Some(blocks[0].clone())
    );

    database.cleanup().await
}

#[tokio::test]
async fn bulk_upserts_and_promotes_lineage_blocks() -> Result<()> {
    let database = TestDatabase::new().await?;
    let blocks = (0_i64..150)
        .map(|number| {
            let block_hash = format!("0xblock{number:064x}");
            let parent_hash = format!("0xparent{number:064x}");
            block(
                "eth-mainnet",
                &block_hash,
                Some(&parent_hash),
                number,
                timestamp(1_717_180_000 + number),
                CanonicalityState::Canonical,
            )
        })
        .collect::<Vec<_>>();

    let inserted = upsert_chain_lineage_blocks(database.pool(), &blocks).await?;

    assert_eq!(inserted.len(), blocks.len());
    assert!(
        inserted
            .iter()
            .all(|block| block.canonicality_state == CanonicalityState::Canonical)
    );

    let promoted_blocks = blocks
        .iter()
        .cloned()
        .map(|mut block| {
            block.canonicality_state = CanonicalityState::Finalized;
            block
        })
        .collect::<Vec<_>>();
    let promoted = upsert_chain_lineage_blocks(database.pool(), &promoted_blocks).await?;

    assert_eq!(promoted.len(), promoted_blocks.len());
    assert!(
        promoted
            .iter()
            .all(|block| block.canonicality_state == CanonicalityState::Finalized)
    );

    database.cleanup().await
}

#[tokio::test]
async fn minimal_lineage_replay_can_be_audit_enriched_without_clearing_fields() -> Result<()> {
    let database = TestDatabase::new().await?;
    let timestamp = timestamp(1_717_171_717);

    let mut minimal = block(
        "eth-mainnet",
        "0xaaa",
        Some("0x999"),
        10,
        timestamp,
        CanonicalityState::Observed,
    );
    minimal.logs_bloom = None;
    minimal.transactions_root = None;
    minimal.receipts_root = None;
    minimal.state_root = None;
    upsert_chain_lineage_blocks(database.pool(), &[minimal.clone()]).await?;

    let audited = block(
        "eth-mainnet",
        "0xaaa",
        Some("0x999"),
        10,
        timestamp,
        CanonicalityState::Canonical,
    );
    let refreshed =
        upsert_chain_lineage_blocks(database.pool(), std::slice::from_ref(&audited)).await?;
    assert_eq!(
        refreshed[0].canonicality_state,
        CanonicalityState::Canonical
    );
    assert_eq!(refreshed[0].logs_bloom, audited.logs_bloom);
    assert_eq!(refreshed[0].transactions_root, audited.transactions_root);
    assert_eq!(refreshed[0].receipts_root, audited.receipts_root);
    assert_eq!(refreshed[0].state_root, audited.state_root);

    let minimal_replay = upsert_chain_lineage_blocks(database.pool(), &[minimal.clone()]).await?;
    assert_eq!(
        minimal_replay[0].canonicality_state,
        CanonicalityState::Canonical
    );
    assert_eq!(minimal_replay[0].logs_bloom, audited.logs_bloom);
    assert_eq!(
        minimal_replay[0].transactions_root,
        audited.transactions_root
    );
    assert_eq!(minimal_replay[0].receipts_root, audited.receipts_root);
    assert_eq!(minimal_replay[0].state_root, audited.state_root);

    let mut conflicting = audited;
    conflicting.state_root = Some("0xconflict".to_owned());
    let error = upsert_chain_lineage_blocks(database.pool(), &[conflicting])
        .await
        .expect_err("conflicting audited lineage field must fail");
    assert!(
        error.to_string().contains("header audit identity mismatch"),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn reobserving_orphaned_block_revives_observed_state_without_rewriting_identity() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let timestamp = timestamp(1_717_171_717);

    upsert_chain_lineage_blocks(
        database.pool(),
        &[block(
            "eth-mainnet",
            "0xaaa",
            Some("0x999"),
            10,
            timestamp,
            CanonicalityState::Observed,
        )],
    )
    .await?;

    mark_chain_lineage_range_orphaned(database.pool(), "eth-mainnet", "0xaaa", None).await?;

    let refreshed = upsert_chain_lineage_blocks(
        database.pool(),
        &[block(
            "eth-mainnet",
            "0xaaa",
            Some("0x999"),
            10,
            timestamp,
            CanonicalityState::Observed,
        )],
    )
    .await?;

    assert_eq!(refreshed[0].canonicality_state, CanonicalityState::Observed);

    database.cleanup().await
}

#[tokio::test]
async fn orphan_range_stops_before_requested_ancestor() -> Result<()> {
    let database = TestDatabase::new().await?;
    let base_timestamp = timestamp(1_717_171_717);

    upsert_chain_lineage_blocks(
        database.pool(),
        &[
            block(
                "eth-mainnet",
                "0x001",
                None,
                1,
                base_timestamp,
                CanonicalityState::Canonical,
            ),
            block(
                "eth-mainnet",
                "0x002",
                Some("0x001"),
                2,
                timestamp(1_717_171_729),
                CanonicalityState::Canonical,
            ),
            block(
                "eth-mainnet",
                "0x003",
                Some("0x002"),
                3,
                timestamp(1_717_171_741),
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let orphaned =
        mark_chain_lineage_range_orphaned(database.pool(), "eth-mainnet", "0x003", Some("0x001"))
            .await?;

    assert_eq!(
        orphaned
            .into_iter()
            .map(|snapshot| (snapshot.block_hash, snapshot.canonicality_state))
            .collect::<Vec<_>>(),
        vec![
            ("0x003".to_owned(), CanonicalityState::Orphaned),
            ("0x002".to_owned(), CanonicalityState::Orphaned),
        ]
    );
    assert_eq!(
        load_chain_lineage_block(database.pool(), "eth-mainnet", "0x001")
            .await?
            .expect("ancestor row must still exist")
            .canonicality_state,
        CanonicalityState::Canonical
    );

    database.cleanup().await
}
