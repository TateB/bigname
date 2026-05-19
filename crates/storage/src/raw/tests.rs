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
    CanonicalityState, ChainLineageBlock, RawLog, default_database_url, load_chain_lineage_block,
    upsert_chain_lineage_blocks, upsert_chain_lineage_blocks_without_snapshots, upsert_raw_logs,
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
async fn bulk_upserts_and_promotes_raw_blocks() -> Result<()> {
    let database = TestDatabase::new().await?;
    let blocks = (0_i64..150)
        .map(|number| RawBlock {
            block_hash: format!("0xblock{number:064x}"),
            parent_hash: Some(format!("0xparent{number:064x}")),
            block_number: number,
            block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_010_000 + number)
                .expect("timestamp must be valid"),
            logs_bloom: Some(vec![number as u8]),
            transactions_root: Some(format!("0xtxroot{number:064x}")),
            receipts_root: Some(format!("0xrcroot{number:064x}")),
            state_root: Some(format!("0xstroot{number:064x}")),
            ..raw_block(CanonicalityState::Canonical)
        })
        .collect::<Vec<_>>();

    let inserted = upsert_raw_blocks(database.pool(), &blocks).await?;

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
    let promoted = upsert_raw_blocks(database.pool(), &promoted_blocks).await?;

    assert_eq!(promoted.len(), promoted_blocks.len());
    assert!(
        promoted
            .iter()
            .all(|block| block.canonicality_state == CanonicalityState::Finalized)
    );

    database.cleanup().await
}

#[tokio::test]
async fn sparse_empty_block_anchors_preserve_raw_facts_and_canonicality() -> Result<()> {
    let database = TestDatabase::new().await?;
    let lineage_anchor = ChainLineageBlock {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: "0xempty".to_owned(),
        parent_hash: Some("0xparent".to_owned()),
        block_number: 1_000,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_001_000)
            .expect("timestamp must be valid"),
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
        canonicality_state: CanonicalityState::Finalized,
    };
    let raw_anchor = RawBlock {
        chain_id: lineage_anchor.chain_id.clone(),
        block_hash: lineage_anchor.block_hash.clone(),
        parent_hash: lineage_anchor.parent_hash.clone(),
        block_number: lineage_anchor.block_number,
        block_timestamp: lineage_anchor.block_timestamp,
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
        canonicality_state: CanonicalityState::Finalized,
    };

    upsert_chain_lineage_blocks_without_snapshots(
        database.pool(),
        std::slice::from_ref(&lineage_anchor),
    )
    .await?;
    upsert_raw_blocks_without_snapshots(database.pool(), std::slice::from_ref(&raw_anchor)).await?;
    let initial_lineage_observed_at = sqlx::query_scalar::<_, OffsetDateTime>(
        "SELECT observed_at FROM chain_lineage WHERE chain_id = $1 AND block_hash = $2",
    )
    .bind("eth-mainnet")
    .bind("0xempty")
    .fetch_one(database.pool())
    .await?;
    let initial_header_audit_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT FROM chain_header_audit WHERE chain_id = $1 AND block_hash = $2",
    )
    .bind("eth-mainnet")
    .bind("0xempty")
    .fetch_one(database.pool())
    .await?;
    assert_eq!(initial_header_audit_count, 0);

    let mut observed_lineage_anchor = lineage_anchor.clone();
    observed_lineage_anchor.canonicality_state = CanonicalityState::Observed;
    let mut observed_raw_anchor = raw_anchor.clone();
    observed_raw_anchor.canonicality_state = CanonicalityState::Observed;
    upsert_chain_lineage_blocks_without_snapshots(database.pool(), &[observed_lineage_anchor])
        .await?;
    upsert_raw_blocks_without_snapshots(database.pool(), &[observed_raw_anchor]).await?;

    let stored_lineage = load_chain_lineage_block(database.pool(), "eth-mainnet", "0xempty")
        .await?
        .expect("empty-block lineage anchor must be retained");
    let stored_raw = load_raw_block(database.pool(), "eth-mainnet", "0xempty")
        .await?
        .expect("empty-block raw anchor must be retained");
    assert_eq!(
        stored_lineage.canonicality_state,
        CanonicalityState::Finalized
    );
    assert_eq!(stored_raw.canonicality_state, CanonicalityState::Finalized);
    assert_eq!(stored_raw.logs_bloom, None);
    assert_eq!(stored_raw.transactions_root, None);
    assert_eq!(stored_raw.receipts_root, None);
    assert_eq!(stored_raw.state_root, None);
    let replayed_lineage_observed_at = sqlx::query_scalar::<_, OffsetDateTime>(
        "SELECT observed_at FROM chain_lineage WHERE chain_id = $1 AND block_hash = $2",
    )
    .bind("eth-mainnet")
    .bind("0xempty")
    .fetch_one(database.pool())
    .await?;
    assert_eq!(replayed_lineage_observed_at, initial_lineage_observed_at);

    let mut audited_lineage_anchor = lineage_anchor.clone();
    audited_lineage_anchor.logs_bloom = Some(vec![0xaa]);
    audited_lineage_anchor.transactions_root = Some("0xtxroot-empty".to_owned());
    audited_lineage_anchor.receipts_root = Some("0xreceipts-empty".to_owned());
    audited_lineage_anchor.state_root = Some("0xstate-empty".to_owned());
    let mut audited_raw_anchor = raw_anchor.clone();
    audited_raw_anchor.logs_bloom = audited_lineage_anchor.logs_bloom.clone();
    audited_raw_anchor.transactions_root = audited_lineage_anchor.transactions_root.clone();
    audited_raw_anchor.receipts_root = audited_lineage_anchor.receipts_root.clone();
    audited_raw_anchor.state_root = audited_lineage_anchor.state_root.clone();
    upsert_chain_lineage_blocks_without_snapshots(
        database.pool(),
        &[audited_lineage_anchor.clone()],
    )
    .await?;
    upsert_raw_blocks_without_snapshots(database.pool(), &[audited_raw_anchor.clone()]).await?;

    let stored_audited_lineage =
        load_chain_lineage_block(database.pool(), "eth-mainnet", "0xempty")
            .await?
            .expect("audited lineage anchor must be retained");
    let stored_audited_raw = load_raw_block(database.pool(), "eth-mainnet", "0xempty")
        .await?
        .expect("audited raw anchor must be retained");
    assert_eq!(stored_audited_lineage.logs_bloom, Some(vec![0xaa]));
    assert_eq!(
        stored_audited_lineage.transactions_root.as_deref(),
        Some("0xtxroot-empty")
    );
    assert_eq!(
        stored_audited_lineage.receipts_root.as_deref(),
        Some("0xreceipts-empty")
    );
    assert_eq!(
        stored_audited_lineage.state_root.as_deref(),
        Some("0xstate-empty")
    );
    assert_eq!(stored_audited_raw.logs_bloom, Some(vec![0xaa]));
    assert_eq!(
        stored_audited_raw.transactions_root.as_deref(),
        Some("0xtxroot-empty")
    );
    assert_eq!(
        stored_audited_raw.receipts_root.as_deref(),
        Some("0xreceipts-empty")
    );
    assert_eq!(
        stored_audited_raw.state_root.as_deref(),
        Some("0xstate-empty")
    );
    let audited_header_audit_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT FROM chain_header_audit WHERE chain_id = $1 AND block_hash = $2",
    )
    .bind("eth-mainnet")
    .bind("0xempty")
    .fetch_one(database.pool())
    .await?;
    assert_eq!(audited_header_audit_count, 1);

    upsert_chain_lineage_blocks_without_snapshots(
        database.pool(),
        std::slice::from_ref(&lineage_anchor),
    )
    .await?;
    upsert_raw_blocks_without_snapshots(database.pool(), std::slice::from_ref(&raw_anchor)).await?;
    let minimal_replayed_lineage =
        load_chain_lineage_block(database.pool(), "eth-mainnet", "0xempty")
            .await?
            .expect("minimal replay must not clear audited lineage fields");
    let minimal_replayed_raw = load_raw_block(database.pool(), "eth-mainnet", "0xempty")
        .await?
        .expect("minimal replay must not clear audited raw fields");
    assert_eq!(minimal_replayed_lineage.logs_bloom, Some(vec![0xaa]));
    assert_eq!(minimal_replayed_raw.logs_bloom, Some(vec![0xaa]));
    assert_eq!(
        minimal_replayed_raw.state_root.as_deref(),
        Some("0xstate-empty")
    );

    let mut conflicting_raw_anchor = raw_anchor.clone();
    conflicting_raw_anchor.state_root = Some("0xchanged".to_owned());
    let error = upsert_raw_blocks_without_snapshots(database.pool(), &[conflicting_raw_anchor])
        .await
        .expect_err("sparse raw block anchor identity must be immutable");
    assert!(
        error.to_string().contains("header audit identity mismatch"),
        "unexpected error: {error:#}"
    );

    for table in ["raw_transactions", "raw_receipts", "raw_logs"] {
        let count = sqlx::query_scalar::<_, i64>(&format!(
            "SELECT COUNT(*)::BIGINT FROM {table} WHERE chain_id = $1 AND block_hash = $2"
        ))
        .bind("eth-mainnet")
        .bind("0xempty")
        .fetch_one(database.pool())
        .await?;
        assert_eq!(count, 0, "empty-block anchor must not create {table} rows");
    }

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
async fn minimal_raw_block_replay_can_be_audit_enriched_without_clearing_fields() -> Result<()> {
    let database = TestDatabase::new().await?;

    let mut minimal = raw_block(CanonicalityState::Observed);
    minimal.logs_bloom = None;
    minimal.transactions_root = None;
    minimal.receipts_root = None;
    minimal.state_root = None;
    upsert_raw_blocks(database.pool(), &[minimal.clone()]).await?;

    let audited = raw_block(CanonicalityState::Canonical);
    let refreshed = upsert_raw_blocks(database.pool(), std::slice::from_ref(&audited)).await?;
    assert_eq!(
        refreshed[0].canonicality_state,
        CanonicalityState::Canonical
    );
    assert_eq!(refreshed[0].logs_bloom, audited.logs_bloom);
    assert_eq!(refreshed[0].transactions_root, audited.transactions_root);
    assert_eq!(refreshed[0].receipts_root, audited.receipts_root);
    assert_eq!(refreshed[0].state_root, audited.state_root);

    let minimal_replay = upsert_raw_blocks(database.pool(), &[minimal]).await?;
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
        error.to_string().contains("header audit identity mismatch"),
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
