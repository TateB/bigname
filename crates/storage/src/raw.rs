use anyhow::{Context, Result, bail};
use sqlx::types::time::OffsetDateTime;
use sqlx::{Executor, PgPool, Postgres, Row, postgres::PgRow};

use crate::CanonicalityState;

/// Persisted exact block fact from a hash-scoped provider fetch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawBlock {
    pub chain_id: String,
    pub block_hash: String,
    pub parent_hash: Option<String>,
    pub block_number: i64,
    pub block_timestamp: OffsetDateTime,
    pub logs_bloom: Option<Vec<u8>>,
    pub transactions_root: Option<String>,
    pub receipts_root: Option<String>,
    pub state_root: Option<String>,
    pub canonicality_state: CanonicalityState,
}

/// Load one raw block fact by hash-first identity.
pub async fn load_raw_block(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<Option<RawBlock>> {
    load_raw_block_internal(pool, chain_id, block_hash).await
}

/// Load a stored set of raw block facts by hash-first identity.
pub async fn load_raw_blocks_by_hashes(
    pool: &PgPool,
    chain_id: &str,
    block_hashes: &[String],
) -> Result<Vec<RawBlock>> {
    if block_hashes.is_empty() {
        return Ok(Vec::new());
    }

    load_raw_block_snapshots_for_hashes(pool, chain_id, block_hashes).await
}

/// Insert missing raw block facts or refresh canonicality when the same block is
/// fetched again. Immutable block metadata must match the stored row.
pub async fn upsert_raw_blocks(pool: &PgPool, blocks: &[RawBlock]) -> Result<Vec<RawBlock>> {
    if blocks.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw block upsert")?;

    let mut snapshots = Vec::with_capacity(blocks.len());
    for block in blocks {
        validate_raw_block(block)?;
        snapshots.push(upsert_raw_block(&mut transaction, block).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw block upsert")?;

    Ok(snapshots)
}

/// Walk a stored raw block branch from `from_hash` through parent links and
/// mark each row `orphaned` until `stop_before_hash` is reached.
pub async fn mark_raw_block_range_orphaned(
    pool: &PgPool,
    chain_id: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<Vec<RawBlock>> {
    if stop_before_hash == Some(from_hash) {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw block orphaning")?;

    let path = load_raw_block_path(&mut *transaction, chain_id, from_hash, stop_before_hash)
        .await
        .with_context(|| {
            format!(
                "failed to load raw block path for chain {chain_id} starting from block {from_hash}"
            )
        })?;
    if path.is_empty() {
        bail!("missing stored raw block for chain {chain_id} block {from_hash}");
    }

    let block_hashes = path
        .iter()
        .map(|block| block.block_hash.clone())
        .collect::<Vec<_>>();
    sqlx::query(
        r#"
        UPDATE raw_blocks
        SET canonicality_state = 'orphaned'::canonicality_state
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
        "#,
    )
    .bind(chain_id)
    .bind(&block_hashes)
    .execute(&mut *transaction)
    .await
    .with_context(|| {
        format!(
            "failed to mark orphaned raw block range for chain {chain_id} from block {from_hash}"
        )
    })?;

    let snapshots = load_raw_block_snapshots_for_hashes(&mut *transaction, chain_id, &block_hashes)
        .await
        .with_context(|| {
            format!(
                "failed to load orphaned raw block range for chain {chain_id} starting from block {from_hash}"
            )
        })?;

    transaction
        .commit()
        .await
        .context("failed to commit raw block orphaning")?;

    Ok(snapshots)
}

async fn upsert_raw_block(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    block: &RawBlock,
) -> Result<RawBlock> {
    if let Some(snapshot) = sqlx::query(
        r#"
        INSERT INTO raw_blocks (
            chain_id,
            block_hash,
            parent_hash,
            block_number,
            block_timestamp,
            logs_bloom,
            transactions_root,
            receipts_root,
            state_root,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::canonicality_state)
        ON CONFLICT (chain_id, block_hash) DO NOTHING
        RETURNING
            chain_id,
            block_hash,
            parent_hash,
            block_number,
            block_timestamp,
            logs_bloom,
            transactions_root,
            receipts_root,
            state_root,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&block.chain_id)
    .bind(&block.block_hash)
    .bind(&block.parent_hash)
    .bind(block.block_number)
    .bind(block.block_timestamp)
    .bind(&block.logs_bloom)
    .bind(&block.transactions_root)
    .bind(&block.receipts_root)
    .bind(&block.state_root)
    .bind(block.canonicality_state.as_str())
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert raw block for chain {} block {}",
            block.chain_id, block.block_hash
        )
    })? {
        return decode_raw_block(snapshot);
    }

    let existing = load_raw_block_internal(&mut **executor, &block.chain_id, &block.block_hash)
        .await?
        .with_context(|| {
            format!(
                "failed to reload existing raw block for chain {} block {} after insert conflict",
                block.chain_id, block.block_hash
            )
        })?;

    ensure_raw_identity_matches(&existing, block)?;
    let next_state = merge_canonicality(existing.canonicality_state, block.canonicality_state);

    let snapshot = sqlx::query(
        r#"
        UPDATE raw_blocks
        SET
            canonicality_state = $3::canonicality_state,
            observed_at = now(),
            fetched_at = now()
        WHERE chain_id = $1
          AND block_hash = $2
        RETURNING
            chain_id,
            block_hash,
            parent_hash,
            block_number,
            block_timestamp,
            logs_bloom,
            transactions_root,
            receipts_root,
            state_root,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&block.chain_id)
    .bind(&block.block_hash)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh existing raw block for chain {} block {}",
            block.chain_id, block.block_hash
        )
    })?;

    decode_raw_block(snapshot)
}

async fn load_raw_block_internal<'e, E>(
    executor: E,
    chain_id: &str,
    block_hash: &str,
) -> Result<Option<RawBlock>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            parent_hash,
            block_number,
            block_timestamp,
            logs_bloom,
            transactions_root,
            receipts_root,
            state_root,
            canonicality_state::TEXT AS canonicality_state
        FROM raw_blocks
        WHERE chain_id = $1
          AND block_hash = $2
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .fetch_optional(executor)
    .await
    .with_context(|| format!("failed to load raw block for chain {chain_id} block {block_hash}"))?;

    row.map(decode_raw_block).transpose()
}

async fn load_raw_block_path<'e, E>(
    executor: E,
    chain_id: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<Vec<RawBlock>>
where
    E: Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(
        r#"
        WITH RECURSIVE raw_block_path AS (
            SELECT chain_id, block_hash, parent_hash, 0 AS depth
            FROM raw_blocks
            WHERE chain_id = $1
              AND block_hash = $2

            UNION ALL

            SELECT parent.chain_id, parent.block_hash, parent.parent_hash, raw_block_path.depth + 1
            FROM raw_blocks AS parent
            JOIN raw_block_path
              ON parent.chain_id = raw_block_path.chain_id
             AND parent.block_hash = raw_block_path.parent_hash
            WHERE $3::TEXT IS NULL
               OR parent.block_hash <> $3::TEXT
        )
        SELECT
            raw.chain_id,
            raw.block_hash,
            raw.parent_hash,
            raw.block_number,
            raw.block_timestamp,
            raw.logs_bloom,
            raw.transactions_root,
            raw.receipts_root,
            raw.state_root,
            raw.canonicality_state::TEXT AS canonicality_state
        FROM raw_block_path
        JOIN raw_blocks AS raw
          ON raw.chain_id = raw_block_path.chain_id
         AND raw.block_hash = raw_block_path.block_hash
        ORDER BY raw_block_path.depth
        "#,
    )
    .bind(chain_id)
    .bind(from_hash)
    .bind(stop_before_hash)
    .fetch_all(executor)
    .await?;

    rows.into_iter().map(decode_raw_block).collect()
}

async fn load_raw_block_snapshots_for_hashes<'e, E>(
    executor: E,
    chain_id: &str,
    block_hashes: &[String],
) -> Result<Vec<RawBlock>>
where
    E: Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            parent_hash,
            block_number,
            block_timestamp,
            logs_bloom,
            transactions_root,
            receipts_root,
            state_root,
            canonicality_state::TEXT AS canonicality_state
        FROM raw_blocks
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
        ORDER BY block_number, block_hash
        "#,
    )
    .bind(chain_id)
    .bind(block_hashes)
    .fetch_all(executor)
    .await
    .with_context(|| {
        format!(
            "failed to load raw block snapshots for chain {chain_id} across {} hashes",
            block_hashes.len()
        )
    })?;

    rows.into_iter().map(decode_raw_block).collect()
}

fn validate_raw_block(block: &RawBlock) -> Result<()> {
    if block.block_number < 0 {
        bail!(
            "raw block for chain {} hash {} has negative block number {}",
            block.chain_id,
            block.block_hash,
            block.block_number
        );
    }

    Ok(())
}

fn ensure_raw_identity_matches(existing: &RawBlock, incoming: &RawBlock) -> Result<()> {
    if existing.parent_hash != incoming.parent_hash
        || existing.block_number != incoming.block_number
        || existing.block_timestamp != incoming.block_timestamp
        || existing.logs_bloom != incoming.logs_bloom
        || existing.transactions_root != incoming.transactions_root
        || existing.receipts_root != incoming.receipts_root
        || existing.state_root != incoming.state_root
    {
        bail!(
            "raw block identity mismatch for chain {} block {}",
            existing.chain_id,
            existing.block_hash
        );
    }

    Ok(())
}

fn merge_canonicality(
    current: CanonicalityState,
    incoming: CanonicalityState,
) -> CanonicalityState {
    match incoming {
        CanonicalityState::Orphaned => CanonicalityState::Orphaned,
        CanonicalityState::Observed => {
            if current == CanonicalityState::Orphaned {
                CanonicalityState::Observed
            } else {
                current
            }
        }
        CanonicalityState::Canonical | CanonicalityState::Safe | CanonicalityState::Finalized => {
            if current == CanonicalityState::Orphaned {
                incoming
            } else {
                current.promote_to(incoming)
            }
        }
    }
}

fn decode_raw_block(row: PgRow) -> Result<RawBlock> {
    Ok(RawBlock {
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        parent_hash: row.try_get("parent_hash").context("missing parent_hash")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number")?,
        block_timestamp: row
            .try_get("block_timestamp")
            .context("missing block_timestamp")?,
        logs_bloom: row.try_get("logs_bloom").context("missing logs_bloom")?,
        transactions_root: row
            .try_get("transactions_root")
            .context("missing transactions_root")?,
        receipts_root: row
            .try_get("receipts_root")
            .context("missing receipts_root")?,
        state_root: row.try_get("state_root").context("missing state_root")?,
        canonicality_state: CanonicalityState::parse(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing canonicality_state")?,
        )?,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::Result;
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
}
