use anyhow::{Context, Result, bail};
use sqlx::types::time::OffsetDateTime;
use sqlx::{Executor, PgPool, Postgres, Row, postgres::PgRow};

/// Persisted lineage snapshot for one chain block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainLineageBlock {
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

/// Persisted canonicality marker for a lineage row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CanonicalityState {
    Observed,
    Canonical,
    Safe,
    Finalized,
    Orphaned,
}

impl CanonicalityState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Observed => "observed",
            Self::Canonical => "canonical",
            Self::Safe => "safe",
            Self::Finalized => "finalized",
            Self::Orphaned => "orphaned",
        }
    }

    pub(crate) fn promote_to(self, target: Self) -> Self {
        match target {
            Self::Observed => {
                if self == Self::Orphaned {
                    Self::Observed
                } else {
                    self
                }
            }
            Self::Canonical | Self::Safe | Self::Finalized => {
                if self == Self::Orphaned {
                    return target;
                }

                if self.rank() >= target.rank() {
                    self
                } else {
                    target
                }
            }
            Self::Orphaned => Self::Orphaned,
        }
    }

    fn merge_upsert(self, incoming: Self) -> Self {
        match incoming {
            Self::Orphaned => Self::Orphaned,
            Self::Observed => {
                if self == Self::Orphaned {
                    Self::Observed
                } else {
                    self
                }
            }
            Self::Canonical | Self::Safe | Self::Finalized => self.promote_to(incoming),
        }
    }

    fn rank(self) -> u8 {
        match self {
            Self::Observed => 0,
            Self::Canonical => 1,
            Self::Safe => 2,
            Self::Finalized => 3,
            Self::Orphaned => 4,
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self> {
        match value {
            "observed" => Ok(Self::Observed),
            "canonical" => Ok(Self::Canonical),
            "safe" => Ok(Self::Safe),
            "finalized" => Ok(Self::Finalized),
            "orphaned" => Ok(Self::Orphaned),
            _ => bail!("unknown canonicality_state value {value}"),
        }
    }
}

/// Load one lineage snapshot by hash-first identity.
pub async fn load_chain_lineage_block(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<Option<ChainLineageBlock>> {
    load_chain_lineage_block_internal(pool, chain_id, block_hash).await
}

/// Insert missing lineage rows or refresh existing rows when the same block hash
/// is observed again. Immutable block metadata must match the stored row.
pub async fn upsert_chain_lineage_blocks(
    pool: &PgPool,
    blocks: &[ChainLineageBlock],
) -> Result<Vec<ChainLineageBlock>> {
    if blocks.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for chain lineage upsert")?;

    let mut snapshots = Vec::with_capacity(blocks.len());
    for block in blocks {
        validate_lineage_block(block)?;
        snapshots.push(upsert_chain_lineage_block(&mut transaction, block).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit chain lineage upsert")?;

    Ok(snapshots)
}

/// Walk a stored branch from `from_hash` through parent links and mark each row
/// `orphaned` until `stop_before_hash` is reached.
pub async fn mark_chain_lineage_range_orphaned(
    pool: &PgPool,
    chain_id: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<Vec<ChainLineageBlock>> {
    if stop_before_hash == Some(from_hash) {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for lineage orphaning")?;

    let path = load_chain_lineage_path(&mut *transaction, chain_id, from_hash, stop_before_hash)
        .await
        .with_context(|| {
            format!(
                "failed to load lineage path for chain {chain_id} starting from block {from_hash}"
            )
        })?;
    if path.is_empty() {
        bail!("missing stored lineage row for chain {chain_id} block {from_hash}");
    }

    let block_hashes = path
        .iter()
        .map(|block| block.block_hash.clone())
        .collect::<Vec<_>>();
    sqlx::query(
        r#"
        UPDATE chain_lineage
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
        format!("failed to mark orphaned lineage range for chain {chain_id} from block {from_hash}")
    })?;

    let snapshots = load_lineage_snapshots_for_hashes(&mut *transaction, chain_id, &block_hashes)
        .await
        .with_context(|| {
            format!(
                "failed to load orphaned lineage range for chain {chain_id} starting from block {from_hash}"
            )
        })?;

    transaction
        .commit()
        .await
        .context("failed to commit lineage orphaning")?;

    Ok(snapshots)
}

pub(crate) async fn ensure_chain_lineage_block(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
) -> Result<ChainLineageBlock> {
    let block = load_chain_lineage_block_internal(&mut **executor, chain_id, block_hash)
        .await?
        .with_context(|| {
            format!("missing stored lineage row for chain {chain_id} block {block_hash}")
        })?;

    if block.block_number != block_number {
        bail!(
            "stored lineage row for chain {chain_id} block {block_hash} has block number {}, expected {block_number}",
            block.block_number
        );
    }

    Ok(block)
}

pub(crate) async fn promote_chain_lineage_path(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    chain_id: &str,
    from_hash: &str,
    target_state: CanonicalityState,
) -> Result<Vec<ChainLineageBlock>> {
    let path = load_chain_lineage_path(&mut **executor, chain_id, from_hash, None)
        .await
        .with_context(|| {
            format!(
                "failed to load lineage path for chain {chain_id} starting from block {from_hash}"
            )
        })?;
    if path.is_empty() {
        bail!("missing stored lineage row for chain {chain_id} block {from_hash}");
    }

    let block_hashes = path
        .iter()
        .map(|block| block.block_hash.clone())
        .collect::<Vec<_>>();

    sqlx::query(
        r#"
        UPDATE chain_lineage
        SET canonicality_state = CASE
            WHEN canonicality_state = 'orphaned'::canonicality_state THEN $3::canonicality_state
            WHEN $3::canonicality_state = 'canonical'::canonicality_state
                AND canonicality_state IN ('safe'::canonicality_state, 'finalized'::canonicality_state)
                THEN canonicality_state
            WHEN $3::canonicality_state = 'safe'::canonicality_state
                AND canonicality_state = 'finalized'::canonicality_state
                THEN canonicality_state
            WHEN $3::canonicality_state = 'observed'::canonicality_state
                THEN canonicality_state
            ELSE $3::canonicality_state
        END
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
        "#,
    )
    .bind(chain_id)
    .bind(&block_hashes)
    .bind(target_state.as_str())
    .execute(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to promote lineage path for chain {chain_id} starting from block {from_hash}"
        )
    })?;

    load_lineage_snapshots_for_hashes(&mut **executor, chain_id, &block_hashes)
        .await
        .with_context(|| {
            format!(
                "failed to reload promoted lineage path for chain {chain_id} starting from block {from_hash}"
            )
        })
}

async fn upsert_chain_lineage_block(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    block: &ChainLineageBlock,
) -> Result<ChainLineageBlock> {
    if let Some(snapshot) = sqlx::query(
        r#"
        INSERT INTO chain_lineage (
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
            "failed to insert lineage row for chain {} block {}",
            block.chain_id, block.block_hash
        )
    })? {
        return decode_lineage_block(snapshot);
    }

    let existing = load_chain_lineage_block_internal(
        &mut **executor,
        &block.chain_id,
        &block.block_hash,
    )
    .await?
    .with_context(|| {
        format!(
            "failed to reload existing lineage row for chain {} block {} after insert conflict",
            block.chain_id, block.block_hash
        )
    })?;

    ensure_lineage_identity_matches(&existing, block)?;
    let next_state = existing
        .canonicality_state
        .merge_upsert(block.canonicality_state);

    let snapshot = sqlx::query(
        r#"
        UPDATE chain_lineage
        SET
            canonicality_state = $3::canonicality_state,
            observed_at = now()
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
            "failed to refresh existing lineage row for chain {} block {}",
            block.chain_id, block.block_hash
        )
    })?;

    decode_lineage_block(snapshot)
}

async fn load_chain_lineage_block_internal<'e, E>(
    executor: E,
    chain_id: &str,
    block_hash: &str,
) -> Result<Option<ChainLineageBlock>>
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
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_hash = $2
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .fetch_optional(executor)
    .await
    .with_context(|| {
        format!("failed to load lineage row for chain {chain_id} block {block_hash}")
    })?;

    row.map(decode_lineage_block).transpose()
}

async fn load_chain_lineage_path<'e, E>(
    executor: E,
    chain_id: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<Vec<ChainLineageBlock>>
where
    E: Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(
        r#"
        WITH RECURSIVE lineage_path AS (
            SELECT chain_id, block_hash, parent_hash, 0 AS depth
            FROM chain_lineage
            WHERE chain_id = $1
              AND block_hash = $2

            UNION ALL

            SELECT parent.chain_id, parent.block_hash, parent.parent_hash, lineage_path.depth + 1
            FROM chain_lineage AS parent
            JOIN lineage_path
              ON parent.chain_id = lineage_path.chain_id
             AND parent.block_hash = lineage_path.parent_hash
            WHERE $3::TEXT IS NULL
               OR parent.block_hash <> $3::TEXT
        )
        SELECT
            lineage.chain_id,
            lineage.block_hash,
            lineage.parent_hash,
            lineage.block_number,
            lineage.block_timestamp,
            lineage.logs_bloom,
            lineage.transactions_root,
            lineage.receipts_root,
            lineage.state_root,
            lineage.canonicality_state::TEXT AS canonicality_state
        FROM lineage_path
        JOIN chain_lineage AS lineage
          ON lineage.chain_id = lineage_path.chain_id
         AND lineage.block_hash = lineage_path.block_hash
        ORDER BY lineage_path.depth
        "#,
    )
    .bind(chain_id)
    .bind(from_hash)
    .bind(stop_before_hash)
    .fetch_all(executor)
    .await?;

    rows.into_iter().map(decode_lineage_block).collect()
}

async fn load_lineage_snapshots_for_hashes<'e, E>(
    executor: E,
    chain_id: &str,
    block_hashes: &[String],
) -> Result<Vec<ChainLineageBlock>>
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
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
        "#,
    )
    .bind(chain_id)
    .bind(block_hashes)
    .fetch_all(executor)
    .await
    .with_context(|| {
        format!(
            "failed to load lineage snapshots for chain {chain_id} across {} hashes",
            block_hashes.len()
        )
    })?;

    let snapshots = rows
        .into_iter()
        .map(decode_lineage_block)
        .collect::<Result<Vec<_>>>()?;
    let snapshots_by_hash = snapshots
        .into_iter()
        .map(|snapshot| (snapshot.block_hash.clone(), snapshot))
        .collect::<std::collections::BTreeMap<_, _>>();

    let mut ordered = Vec::with_capacity(block_hashes.len());
    for block_hash in block_hashes {
        let snapshot = snapshots_by_hash
            .get(block_hash)
            .cloned()
            .with_context(|| {
                format!("failed to reload lineage snapshot for chain {chain_id} block {block_hash}")
            })?;
        ordered.push(snapshot);
    }

    Ok(ordered)
}

fn decode_lineage_block(row: PgRow) -> Result<ChainLineageBlock> {
    let canonicality_state = CanonicalityState::parse(
        &row.try_get::<String, _>("canonicality_state")
            .context("failed to decode lineage canonicality_state")?,
    )?;

    Ok(ChainLineageBlock {
        chain_id: row
            .try_get::<String, _>("chain_id")
            .context("failed to decode lineage chain_id")?,
        block_hash: row
            .try_get::<String, _>("block_hash")
            .context("failed to decode lineage block_hash")?,
        parent_hash: row
            .try_get::<Option<String>, _>("parent_hash")
            .context("failed to decode lineage parent_hash")?,
        block_number: row
            .try_get::<i64, _>("block_number")
            .context("failed to decode lineage block_number")?,
        block_timestamp: row
            .try_get::<OffsetDateTime, _>("block_timestamp")
            .context("failed to decode lineage block_timestamp")?,
        logs_bloom: row
            .try_get::<Option<Vec<u8>>, _>("logs_bloom")
            .context("failed to decode lineage logs_bloom")?,
        transactions_root: row
            .try_get::<Option<String>, _>("transactions_root")
            .context("failed to decode lineage transactions_root")?,
        receipts_root: row
            .try_get::<Option<String>, _>("receipts_root")
            .context("failed to decode lineage receipts_root")?,
        state_root: row
            .try_get::<Option<String>, _>("state_root")
            .context("failed to decode lineage state_root")?,
        canonicality_state,
    })
}

fn ensure_lineage_identity_matches(
    existing: &ChainLineageBlock,
    candidate: &ChainLineageBlock,
) -> Result<()> {
    if existing.chain_id != candidate.chain_id
        || existing.block_hash != candidate.block_hash
        || existing.parent_hash != candidate.parent_hash
        || existing.block_number != candidate.block_number
        || existing.block_timestamp != candidate.block_timestamp
        || existing.logs_bloom != candidate.logs_bloom
        || existing.transactions_root != candidate.transactions_root
        || existing.receipts_root != candidate.receipts_root
        || existing.state_root != candidate.state_root
    {
        bail!(
            "stored lineage row for chain {} block {} does not match the supplied immutable block metadata",
            candidate.chain_id,
            candidate.block_hash
        );
    }

    Ok(())
}

fn validate_lineage_block(block: &ChainLineageBlock) -> Result<()> {
    if block.block_number < 0 {
        bail!(
            "lineage block {} for chain {} has negative block number {}",
            block.block_hash,
            block.chain_id,
            block.block_number
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::Result;
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
    async fn reobserving_orphaned_block_revives_observed_state_without_rewriting_identity()
    -> Result<()> {
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

        let orphaned = mark_chain_lineage_range_orphaned(
            database.pool(),
            "eth-mainnet",
            "0x003",
            Some("0x001"),
        )
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
}
