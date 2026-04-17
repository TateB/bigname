use anyhow::{Context, Result, bail};
use sqlx::PgPool;
use sqlx::{Executor, Postgres, Row, postgres::PgRow};

use crate::lineage::{CanonicalityState, ensure_chain_lineage_block, promote_chain_lineage_path};

/// Persisted checkpoint row for one watched chain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainCheckpoint {
    pub chain_id: String,
    pub canonical_block_hash: Option<String>,
    pub canonical_block_number: Option<i64>,
    pub safe_block_hash: Option<String>,
    pub safe_block_number: Option<i64>,
    pub finalized_block_hash: Option<String>,
    pub finalized_block_number: Option<i64>,
}

/// Hash-first reference for one persisted checkpoint target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointBlockRef {
    pub block_hash: String,
    pub block_number: i64,
}

/// Monotonic checkpoint advancement request for one chain.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChainCheckpointUpdate {
    pub chain_id: String,
    pub canonical: Option<CheckpointBlockRef>,
    pub safe: Option<CheckpointBlockRef>,
    pub finalized: Option<CheckpointBlockRef>,
}

impl ChainCheckpointUpdate {
    fn is_no_op(&self) -> bool {
        self.canonical.is_none() && self.safe.is_none() && self.finalized.is_none()
    }
}

/// Insert missing `chain_checkpoints` rows and return sorted snapshots for the
/// requested chain IDs. Existing rows remain untouched, and omitted chain IDs
/// are not deleted when the watched set shrinks.
pub async fn sync_chain_checkpoints(
    pool: &PgPool,
    chain_ids: &[String],
) -> Result<Vec<ChainCheckpoint>> {
    let chain_ids = collect_chain_ids(chain_ids);
    if chain_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for chain checkpoint sync")?;
    ensure_chain_checkpoint_rows(&mut *transaction, &chain_ids).await?;
    let checkpoints = load_chain_checkpoints(&mut *transaction, &chain_ids).await?;
    transaction
        .commit()
        .await
        .context("failed to commit chain checkpoint sync")?;

    Ok(checkpoints)
}

/// Advance persisted checkpoint pointers for one chain and promote stored
/// lineage rows along the admitted ancestry path.
pub async fn advance_chain_checkpoints(
    pool: &PgPool,
    update: &ChainCheckpointUpdate,
) -> Result<ChainCheckpoint> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for checkpoint advancement")?;

    ensure_chain_checkpoint_rows(&mut *transaction, std::slice::from_ref(&update.chain_id)).await?;
    let current = load_chain_checkpoint_for_update(&mut transaction, &update.chain_id).await?;
    validate_checkpoint_update(&current, update)?;

    if let Some(canonical) = &update.canonical {
        ensure_chain_lineage_block(
            &mut transaction,
            &update.chain_id,
            &canonical.block_hash,
            canonical.block_number,
        )
        .await?;
    }
    if let Some(safe) = &update.safe {
        ensure_chain_lineage_block(
            &mut transaction,
            &update.chain_id,
            &safe.block_hash,
            safe.block_number,
        )
        .await?;
    }
    if let Some(finalized) = &update.finalized {
        ensure_chain_lineage_block(
            &mut transaction,
            &update.chain_id,
            &finalized.block_hash,
            finalized.block_number,
        )
        .await?;
    }

    if let Some(canonical) = &update.canonical {
        promote_chain_lineage_path(
            &mut transaction,
            &update.chain_id,
            &canonical.block_hash,
            CanonicalityState::Canonical,
        )
        .await?;
    }
    if let Some(safe) = &update.safe {
        promote_chain_lineage_path(
            &mut transaction,
            &update.chain_id,
            &safe.block_hash,
            CanonicalityState::Safe,
        )
        .await?;
    }
    if let Some(finalized) = &update.finalized {
        promote_chain_lineage_path(
            &mut transaction,
            &update.chain_id,
            &finalized.block_hash,
            CanonicalityState::Finalized,
        )
        .await?;
    }

    let checkpoint = if update.is_no_op() {
        current
    } else {
        write_chain_checkpoint_update(&mut transaction, update).await?
    };

    transaction
        .commit()
        .await
        .context("failed to commit checkpoint advancement")?;

    Ok(checkpoint)
}

async fn ensure_chain_checkpoint_rows<'e, E>(executor: E, chain_ids: &[String]) -> Result<u64>
where
    E: Executor<'e, Database = Postgres>,
{
    let result = sqlx::query(
        r#"
        INSERT INTO chain_checkpoints (chain_id)
        SELECT DISTINCT candidate.chain_id
        FROM UNNEST($1::TEXT[]) AS candidate(chain_id)
        ON CONFLICT (chain_id) DO NOTHING
        "#,
    )
    .bind(chain_ids)
    .execute(executor)
    .await
    .context("failed to ensure chain checkpoint rows")?;

    Ok(result.rows_affected())
}

async fn load_chain_checkpoints<'e, E>(
    executor: E,
    chain_ids: &[String],
) -> Result<Vec<ChainCheckpoint>>
where
    E: Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(
        r#"
        SELECT
            chain_id,
            canonical_block_hash,
            canonical_block_number,
            safe_block_hash,
            safe_block_number,
            finalized_block_hash,
            finalized_block_number
        FROM chain_checkpoints
        WHERE chain_id = ANY($1::TEXT[])
        ORDER BY chain_id
        "#,
    )
    .bind(chain_ids)
    .fetch_all(executor)
    .await
    .context("failed to load chain checkpoint snapshots")?;

    rows.into_iter().map(decode_snapshot).collect()
}

async fn load_chain_checkpoint_for_update(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<ChainCheckpoint> {
    let row = sqlx::query(
        r#"
        SELECT
            chain_id,
            canonical_block_hash,
            canonical_block_number,
            safe_block_hash,
            safe_block_number,
            finalized_block_hash,
            finalized_block_number
        FROM chain_checkpoints
        WHERE chain_id = $1
        FOR UPDATE
        "#,
    )
    .bind(chain_id)
    .fetch_one(&mut **executor)
    .await
    .with_context(|| format!("failed to load checkpoint row for chain {chain_id}"))?;

    decode_snapshot(row)
}

async fn write_chain_checkpoint_update(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    update: &ChainCheckpointUpdate,
) -> Result<ChainCheckpoint> {
    let row = sqlx::query(
        r#"
        UPDATE chain_checkpoints
        SET
            canonical_block_hash = COALESCE($2, canonical_block_hash),
            canonical_block_number = COALESCE($3, canonical_block_number),
            safe_block_hash = COALESCE($4, safe_block_hash),
            safe_block_number = COALESCE($5, safe_block_number),
            finalized_block_hash = COALESCE($6, finalized_block_hash),
            finalized_block_number = COALESCE($7, finalized_block_number),
            updated_at = now()
        WHERE chain_id = $1
        RETURNING
            chain_id,
            canonical_block_hash,
            canonical_block_number,
            safe_block_hash,
            safe_block_number,
            finalized_block_hash,
            finalized_block_number
        "#,
    )
    .bind(&update.chain_id)
    .bind(
        update
            .canonical
            .as_ref()
            .map(|block| block.block_hash.as_str()),
    )
    .bind(update.canonical.as_ref().map(|block| block.block_number))
    .bind(update.safe.as_ref().map(|block| block.block_hash.as_str()))
    .bind(update.safe.as_ref().map(|block| block.block_number))
    .bind(
        update
            .finalized
            .as_ref()
            .map(|block| block.block_hash.as_str()),
    )
    .bind(update.finalized.as_ref().map(|block| block.block_number))
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to update checkpoint row for chain {}",
            update.chain_id
        )
    })?;

    decode_snapshot(row)
}

fn collect_chain_ids(chain_ids: &[String]) -> Vec<String> {
    let mut chain_ids = chain_ids.to_vec();
    chain_ids.sort();
    chain_ids.dedup();
    chain_ids
}

fn decode_snapshot(row: PgRow) -> Result<ChainCheckpoint> {
    let chain_id = row
        .try_get::<String, _>("chain_id")
        .context("failed to decode chain checkpoint chain_id")?;

    let canonical_block_hash = row
        .try_get::<Option<String>, _>("canonical_block_hash")
        .context("failed to decode canonical checkpoint hash")?;
    let canonical_block_number = row
        .try_get::<Option<i64>, _>("canonical_block_number")
        .context("failed to decode canonical checkpoint number")?;
    validate_checkpoint_pair(
        &chain_id,
        "canonical",
        canonical_block_hash.as_ref(),
        canonical_block_number,
    )?;

    let safe_block_hash = row
        .try_get::<Option<String>, _>("safe_block_hash")
        .context("failed to decode safe checkpoint hash")?;
    let safe_block_number = row
        .try_get::<Option<i64>, _>("safe_block_number")
        .context("failed to decode safe checkpoint number")?;
    validate_checkpoint_pair(
        &chain_id,
        "safe",
        safe_block_hash.as_ref(),
        safe_block_number,
    )?;

    let finalized_block_hash = row
        .try_get::<Option<String>, _>("finalized_block_hash")
        .context("failed to decode finalized checkpoint hash")?;
    let finalized_block_number = row
        .try_get::<Option<i64>, _>("finalized_block_number")
        .context("failed to decode finalized checkpoint number")?;
    validate_checkpoint_pair(
        &chain_id,
        "finalized",
        finalized_block_hash.as_ref(),
        finalized_block_number,
    )?;

    Ok(ChainCheckpoint {
        chain_id: chain_id.clone(),
        canonical_block_hash,
        canonical_block_number,
        safe_block_hash,
        safe_block_number,
        finalized_block_hash,
        finalized_block_number,
    })
}

fn validate_checkpoint_update(
    current: &ChainCheckpoint,
    update: &ChainCheckpointUpdate,
) -> Result<()> {
    if let Some(canonical) = &update.canonical {
        validate_checkpoint_target(&update.chain_id, "canonical", canonical)?;
    }
    if let Some(safe) = &update.safe {
        validate_checkpoint_target(&update.chain_id, "safe", safe)?;
        validate_monotonic_checkpoint_target(
            &update.chain_id,
            "safe",
            current.safe_block_hash.as_ref(),
            current.safe_block_number,
            safe,
        )?;
    }
    if let Some(finalized) = &update.finalized {
        validate_checkpoint_target(&update.chain_id, "finalized", finalized)?;
        validate_monotonic_checkpoint_target(
            &update.chain_id,
            "finalized",
            current.finalized_block_hash.as_ref(),
            current.finalized_block_number,
            finalized,
        )?;
    }

    Ok(())
}

fn validate_checkpoint_target(
    chain_id: &str,
    checkpoint_name: &str,
    target: &CheckpointBlockRef,
) -> Result<()> {
    if target.block_number < 0 {
        bail!(
            "{checkpoint_name} checkpoint for chain {chain_id} has negative block number {}",
            target.block_number
        );
    }

    Ok(())
}

fn validate_monotonic_checkpoint_target(
    chain_id: &str,
    checkpoint_name: &str,
    current_hash: Option<&String>,
    current_number: Option<i64>,
    next: &CheckpointBlockRef,
) -> Result<()> {
    if let Some(current_number) = current_number {
        if next.block_number < current_number {
            bail!(
                "{checkpoint_name} checkpoint for chain {chain_id} cannot move backward from block {current_number} to {}",
                next.block_number
            );
        }

        if next.block_number == current_number
            && current_hash.is_some()
            && current_hash != Some(&next.block_hash)
        {
            bail!(
                "{checkpoint_name} checkpoint for chain {chain_id} cannot switch hash at block number {} from {} to {}",
                current_number,
                current_hash.expect("hash must exist when current_number exists"),
                next.block_hash
            );
        }
    }

    Ok(())
}

fn validate_checkpoint_pair(
    chain_id: &str,
    checkpoint_name: &str,
    hash: Option<&String>,
    block_number: Option<i64>,
) -> Result<()> {
    match (hash, block_number) {
        (Some(_), Some(_)) | (None, None) => Ok(()),
        _ => bail!(
            "stored {checkpoint_name} checkpoint for chain {chain_id} has mismatched hash and number"
        ),
    }
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
        query_scalar,
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
                .context("failed to parse database URL for storage integration tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_storage_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for storage integration tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect storage integration test pool")?;

            crate::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for storage integration tests")?;

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

    fn lineage_block(
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
    async fn syncs_checkpoint_rows_and_loads_snapshots() -> Result<()> {
        let database = TestDatabase::new().await?;

        let watched_chain_ids = vec![
            "base-mainnet".to_owned(),
            "eth-mainnet".to_owned(),
            "eth-mainnet".to_owned(),
        ];
        sync_chain_checkpoints(database.pool(), &watched_chain_ids).await?;

        sqlx::query(
            r#"
            UPDATE chain_checkpoints
            SET
                canonical_block_hash = '0xcanon',
                canonical_block_number = 101,
                safe_block_hash = '0xsafe',
                safe_block_number = 100,
                finalized_block_hash = '0xfinal',
                finalized_block_number = 99
            WHERE chain_id = 'eth-mainnet'
            "#,
        )
        .execute(database.pool())
        .await?;

        let snapshots = sync_chain_checkpoints(database.pool(), &watched_chain_ids).await?;

        assert_eq!(
            snapshots,
            vec![
                ChainCheckpoint {
                    chain_id: "base-mainnet".to_owned(),
                    canonical_block_hash: None,
                    canonical_block_number: None,
                    safe_block_hash: None,
                    safe_block_number: None,
                    finalized_block_hash: None,
                    finalized_block_number: None,
                },
                ChainCheckpoint {
                    chain_id: "eth-mainnet".to_owned(),
                    canonical_block_hash: Some("0xcanon".to_owned()),
                    canonical_block_number: Some(101),
                    safe_block_hash: Some("0xsafe".to_owned()),
                    safe_block_number: Some(100),
                    finalized_block_hash: Some("0xfinal".to_owned()),
                    finalized_block_number: Some(99),
                },
            ]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn ensure_does_not_delete_history_when_watch_set_shrinks() -> Result<()> {
        let database = TestDatabase::new().await?;

        let initial_chain_ids = vec!["base-mainnet".to_owned(), "eth-mainnet".to_owned()];
        let shrunk_chain_ids = vec!["eth-mainnet".to_owned()];
        sync_chain_checkpoints(database.pool(), &initial_chain_ids).await?;
        let shrunk_snapshots = sync_chain_checkpoints(database.pool(), &shrunk_chain_ids).await?;

        let row_count: i64 = query_scalar("SELECT COUNT(*) FROM chain_checkpoints")
            .fetch_one(database.pool())
            .await?;
        let snapshots = sync_chain_checkpoints(database.pool(), &initial_chain_ids).await?;

        assert_eq!(row_count, 2);
        assert_eq!(
            shrunk_snapshots
                .into_iter()
                .map(|snapshot| snapshot.chain_id)
                .collect::<Vec<_>>(),
            vec!["eth-mainnet".to_owned()]
        );
        assert_eq!(
            snapshots
                .into_iter()
                .map(|snapshot| snapshot.chain_id)
                .collect::<Vec<_>>(),
            vec!["base-mainnet".to_owned(), "eth-mainnet".to_owned()]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn empty_chain_set_is_a_no_op() -> Result<()> {
        let database = TestDatabase::new().await?;

        let snapshots = sync_chain_checkpoints(database.pool(), &[]).await?;

        assert!(snapshots.is_empty());

        database.cleanup().await
    }

    #[tokio::test]
    async fn advances_checkpoints_and_promotes_lineage_states() -> Result<()> {
        let database = TestDatabase::new().await?;
        let base_timestamp = timestamp(1_717_171_717);

        upsert_chain_lineage_blocks(
            database.pool(),
            &[
                lineage_block(
                    "eth-mainnet",
                    "0x001",
                    None,
                    1,
                    base_timestamp,
                    CanonicalityState::Observed,
                ),
                lineage_block(
                    "eth-mainnet",
                    "0x002",
                    Some("0x001"),
                    2,
                    timestamp(1_717_171_729),
                    CanonicalityState::Observed,
                ),
                lineage_block(
                    "eth-mainnet",
                    "0x003",
                    Some("0x002"),
                    3,
                    timestamp(1_717_171_741),
                    CanonicalityState::Observed,
                ),
            ],
        )
        .await?;

        let snapshot = advance_chain_checkpoints(
            database.pool(),
            &ChainCheckpointUpdate {
                chain_id: "eth-mainnet".to_owned(),
                canonical: Some(CheckpointBlockRef {
                    block_hash: "0x003".to_owned(),
                    block_number: 3,
                }),
                safe: Some(CheckpointBlockRef {
                    block_hash: "0x002".to_owned(),
                    block_number: 2,
                }),
                finalized: Some(CheckpointBlockRef {
                    block_hash: "0x001".to_owned(),
                    block_number: 1,
                }),
            },
        )
        .await?;

        assert_eq!(
            snapshot,
            ChainCheckpoint {
                chain_id: "eth-mainnet".to_owned(),
                canonical_block_hash: Some("0x003".to_owned()),
                canonical_block_number: Some(3),
                safe_block_hash: Some("0x002".to_owned()),
                safe_block_number: Some(2),
                finalized_block_hash: Some("0x001".to_owned()),
                finalized_block_number: Some(1),
            }
        );

        let canonicality_by_hash = sqlx::query_as::<_, (String, String)>(
            r#"
            SELECT block_hash, canonicality_state::TEXT
            FROM chain_lineage
            WHERE chain_id = 'eth-mainnet'
            ORDER BY block_number
            "#,
        )
        .fetch_all(database.pool())
        .await?;

        assert_eq!(
            canonicality_by_hash,
            vec![
                ("0x001".to_owned(), "finalized".to_owned()),
                ("0x002".to_owned(), "safe".to_owned()),
                ("0x003".to_owned(), "canonical".to_owned()),
            ]
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn rejects_safe_checkpoint_regression() -> Result<()> {
        let database = TestDatabase::new().await?;
        let base_timestamp = timestamp(1_717_171_717);

        upsert_chain_lineage_blocks(
            database.pool(),
            &[
                lineage_block(
                    "eth-mainnet",
                    "0x001",
                    None,
                    1,
                    base_timestamp,
                    CanonicalityState::Observed,
                ),
                lineage_block(
                    "eth-mainnet",
                    "0x002",
                    Some("0x001"),
                    2,
                    timestamp(1_717_171_729),
                    CanonicalityState::Observed,
                ),
            ],
        )
        .await?;

        advance_chain_checkpoints(
            database.pool(),
            &ChainCheckpointUpdate {
                chain_id: "eth-mainnet".to_owned(),
                canonical: Some(CheckpointBlockRef {
                    block_hash: "0x002".to_owned(),
                    block_number: 2,
                }),
                safe: Some(CheckpointBlockRef {
                    block_hash: "0x002".to_owned(),
                    block_number: 2,
                }),
                finalized: None,
            },
        )
        .await?;

        let error = advance_chain_checkpoints(
            database.pool(),
            &ChainCheckpointUpdate {
                chain_id: "eth-mainnet".to_owned(),
                canonical: None,
                safe: Some(CheckpointBlockRef {
                    block_hash: "0x001".to_owned(),
                    block_number: 1,
                }),
                finalized: None,
            },
        )
        .await
        .expect_err("safe checkpoint regression must fail");

        assert!(
            error
                .to_string()
                .contains("safe checkpoint for chain eth-mainnet cannot move backward"),
            "unexpected error: {error:#}"
        );

        database.cleanup().await
    }
}
