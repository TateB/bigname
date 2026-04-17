use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use sqlx::{Executor, PgPool, Postgres, Row, postgres::PgRow};

use crate::CanonicalityState;

/// Persisted exact code-hash observation anchored to one observed block hash.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawCodeHash {
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub contract_address: String,
    pub code_hash: String,
    pub code_byte_length: i64,
    pub canonicality_state: CanonicalityState,
}

/// Insert missing raw code-hash rows or refresh canonicality for already
/// observed block-scoped code observations.
pub async fn upsert_raw_code_hashes(
    pool: &PgPool,
    code_hashes: &[RawCodeHash],
) -> Result<Vec<RawCodeHash>> {
    if code_hashes.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw code-hash upsert")?;

    let mut snapshots = Vec::with_capacity(code_hashes.len());
    for code_hash in code_hashes {
        validate_raw_code_hash(code_hash)?;
        snapshots.push(upsert_raw_code_hash(&mut transaction, code_hash).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw code-hash upsert")?;

    Ok(snapshots)
}

/// Load stored code-hash counts by block hash for one chain.
pub async fn load_raw_code_hash_counts_by_block_hashes(
    pool: &PgPool,
    chain_id: &str,
    block_hashes: &[String],
) -> Result<BTreeMap<String, usize>> {
    if block_hashes.is_empty() {
        return Ok(BTreeMap::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT block_hash, COUNT(*)::BIGINT AS observation_count
        FROM raw_code_hashes
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
        GROUP BY block_hash
        "#,
    )
    .bind(chain_id)
    .bind(block_hashes)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load raw code-hash counts for chain {chain_id} across {} hashes",
            block_hashes.len()
        )
    })?;

    rows.into_iter()
        .map(|row| {
            let block_hash = row
                .try_get::<String, _>("block_hash")
                .context("missing block_hash from code-hash count row")?;
            let observation_count = row
                .try_get::<i64, _>("observation_count")
                .context("missing observation_count from code-hash count row")?;
            let observation_count = usize::try_from(observation_count).with_context(|| {
                format!(
                    "raw code-hash count for chain {chain_id} block {block_hash} does not fit in usize"
                )
            })?;
            Ok((block_hash, observation_count))
        })
        .collect()
}

async fn upsert_raw_code_hash(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    code_hash: &RawCodeHash,
) -> Result<RawCodeHash> {
    if let Some(snapshot) = sqlx::query(
        r#"
        INSERT INTO raw_code_hashes (
            chain_id,
            block_hash,
            block_number,
            contract_address,
            code_hash,
            code_byte_length,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7::canonicality_state)
        ON CONFLICT (chain_id, block_hash, contract_address) DO NOTHING
        RETURNING
            chain_id,
            block_hash,
            block_number,
            contract_address,
            code_hash,
            code_byte_length,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&code_hash.chain_id)
    .bind(&code_hash.block_hash)
    .bind(code_hash.block_number)
    .bind(&code_hash.contract_address)
    .bind(&code_hash.code_hash)
    .bind(code_hash.code_byte_length)
    .bind(code_hash.canonicality_state.as_str())
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert raw code-hash for chain {} block {} contract {}",
            code_hash.chain_id, code_hash.block_hash, code_hash.contract_address
        )
    })? {
        return decode_raw_code_hash(snapshot);
    }

    let existing = load_raw_code_hash_internal(
        &mut **executor,
        &code_hash.chain_id,
        &code_hash.block_hash,
        &code_hash.contract_address,
    )
    .await?
    .with_context(|| {
        format!(
            "failed to reload existing raw code-hash for chain {} block {} contract {} after insert conflict",
            code_hash.chain_id, code_hash.block_hash, code_hash.contract_address
        )
    })?;

    ensure_raw_code_hash_identity_matches(&existing, code_hash)?;
    let next_state = merge_canonicality(existing.canonicality_state, code_hash.canonicality_state);

    let snapshot = sqlx::query(
        r#"
        UPDATE raw_code_hashes
        SET
            canonicality_state = $4::canonicality_state,
            observed_at = now()
        WHERE chain_id = $1
          AND block_hash = $2
          AND contract_address = $3
        RETURNING
            chain_id,
            block_hash,
            block_number,
            contract_address,
            code_hash,
            code_byte_length,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(&code_hash.chain_id)
    .bind(&code_hash.block_hash)
    .bind(&code_hash.contract_address)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh existing raw code-hash for chain {} block {} contract {}",
            code_hash.chain_id, code_hash.block_hash, code_hash.contract_address
        )
    })?;

    decode_raw_code_hash(snapshot)
}

async fn load_raw_code_hash_internal<'e, E>(
    executor: E,
    chain_id: &str,
    block_hash: &str,
    contract_address: &str,
) -> Result<Option<RawCodeHash>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            block_number,
            contract_address,
            code_hash,
            code_byte_length,
            canonicality_state::TEXT AS canonicality_state
        FROM raw_code_hashes
        WHERE chain_id = $1
          AND block_hash = $2
          AND contract_address = $3
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(contract_address)
    .fetch_optional(executor)
    .await
    .with_context(|| {
        format!(
            "failed to load raw code-hash for chain {chain_id} block {block_hash} contract {contract_address}"
        )
    })?;

    row.map(decode_raw_code_hash).transpose()
}

fn validate_raw_code_hash(code_hash: &RawCodeHash) -> Result<()> {
    if code_hash.block_number < 0 {
        bail!(
            "raw code-hash for chain {} block {} contract {} has negative block number {}",
            code_hash.chain_id,
            code_hash.block_hash,
            code_hash.contract_address,
            code_hash.block_number
        );
    }
    if code_hash.code_byte_length < 0 {
        bail!(
            "raw code-hash for chain {} block {} contract {} has negative byte length {}",
            code_hash.chain_id,
            code_hash.block_hash,
            code_hash.contract_address,
            code_hash.code_byte_length
        );
    }
    if code_hash.contract_address.is_empty() {
        bail!(
            "raw code-hash for chain {} block {} has empty contract address",
            code_hash.chain_id,
            code_hash.block_hash
        );
    }
    if code_hash.code_hash.is_empty() {
        bail!(
            "raw code-hash for chain {} block {} contract {} has empty code hash",
            code_hash.chain_id,
            code_hash.block_hash,
            code_hash.contract_address
        );
    }

    Ok(())
}

fn ensure_raw_code_hash_identity_matches(
    existing: &RawCodeHash,
    incoming: &RawCodeHash,
) -> Result<()> {
    if existing.block_number != incoming.block_number
        || existing.code_hash != incoming.code_hash
        || existing.code_byte_length != incoming.code_byte_length
    {
        bail!(
            "raw code-hash identity mismatch for chain {} block {} contract {}",
            existing.chain_id,
            existing.block_hash,
            existing.contract_address
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

fn decode_raw_code_hash(row: PgRow) -> Result<RawCodeHash> {
    Ok(RawCodeHash {
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number")?,
        contract_address: row
            .try_get("contract_address")
            .context("missing contract_address")?,
        code_hash: row.try_get("code_hash").context("missing code_hash")?,
        code_byte_length: row
            .try_get("code_byte_length")
            .context("missing code_byte_length")?,
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
}
