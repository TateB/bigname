use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use sqlx::{Executor, PgPool, Postgres, Row, postgres::PgRow};

use crate::{
    CanonicalityState,
    evm_primitives::{normalize_evm_address, normalize_evm_b256},
};

mod bulk;

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

    let code_hashes = code_hashes
        .iter()
        .map(normalize_raw_code_hash)
        .collect::<Vec<_>>();

    if code_hashes.len() >= bulk::BULK_RAW_CODE_HASH_UPSERT_MIN_ROWS {
        return bulk::upsert_raw_code_hashes_bulk(pool, &code_hashes).await;
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw code-hash upsert")?;

    let mut snapshots = Vec::with_capacity(code_hashes.len());
    for code_hash in &code_hashes {
        validate_raw_code_hash(code_hash)?;
        snapshots.push(upsert_raw_code_hash(&mut transaction, code_hash).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw code-hash upsert")?;

    Ok(snapshots)
}

fn normalize_raw_code_hash(code_hash: &RawCodeHash) -> RawCodeHash {
    RawCodeHash {
        chain_id: code_hash.chain_id.clone(),
        block_hash: normalize_evm_b256(&code_hash.block_hash),
        block_number: code_hash.block_number,
        contract_address: normalize_evm_address(&code_hash.contract_address),
        code_hash: normalize_evm_b256(&code_hash.code_hash),
        code_byte_length: code_hash.code_byte_length,
        canonicality_state: code_hash.canonicality_state,
    }
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
    let block_hashes = block_hashes
        .iter()
        .map(|block_hash| normalize_evm_b256(block_hash))
        .collect::<Vec<_>>();

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
    .bind(&block_hashes)
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
    let next_state = existing
        .canonicality_state
        .merge_observation(code_hash.canonicality_state);

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
    let block_hash = normalize_evm_b256(block_hash);
    let contract_address = normalize_evm_address(contract_address);
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
    .bind(&block_hash)
    .bind(&contract_address)
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

fn decode_raw_code_hash(row: PgRow) -> Result<RawCodeHash> {
    Ok(RawCodeHash {
        chain_id: crate::sql_row::get(&row, "chain_id")?,
        block_hash: crate::sql_row::get(&row, "block_hash")?,
        block_number: crate::sql_row::get(&row, "block_number")?,
        contract_address: crate::sql_row::get(&row, "contract_address")?,
        code_hash: crate::sql_row::get(&row, "code_hash")?,
        code_byte_length: crate::sql_row::get(&row, "code_byte_length")?,
        canonicality_state: crate::sql_row::get(&row, "canonicality_state")?,
    })
}

#[cfg(test)]
mod tests;
