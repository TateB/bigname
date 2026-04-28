use anyhow::{Context, Result};
use sqlx::{Executor, PgPool, Postgres, QueryBuilder};

use super::{
    decode::decode_raw_block,
    types::RawBlock,
    validation::{ensure_raw_identity_matches, merge_canonicality, validate_raw_block},
};

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

    if blocks.len() >= BULK_RAW_BLOCK_UPSERT_MIN_ROWS {
        return upsert_raw_blocks_bulk(pool, blocks).await;
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

/// Insert or refresh raw blocks without returning row snapshots.
pub async fn upsert_raw_blocks_without_snapshots(pool: &PgPool, blocks: &[RawBlock]) -> Result<()> {
    if blocks.is_empty() {
        return Ok(());
    }

    for block in blocks {
        validate_raw_block(block)?;
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw block bulk upsert")?;

    for chunk in blocks.chunks(BULK_RAW_BLOCK_UPSERT_CHUNK_ROWS) {
        let mut builder = QueryBuilder::<Postgres>::new(
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
                canonicality_state::canonicality_state
            FROM (
            "#,
        );

        builder.push_values(chunk, |mut row, block| {
            row.push_bind(&block.chain_id)
                .push_bind(&block.block_hash)
                .push_bind(&block.parent_hash)
                .push_bind(block.block_number)
                .push_bind(block.block_timestamp)
                .push_bind(&block.logs_bloom)
                .push_bind(&block.transactions_root)
                .push_bind(&block.receipts_root)
                .push_bind(&block.state_root)
                .push_bind(block.canonicality_state.as_str());
        });

        builder.push(
            r#"
            ) AS input (
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
            ON CONFLICT (chain_id, block_hash) DO UPDATE
            SET
                canonicality_state = CASE
                    WHEN raw_blocks.canonicality_state = 'orphaned'::canonicality_state THEN EXCLUDED.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'orphaned'::canonicality_state THEN 'orphaned'::canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'canonical'::canonicality_state
                        AND raw_blocks.canonicality_state IN ('safe'::canonicality_state, 'finalized'::canonicality_state)
                        THEN raw_blocks.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'safe'::canonicality_state
                        AND raw_blocks.canonicality_state = 'finalized'::canonicality_state
                        THEN raw_blocks.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'observed'::canonicality_state
                        THEN raw_blocks.canonicality_state
                    ELSE EXCLUDED.canonicality_state
                END,
                observed_at = now(),
                fetched_at = now()
            WHERE raw_blocks.parent_hash IS NOT DISTINCT FROM EXCLUDED.parent_hash
              AND raw_blocks.block_number = EXCLUDED.block_number
              AND raw_blocks.block_timestamp = EXCLUDED.block_timestamp
              AND raw_blocks.logs_bloom IS NOT DISTINCT FROM EXCLUDED.logs_bloom
              AND raw_blocks.transactions_root IS NOT DISTINCT FROM EXCLUDED.transactions_root
              AND raw_blocks.receipts_root IS NOT DISTINCT FROM EXCLUDED.receipts_root
              AND raw_blocks.state_root IS NOT DISTINCT FROM EXCLUDED.state_root
            "#,
        );

        let result = builder
            .build()
            .execute(&mut *transaction)
            .await
            .context("failed to bulk upsert raw blocks")?;
        if result.rows_affected() != chunk.len() as u64 {
            anyhow::bail!(
                "raw block identity mismatch while bulk upserting {} rows",
                chunk.len()
            );
        }
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw block bulk upsert")?;

    Ok(())
}

const BULK_RAW_BLOCK_UPSERT_MIN_ROWS: usize = 128;
const BULK_RAW_BLOCK_UPSERT_CHUNK_ROWS: usize = 5_000;

async fn upsert_raw_blocks_bulk(pool: &PgPool, blocks: &[RawBlock]) -> Result<Vec<RawBlock>> {
    for block in blocks {
        validate_raw_block(block)?;
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw block bulk upsert")?;
    let mut snapshots = Vec::with_capacity(blocks.len());

    for chunk in blocks.chunks(BULK_RAW_BLOCK_UPSERT_CHUNK_ROWS) {
        let mut builder = QueryBuilder::<Postgres>::new(
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
                canonicality_state::canonicality_state
            FROM (
            "#,
        );

        builder.push_values(chunk, |mut row, block| {
            row.push_bind(&block.chain_id)
                .push_bind(&block.block_hash)
                .push_bind(&block.parent_hash)
                .push_bind(block.block_number)
                .push_bind(block.block_timestamp)
                .push_bind(&block.logs_bloom)
                .push_bind(&block.transactions_root)
                .push_bind(&block.receipts_root)
                .push_bind(&block.state_root)
                .push_bind(block.canonicality_state.as_str());
        });

        builder.push(
            r#"
            ) AS input (
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
            ON CONFLICT (chain_id, block_hash) DO UPDATE
            SET
                canonicality_state = CASE
                    WHEN raw_blocks.canonicality_state = 'orphaned'::canonicality_state THEN EXCLUDED.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'orphaned'::canonicality_state THEN 'orphaned'::canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'canonical'::canonicality_state
                        AND raw_blocks.canonicality_state IN ('safe'::canonicality_state, 'finalized'::canonicality_state)
                        THEN raw_blocks.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'safe'::canonicality_state
                        AND raw_blocks.canonicality_state = 'finalized'::canonicality_state
                        THEN raw_blocks.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'observed'::canonicality_state
                        THEN raw_blocks.canonicality_state
                    ELSE EXCLUDED.canonicality_state
                END,
                observed_at = now(),
                fetched_at = now()
            WHERE raw_blocks.parent_hash IS NOT DISTINCT FROM EXCLUDED.parent_hash
              AND raw_blocks.block_number = EXCLUDED.block_number
              AND raw_blocks.block_timestamp = EXCLUDED.block_timestamp
              AND raw_blocks.logs_bloom IS NOT DISTINCT FROM EXCLUDED.logs_bloom
              AND raw_blocks.transactions_root IS NOT DISTINCT FROM EXCLUDED.transactions_root
              AND raw_blocks.receipts_root IS NOT DISTINCT FROM EXCLUDED.receipts_root
              AND raw_blocks.state_root IS NOT DISTINCT FROM EXCLUDED.state_root
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
        );

        let rows = builder
            .build()
            .fetch_all(&mut *transaction)
            .await
            .context("failed to bulk upsert raw blocks")?;
        if rows.len() != chunk.len() {
            anyhow::bail!(
                "raw block identity mismatch while bulk upserting {} rows",
                chunk.len()
            );
        }
        snapshots.extend(
            rows.into_iter()
                .map(decode_raw_block)
                .collect::<Result<Vec<_>>>()?,
        );
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw block bulk upsert")?;

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

pub(super) async fn load_raw_block_snapshots_for_hashes<'e, E>(
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
