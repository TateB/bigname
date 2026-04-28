use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder};

use super::decode::decode_lineage_block;
use super::reads::load_chain_lineage_block_internal;
use super::types::ChainLineageBlock;
use super::validation::{ensure_lineage_identity_matches, validate_lineage_block};

/// Insert missing lineage rows or refresh existing rows when the same block hash
/// is observed again. Immutable block metadata must match the stored row.
pub async fn upsert_chain_lineage_blocks(
    pool: &PgPool,
    blocks: &[ChainLineageBlock],
) -> Result<Vec<ChainLineageBlock>> {
    if blocks.is_empty() {
        return Ok(Vec::new());
    }

    if blocks.len() >= BULK_LINEAGE_UPSERT_MIN_ROWS {
        return upsert_chain_lineage_blocks_bulk(pool, blocks).await;
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

/// Insert or refresh chain lineage blocks without returning row snapshots.
pub async fn upsert_chain_lineage_blocks_without_snapshots(
    pool: &PgPool,
    blocks: &[ChainLineageBlock],
) -> Result<()> {
    if blocks.is_empty() {
        return Ok(());
    }

    for block in blocks {
        validate_lineage_block(block)?;
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for chain lineage bulk upsert")?;

    for chunk in blocks.chunks(BULK_LINEAGE_UPSERT_CHUNK_ROWS) {
        let mut builder = QueryBuilder::<Postgres>::new(
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
                    WHEN chain_lineage.canonicality_state = 'orphaned'::canonicality_state THEN EXCLUDED.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'orphaned'::canonicality_state THEN 'orphaned'::canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'canonical'::canonicality_state
                        AND chain_lineage.canonicality_state IN ('safe'::canonicality_state, 'finalized'::canonicality_state)
                        THEN chain_lineage.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'safe'::canonicality_state
                        AND chain_lineage.canonicality_state = 'finalized'::canonicality_state
                        THEN chain_lineage.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'observed'::canonicality_state
                        THEN chain_lineage.canonicality_state
                    ELSE EXCLUDED.canonicality_state
                END,
                observed_at = now()
            WHERE chain_lineage.parent_hash IS NOT DISTINCT FROM EXCLUDED.parent_hash
              AND chain_lineage.block_number = EXCLUDED.block_number
              AND chain_lineage.block_timestamp = EXCLUDED.block_timestamp
              AND chain_lineage.logs_bloom IS NOT DISTINCT FROM EXCLUDED.logs_bloom
              AND chain_lineage.transactions_root IS NOT DISTINCT FROM EXCLUDED.transactions_root
              AND chain_lineage.receipts_root IS NOT DISTINCT FROM EXCLUDED.receipts_root
              AND chain_lineage.state_root IS NOT DISTINCT FROM EXCLUDED.state_root
            "#,
        );

        let result = builder
            .build()
            .execute(&mut *transaction)
            .await
            .context("failed to bulk upsert chain lineage blocks")?;
        if result.rows_affected() != chunk.len() as u64 {
            anyhow::bail!(
                "chain lineage identity mismatch while bulk upserting {} rows",
                chunk.len()
            );
        }
    }

    transaction
        .commit()
        .await
        .context("failed to commit chain lineage bulk upsert")?;

    Ok(())
}

const BULK_LINEAGE_UPSERT_MIN_ROWS: usize = 128;
const BULK_LINEAGE_UPSERT_CHUNK_ROWS: usize = 5_000;

async fn upsert_chain_lineage_blocks_bulk(
    pool: &PgPool,
    blocks: &[ChainLineageBlock],
) -> Result<Vec<ChainLineageBlock>> {
    for block in blocks {
        validate_lineage_block(block)?;
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for chain lineage bulk upsert")?;
    let mut snapshots = Vec::with_capacity(blocks.len());

    for chunk in blocks.chunks(BULK_LINEAGE_UPSERT_CHUNK_ROWS) {
        let mut builder = QueryBuilder::<Postgres>::new(
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
                    WHEN chain_lineage.canonicality_state = 'orphaned'::canonicality_state THEN EXCLUDED.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'orphaned'::canonicality_state THEN 'orphaned'::canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'canonical'::canonicality_state
                        AND chain_lineage.canonicality_state IN ('safe'::canonicality_state, 'finalized'::canonicality_state)
                        THEN chain_lineage.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'safe'::canonicality_state
                        AND chain_lineage.canonicality_state = 'finalized'::canonicality_state
                        THEN chain_lineage.canonicality_state
                    WHEN EXCLUDED.canonicality_state = 'observed'::canonicality_state
                        THEN chain_lineage.canonicality_state
                    ELSE EXCLUDED.canonicality_state
                END,
                observed_at = now()
            WHERE chain_lineage.parent_hash IS NOT DISTINCT FROM EXCLUDED.parent_hash
              AND chain_lineage.block_number = EXCLUDED.block_number
              AND chain_lineage.block_timestamp = EXCLUDED.block_timestamp
              AND chain_lineage.logs_bloom IS NOT DISTINCT FROM EXCLUDED.logs_bloom
              AND chain_lineage.transactions_root IS NOT DISTINCT FROM EXCLUDED.transactions_root
              AND chain_lineage.receipts_root IS NOT DISTINCT FROM EXCLUDED.receipts_root
              AND chain_lineage.state_root IS NOT DISTINCT FROM EXCLUDED.state_root
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
            .context("failed to bulk upsert chain lineage rows")?;
        if rows.len() != chunk.len() {
            anyhow::bail!(
                "chain lineage identity mismatch while bulk upserting {} rows",
                chunk.len()
            );
        }
        snapshots.extend(
            rows.into_iter()
                .map(decode_lineage_block)
                .collect::<Result<Vec<_>>>()?,
        );
    }

    transaction
        .commit()
        .await
        .context("failed to commit chain lineage bulk upsert")?;

    Ok(snapshots)
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
