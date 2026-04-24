use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres};

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
