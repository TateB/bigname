use anyhow::Result;
use sqlx::{Executor, PgPool, Postgres};

use super::types::RawBlock;
use super::validation::validate_raw_block;
use crate::{
    ChainLineageBlock, load_chain_lineage_block, upsert_chain_lineage_blocks,
    upsert_chain_lineage_blocks_recanonicalizing_orphaned,
    upsert_chain_lineage_blocks_without_snapshots,
};

/// Load one block header anchor by hash-first identity.
pub async fn load_raw_block(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<Option<RawBlock>> {
    Ok(load_chain_lineage_block(pool, chain_id, block_hash)
        .await?
        .map(lineage_to_raw_block))
}

/// Load a stored set of block header anchors by hash-first identity.
pub async fn load_raw_blocks_by_hashes(
    pool: &PgPool,
    chain_id: &str,
    block_hashes: &[String],
) -> Result<Vec<RawBlock>> {
    load_raw_block_snapshots_for_hashes(pool, chain_id, block_hashes).await
}

/// Insert missing block header anchors or refresh canonicality when the same
/// block hash is fetched again.
pub async fn upsert_raw_blocks(pool: &PgPool, blocks: &[RawBlock]) -> Result<Vec<RawBlock>> {
    if blocks.is_empty() {
        return Ok(Vec::new());
    }

    for block in blocks {
        validate_raw_block(block)?;
    }

    let lineage_blocks = blocks.iter().map(raw_block_to_lineage).collect::<Vec<_>>();
    Ok(upsert_chain_lineage_blocks(pool, &lineage_blocks)
        .await?
        .into_iter()
        .map(lineage_to_raw_block)
        .collect())
}

/// Insert missing block header anchors or refresh canonicality using explicitly
/// fresh reconciliation evidence, allowing a stored orphaned row to be restored.
pub async fn upsert_raw_blocks_recanonicalizing_orphaned(
    pool: &PgPool,
    blocks: &[RawBlock],
) -> Result<Vec<RawBlock>> {
    if blocks.is_empty() {
        return Ok(Vec::new());
    }

    for block in blocks {
        validate_raw_block(block)?;
    }

    let lineage_blocks = blocks.iter().map(raw_block_to_lineage).collect::<Vec<_>>();
    Ok(
        upsert_chain_lineage_blocks_recanonicalizing_orphaned(pool, &lineage_blocks)
            .await?
            .into_iter()
            .map(lineage_to_raw_block)
            .collect(),
    )
}

/// Insert or refresh block header anchors without returning row snapshots.
pub async fn upsert_raw_blocks_without_snapshots(pool: &PgPool, blocks: &[RawBlock]) -> Result<()> {
    if blocks.is_empty() {
        return Ok(());
    }

    for block in blocks {
        validate_raw_block(block)?;
    }

    let lineage_blocks = blocks.iter().map(raw_block_to_lineage).collect::<Vec<_>>();
    upsert_chain_lineage_blocks_without_snapshots(pool, &lineage_blocks).await
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
            lineage.chain_id,
            lineage.block_hash,
            lineage.parent_hash,
            lineage.block_number,
            lineage.block_timestamp,
            audit.logs_bloom,
            audit.transactions_root,
            audit.receipts_root,
            audit.state_root,
            lineage.canonicality_state::TEXT AS canonicality_state
        FROM chain_lineage AS lineage
        LEFT JOIN chain_header_audit AS audit
          ON audit.chain_id = lineage.chain_id
         AND audit.block_hash = lineage.block_hash
        WHERE lineage.chain_id = $1
          AND lineage.block_hash = ANY($2::TEXT[])
        ORDER BY lineage.block_number, lineage.block_hash
        "#,
    )
    .bind(chain_id)
    .bind(block_hashes)
    .fetch_all(executor)
    .await?;

    rows.into_iter()
        .map(super::decode::decode_raw_block)
        .collect()
}

fn raw_block_to_lineage(block: &RawBlock) -> ChainLineageBlock {
    ChainLineageBlock {
        chain_id: block.chain_id.clone(),
        block_hash: block.block_hash.clone(),
        parent_hash: block.parent_hash.clone(),
        block_number: block.block_number,
        block_timestamp: block.block_timestamp,
        logs_bloom: block.logs_bloom.clone(),
        transactions_root: block.transactions_root.clone(),
        receipts_root: block.receipts_root.clone(),
        state_root: block.state_root.clone(),
        canonicality_state: block.canonicality_state,
    }
}

fn lineage_to_raw_block(block: ChainLineageBlock) -> RawBlock {
    RawBlock {
        chain_id: block.chain_id,
        block_hash: block.block_hash,
        parent_hash: block.parent_hash,
        block_number: block.block_number,
        block_timestamp: block.block_timestamp,
        logs_bloom: block.logs_bloom,
        transactions_root: block.transactions_root,
        receipts_root: block.receipts_root,
        state_root: block.state_root,
        canonicality_state: block.canonicality_state,
    }
}
