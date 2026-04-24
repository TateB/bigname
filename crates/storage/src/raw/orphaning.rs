use anyhow::{Context, Result, bail};
use sqlx::{Executor, PgPool, Postgres};

use super::{
    block_facts::load_raw_block_snapshots_for_hashes, decode::decode_raw_block, types::RawBlock,
};

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
