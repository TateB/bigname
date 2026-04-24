use anyhow::{Context, Result, bail};
use sqlx::{Executor, PgPool, Postgres};

use super::decode::decode_lineage_block;
use super::types::ChainLineageBlock;

/// Load one lineage snapshot by hash-first identity.
pub async fn load_chain_lineage_block(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<Option<ChainLineageBlock>> {
    load_chain_lineage_block_internal(pool, chain_id, block_hash).await
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

pub(crate) async fn load_chain_lineage_block_internal<'e, E>(
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

pub(crate) async fn load_chain_lineage_path<'e, E>(
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

pub(crate) async fn load_lineage_snapshots_for_hashes<'e, E>(
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
