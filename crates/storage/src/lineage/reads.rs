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
          AND lineage.block_hash = $2
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
        WITH RECURSIVE stop_block AS (
            SELECT block_number
            FROM chain_lineage
            WHERE chain_id = $1
              AND block_hash = $3::TEXT
        ),
        lineage_path AS (
            SELECT chain_id, block_hash, parent_hash, block_number, 0 AS depth
            FROM chain_lineage
            WHERE chain_id = $1
              AND block_hash = $2

            UNION ALL

            SELECT
                parent.chain_id,
                parent.block_hash,
                parent.parent_hash,
                parent.block_number,
                lineage_path.depth + 1
            FROM chain_lineage AS parent
            JOIN lineage_path
              ON parent.chain_id = lineage_path.chain_id
             AND parent.block_hash = lineage_path.parent_hash
            LEFT JOIN stop_block ON TRUE
            WHERE $3::TEXT IS NULL
               OR (
                    stop_block.block_number IS NOT NULL
                    AND lineage_path.block_number > stop_block.block_number
                    AND parent.block_hash <> $3::TEXT
               )
        )
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
        FROM lineage_path
        JOIN chain_lineage AS lineage
          ON lineage.chain_id = lineage_path.chain_id
         AND lineage.block_hash = lineage_path.block_hash
        LEFT JOIN chain_header_audit AS audit
          ON audit.chain_id = lineage.chain_id
         AND audit.block_hash = lineage.block_hash
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

pub(crate) fn ensure_chain_lineage_path_reaches_stop(
    chain_id: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
    path: &[ChainLineageBlock],
) -> Result<()> {
    let Some(stop_before_hash) = stop_before_hash else {
        return Ok(());
    };

    if path
        .iter()
        .any(|block| block.parent_hash.as_deref() == Some(stop_before_hash))
    {
        return Ok(());
    }

    bail!(
        "stored lineage path for chain {chain_id} from block {from_hash} did not reach required ancestor {stop_before_hash}"
    )
}

pub async fn chain_lineage_contains_ancestor(
    pool: &PgPool,
    chain_id: &str,
    descendant_hash: &str,
    ancestor_hash: &str,
) -> Result<bool> {
    chain_lineage_contains_ancestor_internal(pool, chain_id, descendant_hash, ancestor_hash).await
}

pub(crate) async fn chain_lineage_contains_ancestor_internal<'e, E>(
    executor: E,
    chain_id: &str,
    descendant_hash: &str,
    ancestor_hash: &str,
) -> Result<bool>
where
    E: Executor<'e, Database = Postgres>,
{
    let contains = sqlx::query_scalar::<_, bool>(
        r#"
        WITH RECURSIVE ancestor AS (
            SELECT block_number
            FROM chain_lineage
            WHERE chain_id = $1
              AND block_hash = $3
              AND canonicality_state <> 'orphaned'::canonicality_state
        ),
        lineage_path AS (
            SELECT chain_id, block_hash, parent_hash, block_number
            FROM chain_lineage
            WHERE chain_id = $1
              AND block_hash = $2
              AND canonicality_state <> 'orphaned'::canonicality_state

            UNION ALL

            SELECT parent.chain_id, parent.block_hash, parent.parent_hash, parent.block_number
            FROM chain_lineage AS parent
            JOIN lineage_path
              ON parent.chain_id = lineage_path.chain_id
             AND parent.block_hash = lineage_path.parent_hash
            JOIN ancestor
              ON lineage_path.block_number > ancestor.block_number
            WHERE lineage_path.block_hash <> $3
              AND parent.canonicality_state <> 'orphaned'::canonicality_state
        )
        SELECT EXISTS (
            SELECT 1
            FROM lineage_path
            WHERE block_hash = $3
        )
        "#,
    )
    .bind(chain_id)
    .bind(descendant_hash)
    .bind(ancestor_hash)
    .fetch_one(executor)
    .await
    .with_context(|| {
        format!(
            "failed to prove lineage ancestry for chain {chain_id} descendant {descendant_hash} ancestor {ancestor_hash}"
        )
    })?;

    Ok(contains)
}

/// Check whether one stored `(chain_id, block_number, block_hash)` is eligible
/// as an older canonical ancestor of a selected canonical descendant block.
///
/// This intentionally avoids walking parent links. `chain_lineage` is
/// append-only, and reorg repair flips whole losing branches to `orphaned`.
/// If the selected block is canonical-marked, the candidate is canonical-marked,
/// and the candidate is the unique canonical/safe/finalized row at that height,
/// both rows are on the same canonical chain and block-number ordering implies
/// ancestry. During a mid-reorg window where two rows at the candidate height
/// are still canonical-marked, uniqueness fails and the caller skips the
/// candidate conservatively.
pub async fn chain_lineage_contains_canonical_ancestor_position<'e, E>(
    executor: E,
    chain_id: &str,
    descendant_hash: &str,
    descendant_block_number: i64,
    ancestor_block_number: i64,
    ancestor_hash: &str,
) -> Result<bool>
where
    E: Executor<'e, Database = Postgres>,
{
    let contains = sqlx::query_scalar::<_, bool>(
        r#"
        WITH canonical_at_candidate_height AS (
            SELECT block_hash
            FROM chain_lineage
            WHERE chain_id = $1
              AND block_number = $4
              AND canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            LIMIT 2
        )
        SELECT
            EXISTS (
                SELECT 1
                FROM chain_lineage
                WHERE chain_id = $1
                  AND block_hash = $2
                  AND block_number = $3
                  AND canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
            )
            AND (
                SELECT COUNT(*) = 1
                FROM canonical_at_candidate_height
            )
            AND EXISTS (
                SELECT 1
                FROM canonical_at_candidate_height
                WHERE block_hash = $5
            )
        "#,
    )
    .bind(chain_id)
    .bind(descendant_hash)
    .bind(descendant_block_number)
    .bind(ancestor_block_number)
    .bind(ancestor_hash)
    .fetch_one(executor)
    .await
    .with_context(|| {
        format!(
            "failed to check canonical lineage uniqueness for chain {chain_id} descendant {descendant_hash} ancestor {ancestor_hash} at block {ancestor_block_number}"
        )
    })?;

    Ok(contains)
}

pub async fn load_chain_lineage_canonical_child_path(
    pool: &PgPool,
    chain_id: &str,
    from_hash: &str,
    from_number: i64,
    max_blocks: usize,
) -> Result<Vec<ChainLineageBlock>> {
    let mut path = Vec::with_capacity(max_blocks);
    let mut cursor_hash = from_hash.to_owned();
    let mut cursor_number = from_number;

    for _ in 0..max_blocks {
        let next_number = cursor_number
            .checked_add(1)
            .context("stored lineage child block number overflowed while walking path")?;
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
              AND lineage.parent_hash = $2
              AND lineage.block_number = $3
              AND lineage.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            ORDER BY lineage.block_hash
            LIMIT 2
            "#,
        )
        .bind(chain_id)
        .bind(&cursor_hash)
        .bind(next_number)
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!(
                "failed to load stored canonical lineage child for chain {chain_id} parent {cursor_hash} at block {next_number}"
            )
        })?;

        if rows.is_empty() {
            break;
        }
        if rows.len() > 1 {
            break;
        }

        let block = decode_lineage_block(rows.into_iter().next().expect("checked above"))?;
        cursor_hash = block.block_hash.clone();
        cursor_number = block.block_number;
        path.push(block);
    }

    Ok(path)
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
