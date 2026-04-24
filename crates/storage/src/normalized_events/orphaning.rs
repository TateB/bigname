use anyhow::{Context, Result, bail};
use sqlx::{Executor, PgPool, Postgres, Row};

/// Mark block-derived normalized events on a losing branch `orphaned` until
/// `stop_before_hash` is reached.
pub async fn mark_block_derived_normalized_events_range_orphaned(
    pool: &PgPool,
    chain_id: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<u64> {
    if stop_before_hash == Some(from_hash) {
        return Ok(0);
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for normalized-event orphaning")?;

    let block_hashes =
        load_raw_block_hash_path(&mut *transaction, chain_id, from_hash, stop_before_hash)
            .await
            .with_context(|| {
                format!(
                    "failed to load raw block hash path for normalized events on chain {chain_id} starting from block {from_hash}"
                )
            })?;
    if block_hashes.is_empty() {
        bail!("missing stored raw block for chain {chain_id} block {from_hash}");
    }

    let updated_count = sqlx::query(
        r#"
        UPDATE normalized_events
        SET
            canonicality_state = 'orphaned'::canonicality_state,
            observed_at = now()
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND canonicality_state <> 'orphaned'::canonicality_state
        "#,
    )
    .bind(chain_id)
    .bind(&block_hashes)
    .execute(&mut *transaction)
    .await
    .with_context(|| {
        format!("failed to mark block-derived normalized events orphaned for chain {chain_id}")
    })?
    .rows_affected();

    transaction
        .commit()
        .await
        .context("failed to commit normalized-event orphaning")?;

    Ok(updated_count)
}

async fn load_raw_block_hash_path<'e, E>(
    executor: E,
    chain_id: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<Vec<String>>
where
    E: Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(
        r#"
        WITH RECURSIVE raw_block_path AS (
            SELECT block_hash, parent_hash, 0 AS depth
            FROM raw_blocks
            WHERE chain_id = $1
              AND block_hash = $2

            UNION ALL

            SELECT parent.block_hash, parent.parent_hash, raw_block_path.depth + 1
            FROM raw_blocks parent
            JOIN raw_block_path
              ON parent.chain_id = $1
             AND parent.block_hash = raw_block_path.parent_hash
            WHERE $3::TEXT IS NULL
               OR parent.block_hash <> $3::TEXT
        )
        SELECT block_hash
        FROM raw_block_path
        ORDER BY depth
        "#,
    )
    .bind(chain_id)
    .bind(from_hash)
    .bind(stop_before_hash)
    .fetch_all(executor)
    .await?;

    rows.into_iter()
        .map(|row| {
            row.try_get::<String, _>("block_hash")
                .context("missing block_hash in raw block path")
        })
        .collect()
}
