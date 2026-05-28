use anyhow::{Context, Result, bail};
use sqlx::{Executor, PgPool, Postgres};

use super::{load::load_raw_block_hash_path, types::RawFactOrphanCounts};

/// Walk a stored raw-block branch and mark every block-scoped raw fact on that
/// losing branch `orphaned` until `stop_before_hash` is reached.
pub async fn mark_raw_block_facts_range_orphaned(
    pool: &PgPool,
    chain_id: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<RawFactOrphanCounts> {
    if stop_before_hash == Some(from_hash) {
        return Ok(RawFactOrphanCounts::default());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw fact orphaning")?;

    let block_hashes = load_raw_block_hash_path(&mut *transaction, chain_id, from_hash, stop_before_hash)
        .await
        .with_context(|| {
            format!(
                "failed to load raw block hash path for chain {chain_id} starting from block {from_hash}"
            )
        })?;
    if block_hashes.is_empty() {
        bail!("missing stored raw block for chain {chain_id} block {from_hash}");
    }

    let code_hash_count = mark_block_hash_set_orphaned(
        &mut *transaction,
        "raw_code_hashes",
        "observed_at",
        chain_id,
        &block_hashes,
    )
    .await?;
    let transaction_count = mark_block_hash_set_orphaned(
        &mut *transaction,
        "raw_transactions",
        "observed_at",
        chain_id,
        &block_hashes,
    )
    .await?;
    let receipt_count = mark_block_hash_set_orphaned(
        &mut *transaction,
        "raw_receipts",
        "observed_at",
        chain_id,
        &block_hashes,
    )
    .await?;
    let log_count = mark_block_hash_set_orphaned(
        &mut *transaction,
        "raw_logs",
        "observed_at",
        chain_id,
        &block_hashes,
    )
    .await?;
    let call_snapshot_count = mark_block_hash_set_orphaned(
        &mut *transaction,
        "raw_call_snapshots",
        "observed_at",
        chain_id,
        &block_hashes,
    )
    .await?;
    let payload_cache_metadata_count = mark_block_hash_set_orphaned(
        &mut *transaction,
        "raw_payload_cache_metadata",
        "last_observed_at",
        chain_id,
        &block_hashes,
    )
    .await?;
    mark_block_hash_set_orphaned(
        &mut *transaction,
        "event_silent_resolver_call_observations",
        "observed_at",
        chain_id,
        &block_hashes,
    )
    .await?;

    transaction
        .commit()
        .await
        .context("failed to commit raw fact orphaning")?;

    Ok(RawFactOrphanCounts {
        block_count: 0,
        code_hash_count,
        transaction_count,
        receipt_count,
        log_count,
        call_snapshot_count,
        payload_cache_metadata_count,
    })
}

async fn mark_block_hash_set_orphaned<'e, E>(
    executor: E,
    table_name: &str,
    timestamp_column: &str,
    chain_id: &str,
    block_hashes: &[String],
) -> Result<u64>
where
    E: Executor<'e, Database = Postgres>,
{
    let query = format!(
        r#"
        UPDATE {table_name}
        SET
            canonicality_state = 'orphaned'::canonicality_state,
            {timestamp_column} = now()
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND canonicality_state <> 'orphaned'::canonicality_state
        "#
    );

    sqlx::query(&query)
        .bind(chain_id)
        .bind(block_hashes)
        .execute(executor)
        .await
        .with_context(|| {
            format!("failed to mark orphaned raw facts in {table_name} for chain {chain_id}")
        })
        .map(|result| result.rows_affected())
}
