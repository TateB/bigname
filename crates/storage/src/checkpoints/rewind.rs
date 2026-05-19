use anyhow::{Context, Result};
use sqlx::PgPool;

use super::{ChainCheckpoint, CheckpointBlockRef, decode_snapshot, ensure_chain_checkpoint_rows};
use crate::lineage::ensure_chain_lineage_block;

/// Move any checkpoint pointer above or conflicting with an exact ancestor
/// back to that ancestor. This is reserved for explicit rewind repair; normal
/// checkpoint advancement remains monotonic.
pub async fn rewind_chain_checkpoints_to_ancestor(
    pool: &PgPool,
    chain_id: &str,
    ancestor: &CheckpointBlockRef,
) -> Result<ChainCheckpoint> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for checkpoint rewind")?;

    let chain_ids = vec![chain_id.to_owned()];
    ensure_chain_checkpoint_rows(&mut *transaction, &chain_ids).await?;
    ensure_chain_lineage_block(
        &mut transaction,
        chain_id,
        &ancestor.block_hash,
        ancestor.block_number,
    )
    .await?;

    let row = sqlx::query(
        r#"
        UPDATE chain_checkpoints
        SET
            canonical_block_hash = CASE
                WHEN canonical_block_number IS NOT NULL
                 AND (
                    canonical_block_number > $3
                    OR (canonical_block_number = $3 AND canonical_block_hash <> $2)
                 )
                    THEN $2
                ELSE canonical_block_hash
            END,
            canonical_block_number = CASE
                WHEN canonical_block_number IS NOT NULL
                 AND (
                    canonical_block_number > $3
                    OR (canonical_block_number = $3 AND canonical_block_hash <> $2)
                 )
                    THEN $3
                ELSE canonical_block_number
            END,
            safe_block_hash = CASE
                WHEN safe_block_number IS NOT NULL
                 AND (
                    safe_block_number > $3
                    OR (safe_block_number = $3 AND safe_block_hash <> $2)
                 )
                    THEN $2
                ELSE safe_block_hash
            END,
            safe_block_number = CASE
                WHEN safe_block_number IS NOT NULL
                 AND (
                    safe_block_number > $3
                    OR (safe_block_number = $3 AND safe_block_hash <> $2)
                 )
                    THEN $3
                ELSE safe_block_number
            END,
            finalized_block_hash = CASE
                WHEN finalized_block_number IS NOT NULL
                 AND (
                    finalized_block_number > $3
                    OR (finalized_block_number = $3 AND finalized_block_hash <> $2)
                 )
                    THEN $2
                ELSE finalized_block_hash
            END,
            finalized_block_number = CASE
                WHEN finalized_block_number IS NOT NULL
                 AND (
                    finalized_block_number > $3
                    OR (finalized_block_number = $3 AND finalized_block_hash <> $2)
                 )
                    THEN $3
                ELSE finalized_block_number
            END,
            updated_at = now()
        WHERE chain_id = $1
        RETURNING
            chain_id,
            canonical_block_hash,
            canonical_block_number,
            safe_block_hash,
            safe_block_number,
            finalized_block_hash,
            finalized_block_number
        "#,
    )
    .bind(chain_id)
    .bind(&ancestor.block_hash)
    .bind(ancestor.block_number)
    .fetch_one(&mut *transaction)
    .await
    .with_context(|| format!("failed to rewind checkpoint row for chain {chain_id}"))?;
    let checkpoint = decode_snapshot(row)?;

    transaction
        .commit()
        .await
        .context("failed to commit checkpoint rewind")?;

    Ok(checkpoint)
}
