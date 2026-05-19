use anyhow::{Context, Result, bail};
use sqlx::PgPool;

use super::reads::{
    ensure_chain_lineage_path_reaches_stop, load_chain_lineage_path,
    load_lineage_snapshots_for_hashes,
};
use super::types::ChainLineageBlock;

/// Walk a stored branch from `from_hash` through parent links and mark each row
/// `orphaned` until `stop_before_hash` is reached.
pub async fn mark_chain_lineage_range_orphaned(
    pool: &PgPool,
    chain_id: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<Vec<ChainLineageBlock>> {
    if stop_before_hash == Some(from_hash) {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for lineage orphaning")?;

    let path = load_chain_lineage_path(&mut *transaction, chain_id, from_hash, stop_before_hash)
        .await
        .with_context(|| {
            format!(
                "failed to load lineage path for chain {chain_id} starting from block {from_hash}"
            )
        })?;
    if path.is_empty() {
        bail!("missing stored lineage row for chain {chain_id} block {from_hash}");
    }
    ensure_chain_lineage_path_reaches_stop(chain_id, from_hash, stop_before_hash, &path)?;

    let block_hashes = path
        .iter()
        .map(|block| block.block_hash.clone())
        .collect::<Vec<_>>();
    sqlx::query(
        r#"
        UPDATE chain_lineage
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
        format!("failed to mark orphaned lineage range for chain {chain_id} from block {from_hash}")
    })?;

    let snapshots = load_lineage_snapshots_for_hashes(&mut *transaction, chain_id, &block_hashes)
        .await
        .with_context(|| {
            format!(
                "failed to load orphaned lineage range for chain {chain_id} starting from block {from_hash}"
            )
        })?;

    transaction
        .commit()
        .await
        .context("failed to commit lineage orphaning")?;

    Ok(snapshots)
}
