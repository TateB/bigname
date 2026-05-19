use anyhow::{Context, Result, bail};
use sqlx::PgPool;
use sqlx::{Executor, Postgres, Row, postgres::PgRow};

use crate::lineage::{
    CanonicalityState, chain_lineage_contains_ancestor_internal, ensure_chain_lineage_block,
    promote_chain_lineage_path,
};

mod rewind;
pub use rewind::rewind_chain_checkpoints_to_ancestor;

/// Persisted checkpoint row for one watched chain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainCheckpoint {
    pub chain_id: String,
    pub canonical_block_hash: Option<String>,
    pub canonical_block_number: Option<i64>,
    pub safe_block_hash: Option<String>,
    pub safe_block_number: Option<i64>,
    pub finalized_block_hash: Option<String>,
    pub finalized_block_number: Option<i64>,
}

/// Hash-first reference for one persisted checkpoint target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointBlockRef {
    pub block_hash: String,
    pub block_number: i64,
}

/// Monotonic checkpoint advancement request for one chain.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChainCheckpointUpdate {
    pub chain_id: String,
    pub canonical: Option<CheckpointBlockRef>,
    pub safe: Option<CheckpointBlockRef>,
    pub finalized: Option<CheckpointBlockRef>,
}

impl ChainCheckpointUpdate {
    fn is_no_op(&self) -> bool {
        self.canonical.is_none() && self.safe.is_none() && self.finalized.is_none()
    }
}

/// Insert missing `chain_checkpoints` rows and return sorted snapshots for the
/// requested chain IDs. Existing rows remain untouched, and omitted chain IDs
/// are not deleted when the watched set shrinks.
pub async fn sync_chain_checkpoints(
    pool: &PgPool,
    chain_ids: &[String],
) -> Result<Vec<ChainCheckpoint>> {
    let chain_ids = collect_chain_ids(chain_ids);
    if chain_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for chain checkpoint sync")?;
    ensure_chain_checkpoint_rows(&mut *transaction, &chain_ids).await?;
    let checkpoints = load_chain_checkpoints(&mut *transaction, &chain_ids).await?;
    transaction
        .commit()
        .await
        .context("failed to commit chain checkpoint sync")?;

    Ok(checkpoints)
}

/// Load one persisted checkpoint row without mutating the watched-chain set.
pub async fn load_chain_checkpoint(
    pool: &PgPool,
    chain_id: &str,
) -> Result<Option<ChainCheckpoint>> {
    let row = sqlx::query(
        r#"
        SELECT
            chain_id,
            canonical_block_hash,
            canonical_block_number,
            safe_block_hash,
            safe_block_number,
            finalized_block_hash,
            finalized_block_number
        FROM chain_checkpoints
        WHERE chain_id = $1
        "#,
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to load checkpoint row for chain {chain_id}"))?;

    row.map(decode_snapshot).transpose()
}

/// Load persisted checkpoint rows for the requested chain IDs without inserting
/// missing rows. Duplicate chain IDs collapse and returned rows are sorted.
pub async fn load_chain_checkpoint_snapshots(
    pool: &PgPool,
    chain_ids: &[String],
) -> Result<Vec<ChainCheckpoint>> {
    let chain_ids = collect_chain_ids(chain_ids);
    if chain_ids.is_empty() {
        return Ok(Vec::new());
    }

    load_chain_checkpoints(pool, &chain_ids).await
}

/// Advance persisted checkpoint pointers for one chain and promote stored
/// lineage rows along the admitted ancestry path.
pub async fn advance_chain_checkpoints(
    pool: &PgPool,
    update: &ChainCheckpointUpdate,
) -> Result<ChainCheckpoint> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for checkpoint advancement")?;

    ensure_chain_checkpoint_rows(&mut *transaction, std::slice::from_ref(&update.chain_id)).await?;
    let current = load_chain_checkpoint_for_update(&mut transaction, &update.chain_id).await?;
    validate_checkpoint_update(&current, update)?;

    if let Some(canonical) = &update.canonical {
        ensure_chain_lineage_block(
            &mut transaction,
            &update.chain_id,
            &canonical.block_hash,
            canonical.block_number,
        )
        .await?;
    }
    if let Some(safe) = &update.safe {
        ensure_chain_lineage_block(
            &mut transaction,
            &update.chain_id,
            &safe.block_hash,
            safe.block_number,
        )
        .await?;
    }
    if let Some(finalized) = &update.finalized {
        ensure_chain_lineage_block(
            &mut transaction,
            &update.chain_id,
            &finalized.block_hash,
            finalized.block_number,
        )
        .await?;
    }

    if let Some(canonical) = &update.canonical {
        let stop_before_hash = current.canonical_block_hash.as_deref();
        if checkpoint_target_needs_promotion(stop_before_hash, canonical) {
            promote_chain_lineage_path(
                &mut transaction,
                &update.chain_id,
                &canonical.block_hash,
                stop_before_hash,
                CanonicalityState::Canonical,
                false,
            )
            .await?;
        }
    }
    let current_canonical = current
        .canonical_block_hash
        .as_ref()
        .zip(current.canonical_block_number)
        .map(|(block_hash, block_number)| CheckpointBlockRef {
            block_hash: block_hash.clone(),
            block_number,
        });
    let effective_canonical = update.canonical.as_ref().or(current_canonical.as_ref());
    if let Some(safe) = &update.safe {
        ensure_checkpoint_target_on_canonical_branch(
            &mut transaction,
            &update.chain_id,
            "safe",
            effective_canonical,
            safe,
        )
        .await?;
        let stop_before_hash = current.safe_block_hash.as_deref();
        if checkpoint_target_needs_promotion(stop_before_hash, safe) {
            promote_chain_lineage_path(
                &mut transaction,
                &update.chain_id,
                &safe.block_hash,
                stop_before_hash,
                CanonicalityState::Safe,
                stop_before_hash.is_some(),
            )
            .await?;
        }
    }
    if let Some(finalized) = &update.finalized {
        ensure_checkpoint_target_on_canonical_branch(
            &mut transaction,
            &update.chain_id,
            "finalized",
            effective_canonical,
            finalized,
        )
        .await?;
        let stop_before_hash = current.finalized_block_hash.as_deref();
        if checkpoint_target_needs_promotion(stop_before_hash, finalized) {
            promote_chain_lineage_path(
                &mut transaction,
                &update.chain_id,
                &finalized.block_hash,
                stop_before_hash,
                CanonicalityState::Finalized,
                stop_before_hash.is_some(),
            )
            .await?;
        }
    }

    let checkpoint = if update.is_no_op() {
        current
    } else {
        write_chain_checkpoint_update(&mut transaction, update).await?
    };

    transaction
        .commit()
        .await
        .context("failed to commit checkpoint advancement")?;

    Ok(checkpoint)
}

fn checkpoint_target_needs_promotion(
    current_hash: Option<&str>,
    target: &CheckpointBlockRef,
) -> bool {
    current_hash != Some(target.block_hash.as_str())
}

async fn ensure_checkpoint_target_on_canonical_branch(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    chain_id: &str,
    checkpoint_name: &str,
    canonical: Option<&CheckpointBlockRef>,
    target: &CheckpointBlockRef,
) -> Result<()> {
    let Some(canonical) = canonical else {
        return Ok(());
    };
    if canonical.block_hash == target.block_hash {
        return Ok(());
    }
    if target.block_number > canonical.block_number {
        bail!(
            "{checkpoint_name} checkpoint for chain {chain_id} block {} at {} is ahead of canonical branch head {} at {}",
            target.block_hash,
            target.block_number,
            canonical.block_hash,
            canonical.block_number
        );
    }

    if chain_lineage_contains_ancestor_internal(
        &mut **transaction,
        chain_id,
        &canonical.block_hash,
        &target.block_hash,
    )
    .await?
    {
        return Ok(());
    }

    bail!(
        "{checkpoint_name} checkpoint for chain {chain_id} block {} is not on the canonical branch ending at {}",
        target.block_hash,
        canonical.block_hash
    )
}

pub(super) async fn ensure_chain_checkpoint_rows<'e, E>(
    executor: E,
    chain_ids: &[String],
) -> Result<u64>
where
    E: Executor<'e, Database = Postgres>,
{
    let result = sqlx::query(
        r#"
        INSERT INTO chain_checkpoints (chain_id)
        SELECT DISTINCT candidate.chain_id
        FROM UNNEST($1::TEXT[]) AS candidate(chain_id)
        ON CONFLICT (chain_id) DO NOTHING
        "#,
    )
    .bind(chain_ids)
    .execute(executor)
    .await
    .context("failed to ensure chain checkpoint rows")?;

    Ok(result.rows_affected())
}

async fn load_chain_checkpoints<'e, E>(
    executor: E,
    chain_ids: &[String],
) -> Result<Vec<ChainCheckpoint>>
where
    E: Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(
        r#"
        SELECT
            chain_id,
            canonical_block_hash,
            canonical_block_number,
            safe_block_hash,
            safe_block_number,
            finalized_block_hash,
            finalized_block_number
        FROM chain_checkpoints
        WHERE chain_id = ANY($1::TEXT[])
        ORDER BY chain_id
        "#,
    )
    .bind(chain_ids)
    .fetch_all(executor)
    .await
    .context("failed to load chain checkpoint snapshots")?;

    rows.into_iter().map(decode_snapshot).collect()
}

async fn load_chain_checkpoint_for_update(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<ChainCheckpoint> {
    let row = sqlx::query(
        r#"
        SELECT
            chain_id,
            canonical_block_hash,
            canonical_block_number,
            safe_block_hash,
            safe_block_number,
            finalized_block_hash,
            finalized_block_number
        FROM chain_checkpoints
        WHERE chain_id = $1
        FOR UPDATE
        "#,
    )
    .bind(chain_id)
    .fetch_one(&mut **executor)
    .await
    .with_context(|| format!("failed to load checkpoint row for chain {chain_id}"))?;

    decode_snapshot(row)
}

async fn write_chain_checkpoint_update(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    update: &ChainCheckpointUpdate,
) -> Result<ChainCheckpoint> {
    let row = sqlx::query(
        r#"
        UPDATE chain_checkpoints
        SET
            canonical_block_hash = COALESCE($2, canonical_block_hash),
            canonical_block_number = COALESCE($3, canonical_block_number),
            safe_block_hash = COALESCE($4, safe_block_hash),
            safe_block_number = COALESCE($5, safe_block_number),
            finalized_block_hash = COALESCE($6, finalized_block_hash),
            finalized_block_number = COALESCE($7, finalized_block_number),
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
    .bind(&update.chain_id)
    .bind(
        update
            .canonical
            .as_ref()
            .map(|block| block.block_hash.as_str()),
    )
    .bind(update.canonical.as_ref().map(|block| block.block_number))
    .bind(update.safe.as_ref().map(|block| block.block_hash.as_str()))
    .bind(update.safe.as_ref().map(|block| block.block_number))
    .bind(
        update
            .finalized
            .as_ref()
            .map(|block| block.block_hash.as_str()),
    )
    .bind(update.finalized.as_ref().map(|block| block.block_number))
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to update checkpoint row for chain {}",
            update.chain_id
        )
    })?;

    decode_snapshot(row)
}

fn collect_chain_ids(chain_ids: &[String]) -> Vec<String> {
    let mut chain_ids = chain_ids.to_vec();
    chain_ids.sort();
    chain_ids.dedup();
    chain_ids
}

pub(super) fn decode_snapshot(row: PgRow) -> Result<ChainCheckpoint> {
    let chain_id = row
        .try_get::<String, _>("chain_id")
        .context("failed to decode chain checkpoint chain_id")?;

    let canonical_block_hash = row
        .try_get::<Option<String>, _>("canonical_block_hash")
        .context("failed to decode canonical checkpoint hash")?;
    let canonical_block_number = row
        .try_get::<Option<i64>, _>("canonical_block_number")
        .context("failed to decode canonical checkpoint number")?;
    validate_checkpoint_pair(
        &chain_id,
        "canonical",
        canonical_block_hash.as_ref(),
        canonical_block_number,
    )?;

    let safe_block_hash = row
        .try_get::<Option<String>, _>("safe_block_hash")
        .context("failed to decode safe checkpoint hash")?;
    let safe_block_number = row
        .try_get::<Option<i64>, _>("safe_block_number")
        .context("failed to decode safe checkpoint number")?;
    validate_checkpoint_pair(
        &chain_id,
        "safe",
        safe_block_hash.as_ref(),
        safe_block_number,
    )?;

    let finalized_block_hash = row
        .try_get::<Option<String>, _>("finalized_block_hash")
        .context("failed to decode finalized checkpoint hash")?;
    let finalized_block_number = row
        .try_get::<Option<i64>, _>("finalized_block_number")
        .context("failed to decode finalized checkpoint number")?;
    validate_checkpoint_pair(
        &chain_id,
        "finalized",
        finalized_block_hash.as_ref(),
        finalized_block_number,
    )?;

    Ok(ChainCheckpoint {
        chain_id: chain_id.clone(),
        canonical_block_hash,
        canonical_block_number,
        safe_block_hash,
        safe_block_number,
        finalized_block_hash,
        finalized_block_number,
    })
}

fn validate_checkpoint_update(
    current: &ChainCheckpoint,
    update: &ChainCheckpointUpdate,
) -> Result<()> {
    if let Some(canonical) = &update.canonical {
        validate_checkpoint_target(&update.chain_id, "canonical", canonical)?;
    }
    if let Some(safe) = &update.safe {
        validate_checkpoint_target(&update.chain_id, "safe", safe)?;
        validate_monotonic_checkpoint_target(
            &update.chain_id,
            "safe",
            current.safe_block_hash.as_ref(),
            current.safe_block_number,
            safe,
        )?;
    }
    if let Some(finalized) = &update.finalized {
        validate_checkpoint_target(&update.chain_id, "finalized", finalized)?;
        validate_monotonic_checkpoint_target(
            &update.chain_id,
            "finalized",
            current.finalized_block_hash.as_ref(),
            current.finalized_block_number,
            finalized,
        )?;
    }

    Ok(())
}

fn validate_checkpoint_target(
    chain_id: &str,
    checkpoint_name: &str,
    target: &CheckpointBlockRef,
) -> Result<()> {
    if target.block_number < 0 {
        bail!(
            "{checkpoint_name} checkpoint for chain {chain_id} has negative block number {}",
            target.block_number
        );
    }

    Ok(())
}

fn validate_monotonic_checkpoint_target(
    chain_id: &str,
    checkpoint_name: &str,
    current_hash: Option<&String>,
    current_number: Option<i64>,
    next: &CheckpointBlockRef,
) -> Result<()> {
    if let Some(current_number) = current_number {
        if next.block_number < current_number {
            bail!(
                "{checkpoint_name} checkpoint for chain {chain_id} cannot move backward from block {current_number} to {}",
                next.block_number
            );
        }

        if let Some(current_hash) = current_hash
            && next.block_number == current_number
            && current_hash != &next.block_hash
        {
            bail!(
                "{checkpoint_name} checkpoint for chain {chain_id} cannot switch hash at block number {} from {} to {}",
                current_number,
                current_hash,
                next.block_hash
            );
        }
    }

    Ok(())
}

fn validate_checkpoint_pair(
    chain_id: &str,
    checkpoint_name: &str,
    hash: Option<&String>,
    block_number: Option<i64>,
) -> Result<()> {
    match (hash, block_number) {
        (Some(_), Some(_)) | (None, None) => Ok(()),
        _ => bail!(
            "stored {checkpoint_name} checkpoint for chain {chain_id} has mismatched hash and number"
        ),
    }
}

#[cfg(test)]
mod tests;
