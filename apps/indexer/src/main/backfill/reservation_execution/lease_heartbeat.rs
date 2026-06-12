use std::{future::Future, time::Duration};

use anyhow::{Context, Result, bail};
use bigname_storage::{BackfillRange, advance_backfill_range};

use crate::backfill::BackfillJobRunConfig;

pub(crate) async fn run_with_backfill_lease_heartbeat<T, F>(
    pool: &sqlx::PgPool,
    active_range: &BackfillRange,
    config: &BackfillJobRunConfig,
    future: F,
) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    let lease_duration_secs = active_range_lease_duration_secs(active_range)?;
    let heartbeat_interval = Duration::from_secs((lease_duration_secs / 2).max(1) as u64);
    refresh_backfill_range_lease(pool, active_range, config, "before range work").await?;

    tokio::pin!(future);
    let heartbeat_sleep = tokio::time::sleep(heartbeat_interval);
    tokio::pin!(heartbeat_sleep);
    loop {
        tokio::select! {
            result = &mut future => return result,
            _ = &mut heartbeat_sleep => {
                refresh_backfill_range_lease(pool, active_range, config, "during range work").await?;
                heartbeat_sleep
                    .as_mut()
                    .reset(tokio::time::Instant::now() + heartbeat_interval);
            }
        }
    }
}

async fn refresh_backfill_range_lease(
    pool: &sqlx::PgPool,
    active_range: &BackfillRange,
    config: &BackfillJobRunConfig,
    phase: &str,
) -> Result<()> {
    advance_backfill_range(
        pool,
        active_range.backfill_range_id,
        &config.lease_token,
        active_range.checkpoint_block_number,
    )
    .await
    .with_context(|| format!("failed to refresh backfill range lease {phase}"))?;
    Ok(())
}

fn active_range_lease_duration_secs(active_range: &BackfillRange) -> Result<i64> {
    let lease_expires_at = active_range
        .lease_expires_at
        .context("backfill range has no active lease deadline")?;
    let duration_secs = lease_expires_at
        .unix_timestamp()
        .checked_sub(active_range.updated_at.unix_timestamp())
        .context("backfill lease duration timestamp underflowed")?;
    Ok(duration_secs.max(1))
}

pub(crate) fn validate_hash_pinned_chunk_blocks(chunk_blocks: i64) -> Result<()> {
    if chunk_blocks <= 0 {
        bail!("hash-pinned backfill chunk blocks must be positive, got {chunk_blocks}");
    }

    Ok(())
}
