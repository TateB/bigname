use anyhow::{Context, Result};
use sqlx::PgPool;

use super::{
    DiscoveryEdgeMutation, EnsV1SubregistryDiscoverySyncSummary, ReplayAdapterCheckpointContext,
    SUBREGISTRY_CHECKPOINT_PAGE_LIMIT, sync_ens_v1_subregistry_discovery_with_scope,
};

pub async fn sync_ens_v1_subregistry_discovery_with_replay_checkpoint(
    pool: &PgPool,
    chain: &str,
    checkpoint: &ReplayAdapterCheckpointContext,
) -> Result<EnsV1SubregistryDiscoverySyncSummary> {
    sync_ens_v1_subregistry_discovery_with_replay_checkpoint_and_log_limit(
        pool,
        chain,
        checkpoint,
        usize::try_from(SUBREGISTRY_CHECKPOINT_PAGE_LIMIT)?,
    )
    .await
}

pub async fn sync_ens_v1_subregistry_discovery_with_replay_checkpoint_and_log_limit(
    pool: &PgPool,
    chain: &str,
    checkpoint: &ReplayAdapterCheckpointContext,
    max_raw_logs_per_page: usize,
) -> Result<EnsV1SubregistryDiscoverySyncSummary> {
    let checkpoint_page_limit = i64::try_from(max_raw_logs_per_page.max(1))
        .context("subregistry checkpoint page limit overflowed i64")?;
    loop {
        let (summary, repeat_checkpoint_replay) = sync_ens_v1_subregistry_discovery_with_scope(
            pool,
            chain,
            false,
            &[],
            None,
            DiscoveryEdgeMutation::Reconcile,
            Some(checkpoint),
            checkpoint_page_limit,
        )
        .await?;
        if !repeat_checkpoint_replay {
            return Ok(summary);
        }
    }
}
