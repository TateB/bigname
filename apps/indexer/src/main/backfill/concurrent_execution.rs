use anyhow::{Context, Result, bail};
use bigname_manifests::WatchedSourceSelectorPlan;
use bigname_storage::{
    BackfillLifecycleStatus, BackfillRangeSpec, load_backfill_job, reserve_backfill_range,
};
use tokio::task::JoinSet;
use tracing::info;

use crate::provider::ChainProvider;

use super::{
    BackfillJobRunConfig, BackfillJobRunOutcome,
    reservation_execution::{
        backfill_lease_duration_secs, create_hash_pinned_backfill_job_with_ranges,
        refreshed_backfill_lease_expires_at, run_reserved_hash_pinned_backfill_range,
        validate_hash_pinned_chunk_blocks,
    },
};

pub(crate) async fn run_resumable_hash_pinned_backfill_job_concurrently(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    provider: &ChainProvider,
    mut config: BackfillJobRunConfig,
    ranges: Vec<BackfillRangeSpec>,
    worker_count: usize,
) -> Result<BackfillJobRunOutcome> {
    if worker_count == 0 {
        bail!("hash-pinned backfill worker count must be positive");
    }
    config.adapter_sync_mode = config.adapter_sync_mode.hash_pinned_backfill_mode();
    validate_hash_pinned_chunk_blocks(config.hash_pinned_chunk_blocks)?;
    let watched_chain = &source_plan.watched_chain_plan;
    let record =
        create_hash_pinned_backfill_job_with_ranges(pool, source_plan, &config, ranges).await?;
    let mut aggregate =
        BackfillJobRunOutcome::new(record.job.backfill_job_id, source_plan, &config);
    let lease_duration_secs = backfill_lease_duration_secs(config.lease_expires_at)?;
    let active_worker_count = worker_count.min(record.ranges.len().max(1));

    info!(
        service = "indexer",
        command = "backfill",
        backfill_job_id = record.job.backfill_job_id,
        backfill_job_status = record.job.status.as_str(),
        chain = %watched_chain.chain,
        selector_kind = source_plan.selector_kind.as_str(),
        selected_target_count = source_plan.selected_targets.len(),
        deployment_profile = %config.deployment_profile,
        from_block = config.range.from_block,
        to_block = config.range.to_block,
        idempotency_key = %config.idempotency_key,
        hash_pinned_chunk_blocks = config.hash_pinned_chunk_blocks,
        adapter_sync_mode = config.adapter_sync_mode.as_str(),
        header_audit_mode = config.header_audit_mode.as_str(),
        range_count = record.ranges.len(),
        requested_worker_count = worker_count,
        active_worker_count,
        "resumable backfill job loaded for concurrent range workers"
    );

    let mut workers = JoinSet::new();
    let backfill_job_id = record.job.backfill_job_id;
    for worker_index in 0..active_worker_count {
        let pool = pool.clone();
        let source_plan = source_plan.clone();
        let provider = provider.clone();
        let mut worker_config = config.clone();
        worker_config.lease_owner = format!("{}:worker-{worker_index}", config.lease_owner);
        worker_config.lease_token = format!("{}:worker-{worker_index}", config.lease_token);

        workers.spawn(async move {
            let mut outcome =
                BackfillJobRunOutcome::new(backfill_job_id, &source_plan, &worker_config);
            loop {
                let Some(reserved_range) = reserve_backfill_range(
                    &pool,
                    backfill_job_id,
                    &worker_config.lease_owner,
                    &worker_config.lease_token,
                    refreshed_backfill_lease_expires_at(lease_duration_secs)?,
                )
                .await?
                else {
                    break;
                };

                outcome.reserved_range_count += 1;
                run_reserved_hash_pinned_backfill_range(
                    &pool,
                    &source_plan,
                    &provider,
                    &worker_config,
                    &reserved_range,
                    &mut outcome,
                )
                .await?;
                outcome.completed_range_count += 1;
            }

            Ok::<_, anyhow::Error>(outcome)
        });
    }

    while let Some(result) = workers.join_next().await {
        let worker_outcome = match result {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(error)) => {
                workers.abort_all();
                return Err(error);
            }
            Err(error) => {
                workers.abort_all();
                return Err(error).context("hash-pinned backfill worker task failed");
            }
        };
        aggregate.reserved_range_count += worker_outcome.reserved_range_count;
        aggregate.completed_range_count += worker_outcome.completed_range_count;
        aggregate.resolved_block_count += worker_outcome.resolved_block_count;
        aggregate.raw_block_count += worker_outcome.raw_block_count;
        aggregate.raw_transaction_count += worker_outcome.raw_transaction_count;
        aggregate.raw_receipt_count += worker_outcome.raw_receipt_count;
        aggregate.raw_log_count += worker_outcome.raw_log_count;
        aggregate.raw_code_hash_count += worker_outcome.raw_code_hash_count;
    }

    let job = load_backfill_job(pool, record.job.backfill_job_id)
        .await?
        .with_context(|| format!("missing backfill job {}", record.job.backfill_job_id))?;
    if job.status == BackfillLifecycleStatus::Completed {
        info!(
            service = "indexer",
            command = "backfill",
            backfill_job_id = aggregate.backfill_job_id,
            chain = %aggregate.chain,
            from_block = aggregate.from_block,
            to_block = aggregate.to_block,
            idempotency_key = %aggregate.idempotency_key,
            hash_pinned_chunk_blocks = config.hash_pinned_chunk_blocks,
            adapter_sync_mode = config.adapter_sync_mode.as_str(),
            requested_worker_count = worker_count,
            active_worker_count,
            reserved_range_count = aggregate.reserved_range_count,
            completed_range_count = aggregate.completed_range_count,
            resolved_block_count = aggregate.resolved_block_count,
            raw_block_count = aggregate.raw_block_count,
            raw_transaction_count = aggregate.raw_transaction_count,
            raw_receipt_count = aggregate.raw_receipt_count,
            raw_log_count = aggregate.raw_log_count,
            raw_code_hash_count = aggregate.raw_code_hash_count,
            "resumable hash-pinned backfill job completed"
        );
        return Ok(aggregate);
    }

    bail!(
        "backfill job {} has no reservable ranges but is {}; another active lease may still own work",
        record.job.backfill_job_id,
        job.status.as_str()
    );
}
