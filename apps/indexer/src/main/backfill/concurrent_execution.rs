use anyhow::{Context, Result, bail};
use bigname_manifests::WatchedSourceSelectorPlan;
use bigname_storage::{
    BackfillLifecycleStatus, BackfillRangeSpec, load_backfill_job, reserve_backfill_range,
};
use tokio::task::JoinSet;
use tracing::info;

use crate::provider::ChainProvider;

use super::{
    BackfillJobRunConfig, BackfillJobRunOutcome, BackfillTopicPlan, CoinbaseSqlBackfillConfig,
    HistoricalBackfillSourceOps,
    coinbase_sql::load_backfill_topic_plan,
    reservation_execution::{
        backfill_lease_duration_secs, create_coinbase_sql_backfill_job_with_ranges,
        create_hash_pinned_backfill_job_with_ranges,
        ensure_coinbase_sql_registry_range_start_is_replay_safe,
        refreshed_backfill_lease_expires_at, run_reserved_coinbase_sql_backfill_range,
        run_reserved_hash_pinned_backfill_range, validate_hash_pinned_chunk_blocks,
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

pub(crate) async fn run_resumable_coinbase_sql_backfill_job_concurrently<
    H: HistoricalBackfillSourceOps + Clone + Send + Sync + 'static,
>(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    provider: &ChainProvider,
    historical_source: &H,
    mut config: BackfillJobRunConfig,
    coinbase_config: CoinbaseSqlBackfillConfig,
    ranges: Vec<BackfillRangeSpec>,
    worker_count: usize,
) -> Result<BackfillJobRunOutcome> {
    if worker_count == 0 {
        bail!("Coinbase SQL backfill worker count must be positive");
    }
    config.adapter_sync_mode = config.adapter_sync_mode.hash_pinned_backfill_mode();
    coinbase_config.validate()?;
    let topic_plan = load_backfill_topic_plan(pool, source_plan).await?;
    ensure_coinbase_sql_registry_range_start_is_replay_safe(
        source_plan,
        &topic_plan,
        config.range,
    )?;
    let watched_chain = &source_plan.watched_chain_plan;
    let record = create_coinbase_sql_backfill_job_with_ranges(
        pool,
        source_plan,
        &config,
        &coinbase_config,
        &topic_plan,
        ranges,
    )
    .await?;
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
        coinbase_sql_initial_window_blocks = coinbase_config.initial_window_blocks,
        coinbase_sql_max_window_blocks = coinbase_config.max_window_blocks,
        coinbase_sql_page_limit = coinbase_config.page_limit,
        coinbase_sql_query_char_limit = coinbase_config.sql_char_limit,
        coinbase_sql_validation_mode = coinbase_config.validation_mode.as_str(),
        adapter_sync_mode = config.adapter_sync_mode.as_str(),
        header_audit_mode = config.header_audit_mode.as_str(),
        range_count = record.ranges.len(),
        requested_worker_count = worker_count,
        active_worker_count,
        "resumable Coinbase SQL backfill job loaded for concurrent range workers"
    );

    let mut workers = JoinSet::new();
    let backfill_job_id = record.job.backfill_job_id;
    for worker_index in 0..active_worker_count {
        let pool = pool.clone();
        let source_plan = source_plan.clone();
        let provider = provider.clone();
        let historical_source = historical_source.clone();
        let coinbase_config = coinbase_config.clone();
        let topic_plan: BackfillTopicPlan = topic_plan.clone();
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
                run_reserved_coinbase_sql_backfill_range(
                    &pool,
                    &source_plan,
                    &provider,
                    &historical_source,
                    &worker_config,
                    &coinbase_config,
                    &topic_plan,
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
                return Err(error).context("Coinbase SQL backfill worker task failed");
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
            "resumable Coinbase SQL backfill job completed"
        );
        return Ok(aggregate);
    }

    bail!(
        "backfill job {} has no reservable ranges but is {}; another active lease may still own work",
        record.job.backfill_job_id,
        job.status.as_str()
    );
}
