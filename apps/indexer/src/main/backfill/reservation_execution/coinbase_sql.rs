use anyhow::{Context, Result, bail};
use bigname_manifests::WatchedSourceSelectorPlan;
use bigname_storage::{
    BackfillLifecycleStatus, BackfillRange, advance_backfill_range, complete_backfill_range,
    load_backfill_job, reserve_backfill_range,
};
use tracing::{info, warn};

use crate::provider::ChainProviderOps;

use super::{
    backfill_lease_duration_secs, create_coinbase_sql_backfill_job,
    refreshed_backfill_lease_expires_at,
};
use crate::backfill::{
    BackfillBlockRange, BackfillJobRunConfig, BackfillJobRunOutcome, BackfillOutcome,
    BackfillTopicPlan, CoinbaseSqlBackfillConfig, HistoricalBackfillSourceOps,
    HistoricalLogPayloadRequest,
    coinbase_sql::load_backfill_topic_plan,
    failure_recording::{ReservedRangeFailure, record_reserved_range_failure},
    fetching::{
        BackfillCanonicalityEvidence, load_backfill_canonicality_evidence,
        materialize_historical_payload_range,
    },
    range_resolution::resolve_backfill_range,
    selection::{SelectedTargetIntervalIndex, SelectedTargetRangeCursor},
};

pub(crate) async fn run_resumable_coinbase_sql_backfill_job(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    validation_provider: &(impl ChainProviderOps + ?Sized),
    historical_source: &(impl HistoricalBackfillSourceOps + ?Sized),
    mut config: BackfillJobRunConfig,
    coinbase_config: CoinbaseSqlBackfillConfig,
) -> Result<BackfillJobRunOutcome> {
    config.adapter_sync_mode = config.adapter_sync_mode.hash_pinned_backfill_mode();
    coinbase_config.validate()?;
    let watched_chain = &source_plan.watched_chain_plan;
    let topic_plan = load_backfill_topic_plan(pool, source_plan).await?;
    let record =
        create_coinbase_sql_backfill_job(pool, source_plan, &config, &coinbase_config, &topic_plan)
            .await?;
    let mut outcome = BackfillJobRunOutcome::new(record.job.backfill_job_id, source_plan, &config);
    let lease_duration_secs = backfill_lease_duration_secs(config.lease_expires_at)?;

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
        "resumable Coinbase SQL backfill job loaded"
    );

    loop {
        let Some(reserved_range) = reserve_backfill_range(
            pool,
            record.job.backfill_job_id,
            &config.lease_owner,
            &config.lease_token,
            refreshed_backfill_lease_expires_at(lease_duration_secs)?,
        )
        .await?
        else {
            break;
        };

        outcome.reserved_range_count += 1;
        run_reserved_coinbase_sql_backfill_range(
            pool,
            source_plan,
            validation_provider,
            historical_source,
            &config,
            &coinbase_config,
            &topic_plan,
            &reserved_range,
            &mut outcome,
        )
        .await?;
        outcome.completed_range_count += 1;
    }

    let job = load_backfill_job(pool, record.job.backfill_job_id)
        .await?
        .with_context(|| format!("missing backfill job {}", record.job.backfill_job_id))?;
    if job.status == BackfillLifecycleStatus::Completed {
        info!(
            service = "indexer",
            command = "backfill",
            backfill_job_id = outcome.backfill_job_id,
            chain = %outcome.chain,
            from_block = outcome.from_block,
            to_block = outcome.to_block,
            idempotency_key = %outcome.idempotency_key,
            adapter_sync_mode = config.adapter_sync_mode.as_str(),
            reserved_range_count = outcome.reserved_range_count,
            completed_range_count = outcome.completed_range_count,
            resolved_block_count = outcome.resolved_block_count,
            raw_block_count = outcome.raw_block_count,
            raw_transaction_count = outcome.raw_transaction_count,
            raw_receipt_count = outcome.raw_receipt_count,
            raw_log_count = outcome.raw_log_count,
            raw_code_hash_count = outcome.raw_code_hash_count,
            "resumable Coinbase SQL backfill job completed"
        );
        return Ok(outcome);
    }

    bail!(
        "backfill job {} has no reservable ranges but is {}; another active lease may still own work",
        record.job.backfill_job_id,
        job.status.as_str()
    );
}

async fn run_reserved_coinbase_sql_backfill_range(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    validation_provider: &(impl ChainProviderOps + ?Sized),
    historical_source: &(impl HistoricalBackfillSourceOps + ?Sized),
    config: &BackfillJobRunConfig,
    coinbase_config: &CoinbaseSqlBackfillConfig,
    topic_plan: &BackfillTopicPlan,
    reserved_range: &BackfillRange,
    aggregate: &mut BackfillJobRunOutcome,
) -> Result<()> {
    let mut active_range = reserved_range.clone();
    let mut block_number = active_range
        .checkpoint_block_number
        .checked_add(1)
        .context("backfill checkpoint overflowed while computing Coinbase SQL resume block")?;
    let mut window_blocks = coinbase_config.initial_window_blocks;
    let selected_target_index = SelectedTargetIntervalIndex::from_source_plan(source_plan);
    let mut selected_target_range_cursor = SelectedTargetRangeCursor::from_source_plan(source_plan);
    let canonicality_evidence = match load_backfill_canonicality_evidence(
        pool,
        &source_plan.watched_chain_plan.chain,
        validation_provider,
    )
    .await
    {
        Ok(evidence) => evidence,
        Err(error) => {
            return Err(record_reserved_range_failure(ReservedRangeFailure {
                pool,
                reserved_range: &active_range,
                config,
                failure_reason: "Coinbase SQL validation canonicality evidence load failed",
                block_number: Some(block_number),
                attempted_range: None,
                phase: "canonicality_evidence",
                error,
            })
            .await);
        }
    };

    while block_number <= active_range.range_end_block_number {
        let window_end = block_number
            .checked_add(window_blocks - 1)
            .unwrap_or(active_range.range_end_block_number)
            .min(active_range.range_end_block_number);
        let window_range = BackfillBlockRange::new(block_number, window_end)?;
        let selected_target_addresses_for_chunk = selected_target_range_cursor
            .active_addresses_for_monotonic_range(window_range.from_block, window_range.to_block);

        let window_outcome = match run_coinbase_sql_backfill_window(
            pool,
            source_plan,
            &selected_target_index,
            &selected_target_addresses_for_chunk,
            validation_provider,
            historical_source,
            topic_plan,
            window_range,
            canonicality_evidence.clone(),
            config,
            coinbase_config,
        )
        .await
        {
            Ok(outcome) => {
                window_blocks = next_coinbase_sql_window_blocks(
                    window_blocks,
                    coinbase_config,
                    outcome.raw_log_count,
                );
                outcome
            }
            Err(error) => {
                if window_blocks > 1 {
                    let next_window_blocks = (window_blocks / 2).max(1);
                    warn!(
                        service = "indexer",
                        command = "backfill",
                        chain = %source_plan.watched_chain_plan.chain,
                        block_number,
                        attempted_from_block = window_range.from_block,
                        attempted_to_block = window_range.to_block,
                        previous_window_blocks = window_blocks,
                        next_window_blocks,
                        error = %format!("{error:#}"),
                        "Coinbase SQL backfill window failed; retrying with a smaller window before failing the range"
                    );
                    window_blocks = next_window_blocks;
                    continue;
                }
                return Err(record_reserved_range_failure(ReservedRangeFailure {
                    pool,
                    reserved_range: &active_range,
                    config,
                    failure_reason: "Coinbase SQL backfill failed",
                    block_number: Some(block_number),
                    attempted_range: Some(window_range),
                    phase: "coinbase_sql_intake",
                    error,
                })
                .await);
            }
        };
        aggregate.add_range_outcome(&window_outcome);

        active_range = match advance_backfill_range(
            pool,
            active_range.backfill_range_id,
            &config.lease_token,
            window_end,
        )
        .await
        {
            Ok(range) => range,
            Err(error) => {
                return Err(record_reserved_range_failure(ReservedRangeFailure {
                    pool,
                    reserved_range: &active_range,
                    config,
                    failure_reason: "Coinbase SQL backfill checkpoint advance failed",
                    block_number: Some(block_number),
                    attempted_range: Some(window_range),
                    phase: "checkpoint_advance",
                    error,
                })
                .await);
            }
        };

        if window_end == active_range.range_end_block_number {
            break;
        }
        block_number = window_end
            .checked_add(1)
            .context("Coinbase SQL backfill block number overflowed while advancing range")?;
    }

    if let Err(error) =
        complete_backfill_range(pool, active_range.backfill_range_id, &config.lease_token).await
    {
        return Err(record_reserved_range_failure(ReservedRangeFailure {
            pool,
            reserved_range: &active_range,
            config,
            failure_reason: "Coinbase SQL backfill range completion failed",
            block_number: None,
            attempted_range: None,
            phase: "range_completion",
            error,
        })
        .await);
    }

    Ok(())
}

fn next_coinbase_sql_window_blocks(
    current_window_blocks: i64,
    coinbase_config: &CoinbaseSqlBackfillConfig,
    raw_log_count: usize,
) -> i64 {
    if raw_log_count < 10_000 {
        current_window_blocks
            .saturating_mul(2)
            .min(coinbase_config.max_window_blocks)
            .max(1)
    } else if raw_log_count >= coinbase_config.page_limit.saturating_sub(5_000) {
        (current_window_blocks / 2).max(1)
    } else {
        current_window_blocks
    }
}

async fn run_coinbase_sql_backfill_window(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    selected_target_index: &SelectedTargetIntervalIndex,
    selected_target_addresses_for_chunk: &[String],
    validation_provider: &(impl ChainProviderOps + ?Sized),
    historical_source: &(impl HistoricalBackfillSourceOps + ?Sized),
    topic_plan: &BackfillTopicPlan,
    range: BackfillBlockRange,
    canonicality_evidence: BackfillCanonicalityEvidence,
    config: &BackfillJobRunConfig,
    coinbase_config: &CoinbaseSqlBackfillConfig,
) -> Result<BackfillOutcome> {
    let resolved_blocks = resolve_backfill_range(validation_provider, range).await?;
    let block_headers = validation_provider
        .fetch_block_headers_by_hashes(&resolved_blocks)
        .await
        .with_context(|| {
            format!(
                "failed to fetch validation provider headers for Coinbase SQL range {}..={}",
                range.from_block, range.to_block
            )
        })?;
    let historical_payload = historical_source
        .fetch_selected_log_payloads(HistoricalLogPayloadRequest {
            chain: &source_plan.watched_chain_plan.chain,
            source_plan,
            selected_target_index,
            resolved_blocks: &resolved_blocks,
            selected_target_addresses_for_chunk,
            topic_plan,
            range,
            validation_mode: coinbase_config.validation_mode,
        })
        .await?;
    materialize_historical_payload_range(
        pool,
        source_plan,
        selected_target_index,
        validation_provider,
        range,
        canonicality_evidence,
        &resolved_blocks,
        block_headers,
        historical_payload,
        config.adapter_sync_mode,
        config.header_audit_mode,
    )
    .await
}
