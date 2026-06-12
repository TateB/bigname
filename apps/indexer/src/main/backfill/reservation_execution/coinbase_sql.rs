use super::{
    backfill_lease_duration_secs, create_coinbase_sql_backfill_job,
    refreshed_backfill_lease_expires_at, run_with_backfill_lease_heartbeat,
};
use crate::backfill::{
    BackfillBlockRange, BackfillJobRunConfig, BackfillJobRunOutcome, BackfillOutcome,
    BackfillTopicPlan, CoinbaseSqlBackfillConfig, CoinbaseSqlValidationMode,
    HistoricalBackfillSourceOps, HistoricalLogPayload, HistoricalLogPayloadRequest,
    coinbase_sql::load_backfill_topic_plan,
    failure_recording::{ReservedRangeFailure, record_reserved_range_failure},
    fetching::{
        BackfillCanonicalityEvidence, fill_log_payloads_from_validation_provider,
        load_backfill_canonicality_evidence, materialize_historical_payload_range,
    },
    range_resolution::{resolve_backfill_block_numbers, resolve_backfill_range},
    selection::{SelectedTargetIntervalIndex, SelectedTargetRangeCursor},
};
use crate::provider::{ChainProviderOps, ProviderLog, ProviderResolvedBlock};
use anyhow::{Context, Result, bail};
use bigname_manifests::WatchedSourceSelectorPlan;
use bigname_storage::{
    BackfillLifecycleStatus, BackfillRange, advance_backfill_range, complete_backfill_range,
    load_backfill_job, reserve_backfill_range,
};
use std::{collections::BTreeMap, time::Instant};
use tracing::{info, warn};
const MAX_COINBASE_SQL_SAMPLE_VALIDATION_BLOCKS: usize = 512;
const MAX_COINBASE_SQL_SAMPLE_PROVIDER_PAYLOAD_LOGS: usize = 2_000;
const MAX_COINBASE_SQL_SAMPLE_DECODED_PAYLOAD_LOGS: usize = 5_000;
const MAX_COINBASE_SQL_BASENAMES_REGISTRY_SAMPLE_DECODED_PAYLOAD_LOGS: usize = 50_000;
const MAX_COINBASE_SQL_BASENAMES_REGISTRAR_SAMPLE_DECODED_PAYLOAD_LOGS: usize = 15_000;
const MAX_COINBASE_SQL_PRACTICAL_WINDOW_BLOCKS: i64 = 65_536;
const BASENAMES_BASE_REGISTRY_SOURCE_FAMILY: &str = "basenames_base_registry";
const BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY: &str = "basenames_base_registrar";
const BASENAMES_BASE_RESOLVER_SOURCE_FAMILY: &str = "basenames_base_resolver";
pub(crate) async fn run_resumable_coinbase_sql_backfill_job(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    validation_provider: &(impl ChainProviderOps + ?Sized),
    historical_source: &(impl HistoricalBackfillSourceOps + ?Sized),
    mut config: BackfillJobRunConfig,
    coinbase_config: CoinbaseSqlBackfillConfig,
) -> Result<BackfillJobRunOutcome> {
    coinbase_config.validate()?;
    let watched_chain = &source_plan.watched_chain_plan;
    let topic_plan = load_backfill_topic_plan(pool, source_plan).await?;
    config.adapter_sync_mode = effective_coinbase_sql_adapter_sync_mode(
        source_plan,
        &topic_plan,
        config.adapter_sync_mode,
    );
    ensure_coinbase_sql_registry_range_start_is_replay_safe(
        source_plan,
        &topic_plan,
        config.range,
    )?;
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

pub(crate) async fn run_reserved_coinbase_sql_backfill_range(
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
    let canonicality_evidence = match run_with_backfill_lease_heartbeat(
        pool,
        &active_range,
        config,
        load_backfill_canonicality_evidence(
            pool,
            &source_plan.watched_chain_plan.chain,
            validation_provider,
        ),
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
        let window_outcome = match run_with_backfill_lease_heartbeat(
            pool,
            &active_range,
            config,
            run_coinbase_sql_backfill_window(
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
            ),
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
    if raw_log_count >= (coinbase_config.effective_page_limit() / 2).max(1) {
        (current_window_blocks / 2).max(1)
    } else if raw_log_count < coinbase_config.effective_page_limit() {
        current_window_blocks
            .saturating_mul(2)
            .min(coinbase_config.max_window_blocks)
            .min(MAX_COINBASE_SQL_PRACTICAL_WINDOW_BLOCKS)
            .max(1)
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
    let window_started = Instant::now();
    info!(
        service = "indexer",
        command = "backfill",
        chain = %source_plan.watched_chain_plan.chain,
        from_block = range.from_block,
        to_block = range.to_block,
        coinbase_sql_validation_mode = coinbase_config.validation_mode.as_str(),
        "Coinbase SQL backfill window started"
    );
    let (resolved_blocks, block_headers, historical_payload) = match coinbase_config.validation_mode
    {
        CoinbaseSqlValidationMode::Full => {
            let resolved_blocks = resolve_backfill_range(validation_provider, range).await?;
            let block_headers =
                fetch_coinbase_sql_window_headers(validation_provider, &resolved_blocks, range)
                    .await?;
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
            log_coinbase_sql_payload_fetch(
                source_plan,
                range,
                coinbase_config.validation_mode,
                &historical_payload,
            );
            (resolved_blocks, block_headers, historical_payload)
        }
        CoinbaseSqlValidationMode::Sample => {
            let planning_blocks = coinbase_sql_planning_blocks(range);
            let mut historical_payload = historical_source
                .fetch_selected_log_payloads(HistoricalLogPayloadRequest {
                    chain: &source_plan.watched_chain_plan.chain,
                    source_plan,
                    selected_target_index,
                    resolved_blocks: &planning_blocks,
                    selected_target_addresses_for_chunk,
                    topic_plan,
                    range,
                    validation_mode: coinbase_config.validation_mode,
                })
                .await?;
            log_coinbase_sql_payload_fetch(
                source_plan,
                range,
                coinbase_config.validation_mode,
                &historical_payload,
            );
            let sample_block_numbers = coinbase_sql_sample_validation_block_numbers(
                range,
                &historical_payload.logs_by_block,
            );
            let logs_need_validation_provider_payload =
                historical_payload.logs_need_validation_provider_payload;
            let decoded_payload_log_limit = coinbase_sql_sample_decoded_payload_log_limit(
                source_plan,
                &historical_payload,
                logs_need_validation_provider_payload,
            );
            ensure_coinbase_sql_sample_validation_size(
                range,
                historical_payload_log_count(&historical_payload),
                sample_block_numbers.len(),
                logs_need_validation_provider_payload,
                decoded_payload_log_limit,
            )?;
            info!(
                service = "indexer",
                command = "backfill",
                chain = %source_plan.watched_chain_plan.chain,
                from_block = range.from_block,
                to_block = range.to_block,
                sample_block_count = sample_block_numbers.len(),
                "Coinbase SQL sample validation range resolution started"
            );
            let resolved_blocks =
                resolve_backfill_block_numbers(validation_provider, &sample_block_numbers, range)
                .await
                .with_context(|| {
                    format!(
                        "failed to resolve validation-provider returned log blocks for sampled Coinbase SQL range {}..={}",
                        range.from_block, range.to_block
                    )
                })?;
            ensure_coinbase_sql_logs_match_resolved_blocks(
                &historical_payload.logs_by_block,
                &resolved_blocks,
            )?;
            if logs_need_validation_provider_payload {
                info!(
                    service = "indexer",
                    command = "backfill",
                    chain = %source_plan.watched_chain_plan.chain,
                    from_block = range.from_block,
                    to_block = range.to_block,
                    resolved_block_count = resolved_blocks.len(),
                    "Coinbase SQL sample validation log payload fill started"
                );
                let payload_fill_started = Instant::now();
                historical_payload.logs_by_block = fill_log_payloads_from_validation_provider(
                    validation_provider,
                    &resolved_blocks,
                    historical_payload.logs_by_block,
                    &historical_payload.validation_filters,
                    coinbase_config.validation_mode,
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to fill validation-provider log payloads for sampled Coinbase SQL range {}..={}",
                        range.from_block, range.to_block
                    )
                })?;
                historical_payload.logs_need_validation_provider_payload = false;
                info!(
                    service = "indexer",
                    command = "backfill",
                    chain = %source_plan.watched_chain_plan.chain,
                    from_block = range.from_block,
                    to_block = range.to_block,
                    filled_log_count = historical_payload_log_count(&historical_payload),
                    elapsed_ms = payload_fill_started.elapsed().as_millis(),
                    "Coinbase SQL sample validation log payloads filled"
                );
            } else {
                info!(
                    service = "indexer",
                    command = "backfill",
                    chain = %source_plan.watched_chain_plan.chain,
                    from_block = range.from_block,
                    to_block = range.to_block,
                    raw_log_count = historical_payload_log_count(&historical_payload),
                    "Coinbase SQL sample validation log payload fill skipped; decoded SQL parameters supplied log data"
                );
            }
            let block_headers =
                fetch_coinbase_sql_window_headers(validation_provider, &resolved_blocks, range)
                    .await?;
            (resolved_blocks, block_headers, historical_payload)
        }
    };
    let outcome = materialize_historical_payload_range(
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
    .await?;
    info!(
        service = "indexer",
        command = "backfill",
        chain = %source_plan.watched_chain_plan.chain,
        from_block = range.from_block,
        to_block = range.to_block,
        resolved_block_count = outcome.resolved_block_count,
        raw_log_count = outcome.raw_log_count,
        raw_transaction_count = outcome.raw_transaction_count,
        raw_receipt_count = outcome.raw_receipt_count,
        elapsed_ms = window_started.elapsed().as_millis(),
        "Coinbase SQL backfill window materialized"
    );
    Ok(outcome)
}

pub(crate) fn effective_coinbase_sql_adapter_sync_mode(
    source_plan: &WatchedSourceSelectorPlan,
    topic_plan: &BackfillTopicPlan,
    requested_mode: crate::backfill::BackfillAdapterSyncMode,
) -> crate::backfill::BackfillAdapterSyncMode {
    if coinbase_sql_requires_ordered_closure_replay(source_plan, topic_plan) {
        crate::backfill::BackfillAdapterSyncMode::RawOnly
    } else {
        requested_mode.hash_pinned_backfill_mode()
    }
}

fn coinbase_sql_requires_ordered_closure_replay(
    source_plan: &WatchedSourceSelectorPlan,
    topic_plan: &BackfillTopicPlan,
) -> bool {
    super::coinbase_sql_uses_basenames_registry_scan_all(source_plan, topic_plan)
        || source_plan
            .selected_targets
            .iter()
            .any(|target| basenames_authority_source_family_requires_closure(&target.source_family))
}

fn basenames_authority_source_family_requires_closure(source_family: &str) -> bool {
    matches!(
        source_family,
        BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY
            | BASENAMES_BASE_REGISTRY_SOURCE_FAMILY
            | BASENAMES_BASE_RESOLVER_SOURCE_FAMILY
    )
}

fn log_coinbase_sql_payload_fetch(
    source_plan: &WatchedSourceSelectorPlan,
    range: BackfillBlockRange,
    validation_mode: CoinbaseSqlValidationMode,
    payload: &HistoricalLogPayload,
) {
    let raw_log_count = historical_payload_log_count(payload);
    info!(
        service = "indexer",
        command = "backfill",
        chain = %source_plan.watched_chain_plan.chain,
        from_block = range.from_block,
        to_block = range.to_block,
        coinbase_sql_validation_mode = validation_mode.as_str(),
        coinbase_sql_query_count = payload.source_stats.query_count,
        coinbase_sql_page_count = payload.source_stats.page_count,
        coinbase_sql_row_count = payload.source_stats.row_count,
        coinbase_sql_retry_count = payload.source_stats.retry_count,
        coinbase_sql_log_block_count = payload.logs_by_block.len(),
        raw_log_count,
        validation_filter_count = payload.validation_filters.len(),
        "Coinbase SQL payload fetched"
    );
}

fn ensure_coinbase_sql_sample_validation_size(
    range: BackfillBlockRange,
    log_count: usize,
    block_count: usize,
    requires_validation_provider_payload: bool,
    decoded_payload_log_limit: usize,
) -> Result<()> {
    if requires_validation_provider_payload
        && block_count > MAX_COINBASE_SQL_SAMPLE_VALIDATION_BLOCKS
    {
        bail!(
            "Coinbase SQL sample window {}..={} returned logs across {} blocks; refusing provider log validation above {} blocks so the range can retry smaller",
            range.from_block,
            range.to_block,
            block_count,
            MAX_COINBASE_SQL_SAMPLE_VALIDATION_BLOCKS
        );
    }
    let max_log_count = if requires_validation_provider_payload {
        MAX_COINBASE_SQL_SAMPLE_PROVIDER_PAYLOAD_LOGS
    } else {
        decoded_payload_log_limit
    };
    if log_count > max_log_count {
        let validation_label = if requires_validation_provider_payload {
            "provider log-payload validation"
        } else {
            "decoded SQL materialization"
        };
        bail!(
            "Coinbase SQL sample window {}..={} returned {} logs; refusing {} above {} logs so the range can retry smaller",
            range.from_block,
            range.to_block,
            log_count,
            validation_label,
            max_log_count
        );
    }

    Ok(())
}

fn coinbase_sql_sample_decoded_payload_log_limit(
    source_plan: &WatchedSourceSelectorPlan,
    payload: &HistoricalLogPayload,
    requires_validation_provider_payload: bool,
) -> usize {
    if is_basenames_registry_scan_all_decoded_payload(
        source_plan,
        payload,
        requires_validation_provider_payload,
    ) {
        MAX_COINBASE_SQL_BASENAMES_REGISTRY_SAMPLE_DECODED_PAYLOAD_LOGS
    } else if is_basenames_registrar_address_filtered_decoded_payload(
        source_plan,
        payload,
        requires_validation_provider_payload,
    ) {
        MAX_COINBASE_SQL_BASENAMES_REGISTRAR_SAMPLE_DECODED_PAYLOAD_LOGS
    } else {
        MAX_COINBASE_SQL_SAMPLE_DECODED_PAYLOAD_LOGS
    }
}

fn is_basenames_registry_scan_all_decoded_payload(
    source_plan: &WatchedSourceSelectorPlan,
    payload: &HistoricalLogPayload,
    requires_validation_provider_payload: bool,
) -> bool {
    !requires_validation_provider_payload
        && !source_plan.selected_targets.is_empty()
        && source_plan
            .selected_targets
            .iter()
            .all(|target| target.source_family == BASENAMES_BASE_REGISTRY_SOURCE_FAMILY)
        && !payload.validation_filters.is_empty()
        && payload
            .validation_filters
            .iter()
            .all(|filter| filter.addresses.is_empty())
}

fn is_basenames_registrar_address_filtered_decoded_payload(
    source_plan: &WatchedSourceSelectorPlan,
    payload: &HistoricalLogPayload,
    requires_validation_provider_payload: bool,
) -> bool {
    !requires_validation_provider_payload
        && !payload.logs_filtered_by_selected_target_index
        && !source_plan.selected_targets.is_empty()
        && source_plan
            .selected_targets
            .iter()
            .all(|target| target.source_family == BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY)
        && !payload.validation_filters.is_empty()
        && payload
            .validation_filters
            .iter()
            .all(|filter| !filter.addresses.is_empty())
}

pub(crate) fn ensure_coinbase_sql_registry_range_start_is_replay_safe(
    source_plan: &WatchedSourceSelectorPlan,
    topic_plan: &BackfillTopicPlan,
    range: BackfillBlockRange,
) -> Result<()> {
    if source_plan.selector_kind != bigname_manifests::WatchedSourceSelectorKind::SourceFamily
        || source_plan.source_family.as_deref() != Some(BASENAMES_BASE_REGISTRY_SOURCE_FAMILY)
    {
        return Ok(());
    }
    if !topic_plan
        .event_signatures_for_source_family(BASENAMES_BASE_REGISTRY_SOURCE_FAMILY)
        .is_empty()
    {
        return Ok(());
    }

    let Some(earliest_effective_from_block) = source_plan
        .selected_targets
        .iter()
        .map(|target| target.effective_from_block)
        .min()
    else {
        return Ok(());
    };
    if range.from_block > earliest_effective_from_block {
        bail!(
            "Coinbase SQL Basenames registry backfill range starts at {}, after earliest selected target effective_from_block {}; start a new immutable source-identity job at or before that block instead of resuming across possible source-identity drift",
            range.from_block,
            earliest_effective_from_block
        );
    }

    Ok(())
}

fn historical_payload_log_count(payload: &HistoricalLogPayload) -> usize {
    payload.logs_by_block.values().map(Vec::len).sum()
}

fn coinbase_sql_planning_blocks(range: BackfillBlockRange) -> Vec<ProviderResolvedBlock> {
    (range.from_block..=range.to_block)
        .map(|block_number| ProviderResolvedBlock {
            block_number,
            block_hash: String::new(),
        })
        .collect()
}

fn coinbase_sql_sample_validation_block_numbers(
    range: BackfillBlockRange,
    logs_by_block: &BTreeMap<i64, Vec<ProviderLog>>,
) -> Vec<i64> {
    logs_by_block
        .keys()
        .copied()
        .filter(|block_number| *block_number >= range.from_block && *block_number <= range.to_block)
        .collect()
}

async fn fetch_coinbase_sql_window_headers(
    validation_provider: &(impl ChainProviderOps + ?Sized),
    resolved_blocks: &[ProviderResolvedBlock],
    range: BackfillBlockRange,
) -> Result<Vec<crate::provider::ProviderBlock>> {
    validation_provider
        .fetch_block_headers_by_hashes(resolved_blocks)
        .await
        .with_context(|| {
            format!(
                "failed to fetch validation provider headers for Coinbase SQL range {}..={}",
                range.from_block, range.to_block
            )
        })
}

fn ensure_coinbase_sql_logs_match_resolved_blocks(
    logs_by_block: &BTreeMap<i64, Vec<ProviderLog>>,
    resolved_blocks: &[ProviderResolvedBlock],
) -> Result<()> {
    let resolved_by_number = resolved_blocks
        .iter()
        .map(|block| (block.block_number, block.block_hash.clone()))
        .collect::<BTreeMap<_, _>>();
    for (block_number, logs) in logs_by_block {
        let expected_hash = resolved_by_number.get(block_number).with_context(|| {
            format!("Coinbase SQL returned block {block_number} that was not resolved by validation provider")
        })?;
        for log in logs {
            if log.block_number != *block_number {
                bail!(
                    "Coinbase SQL grouped log block {} under block {}",
                    log.block_number,
                    block_number
                );
            }
            if !log.block_hash.eq_ignore_ascii_case(expected_hash) {
                bail!(
                    "Coinbase SQL returned block {} hash {}, validation provider resolved {}",
                    block_number,
                    log.block_hash,
                    expected_hash
                );
            }
        }
    }

    Ok(())
}

#[cfg(test)]
#[path = "coinbase_sql/sample_tests.rs"]
mod sample_tests;

#[cfg(test)]
#[path = "coinbase_sql/tests.rs"]
mod tests;
