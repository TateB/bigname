use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use bigname_manifests::{WatchedSourceSelectorKind, WatchedSourceSelectorPlan};
use bigname_storage::{
    BackfillJobCreate, BackfillLifecycleStatus, BackfillRange, BackfillRangeSpec,
    CanonicalityState, RawCodeHash, RawLog, RawPayloadCacheMetadataUpsert, RawReceipt,
    RawTransaction, advance_backfill_range, complete_backfill_range, create_backfill_job,
    fail_backfill_range, load_backfill_job, reserve_backfill_range, upsert_raw_blocks,
    upsert_raw_code_hashes, upsert_raw_logs, upsert_raw_payload_cache_metadata,
    upsert_raw_receipts, upsert_raw_transactions,
};
use serde_json::json;
use sqlx::types::time::OffsetDateTime;
use tracing::{error, info};

use crate::{
    provider::{
        JsonRpcProvider, ProviderBlockCodeObservationRequest, ProviderLog, ProviderResolvedBlock,
    },
    reconciliation::{
        ensure_provider_bundle_matches_raw_block, provider_block_to_raw_block,
        provider_code_observation_to_raw_code_hash, provider_logs_to_selected_raw_logs,
        provider_raw_payload_cache_metadata_to_upserts, provider_receipts_to_selected_raw_receipts,
        provider_transactions_to_selected_raw_transactions,
        retained_transaction_keys_from_raw_logs, sync_adapter_state_from_persisted_raw_payloads,
        sync_adapter_state_from_scoped_persisted_raw_payloads,
    },
};

const HASH_PINNED_BACKFILL_SCAN_MODE: &str = "hash_pinned_block";
const HASH_PINNED_BACKFILL_CHUNK_BLOCKS: i64 = 32;
const MAX_FAILURE_ERROR_CHARS: usize = 2048;

#[derive(Clone, Debug, Eq, PartialEq)]
struct BackfillLogRangeRequest {
    start_index: usize,
    end_index: usize,
    addresses: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BackfillBlockRange {
    pub(crate) from_block: i64,
    pub(crate) to_block: i64,
}

impl BackfillBlockRange {
    pub(crate) fn new(from_block: i64, to_block: i64) -> Result<Self> {
        if from_block < 0 {
            bail!("backfill from block cannot be negative: {from_block}");
        }
        if to_block < 0 {
            bail!("backfill to block cannot be negative: {to_block}");
        }
        if from_block > to_block {
            bail!("backfill range start {from_block} is after end {to_block}");
        }

        Ok(Self {
            from_block,
            to_block,
        })
    }

    fn block_count(self) -> Result<usize> {
        let span = self
            .to_block
            .checked_sub(self.from_block)
            .and_then(|span| span.checked_add(1))
            .context("backfill range block count overflowed i64")?;
        usize::try_from(span).context("backfill range block count does not fit in usize")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BackfillJobRunConfig {
    pub(crate) deployment_profile: String,
    pub(crate) idempotency_key: String,
    pub(crate) range: BackfillBlockRange,
    pub(crate) lease_owner: String,
    pub(crate) lease_token: String,
    pub(crate) lease_expires_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BackfillOutcome {
    pub(crate) chain: String,
    pub(crate) from_block: i64,
    pub(crate) to_block: i64,
    pub(crate) resolved_block_count: usize,
    pub(crate) raw_block_count: usize,
    pub(crate) raw_transaction_count: usize,
    pub(crate) raw_receipt_count: usize,
    pub(crate) raw_log_count: usize,
    pub(crate) raw_code_hash_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BackfillJobRunOutcome {
    pub(crate) backfill_job_id: i64,
    pub(crate) chain: String,
    pub(crate) from_block: i64,
    pub(crate) to_block: i64,
    pub(crate) idempotency_key: String,
    pub(crate) reserved_range_count: usize,
    pub(crate) completed_range_count: usize,
    pub(crate) resolved_block_count: usize,
    pub(crate) raw_block_count: usize,
    pub(crate) raw_transaction_count: usize,
    pub(crate) raw_receipt_count: usize,
    pub(crate) raw_log_count: usize,
    pub(crate) raw_code_hash_count: usize,
}

impl BackfillJobRunOutcome {
    fn new(
        backfill_job_id: i64,
        source_plan: &WatchedSourceSelectorPlan,
        config: &BackfillJobRunConfig,
    ) -> Self {
        Self {
            backfill_job_id,
            chain: source_plan.watched_chain_plan.chain.clone(),
            from_block: config.range.from_block,
            to_block: config.range.to_block,
            idempotency_key: config.idempotency_key.clone(),
            reserved_range_count: 0,
            completed_range_count: 0,
            resolved_block_count: 0,
            raw_block_count: 0,
            raw_transaction_count: 0,
            raw_receipt_count: 0,
            raw_log_count: 0,
            raw_code_hash_count: 0,
        }
    }

    fn add_range_outcome(&mut self, outcome: &BackfillOutcome) {
        self.resolved_block_count += outcome.resolved_block_count;
        self.raw_block_count += outcome.raw_block_count;
        self.raw_transaction_count += outcome.raw_transaction_count;
        self.raw_receipt_count += outcome.raw_receipt_count;
        self.raw_log_count += outcome.raw_log_count;
        self.raw_code_hash_count += outcome.raw_code_hash_count;
    }
}

pub(crate) async fn run_resumable_hash_pinned_backfill_job(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    provider: &JsonRpcProvider,
    config: BackfillJobRunConfig,
) -> Result<BackfillJobRunOutcome> {
    let watched_chain = &source_plan.watched_chain_plan;
    let request = BackfillJobCreate {
        deployment_profile: config.deployment_profile.clone(),
        chain_id: watched_chain.chain.clone(),
        source_identity: source_plan.source_identity_payload(),
        scan_mode: HASH_PINNED_BACKFILL_SCAN_MODE.to_owned(),
        range_start_block_number: config.range.from_block,
        range_end_block_number: config.range.to_block,
        idempotency_key: config.idempotency_key.clone(),
        ranges: vec![BackfillRangeSpec {
            range_start_block_number: config.range.from_block,
            range_end_block_number: config.range.to_block,
        }],
    };
    let record = create_backfill_job(pool, &request).await?;
    let mut outcome = BackfillJobRunOutcome::new(record.job.backfill_job_id, source_plan, &config);

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
        range_count = record.ranges.len(),
        "resumable backfill job loaded"
    );

    loop {
        let Some(reserved_range) = reserve_backfill_range(
            pool,
            record.job.backfill_job_id,
            &config.lease_owner,
            &config.lease_token,
            config.lease_expires_at,
        )
        .await?
        else {
            break;
        };

        outcome.reserved_range_count += 1;
        run_reserved_hash_pinned_backfill_range(
            pool,
            source_plan,
            provider,
            &config,
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
            reserved_range_count = outcome.reserved_range_count,
            completed_range_count = outcome.completed_range_count,
            resolved_block_count = outcome.resolved_block_count,
            raw_block_count = outcome.raw_block_count,
            raw_transaction_count = outcome.raw_transaction_count,
            raw_receipt_count = outcome.raw_receipt_count,
            raw_log_count = outcome.raw_log_count,
            raw_code_hash_count = outcome.raw_code_hash_count,
            "resumable hash-pinned backfill job completed"
        );
        return Ok(outcome);
    }

    bail!(
        "backfill job {} has no reservable ranges but is {}; another active lease may still own work",
        record.job.backfill_job_id,
        job.status.as_str()
    );
}

pub(crate) async fn run_hash_pinned_backfill_range(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    provider: &JsonRpcProvider,
    range: BackfillBlockRange,
) -> Result<BackfillOutcome> {
    let watched_chain = &source_plan.watched_chain_plan;
    let source_scope = selected_target_sync_scope(source_plan);
    let resolved_blocks = resolve_backfill_range(provider, range).await?;
    let block_hashes = resolved_blocks
        .iter()
        .map(|block| block.block_hash.clone())
        .collect::<Vec<_>>();
    let fetch_logs_by_safe_ranges = resolved_blocks.len() > 1;
    let mut ranged_logs_by_block = if fetch_logs_by_safe_ranges {
        fetch_backfill_logs_by_safe_ranges(provider, source_plan, &resolved_blocks, range).await?
    } else {
        BTreeMap::new()
    };
    let single_block_selected_addresses = resolved_blocks
        .first()
        .map(|block| selected_target_addresses_at_block(source_plan, block.block_number))
        .unwrap_or_default();
    let bundles = if fetch_logs_by_safe_ranges || single_block_selected_addresses.is_empty() {
        provider
            .fetch_block_bundles_without_logs_by_hashes(&resolved_blocks)
            .await
    } else {
        provider
            .fetch_block_bundles_by_hashes(&resolved_blocks)
            .await
    }
    .with_context(|| {
        format!(
            "failed to fetch hash-pinned payload batch for chain {} range {}..={}",
            watched_chain.chain, range.from_block, range.to_block
        )
    })?;
    let mut raw_blocks = Vec::with_capacity(resolved_blocks.len());
    let mut transactions = Vec::<RawTransaction>::new();
    let mut receipts = Vec::<RawReceipt>::new();
    let mut logs = Vec::<RawLog>::new();
    let mut code_hashes = Vec::<RawCodeHash>::new();
    let mut cache_metadata = Vec::<RawPayloadCacheMetadataUpsert>::new();
    let mut raw_blocks_by_hash = BTreeMap::new();
    let mut code_observation_requests = Vec::new();

    for (resolved_block, bundle) in resolved_blocks.iter().zip(bundles.iter()) {
        let selected_addresses =
            selected_target_addresses_at_block(source_plan, resolved_block.block_number);
        if bundle.block.block_number != resolved_block.block_number {
            bail!(
                "provider resolved chain {} block number {} to hash {}, but hash-scoped fetch returned block number {}",
                watched_chain.chain,
                resolved_block.block_number,
                resolved_block.block_hash,
                bundle.block.block_number
            );
        }

        let raw_block = provider_block_to_raw_block(
            &watched_chain.chain,
            &bundle.block,
            CanonicalityState::Observed,
        );
        ensure_provider_bundle_matches_raw_block(&raw_block, &bundle)?;

        cache_metadata.extend(provider_raw_payload_cache_metadata_to_upserts(
            &watched_chain.chain,
            &raw_block,
            &bundle.raw_payloads,
        ));
        let block_logs = if fetch_logs_by_safe_ranges {
            ranged_logs_by_block
                .remove(&resolved_block.block_number)
                .unwrap_or_default()
        } else {
            bundle.logs.clone()
        };
        let selected_logs = provider_logs_to_selected_raw_logs(
            &watched_chain.chain,
            &raw_block,
            &block_logs,
            &selected_addresses,
        )?;
        let retained_transaction_keys = retained_transaction_keys_from_raw_logs(&selected_logs);
        transactions.extend(provider_transactions_to_selected_raw_transactions(
            &watched_chain.chain,
            &raw_block,
            &bundle.transactions,
            &retained_transaction_keys,
        )?);
        receipts.extend(provider_receipts_to_selected_raw_receipts(
            &watched_chain.chain,
            &raw_block,
            &bundle.receipts,
            &retained_transaction_keys,
        )?);
        logs.extend(selected_logs);

        if !selected_addresses.is_empty() {
            code_observation_requests.push(ProviderBlockCodeObservationRequest {
                block_hash: raw_block.block_hash.clone(),
                addresses: selected_addresses.iter().cloned().collect(),
            });
            raw_blocks_by_hash.insert(raw_block.block_hash.clone(), raw_block.clone());
        }

        raw_blocks.push(raw_block);
    }
    if !ranged_logs_by_block.is_empty() {
        bail!("provider returned range logs for unprocessed backfill blocks");
    }

    let code_observation_batches = provider
        .fetch_code_observations_at_block_hashes(&code_observation_requests)
        .await
        .with_context(|| {
            format!(
                "failed to fetch hash-pinned code observation batch for chain {} range {}..={}",
                watched_chain.chain, range.from_block, range.to_block
            )
        })?;
    for batch in code_observation_batches {
        let raw_block = raw_blocks_by_hash.get(&batch.block_hash).with_context(|| {
            format!(
                "provider returned code observations for unrequested block hash {}",
                batch.block_hash
            )
        })?;
        code_hashes.extend(
            batch
                .observations
                .iter()
                .map(|observation| {
                    provider_code_observation_to_raw_code_hash(
                        &watched_chain.chain,
                        raw_block,
                        observation,
                    )
                })
                .collect::<Result<Vec<_>>>()?,
        );
    }

    upsert_raw_blocks(pool, &raw_blocks).await?;
    upsert_raw_payload_cache_metadata(pool, &cache_metadata).await?;
    upsert_raw_transactions(pool, &transactions).await?;
    upsert_raw_receipts(pool, &receipts).await?;
    upsert_raw_logs(pool, &logs).await?;
    upsert_raw_code_hashes(pool, &code_hashes).await?;
    if source_plan.selector_kind == WatchedSourceSelectorKind::WholeActiveWatchedChain {
        sync_adapter_state_from_persisted_raw_payloads(pool, &watched_chain.chain, &block_hashes)
            .await?;
    } else {
        sync_adapter_state_from_scoped_persisted_raw_payloads(
            pool,
            &watched_chain.chain,
            &block_hashes,
            &source_scope,
        )
        .await?;
    }

    let outcome = BackfillOutcome {
        chain: watched_chain.chain.clone(),
        from_block: range.from_block,
        to_block: range.to_block,
        resolved_block_count: resolved_blocks.len(),
        raw_block_count: raw_blocks.len(),
        raw_transaction_count: transactions.len(),
        raw_receipt_count: receipts.len(),
        raw_log_count: logs.len(),
        raw_code_hash_count: code_hashes.len(),
    };
    info!(
        service = "indexer",
        command = "backfill",
        chain = %outcome.chain,
        from_block = outcome.from_block,
        to_block = outcome.to_block,
        resolved_block_count = outcome.resolved_block_count,
        raw_block_count = outcome.raw_block_count,
        raw_transaction_count = outcome.raw_transaction_count,
        raw_receipt_count = outcome.raw_receipt_count,
        raw_log_count = outcome.raw_log_count,
        raw_code_hash_count = outcome.raw_code_hash_count,
        "hash-pinned backfill range completed"
    );

    Ok(outcome)
}

async fn run_reserved_hash_pinned_backfill_range(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    provider: &JsonRpcProvider,
    config: &BackfillJobRunConfig,
    reserved_range: &BackfillRange,
    aggregate: &mut BackfillJobRunOutcome,
) -> Result<()> {
    let mut active_range = reserved_range.clone();
    let mut block_number = active_range.checkpoint_block_number;
    while block_number <= active_range.range_end_block_number {
        let chunk_end = block_number
            .checked_add(HASH_PINNED_BACKFILL_CHUNK_BLOCKS - 1)
            .unwrap_or(active_range.range_end_block_number)
            .min(active_range.range_end_block_number);
        let chunk_range = BackfillBlockRange::new(block_number, chunk_end)?;
        let chunk_outcome =
            match run_hash_pinned_backfill_range(pool, source_plan, provider, chunk_range).await {
                Ok(outcome) => outcome,
                Err(error) => {
                    return Err(record_reserved_range_failure(
                        pool,
                        &active_range,
                        config,
                        "hash-pinned backfill failed",
                        Some(block_number),
                        Some(chunk_range),
                        "hash_pinned_intake",
                        error,
                    )
                    .await);
                }
            };
        aggregate.add_range_outcome(&chunk_outcome);

        active_range = match advance_backfill_range(
            pool,
            active_range.backfill_range_id,
            &config.lease_token,
            chunk_end,
        )
        .await
        {
            Ok(range) => range,
            Err(error) => {
                return Err(record_reserved_range_failure(
                    pool,
                    &active_range,
                    config,
                    "backfill checkpoint advance failed",
                    Some(block_number),
                    Some(chunk_range),
                    "checkpoint_advance",
                    error,
                )
                .await);
            }
        };

        if chunk_end == active_range.range_end_block_number {
            break;
        }
        block_number = chunk_end
            .checked_add(1)
            .context("backfill block number overflowed while advancing range")?;
    }

    if let Err(error) =
        complete_backfill_range(pool, active_range.backfill_range_id, &config.lease_token).await
    {
        return Err(record_reserved_range_failure(
            pool,
            &active_range,
            config,
            "backfill range completion failed",
            None,
            None,
            "range_completion",
            error,
        )
        .await);
    }

    Ok(())
}

async fn fetch_backfill_logs_by_safe_ranges(
    provider: &JsonRpcProvider,
    source_plan: &WatchedSourceSelectorPlan,
    resolved_blocks: &[ProviderResolvedBlock],
    range: BackfillBlockRange,
) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
    let mut logs_by_block = BTreeMap::new();
    for request in selected_log_range_requests(source_plan, resolved_blocks) {
        let request_blocks = &resolved_blocks[request.start_index..request.end_index];
        let from_block = request_blocks
            .first()
            .expect("selected log range request must contain at least one block")
            .block_number;
        let to_block = request_blocks
            .last()
            .expect("selected log range request must contain at least one block")
            .block_number;
        let group_logs = provider
            .fetch_logs_by_block_range(request_blocks, &request.addresses)
            .await
            .with_context(|| {
                format!(
                    "failed to fetch hash-pinned log range {}..={} inside backfill range {}..={}",
                    from_block, to_block, range.from_block, range.to_block
                )
            })?;

        for (block_number, logs) in group_logs {
            if logs_by_block.insert(block_number, logs).is_some() {
                bail!("provider returned duplicate range logs for backfill block {block_number}");
            }
        }
    }

    Ok(logs_by_block)
}

fn selected_log_range_requests(
    source_plan: &WatchedSourceSelectorPlan,
    resolved_blocks: &[ProviderResolvedBlock],
) -> Vec<BackfillLogRangeRequest> {
    let mut requests = Vec::new();
    let mut active_start = None;
    let mut active_addresses = BTreeSet::<String>::new();

    for (index, block) in resolved_blocks.iter().enumerate() {
        let addresses = selected_target_addresses_at_block(source_plan, block.block_number);
        if addresses.is_empty() {
            if let Some(start_index) = active_start.take() {
                requests.push(BackfillLogRangeRequest {
                    start_index,
                    end_index: index,
                    addresses: active_addresses.iter().cloned().collect(),
                });
                active_addresses.clear();
            }
            continue;
        }

        match active_start {
            Some(start_index) if active_addresses == addresses => {
                active_start = Some(start_index);
            }
            Some(start_index) => {
                requests.push(BackfillLogRangeRequest {
                    start_index,
                    end_index: index,
                    addresses: active_addresses.iter().cloned().collect(),
                });
                active_start = Some(index);
                active_addresses = addresses;
            }
            None => {
                active_start = Some(index);
                active_addresses = addresses;
            }
        }
    }

    if let Some(start_index) = active_start {
        requests.push(BackfillLogRangeRequest {
            start_index,
            end_index: resolved_blocks.len(),
            addresses: active_addresses.into_iter().collect(),
        });
    }

    requests
}

fn selected_target_addresses_at_block(
    source_plan: &WatchedSourceSelectorPlan,
    block_number: i64,
) -> BTreeSet<String> {
    source_plan
        .selected_targets
        .iter()
        .filter(|target| {
            target.effective_from_block <= block_number && block_number <= target.effective_to_block
        })
        .map(|target| target.address.to_ascii_lowercase())
        .collect()
}

fn selected_target_sync_scope(
    source_plan: &WatchedSourceSelectorPlan,
) -> Vec<(String, String, i64, i64)> {
    source_plan
        .selected_targets
        .iter()
        .map(|target| {
            (
                target.source_family.clone(),
                target.address.to_ascii_lowercase(),
                target.effective_from_block,
                target.effective_to_block,
            )
        })
        .collect()
}

async fn record_reserved_range_failure(
    pool: &sqlx::PgPool,
    reserved_range: &BackfillRange,
    config: &BackfillJobRunConfig,
    failure_reason: &str,
    block_number: Option<i64>,
    attempted_range: Option<BackfillBlockRange>,
    phase: &str,
    error: anyhow::Error,
) -> anyhow::Error {
    let failure_metadata = json!({
        "phase": phase,
        "block_number": block_number,
        "attempted_range_start_block_number": attempted_range.map(|range| range.from_block),
        "attempted_range_end_block_number": attempted_range.map(|range| range.to_block),
        "range_start_block_number": reserved_range.range_start_block_number,
        "range_end_block_number": reserved_range.range_end_block_number,
        "checkpoint_block_number": reserved_range.checkpoint_block_number,
        "idempotency_key": &config.idempotency_key,
        "error": truncate_failure_error(&format!("{error:#}")),
    });

    match fail_backfill_range(
        pool,
        reserved_range.backfill_range_id,
        &config.lease_token,
        failure_reason,
        failure_metadata,
    )
    .await
    {
        Ok(_) => error.context("recorded persisted backfill failure state"),
        Err(fail_error) => {
            error!(
                service = "indexer",
                command = "backfill",
                backfill_range_id = reserved_range.backfill_range_id,
                failure_record_error = %fail_error,
                "failed to record persisted backfill failure state"
            );
            error.context(format!(
                "failed to record persisted backfill failure state: {fail_error:#}"
            ))
        }
    }
}

fn truncate_failure_error(error: &str) -> String {
    let mut truncated = error
        .chars()
        .take(MAX_FAILURE_ERROR_CHARS)
        .collect::<String>();
    if error.chars().count() > MAX_FAILURE_ERROR_CHARS {
        truncated.push_str("...[truncated]");
    }
    truncated
}

async fn resolve_backfill_range(
    provider: &JsonRpcProvider,
    range: BackfillBlockRange,
) -> Result<Vec<ProviderResolvedBlock>> {
    let block_numbers = (range.from_block..=range.to_block).collect::<Vec<_>>();
    let resolved_blocks = provider
        .fetch_block_hashes_by_numbers(&block_numbers)
        .await
        .with_context(|| {
            format!(
                "failed to resolve backfill block numbers {}..={}",
                range.from_block, range.to_block
            )
        })?;
    if resolved_blocks.len() != range.block_count()? {
        bail!(
            "provider resolved {} backfill blocks for range {}..={} but expected {}",
            resolved_blocks.len(),
            range.from_block,
            range.to_block,
            range.block_count()?
        );
    }

    Ok(resolved_blocks)
}
