use std::io::{self, Write};

#[path = "reservation_execution/coinbase_sql.rs"]
mod coinbase_sql_execution;

use alloy_primitives::{Keccak256, hex};
use anyhow::{Context, Result, bail};
use bigname_manifests::{
    WatchedBackfillTarget, WatchedSourceSelectorKind, WatchedSourceSelectorPlan,
};
use bigname_storage::{
    BackfillJobCreate, BackfillJobRecord, BackfillLifecycleStatus, BackfillRange,
    BackfillRangeSpec, advance_backfill_range, complete_backfill_range, create_backfill_job,
    load_backfill_job, reserve_backfill_range,
};
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::types::time::OffsetDateTime;
use tracing::info;

use crate::{
    ens_v1_resolver::SOURCE_FAMILY_ENS_V1_RESOLVER_L1, provider::ChainProviderOps,
    source_scope::watched_source_plan_uses_generic_resolver_scope,
};

use super::{
    BackfillBlockRange, BackfillJobRunConfig, BackfillJobRunOutcome, BackfillTopicPlan,
    CoinbaseSqlBackfillConfig,
    failure_recording::{ReservedRangeFailure, record_reserved_range_failure},
    fetching::{load_backfill_canonicality_evidence, run_hash_pinned_backfill_range},
    selection::{SelectedTargetIntervalIndex, SelectedTargetRangeCursor},
};

pub(crate) use coinbase_sql_execution::run_resumable_coinbase_sql_backfill_job;

const HASH_PINNED_BACKFILL_SCAN_MODE: &str = "hash_pinned_block";
pub(crate) const COINBASE_SQL_BACKFILL_SCAN_MODE: &str = "coinbase_sql_hash_pinned_logs_v1";
pub(crate) const DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS: i64 = 1_024;
pub(crate) const COMPACT_SOURCE_IDENTITY_SELECTED_TARGET_THRESHOLD: usize = 10_000;

pub(crate) async fn create_hash_pinned_backfill_job(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    config: &BackfillJobRunConfig,
) -> Result<BackfillJobRecord> {
    let ranges = vec![BackfillRangeSpec {
        range_start_block_number: config.range.from_block,
        range_end_block_number: config.range.to_block,
    }];
    create_hash_pinned_backfill_job_with_ranges(pool, source_plan, config, ranges).await
}

pub(crate) async fn create_hash_pinned_backfill_job_with_ranges(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    config: &BackfillJobRunConfig,
    ranges: Vec<BackfillRangeSpec>,
) -> Result<BackfillJobRecord> {
    create_backfill_job(
        pool,
        &BackfillJobCreate {
            deployment_profile: config.deployment_profile.clone(),
            chain_id: source_plan.watched_chain_plan.chain.clone(),
            source_identity: backfill_job_source_identity_payload(source_plan)?,
            scan_mode: HASH_PINNED_BACKFILL_SCAN_MODE.to_owned(),
            range_start_block_number: config.range.from_block,
            range_end_block_number: config.range.to_block,
            idempotency_key: config.idempotency_key.clone(),
            ranges,
        },
    )
    .await
}

pub(crate) async fn create_coinbase_sql_backfill_job(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    config: &BackfillJobRunConfig,
    _coinbase_config: &CoinbaseSqlBackfillConfig,
    topic_plan: &BackfillTopicPlan,
) -> Result<BackfillJobRecord> {
    let ranges = vec![BackfillRangeSpec {
        range_start_block_number: config.range.from_block,
        range_end_block_number: config.range.to_block,
    }];
    create_backfill_job(
        pool,
        &BackfillJobCreate {
            deployment_profile: config.deployment_profile.clone(),
            chain_id: source_plan.watched_chain_plan.chain.clone(),
            source_identity: coinbase_sql_backfill_job_source_identity_payload(
                source_plan,
                topic_plan,
            )?,
            scan_mode: COINBASE_SQL_BACKFILL_SCAN_MODE.to_owned(),
            range_start_block_number: config.range.from_block,
            range_end_block_number: config.range.to_block,
            idempotency_key: config.idempotency_key.clone(),
            ranges,
        },
    )
    .await
}

pub(crate) fn hash_pinned_backfill_range_specs(
    range: BackfillBlockRange,
    range_blocks: i64,
) -> Result<Vec<BackfillRangeSpec>> {
    if range_blocks <= 0 {
        bail!("hash-pinned backfill range blocks must be positive, got {range_blocks}");
    }

    let mut ranges = Vec::new();
    let mut range_start = range.from_block;
    while range_start <= range.to_block {
        let range_end = range_start
            .checked_add(range_blocks - 1)
            .unwrap_or(range.to_block)
            .min(range.to_block);
        ranges.push(BackfillRangeSpec {
            range_start_block_number: range_start,
            range_end_block_number: range_end,
        });
        if range_end == range.to_block {
            break;
        }
        range_start = range_end
            .checked_add(1)
            .context("hash-pinned backfill range start overflowed while partitioning")?;
    }

    Ok(ranges)
}

pub(crate) fn backfill_job_source_identity_payload(
    source_plan: &WatchedSourceSelectorPlan,
) -> Result<Value> {
    if watched_source_plan_uses_generic_resolver_scope(source_plan) {
        return generic_topic_scan_source_identity_payload(source_plan);
    }

    if source_plan.selector_kind != WatchedSourceSelectorKind::SourceFamily
        || source_plan.selected_targets.len() <= COMPACT_SOURCE_IDENTITY_SELECTED_TARGET_THRESHOLD
    {
        return Ok(source_plan.source_identity_payload());
    }

    let selected_targets_digest = keccak256_json_digest(&source_plan.selected_targets)
        .context("failed to digest compact backfill source selected targets")?;
    let mut payload = json!({
        "selector_kind": source_plan.selector_kind.as_str(),
        "source_family": &source_plan.source_family,
        "requested_watched_targets": &source_plan.requested_watched_targets,
        "selected_target_count": source_plan.selected_targets.len(),
        "selected_targets_digest_algorithm": "keccak256",
        "selected_targets_digest": selected_targets_digest,
        "selected_targets_sample": selected_targets_sample(&source_plan.selected_targets),
        "source_identity_payload_format": "selected_targets_digest_v1",
    });
    let source_identity_hash =
        keccak256_json_digest(&payload).context("failed to digest backfill source identity")?;
    payload
        .as_object_mut()
        .expect("compact source identity payload must be an object")
        .insert(
            "source_identity_hash".to_owned(),
            Value::String(source_identity_hash),
        );
    Ok(payload)
}

fn coinbase_sql_backfill_job_source_identity_payload(
    source_plan: &WatchedSourceSelectorPlan,
    topic_plan: &BackfillTopicPlan,
) -> Result<Value> {
    let mut payload = backfill_job_source_identity_payload(source_plan)?;
    let object = payload
        .as_object_mut()
        .context("backfill source identity payload must be an object")?;
    object.insert(
        "backfill_provider".to_owned(),
        Value::String("coinbase_cdp_sql".to_owned()),
    );
    object.insert(
        "scan_mode".to_owned(),
        Value::String(COINBASE_SQL_BACKFILL_SCAN_MODE.to_owned()),
    );
    object.insert(
        "coinbase_sql_plan_version".to_owned(),
        Value::String("base_logs_v1".to_owned()),
    );
    object.insert("validation_provider_required".to_owned(), Value::Bool(true));
    object.insert(
        "topic_filtering".to_owned(),
        Value::String("manifest_abi_topic0_union_v1".to_owned()),
    );
    object.insert(
        "coinbase_sql_topic_plan".to_owned(),
        topic_plan.source_identity_payload()?,
    );

    Ok(payload)
}

fn generic_topic_scan_source_identity_payload(
    source_plan: &WatchedSourceSelectorPlan,
) -> Result<Value> {
    let selected_targets = source_plan
        .selected_targets
        .iter()
        .filter(|target| target.source_family != SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
        .cloned()
        .collect::<Vec<_>>();
    let requested_watched_targets = source_plan.requested_watched_targets.clone();
    let generic_topic_scans = json!([
        {
            "source_family": SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
            "source_identity_payload_format": "generic_resolver_event_topics_v1"
        }
    ]);

    let mut payload = if source_plan.selector_kind == WatchedSourceSelectorKind::SourceFamily
        && source_plan.source_family.as_deref() == Some(SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
    {
        json!({
            "selector_kind": source_plan.selector_kind.as_str(),
            "source_family": &source_plan.source_family,
            "requested_watched_targets": requested_watched_targets,
            "source_identity_payload_format": "generic_resolver_event_topics_v1",
        })
    } else if selected_targets.len() <= COMPACT_SOURCE_IDENTITY_SELECTED_TARGET_THRESHOLD {
        json!({
            "selector_kind": source_plan.selector_kind.as_str(),
            "source_family": &source_plan.source_family,
            "requested_watched_targets": requested_watched_targets,
            "selected_targets": selected_targets,
            "generic_topic_scans": generic_topic_scans,
            "source_identity_payload_format": "selected_targets_with_generic_topic_scans_v1",
        })
    } else {
        let selected_targets_digest = keccak256_json_digest(&selected_targets)
            .context("failed to digest compact generic-topic-scan source selected targets")?;
        json!({
            "selector_kind": source_plan.selector_kind.as_str(),
            "source_family": &source_plan.source_family,
            "requested_watched_targets": requested_watched_targets,
            "selected_target_count": selected_targets.len(),
            "selected_targets_digest_algorithm": "keccak256",
            "selected_targets_digest": selected_targets_digest,
            "selected_targets_sample": selected_targets_sample(&selected_targets),
            "generic_topic_scans": generic_topic_scans,
            "source_identity_payload_format": "selected_targets_digest_with_generic_topic_scans_v1",
        })
    };
    let source_identity_hash = keccak256_json_digest(&payload)
        .context("failed to digest generic-topic-scan backfill source identity")?;
    payload
        .as_object_mut()
        .expect("generic-topic-scan source identity payload must be an object")
        .insert(
            "source_identity_hash".to_owned(),
            Value::String(source_identity_hash),
        );
    Ok(payload)
}

fn selected_targets_sample(selected_targets: &[WatchedBackfillTarget]) -> Value {
    json!({
        "first": selected_targets.first(),
        "last": selected_targets.last(),
    })
}

fn keccak256_json_digest<T>(value: &T) -> Result<String>
where
    T: Serialize + ?Sized,
{
    let mut writer = Keccak256Writer::default();
    serde_json::to_writer(&mut writer, value).context("failed to serialize JSON digest input")?;
    Ok(format!("keccak256:{}", hex_string(&writer.finalize())))
}

#[derive(Default)]
struct Keccak256Writer {
    hasher: Keccak256,
}

impl Keccak256Writer {
    fn finalize(self) -> [u8; 32] {
        self.hasher.finalize().0
    }
}

impl Write for Keccak256Writer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.hasher.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn hex_string(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

pub(crate) async fn run_resumable_hash_pinned_backfill_job(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    provider: &(impl ChainProviderOps + ?Sized),
    mut config: BackfillJobRunConfig,
) -> Result<BackfillJobRunOutcome> {
    config.adapter_sync_mode = config.adapter_sync_mode.hash_pinned_backfill_mode();
    validate_hash_pinned_chunk_blocks(config.hash_pinned_chunk_blocks)?;
    let watched_chain = &source_plan.watched_chain_plan;
    let record = create_hash_pinned_backfill_job(pool, source_plan, &config).await?;
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
        hash_pinned_chunk_blocks = config.hash_pinned_chunk_blocks,
        adapter_sync_mode = config.adapter_sync_mode.as_str(),
        header_audit_mode = config.header_audit_mode.as_str(),
        range_count = record.ranges.len(),
        "resumable backfill job loaded"
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
            hash_pinned_chunk_blocks = config.hash_pinned_chunk_blocks,
            adapter_sync_mode = config.adapter_sync_mode.as_str(),
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

pub(super) async fn run_reserved_hash_pinned_backfill_range(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    provider: &(impl ChainProviderOps + ?Sized),
    config: &BackfillJobRunConfig,
    reserved_range: &BackfillRange,
    aggregate: &mut BackfillJobRunOutcome,
) -> Result<()> {
    let mut active_range = reserved_range.clone();
    let mut block_number = active_range
        .checkpoint_block_number
        .checked_add(1)
        .context("backfill checkpoint overflowed while computing resume block")?;
    let selected_target_index = SelectedTargetIntervalIndex::from_source_plan(source_plan);
    let mut selected_target_range_cursor = SelectedTargetRangeCursor::from_source_plan(source_plan);
    let canonicality_evidence = match load_backfill_canonicality_evidence(
        pool,
        &source_plan.watched_chain_plan.chain,
        provider,
    )
    .await
    {
        Ok(evidence) => evidence,
        Err(error) => {
            return Err(record_reserved_range_failure(ReservedRangeFailure {
                pool,
                reserved_range: &active_range,
                config,
                failure_reason: "backfill canonicality evidence load failed",
                block_number: Some(block_number),
                attempted_range: None,
                phase: "canonicality_evidence",
                error,
            })
            .await);
        }
    };
    while block_number <= active_range.range_end_block_number {
        let chunk_end = block_number
            .checked_add(config.hash_pinned_chunk_blocks - 1)
            .unwrap_or(active_range.range_end_block_number)
            .min(active_range.range_end_block_number);
        let chunk_range = BackfillBlockRange::new(block_number, chunk_end)?;
        let selected_target_addresses_for_chunk = selected_target_range_cursor
            .active_addresses_for_monotonic_range(chunk_range.from_block, chunk_range.to_block);
        let chunk_outcome = match run_hash_pinned_backfill_range(
            pool,
            source_plan,
            &selected_target_index,
            &selected_target_addresses_for_chunk,
            provider,
            chunk_range,
            canonicality_evidence.clone(),
            config.adapter_sync_mode,
            config.header_audit_mode,
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                return Err(record_reserved_range_failure(ReservedRangeFailure {
                    pool,
                    reserved_range: &active_range,
                    config,
                    failure_reason: "hash-pinned backfill failed",
                    block_number: Some(block_number),
                    attempted_range: Some(chunk_range),
                    phase: "hash_pinned_intake",
                    error,
                })
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
                return Err(record_reserved_range_failure(ReservedRangeFailure {
                    pool,
                    reserved_range: &active_range,
                    config,
                    failure_reason: "backfill checkpoint advance failed",
                    block_number: Some(block_number),
                    attempted_range: Some(chunk_range),
                    phase: "checkpoint_advance",
                    error,
                })
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
        return Err(record_reserved_range_failure(ReservedRangeFailure {
            pool,
            reserved_range: &active_range,
            config,
            failure_reason: "backfill range completion failed",
            block_number: None,
            attempted_range: None,
            phase: "range_completion",
            error,
        })
        .await);
    }

    Ok(())
}

pub(super) fn validate_hash_pinned_chunk_blocks(chunk_blocks: i64) -> Result<()> {
    if chunk_blocks <= 0 {
        bail!("hash-pinned backfill chunk blocks must be positive, got {chunk_blocks}");
    }

    Ok(())
}

pub(super) fn backfill_lease_duration_secs(lease_expires_at: OffsetDateTime) -> Result<i64> {
    let duration_secs = lease_expires_at
        .unix_timestamp()
        .checked_sub(OffsetDateTime::now_utc().unix_timestamp())
        .context("backfill lease duration timestamp underflowed")?;
    if duration_secs <= 0 {
        bail!("lease_expires_at must be in the future");
    }

    Ok(duration_secs)
}

pub(super) fn refreshed_backfill_lease_expires_at(duration_secs: i64) -> Result<OffsetDateTime> {
    let deadline = OffsetDateTime::now_utc()
        .unix_timestamp()
        .checked_add(duration_secs)
        .context("backfill lease expiry timestamp overflowed while refreshing range lease")?;
    OffsetDateTime::from_unix_timestamp(deadline)
        .context("refreshed backfill lease expiry timestamp is out of range")
}
