use std::path::Path;

use anyhow::{Context, Result};
use bigname_manifests::{
    ManifestBootstrapSkippedTarget, WatchedSourceSelector, WatchedTargetIdentity,
    load_manifest_declared_bootstrap_targets, load_manifest_declared_watched_source_selector_plan,
    load_manifest_skipped_bootstrap_targets,
};
use tracing::{info, warn};

use crate::{
    backfill::{
        BackfillAdapterSyncMode, BackfillBlockRange, run_resumable_hash_pinned_backfill_job,
    },
    backfill_lease_expires_at, default_backfill_lease_owner, deployment_profile_from_manifest_root,
    generated_backfill_lease_token,
    provider::{ChainProviderOps, ProviderRegistry},
    reconciliation::{
        RawFactNormalizedEventReplayRequest, RawFactNormalizedEventReplaySelection,
        log_raw_fact_normalized_event_replay_outcome, replay_raw_fact_normalized_events,
    },
    runtime::{IntakeChainTask, validate_provider_registry_for_intake_tasks},
};

#[path = "bootstrap_backfill/checkpoints.rs"]
mod checkpoints;
#[path = "bootstrap_backfill/planning.rs"]
mod planning;

use checkpoints::{
    bootstrap_segment_target_ids, load_bootstrap_segment_checkpoint,
    load_bootstrap_target_checkpoint,
};
use planning::{
    BootstrapBackfillTargetRange, bootstrap_target_range, narrow_manifest_bootstrap_source_plan,
    plan_bootstrap_backfill_segments,
};

const BOOTSTRAP_BACKFILL_LEASE_DURATION_SECS: u64 = 300;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct BootstrapBackfillOutcome {
    pub(crate) active_chain_count: usize,
    pub(crate) provider_configured_chain_count: usize,
    pub(crate) missing_provider_chain_count: usize,
    pub(crate) eligible_target_count: usize,
    pub(crate) skipped_unknown_start_target_count: usize,
    pub(crate) skipped_unknown_start_targets: Vec<ManifestBootstrapSkippedTarget>,
    pub(crate) drained_job_count: usize,
    pub(crate) skipped_future_target_count: usize,
    pub(crate) reserved_range_count: usize,
    pub(crate) completed_range_count: usize,
    pub(crate) resolved_block_count: usize,
    pub(crate) raw_block_count: usize,
    pub(crate) raw_transaction_count: usize,
    pub(crate) raw_receipt_count: usize,
    pub(crate) raw_log_count: usize,
    pub(crate) raw_code_hash_count: usize,
    pub(crate) normalized_replay_job_count: usize,
    pub(crate) normalized_replay_synced_count: usize,
    pub(crate) normalized_replay_inserted_count: usize,
}

impl BootstrapBackfillOutcome {
    fn add_job(&mut self, outcome: &crate::backfill::BackfillJobRunOutcome) {
        self.drained_job_count += 1;
        self.reserved_range_count += outcome.reserved_range_count;
        self.completed_range_count += outcome.completed_range_count;
        self.resolved_block_count += outcome.resolved_block_count;
        self.raw_block_count += outcome.raw_block_count;
        self.raw_transaction_count += outcome.raw_transaction_count;
        self.raw_receipt_count += outcome.raw_receipt_count;
        self.raw_log_count += outcome.raw_log_count;
        self.raw_code_hash_count += outcome.raw_code_hash_count;
    }
}

pub(crate) async fn run_startup_bootstrap_backfills(
    pool: &sqlx::PgPool,
    manifests_root: &Path,
    intake_chain_tasks: &[IntakeChainTask],
    provider_registry: &ProviderRegistry,
    hash_pinned_chunk_blocks: i64,
    adapter_sync_mode: BackfillAdapterSyncMode,
    replay_completed_raw_ranges: bool,
) -> Result<BootstrapBackfillOutcome> {
    validate_provider_registry_for_intake_tasks(intake_chain_tasks, provider_registry)?;
    let deployment_profile = deployment_profile_from_manifest_root(manifests_root);
    let lease_owner = format!("{}:bootstrap-backfill", default_backfill_lease_owner());
    let mut outcome = BootstrapBackfillOutcome {
        active_chain_count: intake_chain_tasks.len(),
        ..BootstrapBackfillOutcome::default()
    };

    for task in intake_chain_tasks {
        let skipped_unknown_start_targets =
            load_manifest_skipped_bootstrap_targets(pool, &task.chain).await?;
        outcome.skipped_unknown_start_target_count += skipped_unknown_start_targets.len();
        for skipped_target in &skipped_unknown_start_targets {
            info!(
                service = "indexer",
                command = "run",
                bootstrap_backfill_status = "skipped_unknown_start_target",
                chain = %task.chain,
                source_family = %skipped_target.source_family,
                contract_instance_id = %skipped_target.contract_instance_id,
                address = %skipped_target.address,
                skip_reason = %skipped_target.skip_reason,
                "manifest-declared bootstrap target is skipped because it has no declared start block"
            );
        }
        outcome
            .skipped_unknown_start_targets
            .extend(skipped_unknown_start_targets);

        let Some(provider) = provider_registry.provider_for(&task.chain) else {
            outcome.missing_provider_chain_count += 1;
            warn!(
                service = "indexer",
                command = "run",
                bootstrap_backfill_status = "idle_missing_provider",
                chain = %task.chain,
                intake_address_count = task.addresses.len(),
                "no provider source is configured for an active bootstrap chain; automatic bootstrap backfill will stay idle for this chain"
            );
            continue;
        };
        outcome.provider_configured_chain_count += 1;

        let heads = provider.fetch_chain_heads().await.with_context(|| {
            format!(
                "failed to fetch finite provider head for bootstrap backfill on chain {}",
                task.chain
            )
        })?;
        let provider_head_block = heads.canonical.block_number;
        let bootstrap_targets = load_manifest_declared_bootstrap_targets(pool, &task.chain).await?;
        outcome.eligible_target_count += bootstrap_targets.len();

        info!(
            service = "indexer",
            command = "run",
            bootstrap_backfill_status = "planning",
            chain = %task.chain,
            provider_head_block,
            bootstrap_backfill_range_policy = "manifest_declared_start_to_provider_head",
            hash_pinned_chunk_blocks,
            eligible_bootstrap_target_count = bootstrap_targets.len(),
            skipped_unknown_start_target_count = outcome.skipped_unknown_start_target_count,
            "manifest-declared bootstrap targets loaded"
        );

        let mut target_ranges = Vec::new();
        for target in bootstrap_targets {
            let Some(range) = bootstrap_target_range(&target, provider_head_block)? else {
                outcome.skipped_future_target_count += 1;
                info!(
                    service = "indexer",
                    command = "run",
                    bootstrap_backfill_status = "skipped_future_target",
                    chain = %task.chain,
                    source_family = %target.source_family,
                    contract_instance_id = %target.contract_instance_id,
                    address = %target.address,
                    effective_from_block = target.effective_from_block,
                    effective_to_block = target.effective_to_block,
                    provider_head_block,
                    bootstrap_backfill_range_policy = "manifest_declared_start_to_provider_head",
                    "manifest-declared bootstrap target starts after the provider bootstrap head"
                );
                continue;
            };

            let mut range = range;
            if let Some(stored_checkpoint) = load_bootstrap_target_checkpoint(
                pool,
                &deployment_profile,
                manifests_root,
                &task.chain,
                range,
                &target.contract_instance_id.to_string(),
            )
            .await?
            {
                if stored_checkpoint >= range.to_block {
                    info!(
                        service = "indexer",
                        command = "run",
                        bootstrap_backfill_status = "skipped_target_stored_checkpoint",
                        chain = %task.chain,
                        source_family = %target.source_family,
                        contract_instance_id = %target.contract_instance_id,
                        address = %target.address,
                        from_block = range.from_block,
                        to_block = range.to_block,
                        stored_checkpoint_block = stored_checkpoint,
                        "manifest-declared bootstrap target already has stored backfill checkpoint coverage"
                    );
                    continue;
                }
                if stored_checkpoint >= range.from_block {
                    let resumed_from_block = stored_checkpoint.checked_add(1).with_context(|| {
                        format!(
                            "stored bootstrap checkpoint {stored_checkpoint} overflowed while resuming target"
                        )
                    })?;
                    info!(
                        service = "indexer",
                        command = "run",
                        bootstrap_backfill_status = "resuming_target_after_stored_checkpoint",
                        chain = %task.chain,
                        source_family = %target.source_family,
                        contract_instance_id = %target.contract_instance_id,
                        address = %target.address,
                        from_block = range.from_block,
                        resumed_from_block,
                        to_block = range.to_block,
                        stored_checkpoint_block = stored_checkpoint,
                        "manifest-declared bootstrap target resumes after stored backfill checkpoint"
                    );
                    range = BackfillBlockRange::new(resumed_from_block, range.to_block)?;
                }
            }

            target_ranges.push(BootstrapBackfillTargetRange { target, range });
        }

        for segment in plan_bootstrap_backfill_segments(target_ranges)? {
            let segment_target_ids = bootstrap_segment_target_ids(&segment.targets);
            let mut segment_range = segment.range;
            if let Some(stored_checkpoint) = load_bootstrap_segment_checkpoint(
                pool,
                &deployment_profile,
                manifests_root,
                &task.chain,
                segment.range,
                &segment_target_ids,
            )
            .await?
            {
                if stored_checkpoint >= segment_range.to_block {
                    info!(
                        service = "indexer",
                        command = "run",
                        bootstrap_backfill_status = "skipped_stored_checkpoint",
                        chain = %task.chain,
                        from_block = segment.range.from_block,
                        to_block = segment.range.to_block,
                        stored_checkpoint_block = stored_checkpoint,
                        selected_target_count = segment.targets.len(),
                        "manifest-declared bootstrap segment already has stored backfill checkpoint coverage"
                    );
                    continue;
                }
                if stored_checkpoint >= segment_range.from_block {
                    let resumed_from_block = stored_checkpoint.checked_add(1).with_context(|| {
                        format!(
                            "stored bootstrap checkpoint {stored_checkpoint} overflowed while resuming"
                        )
                    })?;
                    info!(
                        service = "indexer",
                        command = "run",
                        bootstrap_backfill_status = "resuming_after_stored_checkpoint",
                        chain = %task.chain,
                        from_block = segment.range.from_block,
                        resumed_from_block,
                        to_block = segment.range.to_block,
                        stored_checkpoint_block = stored_checkpoint,
                        selected_target_count = segment.targets.len(),
                        "manifest-declared bootstrap segment resumes after stored backfill checkpoint"
                    );
                    segment_range =
                        BackfillBlockRange::new(resumed_from_block, segment_range.to_block)?;
                }
            }

            let mut source_plan = load_manifest_declared_watched_source_selector_plan(
                pool,
                &task.chain,
                WatchedSourceSelector::WatchedTargetSet(
                    segment
                        .targets
                        .iter()
                        .map(|target| WatchedTargetIdentity {
                            contract_instance_id: target.contract_instance_id,
                        })
                        .collect(),
                ),
                segment_range.from_block,
                segment_range.to_block,
            )
            .await
            .with_context(|| {
                format!(
                    "failed to build bootstrap watched-target source plan for chain {} range {}..={}",
                    task.chain, segment_range.from_block, segment_range.to_block
                )
            })?;
            narrow_manifest_bootstrap_source_plan(
                &mut source_plan,
                &segment.targets,
                segment_range,
            )?;

            let source_identity_hash = source_plan.source_identity_hash();
            let idempotency_key = bootstrap_backfill_idempotency_key(
                &deployment_profile,
                manifests_root,
                &task.chain,
                &source_identity_hash,
                segment_range,
            );
            let config = crate::backfill::BackfillJobRunConfig {
                deployment_profile: deployment_profile.clone(),
                idempotency_key,
                range: segment_range,
                lease_owner: lease_owner.clone(),
                lease_token: generated_backfill_lease_token()?,
                lease_expires_at: backfill_lease_expires_at(
                    BOOTSTRAP_BACKFILL_LEASE_DURATION_SECS,
                )?,
                hash_pinned_chunk_blocks,
                adapter_sync_mode,
            };

            let job_outcome =
                run_resumable_hash_pinned_backfill_job(pool, &source_plan, provider, config)
                    .await?;
            outcome.add_job(&job_outcome);
            if replay_completed_raw_ranges && job_outcome.raw_log_count > 0 {
                let replay_outcome = replay_raw_fact_normalized_events(
                    pool,
                    RawFactNormalizedEventReplayRequest {
                        deployment_profile: deployment_profile.clone(),
                        chain: task.chain.clone(),
                        selection: RawFactNormalizedEventReplaySelection::BlockRange {
                            from_block: job_outcome.from_block,
                            to_block: job_outcome.to_block,
                        },
                    },
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to replay normalized events after bootstrap raw backfill for chain {} range {}..={}",
                        task.chain, job_outcome.from_block, job_outcome.to_block
                    )
                })?;
                log_raw_fact_normalized_event_replay_outcome(&replay_outcome);
                outcome.normalized_replay_job_count += 1;
                outcome.normalized_replay_synced_count +=
                    replay_outcome.normalized_event_synced_count;
                outcome.normalized_replay_inserted_count +=
                    replay_outcome.normalized_event_inserted_count;
            }
        }
    }

    info!(
        service = "indexer",
        command = "run",
        bootstrap_backfill_status = "drained",
        active_chain_count = outcome.active_chain_count,
        provider_configured_chain_count = outcome.provider_configured_chain_count,
        missing_provider_chain_count = outcome.missing_provider_chain_count,
        eligible_bootstrap_target_count = outcome.eligible_target_count,
        skipped_unknown_start_target_count = outcome.skipped_unknown_start_target_count,
        drained_bootstrap_job_count = outcome.drained_job_count,
        skipped_future_target_count = outcome.skipped_future_target_count,
        bootstrap_backfill_range_policy = "manifest_declared_start_to_provider_head",
        hash_pinned_chunk_blocks,
        reserved_range_count = outcome.reserved_range_count,
        completed_range_count = outcome.completed_range_count,
        resolved_block_count = outcome.resolved_block_count,
        raw_block_count = outcome.raw_block_count,
        raw_transaction_count = outcome.raw_transaction_count,
        raw_receipt_count = outcome.raw_receipt_count,
        raw_log_count = outcome.raw_log_count,
        raw_code_hash_count = outcome.raw_code_hash_count,
        normalized_replay_job_count = outcome.normalized_replay_job_count,
        normalized_replay_synced_count = outcome.normalized_replay_synced_count,
        normalized_replay_inserted_count = outcome.normalized_replay_inserted_count,
        "startup bootstrap backfill jobs drained before live polling"
    );

    Ok(outcome)
}

pub(crate) fn bootstrap_backfill_idempotency_key(
    deployment_profile: &str,
    manifests_root: &Path,
    chain: &str,
    source_identity_hash: &str,
    range: BackfillBlockRange,
) -> String {
    format!(
        "indexer-bootstrap-backfill:v1:deployment_profile={deployment_profile}:manifest_root={}:chain={chain}:source_identity_hash={source_identity_hash}:from={}:to={}",
        manifests_root.display(),
        range.from_block,
        range.to_block
    )
}
