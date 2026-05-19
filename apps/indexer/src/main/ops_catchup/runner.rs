use std::time::Duration;

use anyhow::{Context, Result, bail};
use bigname_manifests::{
    WatchedSourceSelector, WatchedTargetIdentity, load_watched_contracts,
    load_watched_source_selector_plan,
};
use bigname_storage::{BackfillLifecycleStatus, fail_backfill_job};
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::{
    backfill::{
        BackfillBlockRange, BackfillJobRunConfig, create_hash_pinned_backfill_job,
        run_resumable_hash_pinned_backfill_job,
    },
    backfill_lease_expires_at, default_backfill_lease_owner, generated_backfill_lease_token,
    provider::{ChainProvider, ChainProviderOps, ProviderRegistry},
    runtime::{IntakeChainTask, validate_provider_registry_for_intake_tasks},
};

use super::{
    capacity::{CAPACITY_FAILURE_REASON, capacity_metadata, check_capacity},
    config::{OpsCatchupConfig, OpsCatchupOutcome},
    planning::{CatchupChunk, catchup_targets_for_chain, plan_catchup_chunks},
};

pub(crate) async fn run_ops_finalized_catchup(
    pool: &sqlx::PgPool,
    intake_chain_tasks: &[IntakeChainTask],
    provider_registry: &ProviderRegistry,
    config: OpsCatchupConfig,
) -> Result<OpsCatchupOutcome> {
    validate_provider_registry_for_intake_tasks(intake_chain_tasks, provider_registry)?;
    let mut outcome = OpsCatchupOutcome {
        active_chain_count: intake_chain_tasks.len(),
        ..OpsCatchupOutcome::default()
    };
    let mut iteration = 0_u64;

    loop {
        iteration += 1;
        outcome.follow_iteration_count += 1;
        run_ops_finalized_catchup_iteration(
            pool,
            intake_chain_tasks,
            provider_registry,
            &config,
            &mut outcome,
        )
        .await?;

        if !config.follow {
            break;
        }
        if config
            .follow_iterations
            .is_some_and(|limit| iteration >= limit)
        {
            break;
        }
        sleep(Duration::from_secs(config.follow_poll_interval_secs)).await;
    }

    Ok(outcome)
}

async fn run_ops_finalized_catchup_iteration(
    pool: &sqlx::PgPool,
    intake_chain_tasks: &[IntakeChainTask],
    provider_registry: &ProviderRegistry,
    config: &OpsCatchupConfig,
    outcome: &mut OpsCatchupOutcome,
) -> Result<()> {
    let watched_contracts = load_watched_contracts(pool).await?;

    for task in intake_chain_tasks {
        let (targets, skipped_unknown_start_targets) =
            catchup_targets_for_chain(&watched_contracts, &task.chain);
        let skipped_unknown_start_target_count = skipped_unknown_start_targets.len();
        outcome.skipped_unknown_start_target_count += skipped_unknown_start_target_count;
        if skipped_unknown_start_target_count > 0 {
            info!(
                service = "indexer",
                command = "ops-catchup",
                catchup_status = "skipped_unknown_start_target",
                chain = %task.chain,
                skipped_unknown_start_target_count,
                skip_reason = "unknown_start",
                "watched targets skipped because they have no admitted start block"
            );
        }

        let Some(provider) = provider_registry.provider_for(&task.chain) else {
            outcome.missing_provider_chain_count += 1;
            warn!(
                service = "indexer",
                command = "ops-catchup",
                catchup_status = "idle_missing_provider",
                chain = %task.chain,
                intake_address_count = task.addresses.len(),
                "no provider source is configured for an active catch-up chain"
            );
            continue;
        };
        outcome.provider_configured_chain_count += 1;

        let heads = provider
            .fetch_chain_heads()
            .await
            .with_context(|| format!("failed to fetch finalized head for {}", task.chain))?;
        let Some(finalized_head) = heads.finalized else {
            outcome.no_finalized_head_chain_count += 1;
            warn!(
                service = "indexer",
                command = "ops-catchup",
                catchup_status = "idle_no_finalized_head",
                chain = %task.chain,
                latest_block_number = heads.canonical.block_number,
                "provider did not return a finalized head for catch-up"
            );
            continue;
        };

        let (chunks, skipped_future_target_count) =
            plan_catchup_chunks(&targets, finalized_head.block_number, config.chunk_blocks)?;
        outcome.skipped_future_target_count += skipped_future_target_count;
        outcome.planned_chunk_count += chunks.len();
        for chunk in chunks {
            run_ops_finalized_catchup_chunk(
                pool,
                &task.chain,
                provider,
                config,
                &chunk,
                finalized_head.block_number,
                &finalized_head.block_hash,
                outcome,
            )
            .await?;
        }
    }

    Ok(())
}

async fn run_ops_finalized_catchup_chunk(
    pool: &sqlx::PgPool,
    chain: &str,
    provider: &ChainProvider,
    config: &OpsCatchupConfig,
    chunk: &CatchupChunk,
    finalized_head_block_number: i64,
    finalized_head_block_hash: &str,
    outcome: &mut OpsCatchupOutcome,
) -> Result<()> {
    let source_plan = load_watched_source_selector_plan(
        pool,
        chain,
        WatchedSourceSelector::WatchedTargetSet(
            chunk
                .targets
                .iter()
                .map(|target| WatchedTargetIdentity {
                    contract_instance_id: target.contract_instance_id,
                })
                .collect(),
        ),
        chunk.range.from_block,
        chunk.range.to_block,
    )
    .await?;
    let idempotency_key = ops_catchup_idempotency_key(
        &config.deployment_profile,
        chain,
        &source_plan.source_identity_hash(),
        chunk.range,
    );
    let run_config = BackfillJobRunConfig {
        deployment_profile: config.deployment_profile.clone(),
        idempotency_key,
        range: chunk.range,
        lease_owner: format!("{}:ops-finalized-catchup", default_backfill_lease_owner()),
        lease_token: generated_backfill_lease_token()?,
        lease_expires_at: backfill_lease_expires_at(config.lease_duration_secs)?,
        hash_pinned_chunk_blocks: crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS,
        adapter_sync_mode: crate::backfill::BackfillAdapterSyncMode::Inline,
        header_audit_mode: config.header_audit_mode,
    };
    let record = create_hash_pinned_backfill_job(pool, &source_plan, &run_config).await?;
    if record.job.status == BackfillLifecycleStatus::Completed {
        outcome.reused_completed_chunk_count += 1;
        return Ok(());
    }

    let capacity_snapshot = match check_capacity(pool, &config.capacity, chunk.range).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            outcome.capacity_check_count += 1;
            let metadata = capacity_metadata(
                "check_failed",
                &run_config,
                chunk.range,
                finalized_head_block_number,
                finalized_head_block_hash,
                &config.capacity,
                None,
                Some(&error),
            );
            fail_backfill_job(
                pool,
                record.job.backfill_job_id,
                CAPACITY_FAILURE_REASON,
                metadata,
            )
            .await?;
            return Err(error.context("recorded ops catch-up capacity check failure"));
        }
    };
    outcome.capacity_check_count += 1;

    if !capacity_snapshot.breach_reasons.is_empty() {
        let metadata = capacity_metadata(
            "breached",
            &run_config,
            chunk.range,
            finalized_head_block_number,
            finalized_head_block_hash,
            &config.capacity,
            Some(&capacity_snapshot),
            None,
        );
        fail_backfill_job(
            pool,
            record.job.backfill_job_id,
            CAPACITY_FAILURE_REASON,
            metadata,
        )
        .await?;
        error!(
            service = "indexer",
            command = "ops-catchup",
            catchup_status = "capacity_breached",
            backfill_job_id = record.job.backfill_job_id,
            chain,
            from_block = chunk.range.from_block,
            to_block = chunk.range.to_block,
            postgres_database_size_bytes = capacity_snapshot.postgres_database_size_bytes,
            postgres_max_bytes = config.capacity.postgres_max_bytes,
            writable_free_disk_path = %config.capacity.writable_free_disk_path.display(),
            writable_free_disk_bytes = capacity_snapshot.writable_free_disk_bytes,
            min_writable_free_disk_bytes = config.capacity.min_writable_free_disk_bytes,
            estimated_chunk_write_bytes = capacity_snapshot.estimated_chunk_write_bytes,
            capacity_breach_reasons = ?capacity_snapshot.breach_reasons,
            "ops catch-up chunk failed before range work because capacity is insufficient"
        );
        bail!(
            "ops catch-up capacity guard breached for {chain} range {}..={}",
            chunk.range.from_block,
            chunk.range.to_block
        );
    }

    let job_outcome =
        run_resumable_hash_pinned_backfill_job(pool, &source_plan, provider, run_config).await?;
    outcome.add_job(&job_outcome);
    Ok(())
}

pub(crate) fn ops_catchup_idempotency_key(
    deployment_profile: &str,
    chain: &str,
    source_identity_hash: &str,
    range: BackfillBlockRange,
) -> String {
    format!(
        "indexer-ops-finalized-catchup:v2:deployment_profile={deployment_profile}:chain={chain}:source_identity_hash={source_identity_hash}:from={}:to={}",
        range.from_block, range.to_block
    )
}
