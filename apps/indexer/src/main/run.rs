use anyhow::Result;
use tracing::info;

use crate::{
    backfill::BackfillAdapterSyncMode,
    bootstrap_backfill::run_startup_bootstrap_backfills,
    cli::RunArgs,
    normalized_replay_catchup::{NormalizedReplayCatchupConfig, run_normalized_replay_catchup},
    provider::{ChainProviderKind, ProviderRegistry},
    reconciliation::HeaderAuditMode,
    replay::deployment_profile_from_manifest_root,
    run_mode::IndexerRunMode,
    runtime::{
        build_manifest_runtime_state_with_watch_scope, ensure_manifest_root_ready,
        intake_runtime_state, load_manifest_repository, log_intake_chain_tasks,
        log_manifest_runtime_state, log_manifest_summary, log_provider_registry,
        log_watched_chain_plan, manifest_normalized_event_kind_count, run_poll_loop,
        sync_adapter_owned_raw_log_state, sync_intake_chain_tasks,
        validate_provider_registry_for_intake_tasks, watched_chain_plan_state,
    },
};

pub(crate) async fn run(args: RunArgs) -> Result<()> {
    let manifest_repository = load_manifest_repository(&args.manifests_root)?;
    let manifest_summary = manifest_repository.summary().clone();
    log_manifest_summary(&manifest_summary);
    ensure_manifest_root_ready(&manifest_summary)?;

    let pool = bigname_storage::connect(&args.database).await?;
    let adapter_sync_mode = BackfillAdapterSyncMode::parse(&args.hash_pinned_adapter_sync)?;
    let header_audit_mode =
        HeaderAuditMode::from_retain_audit_fields(args.retain_header_audit_fields);
    let run_mode = IndexerRunMode::new(adapter_sync_mode, args.normalized_replay_catchup_enabled);
    let manifest_runtime_state = build_manifest_runtime_state_with_watch_scope(
        &pool,
        &manifest_repository,
        run_mode.runtime_watch_scope,
    )
    .await?;
    if run_mode.sync_adapter_before_startup_backfill {
        sync_adapter_owned_raw_log_state(&pool, &manifest_runtime_state.watched_chain_plan).await?;
    } else {
        info!(
            service = "indexer",
            adapter_sync_mode = adapter_sync_mode.as_str(),
            runtime_watch_scope = run_mode.runtime_watch_scope.as_str(),
            "startup adapter-owned raw-log sync deferred until bootstrap backfill drains"
        );
    }
    log_manifest_runtime_state(&manifest_runtime_state);
    log_watched_chain_plan("startup", &manifest_runtime_state.watched_chain_plan);
    let watched_chain_plan_state =
        watched_chain_plan_state(&manifest_runtime_state.watched_chain_plan);
    let intake_chain_tasks =
        sync_intake_chain_tasks(&pool, &manifest_runtime_state.watched_chain_plan).await?;
    log_intake_chain_tasks("startup", &intake_chain_tasks);
    let intake_runtime_state = intake_runtime_state(&intake_chain_tasks);
    let provider_registry =
        ProviderRegistry::from_sources(&args.chain_rpc_urls, &args.chain_reth_db_sources)?;
    validate_provider_registry_for_intake_tasks(&intake_chain_tasks, &provider_registry)?;
    log_provider_registry("startup", &intake_chain_tasks, &provider_registry);
    // Automatic normalized catch-up replays bounded chunks after raw bootstrap drains.
    let replay_completed_startup_raw_ranges = false;
    let bootstrap_backfill_outcome = run_startup_bootstrap_backfills(
        &pool,
        &args.manifests_root,
        &intake_chain_tasks,
        &provider_registry,
        args.hash_pinned_chunk_blocks,
        run_mode.startup_backfill_adapter_sync_mode,
        replay_completed_startup_raw_ranges,
        header_audit_mode,
        args.bootstrap_backfill_workers,
        args.bootstrap_backfill_range_blocks,
    )
    .await?;
    if run_mode.sync_adapter_after_startup_backfill {
        info!(
            service = "indexer",
            adapter_sync_mode = adapter_sync_mode.as_str(),
            effective_backfill_adapter_sync_mode =
                run_mode.startup_backfill_adapter_sync_mode.as_str(),
            "startup bootstrap backfill drained; syncing adapter-owned raw-log state before live polling"
        );
        sync_adapter_owned_raw_log_state(&pool, &manifest_runtime_state.watched_chain_plan).await?;
    }
    if run_mode.normalized_replay_catchup_enabled {
        let catchup_config = NormalizedReplayCatchupConfig::new(
            deployment_profile_from_manifest_root(&args.manifests_root),
            intake_chain_tasks
                .iter()
                .map(|task| task.chain.clone())
                .collect::<Vec<_>>(),
            args.normalized_replay_catchup_chunk_blocks,
            args.normalized_replay_catchup_max_logs_per_chunk,
            args.normalized_replay_catchup_poll_interval_secs,
        )?
        .with_defer_projection_indexes(args.normalized_replay_defer_projection_indexes);
        let catchup_pool = pool.clone();
        tokio::spawn(async move {
            if let Err(error) = run_normalized_replay_catchup(catchup_pool, catchup_config).await {
                tracing::warn!(
                    service = "indexer",
                    error = ?error,
                    "automatic normalized-event replay catch-up task exited"
                );
            }
        });
    }

    info!(
        service = "indexer",
        phase = bigname_domain::bootstrap_phase(),
        manifest_loader_status = bigname_manifests::bootstrap_status(),
        manifests_root = %manifest_runtime_state.manifest_summary.root.display(),
        manifests_status = manifest_runtime_state.manifest_summary.status.as_str(),
        manifest_namespace_count = manifest_runtime_state.manifest_summary.namespace_count,
        manifest_source_family_count = manifest_runtime_state.manifest_summary.source_family_count,
        manifest_count = manifest_runtime_state.manifest_summary.manifest_count,
        manifest_sync_status = manifest_runtime_state.sync_summary.status.as_str(),
        synced_manifest_count = manifest_runtime_state.sync_summary.synced_manifest_count,
        synced_active_manifest_count = manifest_runtime_state.sync_summary.active_manifest_count,
        synced_root_count = manifest_runtime_state.sync_summary.root_count,
        synced_contract_count = manifest_runtime_state.sync_summary.contract_count,
        synced_capability_count = manifest_runtime_state.sync_summary.capability_count,
        synced_discovery_rule_count = manifest_runtime_state.sync_summary.discovery_rule_count,
        removed_manifest_count = manifest_runtime_state.sync_summary.removed_manifest_count,
        cleared_discovery_edge_count = manifest_runtime_state.sync_summary.cleared_discovery_edge_count,
        stored_active_manifest_count = manifest_runtime_state.discovery_admission.active_manifest_count,
        stored_active_root_count = manifest_runtime_state.discovery_admission.active_root_count,
        stored_active_contract_count = manifest_runtime_state.discovery_admission.active_contract_count,
        stored_active_rule_count = manifest_runtime_state.discovery_admission.active_rule_count,
        normalized_event_sync_total_count = manifest_runtime_state.manifest_normalized_event_summary.total_synced_count,
        normalized_event_inserted_total_count = manifest_runtime_state.manifest_normalized_event_summary.total_inserted_count,
        normalized_event_kind_count = manifest_runtime_state.manifest_normalized_event_summary.by_kind.len(),
        source_manifest_updated_event_count = manifest_normalized_event_kind_count(
            &manifest_runtime_state.manifest_normalized_event_summary,
            "SourceManifestUpdated"
        ),
        capability_changed_event_count = manifest_normalized_event_kind_count(
            &manifest_runtime_state.manifest_normalized_event_summary,
            "CapabilityChanged"
        ),
        proxy_implementation_changed_event_count = manifest_normalized_event_kind_count(
            &manifest_runtime_state.manifest_normalized_event_summary,
            "ProxyImplementationChanged"
        ),
        watched_entry_count_total = manifest_runtime_state.watched_contract_summary.source_entry_count,
        watched_manifest_root_entry_count = manifest_runtime_state.watched_contract_summary.manifest_root_count,
        watched_manifest_contract_entry_count = manifest_runtime_state.watched_contract_summary.manifest_contract_count,
        watched_discovery_edge_entry_count = manifest_runtime_state.watched_contract_summary.discovery_edge_count,
        watched_chain_count = manifest_runtime_state.watched_contract_summary.chains.len(),
        watched_runtime_chain_count = watched_chain_plan_state.chain_count,
        watched_runtime_address_count = watched_chain_plan_state.address_count,
        watched_runtime_entry_count = watched_chain_plan_state.entry_count,
        intake_runtime_chain_count = intake_runtime_state.chain_count,
        intake_runtime_address_count = intake_runtime_state.address_count,
        intake_runtime_entry_count = intake_runtime_state.entry_count,
        intake_cold_start_chain_count = intake_runtime_state.cold_start_chain_count,
        intake_resumable_chain_count = intake_runtime_state.resumable_chain_count,
        intake_safe_checkpoint_chain_count = intake_runtime_state.safe_checkpoint_chain_count,
        intake_finalized_checkpoint_chain_count = intake_runtime_state.finalized_checkpoint_chain_count,
        provider_configured_chain_count = provider_registry.configured_chain_count(),
        json_rpc_provider_configured_chain_count =
            provider_registry.configured_chain_count_by_kind(ChainProviderKind::JsonRpc),
        reth_db_provider_configured_chain_count =
            provider_registry.configured_chain_count_by_kind(ChainProviderKind::RethDb),
        bootstrap_backfill_active_chain_count = bootstrap_backfill_outcome.active_chain_count,
        bootstrap_backfill_provider_configured_chain_count = bootstrap_backfill_outcome.provider_configured_chain_count,
        bootstrap_backfill_missing_provider_chain_count = bootstrap_backfill_outcome.missing_provider_chain_count,
        bootstrap_backfill_eligible_target_count = bootstrap_backfill_outcome.eligible_target_count,
        bootstrap_backfill_drained_job_count = bootstrap_backfill_outcome.drained_job_count,
        bootstrap_backfill_skipped_future_target_count = bootstrap_backfill_outcome.skipped_future_target_count,
        bootstrap_backfill_reserved_range_count = bootstrap_backfill_outcome.reserved_range_count,
        bootstrap_backfill_completed_range_count = bootstrap_backfill_outcome.completed_range_count,
        bootstrap_backfill_range_policy = "manifest_declared_start_to_provider_head",
        bootstrap_backfill_workers = bootstrap_backfill_outcome.requested_worker_count,
        effective_bootstrap_backfill_workers = bootstrap_backfill_outcome.effective_worker_count,
        bootstrap_backfill_range_blocks = bootstrap_backfill_outcome.range_partition_block_count,
        hash_pinned_chunk_blocks = args.hash_pinned_chunk_blocks,
        hash_pinned_adapter_sync = adapter_sync_mode.as_str(),
        header_audit_mode = header_audit_mode.as_str(),
        effective_hash_pinned_backfill_adapter_sync =
            run_mode.startup_backfill_adapter_sync_mode.as_str(),
        live_poll_adapter_sync = run_mode.live_poll_adapter_sync_enabled,
        live_poll_adapter_sync_after_normalized_replay_catchup =
            run_mode.live_poll_adapter_sync_after_normalized_replay_catchup,
        normalized_replay_catchup_enabled = run_mode.normalized_replay_catchup_enabled,
        normalized_replay_catchup_chunk_blocks = args.normalized_replay_catchup_chunk_blocks,
        normalized_replay_catchup_max_logs_per_chunk = args.normalized_replay_catchup_max_logs_per_chunk,
        normalized_replay_catchup_poll_interval_secs = args.normalized_replay_catchup_poll_interval_secs,
        normalized_replay_defer_projection_indexes = args.normalized_replay_defer_projection_indexes,
        adapter_sync_on_manifest_refresh = run_mode.broad_runtime_refresh_enabled,
        manifest_observation_refresh_enabled = run_mode.broad_runtime_refresh_enabled,
        discovery_refresh_enabled = run_mode.broad_runtime_refresh_enabled,
        watched_plan_refresh_interval_secs = args.poll_interval_secs,
        adapter_status = bigname_adapters::bootstrap_status(),
        poll_interval_secs = args.poll_interval_secs,
        runtime_watch_scope = run_mode.runtime_watch_scope.as_str(),
        "indexer booted"
    );

    run_poll_loop(
        &pool,
        args.manifests_root,
        manifest_runtime_state,
        intake_chain_tasks,
        &provider_registry,
        args.poll_interval_secs,
        run_mode.runtime_watch_scope,
        run_mode.broad_runtime_refresh_enabled,
        run_mode.live_poll_adapter_sync_enabled,
        run_mode.live_poll_adapter_sync_after_normalized_replay_catchup,
        run_mode.broad_runtime_refresh_enabled,
        run_mode.broad_runtime_refresh_enabled,
        header_audit_mode,
    )
    .await
}
