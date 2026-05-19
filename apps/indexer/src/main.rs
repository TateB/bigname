#[path = "main/backfill.rs"]
mod backfill;
#[cfg(test)]
#[allow(dead_code, unused_imports)]
#[path = "main/tests/backfill.rs"]
mod backfill_tests;
#[path = "main/bootstrap_backfill.rs"]
mod bootstrap_backfill;
#[path = "main/cli.rs"]
mod cli;
#[path = "main/ens_v1_resolver.rs"]
mod ens_v1_resolver;
#[path = "main/normalized_replay_catchup.rs"]
mod normalized_replay_catchup;
#[path = "main/ops_catchup.rs"]
mod ops_catchup;
#[cfg(test)]
#[allow(dead_code, unused_imports)]
#[path = "main/tests/ops_catchup.rs"]
mod ops_catchup_tests;
mod provider;
#[path = "main/reconciliation.rs"]
mod reconciliation;
#[path = "main/repair.rs"]
mod repair;
#[path = "main/replay.rs"]
mod replay;
#[path = "main/rewind.rs"]
mod rewind;
#[path = "main/run.rs"]
mod run;
#[path = "main/run_mode.rs"]
mod run_mode;
#[path = "main/runtime.rs"]
mod runtime;
#[path = "main/source_scope.rs"]
mod source_scope;
#[cfg(test)]
#[path = "main/tests.rs"]
mod tests;

#[cfg(test)]
use std::path::PathBuf;

use anyhow::{Context, Result};
use backfill::{
    BackfillAdapterSyncMode, BackfillBlockRange, BackfillJobRunConfig,
    run_resumable_hash_pinned_backfill_job,
};
#[cfg(test)]
use bigname_manifests::{
    ManifestLoadStatus, ManifestLoadSummary, ManifestSyncStatus, ManifestSyncSummary,
    WatchedChainPlan, load_watched_chain_plan, load_watched_contract_summary,
};
use bigname_manifests::{WatchedSourceSelector, load_watched_source_selector_plan};
#[allow(unused_imports)]
use bigname_storage::{
    CanonicalityState, ChainCheckpoint, ChainCheckpointUpdate, CheckpointBlockRef, DatabaseConfig,
    RawCodeHash, RawLog, RawReceipt, RawTransaction, advance_chain_checkpoints,
    upsert_chain_lineage_blocks, upsert_raw_blocks, upsert_raw_code_hashes, upsert_raw_logs,
    upsert_raw_receipts, upsert_raw_transactions,
};
#[allow(unused_imports)]
use bootstrap_backfill::*;
use clap::Parser;
use cli::{
    BackfillArgs, Cli, Command, OpsCatchupArgs, RepairArgs, RepairCommand, ReplayArgs,
    ReplayCommand, ReplayNormalizedEventsArgs, RewindArgs,
};
#[allow(unused_imports)]
use provider::{
    ChainProviderKind, JsonRpcProvider, ProviderBlock, ProviderHeadSnapshot, ProviderRegistry,
};
#[allow(unused_imports)]
use reconciliation::*;
use repair::{EnsV1TextRecordRepairConfig, repair_ens_v1_text_records_from_provider};
pub(crate) use replay::{
    backfill_lease_expires_at, default_backfill_lease_owner, deployment_profile_from_manifest_root,
    generated_backfill_lease_token,
};
use replay::{backfill_source_selector, replay_normalized_events_selection};
#[allow(unused_imports)]
use runtime::*;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing("bigname-indexer");

    match Cli::parse().command {
        Command::Run(args) => run::run(args).await,
        Command::Backfill(args) => run_backfill(args).await,
        Command::OpsCatchup(args) => run_ops_catchup(args).await,
        Command::Replay(args) => run_replay(args).await,
        Command::Rewind(args) => run_rewind(args).await.map(|_| ()),
        Command::Repair(args) => run_repair(args).await,
    }
}

async fn run_backfill(args: BackfillArgs) -> Result<()> {
    let range = BackfillBlockRange::new(args.from_block, args.to_block)?;
    let manifest_repository = load_manifest_repository(&args.manifests_root)?;
    let manifest_summary = manifest_repository.summary().clone();
    log_manifest_summary(&manifest_summary);
    ensure_manifest_root_ready(&manifest_summary)?;

    let pool = bigname_storage::connect(&args.database).await?;
    let selector = backfill_source_selector(&args)?;
    let needs_full_runtime_plan =
        matches!(selector, WatchedSourceSelector::WholeActiveWatchedChain);
    let manifest_runtime_state = if needs_full_runtime_plan {
        build_manifest_runtime_state(&pool, &manifest_repository).await?
    } else {
        build_manifest_runtime_state_with_watch_scope(
            &pool,
            &manifest_repository,
            RuntimeWatchScope::ManifestDeclaredOnly,
        )
        .await?
    };
    log_manifest_runtime_state(&manifest_runtime_state);
    log_watched_chain_plan("backfill", &manifest_runtime_state.watched_chain_plan);
    let provider_registry =
        ProviderRegistry::from_sources(&args.chain_rpc_urls, &args.chain_reth_db_sources)?;
    provider_registry.ensure_configured_chains_admitted(
        manifest_runtime_state
            .watched_chain_plan
            .iter()
            .map(|chain| chain.chain.as_str()),
    )?;
    let source_plan = load_watched_source_selector_plan(
        &pool,
        &args.chain,
        selector,
        range.from_block,
        range.to_block,
    )
    .await?;
    info!(
        service = "indexer",
        command = "backfill",
        chain = %args.chain,
        selector_kind = source_plan.selector_kind.as_str(),
        selected_target_count = source_plan.selected_targets.len(),
        from_block = range.from_block,
        to_block = range.to_block,
        provider_configured_chain_count = provider_registry.configured_chain_count(),
        json_rpc_provider_configured_chain_count =
            provider_registry.configured_chain_count_by_kind(ChainProviderKind::JsonRpc),
        reth_db_provider_configured_chain_count =
            provider_registry.configured_chain_count_by_kind(ChainProviderKind::RethDb),
        "provider registry loaded for hash-pinned backfill"
    );

    let provider = provider_registry.provider_for(&args.chain).ok_or_else(|| {
        anyhow::anyhow!(
            "no provider source configured for watched chain {}; pass --chain-rpc-url {}=<url> or --chain-reth-db-source {}=<reth-datadir>",
            args.chain,
            args.chain,
            args.chain
        )
    })?;

    let deployment_profile = args
        .deployment_profile
        .unwrap_or_else(|| deployment_profile_from_manifest_root(&args.manifests_root));
    let lease_owner = args
        .lease_owner
        .unwrap_or_else(default_backfill_lease_owner);
    let lease_token = match args.lease_token {
        Some(lease_token) => lease_token,
        None => generated_backfill_lease_token()?,
    };
    let lease_expires_at = backfill_lease_expires_at(args.lease_duration_secs)?;
    let adapter_sync_mode = BackfillAdapterSyncMode::parse(&args.hash_pinned_adapter_sync)?;
    let header_audit_mode =
        HeaderAuditMode::from_retain_audit_fields(args.retain_header_audit_fields);
    let config = BackfillJobRunConfig {
        deployment_profile,
        idempotency_key: args.idempotency_key,
        range,
        lease_owner,
        lease_token,
        lease_expires_at,
        hash_pinned_chunk_blocks: args.hash_pinned_chunk_blocks,
        adapter_sync_mode: adapter_sync_mode.hash_pinned_backfill_mode(),
        header_audit_mode,
    };

    run_resumable_hash_pinned_backfill_job(&pool, &source_plan, provider, config).await?;
    Ok(())
}

async fn run_ops_catchup(args: OpsCatchupArgs) -> Result<()> {
    let config = ops_catchup::OpsCatchupConfig::from_args(&args)?;
    let manifest_repository = load_manifest_repository(&args.manifests_root)?;
    let manifest_summary = manifest_repository.summary().clone();
    log_manifest_summary(&manifest_summary);
    ensure_manifest_root_ready(&manifest_summary)?;

    let pool = bigname_storage::connect(&args.database).await?;
    let manifest_runtime_state = build_manifest_runtime_state(&pool, &manifest_repository).await?;
    log_manifest_runtime_state(&manifest_runtime_state);
    log_watched_chain_plan("ops-catchup", &manifest_runtime_state.watched_chain_plan);
    let intake_chain_tasks =
        sync_intake_chain_tasks(&pool, &manifest_runtime_state.watched_chain_plan).await?;
    log_intake_chain_tasks("ops-catchup", &intake_chain_tasks);
    let provider_registry =
        ProviderRegistry::from_sources(&args.chain_rpc_urls, &args.chain_reth_db_sources)?;
    validate_provider_registry_for_intake_tasks(&intake_chain_tasks, &provider_registry)?;
    log_provider_registry("ops-catchup", &intake_chain_tasks, &provider_registry);

    ops_catchup::run_ops_finalized_catchup(&pool, &intake_chain_tasks, &provider_registry, config)
        .await?;
    Ok(())
}

async fn run_replay(args: ReplayArgs) -> Result<()> {
    match args.command {
        ReplayCommand::NormalizedEvents(args) => run_replay_normalized_events(args).await,
    }
}

async fn run_rewind(args: RewindArgs) -> Result<rewind::RewindOutcome> {
    let outcome = rewind::run_rewind(args).await?;
    tracing::info!(
        service = "indexer",
        command = "rewind",
        deployment_profile = %outcome.deployment_profile,
        chain = %outcome.chain,
        from_block_hash = %outcome.from_block_hash,
        ancestor_block_hash = %outcome.ancestor_block_hash,
        ancestor_block_number = outcome.ancestor_block_number,
        orphaned_lineage_count = outcome.orphaned_lineage_count,
        orphaned_raw_code_hash_count = outcome.orphaned_raw_fact_counts.code_hash_count,
        orphaned_raw_transaction_count = outcome.orphaned_raw_fact_counts.transaction_count,
        orphaned_raw_receipt_count = outcome.orphaned_raw_fact_counts.receipt_count,
        orphaned_raw_log_count = outcome.orphaned_raw_fact_counts.log_count,
        orphaned_normalized_event_count = outcome.orphaned_normalized_event_count,
        orphaned_token_lineage_count = outcome.orphaned_identity_counts.token_lineage_count,
        orphaned_resource_count = outcome.orphaned_identity_counts.resource_count,
        orphaned_name_surface_count = outcome.orphaned_identity_counts.name_surface_count,
        orphaned_surface_binding_count = outcome.orphaned_identity_counts.surface_binding_count,
        invalidated_execution_outcome_count = outcome.invalidated_execution_outcome_count,
        "rewind completed"
    );
    Ok(outcome)
}

async fn run_repair(args: RepairArgs) -> Result<()> {
    match args.command {
        RepairCommand::EnsV1TextRecords(args) => {
            let pool = bigname_storage::connect(&args.database).await?;
            let provider_registry =
                ProviderRegistry::from_sources(&args.chain_rpc_urls, &args.chain_reth_db_sources)?;
            let provider = provider_registry.provider_for(&args.chain).with_context(|| {
                format!(
                    "no provider source configured for {}; pass --chain-reth-db-source {}=<datadir> or --chain-rpc-url {}=<url>",
                    args.chain, args.chain, args.chain
                )
            })?;
            let outcome = repair_ens_v1_text_records_from_provider(
                &pool,
                provider,
                EnsV1TextRecordRepairConfig {
                    chain: args.chain,
                    from_block: args.from_block,
                    to_block: args.to_block,
                    chunk_blocks: args.chunk_blocks,
                    candidate_page_size: args.candidate_page_size,
                },
            )
            .await?;
            tracing::info!(
                service = "indexer",
                command = "repair ens-v1-text-records",
                chain = %outcome.chain,
                from_block = outcome.from_block,
                to_block = outcome.to_block,
                candidate_count = outcome.candidate_count,
                fetched_log_count = outcome.fetched_log_count,
                matched_log_count = outcome.matched_log_count,
                repaired_event_count = outcome.repaired_event_count,
                missing_log_count = outcome.missing_log_count,
                skipped_decode_count = outcome.skipped_decode_count,
                "ENSv1 text record normalized-event repair completed"
            );
            Ok(())
        }
    }
}

async fn run_replay_normalized_events(args: ReplayNormalizedEventsArgs) -> Result<()> {
    let selection = replay_normalized_events_selection(&args)?;
    let pool = bigname_storage::connect(&args.database).await?;
    let outcome = replay_raw_fact_normalized_events(
        &pool,
        RawFactNormalizedEventReplayRequest {
            deployment_profile: args.deployment_profile,
            chain: args.chain,
            selection,
        },
    )
    .await?;

    log_raw_fact_normalized_event_replay_outcome(&outcome);
    Ok(())
}
