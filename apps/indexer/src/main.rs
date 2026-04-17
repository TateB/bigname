mod provider;

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use bigname_adapters::{
    BlockDerivedNormalizedEventSyncSummary, ManifestNormalizedEventSyncSummary,
};
use bigname_manifests::{
    DiscoveryAdmissionState, ManifestLoadStatus, ManifestLoadSummary, ManifestRepository,
    ManifestSyncStatus, ManifestSyncSummary, WatchedChainPlan, WatchedContractSummary,
    load_watched_chain_plan, load_watched_contract_summary,
};
use bigname_storage::{
    CanonicalityState, ChainCheckpoint, ChainCheckpointUpdate, ChainLineageBlock,
    CheckpointBlockRef, DatabaseConfig, RawBlock, RawCodeHash, RawLog, RawReceipt, RawTransaction,
    advance_chain_checkpoints, load_chain_lineage_block, load_raw_block, load_raw_blocks_by_hashes,
    load_raw_code_hash_counts_by_block_hashes, mark_block_derived_normalized_events_range_orphaned,
    mark_chain_lineage_range_orphaned, mark_raw_block_facts_range_orphaned, sync_chain_checkpoints,
    upsert_chain_lineage_blocks, upsert_raw_blocks, upsert_raw_code_hashes, upsert_raw_logs,
    upsert_raw_receipts, upsert_raw_transactions,
};
use clap::{Args, Parser, Subcommand};
use provider::{
    ProviderBlock, ProviderBlockBundle, ProviderBlockSelection, ProviderCodeObservation,
    ProviderHeadSnapshot, ProviderLog, ProviderReceipt, ProviderRegistry, ProviderTransaction,
};
use sha3::{Digest, Keccak256};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const MAX_PARENT_FETCH_DEPTH: usize = 32;

#[derive(Parser, Debug)]
#[command(
    name = "bigname-indexer",
    about = "Bootstrap indexer process for bigname"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Run(RunArgs),
}

#[derive(Args, Debug)]
struct RunArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_MANIFESTS_ROOT",
        default_value = "manifests"
    )]
    manifests_root: PathBuf,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_POLL_INTERVAL_SECS",
        default_value_t = 5_u64
    )]
    poll_interval_secs: u64,
    #[arg(
        long = "chain-rpc-url",
        env = "BIGNAME_INDEXER_CHAIN_RPC_URLS",
        value_delimiter = ','
    )]
    chain_rpc_urls: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing("bigname-indexer");

    match Cli::parse().command {
        Command::Run(args) => run(args).await,
    }
}

async fn run(args: RunArgs) -> Result<()> {
    let manifest_repository = load_manifest_repository(&args.manifests_root)?;
    let manifest_summary = manifest_repository.summary().clone();
    log_manifest_summary(&manifest_summary);
    ensure_manifest_root_ready(&manifest_summary)?;

    let pool = bigname_storage::connect(&args.database).await?;
    let manifest_runtime_state = build_manifest_runtime_state(&pool, &manifest_repository).await?;
    log_manifest_runtime_state(&manifest_runtime_state);
    log_watched_chain_plan("startup", &manifest_runtime_state.watched_chain_plan);
    let watched_chain_plan_state =
        watched_chain_plan_state(&manifest_runtime_state.watched_chain_plan);
    let intake_chain_tasks =
        sync_intake_chain_tasks(&pool, &manifest_runtime_state.watched_chain_plan).await?;
    log_intake_chain_tasks("startup", &intake_chain_tasks);
    let intake_runtime_state = intake_runtime_state(&intake_chain_tasks);
    let provider_registry = ProviderRegistry::from_chain_rpc_urls(&args.chain_rpc_urls)?;
    log_provider_registry("startup", &intake_chain_tasks, &provider_registry);

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
        rpc_configured_chain_count = provider_registry.configured_chain_count(),
        watched_plan_refresh_interval_secs = args.poll_interval_secs,
        adapter_status = bigname_adapters::bootstrap_status(),
        poll_interval_secs = args.poll_interval_secs,
        "indexer booted"
    );

    run_poll_loop(
        &pool,
        args.manifests_root,
        manifest_runtime_state,
        intake_chain_tasks,
        &provider_registry,
        args.poll_interval_secs,
    )
    .await
}

fn load_manifest_repository(manifests_root: &Path) -> Result<ManifestRepository> {
    bigname_manifests::load_repository(manifests_root).with_context(|| {
        format!(
            "failed to load repository manifests from {}",
            manifests_root.display()
        )
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DiscoveryAdmissionSnapshot {
    active_manifest_count: usize,
    active_root_count: usize,
    active_contract_count: usize,
    active_rule_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ManifestRuntimeState {
    manifest_summary: ManifestLoadSummary,
    sync_summary: ManifestSyncSummary,
    discovery_admission: DiscoveryAdmissionSnapshot,
    manifest_normalized_event_summary: ManifestNormalizedEventSyncSummary,
    watched_contract_summary: WatchedContractSummary,
    watched_chain_plan: Vec<WatchedChainPlan>,
}

async fn build_manifest_runtime_state(
    pool: &sqlx::PgPool,
    manifest_repository: &ManifestRepository,
) -> Result<ManifestRuntimeState> {
    let manifest_summary = manifest_repository.summary().clone();
    let sync_summary = bigname_manifests::sync_repository(pool, manifest_repository).await?;
    let admission_state = bigname_manifests::load_discovery_admission_state(pool).await?;
    verify_stored_manifest_state(&sync_summary, &admission_state)?;
    let manifest_normalized_event_summary =
        bigname_adapters::sync_manifest_normalized_events(pool).await?;
    let watched_contract_summary = load_watched_contract_summary(pool).await?;
    let watched_chain_plan = load_watched_chain_plan(pool).await?;

    Ok(ManifestRuntimeState {
        manifest_summary,
        sync_summary,
        discovery_admission: discovery_admission_snapshot(&admission_state),
        manifest_normalized_event_summary,
        watched_contract_summary,
        watched_chain_plan,
    })
}

fn discovery_admission_snapshot(state: &DiscoveryAdmissionState) -> DiscoveryAdmissionSnapshot {
    DiscoveryAdmissionSnapshot {
        active_manifest_count: state.active_manifest_count,
        active_root_count: state.active_root_count,
        active_contract_count: state.active_contract_count,
        active_rule_count: state.active_rule_count,
    }
}

fn log_manifest_runtime_state(state: &ManifestRuntimeState) {
    log_manifest_sync_summary(&state.sync_summary);
    log_discovery_admission_state(&state.discovery_admission);
    log_manifest_normalized_event_summary(&state.manifest_normalized_event_summary);
    log_watched_contract_summary(&state.watched_contract_summary);
}

fn log_manifest_summary(summary: &ManifestLoadSummary) {
    match summary.status {
        ManifestLoadStatus::Loaded => info!(
            service = "indexer",
            manifests_root = %summary.root.display(),
            manifests_status = summary.status.as_str(),
            manifest_namespace_count = summary.namespace_count,
            manifest_source_family_count = summary.source_family_count,
            manifest_count = summary.manifest_count,
            "repository manifests loaded"
        ),
        ManifestLoadStatus::Empty => warn!(
            service = "indexer",
            manifests_root = %summary.root.display(),
            manifests_status = summary.status.as_str(),
            manifest_namespace_count = summary.namespace_count,
            manifest_source_family_count = summary.source_family_count,
            manifest_count = summary.manifest_count,
            "manifests root is present but empty; syncing will clear stored manifest state"
        ),
        ManifestLoadStatus::MissingRoot => warn!(
            service = "indexer",
            manifests_root = %summary.root.display(),
            manifests_status = summary.status.as_str(),
            manifest_namespace_count = summary.namespace_count,
            manifest_source_family_count = summary.source_family_count,
            manifest_count = summary.manifest_count,
            "manifests root does not exist"
        ),
        ManifestLoadStatus::InvalidRoot => warn!(
            service = "indexer",
            manifests_root = %summary.root.display(),
            manifests_status = summary.status.as_str(),
            manifest_namespace_count = summary.namespace_count,
            manifest_source_family_count = summary.source_family_count,
            manifest_count = summary.manifest_count,
            "manifests root is not a directory"
        ),
    }
}

fn log_manifest_sync_summary(summary: &ManifestSyncSummary) {
    match summary.status {
        ManifestSyncStatus::Synced => info!(
            service = "indexer",
            manifest_sync_status = summary.status.as_str(),
            synced_manifest_count = summary.synced_manifest_count,
            synced_active_manifest_count = summary.active_manifest_count,
            synced_root_count = summary.root_count,
            synced_contract_count = summary.contract_count,
            synced_capability_count = summary.capability_count,
            synced_discovery_rule_count = summary.discovery_rule_count,
            removed_manifest_count = summary.removed_manifest_count,
            cleared_discovery_edge_count = summary.cleared_discovery_edge_count,
            "repository manifests synced into storage"
        ),
        ManifestSyncStatus::SkippedMissingRoot | ManifestSyncStatus::SkippedInvalidRoot => warn!(
            service = "indexer",
            manifest_sync_status = summary.status.as_str(),
            "manifest sync skipped because the repository root was not usable"
        ),
    }
}

fn ensure_manifest_root_ready(summary: &ManifestLoadSummary) -> Result<()> {
    match summary.status {
        ManifestLoadStatus::Loaded | ManifestLoadStatus::Empty => Ok(()),
        ManifestLoadStatus::MissingRoot => bail!(
            "manifests root {} does not exist; refusing to boot on stale stored manifest state",
            summary.root.display()
        ),
        ManifestLoadStatus::InvalidRoot => bail!(
            "manifests root {} is not a directory; refusing to boot on stale stored manifest state",
            summary.root.display()
        ),
    }
}

fn verify_stored_manifest_state(
    sync_summary: &ManifestSyncSummary,
    admission_state: &DiscoveryAdmissionState,
) -> Result<()> {
    if sync_summary.status == ManifestSyncStatus::Synced
        && sync_summary.active_manifest_count != admission_state.active_manifest_count
    {
        bail!(
            "stored active manifest count {} does not match the synced active manifest count {}",
            admission_state.active_manifest_count,
            sync_summary.active_manifest_count
        );
    }

    Ok(())
}

fn log_discovery_admission_state(state: &DiscoveryAdmissionSnapshot) {
    info!(
        service = "indexer",
        stored_active_manifest_count = state.active_manifest_count,
        stored_active_root_count = state.active_root_count,
        stored_active_contract_count = state.active_contract_count,
        stored_active_rule_count = state.active_rule_count,
        "discovery admission rebuilt from stored manifest state"
    );
}

fn log_manifest_normalized_event_summary(summary: &ManifestNormalizedEventSyncSummary) {
    info!(
        service = "indexer",
        normalized_event_sync_total_count = summary.total_synced_count,
        normalized_event_inserted_total_count = summary.total_inserted_count,
        normalized_event_kind_count = summary.by_kind.len(),
        "adapter-owned manifest normalized events synced from stored manifest state"
    );

    for (event_kind, kind_summary) in &summary.by_kind {
        info!(
            service = "indexer",
            event_kind,
            normalized_event_sync_count = kind_summary.synced_count,
            normalized_event_inserted_count = kind_summary.inserted_count,
            "manifest normalized-event kind synced"
        );
    }
}

fn log_block_derived_normalized_event_summary(
    chain: &str,
    summary: &BlockDerivedNormalizedEventSyncSummary,
) {
    if summary.scanned_log_count == 0 && summary.total_synced_count == 0 {
        return;
    }

    info!(
        service = "indexer",
        chain,
        scanned_raw_log_count = summary.scanned_log_count,
        matched_raw_log_count = summary.matched_log_count,
        normalized_event_sync_total_count = summary.total_synced_count,
        normalized_event_inserted_total_count = summary.total_inserted_count,
        normalized_event_kind_count = summary.by_kind.len(),
        "block-derived normalized events synced from persisted raw payloads"
    );

    for (event_kind, kind_summary) in &summary.by_kind {
        info!(
            service = "indexer",
            chain,
            event_kind,
            normalized_event_sync_count = kind_summary.synced_count,
            normalized_event_inserted_count = kind_summary.inserted_count,
            "block-derived normalized-event kind synced"
        );
    }
}

fn manifest_normalized_event_kind_count(
    summary: &ManifestNormalizedEventSyncSummary,
    event_kind: &str,
) -> usize {
    summary
        .by_kind
        .get(event_kind)
        .map(|kind_summary| kind_summary.synced_count)
        .unwrap_or(0)
}

fn log_watched_contract_summary(summary: &WatchedContractSummary) {
    info!(
        service = "indexer",
        watched_entry_count_total = summary.source_entry_count,
        watched_manifest_root_entry_count = summary.manifest_root_count,
        watched_manifest_contract_entry_count = summary.manifest_contract_count,
        watched_discovery_edge_entry_count = summary.discovery_edge_count,
        watched_chain_count = summary.chains.len(),
        "canonical watched contract set rebuilt from stored manifest state"
    );

    for chain in &summary.chains {
        info!(
            service = "indexer",
            chain = %chain.chain,
            watched_entry_count_total = chain.manifest_root_count
                + chain.manifest_contract_count
                + chain.discovery_edge_count,
            watched_manifest_root_entry_count = chain.manifest_root_count,
            watched_manifest_contract_entry_count = chain.manifest_contract_count,
            watched_discovery_edge_entry_count = chain.discovery_edge_count,
            "watched contract entries rebuilt for chain"
        );
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WatchedChainPlanState {
    chain_count: usize,
    address_count: usize,
    entry_count: usize,
}

fn watched_chain_plan_state(plan: &[WatchedChainPlan]) -> WatchedChainPlanState {
    WatchedChainPlanState {
        chain_count: plan.len(),
        address_count: plan.iter().map(|chain| chain.addresses.len()).sum(),
        entry_count: plan
            .iter()
            .map(|chain| {
                chain.manifest_root_entry_count
                    + chain.manifest_contract_entry_count
                    + chain.discovery_edge_entry_count
            })
            .sum(),
    }
}

fn log_watched_chain_plan(stage: &'static str, plan: &[WatchedChainPlan]) {
    let state = watched_chain_plan_state(plan);

    if state.entry_count == 0 {
        warn!(
            service = "indexer",
            stage,
            watched_chain_count = state.chain_count,
            watched_address_count = state.address_count,
            watched_entry_count_total = state.entry_count,
            "no watched contract entries are active; indexer poll loop will stay idle until manifest state changes"
        );
        return;
    }

    info!(
        service = "indexer",
        stage,
        watched_chain_count = state.chain_count,
        watched_address_count = state.address_count,
        watched_entry_count_total = state.entry_count,
        "runtime watched chain plan rebuilt from stored manifest state"
    );

    for chain in plan {
        info!(
            service = "indexer",
            stage,
            chain = %chain.chain,
            watched_address_count = chain.addresses.len(),
            watched_entry_count_total = chain.manifest_root_entry_count
                + chain.manifest_contract_entry_count
                + chain.discovery_edge_entry_count,
            watched_manifest_root_entry_count = chain.manifest_root_entry_count,
            watched_manifest_contract_entry_count = chain.manifest_contract_entry_count,
            watched_discovery_edge_entry_count = chain.discovery_edge_entry_count,
            "runtime watched chain plan rebuilt for chain"
        );
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct IntakeChainTask {
    chain: String,
    addresses: Vec<String>,
    manifest_root_entry_count: usize,
    manifest_contract_entry_count: usize,
    discovery_edge_entry_count: usize,
    checkpoint: ChainCheckpoint,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct IntakeRuntimeState {
    chain_count: usize,
    address_count: usize,
    entry_count: usize,
    cold_start_chain_count: usize,
    resumable_chain_count: usize,
    safe_checkpoint_chain_count: usize,
    finalized_checkpoint_chain_count: usize,
}

fn checkpoint_mode(checkpoint: &ChainCheckpoint) -> &'static str {
    if checkpoint.canonical_block_hash.is_some() && checkpoint.canonical_block_number.is_some() {
        "resume"
    } else {
        "cold_start"
    }
}

fn intake_runtime_state(tasks: &[IntakeChainTask]) -> IntakeRuntimeState {
    IntakeRuntimeState {
        chain_count: tasks.len(),
        address_count: tasks.iter().map(|task| task.addresses.len()).sum(),
        entry_count: tasks
            .iter()
            .map(|task| {
                task.manifest_root_entry_count
                    + task.manifest_contract_entry_count
                    + task.discovery_edge_entry_count
            })
            .sum(),
        cold_start_chain_count: tasks
            .iter()
            .filter(|task| checkpoint_mode(&task.checkpoint) == "cold_start")
            .count(),
        resumable_chain_count: tasks
            .iter()
            .filter(|task| checkpoint_mode(&task.checkpoint) == "resume")
            .count(),
        safe_checkpoint_chain_count: tasks
            .iter()
            .filter(|task| {
                task.checkpoint.safe_block_hash.is_some()
                    && task.checkpoint.safe_block_number.is_some()
            })
            .count(),
        finalized_checkpoint_chain_count: tasks
            .iter()
            .filter(|task| {
                task.checkpoint.finalized_block_hash.is_some()
                    && task.checkpoint.finalized_block_number.is_some()
            })
            .count(),
    }
}

async fn sync_intake_chain_tasks(
    pool: &sqlx::PgPool,
    watched_chain_plan: &[WatchedChainPlan],
) -> Result<Vec<IntakeChainTask>> {
    let chain_ids = watched_chain_plan
        .iter()
        .map(|chain| chain.chain.clone())
        .collect::<Vec<_>>();
    let checkpoints = sync_chain_checkpoints(pool, &chain_ids).await?;
    let checkpoints = checkpoints
        .into_iter()
        .map(|checkpoint| (checkpoint.chain_id.clone(), checkpoint))
        .collect::<std::collections::BTreeMap<_, _>>();

    let mut tasks = Vec::with_capacity(watched_chain_plan.len());
    for chain in watched_chain_plan {
        let checkpoint = checkpoints.get(&chain.chain).cloned().with_context(|| {
            format!(
                "checkpoint sync did not return a persisted chain row for {}",
                chain.chain
            )
        })?;
        tasks.push(IntakeChainTask {
            chain: chain.chain.clone(),
            addresses: chain.addresses.clone(),
            manifest_root_entry_count: chain.manifest_root_entry_count,
            manifest_contract_entry_count: chain.manifest_contract_entry_count,
            discovery_edge_entry_count: chain.discovery_edge_entry_count,
            checkpoint,
        });
    }

    Ok(tasks)
}

fn log_intake_chain_tasks(stage: &'static str, tasks: &[IntakeChainTask]) {
    let state = intake_runtime_state(tasks);

    if state.entry_count == 0 {
        warn!(
            service = "indexer",
            stage,
            intake_chain_count = state.chain_count,
            intake_address_count = state.address_count,
            intake_entry_count_total = state.entry_count,
            intake_cold_start_chain_count = state.cold_start_chain_count,
            intake_resumable_chain_count = state.resumable_chain_count,
            "no active intake chain tasks are available; persisted checkpoints will stay idle until manifest state changes"
        );
        return;
    }

    info!(
        service = "indexer",
        stage,
        intake_chain_count = state.chain_count,
        intake_address_count = state.address_count,
        intake_entry_count_total = state.entry_count,
        intake_cold_start_chain_count = state.cold_start_chain_count,
        intake_resumable_chain_count = state.resumable_chain_count,
        intake_safe_checkpoint_chain_count = state.safe_checkpoint_chain_count,
        intake_finalized_checkpoint_chain_count = state.finalized_checkpoint_chain_count,
        "runtime intake chain tasks rebuilt from stored watch state and persisted checkpoints"
    );

    for task in tasks {
        info!(
            service = "indexer",
            stage,
            chain = %task.chain,
            intake_checkpoint_mode = checkpoint_mode(&task.checkpoint),
            intake_address_count = task.addresses.len(),
            intake_entry_count_total = task.manifest_root_entry_count
                + task.manifest_contract_entry_count
                + task.discovery_edge_entry_count,
            intake_manifest_root_entry_count = task.manifest_root_entry_count,
            intake_manifest_contract_entry_count = task.manifest_contract_entry_count,
            intake_discovery_edge_entry_count = task.discovery_edge_entry_count,
            canonical_block_number = task.checkpoint.canonical_block_number,
            canonical_block_hash = task.checkpoint.canonical_block_hash.as_deref(),
            safe_block_number = task.checkpoint.safe_block_number,
            safe_block_hash = task.checkpoint.safe_block_hash.as_deref(),
            finalized_block_number = task.checkpoint.finalized_block_number,
            finalized_block_hash = task.checkpoint.finalized_block_hash.as_deref(),
            "runtime intake chain task rebuilt for chain"
        );
    }
}

fn log_provider_registry(
    stage: &'static str,
    tasks: &[IntakeChainTask],
    provider_registry: &ProviderRegistry,
) {
    info!(
        service = "indexer",
        stage,
        rpc_configured_chain_count = provider_registry.configured_chain_count(),
        intake_chain_count = tasks.len(),
        "provider registry loaded for intake chains"
    );

    for task in tasks {
        if provider_registry.provider_for(&task.chain).is_none() {
            warn!(
                service = "indexer",
                stage,
                chain = %task.chain,
                intake_address_count = task.addresses.len(),
                "no RPC provider is configured for an active intake chain; provider-backed head fetch will stay idle for this chain"
            );
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CanonicalReconciliationStatus {
    Initialized,
    Unchanged,
    Appended,
    GapBackfilled,
    ReorgReconciled,
    AwaitingAncestor,
}

impl CanonicalReconciliationStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Initialized => "initialized",
            Self::Unchanged => "unchanged",
            Self::Appended => "appended",
            Self::GapBackfilled => "gap_backfilled",
            Self::ReorgReconciled => "reorg_reconciled",
            Self::AwaitingAncestor => "awaiting_ancestor",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CanonicalReconciliation {
    status: CanonicalReconciliationStatus,
    canonical: Option<CheckpointBlockRef>,
    fetched_parent_count: usize,
    orphaned_block_count: usize,
    reconciled_blocks: Vec<ProviderBlock>,
    raw_orphan_stop_before_hash: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HeadChangeSet {
    canonical_head_changed: bool,
    safe_head_changed: bool,
    finalized_head_changed: bool,
}

impl HeadChangeSet {
    fn requires_raw_payload_refresh(self, canonical_status: CanonicalReconciliationStatus) -> bool {
        canonical_status != CanonicalReconciliationStatus::Unchanged
            || self.safe_head_changed
            || self.finalized_head_changed
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ChainReconciliationOutcome {
    chain: String,
    canonical_status: CanonicalReconciliationStatus,
    canonical_head_changed: bool,
    safe_head_changed: bool,
    finalized_head_changed: bool,
    fetched_parent_count: usize,
    orphaned_block_count: usize,
    canonical_block_number: Option<i64>,
    safe_block_number: Option<i64>,
    finalized_block_number: Option<i64>,
}

fn log_chain_reconciliation_outcome(outcome: &ChainReconciliationOutcome) {
    info!(
        service = "indexer",
        chain = %outcome.chain,
        canonical_reconciliation_status = outcome.canonical_status.as_str(),
        canonical_head_changed = outcome.canonical_head_changed,
        safe_head_changed = outcome.safe_head_changed,
        finalized_head_changed = outcome.finalized_head_changed,
        fetched_parent_count = outcome.fetched_parent_count,
        orphaned_block_count = outcome.orphaned_block_count,
        canonical_block_number = outcome.canonical_block_number,
        safe_block_number = outcome.safe_block_number,
        finalized_block_number = outcome.finalized_block_number,
        "provider heads reconciled for chain"
    );
}

async fn poll_provider_heads(
    pool: &sqlx::PgPool,
    tasks: &mut Vec<IntakeChainTask>,
    provider_registry: &ProviderRegistry,
) -> Result<()> {
    let mut next_tasks = tasks.clone();
    let mut any_change = false;

    for (index, task) in tasks.iter().enumerate() {
        let Some(provider) = provider_registry.provider_for(&task.chain) else {
            continue;
        };

        match reconcile_intake_chain_task(pool, task, provider).await {
            Ok(Some((next_task, outcome))) => {
                log_chain_reconciliation_outcome(&outcome);
                next_tasks[index] = next_task;
                any_change = true;
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    service = "indexer",
                    chain = %task.chain,
                    error = ?error,
                    intake_checkpoint_mode = checkpoint_mode(&task.checkpoint),
                    "failed to fetch and reconcile provider heads for intake chain"
                );
            }
        }
    }

    if any_change {
        *tasks = next_tasks;
    }

    Ok(())
}

async fn reconcile_intake_chain_task(
    pool: &sqlx::PgPool,
    task: &IntakeChainTask,
    provider: &provider::JsonRpcProvider,
) -> Result<Option<(IntakeChainTask, ChainReconciliationOutcome)>> {
    let heads = provider.fetch_chain_heads().await?;
    reconcile_fetched_heads(pool, task, provider, &heads).await
}

async fn reconcile_fetched_heads(
    pool: &sqlx::PgPool,
    task: &IntakeChainTask,
    provider: &provider::JsonRpcProvider,
    heads: &ProviderHeadSnapshot,
) -> Result<Option<(IntakeChainTask, ChainReconciliationOutcome)>> {
    let canonical = reconcile_canonical_head(
        pool,
        provider,
        &task.chain,
        &task.checkpoint,
        &heads.canonical,
    )
    .await?;
    let head_change_set = head_change_set(task, heads, &canonical);

    if canonical.status == CanonicalReconciliationStatus::ReorgReconciled
        && let Some(current_canonical_hash) = task.checkpoint.canonical_block_hash.as_deref()
        && load_raw_block(pool, &task.chain, current_canonical_hash)
            .await?
            .is_some()
    {
        mark_raw_block_facts_range_orphaned(
            pool,
            &task.chain,
            current_canonical_hash,
            canonical.raw_orphan_stop_before_hash.as_deref(),
        )
        .await?;
        let orphaned_normalized_event_count = mark_block_derived_normalized_events_range_orphaned(
            pool,
            &task.chain,
            current_canonical_hash,
            canonical.raw_orphan_stop_before_hash.as_deref(),
        )
        .await?;
        if orphaned_normalized_event_count > 0 {
            info!(
                service = "indexer",
                chain = %task.chain,
                orphaned_normalized_event_count,
                "block-derived normalized events orphaned for the losing branch"
            );
        }
    }

    persist_reconciled_raw_blocks(pool, &task.chain, heads, &canonical).await?;
    if head_change_set.requires_raw_payload_refresh(canonical.status) {
        persist_reconciled_raw_payloads(
            pool,
            &task.chain,
            provider,
            heads,
            &canonical,
            head_change_set,
        )
        .await?;
    }
    persist_reconciled_raw_code_hashes(pool, task, provider, heads, &canonical, head_change_set)
        .await?;

    if let Some(safe_head) = &heads.safe {
        upsert_chain_lineage_blocks(
            pool,
            &[provider_block_to_lineage(
                &task.chain,
                safe_head,
                CanonicalityState::Safe,
            )],
        )
        .await?;
    }
    if let Some(finalized_head) = &heads.finalized {
        upsert_chain_lineage_blocks(
            pool,
            &[provider_block_to_lineage(
                &task.chain,
                finalized_head,
                CanonicalityState::Finalized,
            )],
        )
        .await?;
    }

    let next_checkpoint = advance_chain_checkpoints(
        pool,
        &ChainCheckpointUpdate {
            chain_id: task.chain.clone(),
            canonical: canonical.canonical.clone(),
            safe: heads.safe.as_ref().map(provider_block_to_checkpoint_ref),
            finalized: heads
                .finalized
                .as_ref()
                .map(provider_block_to_checkpoint_ref),
        },
    )
    .await?;

    if !head_change_set.canonical_head_changed
        && !head_change_set.safe_head_changed
        && !head_change_set.finalized_head_changed
        && canonical.status == CanonicalReconciliationStatus::Unchanged
    {
        return Ok(None);
    }

    let mut next_task = task.clone();
    next_task.checkpoint = next_checkpoint.clone();

    Ok(Some((
        next_task,
        ChainReconciliationOutcome {
            chain: task.chain.clone(),
            canonical_status: canonical.status,
            canonical_head_changed: head_change_set.canonical_head_changed,
            safe_head_changed: head_change_set.safe_head_changed,
            finalized_head_changed: head_change_set.finalized_head_changed,
            fetched_parent_count: canonical.fetched_parent_count,
            orphaned_block_count: canonical.orphaned_block_count,
            canonical_block_number: next_checkpoint.canonical_block_number,
            safe_block_number: next_checkpoint.safe_block_number,
            finalized_block_number: next_checkpoint.finalized_block_number,
        },
    )))
}

async fn reconcile_canonical_head(
    pool: &sqlx::PgPool,
    provider: &provider::JsonRpcProvider,
    chain: &str,
    checkpoint: &ChainCheckpoint,
    latest_head: &ProviderBlock,
) -> Result<CanonicalReconciliation> {
    let latest_hash = latest_head.block_hash.as_str();
    let current_canonical_hash = checkpoint.canonical_block_hash.as_deref();

    if current_canonical_hash.is_none() {
        upsert_chain_lineage_blocks(
            pool,
            &[provider_block_to_lineage(
                chain,
                latest_head,
                CanonicalityState::Canonical,
            )],
        )
        .await?;
        return Ok(CanonicalReconciliation {
            status: CanonicalReconciliationStatus::Initialized,
            canonical: Some(provider_block_to_checkpoint_ref(latest_head)),
            fetched_parent_count: 0,
            orphaned_block_count: 0,
            reconciled_blocks: vec![latest_head.clone()],
            raw_orphan_stop_before_hash: None,
        });
    }

    if current_canonical_hash == Some(latest_hash) {
        upsert_chain_lineage_blocks(
            pool,
            &[provider_block_to_lineage(
                chain,
                latest_head,
                CanonicalityState::Canonical,
            )],
        )
        .await?;
        return Ok(CanonicalReconciliation {
            status: CanonicalReconciliationStatus::Unchanged,
            canonical: Some(provider_block_to_checkpoint_ref(latest_head)),
            fetched_parent_count: 0,
            orphaned_block_count: 0,
            reconciled_blocks: vec![latest_head.clone()],
            raw_orphan_stop_before_hash: None,
        });
    }

    let mut path = vec![latest_head.clone()];
    let mut cursor = latest_head.clone();
    let mut fetched_parent_count = 0usize;
    let mut common_ancestor_hash = None::<String>;

    for _ in 0..MAX_PARENT_FETCH_DEPTH {
        if cursor.parent_hash.as_deref() == current_canonical_hash {
            common_ancestor_hash = current_canonical_hash.map(ToOwned::to_owned);
            break;
        }

        let Some(parent_hash) = cursor.parent_hash.clone() else {
            break;
        };

        if let Some(stored_parent) = load_chain_lineage_block(pool, chain, &parent_hash).await? {
            if stored_parent.canonicality_state != CanonicalityState::Orphaned {
                common_ancestor_hash = Some(stored_parent.block_hash.clone());
                break;
            }

            cursor = lineage_block_to_provider(&stored_parent);
            path.push(cursor.clone());
            continue;
        }

        let fetched_parent = provider.fetch_block_by_hash(&parent_hash).await?;
        fetched_parent_count += 1;
        if Some(fetched_parent.block_hash.as_str()) == current_canonical_hash {
            common_ancestor_hash = Some(fetched_parent.block_hash.clone());
            break;
        }

        cursor = fetched_parent.clone();
        path.push(fetched_parent);
    }

    if common_ancestor_hash.is_none() {
        for block in &path {
            upsert_chain_lineage_blocks(
                pool,
                &[provider_block_to_lineage(
                    chain,
                    block,
                    CanonicalityState::Observed,
                )],
            )
            .await?;
        }

        return Ok(CanonicalReconciliation {
            status: CanonicalReconciliationStatus::AwaitingAncestor,
            canonical: None,
            fetched_parent_count,
            orphaned_block_count: 0,
            reconciled_blocks: path,
            raw_orphan_stop_before_hash: None,
        });
    }

    let common_ancestor_hash = common_ancestor_hash.expect("checked above");
    let mut orphaned_block_count = 0usize;
    let status = if Some(common_ancestor_hash.as_str()) == current_canonical_hash {
        if path.len() == 1 {
            CanonicalReconciliationStatus::Appended
        } else {
            CanonicalReconciliationStatus::GapBackfilled
        }
    } else {
        orphaned_block_count = orphan_canonical_branch(
            pool,
            chain,
            current_canonical_hash.expect("current checkpoint must exist"),
            Some(common_ancestor_hash.as_str()),
        )
        .await?;
        CanonicalReconciliationStatus::ReorgReconciled
    };

    for block in path.iter().rev() {
        upsert_chain_lineage_blocks(
            pool,
            &[provider_block_to_lineage(
                chain,
                block,
                CanonicalityState::Canonical,
            )],
        )
        .await?;
    }

    Ok(CanonicalReconciliation {
        status,
        canonical: Some(provider_block_to_checkpoint_ref(latest_head)),
        fetched_parent_count,
        orphaned_block_count,
        reconciled_blocks: path,
        raw_orphan_stop_before_hash: (status == CanonicalReconciliationStatus::ReorgReconciled)
            .then_some(common_ancestor_hash),
    })
}

async fn orphan_canonical_branch(
    pool: &sqlx::PgPool,
    chain: &str,
    from_hash: &str,
    stop_before_hash: Option<&str>,
) -> Result<usize> {
    let mut orphaned_block_count = 0usize;
    let mut cursor_hash = Some(from_hash.to_owned());

    while let Some(block_hash) = cursor_hash {
        if Some(block_hash.as_str()) == stop_before_hash {
            break;
        }

        let snapshots =
            mark_chain_lineage_range_orphaned(pool, chain, &block_hash, stop_before_hash).await?;
        orphaned_block_count += snapshots.len();
        cursor_hash = None;
    }

    Ok(orphaned_block_count)
}

fn provider_block_to_lineage(
    chain: &str,
    block: &ProviderBlock,
    canonicality_state: CanonicalityState,
) -> ChainLineageBlock {
    ChainLineageBlock {
        chain_id: chain.to_owned(),
        block_hash: block.block_hash.clone(),
        parent_hash: block.parent_hash.clone(),
        block_number: block.block_number,
        block_timestamp: sqlx::types::time::OffsetDateTime::from_unix_timestamp(
            block.block_timestamp_unix_secs,
        )
        .expect("provider block timestamp must fit in OffsetDateTime"),
        logs_bloom: block.logs_bloom.clone(),
        transactions_root: block.transactions_root.clone(),
        receipts_root: block.receipts_root.clone(),
        state_root: block.state_root.clone(),
        canonicality_state,
    }
}

fn lineage_block_to_provider(block: &ChainLineageBlock) -> ProviderBlock {
    ProviderBlock {
        block_hash: block.block_hash.clone(),
        parent_hash: block.parent_hash.clone(),
        block_number: block.block_number,
        block_timestamp_unix_secs: block.block_timestamp.unix_timestamp(),
        logs_bloom: block.logs_bloom.clone(),
        transactions_root: block.transactions_root.clone(),
        receipts_root: block.receipts_root.clone(),
        state_root: block.state_root.clone(),
    }
}

fn provider_block_to_checkpoint_ref(block: &ProviderBlock) -> CheckpointBlockRef {
    CheckpointBlockRef {
        block_hash: block.block_hash.clone(),
        block_number: block.block_number,
    }
}

fn head_change_set(
    task: &IntakeChainTask,
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
) -> HeadChangeSet {
    let next_safe = heads.safe.as_ref().map(provider_block_to_checkpoint_ref);
    let next_finalized = heads
        .finalized
        .as_ref()
        .map(provider_block_to_checkpoint_ref);

    HeadChangeSet {
        canonical_head_changed: checkpoint_ref_changed(
            task.checkpoint.canonical_block_hash.as_deref(),
            task.checkpoint.canonical_block_number,
            canonical.canonical.as_ref(),
        ),
        safe_head_changed: checkpoint_ref_changed(
            task.checkpoint.safe_block_hash.as_deref(),
            task.checkpoint.safe_block_number,
            next_safe.as_ref(),
        ),
        finalized_head_changed: checkpoint_ref_changed(
            task.checkpoint.finalized_block_hash.as_deref(),
            task.checkpoint.finalized_block_number,
            next_finalized.as_ref(),
        ),
    }
}

fn checkpoint_ref_changed(
    current_hash: Option<&str>,
    current_number: Option<i64>,
    next: Option<&CheckpointBlockRef>,
) -> bool {
    let Some(next) = next else {
        return false;
    };

    current_hash != Some(next.block_hash.as_str()) || current_number != Some(next.block_number)
}

async fn persist_reconciled_raw_blocks(
    pool: &sqlx::PgPool,
    chain: &str,
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
) -> Result<()> {
    let mut blocks = std::collections::BTreeMap::<String, bigname_storage::RawBlock>::new();

    let canonical_state = canonical_raw_state(canonical.status);
    for block in &canonical.reconciled_blocks {
        insert_raw_block_candidate(&mut blocks, chain, block, canonical_state);
    }
    if let Some(safe) = &heads.safe {
        insert_raw_block_candidate(&mut blocks, chain, safe, CanonicalityState::Safe);
    }
    if let Some(finalized) = &heads.finalized {
        insert_raw_block_candidate(&mut blocks, chain, finalized, CanonicalityState::Finalized);
    }

    upsert_raw_blocks(pool, &blocks.into_values().collect::<Vec<_>>()).await?;
    Ok(())
}

async fn persist_reconciled_raw_payloads(
    pool: &sqlx::PgPool,
    chain: &str,
    provider: &provider::JsonRpcProvider,
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
    head_change_set: HeadChangeSet,
) -> Result<()> {
    let block_hashes = raw_payload_candidate_hashes(heads, canonical, head_change_set);
    if block_hashes.is_empty() {
        return Ok(());
    }

    let raw_blocks = load_raw_blocks_by_hashes(pool, chain, &block_hashes).await?;
    if raw_blocks.len() != block_hashes.len() {
        bail!(
            "stored raw block count {} does not match the raw payload fetch plan size {} for chain {}",
            raw_blocks.len(),
            block_hashes.len(),
            chain
        );
    }

    let mut transactions = Vec::<RawTransaction>::new();
    let mut receipts = Vec::<RawReceipt>::new();
    let mut logs = Vec::<RawLog>::new();

    for raw_block in &raw_blocks {
        let bundle = provider
            .fetch_block_bundle_by_hash(&raw_block.block_hash)
            .await?;
        ensure_provider_bundle_matches_raw_block(raw_block, &bundle)?;

        transactions.extend(
            bundle
                .transactions
                .iter()
                .map(|transaction| {
                    provider_transaction_to_raw_transaction(chain, raw_block, transaction)
                })
                .collect::<Result<Vec<_>>>()?,
        );
        receipts.extend(
            bundle
                .receipts
                .iter()
                .map(|receipt| provider_receipt_to_raw_receipt(chain, raw_block, receipt))
                .collect::<Result<Vec<_>>>()?,
        );
        logs.extend(
            bundle
                .logs
                .iter()
                .map(|log| provider_log_to_raw_log(chain, raw_block, log))
                .collect::<Result<Vec<_>>>()?,
        );
    }

    upsert_raw_transactions(pool, &transactions).await?;
    upsert_raw_receipts(pool, &receipts).await?;
    upsert_raw_logs(pool, &logs).await?;
    let normalized_event_summary =
        bigname_adapters::sync_block_derived_normalized_events(pool, chain, &block_hashes).await?;
    log_block_derived_normalized_event_summary(chain, &normalized_event_summary);

    Ok(())
}

async fn persist_reconciled_raw_code_hashes(
    pool: &sqlx::PgPool,
    task: &IntakeChainTask,
    provider: &provider::JsonRpcProvider,
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
    head_change_set: HeadChangeSet,
) -> Result<()> {
    if task.addresses.is_empty() {
        return Ok(());
    }

    let refreshed_block_hashes = raw_payload_candidate_hashes(heads, canonical, head_change_set)
        .into_iter()
        .collect::<BTreeSet<_>>();
    let candidate_hashes = raw_code_hash_candidate_hashes(heads, canonical, head_change_set);
    if candidate_hashes.is_empty() {
        return Ok(());
    }

    let raw_blocks = load_raw_blocks_by_hashes(pool, &task.chain, &candidate_hashes).await?;
    if raw_blocks.len() != candidate_hashes.len() {
        bail!(
            "stored raw block count {} does not match the raw code-hash fetch plan size {} for chain {}",
            raw_blocks.len(),
            candidate_hashes.len(),
            task.chain
        );
    }

    let stored_counts =
        load_raw_code_hash_counts_by_block_hashes(pool, &task.chain, &candidate_hashes).await?;
    let raw_blocks = raw_blocks
        .into_iter()
        .filter(|raw_block| {
            refreshed_block_hashes.contains(&raw_block.block_hash)
                || stored_counts
                    .get(&raw_block.block_hash)
                    .copied()
                    .unwrap_or(0)
                    < task.addresses.len()
        })
        .collect::<Vec<_>>();
    if raw_blocks.is_empty() {
        return Ok(());
    }

    let mut code_hashes = Vec::<RawCodeHash>::new();
    for raw_block in &raw_blocks {
        let observations = provider
            .fetch_code_observations_at_block(
                &task.addresses,
                ProviderBlockSelection::Number(raw_block.block_number),
            )
            .await?;
        code_hashes.extend(
            observations
                .iter()
                .map(|observation| {
                    provider_code_observation_to_raw_code_hash(&task.chain, raw_block, observation)
                })
                .collect::<Result<Vec<_>>>()?,
        );
    }

    upsert_raw_code_hashes(pool, &code_hashes).await?;
    Ok(())
}

fn raw_payload_candidate_hashes(
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
    head_change_set: HeadChangeSet,
) -> Vec<String> {
    let mut hashes = BTreeSet::new();

    for block in &canonical.reconciled_blocks {
        hashes.insert(block.block_hash.clone());
    }

    if head_change_set.safe_head_changed
        || canonical.status == CanonicalReconciliationStatus::Initialized
    {
        if let Some(safe) = &heads.safe {
            hashes.insert(safe.block_hash.clone());
        }
    }

    if head_change_set.finalized_head_changed
        || canonical.status == CanonicalReconciliationStatus::Initialized
    {
        if let Some(finalized) = &heads.finalized {
            hashes.insert(finalized.block_hash.clone());
        }
    }

    hashes.into_iter().collect()
}

fn raw_code_hash_candidate_hashes(
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
    head_change_set: HeadChangeSet,
) -> Vec<String> {
    let mut hashes = raw_payload_candidate_hashes(heads, canonical, head_change_set)
        .into_iter()
        .collect::<BTreeSet<_>>();

    if let Some(canonical) = canonical.canonical.as_ref() {
        hashes.insert(canonical.block_hash.clone());
    }
    if let Some(safe) = &heads.safe {
        hashes.insert(safe.block_hash.clone());
    }
    if let Some(finalized) = &heads.finalized {
        hashes.insert(finalized.block_hash.clone());
    }

    hashes.into_iter().collect()
}

fn ensure_provider_bundle_matches_raw_block(
    raw_block: &RawBlock,
    bundle: &ProviderBlockBundle,
) -> Result<()> {
    let candidate = provider_block_to_raw_block(
        raw_block.chain_id.as_str(),
        &bundle.block,
        raw_block.canonicality_state,
    );

    if candidate.block_hash != raw_block.block_hash
        || candidate.parent_hash != raw_block.parent_hash
        || candidate.block_number != raw_block.block_number
        || candidate.block_timestamp != raw_block.block_timestamp
        || candidate.logs_bloom != raw_block.logs_bloom
        || candidate.transactions_root != raw_block.transactions_root
        || candidate.receipts_root != raw_block.receipts_root
        || candidate.state_root != raw_block.state_root
    {
        bail!(
            "provider bundle block {} does not match stored raw block facts for chain {}",
            raw_block.block_hash,
            raw_block.chain_id
        );
    }

    Ok(())
}

fn canonical_raw_state(status: CanonicalReconciliationStatus) -> CanonicalityState {
    match status {
        CanonicalReconciliationStatus::AwaitingAncestor => CanonicalityState::Observed,
        CanonicalReconciliationStatus::Initialized
        | CanonicalReconciliationStatus::Unchanged
        | CanonicalReconciliationStatus::Appended
        | CanonicalReconciliationStatus::GapBackfilled
        | CanonicalReconciliationStatus::ReorgReconciled => CanonicalityState::Canonical,
    }
}

fn insert_raw_block_candidate(
    blocks: &mut std::collections::BTreeMap<String, bigname_storage::RawBlock>,
    chain: &str,
    block: &ProviderBlock,
    canonicality_state: CanonicalityState,
) {
    let candidate = provider_block_to_raw_block(chain, block, canonicality_state);
    blocks
        .entry(candidate.block_hash.clone())
        .and_modify(|existing| {
            existing.canonicality_state =
                preferred_canonicality(existing.canonicality_state, candidate.canonicality_state);
        })
        .or_insert(candidate);
}

fn preferred_canonicality(
    current: CanonicalityState,
    incoming: CanonicalityState,
) -> CanonicalityState {
    if canonicality_rank(incoming) > canonicality_rank(current) {
        incoming
    } else {
        current
    }
}

fn canonicality_rank(state: CanonicalityState) -> u8 {
    match state {
        CanonicalityState::Observed => 0,
        CanonicalityState::Canonical => 1,
        CanonicalityState::Safe => 2,
        CanonicalityState::Finalized => 3,
        CanonicalityState::Orphaned => 4,
    }
}

fn provider_transaction_to_raw_transaction(
    chain: &str,
    raw_block: &RawBlock,
    transaction: &ProviderTransaction,
) -> Result<RawTransaction> {
    ensure_block_scoped_identity(
        "transaction",
        chain,
        &raw_block.block_hash,
        raw_block.block_number,
        &transaction.block_hash,
        transaction.block_number,
    )?;

    Ok(RawTransaction {
        chain_id: chain.to_owned(),
        block_hash: transaction.block_hash.clone(),
        block_number: transaction.block_number,
        transaction_hash: transaction.transaction_hash.clone(),
        transaction_index: transaction.transaction_index,
        from_address: transaction.from.clone(),
        to_address: transaction.to.clone(),
        canonicality_state: raw_block.canonicality_state,
    })
}

fn provider_receipt_to_raw_receipt(
    chain: &str,
    raw_block: &RawBlock,
    receipt: &ProviderReceipt,
) -> Result<RawReceipt> {
    ensure_block_scoped_identity(
        "receipt",
        chain,
        &raw_block.block_hash,
        raw_block.block_number,
        &receipt.block_hash,
        receipt.block_number,
    )?;

    Ok(RawReceipt {
        chain_id: chain.to_owned(),
        block_hash: receipt.block_hash.clone(),
        block_number: receipt.block_number,
        transaction_hash: receipt.transaction_hash.clone(),
        transaction_index: receipt.transaction_index,
        contract_address: receipt.contract_address.clone(),
        status: parse_receipt_status(receipt.status)?,
        gas_used: receipt.gas_used,
        cumulative_gas_used: receipt.cumulative_gas_used,
        logs_bloom: receipt.logs_bloom.clone(),
        canonicality_state: raw_block.canonicality_state,
    })
}

fn provider_log_to_raw_log(chain: &str, raw_block: &RawBlock, log: &ProviderLog) -> Result<RawLog> {
    ensure_block_scoped_identity(
        "log",
        chain,
        &raw_block.block_hash,
        raw_block.block_number,
        &log.block_hash,
        log.block_number,
    )?;

    Ok(RawLog {
        chain_id: chain.to_owned(),
        block_hash: log.block_hash.clone(),
        block_number: log.block_number,
        transaction_hash: log.transaction_hash.clone(),
        transaction_index: log.transaction_index,
        log_index: log.log_index,
        emitting_address: log.address.clone(),
        topics: log.topics.clone(),
        data: parse_hex_bytes(&log.data)?,
        canonicality_state: raw_block.canonicality_state,
    })
}

fn provider_code_observation_to_raw_code_hash(
    chain: &str,
    raw_block: &RawBlock,
    observation: &ProviderCodeObservation,
) -> Result<RawCodeHash> {
    let code_byte_length = i64::try_from(observation.code.len()).with_context(|| {
        format!(
            "provider code observation byte length {} does not fit in i64 for chain {} block {} contract {}",
            observation.code.len(),
            chain,
            raw_block.block_hash,
            observation.address
        )
    })?;

    Ok(RawCodeHash {
        chain_id: chain.to_owned(),
        block_hash: raw_block.block_hash.clone(),
        block_number: raw_block.block_number,
        contract_address: observation.address.clone(),
        code_hash: keccak256_hex(&observation.code),
        code_byte_length,
        canonicality_state: raw_block.canonicality_state,
    })
}

fn ensure_block_scoped_identity(
    fact_kind: &str,
    chain: &str,
    expected_block_hash: &str,
    expected_block_number: i64,
    actual_block_hash: &str,
    actual_block_number: i64,
) -> Result<()> {
    if actual_block_hash != expected_block_hash || actual_block_number != expected_block_number {
        bail!(
            "provider {} block scope mismatch for chain {} expected {}@{} got {}@{}",
            fact_kind,
            chain,
            expected_block_hash,
            expected_block_number,
            actual_block_hash,
            actual_block_number
        );
    }

    Ok(())
}

fn parse_receipt_status(status: Option<i64>) -> Result<Option<bool>> {
    match status {
        Some(0) => Ok(Some(false)),
        Some(1) => Ok(Some(true)),
        Some(other) => bail!("unsupported receipt status value {other}"),
        None => Ok(None),
    }
}

fn keccak256_hex(bytes: &[u8]) -> String {
    let mut hasher = Keccak256::new();
    hasher.update(bytes);
    hex_string(&hasher.finalize())
}

fn parse_hex_bytes(value: &str) -> Result<Vec<u8>> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if value.len() % 2 != 0 {
        bail!("invalid hex byte string with odd length");
    }

    let mut bytes = Vec::with_capacity(value.len() / 2);
    let chars = value.as_bytes();
    let mut index = 0;
    while index < chars.len() {
        let byte =
            std::str::from_utf8(&chars[index..index + 2]).context("invalid UTF-8 in hex string")?;
        bytes.push(
            u8::from_str_radix(byte, 16)
                .with_context(|| format!("failed to parse hex byte {byte}"))?,
        );
        index += 2;
    }

    Ok(bytes)
}

fn hex_string(bytes: &[u8]) -> String {
    let mut output = String::from("0x");
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }

    output
}

fn provider_block_to_raw_block(
    chain: &str,
    block: &ProviderBlock,
    canonicality_state: CanonicalityState,
) -> bigname_storage::RawBlock {
    bigname_storage::RawBlock {
        chain_id: chain.to_owned(),
        block_hash: block.block_hash.clone(),
        parent_hash: block.parent_hash.clone(),
        block_number: block.block_number,
        block_timestamp: sqlx::types::time::OffsetDateTime::from_unix_timestamp(
            block.block_timestamp_unix_secs,
        )
        .expect("provider block timestamp must fit in OffsetDateTime"),
        logs_bloom: block.logs_bloom.clone(),
        transactions_root: block.transactions_root.clone(),
        receipts_root: block.receipts_root.clone(),
        state_root: block.state_root.clone(),
        canonicality_state,
    }
}

async fn refresh_watched_chain_plan(
    pool: &sqlx::PgPool,
    current_plan: &[WatchedChainPlan],
) -> Result<Option<Vec<WatchedChainPlan>>> {
    let next_plan = load_watched_chain_plan(pool).await?;
    if next_plan == current_plan {
        Ok(None)
    } else {
        Ok(Some(next_plan))
    }
}

async fn refresh_intake_chain_tasks(
    pool: &sqlx::PgPool,
    current_tasks: &[IntakeChainTask],
    watched_chain_plan: &[WatchedChainPlan],
) -> Result<Option<Vec<IntakeChainTask>>> {
    let next_tasks = sync_intake_chain_tasks(pool, watched_chain_plan).await?;
    if next_tasks == current_tasks {
        Ok(None)
    } else {
        Ok(Some(next_tasks))
    }
}

async fn run_poll_loop(
    pool: &sqlx::PgPool,
    manifests_root: PathBuf,
    mut manifest_runtime_state: ManifestRuntimeState,
    mut intake_chain_tasks: Vec<IntakeChainTask>,
    provider_registry: &ProviderRegistry,
    poll_interval_secs: u64,
) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(poll_interval_secs));
    interval.tick().await;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!(service = "indexer", "shutdown signal received");
                return Ok(());
            }
            _ = interval.tick() => {
                match load_manifest_repository(&manifests_root) {
                    Ok(manifest_repository) => {
                        let manifest_summary = manifest_repository.summary().clone();
                        if manifest_summary != manifest_runtime_state.manifest_summary {
                            log_manifest_summary(&manifest_summary);
                        }

                        if let Err(error) = ensure_manifest_root_ready(&manifest_summary) {
                            let current_watch_state =
                                watched_chain_plan_state(&manifest_runtime_state.watched_chain_plan);
                            let current_intake_state = intake_runtime_state(&intake_chain_tasks);
                            warn!(
                                service = "indexer",
                                refresh_reason = "timer",
                                plan_source = "repository_manifest_reload",
                                error = ?error,
                                manifests_root = %manifest_summary.root.display(),
                                manifests_status = manifest_summary.status.as_str(),
                                watched_chain_count = current_watch_state.chain_count,
                                watched_address_count = current_watch_state.address_count,
                                watched_entry_count_total = current_watch_state.entry_count,
                                intake_chain_count = current_intake_state.chain_count,
                                intake_address_count = current_intake_state.address_count,
                                intake_entry_count_total = current_intake_state.entry_count,
                                "failed to reload repository manifests; keeping last successful runtime state"
                            );
                        } else {
                            match build_manifest_runtime_state(pool, &manifest_repository).await {
                                Ok(next_manifest_runtime_state) => {
                                    let manifest_state_changed =
                                        next_manifest_runtime_state != manifest_runtime_state;
                                    let watched_plan_changed = next_manifest_runtime_state
                                        .watched_chain_plan
                                        != manifest_runtime_state.watched_chain_plan;

                                    if manifest_state_changed {
                                        let previous_watch_state = watched_chain_plan_state(
                                            &manifest_runtime_state.watched_chain_plan,
                                        );
                                        let next_watch_state = watched_chain_plan_state(
                                            &next_manifest_runtime_state.watched_chain_plan,
                                        );
                                        info!(
                                            service = "indexer",
                                            refresh_reason = "timer",
                                            plan_source = "repository_manifest_reload",
                                            manifest_state_changed = true,
                                            watched_plan_changed,
                                            previous_manifest_count = manifest_runtime_state.manifest_summary.manifest_count,
                                            manifest_count = next_manifest_runtime_state.manifest_summary.manifest_count,
                                            previous_active_manifest_count = manifest_runtime_state.discovery_admission.active_manifest_count,
                                            stored_active_manifest_count = next_manifest_runtime_state.discovery_admission.active_manifest_count,
                                            previous_watched_chain_count = previous_watch_state.chain_count,
                                            previous_watched_address_count = previous_watch_state.address_count,
                                            previous_watched_entry_count_total = previous_watch_state.entry_count,
                                            watched_chain_count = next_watch_state.chain_count,
                                            watched_address_count = next_watch_state.address_count,
                                            watched_entry_count_total = next_watch_state.entry_count,
                                            "repository manifest refresh changed stored runtime state"
                                        );
                                        log_manifest_runtime_state(&next_manifest_runtime_state);
                                    }

                                    if watched_plan_changed {
                                        match sync_intake_chain_tasks(
                                            pool,
                                            &next_manifest_runtime_state.watched_chain_plan,
                                        )
                                        .await
                                        {
                                            Ok(next_tasks) => {
                                                let previous_watch_state = watched_chain_plan_state(
                                                    &manifest_runtime_state.watched_chain_plan,
                                                );
                                                let next_watch_state = watched_chain_plan_state(
                                                    &next_manifest_runtime_state.watched_chain_plan,
                                                );
                                                let previous_intake_state =
                                                    intake_runtime_state(&intake_chain_tasks);
                                                let next_intake_state =
                                                    intake_runtime_state(&next_tasks);

                                                info!(
                                                    service = "indexer",
                                                    refresh_reason = "timer",
                                                    watched_plan_changed = true,
                                                    plan_source = "repository_manifest_reload",
                                                    previous_watched_chain_count = previous_watch_state.chain_count,
                                                    previous_watched_address_count = previous_watch_state.address_count,
                                                    previous_watched_entry_count_total = previous_watch_state.entry_count,
                                                    watched_chain_count = next_watch_state.chain_count,
                                                    watched_address_count = next_watch_state.address_count,
                                                    watched_entry_count_total = next_watch_state.entry_count,
                                                    previous_intake_chain_count = previous_intake_state.chain_count,
                                                    previous_intake_address_count = previous_intake_state.address_count,
                                                    previous_intake_entry_count_total = previous_intake_state.entry_count,
                                                    intake_chain_count = next_intake_state.chain_count,
                                                    intake_address_count = next_intake_state.address_count,
                                                    intake_entry_count_total = next_intake_state.entry_count,
                                                    intake_cold_start_chain_count = next_intake_state.cold_start_chain_count,
                                                    intake_resumable_chain_count = next_intake_state.resumable_chain_count,
                                                    "runtime watched chain plan changed after repository manifest refresh"
                                                );
                                                log_watched_chain_plan(
                                                    "refresh",
                                                    &next_manifest_runtime_state.watched_chain_plan,
                                                );
                                                log_intake_chain_tasks("refresh", &next_tasks);
                                                log_provider_registry(
                                                    "refresh",
                                                    &next_tasks,
                                                    provider_registry,
                                                );
                                                manifest_runtime_state = next_manifest_runtime_state;
                                                intake_chain_tasks = next_tasks;
                                            }
                                            Err(error) => {
                                                let current_watch_state = watched_chain_plan_state(
                                                    &manifest_runtime_state.watched_chain_plan,
                                                );
                                                let current_intake_state =
                                                    intake_runtime_state(&intake_chain_tasks);
                                                warn!(
                                                    service = "indexer",
                                                    refresh_reason = "timer",
                                                    plan_source = "repository_manifest_reload",
                                                    error = ?error,
                                                    watched_chain_count = current_watch_state.chain_count,
                                                    watched_address_count = current_watch_state.address_count,
                                                    watched_entry_count_total = current_watch_state.entry_count,
                                                    intake_chain_count = current_intake_state.chain_count,
                                                    intake_address_count = current_intake_state.address_count,
                                                    intake_entry_count_total = current_intake_state.entry_count,
                                                    "failed to sync intake chain tasks for a changed watch plan after repository manifest refresh; keeping last successful runtime state"
                                                );
                                            }
                                        }
                                    } else {
                                        manifest_runtime_state = next_manifest_runtime_state;
                                    }
                                }
                                Err(error) => {
                                    let current_watch_state = watched_chain_plan_state(
                                        &manifest_runtime_state.watched_chain_plan,
                                    );
                                    let current_intake_state = intake_runtime_state(&intake_chain_tasks);
                                    warn!(
                                        service = "indexer",
                                        refresh_reason = "timer",
                                        plan_source = "repository_manifest_reload",
                                        error = ?error,
                                        watched_chain_count = current_watch_state.chain_count,
                                        watched_address_count = current_watch_state.address_count,
                                        watched_entry_count_total = current_watch_state.entry_count,
                                        intake_chain_count = current_intake_state.chain_count,
                                        intake_address_count = current_intake_state.address_count,
                                        intake_entry_count_total = current_intake_state.entry_count,
                                        "failed to sync repository manifests into storage during refresh; keeping last successful runtime state"
                                    );
                                }
                            }
                        }
                    }
                    Err(error) => {
                        let current_watch_state =
                            watched_chain_plan_state(&manifest_runtime_state.watched_chain_plan);
                        let current_intake_state = intake_runtime_state(&intake_chain_tasks);
                        warn!(
                            service = "indexer",
                            refresh_reason = "timer",
                            plan_source = "repository_manifest_reload",
                            error = ?error,
                            manifests_root = %manifests_root.display(),
                            watched_chain_count = current_watch_state.chain_count,
                            watched_address_count = current_watch_state.address_count,
                            watched_entry_count_total = current_watch_state.entry_count,
                            intake_chain_count = current_intake_state.chain_count,
                            intake_address_count = current_intake_state.address_count,
                            intake_entry_count_total = current_intake_state.entry_count,
                            "failed to load repository manifests during refresh; keeping last successful runtime state"
                        );
                    }
                }

                match refresh_intake_chain_tasks(
                    pool,
                    &intake_chain_tasks,
                    &manifest_runtime_state.watched_chain_plan,
                )
                .await
                {
                    Ok(Some(next_tasks)) => {
                        let previous_state = intake_runtime_state(&intake_chain_tasks);
                        let next_state = intake_runtime_state(&next_tasks);
                        info!(
                            service = "indexer",
                            refresh_reason = "timer",
                            watched_plan_changed = false,
                            checkpoint_state_changed = true,
                            plan_source = "stored_manifest_state",
                            previous_intake_chain_count = previous_state.chain_count,
                            previous_intake_address_count = previous_state.address_count,
                            previous_intake_entry_count_total = previous_state.entry_count,
                            previous_intake_cold_start_chain_count = previous_state.cold_start_chain_count,
                            previous_intake_resumable_chain_count = previous_state.resumable_chain_count,
                            intake_chain_count = next_state.chain_count,
                            intake_address_count = next_state.address_count,
                            intake_entry_count_total = next_state.entry_count,
                            intake_cold_start_chain_count = next_state.cold_start_chain_count,
                            intake_resumable_chain_count = next_state.resumable_chain_count,
                            intake_safe_checkpoint_chain_count = next_state.safe_checkpoint_chain_count,
                            intake_finalized_checkpoint_chain_count = next_state.finalized_checkpoint_chain_count,
                            "persisted checkpoint state changed for active intake chains"
                        );
                        log_intake_chain_tasks("checkpoint-refresh", &next_tasks);
                        intake_chain_tasks = next_tasks;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        let current_watch_state =
                            watched_chain_plan_state(&manifest_runtime_state.watched_chain_plan);
                        let current_intake_state = intake_runtime_state(&intake_chain_tasks);
                        warn!(
                            service = "indexer",
                            refresh_reason = "timer",
                            plan_source = "stored_manifest_state",
                            error = ?error,
                            watched_chain_count = current_watch_state.chain_count,
                            watched_address_count = current_watch_state.address_count,
                            watched_entry_count_total = current_watch_state.entry_count,
                            intake_chain_count = current_intake_state.chain_count,
                            intake_address_count = current_intake_state.address_count,
                            intake_entry_count_total = current_intake_state.entry_count,
                            "failed to refresh runtime intake chain tasks; keeping last successful state"
                        );
                    }
                }

                poll_provider_heads(pool, &mut intake_chain_tasks, provider_registry).await?;
            }
        }
    }
}

fn init_tracing(service: &'static str) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    if std::env::var_os("BIGNAME_LOG_JSON").is_some() {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .json()
            .with_target(false)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .compact()
            .with_target(false)
            .init();
    }

    info!(
        service = service,
        phase = bigname_domain::bootstrap_phase(),
        "logging configured"
    );
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        str::FromStr,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::Context;
    use bigname_manifests::load_discovery_admission_state;
    use bigname_storage::default_database_url;
    use serde_json::{Value, json};
    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        task::JoinHandle,
    };

    use super::*;

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

    struct TestManifestDir {
        path: PathBuf,
    }

    impl TestManifestDir {
        fn new() -> Result<Self> {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "bigname-indexer-manifests-tests-{}-{unique}-{sequence}",
                std::process::id(),
            ));
            fs::create_dir_all(&path)
                .with_context(|| format!("failed to create test directory {}", path.display()))?;
            Ok(Self { path })
        }

        fn write_manifest(&self, contents: &str) -> Result<PathBuf> {
            let directory = self.path.join("ens").join("ens_v2_registry_l1");
            fs::create_dir_all(&directory)
                .with_context(|| format!("failed to create {}", directory.display()))?;
            let path = directory.join("v1.toml");
            fs::write(&path, contents)
                .with_context(|| format!("failed to write {}", path.display()))?;
            Ok(path)
        }
    }

    impl Drop for TestManifestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    struct TestDatabase {
        admin_pool: PgPool,
        pool: PgPool,
        database_name: String,
    }

    impl TestDatabase {
        async fn new() -> Result<Self> {
            let database_url = std::env::var("BIGNAME_DATABASE_URL")
                .or_else(|_| std::env::var("DATABASE_URL"))
                .unwrap_or_else(|_| default_database_url().to_owned());
            let base_options = PgConnectOptions::from_str(&database_url)
                .context("failed to parse database URL for indexer tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_indexer_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for indexer tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect indexer test pool")?;

            sqlx::query(
                r#"
                CREATE TYPE canonicality_state AS ENUM (
                    'observed',
                    'canonical',
                    'safe',
                    'finalized',
                    'orphaned'
                )
                "#,
            )
            .execute(&pool)
            .await
            .context("failed to create canonicality_state type for indexer tests")?;
            sqlx::query(
                r#"
                CREATE TYPE manifest_rollout_status AS ENUM (
                    'draft',
                    'shadow',
                    'active',
                    'deprecated'
                )
                "#,
            )
            .execute(&pool)
            .await
            .context("failed to create manifest_rollout_status type for indexer tests")?;
            sqlx::query(
                r#"
                CREATE TYPE capability_support_status AS ENUM (
                    'unsupported',
                    'shadow',
                    'supported'
                )
                "#,
            )
            .execute(&pool)
            .await
            .context("failed to create capability_support_status type for indexer tests")?;
            sqlx::query(
                r#"
                CREATE TABLE manifest_versions (
                    manifest_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    manifest_version BIGINT NOT NULL DEFAULT 1,
                    namespace TEXT NOT NULL DEFAULT 'ens',
                    source_family TEXT NOT NULL DEFAULT 'ens_test',
                    chain TEXT NOT NULL,
                    deployment_epoch TEXT NOT NULL DEFAULT 'bootstrap',
                    rollout_status manifest_rollout_status NOT NULL,
                    normalizer_version TEXT NOT NULL DEFAULT 'uts46-v1',
                    file_path TEXT NOT NULL DEFAULT 'tests/v1.toml',
                    manifest_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (namespace, source_family, chain, deployment_epoch, manifest_version)
                )
                "#,
            )
            .execute(&pool)
            .await
            .context("failed to create manifest_versions table for indexer tests")?;
            sqlx::query(
                r#"
                CREATE TABLE manifest_roots (
                    manifest_id BIGINT NOT NULL,
                    name TEXT NOT NULL DEFAULT 'RootRegistry',
                    address TEXT NOT NULL,
                    code_hash TEXT,
                    abi_ref TEXT
                )
                "#,
            )
            .execute(&pool)
            .await
            .context("failed to create manifest_roots table for indexer tests")?;
            sqlx::query(
                r#"
                CREATE TABLE manifest_contracts (
                    manifest_id BIGINT NOT NULL,
                    role TEXT NOT NULL,
                    address TEXT NOT NULL,
                    proxy_kind TEXT NOT NULL DEFAULT 'none',
                    implementation TEXT
                )
                "#,
            )
            .execute(&pool)
            .await
            .context("failed to create manifest_contracts table for indexer tests")?;
            sqlx::query(
                r#"
                CREATE TABLE manifest_capability_flags (
                    manifest_id BIGINT NOT NULL,
                    capability_name TEXT NOT NULL,
                    status capability_support_status NOT NULL,
                    notes TEXT
                )
                "#,
            )
            .execute(&pool)
            .await
            .context("failed to create manifest_capability_flags table for indexer tests")?;
            sqlx::query(
                r#"
                CREATE TABLE manifest_discovery_rules (
                    manifest_id BIGINT NOT NULL,
                    edge_kind TEXT NOT NULL,
                    from_role TEXT NOT NULL,
                    admission TEXT NOT NULL
                )
                "#,
            )
            .execute(&pool)
            .await
            .context("failed to create manifest_discovery_rules table for indexer tests")?;
            sqlx::query(
                r#"
                CREATE TABLE discovery_edges (
                    chain_id TEXT NOT NULL,
                    edge_kind TEXT NOT NULL DEFAULT 'proxy_implementation',
                    from_address TEXT NOT NULL DEFAULT '0x0000000000000000000000000000000000000000',
                    to_address TEXT NOT NULL,
                    discovery_source TEXT NOT NULL DEFAULT 'test',
                    source_manifest_id BIGINT,
                    admission TEXT NOT NULL DEFAULT 'test',
                    active_from_block_number BIGINT,
                    active_from_block_hash TEXT,
                    active_to_block_number BIGINT,
                    active_to_block_hash TEXT,
                    provenance JSONB NOT NULL DEFAULT '{}'::jsonb
                )
                "#,
            )
            .execute(&pool)
            .await
            .context("failed to create discovery_edges table for indexer tests")?;
            sqlx::query(
                r#"
                CREATE TABLE chain_lineage (
                    chain_id TEXT NOT NULL,
                    block_hash TEXT NOT NULL,
                    parent_hash TEXT,
                    block_number BIGINT NOT NULL,
                    block_timestamp TIMESTAMPTZ NOT NULL,
                    logs_bloom BYTEA,
                    transactions_root TEXT,
                    receipts_root TEXT,
                    state_root TEXT,
                    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
                    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (chain_id, block_hash)
                )
                "#,
            )
            .execute(&pool)
            .await
            .context("failed to create chain_lineage table for indexer tests")?;
            sqlx::query(
                r#"
                CREATE TABLE chain_checkpoints (
                    chain_id TEXT PRIMARY KEY,
                    canonical_block_hash TEXT,
                    canonical_block_number BIGINT,
                    safe_block_hash TEXT,
                    safe_block_number BIGINT,
                    finalized_block_hash TEXT,
                    finalized_block_number BIGINT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                "#,
            )
            .execute(&pool)
            .await
            .context("failed to create chain_checkpoints table for indexer tests")?;
            sqlx::query(
                r#"
                CREATE TABLE raw_blocks (
                    raw_block_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    chain_id TEXT NOT NULL,
                    block_hash TEXT NOT NULL,
                    parent_hash TEXT,
                    block_number BIGINT NOT NULL,
                    block_timestamp TIMESTAMPTZ NOT NULL,
                    logs_bloom BYTEA,
                    transactions_root TEXT,
                    receipts_root TEXT,
                    state_root TEXT,
                    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
                    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (chain_id, block_hash)
                )
                "#,
            )
            .execute(&pool)
            .await
            .context("failed to create raw_blocks table for indexer tests")?;
            sqlx::query(
                r#"
                CREATE TABLE raw_transactions (
                    raw_transaction_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    chain_id TEXT NOT NULL,
                    block_hash TEXT NOT NULL,
                    block_number BIGINT NOT NULL,
                    transaction_hash TEXT NOT NULL,
                    transaction_index BIGINT NOT NULL,
                    from_address TEXT NOT NULL,
                    to_address TEXT,
                    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
                    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (chain_id, block_hash, transaction_index)
                )
                "#,
            )
            .execute(&pool)
            .await
            .context("failed to create raw_transactions table for indexer tests")?;
            sqlx::query(
                r#"
                CREATE TABLE raw_code_hashes (
                    raw_code_hash_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    chain_id TEXT NOT NULL,
                    block_hash TEXT NOT NULL,
                    block_number BIGINT NOT NULL,
                    contract_address TEXT NOT NULL,
                    code_hash TEXT NOT NULL,
                    code_byte_length BIGINT NOT NULL,
                    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
                    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (chain_id, block_hash, contract_address)
                )
                "#,
            )
            .execute(&pool)
            .await
            .context("failed to create raw_code_hashes table for indexer tests")?;
            sqlx::query(
                r#"
                CREATE TABLE raw_receipts (
                    raw_receipt_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    chain_id TEXT NOT NULL,
                    block_hash TEXT NOT NULL,
                    block_number BIGINT NOT NULL,
                    transaction_hash TEXT NOT NULL,
                    transaction_index BIGINT NOT NULL,
                    contract_address TEXT,
                    status BOOLEAN,
                    gas_used BIGINT,
                    cumulative_gas_used BIGINT,
                    logs_bloom BYTEA,
                    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
                    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (chain_id, block_hash, transaction_index)
                )
                "#,
            )
            .execute(&pool)
            .await
            .context("failed to create raw_receipts table for indexer tests")?;
            sqlx::query(
                r#"
                CREATE TABLE raw_logs (
                    raw_log_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    chain_id TEXT NOT NULL,
                    block_hash TEXT NOT NULL,
                    block_number BIGINT NOT NULL,
                    transaction_hash TEXT NOT NULL,
                    transaction_index BIGINT NOT NULL,
                    log_index BIGINT NOT NULL,
                    emitting_address TEXT NOT NULL,
                    topics TEXT[] NOT NULL DEFAULT '{}',
                    data BYTEA NOT NULL DEFAULT '\x',
                    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
                    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (chain_id, block_hash, log_index)
                )
                "#,
            )
            .execute(&pool)
            .await
            .context("failed to create raw_logs table for indexer tests")?;
            sqlx::query(
                r#"
                CREATE TABLE normalized_events (
                    normalized_event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    event_identity TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    logical_name_id TEXT,
                    resource_id UUID,
                    event_kind TEXT NOT NULL,
                    source_family TEXT NOT NULL,
                    manifest_version BIGINT NOT NULL,
                    source_manifest_id BIGINT,
                    chain_id TEXT,
                    block_number BIGINT,
                    block_hash TEXT,
                    transaction_hash TEXT,
                    log_index BIGINT,
                    raw_fact_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
                    derivation_kind TEXT NOT NULL,
                    canonicality_state canonicality_state NOT NULL DEFAULT 'observed',
                    before_state JSONB NOT NULL DEFAULT '{}'::jsonb,
                    after_state JSONB NOT NULL DEFAULT '{}'::jsonb,
                    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (event_identity)
                )
                "#,
            )
            .execute(&pool)
            .await
            .context("failed to create normalized_events table for indexer tests")?;

            Ok(Self {
                admin_pool,
                pool,
                database_name,
            })
        }

        fn pool(&self) -> &PgPool {
            &self.pool
        }

        async fn cleanup(self) -> Result<()> {
            self.pool.close().await;
            sqlx::query(&format!(
                r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
                self.database_name
            ))
            .execute(&self.admin_pool)
            .await
            .with_context(|| format!("failed to drop test database {}", self.database_name))?;
            self.admin_pool.close().await;
            Ok(())
        }
    }

    fn manifest_load_summary(status: ManifestLoadStatus) -> ManifestLoadSummary {
        ManifestLoadSummary {
            root: PathBuf::from("/tmp/manifests"),
            status,
            namespace_count: usize::from(matches!(status, ManifestLoadStatus::Loaded)),
            source_family_count: usize::from(matches!(status, ManifestLoadStatus::Loaded)),
            manifest_count: usize::from(matches!(status, ManifestLoadStatus::Loaded)),
        }
    }

    fn synced_manifest_summary(active_manifest_count: usize) -> ManifestSyncSummary {
        ManifestSyncSummary {
            status: ManifestSyncStatus::Synced,
            synced_manifest_count: active_manifest_count,
            active_manifest_count,
            root_count: 0,
            contract_count: 0,
            capability_count: 0,
            discovery_rule_count: 0,
            removed_manifest_count: 0,
            cleared_discovery_edge_count: 0,
        }
    }

    fn manifest_contents(root_address: &str, capability_status: &str) -> String {
        format!(
            r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v2_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v2"
rollout_status = "active"
normalizer_version = "uts46-v1"

[capability_flags]
exact_lookup = "{capability_status}"

[[roots]]
name = "RootRegistry"
address = "{root_address}"

[[contracts]]
role = "registry"
address = "0x00000000000000000000000000000000000000aa"
proxy_kind = "none"

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"
"#
        )
    }

    fn provider_block(
        block_hash: &str,
        parent_hash: Option<&str>,
        block_number: i64,
    ) -> ProviderBlock {
        ProviderBlock {
            block_hash: block_hash.to_owned(),
            parent_hash: parent_hash.map(ToOwned::to_owned),
            block_number,
            block_timestamp_unix_secs: 1_700_000_000 + block_number,
            logs_bloom: None,
            transactions_root: Some(format!("0xtransactions{block_number:02x}")),
            receipts_root: Some(format!("0xreceipts{block_number:02x}")),
            state_root: Some(format!("0xstate{block_number:02x}")),
        }
    }

    async fn bundle_provider(
        blocks: Vec<ProviderBlock>,
    ) -> Result<(provider::JsonRpcProvider, JoinHandle<()>)> {
        let blocks = Arc::new(
            blocks
                .into_iter()
                .map(|block| (block.block_hash.clone(), block))
                .collect::<std::collections::BTreeMap<_, _>>(),
        );

        let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
            let method = body
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let params = body
                .get("params")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();

            let result = match method {
                "eth_getBlockByHash" => {
                    let block_hash = params
                        .first()
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_ascii_lowercase();
                    let block = blocks
                        .get(&block_hash)
                        .unwrap_or_else(|| panic!("unexpected block bundle request: {body}"));
                    rpc_block_bundle_payload(block)
                }
                "eth_getLogs" => {
                    let block_hash = params
                        .first()
                        .and_then(Value::as_object)
                        .and_then(|filter| filter.get("blockHash"))
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_ascii_lowercase();
                    let block = blocks
                        .get(&block_hash)
                        .unwrap_or_else(|| panic!("unexpected log request: {body}"));
                    Value::Array(vec![rpc_log_payload(block)])
                }
                "eth_getBlockReceipts" => {
                    let block_hash = params
                        .first()
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_ascii_lowercase();
                    let block = blocks
                        .get(&block_hash)
                        .unwrap_or_else(|| panic!("unexpected receipt request: {body}"));
                    Value::Array(vec![rpc_receipt_payload(block)])
                }
                "eth_getCode" => {
                    let address = params
                        .first()
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_ascii_lowercase();
                    let code = if address == "0x0000000000000000000000000000000000000002" {
                        "0x"
                    } else {
                        "0x6001600155"
                    };
                    Value::String(code.to_owned())
                }
                _ => panic!("unexpected RPC request: {body}"),
            };

            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": result,
            })
        }))
        .await?;

        Ok((provider::JsonRpcProvider::new(&url)?, server))
    }

    fn transaction_hash_for_block(block: &ProviderBlock) -> String {
        let seed = format!(
            "{}{:x}",
            block.block_hash.trim_start_matches("0x"),
            block.block_number
        );
        let suffix = if seed.len() > 64 {
            &seed[seed.len() - 64..]
        } else {
            seed.as_str()
        };

        format!("0x{suffix:0>64}")
    }

    fn rpc_block_bundle_payload(block: &ProviderBlock) -> Value {
        let transaction_hash = transaction_hash_for_block(block);
        json!({
            "hash": block.block_hash.clone(),
            "parentHash": block.parent_hash.clone().unwrap_or_else(|| {
                "0x0000000000000000000000000000000000000000000000000000000000000000".to_owned()
            }),
            "number": format!("0x{:x}", block.block_number),
            "timestamp": format!("0x{:x}", block.block_timestamp_unix_secs),
            "logsBloom": block.logs_bloom.as_ref().map(|bytes| hex_string(bytes)),
            "transactionsRoot": block.transactions_root.clone(),
            "receiptsRoot": block.receipts_root.clone(),
            "stateRoot": block.state_root.clone(),
            "transactions": [
                {
                    "hash": transaction_hash,
                    "blockHash": block.block_hash.clone(),
                    "blockNumber": format!("0x{:x}", block.block_number),
                    "transactionIndex": "0x0",
                    "from": "0x0000000000000000000000000000000000000001",
                    "to": "0x0000000000000000000000000000000000000002"
                }
            ]
        })
    }

    fn rpc_receipt_payload(block: &ProviderBlock) -> Value {
        json!({
            "transactionHash": transaction_hash_for_block(block),
            "blockHash": block.block_hash.clone(),
            "blockNumber": format!("0x{:x}", block.block_number),
            "transactionIndex": "0x0",
            "contractAddress": null,
            "status": "0x1",
            "gasUsed": "0x5208",
            "cumulativeGasUsed": "0x5208",
            "logsBloom": block.logs_bloom.as_ref().map(|bytes| hex_string(bytes)),
        })
    }

    fn dns_encoded_test_name() -> Vec<u8> {
        vec![
            7, b'w', b'r', b'a', b'p', b'p', b'e', b'd', 3, b'e', b't', b'h', 0,
        ]
    }

    fn name_wrapped_topic0() -> String {
        keccak256_hex(b"NameWrapped(bytes,bytes32,address,uint32,uint64)")
    }

    fn namehash_for_dns_name(dns_name: &[u8]) -> String {
        let mut labels = Vec::<Vec<u8>>::new();
        let mut cursor = 0usize;
        while cursor < dns_name.len() {
            let length = usize::from(dns_name[cursor]);
            cursor += 1;
            if length == 0 {
                break;
            }
            labels.push(dns_name[cursor..cursor + length].to_vec());
            cursor += length;
        }

        let mut node = [0u8; 32];
        for label in labels.iter().rev() {
            let label_hash = {
                let mut hasher = Keccak256::new();
                hasher.update(label);
                let digest = hasher.finalize();
                let mut output = [0u8; 32];
                output.copy_from_slice(&digest);
                output
            };
            let mut hasher = Keccak256::new();
            hasher.update(node);
            hasher.update(label_hash);
            let digest = hasher.finalize();
            node.copy_from_slice(&digest);
        }

        hex_string(&node)
    }

    fn encode_name_wrapped_log_data(dns_name: &[u8]) -> String {
        let mut data = Vec::new();

        let mut push_word = |value: [u8; 32]| data.extend_from_slice(&value);
        push_word(abi_word_u64(128));
        push_word(abi_word_address(
            "0x0000000000000000000000000000000000000001",
        ));
        push_word(abi_word_u64(0));
        push_word(abi_word_u64(0));
        push_word(abi_word_u64(
            u64::try_from(dns_name.len()).expect("dns name test payload length must fit in u64"),
        ));
        data.extend_from_slice(dns_name);
        let padded_length = ((dns_name.len() + 31) / 32) * 32;
        data.resize(32 * 5 + padded_length, 0);

        hex_string(&data)
    }

    fn abi_word_u64(value: u64) -> [u8; 32] {
        let mut word = [0u8; 32];
        word[24..].copy_from_slice(&value.to_be_bytes());
        word
    }

    fn abi_word_address(address: &str) -> [u8; 32] {
        let address = address.strip_prefix("0x").unwrap_or(address);
        let mut word = [0u8; 32];
        for (index, chunk) in address.as_bytes().chunks(2).enumerate() {
            let hex = std::str::from_utf8(chunk).expect("test address must be utf-8 hex");
            word[12 + index] =
                u8::from_str_radix(hex, 16).expect("test address chunk must be valid hex");
        }
        word
    }

    fn rpc_log_payload(block: &ProviderBlock) -> Value {
        let dns_name = dns_encoded_test_name();
        json!({
            "blockHash": block.block_hash.clone(),
            "blockNumber": format!("0x{:x}", block.block_number),
            "transactionHash": transaction_hash_for_block(block),
            "transactionIndex": "0x0",
            "logIndex": "0x0",
            "address": "0x0000000000000000000000000000000000000001",
            "topics": [
                name_wrapped_topic0(),
                namehash_for_dns_name(&dns_name)
            ],
            "data": encode_name_wrapped_log_data(&dns_name)
        })
    }

    fn hex_string(bytes: &[u8]) -> String {
        let mut output = String::from("0x");
        for byte in bytes {
            output.push_str(&format!("{byte:02x}"));
        }
        output
    }

    async fn spawn_json_rpc_server(
        handler: Arc<dyn Fn(Value) -> Value + Send + Sync>,
    ) -> Result<(String, JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("failed to bind JSON-RPC test server")?;
        let address = listener
            .local_addr()
            .context("failed to read JSON-RPC test server address")?;
        let url = format!("http://{address}");

        let server = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let handler = Arc::clone(&handler);
                tokio::spawn(async move {
                    let mut buffer = Vec::new();
                    let mut chunk = [0_u8; 4096];
                    loop {
                        let Ok(bytes_read) = stream.read(&mut chunk).await else {
                            return;
                        };
                        if bytes_read == 0 {
                            return;
                        }
                        buffer.extend_from_slice(&chunk[..bytes_read]);
                        if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
                            break;
                        }
                    }

                    let header_end = buffer
                        .windows(4)
                        .position(|window| window == b"\r\n\r\n")
                        .map(|index| index + 4)
                        .expect("HTTP request must contain header terminator");
                    let header = &buffer[..header_end];
                    let header_text = String::from_utf8_lossy(header).to_ascii_lowercase();
                    let content_length = header_text
                        .lines()
                        .find_map(|line| {
                            line.strip_prefix("content-length:")
                                .and_then(|value| value.trim().parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    let mut body = buffer[header_end..].to_vec();
                    while body.len() < content_length {
                        let Ok(bytes_read) = stream.read(&mut chunk).await else {
                            return;
                        };
                        if bytes_read == 0 {
                            return;
                        }
                        body.extend_from_slice(&chunk[..bytes_read]);
                    }
                    body.truncate(content_length);

                    let request_body = serde_json::from_slice::<Value>(&body)
                        .expect("JSON-RPC test request body must decode");
                    let response_body = handler(request_body).to_string();
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                        response_body.len(),
                        response_body
                    );

                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        });

        Ok((url, server))
    }

    #[test]
    fn ensure_manifest_root_ready_accepts_loaded_root() -> Result<()> {
        ensure_manifest_root_ready(&manifest_load_summary(ManifestLoadStatus::Loaded))
    }

    #[test]
    fn ensure_manifest_root_ready_accepts_empty_root() -> Result<()> {
        ensure_manifest_root_ready(&manifest_load_summary(ManifestLoadStatus::Empty))
    }

    #[test]
    fn ensure_manifest_root_ready_rejects_missing_root() {
        let error =
            ensure_manifest_root_ready(&manifest_load_summary(ManifestLoadStatus::MissingRoot))
                .expect_err("missing root must fail");

        assert!(
            error
                .to_string()
                .contains("refusing to boot on stale stored manifest state")
        );
        assert!(
            error.to_string().contains("/tmp/manifests does not exist"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn ensure_manifest_root_ready_rejects_invalid_root() {
        let error =
            ensure_manifest_root_ready(&manifest_load_summary(ManifestLoadStatus::InvalidRoot))
                .expect_err("invalid root must fail");

        assert!(
            error
                .to_string()
                .contains("refusing to boot on stale stored manifest state")
        );
        assert!(
            error
                .to_string()
                .contains("/tmp/manifests is not a directory"),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn verify_stored_manifest_state_accepts_matching_active_manifest_count() -> Result<()> {
        let database = TestDatabase::new().await?;
        let admission_state = load_discovery_admission_state(database.pool()).await?;

        verify_stored_manifest_state(&synced_manifest_summary(0), &admission_state)?;

        database.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn verify_stored_manifest_state_rejects_mismatched_active_manifest_count() -> Result<()> {
        let database = TestDatabase::new().await?;
        let admission_state = load_discovery_admission_state(database.pool()).await?;

        let error = verify_stored_manifest_state(&synced_manifest_summary(1), &admission_state)
            .expect_err("mismatched counts must fail");

        assert!(
            error.to_string().contains(
                "stored active manifest count 0 does not match the synced active manifest count 1"
            ),
            "unexpected error: {error:#}"
        );

        database.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn load_watched_contract_summary_rebuilds_counts_from_storage() -> Result<()> {
        let database = TestDatabase::new().await?;

        sqlx::query(
            r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES
                (1, 'ethereum-mainnet', 'active'),
                (2, 'base-mainnet', 'shadow')
            "#,
        )
        .execute(database.pool())
        .await
        .context("failed to insert manifest_versions for watched summary test")?;
        sqlx::query(
            r#"
            INSERT INTO manifest_roots (manifest_id, address)
            VALUES (1, '0x0000000000000000000000000000000000000001')
            "#,
        )
        .execute(database.pool())
        .await
        .context("failed to insert manifest_roots for watched summary test")?;
        sqlx::query(
            r#"
            INSERT INTO manifest_contracts (manifest_id, role, address, implementation)
            VALUES
                (
                    1,
                    'registry',
                    '0x00000000000000000000000000000000000000aa',
                    '0x00000000000000000000000000000000000000dd'
                ),
                (
                    2,
                    'registry',
                    '0x00000000000000000000000000000000000000bb',
                    NULL
                )
            "#,
        )
        .execute(database.pool())
        .await
        .context("failed to insert manifest_contracts for watched summary test")?;
        sqlx::query(
            r#"
            INSERT INTO discovery_edges (chain_id, to_address, source_manifest_id)
            VALUES
                ('ethereum-mainnet', '0x00000000000000000000000000000000000000cc', 1),
                ('ethereum-mainnet', '0x00000000000000000000000000000000000000dd', 1)
            "#,
        )
        .execute(database.pool())
        .await
        .context("failed to insert discovery_edges for watched summary test")?;

        let summary = load_watched_contract_summary(database.pool()).await?;
        assert_eq!(summary.unique_contract_count, 4);
        assert_eq!(summary.source_entry_count, 4);
        assert_eq!(summary.manifest_root_count, 1);
        assert_eq!(summary.manifest_contract_count, 1);
        assert_eq!(summary.discovery_edge_count, 2);
        assert_eq!(summary.chains.len(), 1);
        assert_eq!(summary.chains[0].chain, "ethereum-mainnet");
        assert_eq!(summary.chains[0].unique_contract_count, 4);
        assert_eq!(summary.chains[0].manifest_root_count, 1);
        assert_eq!(summary.chains[0].manifest_contract_count, 1);
        assert_eq!(summary.chains[0].discovery_edge_count, 2);

        let watched_plan = load_watched_chain_plan(database.pool()).await?;
        assert_eq!(watched_plan.len(), 1);
        assert_eq!(
            watched_plan[0].addresses,
            vec![
                "0x0000000000000000000000000000000000000001".to_owned(),
                "0x00000000000000000000000000000000000000aa".to_owned(),
                "0x00000000000000000000000000000000000000cc".to_owned(),
                "0x00000000000000000000000000000000000000dd".to_owned(),
            ]
        );
        assert_eq!(watched_plan[0].manifest_root_entry_count, 1);
        assert_eq!(watched_plan[0].manifest_contract_entry_count, 1);
        assert_eq!(watched_plan[0].discovery_edge_entry_count, 2);

        database.cleanup().await?;
        Ok(())
    }

    #[test]
    fn watched_chain_plan_state_counts_chains_addresses_and_entries() {
        let state = watched_chain_plan_state(&[
            WatchedChainPlan {
                chain: "ethereum-mainnet".to_owned(),
                addresses: vec![
                    "0x0000000000000000000000000000000000000001".to_owned(),
                    "0x00000000000000000000000000000000000000aa".to_owned(),
                ],
                manifest_root_entry_count: 1,
                manifest_contract_entry_count: 1,
                discovery_edge_entry_count: 0,
            },
            WatchedChainPlan {
                chain: "base-mainnet".to_owned(),
                addresses: vec!["0x00000000000000000000000000000000000000bb".to_owned()],
                manifest_root_entry_count: 0,
                manifest_contract_entry_count: 1,
                discovery_edge_entry_count: 1,
            },
        ]);

        assert_eq!(
            state,
            WatchedChainPlanState {
                chain_count: 2,
                address_count: 3,
                entry_count: 4,
            }
        );
    }

    #[test]
    fn intake_runtime_state_counts_checkpoint_modes() {
        let state = intake_runtime_state(&[
            IntakeChainTask {
                chain: "ethereum-mainnet".to_owned(),
                addresses: vec![
                    "0x0000000000000000000000000000000000000001".to_owned(),
                    "0x00000000000000000000000000000000000000aa".to_owned(),
                ],
                manifest_root_entry_count: 1,
                manifest_contract_entry_count: 1,
                discovery_edge_entry_count: 0,
                checkpoint: ChainCheckpoint {
                    chain_id: "ethereum-mainnet".to_owned(),
                    canonical_block_hash: Some(
                        "0x00000000000000000000000000000000000000000000000000000000000000aa"
                            .to_owned(),
                    ),
                    canonical_block_number: Some(42),
                    safe_block_hash: Some(
                        "0x0000000000000000000000000000000000000000000000000000000000000099"
                            .to_owned(),
                    ),
                    safe_block_number: Some(41),
                    finalized_block_hash: None,
                    finalized_block_number: None,
                },
            },
            IntakeChainTask {
                chain: "base-mainnet".to_owned(),
                addresses: vec!["0x00000000000000000000000000000000000000bb".to_owned()],
                manifest_root_entry_count: 0,
                manifest_contract_entry_count: 1,
                discovery_edge_entry_count: 1,
                checkpoint: ChainCheckpoint {
                    chain_id: "base-mainnet".to_owned(),
                    canonical_block_hash: None,
                    canonical_block_number: None,
                    safe_block_hash: None,
                    safe_block_number: None,
                    finalized_block_hash: None,
                    finalized_block_number: None,
                },
            },
        ]);

        assert_eq!(
            state,
            IntakeRuntimeState {
                chain_count: 2,
                address_count: 3,
                entry_count: 4,
                cold_start_chain_count: 1,
                resumable_chain_count: 1,
                safe_checkpoint_chain_count: 1,
                finalized_checkpoint_chain_count: 0,
            }
        );
    }

    #[tokio::test]
    async fn sync_intake_chain_tasks_creates_missing_checkpoint_rows() -> Result<()> {
        let database = TestDatabase::new().await?;

        sqlx::query(
            r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
        )
        .execute(database.pool())
        .await
        .context("failed to insert manifest_versions for intake task sync test")?;
        sqlx::query(
            r#"
            INSERT INTO manifest_roots (manifest_id, address)
            VALUES (1, '0x0000000000000000000000000000000000000001')
            "#,
        )
        .execute(database.pool())
        .await
        .context("failed to insert manifest_roots for intake task sync test")?;

        let watched_plan = load_watched_chain_plan(database.pool()).await?;
        let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;

        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].chain, "ethereum-mainnet");
        assert_eq!(
            tasks[0].addresses,
            vec!["0x0000000000000000000000000000000000000001".to_owned()]
        );
        assert_eq!(checkpoint_mode(&tasks[0].checkpoint), "cold_start");
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_checkpoints")
                .fetch_one(database.pool())
                .await?,
            1
        );

        database.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn sync_intake_chain_tasks_preserves_manifest_contract_implementation_addresses()
    -> Result<()> {
        let database = TestDatabase::new().await?;
        let contract_address = "0x00000000000000000000000000000000000000aa";
        let implementation_address = "0x00000000000000000000000000000000000000bb";

        sqlx::query(
            r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
        )
        .execute(database.pool())
        .await
        .context("failed to insert manifest_versions for implementation watch-plan test")?;
        sqlx::query(
            r#"
            INSERT INTO manifest_contracts (manifest_id, role, address, implementation)
            VALUES (1, 'registry', $1, $2)
            "#,
        )
        .bind(contract_address)
        .bind(implementation_address)
        .execute(database.pool())
        .await
        .context("failed to insert manifest_contracts for implementation watch-plan test")?;
        sqlx::query(
            r#"
            INSERT INTO discovery_edges (chain_id, to_address, source_manifest_id)
            VALUES ('ethereum-mainnet', $1, 1)
            "#,
        )
        .bind(implementation_address)
        .execute(database.pool())
        .await
        .context("failed to insert discovery_edges for implementation watch-plan test")?;

        let watched_plan = load_watched_chain_plan(database.pool()).await?;

        let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;

        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].chain, "ethereum-mainnet");
        assert_eq!(
            tasks[0].addresses,
            vec![
                contract_address.to_owned(),
                implementation_address.to_owned()
            ]
        );
        assert_eq!(tasks[0].manifest_root_entry_count, 0);
        assert_eq!(tasks[0].manifest_contract_entry_count, 1);
        assert_eq!(tasks[0].discovery_edge_entry_count, 1);
        assert_eq!(checkpoint_mode(&tasks[0].checkpoint), "cold_start");
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_checkpoints")
                .fetch_one(database.pool())
                .await?,
            1
        );

        database.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_fetched_heads_initializes_chain_from_provider_heads() -> Result<()> {
        let database = TestDatabase::new().await?;

        sqlx::query(
            r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
        )
        .execute(database.pool())
        .await
        .context("failed to insert manifest_versions for cold start reconciliation test")?;
        sqlx::query(
            r#"
            INSERT INTO manifest_roots (manifest_id, address)
            VALUES (1, '0x0000000000000000000000000000000000000001')
            "#,
        )
        .execute(database.pool())
        .await
        .context("failed to insert manifest_roots for cold start reconciliation test")?;

        let watched_plan = load_watched_chain_plan(database.pool()).await?;
        let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
        let canonical_head = provider_block(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            42,
        );
        let safe_head = provider_block(
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            Some("0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"),
            41,
        );
        let finalized_head = provider_block(
            "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            Some("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
            40,
        );
        let (provider, server) = bundle_provider(vec![
            canonical_head.clone(),
            safe_head.clone(),
            finalized_head.clone(),
        ])
        .await?;

        let (next_task, outcome) = reconcile_fetched_heads(
            database.pool(),
            &tasks[0],
            &provider,
            &ProviderHeadSnapshot {
                canonical: canonical_head,
                safe: Some(safe_head),
                finalized: Some(finalized_head),
            },
        )
        .await?
        .expect("cold start reconciliation must update task state");

        assert_eq!(
            outcome.canonical_status,
            CanonicalReconciliationStatus::Initialized
        );
        assert!(outcome.canonical_head_changed);
        assert!(outcome.safe_head_changed);
        assert!(outcome.finalized_head_changed);
        assert_eq!(next_task.checkpoint.canonical_block_number, Some(42));
        assert_eq!(next_task.checkpoint.safe_block_number, Some(41));
        assert_eq!(next_task.checkpoint.finalized_block_number, Some(40));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_lineage")
                .fetch_one(database.pool())
                .await?,
            3
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_blocks")
                .fetch_one(database.pool())
                .await?,
            3
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_transactions")
                .fetch_one(database.pool())
                .await?,
            3
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_code_hashes")
                .fetch_one(database.pool())
                .await?,
            3
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_receipts")
                .fetch_one(database.pool())
                .await?,
            3
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs")
                .fetch_one(database.pool())
                .await?,
            3
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
                .fetch_one(database.pool())
                .await?,
            3
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PreimageObserved'"
            )
            .fetch_one(database.pool())
            .await?,
            3
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_number = 42"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_number = 41"
            )
            .fetch_one(database.pool())
            .await?,
            "safe".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_number = 40"
            )
            .fetch_one(database.pool())
            .await?,
            "finalized".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_number = 42"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_number = 41"
            )
            .fetch_one(database.pool())
            .await?,
            "safe".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_number = 40"
            )
            .fetch_one(database.pool())
            .await?,
            "finalized".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_number = 42"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_number = 41"
            )
            .fetch_one(database.pool())
            .await?,
            "safe".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_number = 40"
            )
            .fetch_one(database.pool())
            .await?,
            "finalized".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_transactions WHERE block_number = 42"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_receipts WHERE block_number = 41"
            )
            .fetch_one(database.pool())
            .await?,
            "safe".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_logs WHERE block_number = 40"
            )
            .fetch_one(database.pool())
            .await?,
            "finalized".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM normalized_events WHERE block_number = 42"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM normalized_events WHERE block_number = 41"
            )
            .fetch_one(database.pool())
            .await?,
            "safe".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM normalized_events WHERE block_number = 40"
            )
            .fetch_one(database.pool())
            .await?,
            "finalized".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->>'decoded_name' FROM normalized_events WHERE block_number = 42"
            )
            .fetch_one(database.pool())
            .await?,
            "wrapped.eth".to_owned()
        );

        server.abort();
        database.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn build_manifest_runtime_state_loads_checked_in_repository_seed() -> Result<()> {
        let database = TestDatabase::new().await?;
        let manifests_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../manifests");
        let manifest_repository = load_manifest_repository(&manifests_root)?;

        let runtime_state =
            build_manifest_runtime_state(database.pool(), &manifest_repository).await?;

        assert_eq!(
            runtime_state.manifest_summary.status,
            ManifestLoadStatus::Loaded
        );
        assert_eq!(runtime_state.manifest_summary.namespace_count, 2);
        assert_eq!(runtime_state.manifest_summary.source_family_count, 3);
        assert_eq!(runtime_state.manifest_summary.manifest_count, 3);
        assert_eq!(
            runtime_state.sync_summary.status,
            ManifestSyncStatus::Synced
        );
        assert_eq!(runtime_state.sync_summary.synced_manifest_count, 3);
        assert_eq!(runtime_state.sync_summary.active_manifest_count, 2);
        assert_eq!(runtime_state.sync_summary.root_count, 2);
        assert_eq!(runtime_state.sync_summary.contract_count, 2);
        assert_eq!(runtime_state.sync_summary.capability_count, 5);
        assert_eq!(runtime_state.sync_summary.discovery_rule_count, 1);
        assert_eq!(runtime_state.discovery_admission.active_manifest_count, 2);
        assert_eq!(runtime_state.discovery_admission.active_root_count, 2);
        assert_eq!(runtime_state.discovery_admission.active_contract_count, 2);
        assert_eq!(runtime_state.discovery_admission.active_rule_count, 1);
        assert_eq!(
            runtime_state
                .manifest_normalized_event_summary
                .total_synced_count,
            6
        );
        assert_eq!(
            runtime_state.watched_contract_summary.unique_contract_count,
            2
        );
        assert_eq!(runtime_state.watched_contract_summary.source_entry_count, 4);
        assert_eq!(
            runtime_state.watched_contract_summary.manifest_root_count,
            2
        );
        assert_eq!(
            runtime_state
                .watched_contract_summary
                .manifest_contract_count,
            2
        );
        assert_eq!(
            runtime_state.watched_contract_summary.discovery_edge_count,
            0
        );
        assert_eq!(
            runtime_state.watched_chain_plan,
            vec![WatchedChainPlan {
                chain: "ethereum-mainnet".to_owned(),
                addresses: vec![
                    "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
                    "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85".to_owned(),
                ],
                manifest_root_entry_count: 2,
                manifest_contract_entry_count: 2,
                discovery_edge_entry_count: 0,
            }]
        );

        let stored_admission = load_discovery_admission_state(database.pool()).await?;
        assert_eq!(stored_admission.active_manifest_count, 2);

        database.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn refresh_watched_chain_plan_detects_storage_changes() -> Result<()> {
        let database = TestDatabase::new().await?;

        sqlx::query(
            r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
        )
        .execute(database.pool())
        .await
        .context("failed to insert manifest_versions for watched plan refresh test")?;
        sqlx::query(
            r#"
            INSERT INTO manifest_roots (manifest_id, address)
            VALUES (1, '0x0000000000000000000000000000000000000001')
            "#,
        )
        .execute(database.pool())
        .await
        .context("failed to insert manifest_roots for watched plan refresh test")?;

        let initial_plan = load_watched_chain_plan(database.pool()).await?;
        assert_eq!(
            refresh_watched_chain_plan(database.pool(), &initial_plan).await?,
            None
        );

        sqlx::query(
            r#"
            INSERT INTO discovery_edges (chain_id, to_address, source_manifest_id)
            VALUES ('ethereum-mainnet', '0x00000000000000000000000000000000000000cc', 1)
            "#,
        )
        .execute(database.pool())
        .await
        .context("failed to insert discovery_edges for watched plan refresh test")?;

        let refreshed_plan = refresh_watched_chain_plan(database.pool(), &initial_plan)
            .await?
            .expect("watch plan change must be detected");
        assert_eq!(refreshed_plan.len(), 1);
        assert_eq!(refreshed_plan[0].chain, "ethereum-mainnet");
        assert_eq!(
            refreshed_plan[0].addresses,
            vec![
                "0x0000000000000000000000000000000000000001".to_owned(),
                "0x00000000000000000000000000000000000000cc".to_owned(),
            ]
        );
        assert_eq!(refreshed_plan[0].manifest_root_entry_count, 1);
        assert_eq!(refreshed_plan[0].manifest_contract_entry_count, 0);
        assert_eq!(refreshed_plan[0].discovery_edge_entry_count, 1);

        database.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn refresh_intake_chain_tasks_detects_checkpoint_updates() -> Result<()> {
        let database = TestDatabase::new().await?;

        sqlx::query(
            r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
        )
        .execute(database.pool())
        .await
        .context("failed to insert manifest_versions for checkpoint refresh test")?;
        sqlx::query(
            r#"
            INSERT INTO manifest_roots (manifest_id, address)
            VALUES (1, '0x0000000000000000000000000000000000000001')
            "#,
        )
        .execute(database.pool())
        .await
        .context("failed to insert manifest_roots for checkpoint refresh test")?;

        let watched_plan = load_watched_chain_plan(database.pool()).await?;
        let initial_tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
        assert_eq!(
            refresh_intake_chain_tasks(database.pool(), &initial_tasks, &watched_plan).await?,
            None
        );

        sqlx::query(
            r#"
            UPDATE chain_checkpoints
            SET
                canonical_block_hash = '0x00000000000000000000000000000000000000000000000000000000000000aa',
                canonical_block_number = 42,
                safe_block_hash = '0x0000000000000000000000000000000000000000000000000000000000000099',
                safe_block_number = 41,
                finalized_block_hash = '0x0000000000000000000000000000000000000000000000000000000000000088',
                finalized_block_number = 40
            WHERE chain_id = 'ethereum-mainnet'
            "#,
        )
        .execute(database.pool())
        .await
        .context("failed to update chain_checkpoints for checkpoint refresh test")?;

        let refreshed_tasks =
            refresh_intake_chain_tasks(database.pool(), &initial_tasks, &watched_plan)
                .await?
                .expect("checkpoint change must be detected");
        assert_eq!(refreshed_tasks.len(), 1);
        assert_eq!(
            refreshed_tasks[0].checkpoint.canonical_block_number,
            Some(42)
        );
        assert_eq!(checkpoint_mode(&refreshed_tasks[0].checkpoint), "resume");
        assert_eq!(
            intake_runtime_state(&refreshed_tasks),
            IntakeRuntimeState {
                chain_count: 1,
                address_count: 1,
                entry_count: 1,
                cold_start_chain_count: 0,
                resumable_chain_count: 1,
                safe_checkpoint_chain_count: 1,
                finalized_checkpoint_chain_count: 1,
            }
        );

        database.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn build_manifest_runtime_state_reloads_repository_changes_without_restart() -> Result<()>
    {
        let database = TestDatabase::new().await?;
        let manifests = TestManifestDir::new()?;
        let manifest_path = manifests.write_manifest(&manifest_contents(
            "0x0000000000000000000000000000000000000001",
            "shadow",
        ))?;

        let initial_repository = load_manifest_repository(&manifests.path)?;
        assert_eq!(
            initial_repository.summary().status,
            ManifestLoadStatus::Loaded
        );
        let initial_state =
            build_manifest_runtime_state(database.pool(), &initial_repository).await?;
        assert_eq!(initial_state.watched_chain_plan.len(), 1);
        assert_eq!(
            initial_state.watched_chain_plan[0].addresses,
            vec![
                "0x0000000000000000000000000000000000000001".to_owned(),
                "0x00000000000000000000000000000000000000aa".to_owned(),
            ]
        );
        assert_eq!(
            initial_state
                .manifest_normalized_event_summary
                .total_inserted_count,
            2
        );

        fs::write(
            &manifest_path,
            manifest_contents("0x0000000000000000000000000000000000000002", "supported"),
        )
        .with_context(|| format!("failed to rewrite {}", manifest_path.display()))?;

        let refreshed_repository = load_manifest_repository(&manifests.path)?;
        let refreshed_state =
            build_manifest_runtime_state(database.pool(), &refreshed_repository).await?;
        assert_eq!(
            refreshed_state.watched_chain_plan,
            vec![WatchedChainPlan {
                chain: "ethereum-mainnet".to_owned(),
                addresses: vec![
                    "0x0000000000000000000000000000000000000002".to_owned(),
                    "0x00000000000000000000000000000000000000aa".to_owned(),
                ],
                manifest_root_entry_count: 1,
                manifest_contract_entry_count: 1,
                discovery_edge_entry_count: 0,
            }]
        );
        assert_eq!(
            refreshed_state
                .manifest_normalized_event_summary
                .total_inserted_count,
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE event_kind = 'CapabilityChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            2
        );

        database.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn build_manifest_runtime_state_accepts_empty_root_and_clears_watch_plan() -> Result<()> {
        let database = TestDatabase::new().await?;
        let manifests = TestManifestDir::new()?;
        let manifest_path = manifests.write_manifest(&manifest_contents(
            "0x0000000000000000000000000000000000000001",
            "shadow",
        ))?;

        let initial_repository = load_manifest_repository(&manifests.path)?;
        let initial_state =
            build_manifest_runtime_state(database.pool(), &initial_repository).await?;
        assert_eq!(
            initial_state.manifest_summary.status,
            ManifestLoadStatus::Loaded
        );
        assert_eq!(initial_state.watched_chain_plan.len(), 1);

        fs::remove_file(&manifest_path)
            .with_context(|| format!("failed to remove {}", manifest_path.display()))?;

        let empty_repository = load_manifest_repository(&manifests.path)?;
        assert_eq!(empty_repository.summary().status, ManifestLoadStatus::Empty);
        let empty_state = build_manifest_runtime_state(database.pool(), &empty_repository).await?;
        assert_eq!(
            empty_state.manifest_summary.status,
            ManifestLoadStatus::Empty
        );
        assert!(empty_state.watched_chain_plan.is_empty());
        assert_eq!(empty_state.discovery_admission.active_manifest_count, 0);
        assert_eq!(empty_state.watched_contract_summary.source_entry_count, 0);

        database.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_fetched_heads_backfills_code_hashes_for_new_watched_addresses() -> Result<()>
    {
        let database = TestDatabase::new().await?;

        sqlx::query(
            r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
        )
        .execute(database.pool())
        .await
        .context("failed to insert manifest_versions for code-hash backfill test")?;
        sqlx::query(
            r#"
            INSERT INTO manifest_roots (manifest_id, address)
            VALUES (1, '0x0000000000000000000000000000000000000001')
            "#,
        )
        .execute(database.pool())
        .await
        .context("failed to insert initial manifest_roots for code-hash backfill test")?;

        let watched_plan = load_watched_chain_plan(database.pool()).await?;
        let mut tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
        let canonical_head = provider_block(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            42,
        );
        let (provider, server) = bundle_provider(vec![canonical_head.clone()]).await?;

        let (next_task, initial_outcome) = reconcile_fetched_heads(
            database.pool(),
            &tasks[0],
            &provider,
            &ProviderHeadSnapshot {
                canonical: canonical_head.clone(),
                safe: None,
                finalized: None,
            },
        )
        .await?
        .expect("initial code-hash reconciliation must update task state");
        assert_eq!(
            initial_outcome.canonical_status,
            CanonicalReconciliationStatus::Initialized
        );
        tasks[0] = next_task;
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_code_hashes")
                .fetch_one(database.pool())
                .await?,
            1
        );

        sqlx::query(
            r#"
            INSERT INTO manifest_roots (manifest_id, address)
            VALUES (1, '0x0000000000000000000000000000000000000002')
            "#,
        )
        .execute(database.pool())
        .await
        .context("failed to insert second watched manifest root for code-hash backfill test")?;

        let watched_plan = load_watched_chain_plan(database.pool()).await?;
        tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
        let unchanged = reconcile_fetched_heads(
            database.pool(),
            &tasks[0],
            &provider,
            &ProviderHeadSnapshot {
                canonical: canonical_head,
                safe: None,
                finalized: None,
            },
        )
        .await?;
        assert!(
            unchanged.is_none(),
            "unchanged heads should not report a task transition"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_code_hashes")
                .fetch_one(database.pool())
                .await?,
            2
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT code_byte_length FROM raw_code_hashes WHERE contract_address = '0x0000000000000000000000000000000000000002'"
            )
            .fetch_one(database.pool())
            .await?,
            0
        );

        server.abort();
        database.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_fetched_heads_marks_losing_branch_orphaned_on_reorg() -> Result<()> {
        let database = TestDatabase::new().await?;

        sqlx::query(
            r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
        )
        .execute(database.pool())
        .await
        .context("failed to insert manifest_versions for reorg reconciliation test")?;
        sqlx::query(
            r#"
            INSERT INTO manifest_roots (manifest_id, address)
            VALUES (1, '0x0000000000000000000000000000000000000001')
            "#,
        )
        .execute(database.pool())
        .await
        .context("failed to insert manifest_roots for reorg reconciliation test")?;

        let watched_plan = load_watched_chain_plan(database.pool()).await?;
        let mut tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
        let ancestor_block = provider_block(
            "0x1111111111111111111111111111111111111111111111111111111111111111",
            Some("0x0000000000000000000000000000000000000000000000000000000000000000"),
            41,
        );
        let losing_block = provider_block(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
            42,
        );
        let new_parent_block = provider_block(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
            42,
        );
        let new_head_block = provider_block(
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            43,
        );
        upsert_chain_lineage_blocks(
            database.pool(),
            &[provider_block_to_lineage(
                "ethereum-mainnet",
                &ancestor_block,
                CanonicalityState::Canonical,
            )],
        )
        .await?;
        upsert_chain_lineage_blocks(
            database.pool(),
            &[provider_block_to_lineage(
                "ethereum-mainnet",
                &losing_block,
                CanonicalityState::Canonical,
            )],
        )
        .await?;
        upsert_chain_lineage_blocks(
            database.pool(),
            &[provider_block_to_lineage(
                "ethereum-mainnet",
                &new_parent_block,
                CanonicalityState::Orphaned,
            )],
        )
        .await?;
        upsert_raw_blocks(
            database.pool(),
            &[
                provider_block_to_raw_block(
                    "ethereum-mainnet",
                    &ancestor_block,
                    CanonicalityState::Canonical,
                ),
                provider_block_to_raw_block(
                    "ethereum-mainnet",
                    &losing_block,
                    CanonicalityState::Canonical,
                ),
                provider_block_to_raw_block(
                    "ethereum-mainnet",
                    &new_parent_block,
                    CanonicalityState::Orphaned,
                ),
            ],
        )
        .await?;
        upsert_raw_transactions(
            database.pool(),
            &[RawTransaction {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: losing_block.block_hash.clone(),
                block_number: losing_block.block_number,
                transaction_hash: transaction_hash_for_block(&losing_block),
                transaction_index: 0,
                from_address: "0x0000000000000000000000000000000000000001".to_owned(),
                to_address: Some("0x0000000000000000000000000000000000000002".to_owned()),
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await?;
        upsert_raw_code_hashes(
            database.pool(),
            &[RawCodeHash {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: losing_block.block_hash.clone(),
                block_number: losing_block.block_number,
                contract_address: "0x0000000000000000000000000000000000000001".to_owned(),
                code_hash: "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
                    .to_owned(),
                code_byte_length: 32,
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await?;
        upsert_raw_receipts(
            database.pool(),
            &[RawReceipt {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: losing_block.block_hash.clone(),
                block_number: losing_block.block_number,
                transaction_hash: transaction_hash_for_block(&losing_block),
                transaction_index: 0,
                contract_address: None,
                status: Some(true),
                gas_used: Some(21_000),
                cumulative_gas_used: Some(21_000),
                logs_bloom: losing_block.logs_bloom.clone(),
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await?;
        upsert_raw_logs(
            database.pool(),
            &[RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: losing_block.block_hash.clone(),
                block_number: losing_block.block_number,
                transaction_hash: transaction_hash_for_block(&losing_block),
                transaction_index: 0,
                log_index: 0,
                emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
                topics: vec![
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                ],
                data: vec![0xde, 0xad, 0xbe, 0xef],
                canonicality_state: CanonicalityState::Canonical,
            }],
        )
        .await?;
        sqlx::query(
            r#"
            INSERT INTO normalized_events (
                event_identity,
                namespace,
                event_kind,
                source_family,
                manifest_version,
                source_manifest_id,
                chain_id,
                block_number,
                block_hash,
                transaction_hash,
                log_index,
                raw_fact_ref,
                derivation_kind,
                canonicality_state,
                before_state,
                after_state
            )
            VALUES (
                'raw_log_preimage_observed:1:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0xtx2a:0:0x00000000000000000000000000000000000000aa',
                'ens',
                'PreimageObserved',
                'ens_test',
                1,
                1,
                'ethereum-mainnet',
                42,
                '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                $1,
                0,
                '{"kind":"raw_log"}'::jsonb,
                'raw_log_preimage_observation',
                'canonical'::canonicality_state,
                '{}'::jsonb,
                '{"decoded_name":"wrapped.eth"}'::jsonb
            )
            "#,
        )
        .bind(transaction_hash_for_block(&losing_block))
        .execute(database.pool())
        .await
        .context("failed to insert normalized event for reorg reconciliation test")?;
        tasks[0].checkpoint = advance_chain_checkpoints(
            database.pool(),
            &ChainCheckpointUpdate {
                chain_id: "ethereum-mainnet".to_owned(),
                canonical: Some(CheckpointBlockRef {
                    block_hash:
                        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                            .to_owned(),
                    block_number: 42,
                }),
                safe: None,
                finalized: None,
            },
        )
        .await?;
        let (provider, server) =
            bundle_provider(vec![new_parent_block.clone(), new_head_block.clone()]).await?;

        let (next_task, outcome) = reconcile_fetched_heads(
            database.pool(),
            &tasks[0],
            &provider,
            &ProviderHeadSnapshot {
                canonical: new_head_block,
                safe: None,
                finalized: None,
            },
        )
        .await?
        .expect("reorg reconciliation must update task state");

        assert_eq!(
            outcome.canonical_status,
            CanonicalReconciliationStatus::ReorgReconciled
        );
        assert_eq!(outcome.orphaned_block_count, 1);
        assert_eq!(next_task.checkpoint.canonical_block_number, Some(43));
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_hash = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_hash = '0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_hash = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_hash = '0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_hash = '0x1111111111111111111111111111111111111111111111111111111111111111'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_transactions WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_receipts WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_logs WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM normalized_events WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_transactions WHERE block_hash = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_hash = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_transactions WHERE block_hash = '0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_hash = '0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );

        server.abort();
        database.cleanup().await?;
        Ok(())
    }
}
