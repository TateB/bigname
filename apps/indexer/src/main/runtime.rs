use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use bigname_adapters::{
    BlockDerivedNormalizedEventSyncSummary, EnsV1ReverseClaimSyncSummary,
    EnsV1SubregistryDiscoverySyncSummary, EnsV1UnwrappedAuthoritySyncSummary,
    EnsV2PermissionsSyncSummary, EnsV2RegistrarSyncSummary,
    EnsV2RegistryResourceSurfaceSyncSummary, EnsV2ResolverSyncSummary,
    ManifestNormalizedEventSyncSummary,
};
use bigname_manifests::{
    DiscoveryAdmissionState, ManifestLoadStatus, ManifestLoadSummary, ManifestRepository,
    ManifestSyncStatus, ManifestSyncSummary, WatchedChainPlan, WatchedContractSummary,
    load_watched_chain_plan, load_watched_contract_summary,
};
use bigname_storage::{ChainCheckpoint, sync_chain_checkpoints};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::provider::ProviderRegistry;
use crate::reconciliation::poll_provider_heads;
pub(crate) fn load_manifest_repository(manifests_root: &Path) -> Result<ManifestRepository> {
    bigname_manifests::load_repository(manifests_root).with_context(|| {
        format!(
            "failed to load repository manifests from {}",
            manifests_root.display()
        )
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DiscoveryAdmissionSnapshot {
    pub(crate) active_manifest_count: usize,
    pub(crate) active_root_count: usize,
    pub(crate) active_contract_count: usize,
    pub(crate) active_rule_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestRuntimeState {
    pub(crate) manifest_summary: ManifestLoadSummary,
    pub(crate) sync_summary: ManifestSyncSummary,
    pub(crate) discovery_admission: DiscoveryAdmissionSnapshot,
    pub(crate) manifest_normalized_event_summary: ManifestNormalizedEventSyncSummary,
    pub(crate) watched_contract_summary: WatchedContractSummary,
    pub(crate) watched_chain_plan: Vec<WatchedChainPlan>,
}

pub(crate) async fn build_manifest_runtime_state(
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

pub(crate) fn discovery_admission_snapshot(
    state: &DiscoveryAdmissionState,
) -> DiscoveryAdmissionSnapshot {
    DiscoveryAdmissionSnapshot {
        active_manifest_count: state.active_manifest_count,
        active_root_count: state.active_root_count,
        active_contract_count: state.active_contract_count,
        active_rule_count: state.active_rule_count,
    }
}

pub(crate) fn log_manifest_runtime_state(state: &ManifestRuntimeState) {
    log_manifest_sync_summary(&state.sync_summary);
    log_discovery_admission_state(&state.discovery_admission);
    log_manifest_normalized_event_summary(&state.manifest_normalized_event_summary);
    log_watched_contract_summary(&state.watched_contract_summary);
}

pub(crate) fn log_manifest_summary(summary: &ManifestLoadSummary) {
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

pub(crate) fn log_manifest_sync_summary(summary: &ManifestSyncSummary) {
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

pub(crate) fn ensure_manifest_root_ready(summary: &ManifestLoadSummary) -> Result<()> {
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

pub(crate) fn verify_stored_manifest_state(
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

pub(crate) fn log_discovery_admission_state(state: &DiscoveryAdmissionSnapshot) {
    info!(
        service = "indexer",
        stored_active_manifest_count = state.active_manifest_count,
        stored_active_root_count = state.active_root_count,
        stored_active_contract_count = state.active_contract_count,
        stored_active_rule_count = state.active_rule_count,
        "discovery admission rebuilt from stored manifest state"
    );
}

pub(crate) fn log_manifest_normalized_event_summary(summary: &ManifestNormalizedEventSyncSummary) {
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

pub(crate) fn log_block_derived_normalized_event_summary(
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

pub(crate) fn log_ens_v1_reverse_claim_sync_summary(
    chain: &str,
    summary: &EnsV1ReverseClaimSyncSummary,
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
        primary_claim_enriched_event_count = summary.total_synced_count,
        normalized_event_kind_count = summary.by_kind.len(),
        "ENSv1 reverse claim synced from stored raw logs with additive primary-claim enrichment"
    );

    for (event_kind, kind_summary) in &summary.by_kind {
        info!(
            service = "indexer",
            chain,
            event_kind,
            normalized_event_sync_count = kind_summary.synced_count,
            normalized_event_inserted_count = kind_summary.inserted_count,
            "ENSv1 reverse claim event kind synced"
        );
    }
}

pub(crate) fn log_ens_v1_unwrapped_authority_sync_summary(
    chain: &str,
    summary: &EnsV1UnwrappedAuthoritySyncSummary,
) {
    if summary.scanned_log_count == 0
        && summary.total_name_surface_count == 0
        && summary.total_resource_count == 0
        && summary.total_surface_binding_count == 0
        && summary.total_normalized_event_count == 0
    {
        return;
    }

    info!(
        service = "indexer",
        chain,
        scanned_raw_log_count = summary.scanned_log_count,
        matched_raw_log_count = summary.matched_log_count,
        identity_name_surface_count = summary.total_name_surface_count,
        identity_resource_count = summary.total_resource_count,
        identity_surface_binding_count = summary.total_surface_binding_count,
        identity_normalized_event_count = summary.total_normalized_event_count,
        identity_event_kind_count = summary.by_kind.len(),
        "ENSv1 unwrapped authority synced from stored raw logs"
    );

    for (event_kind, count) in &summary.by_kind {
        info!(
            service = "indexer",
            chain,
            event_kind,
            normalized_event_sync_count = count,
            "ENSv1 unwrapped authority event kind synced"
        );
    }
}

pub(crate) fn manifest_normalized_event_kind_count(
    summary: &ManifestNormalizedEventSyncSummary,
    event_kind: &str,
) -> usize {
    summary
        .by_kind
        .get(event_kind)
        .map(|kind_summary| kind_summary.synced_count)
        .unwrap_or(0)
}

pub(crate) async fn refresh_manifest_normalized_events_from_storage(
    pool: &sqlx::PgPool,
    manifest_runtime_state: &ManifestRuntimeState,
) -> Result<Option<ManifestRuntimeState>> {
    let next_summary = bigname_adapters::sync_manifest_normalized_events(pool).await?;
    if next_summary.total_inserted_count == 0 {
        return Ok(None);
    }

    let mut next_manifest_runtime_state = manifest_runtime_state.clone();
    next_manifest_runtime_state.manifest_normalized_event_summary = next_summary;
    Ok(Some(next_manifest_runtime_state))
}

pub(crate) fn log_watched_contract_summary(summary: &WatchedContractSummary) {
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

pub(crate) fn log_ens_v1_subregistry_discovery_sync_summary(
    chain: &str,
    summary: &EnsV1SubregistryDiscoverySyncSummary,
) {
    if summary.scanned_log_count == 0
        && summary.inserted_edge_count == 0
        && summary.deactivated_edge_count == 0
    {
        return;
    }

    info!(
        service = "indexer",
        chain,
        discovery_source_family = "ens_v1_registry",
        discovery_scanned_log_count = summary.scanned_log_count,
        discovery_matched_log_count = summary.matched_log_count,
        discovery_active_observation_count = summary.active_observation_count,
        discovery_active_edge_count = summary.active_edge_count,
        discovery_admitted_edge_count = summary.admitted_edge_count,
        discovery_inserted_edge_count = summary.inserted_edge_count,
        discovery_deactivated_edge_count = summary.deactivated_edge_count,
        "ENSv1 registry discovery synced from stored raw logs"
    );
}

pub(crate) fn log_ens_v2_registry_resource_surface_sync_summary(
    chain: &str,
    summary: &EnsV2RegistryResourceSurfaceSyncSummary,
) {
    if summary.scanned_log_count == 0
        && summary.total_name_surface_count == 0
        && summary.total_resource_count == 0
        && summary.total_surface_binding_count == 0
        && summary.total_normalized_event_count == 0
        && summary.inserted_edge_count == 0
        && summary.deactivated_edge_count == 0
    {
        return;
    }

    info!(
        service = "indexer",
        chain,
        scanned_raw_log_count = summary.scanned_log_count,
        matched_raw_log_count = summary.matched_log_count,
        identity_name_surface_count = summary.total_name_surface_count,
        identity_resource_count = summary.total_resource_count,
        identity_surface_binding_count = summary.total_surface_binding_count,
        identity_normalized_event_count = summary.total_normalized_event_count,
        discovery_active_observation_count = summary.active_discovery_observation_count,
        discovery_active_edge_count = summary.active_edge_count,
        discovery_admitted_edge_count = summary.admitted_edge_count,
        discovery_inserted_edge_count = summary.inserted_edge_count,
        discovery_deactivated_edge_count = summary.deactivated_edge_count,
        "ENSv2 registry resource/surface state synced from stored raw logs"
    );
}

pub(crate) fn log_ens_v2_registrar_sync_summary(chain: &str, summary: &EnsV2RegistrarSyncSummary) {
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
        "ENSv2 registrar facts synced from stored raw logs"
    );
}

pub(crate) fn log_ens_v2_resolver_sync_summary(chain: &str, summary: &EnsV2ResolverSyncSummary) {
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
        "ENSv2 resolver facts synced from stored raw logs"
    );
}

pub(crate) fn log_ens_v2_permissions_sync_summary(
    chain: &str,
    summary: &EnsV2PermissionsSyncSummary,
) {
    if summary.scanned_log_count == 0
        && summary.total_resource_count == 0
        && summary.total_synced_count == 0
    {
        return;
    }

    info!(
        service = "indexer",
        chain,
        scanned_raw_log_count = summary.scanned_log_count,
        matched_raw_log_count = summary.matched_log_count,
        identity_resource_count = summary.total_resource_count,
        normalized_event_sync_total_count = summary.total_synced_count,
        normalized_event_inserted_total_count = summary.total_inserted_count,
        normalized_event_kind_count = summary.by_kind.len(),
        "ENSv2 permissions facts synced from stored raw logs"
    );
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WatchedChainPlanState {
    pub(crate) chain_count: usize,
    pub(crate) address_count: usize,
    pub(crate) entry_count: usize,
}

pub(crate) fn watched_chain_plan_state(plan: &[WatchedChainPlan]) -> WatchedChainPlanState {
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

pub(crate) fn log_watched_chain_plan(stage: &'static str, plan: &[WatchedChainPlan]) {
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
pub(crate) struct IntakeChainTask {
    pub(crate) chain: String,
    pub(crate) addresses: Vec<String>,
    pub(crate) manifest_root_entry_count: usize,
    pub(crate) manifest_contract_entry_count: usize,
    pub(crate) discovery_edge_entry_count: usize,
    pub(crate) checkpoint: ChainCheckpoint,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IntakeRuntimeState {
    pub(crate) chain_count: usize,
    pub(crate) address_count: usize,
    pub(crate) entry_count: usize,
    pub(crate) cold_start_chain_count: usize,
    pub(crate) resumable_chain_count: usize,
    pub(crate) safe_checkpoint_chain_count: usize,
    pub(crate) finalized_checkpoint_chain_count: usize,
}

pub(crate) fn checkpoint_mode(checkpoint: &ChainCheckpoint) -> &'static str {
    if checkpoint.canonical_block_hash.is_some() && checkpoint.canonical_block_number.is_some() {
        "resume"
    } else {
        "cold_start"
    }
}

pub(crate) fn intake_runtime_state(tasks: &[IntakeChainTask]) -> IntakeRuntimeState {
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

pub(crate) async fn sync_intake_chain_tasks(
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

pub(crate) fn log_intake_chain_tasks(stage: &'static str, tasks: &[IntakeChainTask]) {
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

pub(crate) fn log_provider_registry(
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

pub(crate) async fn refresh_watched_chain_plan(
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

pub(crate) async fn refresh_intake_chain_tasks(
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

pub(crate) async fn sync_adapter_owned_raw_log_state(
    pool: &sqlx::PgPool,
    watched_chain_plan: &[WatchedChainPlan],
) -> Result<()> {
    for chain in watched_chain_plan {
        let summary = bigname_adapters::sync_ens_v1_reverse_claim(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv1 reverse claim from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v1_reverse_claim_sync_summary(&chain.chain, &summary);

        let summary = bigname_adapters::sync_ens_v1_unwrapped_authority(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv1 unwrapped authority from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v1_unwrapped_authority_sync_summary(&chain.chain, &summary);

        let summary = bigname_adapters::sync_ens_v2_registry_resource_surface(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 registry resource/surface state from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v2_registry_resource_surface_sync_summary(&chain.chain, &summary);

        let summary = bigname_adapters::sync_ens_v2_registrar(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 registrar state from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v2_registrar_sync_summary(&chain.chain, &summary);

        let summary = bigname_adapters::sync_ens_v2_resolver(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 resolver state from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v2_resolver_sync_summary(&chain.chain, &summary);

        let summary = bigname_adapters::sync_ens_v2_permissions(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 permissions state from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v2_permissions_sync_summary(&chain.chain, &summary);
    }

    Ok(())
}

pub(crate) async fn refresh_runtime_state_from_storage_discovery(
    pool: &sqlx::PgPool,
    manifest_runtime_state: &ManifestRuntimeState,
) -> Result<Option<(ManifestRuntimeState, Vec<IntakeChainTask>)>> {
    for chain in &manifest_runtime_state.watched_chain_plan {
        let summary = bigname_adapters::sync_ens_v1_subregistry_discovery(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv1 registry discovery from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v1_subregistry_discovery_sync_summary(&chain.chain, &summary);

        let summary = bigname_adapters::sync_ens_v2_registry_resource_surface(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 registry discovery from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v2_registry_resource_surface_sync_summary(&chain.chain, &summary);
    }

    let Some(next_watched_chain_plan) =
        refresh_watched_chain_plan(pool, &manifest_runtime_state.watched_chain_plan).await?
    else {
        return Ok(None);
    };
    sync_adapter_owned_raw_log_state(pool, &next_watched_chain_plan).await?;
    let next_intake_chain_tasks = sync_intake_chain_tasks(pool, &next_watched_chain_plan).await?;
    let mut next_manifest_runtime_state = manifest_runtime_state.clone();
    next_manifest_runtime_state.watched_contract_summary =
        load_watched_contract_summary(pool).await?;
    next_manifest_runtime_state.watched_chain_plan = next_watched_chain_plan;

    Ok(Some((next_manifest_runtime_state, next_intake_chain_tasks)))
}

pub(crate) async fn run_poll_loop(
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

                                    if (manifest_state_changed || watched_plan_changed)
                                        && let Err(error) = sync_adapter_owned_raw_log_state(
                                            pool,
                                            &next_manifest_runtime_state.watched_chain_plan,
                                        )
                                        .await
                                    {
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
                                            "failed to sync adapter-owned raw-log state after repository manifest refresh; keeping last successful runtime state"
                                        );
                                        continue;
                                    }

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

                match refresh_manifest_normalized_events_from_storage(
                    pool,
                    &manifest_runtime_state,
                )
                .await
                {
                    Ok(Some(next_manifest_runtime_state)) => {
                        info!(
                            service = "indexer",
                            refresh_reason = "timer",
                            plan_source = "stored_manifest_observations",
                            normalized_event_inserted_total_count = next_manifest_runtime_state
                                .manifest_normalized_event_summary
                                .total_inserted_count,
                            normalized_event_sync_total_count = next_manifest_runtime_state
                                .manifest_normalized_event_summary
                                .total_synced_count,
                            normalized_event_kind_count = next_manifest_runtime_state
                                .manifest_normalized_event_summary
                                .by_kind
                                .len(),
                            "manifest observation alert events changed after provider polling"
                        );
                        log_manifest_normalized_event_summary(
                            &next_manifest_runtime_state.manifest_normalized_event_summary,
                        );
                        manifest_runtime_state = next_manifest_runtime_state;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        let current_watch_state =
                            watched_chain_plan_state(&manifest_runtime_state.watched_chain_plan);
                        let current_intake_state = intake_runtime_state(&intake_chain_tasks);
                        warn!(
                            service = "indexer",
                            refresh_reason = "timer",
                            plan_source = "stored_manifest_observations",
                            error = ?error,
                            watched_chain_count = current_watch_state.chain_count,
                            watched_address_count = current_watch_state.address_count,
                            watched_entry_count_total = current_watch_state.entry_count,
                            intake_chain_count = current_intake_state.chain_count,
                            intake_address_count = current_intake_state.address_count,
                            intake_entry_count_total = current_intake_state.entry_count,
                            "failed to refresh manifest observation alert events after provider polling; keeping last successful state"
                        );
                    }
                }

                match refresh_runtime_state_from_storage_discovery(pool, &manifest_runtime_state)
                    .await
                {
                    Ok(Some((next_manifest_runtime_state, next_tasks))) => {
                        let previous_watch_state =
                            watched_chain_plan_state(&manifest_runtime_state.watched_chain_plan);
                        let next_watch_state =
                            watched_chain_plan_state(&next_manifest_runtime_state.watched_chain_plan);
                        let previous_intake_state = intake_runtime_state(&intake_chain_tasks);
                        let next_intake_state = intake_runtime_state(&next_tasks);

                        info!(
                            service = "indexer",
                            refresh_reason = "timer",
                            watched_plan_changed = true,
                            checkpoint_state_changed = false,
                            plan_source = "stored_discovery_state",
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
                            intake_safe_checkpoint_chain_count = next_intake_state.safe_checkpoint_chain_count,
                            intake_finalized_checkpoint_chain_count = next_intake_state.finalized_checkpoint_chain_count,
                            "runtime watched chain plan changed after stored discovery sync"
                        );
                        log_watched_contract_summary(&next_manifest_runtime_state.watched_contract_summary);
                        log_watched_chain_plan(
                            "discovery-refresh",
                            &next_manifest_runtime_state.watched_chain_plan,
                        );
                        log_intake_chain_tasks("discovery-refresh", &next_tasks);
                        log_provider_registry("discovery-refresh", &next_tasks, provider_registry);
                        manifest_runtime_state = next_manifest_runtime_state;
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
                            plan_source = "stored_discovery_state",
                            error = ?error,
                            watched_chain_count = current_watch_state.chain_count,
                            watched_address_count = current_watch_state.address_count,
                            watched_entry_count_total = current_watch_state.entry_count,
                            intake_chain_count = current_intake_state.chain_count,
                            intake_address_count = current_intake_state.address_count,
                            intake_entry_count_total = current_intake_state.entry_count,
                            "failed to refresh runtime watch state from stored discovery edges; keeping last successful state"
                        );
                    }
                }
            }
        }
    }
}

pub(crate) fn init_tracing(service: &'static str) {
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
