use std::path::Path;

use anyhow::{Context, Result, bail};
use bigname_adapters::{
    ManifestNormalizedEventKindSyncSummary, ManifestNormalizedEventSyncSummary,
};
use bigname_manifests::{
    DiscoveryAdmissionState, ManifestLoadStatus, ManifestLoadSummary, ManifestRepository,
    ManifestSyncStatus, ManifestSyncSummary, WatchedChainPlan, WatchedContractSummary,
    load_manifest_declared_watched_chain_plan, load_manifest_declared_watched_contract_summary,
    load_watched_chain_plan, load_watched_contract_summary,
};

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
    pub(crate) manifest_repository: ManifestRepository,
    pub(crate) manifest_summary: ManifestLoadSummary,
    pub(crate) sync_summary: ManifestSyncSummary,
    pub(crate) discovery_admission: DiscoveryAdmissionSnapshot,
    pub(crate) manifest_normalized_event_summary: ManifestNormalizedEventSyncSummary,
    pub(crate) watched_contract_summary: WatchedContractSummary,
    pub(crate) watched_chain_plan: Vec<WatchedChainPlan>,
}

impl ManifestRuntimeState {
    pub(crate) fn repository_refresh_needed(
        &self,
        manifest_repository: &ManifestRepository,
    ) -> bool {
        manifest_repository != &self.manifest_repository
            || self.sync_summary.status == ManifestSyncStatus::SkippedPendingBaseRederiveReplay
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeWatchScope {
    ActiveWatchedChain,
    ManifestDeclaredOnly,
}

impl RuntimeWatchScope {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ActiveWatchedChain => "active_watched_chain",
            Self::ManifestDeclaredOnly => "manifest_declared_only",
        }
    }
}

pub(crate) async fn build_manifest_runtime_state(
    pool: &sqlx::PgPool,
    manifest_repository: &ManifestRepository,
) -> Result<ManifestRuntimeState> {
    build_manifest_runtime_state_with_watch_scope(
        pool,
        manifest_repository,
        RuntimeWatchScope::ActiveWatchedChain,
    )
    .await
}

pub(crate) async fn build_manifest_runtime_state_with_watch_scope(
    pool: &sqlx::PgPool,
    manifest_repository: &ManifestRepository,
    watch_scope: RuntimeWatchScope,
) -> Result<ManifestRuntimeState> {
    let manifest_summary = manifest_repository.summary().clone();
    let (
        sync_summary,
        discovery_admission,
        manifest_normalized_event_summary,
        watched_contract_summary,
        watched_chain_plan,
    ) = match watch_scope {
        RuntimeWatchScope::ActiveWatchedChain => {
            let sync_summary =
                sync_repository_or_load_stored_for_pending_rederive(pool, manifest_repository)
                    .await?;
            let admission_state = bigname_manifests::load_discovery_admission_state(pool).await?;
            verify_stored_manifest_state(&sync_summary, &admission_state)?;
            (
                sync_summary,
                discovery_admission_snapshot(&admission_state),
                bigname_adapters::sync_manifest_normalized_events(pool).await?,
                load_watched_contract_summary(pool).await?,
                load_watched_chain_plan(pool).await?,
            )
        }
        RuntimeWatchScope::ManifestDeclaredOnly => {
            let sync_summary =
                sync_repository_or_load_stored_for_pending_rederive(pool, manifest_repository)
                    .await?;
            let stored_active_manifest_count = load_stored_active_manifest_count(pool).await?;
            verify_stored_manifest_count(&sync_summary, stored_active_manifest_count)?;
            (
                sync_summary.clone(),
                stored_only_discovery_admission_snapshot(&sync_summary),
                empty_manifest_normalized_event_summary(),
                load_manifest_declared_watched_contract_summary(pool).await?,
                load_manifest_declared_watched_chain_plan(pool).await?,
            )
        }
    };

    Ok(ManifestRuntimeState {
        manifest_repository: manifest_repository.clone(),
        manifest_summary,
        sync_summary,
        discovery_admission,
        manifest_normalized_event_summary,
        watched_contract_summary,
        watched_chain_plan,
    })
}

async fn sync_repository_or_load_stored_for_pending_rederive(
    pool: &sqlx::PgPool,
    manifest_repository: &ManifestRepository,
) -> Result<ManifestSyncSummary> {
    if bigname_storage::base_normalized_rederive_manifest_sync_pending_replay(pool).await? {
        return load_stored_manifest_sync_summary_for_pending_rederive(pool).await;
    }

    bigname_manifests::sync_repository(pool, manifest_repository).await
}

async fn load_stored_manifest_sync_summary_for_pending_rederive(
    pool: &sqlx::PgPool,
) -> Result<ManifestSyncSummary> {
    let (active_manifest_count, root_count, contract_count, capability_count, discovery_rule_count) =
        sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
            r#"
        SELECT
            (SELECT COUNT(*)::BIGINT
             FROM manifest_versions
             WHERE rollout_status = 'active') AS active_manifest_count,
            (SELECT COUNT(*)::BIGINT
             FROM manifest_contract_instances
             WHERE declaration_kind = 'root') AS root_count,
            (SELECT COUNT(*)::BIGINT
             FROM manifest_contract_instances
             WHERE declaration_kind = 'contract') AS contract_count,
            (SELECT COUNT(*)::BIGINT
             FROM manifest_capability_flags) AS capability_count,
            (SELECT COUNT(*)::BIGINT
             FROM manifest_discovery_rules) AS discovery_rule_count
        "#,
        )
        .fetch_one(pool)
        .await
        .context(
            "failed to load stored manifest sync summary while Base rederive replay is pending",
        )?;

    Ok(ManifestSyncSummary {
        status: ManifestSyncStatus::SkippedPendingBaseRederiveReplay,
        synced_manifest_count: 0,
        active_manifest_count: usize::try_from(active_manifest_count)
            .context("stored active manifest count cannot fit in usize")?,
        root_count: usize::try_from(root_count).context("stored root count cannot fit in usize")?,
        contract_count: usize::try_from(contract_count)
            .context("stored contract count cannot fit in usize")?,
        capability_count: usize::try_from(capability_count)
            .context("stored capability count cannot fit in usize")?,
        discovery_rule_count: usize::try_from(discovery_rule_count)
            .context("stored discovery-rule count cannot fit in usize")?,
        removed_manifest_count: 0,
        cleared_discovery_edge_count: 0,
    })
}

async fn load_stored_active_manifest_count(pool: &sqlx::PgPool) -> Result<usize> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT FROM manifest_versions WHERE rollout_status = 'active'",
    )
    .fetch_one(pool)
    .await
    .context("failed to count stored active manifest versions")?;

    Ok(count as usize)
}

fn stored_only_discovery_admission_snapshot(
    sync_summary: &ManifestSyncSummary,
) -> DiscoveryAdmissionSnapshot {
    DiscoveryAdmissionSnapshot {
        active_manifest_count: sync_summary.active_manifest_count,
        active_root_count: sync_summary.root_count,
        active_contract_count: sync_summary.contract_count,
        active_rule_count: sync_summary.discovery_rule_count,
    }
}

fn empty_manifest_normalized_event_summary() -> ManifestNormalizedEventSyncSummary {
    ManifestNormalizedEventSyncSummary {
        total_synced_count: 0,
        total_inserted_count: 0,
        by_kind: std::collections::BTreeMap::<String, ManifestNormalizedEventKindSyncSummary>::new(
        ),
    }
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
    verify_stored_manifest_count(sync_summary, admission_state.active_manifest_count)
}

fn verify_stored_manifest_count(
    sync_summary: &ManifestSyncSummary,
    stored_active_manifest_count: usize,
) -> Result<()> {
    if matches!(
        sync_summary.status,
        ManifestSyncStatus::Synced | ManifestSyncStatus::SkippedPendingBaseRederiveReplay
    ) && sync_summary.active_manifest_count != stored_active_manifest_count
    {
        bail!(
            "stored active manifest count {} does not match the synced active manifest count {}",
            stored_active_manifest_count,
            sync_summary.active_manifest_count
        );
    }

    Ok(())
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
