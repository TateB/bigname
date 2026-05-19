use bigname_adapters::{
    BlockDerivedNormalizedEventSyncSummary, EnsV1ReverseClaimSyncSummary,
    EnsV1SubregistryDiscoverySyncSummary, EnsV1UnwrappedAuthoritySyncSummary,
    EnsV2PermissionsSyncSummary, EnsV2RegistrarSyncSummary,
    EnsV2RegistryResourceSurfaceSyncSummary, EnsV2ResolverSyncSummary,
    ManifestNormalizedEventSyncSummary,
};
use bigname_manifests::{
    ManifestLoadStatus, ManifestLoadSummary, ManifestSyncStatus, ManifestSyncSummary,
    WatchedContractSummary,
};
use tracing::{info, warn};

use super::manifest::{DiscoveryAdmissionSnapshot, ManifestRuntimeState};

#[path = "logging/runtime_plan.rs"]
mod runtime_plan;

pub(crate) use runtime_plan::{
    log_intake_chain_tasks, log_provider_registry, log_watched_chain_plan,
};

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
        && summary.total_normalized_event_inserted_count == 0
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
        identity_normalized_event_inserted_count = summary.total_normalized_event_inserted_count,
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
        normalized_event_sync_total_count = summary.total_normalized_event_count,
        normalized_event_inserted_total_count = summary.total_normalized_event_inserted_count,
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
        && summary.total_normalized_event_inserted_count == 0
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
        identity_normalized_event_inserted_count = summary.total_normalized_event_inserted_count,
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
