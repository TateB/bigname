//! Bootstrap adapter surface for the repo skeleton.

#[cfg(test)]
use std::sync::{Arc, OnceLock};

mod adapter_manifest;
mod block_derived_normalized_events;
mod ens_v1_reverse_claim;
mod ens_v1_subregistry_discovery;
mod ens_v1_unwrapped_authority;
mod ens_v2_common;
mod ens_v2_permissions;
mod ens_v2_registrar;
mod ens_v2_registry;
mod ens_v2_resolver;
mod evm_abi;
mod manifest_normalized_events;
mod normalized_event_support;
mod registry_migration_cache;

/// Current adapter bootstrap status.
pub const fn bootstrap_status() -> &'static str {
    "reverse-claim-source-observation-ready"
}

pub use block_derived_normalized_events::{
    BlockDerivedNormalizedEventKindSyncSummary, BlockDerivedNormalizedEventSyncSummary,
    sync_block_derived_normalized_events,
    sync_block_derived_normalized_events_with_scanned_log_count,
};
pub use ens_v1_reverse_claim::{
    EnsV1ReverseClaimKindSyncSummary, EnsV1ReverseClaimSyncSummary, sync_ens_v1_reverse_claim,
    sync_ens_v1_reverse_claim_range,
};
pub use ens_v1_subregistry_discovery::{
    EnsV1SubregistryDiscoverySyncSummary, ReplayAdapterCheckpointContext,
    sync_ens_v1_subregistry_discovery, sync_ens_v1_subregistry_discovery_with_replay_checkpoint,
    sync_ens_v1_subregistry_discovery_with_replay_checkpoint_and_log_limit,
};
pub use ens_v1_unwrapped_authority::{
    EnsV1TextRecordChange, EnsV1UnwrappedAuthoritySyncSummary, decode_ens_v1_text_record_change,
    sync_ens_v1_unwrapped_authority,
    sync_ens_v1_unwrapped_authority_with_replay_checkpoint_and_log_limit,
};
pub use ens_v2_permissions::{
    EnsV2PermissionsKindSyncSummary, EnsV2PermissionsSyncSummary, sync_ens_v2_permissions,
    sync_ens_v2_permissions_through_block,
};
pub use ens_v2_registrar::{
    EnsV2RegistrarKindSyncSummary, EnsV2RegistrarSyncSummary, sync_ens_v2_registrar,
    sync_ens_v2_registrar_through_block,
};
pub use ens_v2_registry::{
    EnsV2RegistryResourceSurfaceSyncSummary, sync_ens_v2_registry_resource_surface,
    sync_ens_v2_registry_resource_surface_through_block,
};
pub use ens_v2_resolver::{
    EnsV2ResolverKindSyncSummary, EnsV2ResolverSyncSummary, sync_ens_v2_resolver,
    sync_ens_v2_resolver_through_block,
};
pub use manifest_normalized_events::{
    ManifestNormalizedEventKindSyncSummary, ManifestNormalizedEventSyncSummary,
    sync_manifest_normalized_events,
};

pub async fn clear_replay_adapter_checkpoints(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    cursor_kind: &str,
) -> anyhow::Result<()> {
    ens_v1_subregistry_discovery::clear_replay_adapter_checkpoints(
        pool,
        deployment_profile,
        chain,
        cursor_kind,
    )
    .await?;
    ens_v1_unwrapped_authority::clear_replay_adapter_checkpoints(
        pool,
        deployment_profile,
        chain,
        cursor_kind,
    )
    .await
}

#[cfg(test)]
static TEST_DB_SEMAPHORE: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();

#[cfg(test)]
pub(crate) async fn acquire_test_db_permit() -> tokio::sync::OwnedSemaphorePermit {
    TEST_DB_SEMAPHORE
        .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(1)))
        .clone()
        .acquire_owned()
        .await
        .expect("test DB semaphore must stay open")
}
