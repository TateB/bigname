use anyhow::Result;
use tracing::info;

use crate::runtime::{
    log_block_derived_normalized_event_summary, log_ens_v1_reverse_claim_sync_summary,
    log_ens_v1_subregistry_discovery_sync_summary, log_ens_v1_unwrapped_authority_sync_summary,
    log_ens_v2_permissions_sync_summary, log_ens_v2_registrar_sync_summary,
    log_ens_v2_registry_resource_surface_sync_summary, log_ens_v2_resolver_sync_summary,
};

use super::types::PersistedRawPayloadAdapterSyncSummary;

#[path = "adapter_sync/scope.rs"]
mod scope;
use scope::{
    load_live_adapter_source_scope, source_scope_includes_ens_v1_unwrapped_authority,
    source_scope_includes_ens_v2_registrar, source_scope_includes_ens_v2_registry,
    source_scope_includes_ens_v2_resolver, source_scope_includes_reverse_claim,
};

pub(crate) async fn sync_adapter_state_from_persisted_raw_payloads(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
) -> Result<PersistedRawPayloadAdapterSyncSummary> {
    let source_scope = load_live_adapter_source_scope(pool, chain, block_hashes).await?;
    sync_adapter_state_from_persisted_raw_payloads_with_mode(
        pool,
        chain,
        block_hashes,
        Some(&source_scope),
        PersistedRawPayloadAdapterSyncMode::LiveOrBackfill,
        true,
    )
    .await
}

pub(crate) async fn sync_live_adapter_state_from_persisted_raw_payloads(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
) -> Result<PersistedRawPayloadAdapterSyncSummary> {
    let source_scope = load_live_adapter_source_scope(pool, chain, block_hashes).await?;
    sync_adapter_state_from_persisted_raw_payloads_with_mode(
        pool,
        chain,
        block_hashes,
        Some(&source_scope),
        PersistedRawPayloadAdapterSyncMode::LivePoll,
        true,
    )
    .await
}

pub(crate) async fn sync_replay_normalized_events_from_persisted_raw_payloads(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    canonical_raw_log_count: usize,
) -> Result<PersistedRawPayloadAdapterSyncSummary> {
    sync_adapter_state_from_persisted_raw_payloads_with_mode(
        pool,
        chain,
        block_hashes,
        source_scope,
        PersistedRawPayloadAdapterSyncMode::RawFactReplay {
            canonical_raw_log_count,
        },
        false,
    )
    .await
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PersistedRawPayloadAdapterSyncMode {
    LivePoll,
    LiveOrBackfill,
    RawFactReplay { canonical_raw_log_count: usize },
}

async fn sync_adapter_state_from_persisted_raw_payloads_with_mode(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    mode: PersistedRawPayloadAdapterSyncMode,
    reload_live_source_scope: bool,
) -> Result<PersistedRawPayloadAdapterSyncSummary> {
    let mut aggregate = PersistedRawPayloadAdapterSyncSummary::default();
    let mut active_source_scope = source_scope.map(<[_]>::to_vec);

    let normalized_event_summary = match mode {
        PersistedRawPayloadAdapterSyncMode::RawFactReplay {
            canonical_raw_log_count,
        } => {
            bigname_adapters::sync_block_derived_normalized_events_with_scanned_log_count(
                pool,
                chain,
                block_hashes,
                active_source_scope.as_deref(),
                canonical_raw_log_count,
            )
            .await?
        }
        PersistedRawPayloadAdapterSyncMode::LivePoll
        | PersistedRawPayloadAdapterSyncMode::LiveOrBackfill => {
            bigname_adapters::sync_block_derived_normalized_events(
                pool,
                chain,
                block_hashes,
                active_source_scope.as_deref(),
            )
            .await?
        }
    };
    log_block_derived_normalized_event_summary(chain, &normalized_event_summary);
    aggregate.add_counts(
        normalized_event_summary.scanned_log_count,
        normalized_event_summary.matched_log_count,
        normalized_event_summary.total_synced_count,
        normalized_event_summary.total_inserted_count,
    );
    let subregistry_discovery_summary = match mode {
        PersistedRawPayloadAdapterSyncMode::LivePoll
        | PersistedRawPayloadAdapterSyncMode::LiveOrBackfill => {
            if let Some(source_scope) = active_source_scope.as_deref() {
                bigname_adapters::EnsV1SubregistryDiscoverySyncSummary::sync_for_block_hashes_with_source_scope(
                    pool,
                    chain,
                    block_hashes,
                    source_scope,
                )
                .await?
            } else {
                bigname_adapters::EnsV1SubregistryDiscoverySyncSummary::sync_for_block_hashes_without_discovery_reconciliation(
                    pool,
                    chain,
                    block_hashes,
                )
                .await?
            }
        }
        PersistedRawPayloadAdapterSyncMode::RawFactReplay { .. } => {
            if let Some(source_scope) = active_source_scope.as_deref() {
                bigname_adapters::EnsV1SubregistryDiscoverySyncSummary::sync_for_block_hashes_with_source_scope_without_discovery_reconciliation(
                    pool,
                    chain,
                    block_hashes,
                    source_scope,
                )
                .await?
            } else {
                bigname_adapters::EnsV1SubregistryDiscoverySyncSummary::sync_for_block_hashes_without_discovery_reconciliation(
                    pool,
                    chain,
                    block_hashes,
                )
                .await?
            }
        }
    };
    log_ens_v1_subregistry_discovery_sync_summary(chain, &subregistry_discovery_summary);
    aggregate.add_counts(
        subregistry_discovery_summary.scanned_log_count,
        subregistry_discovery_summary.matched_log_count,
        subregistry_discovery_summary.total_normalized_event_count,
        subregistry_discovery_summary.total_normalized_event_inserted_count,
    );
    if reload_live_source_scope
        && (subregistry_discovery_summary.inserted_edge_count > 0
            || subregistry_discovery_summary.deactivated_edge_count > 0)
    {
        active_source_scope =
            Some(load_live_adapter_source_scope(pool, chain, block_hashes).await?);
    }
    let source_scope = active_source_scope.as_deref();
    if source_scope.map_or(true, source_scope_includes_reverse_claim) {
        let reverse_claim_summary = if let Some(source_scope) = source_scope {
            bigname_adapters::EnsV1ReverseClaimSyncSummary::sync_for_block_hashes_with_source_scope(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await?
        } else {
            bigname_adapters::EnsV1ReverseClaimSyncSummary::sync_for_block_hashes(
                pool,
                chain,
                block_hashes,
            )
            .await?
        };
        log_ens_v1_reverse_claim_sync_summary(chain, &reverse_claim_summary);
        aggregate.add_counts(
            reverse_claim_summary.scanned_log_count,
            reverse_claim_summary.matched_log_count,
            reverse_claim_summary.total_synced_count,
            reverse_claim_summary.total_inserted_count,
        );
    }
    if source_scope.map_or(false, |source_scope| {
        !source_scope_includes_reverse_claim(source_scope)
    }) {
        info!(
            service = "indexer",
            chain, "ENSv1 reverse-claim adapter sync skipped outside selected source scope"
        );
    }
    if source_scope.map_or(true, source_scope_includes_ens_v1_unwrapped_authority) {
        let unwrapped_authority_summary = if let Some(source_scope) = source_scope {
            bigname_adapters::EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes_with_source_scope(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await?
        } else {
            bigname_adapters::EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
                pool,
                chain,
                block_hashes,
            )
            .await?
        };
        log_ens_v1_unwrapped_authority_sync_summary(chain, &unwrapped_authority_summary);
        aggregate.add_counts(
            unwrapped_authority_summary.scanned_log_count,
            unwrapped_authority_summary.matched_log_count,
            unwrapped_authority_summary.total_normalized_event_count,
            unwrapped_authority_summary.total_normalized_event_inserted_count,
        );
    }
    if source_scope.map_or(false, |source_scope| {
        !source_scope_includes_ens_v1_unwrapped_authority(source_scope)
    }) {
        info!(
            service = "indexer",
            chain, "ENSv1 unwrapped-authority adapter sync skipped outside selected source scope"
        );
    }
    if source_scope.map_or(true, source_scope_includes_ens_v2_registry) {
        let ens_v2_registry_summary = if let Some(source_scope) = source_scope {
            bigname_adapters::EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await?
        } else {
            bigname_adapters::EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes(
                pool,
                chain,
                block_hashes,
            )
            .await?
        };
        log_ens_v2_registry_resource_surface_sync_summary(chain, &ens_v2_registry_summary);
        aggregate.add_counts(
            ens_v2_registry_summary.scanned_log_count,
            ens_v2_registry_summary.matched_log_count,
            ens_v2_registry_summary.total_normalized_event_count,
            ens_v2_registry_summary.total_normalized_event_inserted_count,
        );
        if reload_live_source_scope
            && (ens_v2_registry_summary.inserted_edge_count > 0
                || ens_v2_registry_summary.deactivated_edge_count > 0)
        {
            active_source_scope =
                Some(load_live_adapter_source_scope(pool, chain, block_hashes).await?);
        }
    }
    let source_scope = active_source_scope.as_deref();
    if source_scope.map_or(false, source_scope_includes_ens_v2_registrar) {
        let ens_v2_registrar_summary =
            bigname_adapters::EnsV2RegistrarSyncSummary::sync_for_block_hashes_with_source_scope(
                pool,
                chain,
                block_hashes,
                source_scope.expect("registrar source scope was checked before scoped sync"),
            )
            .await?;
        log_ens_v2_registrar_sync_summary(chain, &ens_v2_registrar_summary);
        aggregate.add_counts(
            ens_v2_registrar_summary.scanned_log_count,
            ens_v2_registrar_summary.matched_log_count,
            ens_v2_registrar_summary.total_synced_count,
            0,
        );
    } else if source_scope.is_none() {
        let ens_v2_registrar_summary =
            bigname_adapters::EnsV2RegistrarSyncSummary::sync_for_block_hashes(
                pool,
                chain,
                block_hashes,
            )
            .await?;
        log_ens_v2_registrar_sync_summary(chain, &ens_v2_registrar_summary);
        aggregate.add_counts(
            ens_v2_registrar_summary.scanned_log_count,
            ens_v2_registrar_summary.matched_log_count,
            ens_v2_registrar_summary.total_synced_count,
            0,
        );
    }
    if source_scope.map_or(true, source_scope_includes_ens_v2_resolver) {
        let ens_v2_resolver_summary = if let Some(source_scope) = source_scope {
            bigname_adapters::EnsV2ResolverSyncSummary::sync_for_block_hashes_with_source_scope(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await?
        } else {
            bigname_adapters::EnsV2ResolverSyncSummary::sync_for_block_hashes(
                pool,
                chain,
                block_hashes,
            )
            .await?
        };
        log_ens_v2_resolver_sync_summary(chain, &ens_v2_resolver_summary);
        aggregate.add_counts(
            ens_v2_resolver_summary.scanned_log_count,
            ens_v2_resolver_summary.matched_log_count,
            ens_v2_resolver_summary.total_synced_count,
            0,
        );
        let ens_v2_permissions_summary = if let Some(source_scope) = source_scope {
            bigname_adapters::EnsV2PermissionsSyncSummary::sync_for_block_hashes_with_source_scope(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await?
        } else {
            bigname_adapters::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
                pool,
                chain,
                block_hashes,
            )
            .await?
        };
        log_ens_v2_permissions_sync_summary(chain, &ens_v2_permissions_summary);
        aggregate.add_counts(
            ens_v2_permissions_summary.scanned_log_count,
            ens_v2_permissions_summary.matched_log_count,
            ens_v2_permissions_summary.total_synced_count,
            0,
        );
    }
    if mode == PersistedRawPayloadAdapterSyncMode::LivePoll {
        info!(
            service = "indexer",
            command = "poll",
            chain,
            block_hash_count = block_hashes.len(),
            source_scope_target_count = source_scope.map_or(0, <[_]>::len),
            scanned_log_count = aggregate.scanned_log_count,
            matched_log_count = aggregate.matched_log_count,
            normalized_event_sync_total_count = aggregate.total_synced_count,
            normalized_event_inserted_total_count = aggregate.total_inserted_count,
            "live poll adapter sync completed"
        );
    }
    Ok(aggregate)
}

pub(crate) async fn sync_adapter_state_from_scoped_persisted_raw_payloads(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: &[(String, String, i64, i64)],
) -> Result<()> {
    sync_adapter_state_from_persisted_raw_payloads_with_mode(
        pool,
        chain,
        block_hashes,
        Some(source_scope),
        PersistedRawPayloadAdapterSyncMode::LiveOrBackfill,
        false,
    )
    .await
    .map(|_| ())?;
    Ok(())
}
