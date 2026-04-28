use std::collections::{BTreeMap, HashSet};

use anyhow::Result;
use bigname_manifests::reconcile_discovery_observations;
use sqlx::PgPool;

use crate::registry_migration_cache::MigratedRegistryNodes;

mod assignment;
mod emitter;
mod event;
mod hex_topic;
mod loader;
mod migration_guard;
mod scope;

use assignment::{
    build_registry_assignment, ens_v1_resolver_discovery_source,
    ens_v1_subregistry_discovery_source,
};
use emitter::emit_registry_changed_events;
use hex_topic::{ZERO_ADDRESS, normalize_address};
use loader::{load_active_emitters, load_registry_raw_logs, stream_registry_raw_logs};
use migration_guard::{registry_migration_guard_action, rewrite_old_registry_assignment};
use scope::{
    load_active_registry_edge_observations_excluding_keys,
    load_migrated_registry_nodes_before_block, normalized_registry_source_scope_targets,
};

const ENS_V1_REGISTRY_SOURCE_FAMILY: &str = "ens_v1_registry_l1";
#[cfg(test)]
const ENS_V1_RESOLVER_SOURCE_FAMILY: &str = "ens_v1_resolver_l1";
const BASENAMES_BASE_REGISTRY_SOURCE_FAMILY: &str = "basenames_base_registry";
#[cfg(test)]
const BASENAMES_BASE_RESOLVER_SOURCE_FAMILY: &str = "basenames_base_resolver";
const SUBREGISTRY_EDGE_KIND: &str = "subregistry";
const RESOLVER_EDGE_KIND: &str = "resolver";
const CONTRACT_ROLE_REGISTRY: &str = "registry";
const CONTRACT_ROLE_REGISTRY_OLD: &str = "registry_old";
const EVENT_KIND_SUBREGISTRY_CHANGED: &str = "SubregistryChanged";
const EVENT_KIND_RESOLVER_CHANGED: &str = "ResolverChanged";
const DERIVATION_KIND_ENS_V1_SUBREGISTRY_CHANGED: &str = "ens_v1_subregistry_changed";
const DERIVATION_KIND_ENS_V1_REGISTRY_RESOLVER_CHANGED: &str = "ens_v1_registry_resolver_changed";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV1SubregistryDiscoverySyncSummary {
    pub scanned_log_count: usize,
    pub matched_log_count: usize,
    pub active_observation_count: usize,
    pub active_edge_count: usize,
    pub admitted_edge_count: usize,
    pub inserted_edge_count: usize,
    pub deactivated_edge_count: usize,
    pub total_normalized_event_count: usize,
    pub total_normalized_event_inserted_count: usize,
}

pub async fn sync_ens_v1_subregistry_discovery(
    pool: &PgPool,
    chain: &str,
) -> Result<EnsV1SubregistryDiscoverySyncSummary> {
    sync_ens_v1_subregistry_discovery_with_scope(
        pool,
        chain,
        false,
        &[],
        None,
        DiscoveryEdgeMutation::Reconcile,
    )
    .await
}

impl EnsV1SubregistryDiscoverySyncSummary {
    pub async fn sync_for_block_hashes_with_source_scope(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
    ) -> Result<Self> {
        sync_ens_v1_subregistry_discovery_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            Some(source_scope),
            DiscoveryEdgeMutation::Reconcile,
        )
        .await
    }

    pub async fn sync_for_block_hashes_with_source_scope_without_discovery_reconciliation(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
    ) -> Result<Self> {
        sync_ens_v1_subregistry_discovery_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            Some(source_scope),
            DiscoveryEdgeMutation::Skip,
        )
        .await
    }

    pub async fn sync_for_block_hashes_without_discovery_reconciliation(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v1_subregistry_discovery_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            None,
            DiscoveryEdgeMutation::Skip,
        )
        .await
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DiscoveryEdgeMutation {
    Reconcile,
    Skip,
}

async fn sync_ens_v1_subregistry_discovery_with_scope(
    pool: &PgPool,
    chain: &str,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    discovery_edge_mutation: DiscoveryEdgeMutation,
) -> Result<EnsV1SubregistryDiscoverySyncSummary> {
    let source_scope = source_scope.map(normalized_registry_source_scope_targets);
    if source_scope.as_ref().is_some_and(Vec::is_empty) {
        return Ok(EnsV1SubregistryDiscoverySyncSummary {
            scanned_log_count: 0,
            matched_log_count: 0,
            active_observation_count: 0,
            active_edge_count: 0,
            admitted_edge_count: 0,
            inserted_edge_count: 0,
            deactivated_edge_count: 0,
            total_normalized_event_count: 0,
            total_normalized_event_inserted_count: 0,
        });
    }

    let emitters = load_active_emitters(pool, chain, source_scope.as_deref()).await?;
    let discovery_sources = [
        ens_v1_subregistry_discovery_source(chain),
        ens_v1_resolver_discovery_source(chain),
    ];

    let mut scanned_log_count = 0;
    let mut matched_log_count = 0;
    let mut latest_assignments = BTreeMap::<String, assignment::ObservedRegistryAssignment>::new();
    let mut migrated_registry_nodes = MigratedRegistryNodes::empty();
    if !restrict_to_block_hashes && source_scope.is_none() {
        if !emitters.is_empty() {
            scanned_log_count = stream_registry_raw_logs(pool, chain, &emitters, |raw_log| {
                if apply_registry_raw_log(
                    &raw_log,
                    chain,
                    &emitters,
                    &mut latest_assignments,
                    &mut migrated_registry_nodes,
                )? {
                    matched_log_count += 1;
                }
                Ok(())
            })
            .await?;
        }
    } else {
        let raw_logs = load_registry_raw_logs(
            pool,
            chain,
            &emitters,
            restrict_to_block_hashes,
            block_hashes,
            source_scope.as_deref(),
        )
        .await?;
        if source_scope.is_some() && raw_logs.is_empty() {
            return Ok(EnsV1SubregistryDiscoverySyncSummary {
                scanned_log_count: 0,
                matched_log_count: 0,
                active_observation_count: 0,
                active_edge_count: 0,
                admitted_edge_count: 0,
                inserted_edge_count: 0,
                deactivated_edge_count: 0,
                total_normalized_event_count: 0,
                total_normalized_event_inserted_count: 0,
            });
        }
        scanned_log_count = raw_logs.len();
        let preload_migrated_registry_nodes = raw_logs
            .iter()
            .any(|raw_log| raw_log.contract_role.as_deref() == Some(CONTRACT_ROLE_REGISTRY_OLD));
        if preload_migrated_registry_nodes {
            let first_selected_block = raw_logs.iter().map(|raw_log| raw_log.block_number).min();
            if let Some(first_selected_block) = first_selected_block {
                migrated_registry_nodes = load_migrated_registry_nodes_before_block(
                    pool,
                    chain,
                    &emitters,
                    first_selected_block,
                )
                .await?
            }
        }
        matched_log_count += apply_registry_raw_logs(
            &raw_logs,
            chain,
            &emitters,
            &mut latest_assignments,
            &mut migrated_registry_nodes,
        )?;
    }

    let mut reconciliation = EnsV1SubregistryDiscoverySyncSummary {
        scanned_log_count,
        matched_log_count,
        active_observation_count: latest_assignments
            .values()
            .filter(|assignment| {
                normalize_address(&assignment.observation.to_address) != ZERO_ADDRESS
            })
            .count(),
        active_edge_count: 0,
        admitted_edge_count: 0,
        inserted_edge_count: 0,
        deactivated_edge_count: 0,
        total_normalized_event_count: 0,
        total_normalized_event_inserted_count: 0,
    };
    if discovery_edge_mutation == DiscoveryEdgeMutation::Reconcile {
        if source_scope.is_some() {
            let observations = latest_assignments
                .values()
                .map(|assignment| assignment.observation.clone())
                .collect::<Vec<_>>();
            let touched_observation_keys = latest_assignments
                .values()
                .map(|assignment| {
                    (
                        assignment.observation.discovery_source.clone(),
                        assignment.observation_key.clone(),
                    )
                })
                .collect::<HashSet<_>>();
            let mut carry_forward = load_active_registry_edge_observations_excluding_keys(
                pool,
                &discovery_sources,
                &touched_observation_keys,
            )
            .await?;
            carry_forward.extend(observations.clone());
            for discovery_source in &discovery_sources {
                let source_observations = carry_forward
                    .iter()
                    .filter(|observation| observation.discovery_source == discovery_source.as_str())
                    .cloned()
                    .collect::<Vec<_>>();
                let source_reconciliation =
                    reconcile_discovery_observations(pool, discovery_source, &source_observations)
                        .await?;
                reconciliation.active_edge_count += source_reconciliation.active_edge_count;
                reconciliation.admitted_edge_count += source_reconciliation.admitted_edge_count;
                reconciliation.inserted_edge_count += source_reconciliation.inserted_edge_count;
                reconciliation.deactivated_edge_count +=
                    source_reconciliation.deactivated_edge_count;
            }
        } else {
            for discovery_source in &discovery_sources {
                let source_observations = latest_assignments
                    .values()
                    .filter(|assignment| {
                        assignment.observation.discovery_source == discovery_source.as_str()
                    })
                    .map(|assignment| assignment.observation.clone())
                    .collect::<Vec<_>>();
                let source_reconciliation =
                    reconcile_discovery_observations(pool, discovery_source, &source_observations)
                        .await?;
                reconciliation.active_edge_count += source_reconciliation.active_edge_count;
                reconciliation.admitted_edge_count += source_reconciliation.admitted_edge_count;
                reconciliation.inserted_edge_count += source_reconciliation.inserted_edge_count;
                reconciliation.deactivated_edge_count +=
                    source_reconciliation.deactivated_edge_count;
            }
        }
    }

    let event_summary =
        emit_registry_changed_events(pool, &latest_assignments, &discovery_sources).await?;
    reconciliation.total_normalized_event_count = event_summary.synced_count;
    reconciliation.total_normalized_event_inserted_count = event_summary.inserted_count;

    Ok(reconciliation)
}

fn apply_registry_raw_logs(
    raw_logs: &[loader::RegistryRawLogRow],
    chain: &str,
    emitters: &[loader::ActiveEmitter],
    latest_assignments: &mut BTreeMap<String, assignment::ObservedRegistryAssignment>,
    migrated_registry_nodes: &mut MigratedRegistryNodes,
) -> Result<usize> {
    let mut matched_log_count = 0;
    for raw_log in raw_logs {
        if apply_registry_raw_log(
            raw_log,
            chain,
            emitters,
            latest_assignments,
            migrated_registry_nodes,
        )? {
            matched_log_count += 1;
        }
    }
    Ok(matched_log_count)
}

fn apply_registry_raw_log(
    raw_log: &loader::RegistryRawLogRow,
    chain: &str,
    emitters: &[loader::ActiveEmitter],
    latest_assignments: &mut BTreeMap<String, assignment::ObservedRegistryAssignment>,
    migrated_registry_nodes: &mut MigratedRegistryNodes,
) -> Result<bool> {
    let migration_guard = registry_migration_guard_action(raw_log)?;
    if migration_guard.suppressed_by(migrated_registry_nodes) {
        return Ok(false);
    }

    let Some(mut assignment) = build_registry_assignment(raw_log, chain)? else {
        if let Some(node) = migration_guard.mark_migrated_node() {
            migrated_registry_nodes.insert(node.to_owned());
        }
        return Ok(false);
    };
    rewrite_old_registry_assignment(&mut assignment, emitters, &migration_guard);
    latest_assignments.insert(
        format!(
            "{}:{}",
            assignment.observation.discovery_source, assignment.observation_key
        ),
        assignment,
    );
    if let Some(node) = migration_guard.mark_migrated_node() {
        migrated_registry_nodes.insert(node.to_owned());
    }
    Ok(true)
}

#[cfg(test)]
mod tests;
