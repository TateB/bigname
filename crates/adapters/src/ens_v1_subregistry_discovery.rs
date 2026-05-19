use std::collections::BTreeMap;

use anyhow::Result;
use bigname_manifests::{
    reconcile_discovery_observations, reconcile_scoped_discovery_observations,
};
use sqlx::PgPool;

use crate::registry_migration_cache::MigratedRegistryNodes;

mod assignment;
mod checkpoint;
mod emitter;
mod event;
mod hex_topic;
mod loader;
mod migration_guard;
mod scope;

use assignment::{
    ObservedRegistryAssignment, build_registry_assignment, ens_v1_resolver_discovery_source,
    ens_v1_subregistry_discovery_source,
};
use checkpoint::SubregistryReplayCheckpoint;
use emitter::{
    RegistryChangedEventEmitSummary, emit_registry_changed_event_chunk,
    emit_registry_changed_events, flush_registry_changed_events,
};
use hex_topic::{ZERO_ADDRESS, normalize_address};
use loader::{
    load_active_emitters, load_registry_raw_log_checkpoint_page, load_registry_raw_logs,
    stream_registry_raw_logs,
};
use migration_guard::{
    current_registry_emitter, registry_migration_guard_action, rewrite_old_registry_assignment,
};
use scope::{load_migrated_registry_nodes_before_block, normalized_registry_source_scope_targets};

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
const SUBREGISTRY_CHECKPOINT_EVENT_PAGE_LIMIT: i64 = 20_000;

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

pub use checkpoint::{ReplayAdapterCheckpointContext, clear_replay_adapter_checkpoints};

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
        None,
    )
    .await
}

pub async fn sync_ens_v1_subregistry_discovery_with_replay_checkpoint(
    pool: &PgPool,
    chain: &str,
    checkpoint: &ReplayAdapterCheckpointContext,
) -> Result<EnsV1SubregistryDiscoverySyncSummary> {
    sync_ens_v1_subregistry_discovery_with_scope(
        pool,
        chain,
        false,
        &[],
        None,
        DiscoveryEdgeMutation::Reconcile,
        Some(checkpoint),
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
            None,
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
            None,
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
            None,
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
    replay_checkpoint: Option<&ReplayAdapterCheckpointContext>,
) -> Result<EnsV1SubregistryDiscoverySyncSummary> {
    let source_scope = source_scope.map(normalized_registry_source_scope_targets);
    let use_replay_checkpoint = !restrict_to_block_hashes
        && source_scope.is_none()
        && discovery_edge_mutation == DiscoveryEdgeMutation::Reconcile
        && replay_checkpoint.is_some();
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
    let current_registry = current_registry_emitter(&emitters).cloned();
    let discovery_sources = [
        ens_v1_subregistry_discovery_source(chain),
        ens_v1_resolver_discovery_source(chain),
    ];

    let mut scanned_log_count = 0;
    let mut matched_log_count = 0;
    let mut latest_assignments = BTreeMap::<String, assignment::ObservedRegistryAssignment>::new();
    let mut migrated_registry_nodes = MigratedRegistryNodes::empty();
    let mut active_checkpoint = if use_replay_checkpoint {
        let checkpoint = replay_checkpoint.expect("checkpoint presence was checked");
        let active_checkpoint =
            SubregistryReplayCheckpoint::load_or_start(pool, chain, checkpoint).await?;
        if let Some(summary) = active_checkpoint.completed_summary()? {
            return Ok(summary);
        }
        if !active_checkpoint.stream_complete() {
            let staged = active_checkpoint.load_staged_state(pool).await?;
            latest_assignments = staged.latest_assignments;
            migrated_registry_nodes = staged.migrated_registry_nodes;
        }
        scanned_log_count = active_checkpoint.scanned_log_count();
        matched_log_count = active_checkpoint.matched_log_count();
        Some(active_checkpoint)
    } else {
        None
    };
    if !restrict_to_block_hashes && source_scope.is_none() {
        if !emitters.is_empty() {
            if let Some(checkpoint) = active_checkpoint.as_mut() {
                let (scanned, matched) = sync_checkpointed_registry_raw_logs(
                    pool,
                    chain,
                    &emitters,
                    current_registry.as_ref(),
                    checkpoint,
                    &mut latest_assignments,
                    &mut migrated_registry_nodes,
                )
                .await?;
                scanned_log_count = scanned;
                matched_log_count = matched;
            } else {
                scanned_log_count = stream_registry_raw_logs(pool, chain, &emitters, |raw_log| {
                    let applied = apply_registry_raw_log(
                        &raw_log,
                        chain,
                        current_registry.as_ref(),
                        &mut latest_assignments,
                        &mut migrated_registry_nodes,
                    )?;
                    if applied.matched {
                        matched_log_count += 1;
                    }
                    Ok(())
                })
                .await?;
            }
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
            current_registry.as_ref(),
            &mut latest_assignments,
            &mut migrated_registry_nodes,
        )?;
    }

    let finalize_from_checkpoint = active_checkpoint
        .as_ref()
        .is_some_and(SubregistryReplayCheckpoint::stream_complete);
    if finalize_from_checkpoint {
        drop(std::mem::take(&mut latest_assignments));
        drop(std::mem::replace(
            &mut migrated_registry_nodes,
            MigratedRegistryNodes::empty(),
        ));
    }

    let active_observation_count = if finalize_from_checkpoint {
        active_checkpoint
            .as_ref()
            .expect("finalizing checkpoint should be present")
            .active_assignment_count(pool, &discovery_sources)
            .await?
    } else {
        latest_assignments
            .values()
            .filter(|assignment| normalize_address(&assignment.to_address) != ZERO_ADDRESS)
            .count()
    };

    let mut reconciliation = EnsV1SubregistryDiscoverySyncSummary {
        scanned_log_count,
        matched_log_count,
        active_observation_count,
        active_edge_count: 0,
        admitted_edge_count: 0,
        inserted_edge_count: 0,
        deactivated_edge_count: 0,
        total_normalized_event_count: 0,
        total_normalized_event_inserted_count: 0,
    };
    if discovery_edge_mutation == DiscoveryEdgeMutation::Reconcile {
        if finalize_from_checkpoint {
            reconcile_subregistry_discovery_from_checkpoint(
                pool,
                active_checkpoint
                    .as_ref()
                    .expect("finalizing checkpoint should be present"),
                &discovery_sources,
                &mut reconciliation,
            )
            .await?;
        } else if source_scope.is_some() {
            let observations = latest_assignments
                .values()
                .map(ObservedRegistryAssignment::discovery_observation)
                .collect::<Result<Vec<_>>>()?;
            for discovery_source in &discovery_sources {
                let source_observations = observations
                    .iter()
                    .filter(|observation| observation.discovery_source == discovery_source.as_str())
                    .cloned()
                    .collect::<Vec<_>>();
                let source_reconciliation = reconcile_scoped_discovery_observations(
                    pool,
                    discovery_source,
                    &source_observations,
                )
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
                    .filter(|assignment| assignment.discovery_source == discovery_source.as_str())
                    .map(ObservedRegistryAssignment::discovery_observation)
                    .collect::<Result<Vec<_>>>()?;
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

    let event_summary = if finalize_from_checkpoint {
        emit_registry_changed_events_from_checkpoint(
            pool,
            active_checkpoint
                .as_ref()
                .expect("finalizing checkpoint should be present"),
            &discovery_sources,
        )
        .await?
    } else {
        emit_registry_changed_events(pool, &latest_assignments, &discovery_sources).await?
    };
    reconciliation.total_normalized_event_count = event_summary.synced_count;
    reconciliation.total_normalized_event_inserted_count = event_summary.inserted_count;

    if let Some(checkpoint) = active_checkpoint.as_mut() {
        checkpoint.mark_completed(pool, &reconciliation).await?;
    }

    Ok(reconciliation)
}

async fn reconcile_subregistry_discovery_from_checkpoint(
    pool: &PgPool,
    checkpoint: &SubregistryReplayCheckpoint,
    discovery_sources: &[String],
    reconciliation: &mut EnsV1SubregistryDiscoverySyncSummary,
) -> Result<()> {
    for discovery_source in discovery_sources {
        let source_observations = checkpoint
            .load_discovery_observations(pool, discovery_source)
            .await?;
        let source_reconciliation =
            reconcile_discovery_observations(pool, discovery_source, &source_observations).await?;
        reconciliation.active_edge_count += source_reconciliation.active_edge_count;
        reconciliation.admitted_edge_count += source_reconciliation.admitted_edge_count;
        reconciliation.inserted_edge_count += source_reconciliation.inserted_edge_count;
        reconciliation.deactivated_edge_count += source_reconciliation.deactivated_edge_count;
    }
    Ok(())
}

async fn emit_registry_changed_events_from_checkpoint(
    pool: &PgPool,
    checkpoint: &SubregistryReplayCheckpoint,
    discovery_sources: &[String],
) -> Result<RegistryChangedEventEmitSummary> {
    let mut events = Vec::with_capacity(usize::try_from(SUBREGISTRY_CHECKPOINT_EVENT_PAGE_LIMIT)?);
    let mut summary = RegistryChangedEventEmitSummary::default();
    for discovery_source in discovery_sources {
        let mut after_key = None::<String>;
        loop {
            let page = checkpoint
                .load_assignment_page(
                    pool,
                    discovery_source,
                    after_key.as_deref(),
                    SUBREGISTRY_CHECKPOINT_EVENT_PAGE_LIMIT,
                )
                .await?;
            let Some((last_key, _)) = page.last() else {
                break;
            };
            after_key = Some(last_key.clone());
            let assignments = page
                .iter()
                .map(|(_, assignment)| assignment)
                .collect::<Vec<_>>();
            emit_registry_changed_event_chunk(pool, &assignments, &mut events, &mut summary)
                .await?;
        }
    }
    flush_registry_changed_events(pool, &mut events, &mut summary).await?;
    Ok(summary)
}

const SUBREGISTRY_CHECKPOINT_PAGE_LIMIT: i64 = 10_000;

async fn sync_checkpointed_registry_raw_logs(
    pool: &PgPool,
    chain: &str,
    emitters: &[loader::ActiveEmitter],
    current_registry: Option<&loader::ActiveEmitter>,
    checkpoint: &mut SubregistryReplayCheckpoint,
    latest_assignments: &mut BTreeMap<String, assignment::ObservedRegistryAssignment>,
    migrated_registry_nodes: &mut MigratedRegistryNodes,
) -> Result<(usize, usize)> {
    if checkpoint.stream_complete() {
        return Ok((
            checkpoint.scanned_log_count(),
            checkpoint.matched_log_count(),
        ));
    }

    let mut start_after = checkpoint.last_position();
    let mut scanned_log_count = checkpoint.scanned_log_count();
    let mut matched_log_count = checkpoint.matched_log_count();
    loop {
        let page = load_registry_raw_log_checkpoint_page(
            pool,
            chain,
            emitters,
            checkpoint.range_start_block_number(),
            checkpoint.target_block_number(),
            start_after.as_ref(),
            SUBREGISTRY_CHECKPOINT_PAGE_LIMIT,
        )
        .await?;
        let Some(last_position) = page.last_position else {
            checkpoint
                .mark_stream_complete(pool, scanned_log_count, matched_log_count)
                .await?;
            break;
        };

        let mut changed_assignment_keys = Vec::<String>::new();
        let mut migrated_nodes = Vec::<String>::new();
        for raw_log in &page.raw_logs {
            let applied = apply_registry_raw_log(
                raw_log,
                chain,
                current_registry,
                latest_assignments,
                migrated_registry_nodes,
            )?;
            scanned_log_count += 1;
            if applied.matched {
                matched_log_count += 1;
            }
            if let Some(assignment_key) = applied.assignment_key {
                changed_assignment_keys.push(assignment_key);
            }
            if let Some(migrated_node) = applied.migrated_node {
                migrated_nodes.push(migrated_node);
            }
        }

        checkpoint
            .save_progress(
                pool,
                &last_position,
                scanned_log_count,
                matched_log_count,
                latest_assignments,
                &changed_assignment_keys,
                &migrated_nodes,
                migrated_registry_nodes.delta_nodes().count(),
            )
            .await?;
        start_after = Some(last_position);
    }

    Ok((scanned_log_count, matched_log_count))
}

fn apply_registry_raw_logs(
    raw_logs: &[loader::RegistryRawLogRow],
    chain: &str,
    current_registry: Option<&loader::ActiveEmitter>,
    latest_assignments: &mut BTreeMap<String, assignment::ObservedRegistryAssignment>,
    migrated_registry_nodes: &mut MigratedRegistryNodes,
) -> Result<usize> {
    let mut matched_log_count = 0;
    for raw_log in raw_logs {
        if apply_registry_raw_log(
            raw_log,
            chain,
            current_registry,
            latest_assignments,
            migrated_registry_nodes,
        )?
        .matched
        {
            matched_log_count += 1;
        }
    }
    Ok(matched_log_count)
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct AppliedRegistryRawLog {
    matched: bool,
    assignment_key: Option<String>,
    migrated_node: Option<String>,
}

fn apply_registry_raw_log(
    raw_log: &loader::RegistryRawLogRow,
    chain: &str,
    current_registry: Option<&loader::ActiveEmitter>,
    latest_assignments: &mut BTreeMap<String, assignment::ObservedRegistryAssignment>,
    migrated_registry_nodes: &mut MigratedRegistryNodes,
) -> Result<AppliedRegistryRawLog> {
    let migration_guard = registry_migration_guard_action(raw_log)?;
    if migration_guard.suppressed_by(migrated_registry_nodes) {
        return Ok(AppliedRegistryRawLog::default());
    }

    let Some(mut assignment) = build_registry_assignment(raw_log, chain)? else {
        let migrated_node = migration_guard.mark_migrated_node().and_then(|node| {
            migrated_registry_nodes
                .insert(node.to_owned())
                .then(|| node.to_owned())
        });
        return Ok(AppliedRegistryRawLog {
            migrated_node,
            ..AppliedRegistryRawLog::default()
        });
    };
    rewrite_old_registry_assignment(&mut assignment, current_registry, &migration_guard);
    let assignment_key = format!(
        "{}:{}",
        assignment.discovery_source, assignment.observation_key
    );
    latest_assignments.insert(assignment_key.clone(), assignment);
    let migrated_node = migration_guard.mark_migrated_node().and_then(|node| {
        migrated_registry_nodes
            .insert(node.to_owned())
            .then(|| node.to_owned())
    });
    Ok(AppliedRegistryRawLog {
        matched: true,
        assignment_key: Some(assignment_key),
        migrated_node,
    })
}

#[cfg(test)]
mod tests;
