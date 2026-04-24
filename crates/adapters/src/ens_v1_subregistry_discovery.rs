use std::collections::BTreeMap;

use anyhow::Result;
use bigname_manifests::reconcile_discovery_observations;
use sqlx::PgPool;

mod assignment;
mod emitter;
mod event;
mod hex_topic;
mod loader;

use assignment::{
    build_registry_assignment, ens_v1_resolver_discovery_source,
    ens_v1_subregistry_discovery_source,
};
use emitter::emit_registry_changed_events;
use hex_topic::{ZERO_ADDRESS, normalize_address};
use loader::{load_active_emitters, load_registry_raw_logs};

const ENS_V1_REGISTRY_SOURCE_FAMILY: &str = "ens_v1_registry_l1";
#[cfg(test)]
const ENS_V1_RESOLVER_SOURCE_FAMILY: &str = "ens_v1_resolver_l1";
const BASENAMES_BASE_REGISTRY_SOURCE_FAMILY: &str = "basenames_base_registry";
#[cfg(test)]
const BASENAMES_BASE_RESOLVER_SOURCE_FAMILY: &str = "basenames_base_resolver";
const SUBREGISTRY_EDGE_KIND: &str = "subregistry";
const RESOLVER_EDGE_KIND: &str = "resolver";
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
}

pub async fn sync_ens_v1_subregistry_discovery(
    pool: &PgPool,
    chain: &str,
) -> Result<EnsV1SubregistryDiscoverySyncSummary> {
    let emitters = load_active_emitters(pool, chain).await?;
    let raw_logs = load_registry_raw_logs(pool, chain, &emitters).await?;
    let discovery_sources = [
        ens_v1_subregistry_discovery_source(chain),
        ens_v1_resolver_discovery_source(chain),
    ];

    let mut matched_log_count = 0;
    let mut latest_assignments = BTreeMap::<String, assignment::ObservedRegistryAssignment>::new();
    for raw_log in &raw_logs {
        let Some(assignment) = build_registry_assignment(raw_log, chain)? else {
            continue;
        };
        matched_log_count += 1;
        latest_assignments.insert(
            format!(
                "{}:{}",
                assignment.observation.discovery_source, assignment.observation_key
            ),
            assignment,
        );
    }

    let observations = latest_assignments
        .values()
        .map(|assignment| assignment.observation.clone())
        .collect::<Vec<_>>();
    let mut reconciliation = EnsV1SubregistryDiscoverySyncSummary {
        scanned_log_count: raw_logs.len(),
        matched_log_count,
        active_observation_count: observations
            .iter()
            .filter(|observation| normalize_address(&observation.to_address) != ZERO_ADDRESS)
            .count(),
        active_edge_count: 0,
        admitted_edge_count: 0,
        inserted_edge_count: 0,
        deactivated_edge_count: 0,
    };
    for discovery_source in &discovery_sources {
        let source_observations = observations
            .iter()
            .filter(|observation| observation.discovery_source == discovery_source.as_str())
            .cloned()
            .collect::<Vec<_>>();
        let source_reconciliation =
            reconcile_discovery_observations(pool, discovery_source, &source_observations).await?;
        reconciliation.active_edge_count += source_reconciliation.active_edge_count;
        reconciliation.admitted_edge_count += source_reconciliation.admitted_edge_count;
        reconciliation.inserted_edge_count += source_reconciliation.inserted_edge_count;
        reconciliation.deactivated_edge_count += source_reconciliation.deactivated_edge_count;
    }

    emit_registry_changed_events(pool, &latest_assignments, &discovery_sources).await?;

    Ok(reconciliation)
}

#[cfg(test)]
mod tests;
