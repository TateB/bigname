use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{Context, Result};
use bigname_manifests::DiscoveryObservation;
use bigname_storage::{
    NameSurface, NormalizedEvent, Resource, SurfaceBinding, TokenLineage, upsert_name_surfaces,
    upsert_normalized_events_with_summary, upsert_resources, upsert_token_lineages,
};
use sqlx::PgPool;
use sqlx::types::Uuid;

mod constants;
mod decode;
mod discovery;
mod emitters;
mod events;
mod identity;
mod load;
mod names;
mod normalized;
mod types;
mod util;

use crate::adapter_manifest::load_required_active_manifest_event_topic0s_by_signature;
use crate::normalized_event_support::count_events_by_kind;
use constants::*;
use decode::build_registry_observation;
use discovery::{latest_discovery_observations, reconcile_discovery_observations_by_source};
use emitters::{load_active_emitters, normalized_source_scope_targets};
use events::{RegistryObservationContext, apply_registry_observation};
use identity::{
    build_name_surface, build_resource, build_resource_events, build_surface_binding,
    build_token_lineage, upsert_surface_bindings_close_before_open,
};
use load::{RawLogCanonicalityFilter, load_registry_raw_logs};
use names::initial_registry_suffixes;
use types::*;
use util::normalize_address;

#[cfg(test)]
use crate::evm_abi::keccak_signature_hex;
#[cfg(test)]
use bigname_manifests::WatchedContractSource;
#[cfg(test)]
use bigname_storage::{CanonicalityState, upsert_surface_bindings};
#[cfg(test)]
use emitters::{preferred_emitters_by_scope, source_rank};
#[cfg(test)]
use serde_json::{Value, json};
#[cfg(test)]
use sqlx::types::time::OffsetDateTime;
#[cfg(test)]
use util::{deterministic_uuid, event_position_timestamp, hex_string, keccak256_bytes};

pub struct EnsV2RegistryResourceSurfaceSyncSummary {
    pub scanned_log_count: usize,
    pub matched_log_count: usize,
    pub total_name_surface_count: usize,
    pub total_resource_count: usize,
    pub total_surface_binding_count: usize,
    pub total_normalized_event_count: usize,
    pub total_normalized_event_inserted_count: usize,
    pub active_discovery_observation_count: usize,
    pub active_edge_count: usize,
    pub admitted_edge_count: usize,
    pub inserted_edge_count: usize,
    pub deactivated_edge_count: usize,
    pub by_kind: BTreeMap<String, usize>,
}

impl EnsV2RegistryResourceSurfaceSyncSummary {
    pub fn empty(scanned_log_count: usize) -> Self {
        Self {
            scanned_log_count,
            matched_log_count: 0,
            total_name_surface_count: 0,
            total_resource_count: 0,
            total_surface_binding_count: 0,
            total_normalized_event_count: 0,
            total_normalized_event_inserted_count: 0,
            active_discovery_observation_count: 0,
            active_edge_count: 0,
            admitted_edge_count: 0,
            inserted_edge_count: 0,
            deactivated_edge_count: 0,
            by_kind: BTreeMap::new(),
        }
    }

    pub async fn sync_for_block_hashes(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v2_registry_resource_surface_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            None,
            RawLogCanonicalityFilter::IncludeObserved,
            None,
        )
        .await
    }

    pub async fn sync_for_block_hashes_canonical_only(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v2_registry_resource_surface_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            None,
            RawLogCanonicalityFilter::CanonicalOnly,
            None,
        )
        .await
    }

    pub async fn sync_for_block_hashes_with_source_scope(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
    ) -> Result<Self> {
        sync_ens_v2_registry_resource_surface_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            Some(source_scope),
            RawLogCanonicalityFilter::IncludeObserved,
            None,
        )
        .await
    }

    pub async fn sync_for_block_hashes_with_source_scope_canonical_only(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
    ) -> Result<Self> {
        sync_ens_v2_registry_resource_surface_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            Some(source_scope),
            RawLogCanonicalityFilter::CanonicalOnly,
            None,
        )
        .await
    }
}

pub async fn sync_ens_v2_registry_resource_surface(
    pool: &PgPool,
    chain: &str,
) -> Result<EnsV2RegistryResourceSurfaceSyncSummary> {
    sync_ens_v2_registry_resource_surface_with_scope(
        pool,
        chain,
        false,
        &[],
        None,
        RawLogCanonicalityFilter::IncludeObserved,
        None,
    )
    .await
}

pub async fn sync_ens_v2_registry_resource_surface_through_block(
    pool: &PgPool,
    chain: &str,
    target_block_number: i64,
) -> Result<EnsV2RegistryResourceSurfaceSyncSummary> {
    sync_ens_v2_registry_resource_surface_with_scope(
        pool,
        chain,
        false,
        &[],
        None,
        RawLogCanonicalityFilter::CanonicalOnly,
        Some(target_block_number),
    )
    .await
}

async fn sync_ens_v2_registry_resource_surface_with_scope(
    pool: &PgPool,
    chain: &str,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    canonicality_filter: RawLogCanonicalityFilter,
    max_block_number: Option<i64>,
) -> Result<EnsV2RegistryResourceSurfaceSyncSummary> {
    let source_scope = source_scope.map(normalized_source_scope_targets);
    if source_scope.as_ref().is_some_and(Vec::is_empty) {
        return Ok(EnsV2RegistryResourceSurfaceSyncSummary::empty(0));
    }
    let scoped_emitter_identities = source_scope.as_ref().map(|source_scope| {
        source_scope
            .iter()
            .map(|target| (target.source_family.clone(), target.address.clone()))
            .collect::<HashSet<_>>()
    });

    let active_emitters =
        load_active_emitters(pool, chain, scoped_emitter_identities.as_ref()).await?;
    if active_emitters.is_empty() {
        return Ok(EnsV2RegistryResourceSurfaceSyncSummary::empty(0));
    }
    let manifest_ids = active_emitters
        .iter()
        .map(|emitter| emitter.source_manifest_id)
        .collect::<Vec<_>>();
    let event_topics = load_required_active_manifest_event_topic0s_by_signature(
        pool,
        &manifest_ids,
        &ABI_EVENT_SIGNATURES,
        "ENSv2 registry",
    )
    .await?;

    let raw_logs = load_registry_raw_logs(
        pool,
        chain,
        &active_emitters,
        restrict_to_block_hashes,
        block_hashes,
        source_scope.as_deref(),
        canonicality_filter,
        max_block_number,
    )
    .await?;
    let scanned_log_count = raw_logs.len();
    if raw_logs.is_empty() {
        return Ok(EnsV2RegistryResourceSurfaceSyncSummary::empty(
            scanned_log_count,
        ));
    }

    let mut matched_log_count = 0usize;
    let mut registry_suffix_by_address = initial_registry_suffixes(&active_emitters);
    let mut registry_contract_by_address = active_emitters
        .iter()
        .map(|emitter| (emitter.address.clone(), emitter.contract_instance_id))
        .collect::<HashMap<_, _>>();
    let mut states_by_registry_token = BTreeMap::<(String, String), RegistryNameState>::new();
    let mut linked_resource_states = BTreeMap::<Uuid, RegistryNameState>::new();
    let mut closed_bindings = BTreeMap::<Uuid, SurfaceBinding>::new();
    let mut token_aliases = HashMap::<(String, String), (String, String)>::new();
    let mut observations = Vec::<DiscoveryObservation>::new();
    let mut graph_events = Vec::<NormalizedEvent>::new();

    for raw_log in &raw_logs {
        let Some(observation) = build_registry_observation(raw_log, &event_topics)? else {
            continue;
        };
        matched_log_count += 1;
        let mut context = RegistryObservationContext {
            registry_suffix_by_address: &mut registry_suffix_by_address,
            registry_contract_by_address: &mut registry_contract_by_address,
            states_by_registry_token: &mut states_by_registry_token,
            linked_resource_states: &mut linked_resource_states,
            closed_bindings: &mut closed_bindings,
            token_aliases: &mut token_aliases,
            observations: &mut observations,
            graph_events: &mut graph_events,
        };
        apply_registry_observation(observation, &mut context)?;
    }

    let latest_observations = latest_discovery_observations(observations)?;
    let reconciliation = reconcile_discovery_observations_by_source(pool, &latest_observations)
        .await
        .with_context(|| format!("failed to reconcile ENSv2 discovery observations for {chain}"))?;

    let mut token_lineages = Vec::<TokenLineage>::new();
    let mut resources = Vec::<Resource>::new();
    let mut surfaces = Vec::<NameSurface>::new();
    let mut bindings = Vec::<SurfaceBinding>::new();
    let mut events = graph_events;

    for state in linked_resource_states.values() {
        let Some(link) = state.resource.as_ref() else {
            continue;
        };
        token_lineages.push(build_token_lineage(pool, state, link).await?);
        resources.push(build_resource(pool, state, link).await?);
        surfaces.push(build_name_surface(pool, &state.name, &state.first_ref).await?);
        if let Some(closed_binding) = closed_bindings.get(&link.surface_binding_id) {
            bindings.push(closed_binding.clone());
        } else {
            bindings.push(build_surface_binding(pool, state, link).await?);
        }
        events.extend(build_resource_events(state, link));
    }

    let by_kind = count_events_by_kind(&events);
    upsert_token_lineages(pool, &token_lineages).await?;
    upsert_resources(pool, &resources).await?;
    upsert_name_surfaces(pool, &surfaces).await?;
    upsert_surface_bindings_close_before_open(pool, &bindings).await?;
    let normalized_event_upsert = upsert_normalized_events_with_summary(pool, &events).await?;

    Ok(EnsV2RegistryResourceSurfaceSyncSummary {
        scanned_log_count,
        matched_log_count,
        total_name_surface_count: surfaces.len(),
        total_resource_count: resources.len(),
        total_surface_binding_count: bindings.len(),
        total_normalized_event_count: events.len(),
        total_normalized_event_inserted_count: normalized_event_upsert.inserted_count,
        active_discovery_observation_count: latest_observations
            .iter()
            .filter(|observation| normalize_address(&observation.to_address) != ZERO_ADDRESS)
            .count(),
        active_edge_count: reconciliation.active_edge_count,
        admitted_edge_count: reconciliation.admitted_edge_count,
        inserted_edge_count: reconciliation.inserted_edge_count,
        deactivated_edge_count: reconciliation.deactivated_edge_count,
        by_kind,
    })
}

#[cfg(test)]
mod tests;
