use std::collections::BTreeMap;

use anyhow::Result;
use bigname_storage::upsert_normalized_events_with_summary;
use sqlx::PgPool;

use super::{
    assignment::ObservedRegistryAssignment,
    event::build_registry_changed_event,
    hex_topic::ZERO_ADDRESS,
    loader::{ActiveRegistryEdge, load_registry_edges_by_observation_point},
};

const NORMALIZED_EVENT_UPSERT_CHUNK_SIZE: usize = 5_000;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct RegistryChangedEventEmitSummary {
    pub synced_count: usize,
    pub inserted_count: usize,
}

pub(super) async fn emit_registry_changed_events(
    pool: &PgPool,
    latest_assignments: &BTreeMap<String, ObservedRegistryAssignment>,
    discovery_sources: &[String],
) -> Result<RegistryChangedEventEmitSummary> {
    let discovery_sources = discovery_sources
        .iter()
        .map(String::as_str)
        .collect::<std::collections::HashSet<_>>();
    let mut events = Vec::with_capacity(NORMALIZED_EVENT_UPSERT_CHUNK_SIZE);
    let mut assignments = Vec::with_capacity(NORMALIZED_EVENT_UPSERT_CHUNK_SIZE);
    let mut summary = RegistryChangedEventEmitSummary::default();
    for assignment in latest_assignments.values() {
        if !discovery_sources.contains(assignment.discovery_source.as_str()) {
            continue;
        }
        assignments.push(assignment);
        if assignments.len() >= NORMALIZED_EVENT_UPSERT_CHUNK_SIZE {
            emit_registry_changed_event_chunk(pool, &assignments, &mut events, &mut summary)
                .await?;
            assignments.clear();
        }
    }
    emit_registry_changed_event_chunk(pool, &assignments, &mut events, &mut summary).await?;
    flush_registry_changed_events(pool, &mut events, &mut summary).await?;
    Ok(summary)
}

pub(super) async fn emit_registry_changed_event_chunk(
    pool: &PgPool,
    assignments: &[&ObservedRegistryAssignment],
    events: &mut Vec<bigname_storage::NormalizedEvent>,
    summary: &mut RegistryChangedEventEmitSummary,
) -> Result<()> {
    if assignments.is_empty() {
        return Ok(());
    }

    let requested_edges = assignments
        .iter()
        .filter(|assignment| assignment.to_address != ZERO_ADDRESS)
        .map(|assignment| registry_edge_lookup_key(assignment))
        .collect::<Vec<_>>();
    let edges_by_observation_point =
        load_registry_edges_by_observation_point(pool, &requested_edges).await?;

    for assignment in assignments {
        let edge = edge_for_assignment(&edges_by_observation_point, assignment);
        if let Some(event) = build_registry_changed_event(assignment, edge)? {
            events.push(event);
        }
        if events.len() >= NORMALIZED_EVENT_UPSERT_CHUNK_SIZE {
            flush_registry_changed_events(pool, events, summary).await?;
        }
    }

    Ok(())
}

pub(super) async fn flush_registry_changed_events(
    pool: &PgPool,
    events: &mut Vec<bigname_storage::NormalizedEvent>,
    summary: &mut RegistryChangedEventEmitSummary,
) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }

    let upsert = upsert_normalized_events_with_summary(pool, events).await?;
    summary.synced_count += events.len();
    summary.inserted_count += upsert.inserted_count;
    events.clear();
    Ok(())
}

fn edge_for_assignment<'a>(
    edges_by_observation_point: &'a std::collections::HashMap<
        (String, String, i64, String),
        ActiveRegistryEdge,
    >,
    assignment: &ObservedRegistryAssignment,
) -> Option<&'a ActiveRegistryEdge> {
    edges_by_observation_point.get(&registry_edge_lookup_key(assignment))
}

fn registry_edge_lookup_key(
    assignment: &ObservedRegistryAssignment,
) -> (String, String, i64, String) {
    (
        assignment.discovery_source.clone(),
        assignment.observation_key.clone(),
        assignment.raw_log.block_number,
        assignment.raw_log.block_hash.clone(),
    )
}
