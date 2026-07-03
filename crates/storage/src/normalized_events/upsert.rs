use std::{
    collections::{BTreeMap, HashMap},
    time::Instant,
};

use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Postgres};
use tracing::info;

use crate::label_preimages::upsert_label_preimages_from_normalized_events;

use super::{types::NormalizedEvent, validation::validate_normalized_event};

#[path = "upsert/batch.rs"]
mod batch;
#[path = "upsert/repair.rs"]
mod repair;
#[path = "upsert/sanitize.rs"]
mod sanitize;

use batch::{
    insert_normalized_events_do_nothing, load_normalized_events_by_identities,
    upsert_normalized_event_batch,
};
use repair::{
    normalized_event_identity_repair_allowed, repair_after_state_conflicts,
    repair_resource_id_conflicts, supersede_basenames_registry_boundary_derivation_change_events,
};
use sanitize::jsonb_safe_normalized_event;
pub use sanitize::serialize_jsonb_value;

pub(super) const NORMALIZED_EVENT_FAST_INSERT_BATCH_SIZE: usize = 10_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedEventUpsertSummary {
    pub snapshots: Vec<NormalizedEvent>,
    pub inserted_count: usize,
}

/// Insert missing normalized events or refresh canonicality for existing rows.
pub async fn upsert_normalized_events(
    pool: &PgPool,
    events: &[NormalizedEvent],
) -> Result<Vec<NormalizedEvent>> {
    Ok(upsert_normalized_events_with_summary(pool, events)
        .await?
        .snapshots)
}

/// Insert missing normalized events or refresh canonicality for existing rows.
pub async fn upsert_normalized_events_with_summary(
    pool: &PgPool,
    events: &[NormalizedEvent],
) -> Result<NormalizedEventUpsertSummary> {
    if events.is_empty() {
        return Ok(NormalizedEventUpsertSummary {
            snapshots: Vec::new(),
            inserted_count: 0,
        });
    }

    let total_started = Instant::now();
    let begin_started = Instant::now();
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for normalized-event upsert")?;
    let transaction_begin_ms = begin_started.elapsed().as_millis();

    let validate_started = Instant::now();
    let mut jsonb_safe_events = Vec::with_capacity(events.len());
    for event in events {
        validate_normalized_event(event)?;
        jsonb_safe_events.push(jsonb_safe_normalized_event(event));
    }
    let validate_and_sanitize_ms = validate_started.elapsed().as_millis();

    let mut snapshots = Vec::with_capacity(events.len());
    let mut inserted_count = 0usize;
    let mut chunk_count = 0usize;
    let mut fast_insert_ms = 0u128;
    let mut conflict_partition_ms = 0u128;
    let mut conflict_load_ms = 0u128;
    let mut conflict_snapshot_ms = 0u128;
    let mut conflict_update_ms = 0u128;
    let mut after_state_repair_ms = 0u128;
    let mut after_state_repaired_count = 0usize;
    let mut resource_id_repair_ms = 0u128;
    let mut resource_id_repaired_count = 0usize;
    let mut boundary_supersession_ms = 0u128;
    let mut boundary_superseded_count = 0usize;
    let mut label_preimage_ms = 0u128;
    let mut label_preimage_count = 0usize;
    let mut output_snapshot_ms = 0u128;
    for chunk in jsonb_safe_events.chunks(NORMALIZED_EVENT_FAST_INSERT_BATCH_SIZE) {
        chunk_count += 1;
        let conflict_load_started = Instant::now();
        let mut existing_events =
            validate_existing_normalized_events(&mut transaction, chunk).await?;
        conflict_load_ms += conflict_load_started.elapsed().as_millis();

        let conflict_partition_started = Instant::now();
        let mut missing_events = Vec::new();
        let mut conflicted_events = Vec::new();
        for event in chunk {
            if existing_events.contains_key(&event.event_identity) {
                conflicted_events.push(event.clone());
            } else {
                missing_events.push(event.clone());
            }
        }
        conflict_partition_ms += conflict_partition_started.elapsed().as_millis();

        let fast_insert_started = Instant::now();
        let inserted_identities =
            insert_normalized_events_do_nothing(&mut transaction, &missing_events).await?;
        fast_insert_ms += fast_insert_started.elapsed().as_millis();
        inserted_count += inserted_identities.len();

        let raced_conflicted_events = missing_events
            .iter()
            .filter(|event| !inserted_identities.contains(&event.event_identity))
            .cloned()
            .collect::<Vec<_>>();
        if !raced_conflicted_events.is_empty() {
            let conflict_load_started = Instant::now();
            let raced_existing =
                validate_existing_normalized_events(&mut transaction, &raced_conflicted_events)
                    .await?;
            conflict_load_ms += conflict_load_started.elapsed().as_millis();
            existing_events.extend(raced_existing);
            conflicted_events.extend(raced_conflicted_events);
        }

        let after_state_repair_started = Instant::now();
        after_state_repaired_count +=
            repair_after_state_conflicts(&mut transaction, &conflicted_events, &existing_events)
                .await?;
        after_state_repair_ms += after_state_repair_started.elapsed().as_millis();

        let resource_id_repair_started = Instant::now();
        resource_id_repaired_count +=
            repair_resource_id_conflicts(&mut transaction, &conflicted_events, &existing_events)
                .await?;
        resource_id_repair_ms += resource_id_repair_started.elapsed().as_millis();

        let events_requiring_canonicality_refresh = conflicted_events
            .iter()
            .filter(|event| {
                existing_events
                    .get(&event.event_identity)
                    .map(|existing| {
                        existing
                            .canonicality_state
                            .merge_observation(event.canonicality_state)
                            != existing.canonicality_state
                    })
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();

        let conflict_snapshot_started = Instant::now();
        let mut conflicting_snapshots =
            normalized_event_snapshots_after_upsert(&conflicted_events, &existing_events);
        let mut conflicting_snapshots_by_identity = conflicting_snapshots
            .drain(..)
            .map(|event| (event.event_identity.clone(), event))
            .collect::<HashMap<_, _>>();
        conflict_snapshot_ms += conflict_snapshot_started.elapsed().as_millis();

        let conflict_update_started = Instant::now();
        for event in &events_requiring_canonicality_refresh {
            upsert_normalized_event_batch(&mut transaction, std::slice::from_ref(event)).await?;
        }
        conflict_update_ms += conflict_update_started.elapsed().as_millis();

        let boundary_supersession_started = Instant::now();
        boundary_superseded_count +=
            supersede_basenames_registry_boundary_derivation_change_events(&mut transaction, chunk)
                .await?;
        boundary_supersession_ms += boundary_supersession_started.elapsed().as_millis();

        let label_preimage_started = Instant::now();
        let changed_labelhashes =
            upsert_label_preimages_from_normalized_events(&mut transaction, chunk).await?;
        label_preimage_count += changed_labelhashes.len();
        label_preimage_ms += label_preimage_started.elapsed().as_millis();

        let output_snapshot_started = Instant::now();
        for event in chunk {
            if inserted_identities.contains(&event.event_identity) {
                snapshots.push(event.clone());
            } else {
                snapshots.push(
                    conflicting_snapshots_by_identity
                        .remove(&event.event_identity)
                        .unwrap_or_else(|| event.clone()),
                );
            }
        }
        output_snapshot_ms += output_snapshot_started.elapsed().as_millis();
    }

    let commit_started = Instant::now();
    transaction
        .commit()
        .await
        .context("failed to commit normalized-event upsert")?;
    let commit_ms = commit_started.elapsed().as_millis();

    info!(
        service = "storage",
        operation = "upsert_normalized_events",
        normalized_event_count = events.len(),
        inserted_count,
        conflict_count = events.len().saturating_sub(inserted_count),
        chunk_count,
        event_kind_counts = ?count_normalized_events_by_event_kind(events),
        source_family_counts = ?count_normalized_events_by_source_family(events),
        transaction_begin_ms,
        validate_and_sanitize_ms,
        fast_insert_ms,
        conflict_partition_ms,
        conflict_load_ms,
        conflict_snapshot_ms,
        conflict_update_ms,
        after_state_repair_ms,
        after_state_repaired_count,
        resource_id_repair_ms,
        resource_id_repaired_count,
        boundary_supersession_ms,
        boundary_superseded_count,
        label_preimage_ms,
        label_preimage_count,
        output_snapshot_ms,
        commit_ms,
        elapsed_ms = total_started.elapsed().as_millis(),
        "normalized-event upsert timing completed"
    );

    Ok(NormalizedEventUpsertSummary {
        snapshots,
        inserted_count,
    })
}

/// Insert missing normalized events or refresh canonicality for existing rows without returning
/// event snapshots. Use this for replay paths that already own their in-memory event state and need
/// bounded memory while committing large batches.
pub async fn upsert_normalized_events_count_only(
    pool: &PgPool,
    events: &[NormalizedEvent],
) -> Result<usize> {
    if events.is_empty() {
        return Ok(0);
    }

    let total_started = Instant::now();
    let mut inserted_count = 0usize;
    let mut chunk_count = 0usize;
    let mut transaction_begin_ms = 0u128;
    let mut validate_and_sanitize_ms = 0u128;
    let mut fast_insert_ms = 0u128;
    let mut conflict_partition_ms = 0u128;
    let mut conflict_load_ms = 0u128;
    let mut conflict_update_ms = 0u128;
    let mut after_state_repair_ms = 0u128;
    let mut after_state_repaired_count = 0usize;
    let mut resource_id_repair_ms = 0u128;
    let mut resource_id_repaired_count = 0usize;
    let mut boundary_supersession_ms = 0u128;
    let mut boundary_superseded_count = 0usize;
    let mut label_preimage_ms = 0u128;
    let mut label_preimage_count = 0usize;
    let mut commit_ms = 0u128;

    for raw_chunk in events.chunks(NORMALIZED_EVENT_FAST_INSERT_BATCH_SIZE) {
        chunk_count += 1;
        let begin_started = Instant::now();
        let mut transaction = pool
            .begin()
            .await
            .context("failed to open transaction for normalized-event count-only upsert chunk")?;
        transaction_begin_ms += begin_started.elapsed().as_millis();

        let validate_started = Instant::now();
        let mut chunk = Vec::with_capacity(raw_chunk.len());
        for event in raw_chunk {
            validate_normalized_event(event)?;
            chunk.push(jsonb_safe_normalized_event(event));
        }
        validate_and_sanitize_ms += validate_started.elapsed().as_millis();

        let conflict_load_started = Instant::now();
        let mut existing_events =
            validate_existing_normalized_events(&mut transaction, &chunk).await?;
        conflict_load_ms += conflict_load_started.elapsed().as_millis();

        let conflict_partition_started = Instant::now();
        let mut missing_events = Vec::new();
        let mut conflicted_events = Vec::new();
        for event in &chunk {
            if existing_events.contains_key(&event.event_identity) {
                conflicted_events.push(event.clone());
            } else {
                missing_events.push(event.clone());
            }
        }
        conflict_partition_ms += conflict_partition_started.elapsed().as_millis();

        let fast_insert_started = Instant::now();
        let inserted_identities =
            insert_normalized_events_do_nothing(&mut transaction, &missing_events).await?;
        fast_insert_ms += fast_insert_started.elapsed().as_millis();
        inserted_count += inserted_identities.len();

        let raced_conflicted_events = missing_events
            .iter()
            .filter(|event| !inserted_identities.contains(&event.event_identity))
            .cloned()
            .collect::<Vec<_>>();
        if !raced_conflicted_events.is_empty() {
            let conflict_load_started = Instant::now();
            let raced_existing =
                validate_existing_normalized_events(&mut transaction, &raced_conflicted_events)
                    .await?;
            conflict_load_ms += conflict_load_started.elapsed().as_millis();
            existing_events.extend(raced_existing);
            conflicted_events.extend(raced_conflicted_events);
        }

        let after_state_repair_started = Instant::now();
        after_state_repaired_count +=
            repair_after_state_conflicts(&mut transaction, &conflicted_events, &existing_events)
                .await?;
        after_state_repair_ms += after_state_repair_started.elapsed().as_millis();

        let resource_id_repair_started = Instant::now();
        resource_id_repaired_count +=
            repair_resource_id_conflicts(&mut transaction, &conflicted_events, &existing_events)
                .await?;
        resource_id_repair_ms += resource_id_repair_started.elapsed().as_millis();

        let events_requiring_canonicality_refresh = conflicted_events
            .iter()
            .filter(|event| {
                existing_events
                    .get(&event.event_identity)
                    .map(|existing| {
                        existing
                            .canonicality_state
                            .merge_observation(event.canonicality_state)
                            != existing.canonicality_state
                    })
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();

        let conflict_update_started = Instant::now();
        for event in &events_requiring_canonicality_refresh {
            upsert_normalized_event_batch(&mut transaction, std::slice::from_ref(event)).await?;
        }
        conflict_update_ms += conflict_update_started.elapsed().as_millis();

        let boundary_supersession_started = Instant::now();
        boundary_superseded_count +=
            supersede_basenames_registry_boundary_derivation_change_events(
                &mut transaction,
                &chunk,
            )
            .await?;
        boundary_supersession_ms += boundary_supersession_started.elapsed().as_millis();

        let label_preimage_started = Instant::now();
        let changed_labelhashes =
            upsert_label_preimages_from_normalized_events(&mut transaction, &chunk).await?;
        label_preimage_count += changed_labelhashes.len();
        label_preimage_ms += label_preimage_started.elapsed().as_millis();

        let commit_started = Instant::now();
        transaction
            .commit()
            .await
            .context("failed to commit normalized-event count-only upsert chunk")?;
        commit_ms += commit_started.elapsed().as_millis();
    }

    info!(
        service = "storage",
        operation = "upsert_normalized_events_count_only",
        normalized_event_count = events.len(),
        inserted_count,
        conflict_count = events.len().saturating_sub(inserted_count),
        chunk_count,
        event_kind_counts = ?count_normalized_events_by_event_kind(events),
        source_family_counts = ?count_normalized_events_by_source_family(events),
        transaction_begin_ms,
        validate_and_sanitize_ms,
        fast_insert_ms,
        conflict_partition_ms,
        conflict_load_ms,
        conflict_update_ms,
        after_state_repair_ms,
        after_state_repaired_count,
        resource_id_repair_ms,
        resource_id_repaired_count,
        boundary_supersession_ms,
        boundary_superseded_count,
        label_preimage_ms,
        label_preimage_count,
        commit_ms,
        elapsed_ms = total_started.elapsed().as_millis(),
        "normalized-event count-only upsert timing completed"
    );

    Ok(inserted_count)
}

fn count_normalized_events_by_event_kind(events: &[NormalizedEvent]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        *counts.entry(event.event_kind.clone()).or_insert(0) += 1;
    }
    counts
}

fn count_normalized_events_by_source_family(events: &[NormalizedEvent]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        *counts.entry(event.source_family.clone()).or_insert(0) += 1;
    }
    counts
}

async fn validate_existing_normalized_events(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
) -> Result<HashMap<String, NormalizedEvent>> {
    let event_identities = events
        .iter()
        .map(|event| event.event_identity.clone())
        .collect::<Vec<_>>();
    let existing_events = load_normalized_events_by_identities(executor, &event_identities).await?;
    let existing_by_identity = existing_events
        .into_iter()
        .map(|event| (event.event_identity.clone(), event))
        .collect::<HashMap<_, _>>();

    for event in events {
        if let Some(existing) = existing_by_identity.get(&event.event_identity) {
            ensure_normalized_event_identity_matches(existing, event)?;
        }
    }

    Ok(existing_by_identity)
}

fn normalized_event_snapshots_after_upsert(
    events: &[NormalizedEvent],
    existing_by_identity: &HashMap<String, NormalizedEvent>,
) -> Vec<NormalizedEvent> {
    events
        .iter()
        .map(|event| {
            let mut snapshot = event.clone();
            if let Some(existing) = existing_by_identity.get(&event.event_identity) {
                snapshot.canonicality_state = existing
                    .canonicality_state
                    .merge_observation(event.canonicality_state);
            }
            snapshot
        })
        .collect()
}

fn ensure_normalized_event_identity_matches(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
) -> Result<()> {
    let differing_fields = normalized_event_identity_differences(existing, incoming);
    if !differing_fields.is_empty() {
        if normalized_event_identity_repair_allowed(existing, incoming, &differing_fields) {
            return Ok(());
        }

        bail!(
            "normalized event identity mismatch for event {} (differing_fields={}, existing={}, incoming={})",
            existing.event_identity,
            differing_fields.join(","),
            normalized_event_identity_summary(existing),
            normalized_event_identity_summary(incoming)
        );
    }

    Ok(())
}

pub(super) fn normalized_event_identity_differences(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
) -> Vec<&'static str> {
    let mut fields = Vec::new();
    if existing.namespace != incoming.namespace {
        fields.push("namespace");
    }
    if existing.logical_name_id != incoming.logical_name_id {
        fields.push("logical_name_id");
    }
    if existing.resource_id != incoming.resource_id {
        fields.push("resource_id");
    }
    if existing.event_kind != incoming.event_kind {
        fields.push("event_kind");
    }
    if existing.source_family != incoming.source_family {
        fields.push("source_family");
    }
    if existing.manifest_version != incoming.manifest_version {
        fields.push("manifest_version");
    }
    if existing.source_manifest_id != incoming.source_manifest_id {
        fields.push("source_manifest_id");
    }
    if existing.chain_id != incoming.chain_id {
        fields.push("chain_id");
    }
    if existing.block_number != incoming.block_number {
        fields.push("block_number");
    }
    if existing.block_hash != incoming.block_hash {
        fields.push("block_hash");
    }
    if existing.transaction_hash != incoming.transaction_hash {
        fields.push("transaction_hash");
    }
    if existing.log_index != incoming.log_index {
        fields.push("log_index");
    }
    if existing.raw_fact_ref != incoming.raw_fact_ref {
        fields.push("raw_fact_ref");
    }
    if existing.derivation_kind != incoming.derivation_kind {
        fields.push("derivation_kind");
    }
    if existing.before_state != incoming.before_state {
        fields.push("before_state");
    }
    if existing.after_state != incoming.after_state {
        fields.push("after_state");
    }
    fields
}

pub(super) fn normalized_event_identity_summary(event: &NormalizedEvent) -> String {
    format!(
        "namespace={:?} logical_name_id={:?} resource_id={:?} event_kind={:?} source_family={:?} manifest_version={:?} source_manifest_id={:?} chain_id={:?} block_number={:?} block_hash={:?} transaction_hash={:?} log_index={:?} raw_fact_ref={} derivation_kind={:?} before_state={} after_state={}",
        event.namespace,
        event.logical_name_id,
        event.resource_id,
        event.event_kind,
        event.source_family,
        event.manifest_version,
        event.source_manifest_id,
        event.chain_id,
        event.block_number,
        event.block_hash,
        event.transaction_hash,
        event.log_index,
        event.raw_fact_ref,
        event.derivation_kind,
        event.before_state,
        event.after_state
    )
}
