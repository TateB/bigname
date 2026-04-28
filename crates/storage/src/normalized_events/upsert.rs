use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Postgres};

use crate::CanonicalityState;

use super::{types::NormalizedEvent, validation::validate_normalized_event};

#[path = "upsert/batch.rs"]
mod batch;
#[path = "upsert/sanitize.rs"]
mod sanitize;

use batch::{
    insert_normalized_events_do_nothing, load_normalized_events_by_identities,
    upsert_normalized_event_batch,
};
use sanitize::jsonb_safe_normalized_event;

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

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for normalized-event upsert")?;

    let mut jsonb_safe_events = Vec::with_capacity(events.len());
    for event in events {
        validate_normalized_event(event)?;
        jsonb_safe_events.push(jsonb_safe_normalized_event(event));
    }

    let mut snapshots = Vec::with_capacity(events.len());
    let mut inserted_count = 0usize;
    for chunk in jsonb_safe_events.chunks(NORMALIZED_EVENT_FAST_INSERT_BATCH_SIZE) {
        let mut inserted_identities =
            insert_normalized_events_do_nothing(&mut transaction, chunk).await?;
        let mut inserted_flags = Vec::with_capacity(chunk.len());
        let mut conflicted_events = Vec::new();
        for event in chunk {
            let inserted = inserted_identities.remove(&event.event_identity);
            inserted_flags.push(inserted);
            if !inserted {
                conflicted_events.push(event.clone());
            } else {
                inserted_count += 1;
            }
        }
        let existing_events =
            validate_existing_normalized_events(&mut transaction, &conflicted_events).await?;
        let mut conflicting_snapshots =
            normalized_event_snapshots_after_upsert(&conflicted_events, &existing_events);
        let mut conflicting_snapshots_by_identity = conflicting_snapshots
            .drain(..)
            .map(|event| (event.event_identity.clone(), event))
            .collect::<HashMap<_, _>>();
        for event in &conflicted_events {
            upsert_normalized_event_batch(&mut transaction, std::slice::from_ref(event)).await?;
        }
        for (event, inserted) in chunk.iter().zip(inserted_flags) {
            if inserted {
                snapshots.push(event.clone());
            } else {
                snapshots.push(
                    conflicting_snapshots_by_identity
                        .remove(&event.event_identity)
                        .unwrap_or_else(|| event.clone()),
                );
            }
        }
    }

    transaction
        .commit()
        .await
        .context("failed to commit normalized-event upsert")?;

    Ok(NormalizedEventUpsertSummary {
        snapshots,
        inserted_count,
    })
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
                snapshot.canonicality_state = merged_canonicality_state(
                    existing.canonicality_state,
                    event.canonicality_state,
                );
            }
            snapshot
        })
        .collect()
}

fn merged_canonicality_state(
    existing: CanonicalityState,
    incoming: CanonicalityState,
) -> CanonicalityState {
    match (existing, incoming) {
        (_, CanonicalityState::Orphaned) => CanonicalityState::Orphaned,
        (CanonicalityState::Orphaned, CanonicalityState::Observed) => CanonicalityState::Observed,
        (existing, CanonicalityState::Observed) => existing,
        (CanonicalityState::Orphaned, incoming) => incoming,
        (CanonicalityState::Finalized, _) | (_, CanonicalityState::Finalized) => {
            CanonicalityState::Finalized
        }
        (CanonicalityState::Safe, _) | (_, CanonicalityState::Safe) => CanonicalityState::Safe,
        (CanonicalityState::Canonical, _) | (_, CanonicalityState::Canonical) => {
            CanonicalityState::Canonical
        }
    }
}

fn ensure_normalized_event_identity_matches(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
) -> Result<()> {
    let differing_fields = normalized_event_identity_differences(existing, incoming);
    if !differing_fields.is_empty() {
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

fn normalized_event_identity_differences(
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

fn normalized_event_identity_summary(event: &NormalizedEvent) -> String {
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
