use std::collections::{BTreeMap, HashSet};

use anyhow::Result;
use bigname_storage::{NormalizedEvent, upsert_normalized_events};
use sqlx::PgPool;

mod constants;
mod decoding;
mod event_builders;
mod loading;
mod persistence;
mod preimage_observation;
mod source_selection;
mod types;

use event_builders::build_preimage_observed_events;
use loading::{load_scanned_log_count, load_watched_raw_logs};
use persistence::{
    count_events_by_kind, count_inserted_events_by_kind, load_existing_event_identities,
};

pub use types::{
    BlockDerivedNormalizedEventKindSyncSummary, BlockDerivedNormalizedEventSyncSummary,
};

#[cfg(test)]
use anyhow::Context;
#[cfg(test)]
use bigname_storage::CanonicalityState;
#[cfg(test)]
use constants::*;
#[cfg(test)]
use decoding::{
    hex_string, keccak_signature_hex, keccak256_hex, name_wrapped_topic0,
    registrar_name_registered_topic0, registrar_name_renewed_topic0,
};
#[cfg(test)]
use preimage_observation::observe_dns_encoded_name;
#[cfg(test)]
use types::WatchedRawLogRow;

/// Sync the first block-derived normalized events from stored raw logs.
pub async fn sync_block_derived_normalized_events(
    pool: &PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
) -> Result<BlockDerivedNormalizedEventSyncSummary> {
    sync_block_derived_normalized_events_inner(pool, chain, block_hashes, source_scope, None).await
}

/// Sync block-derived normalized events when the caller already knows how many
/// canonical raw logs the replay selected.
pub async fn sync_block_derived_normalized_events_with_scanned_log_count(
    pool: &PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    scanned_log_count: usize,
) -> Result<BlockDerivedNormalizedEventSyncSummary> {
    sync_block_derived_normalized_events_inner(
        pool,
        chain,
        block_hashes,
        source_scope,
        Some(scanned_log_count),
    )
    .await
}

async fn sync_block_derived_normalized_events_inner(
    pool: &PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    known_scanned_log_count: Option<usize>,
) -> Result<BlockDerivedNormalizedEventSyncSummary> {
    if block_hashes.is_empty() {
        return Ok(empty_summary(known_scanned_log_count.unwrap_or(0)));
    }

    let scanned_log_count = match known_scanned_log_count {
        Some(scanned_log_count) => scanned_log_count,
        None => load_scanned_log_count(pool, chain, block_hashes).await?,
    };
    let raw_logs = load_watched_raw_logs(pool, chain, block_hashes, source_scope).await?;
    if raw_logs.is_empty() {
        return Ok(empty_summary(scanned_log_count));
    }

    let mut matched_log_refs = HashSet::new();
    let mut events = Vec::new();
    for raw_log in &raw_logs {
        let observed_events = build_preimage_observed_events(raw_log)?;
        if observed_events.is_empty() {
            continue;
        }
        matched_log_refs.insert((
            raw_log.chain_id.clone(),
            raw_log.block_hash.clone(),
            raw_log.transaction_hash.clone(),
            raw_log.log_index,
        ));
        events.extend(observed_events);
    }

    if events.is_empty() {
        return Ok(empty_summary(scanned_log_count));
    }

    let existing_event_identities = load_existing_event_identities(pool, &events).await?;
    let inserted_by_kind = count_inserted_events_by_kind(&events, &existing_event_identities);
    let synced_by_kind = count_events_by_kind(&events);

    upsert_normalized_events(pool, &events).await?;

    Ok(build_summary(
        scanned_log_count,
        matched_log_refs.len(),
        &events,
        synced_by_kind,
        inserted_by_kind,
    ))
}

fn empty_summary(scanned_log_count: usize) -> BlockDerivedNormalizedEventSyncSummary {
    BlockDerivedNormalizedEventSyncSummary {
        scanned_log_count,
        matched_log_count: 0,
        total_synced_count: 0,
        total_inserted_count: 0,
        by_kind: BTreeMap::new(),
    }
}

fn build_summary(
    scanned_log_count: usize,
    matched_log_count: usize,
    events: &[NormalizedEvent],
    synced_by_kind: BTreeMap<String, usize>,
    inserted_by_kind: BTreeMap<String, usize>,
) -> BlockDerivedNormalizedEventSyncSummary {
    let by_kind = synced_by_kind
        .into_iter()
        .map(|(event_kind, synced_count)| {
            let inserted_count = inserted_by_kind.get(&event_kind).copied().unwrap_or(0);
            (
                event_kind,
                BlockDerivedNormalizedEventKindSyncSummary {
                    synced_count,
                    inserted_count,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    BlockDerivedNormalizedEventSyncSummary {
        scanned_log_count,
        matched_log_count,
        total_synced_count: events.len(),
        total_inserted_count: inserted_by_kind.values().sum(),
        by_kind,
    }
}

#[cfg(test)]
mod tests;
