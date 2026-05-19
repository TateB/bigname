use std::{
    collections::{BTreeMap, HashSet},
    time::Instant,
};

use anyhow::Result;
use sqlx::PgPool;
use tracing::info;

#[cfg(test)]
use bigname_storage::NormalizedEvent;

mod constants;
mod decoding;
mod event_builders;
mod event_topics;
mod loading;
mod preimage_observation;
mod source_selection;
mod types;

use crate::normalized_event_support::{
    NormalizedEventSyncCounts, count_events_by_kind, upsert_normalized_events_with_counts,
};
use event_builders::build_preimage_observed_events;
use loading::{RawLogCanonicalityFilter, load_scanned_log_count, load_watched_raw_logs};

pub use types::{
    BlockDerivedNormalizedEventKindSyncSummary, BlockDerivedNormalizedEventSyncSummary,
};

#[cfg(test)]
use crate::evm_abi::keccak_signature_hex;
#[cfg(test)]
use anyhow::Context;
#[cfg(test)]
use bigname_storage::CanonicalityState;
#[cfg(test)]
use constants::*;
#[cfg(test)]
use decoding::{hex_string, keccak256_hex};
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
    sync_block_derived_normalized_events_inner(
        pool,
        chain,
        block_hashes,
        source_scope,
        RawLogCanonicalityFilter::IncludeObserved,
        None,
    )
    .await
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
        RawLogCanonicalityFilter::CanonicalOnly,
        Some(scanned_log_count),
    )
    .await
}

async fn sync_block_derived_normalized_events_inner(
    pool: &PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    canonicality_filter: RawLogCanonicalityFilter,
    known_scanned_log_count: Option<usize>,
) -> Result<BlockDerivedNormalizedEventSyncSummary> {
    if block_hashes.is_empty() {
        return Ok(empty_summary(known_scanned_log_count.unwrap_or(0)));
    }

    let total_started = Instant::now();
    let source_scope_target_count = source_scope.map_or(0, <[_]>::len);
    let scanned_started = Instant::now();
    let scanned_log_count = match known_scanned_log_count {
        Some(scanned_log_count) => scanned_log_count,
        None => load_scanned_log_count(pool, chain, block_hashes, canonicality_filter).await?,
    };
    let load_scanned_log_count_ms = scanned_started.elapsed().as_millis();

    let watched_raw_logs_started = Instant::now();
    let raw_log_load =
        load_watched_raw_logs(pool, chain, block_hashes, source_scope, canonicality_filter).await?;
    let load_watched_raw_logs_ms = watched_raw_logs_started.elapsed().as_millis();
    let raw_logs = raw_log_load.raw_logs;
    if raw_logs.is_empty() {
        let summary = empty_summary(scanned_log_count);
        log_block_derived_normalization_timing(
            chain,
            block_hashes.len(),
            source_scope_target_count,
            raw_logs.len(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &summary,
            load_scanned_log_count_ms,
            load_watched_raw_logs_ms,
            0,
            0,
            total_started.elapsed().as_millis(),
        );
        return Ok(summary);
    }

    let build_started = Instant::now();
    let mut matched_log_refs = HashSet::new();
    let mut events = Vec::new();
    let mut build_by_source_family = BTreeMap::<String, SourceFamilyBuildTiming>::new();
    for raw_log in &raw_logs {
        let row_started = Instant::now();
        let observed_events = build_preimage_observed_events(raw_log, &raw_log_load.event_topics)?;
        let elapsed_us = row_started.elapsed().as_micros();
        let source_family_timing = build_by_source_family
            .entry(raw_log.source_family.clone())
            .or_default();
        source_family_timing.raw_log_count += 1;
        source_family_timing.elapsed_us += elapsed_us;
        if observed_events.is_empty() {
            continue;
        }
        source_family_timing.matched_log_count += 1;
        source_family_timing.normalized_event_count += observed_events.len();
        matched_log_refs.insert((
            raw_log.chain_id.clone(),
            raw_log.block_hash.clone(),
            raw_log.transaction_hash.clone(),
            raw_log.log_index,
        ));
        events.extend(observed_events);
    }
    let build_events_ms = build_started.elapsed().as_millis();

    if events.is_empty() {
        let summary = empty_summary(scanned_log_count);
        log_block_derived_normalization_timing(
            chain,
            block_hashes.len(),
            source_scope_target_count,
            raw_logs.len(),
            &build_by_source_family,
            &BTreeMap::new(),
            &summary,
            load_scanned_log_count_ms,
            load_watched_raw_logs_ms,
            build_events_ms,
            0,
            total_started.elapsed().as_millis(),
        );
        return Ok(summary);
    }

    let event_kind_counts = count_events_by_kind(&events);
    let persistence_started = Instant::now();
    let counts =
        upsert_normalized_events_with_counts(pool, &events, "block-derived normalized-event")
            .await?;
    let persistence_ms = persistence_started.elapsed().as_millis();

    let summary = build_summary(scanned_log_count, matched_log_refs.len(), counts);
    log_block_derived_normalization_timing(
        chain,
        block_hashes.len(),
        source_scope_target_count,
        raw_logs.len(),
        &build_by_source_family,
        &event_kind_counts,
        &summary,
        load_scanned_log_count_ms,
        load_watched_raw_logs_ms,
        build_events_ms,
        persistence_ms,
        total_started.elapsed().as_millis(),
    );

    Ok(summary)
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct SourceFamilyBuildTiming {
    raw_log_count: usize,
    matched_log_count: usize,
    normalized_event_count: usize,
    elapsed_us: u128,
}

fn log_block_derived_normalization_timing(
    chain: &str,
    block_hash_count: usize,
    source_scope_target_count: usize,
    watched_raw_log_count: usize,
    build_by_source_family: &BTreeMap<String, SourceFamilyBuildTiming>,
    event_kind_counts: &BTreeMap<String, usize>,
    summary: &BlockDerivedNormalizedEventSyncSummary,
    load_scanned_log_count_ms: u128,
    load_watched_raw_logs_ms: u128,
    build_events_ms: u128,
    persistence_ms: u128,
    elapsed_ms: u128,
) {
    info!(
        service = "adapters",
        adapter = "block_derived_normalized_events",
        chain,
        block_hash_count,
        source_scope_target_count,
        scanned_log_count = summary.scanned_log_count,
        watched_raw_log_count,
        matched_log_count = summary.matched_log_count,
        normalized_event_synced_count = summary.total_synced_count,
        normalized_event_inserted_count = summary.total_inserted_count,
        event_kind_counts = ?event_kind_counts,
        build_by_source_family = ?build_by_source_family,
        load_scanned_log_count_ms,
        load_watched_raw_logs_ms,
        build_events_ms,
        persistence_ms,
        elapsed_ms,
        "block-derived normalized-event timing completed"
    );
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
    counts: NormalizedEventSyncCounts,
) -> BlockDerivedNormalizedEventSyncSummary {
    let (total_synced_count, total_inserted_count, by_kind) =
        counts.into_parts_by_kind(|synced_count, inserted_count| {
            BlockDerivedNormalizedEventKindSyncSummary {
                synced_count,
                inserted_count,
            }
        });

    BlockDerivedNormalizedEventSyncSummary {
        scanned_log_count,
        matched_log_count,
        total_synced_count,
        total_inserted_count,
        by_kind,
    }
}

#[cfg(test)]
mod tests;
