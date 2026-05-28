use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use bigname_manifests::{
    WatchedBackfillTarget, WatchedSourceSelectorPlan,
    load_active_manifest_abi_events_by_chain_and_source_families,
};
use tracing::warn;

use crate::provider::ProviderResolvedBlock;

use super::query::CoinbaseSqlFilterPack;
use crate::backfill::{
    BackfillTopicPlan, HistoricalLogPayloadRequest,
    selection::{BackfillLogRangeRequest, selected_log_range_requests},
};

pub(crate) async fn load_backfill_topic_plan(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
) -> Result<BackfillTopicPlan> {
    let source_families = source_plan
        .selected_targets
        .iter()
        .map(|target| target.source_family.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let events = load_active_manifest_abi_events_by_chain_and_source_families(
        pool,
        &source_plan.watched_chain_plan.chain,
        &source_families,
    )
    .await
    .context("failed to load manifest ABI event topics for Coinbase SQL backfill")?;

    let mut topics_by_family = BTreeMap::<String, BTreeSet<String>>::new();
    for event in events {
        if let Some(topic0) = event.topic0 {
            topics_by_family
                .entry(event.source_family)
                .or_default()
                .insert(topic0.to_ascii_lowercase());
        }
    }

    let source_families_without_topics = source_families
        .iter()
        .filter(|source_family| {
            !topics_by_family
                .get(*source_family)
                .is_some_and(|topics| !topics.is_empty())
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    if !source_families_without_topics.is_empty() {
        warn!(
            service = "indexer",
            command = "backfill",
            chain = %source_plan.watched_chain_plan.chain,
            source_families = %source_families_without_topics.iter().cloned().collect::<Vec<_>>().join(","),
            "Coinbase SQL backfill selected source families with no active manifest ABI topics; those packs use address-only SQL scans"
        );
    }

    Ok(BackfillTopicPlan::new(
        topics_by_family
            .into_iter()
            .map(|(family, topics)| (family, topics.into_iter().collect()))
            .collect(),
        source_families_without_topics,
    ))
}

pub(super) fn build_filter_packs(
    request: &HistoricalLogPayloadRequest<'_>,
) -> Vec<CoinbaseSqlFilterPack> {
    selected_log_range_requests(request.source_plan, request.resolved_blocks)
        .into_iter()
        .flat_map(|range_request| {
            split_range_request_by_source_families(
                request.source_plan,
                request.selected_target_addresses_for_chunk,
                request.resolved_blocks,
                range_request,
            )
            .into_iter()
            .flat_map(|segment| {
                packs_for_source_family_segment(request.chain, request.topic_plan, segment)
            })
        })
        .collect()
}

fn packs_for_source_family_segment(
    chain: &str,
    topic_plan: &BackfillTopicPlan,
    segment: CoinbaseSqlSourceFamilySegment,
) -> Vec<CoinbaseSqlFilterPack> {
    let mut packs_by_topics = BTreeMap::<Option<Vec<String>>, CoinbaseSqlFilterPack>::new();
    for (source_family, addresses) in segment.addresses_by_source_family {
        let topic0s = topic_plan.topic0s_for_source_family(&source_family);
        let topic_key = topic_plan
            .source_family_has_topics(&source_family)
            .then(|| topic0s.to_vec());
        let entry =
            packs_by_topics
                .entry(topic_key.clone())
                .or_insert_with(|| CoinbaseSqlFilterPack {
                    chain: chain.to_owned(),
                    from_block: segment.from_block,
                    to_block: segment.to_block,
                    addresses: Vec::new(),
                    topic0s: topic_key.unwrap_or_default(),
                    scan_all_emitters: false,
                    source_families: Vec::new(),
                });
        entry.addresses.extend(addresses);
        entry.source_families.push(source_family);
    }

    packs_by_topics
        .into_values()
        .map(|mut pack| {
            pack.addresses.sort();
            pack.addresses.dedup();
            pack.source_families.sort();
            pack.source_families.dedup();
            pack
        })
        .collect()
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CoinbaseSqlSourceFamilySegment {
    from_block: i64,
    to_block: i64,
    addresses_by_source_family: BTreeMap<String, BTreeSet<String>>,
}

fn split_range_request_by_source_families(
    source_plan: &WatchedSourceSelectorPlan,
    selected_target_addresses_for_chunk: &[String],
    resolved_blocks: &[ProviderResolvedBlock],
    range_request: BackfillLogRangeRequest,
) -> Vec<CoinbaseSqlSourceFamilySegment> {
    let mut segments = Vec::new();
    let mut active_start_index = None;
    let mut active_source_family_addresses = BTreeMap::<String, BTreeSet<String>>::new();

    for (index, block) in resolved_blocks[range_request.start_index..range_request.end_index]
        .iter()
        .enumerate()
    {
        let absolute_index = range_request.start_index + index;
        let source_family_addresses = active_source_family_addresses_at_block(
            source_plan,
            selected_target_addresses_for_chunk,
            &range_request.addresses,
            block.block_number,
        );
        if source_family_addresses.is_empty() {
            if let Some(start_index) = active_start_index.take() {
                push_source_family_segment(
                    &mut segments,
                    resolved_blocks,
                    start_index,
                    absolute_index,
                    &active_source_family_addresses,
                );
                active_source_family_addresses.clear();
            }
            continue;
        }

        match active_start_index {
            Some(start_index) if active_source_family_addresses == source_family_addresses => {
                active_start_index = Some(start_index);
            }
            Some(start_index) => {
                push_source_family_segment(
                    &mut segments,
                    resolved_blocks,
                    start_index,
                    absolute_index,
                    &active_source_family_addresses,
                );
                active_start_index = Some(absolute_index);
                active_source_family_addresses = source_family_addresses;
            }
            None => {
                active_start_index = Some(absolute_index);
                active_source_family_addresses = source_family_addresses;
            }
        }
    }

    if let Some(start_index) = active_start_index {
        push_source_family_segment(
            &mut segments,
            resolved_blocks,
            start_index,
            range_request.end_index,
            &active_source_family_addresses,
        );
    }

    segments
}

fn push_source_family_segment(
    segments: &mut Vec<CoinbaseSqlSourceFamilySegment>,
    resolved_blocks: &[ProviderResolvedBlock],
    start_index: usize,
    end_index: usize,
    addresses_by_source_family: &BTreeMap<String, BTreeSet<String>>,
) {
    if start_index >= end_index || addresses_by_source_family.is_empty() {
        return;
    }
    let from_block = resolved_blocks[start_index].block_number;
    let to_block = resolved_blocks[end_index - 1].block_number;
    segments.push(CoinbaseSqlSourceFamilySegment {
        from_block,
        to_block,
        addresses_by_source_family: addresses_by_source_family.clone(),
    });
}

fn active_source_family_addresses_at_block(
    source_plan: &WatchedSourceSelectorPlan,
    selected_target_addresses_for_chunk: &[String],
    request_addresses: &[String],
    block_number: i64,
) -> BTreeMap<String, BTreeSet<String>> {
    let request_addresses = request_addresses
        .iter()
        .map(|address| address.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let chunk_addresses = selected_target_addresses_for_chunk
        .iter()
        .map(|address| address.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();

    let mut addresses_by_family = BTreeMap::<String, BTreeSet<String>>::new();
    for target in source_plan
        .selected_targets
        .iter()
        .filter(|target| target_overlaps_block(target, block_number))
        .filter(|target| {
            let address = target.address.to_ascii_lowercase();
            request_addresses.contains(&address)
                && (chunk_addresses.is_empty() || chunk_addresses.contains(&address))
        })
    {
        addresses_by_family
            .entry(target.source_family.clone())
            .or_default()
            .insert(target.address.to_ascii_lowercase());
    }

    addresses_by_family
}

fn target_overlaps_block(target: &WatchedBackfillTarget, block_number: i64) -> bool {
    target.effective_from_block <= block_number && block_number <= target.effective_to_block
}

#[allow(dead_code)]
fn _assert_contiguous_blocks(_blocks: &[ProviderResolvedBlock]) {}
