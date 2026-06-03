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

const BASENAMES_BASE_REGISTRY_SOURCE_FAMILY: &str = "basenames_base_registry";
const BASENAMES_SCAN_ALL_SOURCE_FAMILIES: &[&str] = &[BASENAMES_BASE_REGISTRY_SOURCE_FAMILY];
const SCAN_ALL_EMITTERS_ADDRESS_THRESHOLD: usize = 512;

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
    let mut event_signatures_by_family = BTreeMap::<String, BTreeSet<String>>::new();
    for event in events {
        if let Some(topic0) = event.topic0 {
            topics_by_family
                .entry(event.source_family.clone())
                .or_default()
                .insert(topic0.to_ascii_lowercase());
            event_signatures_by_family
                .entry(event.source_family)
                .or_default()
                .insert(event.canonical_signature);
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
        event_signatures_by_family
            .into_iter()
            .map(|(family, signatures)| (family, signatures.into_iter().collect()))
            .collect(),
        source_families_without_topics,
    ))
}

pub(super) fn build_filter_packs(
    request: &HistoricalLogPayloadRequest<'_>,
) -> Vec<CoinbaseSqlFilterPack> {
    for source_family in BASENAMES_SCAN_ALL_SOURCE_FAMILIES {
        if let Some(packs) = scan_all_source_family_filter_packs(request, source_family) {
            return packs;
        }
    }

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

fn scan_all_source_family_filter_packs(
    request: &HistoricalLogPayloadRequest<'_>,
    source_family: &str,
) -> Option<Vec<CoinbaseSqlFilterPack>> {
    if request.resolved_blocks.is_empty()
        || request.selected_target_addresses_for_chunk.is_empty()
        || !request
            .source_plan
            .selected_targets
            .iter()
            .all(|target| target.source_family == source_family)
    {
        return None;
    }

    let topic0s = request.topic_plan.topic0s_for_source_family(source_family);
    let event_signatures = request
        .topic_plan
        .event_signatures_for_source_family(source_family);
    if topic0s.is_empty() || event_signatures.is_empty() {
        return None;
    }

    Some(vec![CoinbaseSqlFilterPack {
        chain: request.chain.to_owned(),
        from_block: request
            .resolved_blocks
            .first()
            .expect("resolved blocks are not empty")
            .block_number,
        to_block: request
            .resolved_blocks
            .last()
            .expect("resolved blocks are not empty")
            .block_number,
        addresses: Vec::new(),
        topic0s: topic0s.to_vec(),
        event_signatures: event_signatures.to_vec(),
        scan_all_emitters: true,
        source_families: vec![source_family.to_owned()],
    }])
}

fn packs_for_source_family_segment(
    chain: &str,
    topic_plan: &BackfillTopicPlan,
    segment: CoinbaseSqlSourceFamilySegment,
) -> Vec<CoinbaseSqlFilterPack> {
    let mut packs_by_topics =
        BTreeMap::<(Option<Vec<String>>, Option<Vec<String>>, bool), CoinbaseSqlFilterPack>::new();
    for (source_family, addresses) in segment.addresses_by_source_family {
        let topic0s = topic_plan.topic0s_for_source_family(&source_family);
        let event_signatures = topic_plan.event_signatures_for_source_family(&source_family);
        let topic_key = topic_plan
            .source_family_has_topics(&source_family)
            .then(|| topic0s.to_vec());
        let event_signature_key = topic_plan
            .source_family_has_topics(&source_family)
            .then(|| event_signatures.to_vec())
            .filter(|signatures| !signatures.is_empty());
        let scan_all_emitters = should_scan_all_emitters(
            &source_family,
            addresses.len(),
            topic_key.as_ref(),
            event_signature_key.as_ref(),
        );
        let entry = packs_by_topics
            .entry((
                topic_key.clone(),
                event_signature_key.clone(),
                scan_all_emitters,
            ))
            .or_insert_with(|| CoinbaseSqlFilterPack {
                chain: chain.to_owned(),
                from_block: segment.from_block,
                to_block: segment.to_block,
                addresses: Vec::new(),
                topic0s: topic_key.unwrap_or_default(),
                event_signatures: event_signature_key.unwrap_or_default(),
                scan_all_emitters,
                source_families: Vec::new(),
            });
        if !scan_all_emitters {
            entry.addresses.extend(addresses);
        }
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

fn should_scan_all_emitters(
    source_family: &str,
    address_count: usize,
    topic0s: Option<&Vec<String>>,
    event_signatures: Option<&Vec<String>>,
) -> bool {
    source_family == BASENAMES_BASE_REGISTRY_SOURCE_FAMILY
        && address_count > SCAN_ALL_EMITTERS_ADDRESS_THRESHOLD
        && topic0s.is_some_and(|topic0s| !topic0s.is_empty())
        && event_signatures.is_some_and(|event_signatures| !event_signatures.is_empty())
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
