use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
};

use anyhow::{Context, Result, bail};
use bigname_manifests::{WatchedSourceSelectorKind, WatchedSourceSelectorPlan};
use sha3::Digest;
use tracing::info;

use crate::provider::{ChainProviderOps, ProviderLog, ProviderResolvedBlock};

use super::super::{
    BackfillBlockRange,
    selection::{
        SelectedTargetIntervalIndex, selected_log_range_requests,
        selected_target_addresses_at_block,
    },
};

const LARGE_SOURCE_FAMILY_TOPIC_FILTER_TARGET_THRESHOLD: usize = 10_000;
const SOURCE_FAMILY_ENS_V1_RESOLVER_L1: &str = "ens_v1_resolver_l1";
const ENS_V1_RESOLVER_EVENT_SIGNATURES: &[&str] = &[
    "ABIChanged(bytes32,uint256)",
    "AddrChanged(bytes32,address)",
    "AddressChanged(bytes32,uint256,bytes)",
    "ApprovalForAll(address,address,bool)",
    "Approved(address,bytes32,address,bool)",
    "ContentChanged(bytes32,bytes32)",
    "ContenthashChanged(bytes32,bytes)",
    "DNSRecordChanged(bytes32,bytes,uint16,bytes)",
    "DNSRecordDeleted(bytes32,bytes,uint16)",
    "DNSZonehashChanged(bytes32,bytes,bytes)",
    "DataChanged(bytes32,string,string,bytes)",
    "InterfaceChanged(bytes32,bytes4,address)",
    "NameChanged(bytes32,string)",
    "PubkeyChanged(bytes32,bytes32,bytes32)",
    "TextChanged(bytes32,string,string)",
    "VerifierChanged(bytes,address)",
    "VersionChanged(bytes32,uint64)",
];

pub(super) async fn fetch_backfill_logs_by_safe_ranges(
    provider: &(impl ChainProviderOps + ?Sized),
    source_plan: &WatchedSourceSelectorPlan,
    selected_target_index: &SelectedTargetIntervalIndex,
    selected_target_addresses_for_chunk: &[String],
    resolved_blocks: &[ProviderResolvedBlock],
    range: BackfillBlockRange,
) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
    if let Some(topic0s) = source_family_topic0s_for_range_scan(source_plan) {
        return fetch_topic_first_logs_by_safe_ranges(
            provider,
            source_plan,
            selected_target_index,
            selected_target_addresses_for_chunk,
            resolved_blocks,
            range,
            topic0s,
        )
        .await;
    }

    let mut logs_by_block = BTreeMap::new();
    for request in selected_log_range_requests(source_plan, resolved_blocks) {
        let request_blocks = &resolved_blocks[request.start_index..request.end_index];
        let from_block = request_blocks
            .first()
            .expect("selected log range request must contain at least one block")
            .block_number;
        let to_block = request_blocks
            .last()
            .expect("selected log range request must contain at least one block")
            .block_number;
        let group_logs = provider
            .fetch_logs_by_block_range(request_blocks, &request.addresses)
            .await
            .with_context(|| {
                format!(
                    "failed to fetch hash-pinned log range {}..={} inside backfill range {}..={}",
                    from_block, to_block, range.from_block, range.to_block
                )
            })?;

        for (block_number, logs) in group_logs {
            if logs_by_block.insert(block_number, logs).is_some() {
                bail!("provider returned duplicate range logs for backfill block {block_number}");
            }
        }
    }

    Ok(logs_by_block)
}

async fn fetch_topic_first_logs_by_safe_ranges(
    provider: &(impl ChainProviderOps + ?Sized),
    source_plan: &WatchedSourceSelectorPlan,
    selected_target_index: &SelectedTargetIntervalIndex,
    selected_target_addresses_for_chunk: &[String],
    resolved_blocks: &[ProviderResolvedBlock],
    range: BackfillBlockRange,
    topic0s: Vec<String>,
) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
    if selected_target_addresses_for_chunk.is_empty() {
        info!(
            service = "indexer",
            command = "backfill",
            chain = %source_plan.watched_chain_plan.chain,
            source_family = source_plan.source_family.as_deref(),
            selected_target_count = source_plan.selected_targets.len(),
            topic0_count = topic0s.len(),
            selected_target_address_count = 0usize,
            from_block = range.from_block,
            to_block = range.to_block,
            "skipping hash-pinned topic-first range log fetch with no active selected targets"
        );
        return Ok(BTreeMap::new());
    }

    let mut logs_by_block = BTreeMap::new();
    let group_logs = provider
        .fetch_logs_by_block_range_for_topic0s_and_addresses(
            resolved_blocks,
            &topic0s,
            selected_target_addresses_for_chunk,
        )
        .await
        .with_context(|| {
            format!(
                "failed to fetch hash-pinned topic0 log range inside backfill range {}..={}",
                range.from_block, range.to_block
            )
        })?;
    let mut provider_log_count = 0usize;
    let mut selected_log_count = 0usize;
    for (block_number, logs) in group_logs {
        provider_log_count += logs.len();
        let selected_logs = logs
            .into_iter()
            .filter(|log| selected_target_index.contains(&log.address, block_number))
            .collect::<Vec<_>>();
        selected_log_count += selected_logs.len();
        if logs_by_block.insert(block_number, selected_logs).is_some() {
            bail!("provider returned duplicate range logs for backfill block {block_number}");
        }
    }
    info!(
        service = "indexer",
        command = "backfill",
        chain = %source_plan.watched_chain_plan.chain,
        source_family = source_plan.source_family.as_deref(),
        selected_target_count = source_plan.selected_targets.len(),
        topic0_count = topic0s.len(),
        selected_target_address_count = selected_target_addresses_for_chunk.len(),
        provider_log_count,
        selected_log_count,
        from_block = range.from_block,
        to_block = range.to_block,
        "hash-pinned topic-first range logs filtered to selected targets"
    );

    Ok(logs_by_block)
}

pub(super) fn selected_addresses_for_materialized_block(
    source_plan: &WatchedSourceSelectorPlan,
    selected_target_index: &SelectedTargetIntervalIndex,
    topic_filtered_source_family: bool,
    block_number: i64,
    block_logs: &[ProviderLog],
) -> BTreeSet<String> {
    if topic_filtered_source_family {
        selected_target_index.addresses_for_logs_at_block(block_logs, block_number)
    } else {
        selected_target_addresses_at_block(source_plan, block_number)
    }
}

pub(super) fn uses_topic_first_source_family_scan(source_plan: &WatchedSourceSelectorPlan) -> bool {
    source_family_topic0s_for_range_scan(source_plan).is_some()
}

fn source_family_topic0s_for_range_scan(
    source_plan: &WatchedSourceSelectorPlan,
) -> Option<Vec<String>> {
    if source_plan.selector_kind != WatchedSourceSelectorKind::SourceFamily {
        return None;
    }
    if source_plan.source_family.as_deref() != Some(SOURCE_FAMILY_ENS_V1_RESOLVER_L1) {
        return None;
    }
    if source_plan.selected_targets.len() <= LARGE_SOURCE_FAMILY_TOPIC_FILTER_TARGET_THRESHOLD {
        return None;
    }

    Some(
        ENS_V1_RESOLVER_EVENT_SIGNATURES
            .iter()
            .map(|signature| topic0_hex(signature))
            .collect(),
    )
}

fn topic0_hex(signature: &str) -> String {
    let digest = sha3::Keccak256::digest(signature.as_bytes());
    let mut output = String::with_capacity(66);
    output.push_str("0x");
    for byte in digest {
        write!(&mut output, "{byte:02x}").expect("writing to String must not fail");
    }
    output
}
