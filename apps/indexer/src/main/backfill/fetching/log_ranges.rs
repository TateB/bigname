use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use bigname_manifests::WatchedSourceSelectorPlan;
use tracing::info;

use crate::{
    ens_v1_resolver::{SOURCE_FAMILY_ENS_V1_RESOLVER_L1, generic_resolver_record_topic0s},
    provider::{ChainProviderOps, ProviderLog, ProviderResolvedBlock},
    source_scope::watched_source_plan_uses_generic_resolver_scope,
};

use super::super::{
    BackfillBlockRange,
    selection::{
        SelectedTargetIntervalIndex, selected_log_range_requests,
        selected_log_range_requests_without_source_family, selected_target_addresses_at_block,
    },
};

pub(super) async fn fetch_backfill_logs_by_safe_ranges(
    provider: &(impl ChainProviderOps + ?Sized),
    source_plan: &WatchedSourceSelectorPlan,
    selected_target_index: &SelectedTargetIntervalIndex,
    selected_target_addresses_for_chunk: &[String],
    resolved_blocks: &[ProviderResolvedBlock],
    range: BackfillBlockRange,
) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
    if let Some(topic_scan) = source_family_topic_scan(source_plan) {
        let mut logs_by_block = fetch_topic_first_logs_by_safe_ranges(
            provider,
            source_plan,
            selected_target_index,
            selected_target_addresses_for_chunk,
            resolved_blocks,
            range,
            topic_scan,
        )
        .await?;
        let address_scoped_logs = fetch_address_scoped_logs_by_safe_ranges(
            provider,
            source_plan,
            resolved_blocks,
            range,
            true,
        )
        .await?;
        merge_logs_by_block(&mut logs_by_block, address_scoped_logs);
        return Ok(logs_by_block);
    }

    fetch_address_scoped_logs_by_safe_ranges(provider, source_plan, resolved_blocks, range, false)
        .await
}

async fn fetch_address_scoped_logs_by_safe_ranges(
    provider: &(impl ChainProviderOps + ?Sized),
    source_plan: &WatchedSourceSelectorPlan,
    resolved_blocks: &[ProviderResolvedBlock],
    range: BackfillBlockRange,
    omit_generic_resolver_targets: bool,
) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
    let mut logs_by_block = BTreeMap::new();
    let requests = if omit_generic_resolver_targets {
        selected_log_range_requests_without_source_family(
            source_plan,
            resolved_blocks,
            SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
        )
    } else {
        selected_log_range_requests(source_plan, resolved_blocks)
    };
    for request in requests {
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
    topic_scan: SourceFamilyTopicScan,
) -> Result<BTreeMap<i64, Vec<ProviderLog>>> {
    let address_filter = if topic_scan.scan_all_emitters {
        &[][..]
    } else {
        selected_target_addresses_for_chunk
    };
    if address_filter.is_empty() && !topic_scan.scan_all_emitters {
        info!(
            service = "indexer",
            command = "backfill",
            chain = %source_plan.watched_chain_plan.chain,
            source_family = source_plan.source_family.as_deref(),
            selected_target_count = source_plan.selected_targets.len(),
            topic0_count = topic_scan.topic0s.len(),
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
            &topic_scan.topic0s,
            address_filter,
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
        let selected_logs = if topic_scan.scan_all_emitters {
            logs
        } else {
            logs.into_iter()
                .filter(|log| selected_target_index.contains(&log.address, block_number))
                .collect::<Vec<_>>()
        };
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
        topic0_count = topic_scan.topic0s.len(),
        selected_target_address_count = address_filter.len(),
        scan_all_emitters = topic_scan.scan_all_emitters,
        provider_log_count,
        selected_log_count,
        from_block = range.from_block,
        to_block = range.to_block,
        "hash-pinned topic-first range logs fetched"
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
    if scans_all_resolver_event_emitters(source_plan) {
        block_logs
            .iter()
            .map(|log| log.address.to_ascii_lowercase())
            .collect()
    } else if topic_filtered_source_family {
        selected_target_index.addresses_for_logs_at_block(block_logs, block_number)
    } else {
        selected_target_addresses_at_block(source_plan, block_number)
    }
}

pub(super) fn uses_topic_first_source_family_scan(source_plan: &WatchedSourceSelectorPlan) -> bool {
    source_family_topic_scan(source_plan).is_some()
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SourceFamilyTopicScan {
    topic0s: Vec<String>,
    scan_all_emitters: bool,
}

fn source_family_topic_scan(
    source_plan: &WatchedSourceSelectorPlan,
) -> Option<SourceFamilyTopicScan> {
    if !watched_source_plan_uses_generic_resolver_scope(source_plan) {
        return None;
    }

    Some(SourceFamilyTopicScan {
        topic0s: generic_resolver_record_topic0s(),
        scan_all_emitters: true,
    })
}

fn scans_all_resolver_event_emitters(source_plan: &WatchedSourceSelectorPlan) -> bool {
    watched_source_plan_uses_generic_resolver_scope(source_plan)
}

fn merge_logs_by_block(
    logs_by_block: &mut BTreeMap<i64, Vec<ProviderLog>>,
    incoming_logs_by_block: BTreeMap<i64, Vec<ProviderLog>>,
) {
    for (block_number, incoming_logs) in incoming_logs_by_block {
        let logs = logs_by_block.entry(block_number).or_default();
        for incoming_log in incoming_logs {
            if !logs.iter().any(|log| same_log_identity(log, &incoming_log)) {
                logs.push(incoming_log);
            }
        }
        logs.sort_by(|left, right| {
            left.transaction_index
                .cmp(&right.transaction_index)
                .then_with(|| left.log_index.cmp(&right.log_index))
        });
    }
}

fn same_log_identity(left: &ProviderLog, right: &ProviderLog) -> bool {
    left.block_hash.eq_ignore_ascii_case(&right.block_hash)
        && left
            .transaction_hash
            .eq_ignore_ascii_case(&right.transaction_hash)
        && left.log_index == right.log_index
}
