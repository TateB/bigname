use std::collections::{BTreeMap, BTreeSet};

use bigname_manifests::{WatchedBackfillTarget, WatchedSourceSelectorPlan};

use crate::provider::{ProviderLog, ProviderResolvedBlock};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BackfillLogRangeRequest {
    pub(crate) start_index: usize,
    pub(crate) end_index: usize,
    pub(crate) addresses: Vec<String>,
}

pub(crate) fn selected_log_range_requests(
    source_plan: &WatchedSourceSelectorPlan,
    resolved_blocks: &[ProviderResolvedBlock],
) -> Vec<BackfillLogRangeRequest> {
    selected_log_range_requests_with_filter(source_plan, resolved_blocks, |_| true)
}

pub(super) fn selected_log_range_requests_without_source_family(
    source_plan: &WatchedSourceSelectorPlan,
    resolved_blocks: &[ProviderResolvedBlock],
    excluded_source_family: &str,
) -> Vec<BackfillLogRangeRequest> {
    selected_log_range_requests_with_filter(source_plan, resolved_blocks, |target| {
        target.source_family != excluded_source_family
    })
}

fn selected_log_range_requests_with_filter(
    source_plan: &WatchedSourceSelectorPlan,
    resolved_blocks: &[ProviderResolvedBlock],
    include_target: impl Fn(&WatchedBackfillTarget) -> bool,
) -> Vec<BackfillLogRangeRequest> {
    let mut requests = Vec::new();
    let mut active_start = None;
    let mut active_addresses = BTreeSet::<String>::new();

    for (index, block) in resolved_blocks.iter().enumerate() {
        let addresses = selected_target_addresses_at_block_with_filter(
            source_plan,
            block.block_number,
            &include_target,
        );
        if addresses.is_empty() {
            if let Some(start_index) = active_start.take() {
                requests.push(BackfillLogRangeRequest {
                    start_index,
                    end_index: index,
                    addresses: active_addresses.iter().cloned().collect(),
                });
                active_addresses.clear();
            }
            continue;
        }

        match active_start {
            Some(start_index) if active_addresses == addresses => {
                active_start = Some(start_index);
            }
            Some(start_index) => {
                requests.push(BackfillLogRangeRequest {
                    start_index,
                    end_index: index,
                    addresses: active_addresses.iter().cloned().collect(),
                });
                active_start = Some(index);
                active_addresses = addresses;
            }
            None => {
                active_start = Some(index);
                active_addresses = addresses;
            }
        }
    }

    if let Some(start_index) = active_start {
        requests.push(BackfillLogRangeRequest {
            start_index,
            end_index: resolved_blocks.len(),
            addresses: active_addresses.into_iter().collect(),
        });
    }

    requests
}

pub(super) fn selected_target_addresses_at_block(
    source_plan: &WatchedSourceSelectorPlan,
    block_number: i64,
) -> BTreeSet<String> {
    selected_target_addresses_at_block_with_filter(source_plan, block_number, &|_| true)
}

fn selected_target_addresses_at_block_with_filter(
    source_plan: &WatchedSourceSelectorPlan,
    block_number: i64,
    include_target: &impl Fn(&WatchedBackfillTarget) -> bool,
) -> BTreeSet<String> {
    source_plan
        .selected_targets
        .iter()
        .filter(|target| include_target(target))
        .filter(|target| {
            target.effective_from_block <= block_number && block_number <= target.effective_to_block
        })
        .map(|target| target.address.to_ascii_lowercase())
        .collect()
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct SelectedTargetIntervalIndex {
    intervals_by_address: BTreeMap<String, Vec<(i64, i64)>>,
}

impl SelectedTargetIntervalIndex {
    pub(crate) fn from_source_plan(source_plan: &WatchedSourceSelectorPlan) -> Self {
        let mut intervals_by_address = BTreeMap::<String, Vec<(i64, i64)>>::new();
        for target in &source_plan.selected_targets {
            intervals_by_address
                .entry(target.address.to_ascii_lowercase())
                .or_default()
                .push((target.effective_from_block, target.effective_to_block));
        }

        for intervals in intervals_by_address.values_mut() {
            intervals.sort_unstable();
        }

        Self {
            intervals_by_address,
        }
    }

    pub(crate) fn contains(&self, address: &str, block_number: i64) -> bool {
        self.intervals_by_address
            .get(&address.to_ascii_lowercase())
            .is_some_and(|intervals| {
                intervals.iter().any(|(from_block, to_block)| {
                    *from_block <= block_number && block_number <= *to_block
                })
            })
    }

    pub(crate) fn addresses_for_logs_at_block(
        &self,
        logs: &[ProviderLog],
        block_number: i64,
    ) -> BTreeSet<String> {
        logs.iter()
            .filter(|log| self.contains(&log.address, block_number))
            .map(|log| log.address.to_ascii_lowercase())
            .collect()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct SelectedTargetRangeCursor {
    start_events: Vec<(i64, String)>,
    end_events: Vec<(i64, String)>,
    next_start_index: usize,
    next_end_index: usize,
    active_counts_by_address: BTreeMap<String, usize>,
}

impl SelectedTargetRangeCursor {
    pub(crate) fn from_source_plan(source_plan: &WatchedSourceSelectorPlan) -> Self {
        let mut start_events = Vec::with_capacity(source_plan.selected_targets.len());
        let mut end_events = Vec::with_capacity(source_plan.selected_targets.len());
        for target in &source_plan.selected_targets {
            let address = target.address.to_ascii_lowercase();
            start_events.push((target.effective_from_block, address.clone()));
            end_events.push((target.effective_to_block, address));
        }

        start_events.sort_unstable();
        end_events.sort_unstable();

        Self {
            start_events,
            end_events,
            next_start_index: 0,
            next_end_index: 0,
            active_counts_by_address: BTreeMap::new(),
        }
    }

    pub(crate) fn active_addresses_for_monotonic_range(
        &mut self,
        from_block: i64,
        to_block: i64,
    ) -> Vec<String> {
        while self
            .start_events
            .get(self.next_start_index)
            .is_some_and(|(start_block, _)| *start_block <= to_block)
        {
            let (_, address) = &self.start_events[self.next_start_index];
            *self
                .active_counts_by_address
                .entry(address.clone())
                .or_default() += 1;
            self.next_start_index += 1;
        }

        while self
            .end_events
            .get(self.next_end_index)
            .is_some_and(|(end_block, _)| *end_block < from_block)
        {
            let (_, address) = &self.end_events[self.next_end_index];
            let remove_address = if let Some(count) = self.active_counts_by_address.get_mut(address)
            {
                *count = count.saturating_sub(1);
                *count == 0
            } else {
                false
            };
            if remove_address {
                self.active_counts_by_address.remove(address);
            }
            self.next_end_index += 1;
        }

        self.active_counts_by_address.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use bigname_manifests::{
        WatchedBackfillTarget, WatchedChainPlan, WatchedSourceSelectorKind,
        WatchedSourceSelectorPlan,
    };

    use super::*;

    #[test]
    fn selected_target_interval_index_filters_logs_by_active_block() {
        let source_plan = WatchedSourceSelectorPlan {
            chain: "ethereum-mainnet".to_owned(),
            selector_kind: WatchedSourceSelectorKind::SourceFamily,
            source_family: Some("ens_v1_resolver_l1".to_owned()),
            requested_watched_targets: Vec::new(),
            selected_targets: vec![WatchedBackfillTarget {
                source_family: "ens_v1_resolver_l1".to_owned(),
                contract_instance_id: sqlx::types::Uuid::nil(),
                address: "0x1111111111111111111111111111111111111111".to_owned(),
                effective_from_block: 10,
                effective_to_block: 20,
            }],
            watched_chain_plan: WatchedChainPlan {
                chain: "ethereum-mainnet".to_owned(),
                addresses: Vec::new(),
                manifest_root_entry_count: 0,
                manifest_contract_entry_count: 0,
                discovery_edge_entry_count: 0,
            },
        };
        let index = SelectedTargetIntervalIndex::from_source_plan(&source_plan);
        let logs = vec![ProviderLog {
            block_hash: "0xabc".to_owned(),
            block_number: 15,
            transaction_hash: "0xdef".to_owned(),
            transaction_index: 0,
            log_index: 0,
            address: "0x1111111111111111111111111111111111111111".to_owned(),
            topics: Vec::new(),
            data: "0x".to_owned(),
        }];

        assert!(index.contains("0x1111111111111111111111111111111111111111", 10));
        assert!(index.contains("0x1111111111111111111111111111111111111111", 20));
        assert!(!index.contains("0x1111111111111111111111111111111111111111", 21));
        assert_eq!(
            index.addresses_for_logs_at_block(&logs, 15),
            BTreeSet::from(["0x1111111111111111111111111111111111111111".to_owned()])
        );
    }

    #[test]
    fn selected_target_range_cursor_tracks_active_addresses_monotonically() {
        let source_plan = WatchedSourceSelectorPlan {
            chain: "ethereum-mainnet".to_owned(),
            selector_kind: WatchedSourceSelectorKind::SourceFamily,
            source_family: Some("ens_v1_resolver_l1".to_owned()),
            requested_watched_targets: Vec::new(),
            selected_targets: vec![
                WatchedBackfillTarget {
                    source_family: "ens_v1_resolver_l1".to_owned(),
                    contract_instance_id: sqlx::types::Uuid::nil(),
                    address: "0x1111111111111111111111111111111111111111".to_owned(),
                    effective_from_block: 10,
                    effective_to_block: 20,
                },
                WatchedBackfillTarget {
                    source_family: "ens_v1_resolver_l1".to_owned(),
                    contract_instance_id: sqlx::types::Uuid::nil(),
                    address: "0x2222222222222222222222222222222222222222".to_owned(),
                    effective_from_block: 18,
                    effective_to_block: 30,
                },
            ],
            watched_chain_plan: WatchedChainPlan {
                chain: "ethereum-mainnet".to_owned(),
                addresses: Vec::new(),
                manifest_root_entry_count: 0,
                manifest_contract_entry_count: 0,
                discovery_edge_entry_count: 0,
            },
        };
        let mut cursor = SelectedTargetRangeCursor::from_source_plan(&source_plan);

        assert_eq!(
            cursor.active_addresses_for_monotonic_range(0, 9),
            Vec::<String>::new()
        );
        assert_eq!(
            cursor.active_addresses_for_monotonic_range(10, 17),
            vec!["0x1111111111111111111111111111111111111111".to_owned()]
        );
        assert_eq!(
            cursor.active_addresses_for_monotonic_range(18, 20),
            vec![
                "0x1111111111111111111111111111111111111111".to_owned(),
                "0x2222222222222222222222222222222222222222".to_owned()
            ]
        );
        assert_eq!(
            cursor.active_addresses_for_monotonic_range(21, 30),
            vec!["0x2222222222222222222222222222222222222222".to_owned()]
        );
    }
}
