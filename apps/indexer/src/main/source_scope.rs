use std::collections::BTreeSet;

use bigname_manifests::{WatchedContract, WatchedSourceSelectorPlan};

use crate::ens_v1_resolver::{GENERIC_SOURCE_SCOPE_ADDRESS, SOURCE_FAMILY_ENS_V1_RESOLVER_L1};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SourceScope {
    targets: Vec<SourceScopeTarget>,
}

impl SourceScope {
    pub(crate) fn from_watched_source_plan(
        source_plan: &WatchedSourceSelectorPlan,
        from_block: i64,
        to_block: i64,
    ) -> Self {
        let mut targets = Vec::new();
        if watched_source_plan_uses_generic_resolver_scope(source_plan) {
            targets.push(SourceScopeTarget {
                source_family: SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned(),
                address: GENERIC_SOURCE_SCOPE_ADDRESS.to_owned(),
                from_block,
                to_block,
            });
        }

        targets.extend(source_plan.selected_targets.iter().filter_map(|target| {
            if target.source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1 {
                return None;
            }
            let effective_from_block = target.effective_from_block.max(from_block);
            let effective_to_block = target.effective_to_block.min(to_block);
            if effective_from_block > effective_to_block {
                return None;
            }
            Some(SourceScopeTarget {
                source_family: target.source_family.clone(),
                address: target.address.to_ascii_lowercase(),
                from_block: effective_from_block,
                to_block: effective_to_block,
            })
        }));

        Self { targets }
    }

    pub(crate) fn from_watched_contracts(
        watched_contracts: &[WatchedContract],
        chain: &str,
        from_block: i64,
        to_block: i64,
        include_generic_resolver_scope: bool,
    ) -> Self {
        let mut targets = BTreeSet::new();
        if include_generic_resolver_scope {
            targets.insert(SourceScopeTarget {
                source_family: SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned(),
                address: GENERIC_SOURCE_SCOPE_ADDRESS.to_owned(),
                from_block,
                to_block,
            });
        }

        for contract in watched_contracts {
            if contract.chain != chain {
                continue;
            }
            if include_generic_resolver_scope
                && contract.source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1
            {
                continue;
            }

            let effective_from_block = contract
                .active_from_block_number
                .map_or(from_block, |active_from| active_from.max(from_block));
            let effective_to_block = contract
                .active_to_block_number
                .map_or(to_block, |active_to| active_to.min(to_block));
            if effective_from_block > effective_to_block {
                continue;
            }

            targets.insert(SourceScopeTarget {
                source_family: contract.source_family.clone(),
                address: contract.address.to_ascii_lowercase(),
                from_block: effective_from_block,
                to_block: effective_to_block,
            });
        }

        Self {
            targets: targets.into_iter().collect(),
        }
    }

    pub(crate) fn into_targets(self) -> Vec<SourceScopeTarget> {
        self.targets
    }

    pub(crate) fn adapter_sync_scope(&self) -> Vec<(String, String, i64, i64)> {
        self.targets
            .iter()
            .map(|target| {
                (
                    target.source_family.clone(),
                    target.address.clone(),
                    target.from_block,
                    target.to_block,
                )
            })
            .collect()
    }
}

pub(crate) fn watched_source_plan_uses_generic_resolver_scope(
    source_plan: &WatchedSourceSelectorPlan,
) -> bool {
    source_plan.source_family.as_deref() == Some(SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
        || source_plan
            .selected_targets
            .iter()
            .any(|target| target.source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct SourceScopeTarget {
    pub(crate) source_family: String,
    pub(crate) address: String,
    pub(crate) from_block: i64,
    pub(crate) to_block: i64,
}

#[cfg(test)]
mod tests {
    use bigname_manifests::{
        WatchedBackfillTarget, WatchedChainPlan, WatchedContract, WatchedContractSource,
        WatchedSourceSelectorKind, WatchedSourceSelectorPlan,
    };

    use super::*;

    #[test]
    fn watched_source_plan_uses_one_shape_for_adapter_and_replay_scope() {
        let source_plan = WatchedSourceSelectorPlan {
            chain: "ethereum-mainnet".to_owned(),
            selector_kind: WatchedSourceSelectorKind::WatchedTargetSet,
            source_family: None,
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
                    source_family: "ens_v1_registry_l1".to_owned(),
                    contract_instance_id: sqlx::types::Uuid::nil(),
                    address: "0xABCDEFabcdefABCDEFabcdefabcdefABCDEFabcd".to_owned(),
                    effective_from_block: 12,
                    effective_to_block: 18,
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

        let scope = SourceScope::from_watched_source_plan(&source_plan, 10, 30);

        assert_eq!(
            scope.adapter_sync_scope(),
            vec![
                ("ens_v1_resolver_l1".to_owned(), "*".to_owned(), 10, 30),
                (
                    "ens_v1_registry_l1".to_owned(),
                    "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned(),
                    12,
                    18
                ),
            ]
        );
    }

    #[test]
    fn watched_source_plan_scope_clips_targets_to_requested_range() {
        let source_plan = WatchedSourceSelectorPlan {
            chain: "base-mainnet".to_owned(),
            selector_kind: WatchedSourceSelectorKind::WatchedTargetSet,
            source_family: None,
            requested_watched_targets: Vec::new(),
            selected_targets: vec![
                WatchedBackfillTarget {
                    source_family: "basenames_base_registry".to_owned(),
                    contract_instance_id: sqlx::types::Uuid::nil(),
                    address: "0xABCDEFabcdefABCDEFabcdefabcdefABCDEFabcd".to_owned(),
                    effective_from_block: 10,
                    effective_to_block: 30,
                },
                WatchedBackfillTarget {
                    source_family: "basenames_base_registry".to_owned(),
                    contract_instance_id: sqlx::types::Uuid::nil(),
                    address: "0x2222222222222222222222222222222222222222".to_owned(),
                    effective_from_block: 1,
                    effective_to_block: 9,
                },
            ],
            watched_chain_plan: WatchedChainPlan {
                chain: "base-mainnet".to_owned(),
                addresses: Vec::new(),
                manifest_root_entry_count: 0,
                manifest_contract_entry_count: 0,
                discovery_edge_entry_count: 0,
            },
        };

        let scope = SourceScope::from_watched_source_plan(&source_plan, 20, 40);

        assert_eq!(
            scope.adapter_sync_scope(),
            vec![(
                "basenames_base_registry".to_owned(),
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned(),
                20,
                30
            )]
        );
    }

    #[test]
    fn watched_contract_scope_clips_ranges_and_adds_generic_resolver_once() {
        let watched_contracts = vec![
            WatchedContract {
                chain: "ethereum-mainnet".to_owned(),
                source_family: "ens_v1_registry_l1".to_owned(),
                address: "0xABCDEFabcdefABCDEFabcdefabcdefABCDEFabcd".to_owned(),
                contract_instance_id: sqlx::types::Uuid::nil(),
                source: WatchedContractSource::ManifestRoot,
                source_manifest_id: Some(1),
                active_from_block_number: Some(12),
                active_to_block_number: Some(18),
            },
            WatchedContract {
                chain: "ethereum-mainnet".to_owned(),
                source_family: "ens_v1_resolver_l1".to_owned(),
                address: "0x2222222222222222222222222222222222222222".to_owned(),
                contract_instance_id: sqlx::types::Uuid::nil(),
                source: WatchedContractSource::DiscoveryEdge,
                source_manifest_id: Some(1),
                active_from_block_number: Some(13),
                active_to_block_number: Some(17),
            },
            WatchedContract {
                chain: "base-mainnet".to_owned(),
                source_family: "basenames_registry_l2".to_owned(),
                address: "0x1111111111111111111111111111111111111111".to_owned(),
                contract_instance_id: sqlx::types::Uuid::nil(),
                source: WatchedContractSource::ManifestRoot,
                source_manifest_id: Some(2),
                active_from_block_number: Some(10),
                active_to_block_number: Some(20),
            },
        ];

        let scope = SourceScope::from_watched_contracts(
            &watched_contracts,
            "ethereum-mainnet",
            10,
            30,
            true,
        );

        assert_eq!(
            scope.adapter_sync_scope(),
            vec![
                (
                    "ens_v1_registry_l1".to_owned(),
                    "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_owned(),
                    12,
                    18
                ),
                ("ens_v1_resolver_l1".to_owned(), "*".to_owned(), 10, 30),
            ]
        );
    }
}
