use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use super::types::{
    AdmittedDiscoveryEdge, DiscoveryCandidate, StoredActiveContract, StoredActiveRoot,
    StoredDiscoveryRule,
};
use crate::normalize_address;

pub struct DiscoveryAdmissionState {
    pub active_manifest_count: usize,
    pub active_root_count: usize,
    pub active_contract_count: usize,
    pub active_rule_count: usize,
    pub(super) active_roots: Vec<StoredActiveRoot>,
    pub(super) active_root_manifest_ids: HashSet<i64>,
    pub(super) active_contracts: Vec<StoredActiveContract>,
    pub(super) known_contract_instances_by_address: HashMap<(String, String), Uuid>,
    pub(super) rules_by_manifest_id: HashMap<i64, Vec<StoredDiscoveryRule>>,
}

impl DiscoveryAdmissionState {
    pub fn has_authoritative_address(&self, chain: &str, address: &str) -> bool {
        let normalized_address = normalize_address(address);
        let key = (chain.to_owned(), normalized_address);

        self.active_roots
            .iter()
            .any(|root| root.chain == key.0 && root.address == key.1)
            || self
                .active_contracts
                .iter()
                .any(|contract| contract.chain == key.0 && contract.address == key.1)
    }

    pub fn admit_candidate(
        &self,
        candidate: &DiscoveryCandidate<'_>,
    ) -> Vec<AdmittedDiscoveryEdge> {
        self.admit_candidate_against_contracts(&self.active_contracts, candidate)
    }

    pub(super) fn admit_candidate_against_contracts(
        &self,
        active_contracts: &[StoredActiveContract],
        candidate: &DiscoveryCandidate<'_>,
    ) -> Vec<AdmittedDiscoveryEdge> {
        let normalized_from_address = normalize_address(candidate.from_address);
        let normalized_to_address = normalize_address(candidate.to_address);
        self.admit_candidate_against_matching_contracts(
            active_contracts.iter().filter(|contract| {
                contract.chain == candidate.chain && contract.address == normalized_from_address
            }),
            candidate,
            &normalized_from_address,
            &normalized_to_address,
        )
    }

    pub(super) fn admit_candidate_against_contract_lookup(
        &self,
        active_contracts_by_address: &HashMap<(String, String), Vec<StoredActiveContract>>,
        candidate: &DiscoveryCandidate<'_>,
    ) -> Vec<AdmittedDiscoveryEdge> {
        let normalized_from_address = normalize_address(candidate.from_address);
        let normalized_to_address = normalize_address(candidate.to_address);
        let key = (candidate.chain.to_owned(), normalized_from_address.clone());
        let Some(active_contracts) = active_contracts_by_address.get(&key) else {
            return Vec::new();
        };

        self.admit_candidate_against_matching_contracts(
            active_contracts.iter(),
            candidate,
            &normalized_from_address,
            &normalized_to_address,
        )
    }

    fn admit_candidate_against_matching_contracts<'a>(
        &self,
        active_contracts: impl Iterator<Item = &'a StoredActiveContract>,
        candidate: &DiscoveryCandidate<'_>,
        normalized_from_address: &str,
        normalized_to_address: &str,
    ) -> Vec<AdmittedDiscoveryEdge> {
        let mut admitted_edges = HashSet::new();

        for contract in active_contracts {
            if !self
                .active_root_manifest_ids
                .contains(&contract.manifest_id)
            {
                continue;
            }

            let Some(rules) = self.rules_by_manifest_id.get(&contract.manifest_id) else {
                continue;
            };

            for rule in rules.iter().filter(|rule| {
                rule.edge_kind == candidate.edge_kind && rule.from_role == contract.role
            }) {
                admitted_edges.insert(AdmittedDiscoveryEdge {
                    source_manifest_id: contract.manifest_id,
                    chain: candidate.chain.to_owned(),
                    from_contract_instance_id: contract.contract_instance_id,
                    to_contract_instance_id: self
                        .known_contract_instances_by_address
                        .get(&(candidate.chain.to_owned(), normalized_to_address.to_owned()))
                        .copied(),
                    from_address: normalized_from_address.to_owned(),
                    to_address: normalized_to_address.to_owned(),
                    edge_kind: candidate.edge_kind.to_owned(),
                    discovery_source: candidate.discovery_source.to_owned(),
                    admission: rule.admission.clone(),
                    from_role: contract.role.clone(),
                });
            }
        }

        let mut admitted_edges = admitted_edges.into_iter().collect::<Vec<_>>();
        admitted_edges.sort_by(|left, right| {
            (
                left.source_manifest_id,
                left.chain.as_str(),
                left.from_contract_instance_id,
                left.to_contract_instance_id,
                left.to_address.as_str(),
                left.edge_kind.as_str(),
                left.discovery_source.as_str(),
                left.admission.as_str(),
                left.from_role.as_str(),
            )
                .cmp(&(
                    right.source_manifest_id,
                    right.chain.as_str(),
                    right.from_contract_instance_id,
                    right.to_contract_instance_id,
                    right.to_address.as_str(),
                    right.edge_kind.as_str(),
                    right.discovery_source.as_str(),
                    right.admission.as_str(),
                    right.from_role.as_str(),
                ))
        });
        admitted_edges
    }
}
