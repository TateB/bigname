use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, bail};
use serde_json::Value;
use uuid::Uuid;

use super::{
    common::contract_id,
    manifest::{self, ManifestEvent, ManifestProvenance, ManifestSource},
    model::{AddressAdmissionInput, DiscoveryRuleInput, ManifestInput, RawLogInput},
    protocol,
    seam::MIGRATION_REGISTRY_ASSOCIATION_KIND,
};

#[derive(Clone, Debug)]
pub(super) struct Selected {
    pub source: ManifestSource,
    pub event: ManifestEvent,
    pub contract_instance_id: Uuid,
    pub emitter_role: Option<String>,
    pub match_all: bool,
    pub manifest_declared_emitter: bool,
}

pub(super) struct Catalog {
    manifests: Vec<ManifestSource>,
    by_id: BTreeMap<i64, usize>,
    provenance_by_id: BTreeMap<i64, ManifestProvenance>,
    rules: Vec<DiscoveryRuleInput>,
    admissions: Vec<AddressAdmissionInput>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct MigrationRegistryCorrelation {
    pub id: String,
    pub evidence: Vec<Value>,
}

impl Catalog {
    pub(super) fn new(
        manifests: Vec<ManifestInput>,
        rules: Vec<DiscoveryRuleInput>,
        admissions: Vec<AddressAdmissionInput>,
    ) -> anyhow::Result<Self> {
        let provenance = manifests.clone();
        Self::new_with_provenance(manifests, provenance, rules, admissions)
    }

    pub(super) fn new_with_provenance(
        manifests: Vec<ManifestInput>,
        provenance: Vec<ManifestInput>,
        rules: Vec<DiscoveryRuleInput>,
        admissions: Vec<AddressAdmissionInput>,
    ) -> anyhow::Result<Self> {
        let mut provenance_by_id = BTreeMap::new();
        for source in provenance.iter().chain(manifests.iter()) {
            let candidate = ManifestProvenance::from_input(source);
            if let Some(previous) = provenance_by_id.insert(source.manifest_id, candidate.clone())
                && previous != candidate
            {
                bail!(
                    "manifest {} has conflicting provenance metadata",
                    source.manifest_id
                );
            }
        }
        let mut manifests = manifests
            .into_iter()
            .map(manifest::decode)
            .collect::<anyhow::Result<Vec<_>>>()?;
        for source in &manifests {
            protocol::validate_manifest(source, &rules)?;
        }
        manifests.sort_by_key(|source| source.manifest_id);
        if manifests.is_empty() {
            bail!("interpretation requires at least one active manifest");
        }
        let by_id = manifests
            .iter()
            .enumerate()
            .map(|(index, source)| (source.manifest_id, index))
            .collect();
        Ok(Self {
            manifests,
            by_id,
            provenance_by_id,
            rules,
            admissions,
        })
    }

    pub(super) fn v2_suffix_anchors(&self) -> Vec<(String, String, Vec<String>)> {
        let mut anchors = BTreeMap::new();
        for admission in self
            .admissions
            .iter()
            .filter(|admission| admission.discovery_edge_kind.is_none())
        {
            let Some(source) = admission
                .source_manifest_id
                .and_then(|manifest_id| self.source(manifest_id))
            else {
                continue;
            };
            let suffix = match source.source_family.as_str() {
                "ens_v2_root_l1" => Vec::new(),
                "ens_v2_registry_l1" => vec!["eth".to_owned()],
                _ => continue,
            };
            anchors.insert(
                admission.address.to_ascii_lowercase(),
                (source.namespace.clone(), suffix),
            );
        }
        anchors
            .into_iter()
            .map(|(address, (namespace, suffix))| (address, namespace, suffix))
            .collect()
    }

    pub(super) fn select(&self, raw: &RawLogInput) -> anyhow::Result<Option<Selected>> {
        let Some(topic0) = raw.topics.first() else {
            return Ok(None);
        };
        let manifest_declared_emitter = self
            .admissions
            .iter()
            .any(|admission| admission.discovery_edge_kind.is_none() && applies(admission, raw));
        let declared_namespaces = self
            .admissions
            .iter()
            .filter(|admission| admission.discovery_edge_kind.is_none() && applies(admission, raw))
            .filter_map(|admission| admission.source_manifest_id)
            .filter_map(|manifest_id| self.source(manifest_id))
            .map(|source| source.namespace.as_str())
            .collect::<BTreeSet<_>>();
        let announcement_namespaces = self
            .admissions
            .iter()
            .filter(|admission| {
                admission.discovery_edge_kind.as_deref()
                    != Some(MIGRATION_REGISTRY_ASSOCIATION_KIND)
                    && applies(admission, raw)
                    && admission.discovery_edge_kind.as_deref() == Some("registry_announcement")
            })
            .filter_map(|admission| admission.source_manifest_id)
            .filter_map(|manifest_id| self.source(manifest_id))
            .map(|source| source.namespace.as_str())
            .collect::<BTreeSet<_>>();
        let resolver_namespaces = self
            .admissions
            .iter()
            .filter(|admission| {
                applies(admission, raw)
                    && admission.discovery_edge_kind.as_deref() == Some("resolver")
            })
            .filter_map(|admission| admission.source_manifest_id)
            .filter_map(|manifest_id| self.source(manifest_id))
            .map(|source| source.namespace.as_str())
            .collect::<BTreeSet<_>>();
        let mut candidates = Vec::new();
        for admission in self.admissions.iter().filter(|admission| {
            admission.discovery_edge_kind.as_deref() != Some(MIGRATION_REGISTRY_ASSOCIATION_KIND)
                && applies(admission, raw)
        }) {
            let Some(manifest_id) = admission.source_manifest_id else {
                continue;
            };
            let source = self
                .source(manifest_id)
                .with_context(|| format!("admission references inactive manifest {manifest_id}"))?;
            let rank = match admission.discovery_edge_kind.as_deref() {
                Some("registry_announcement") => 1,
                None if announcement_namespaces.contains(source.namespace.as_str()) => 0,
                None if resolver_namespaces.contains(source.namespace.as_str()) => 0,
                None if !announcement_namespaces.is_empty() => 2,
                None if !resolver_namespaces.is_empty() => 2,
                Some("resolver") if declared_namespaces.contains(source.namespace.as_str()) => 1,
                // Protocol-specific discovery, including resolver admission, is direct authority
                // rather than the declaration fallback ranked around registry announcements.
                _ => 0,
            };
            let target_family = inferred_family(
                &source.source_family,
                admission.discovery_edge_kind.as_deref(),
            );
            if let Some(target_family) =
                target_family.filter(|family| *family != source.source_family)
            {
                for inferred in self.manifests.iter().filter(|candidate| {
                    candidate.namespace == source.namespace
                        && candidate.chain_id == source.chain_id
                        && candidate.deployment_label == source.deployment_label
                        && candidate.source_family == target_family
                }) {
                    self.push_candidates(
                        &mut candidates,
                        rank,
                        inferred,
                        topic0,
                        None,
                        admission.discovery_edge_kind.as_deref(),
                        admission.contract_instance_id,
                        manifest_declared_emitter,
                    );
                }
            } else {
                self.push_candidates(
                    &mut candidates,
                    rank,
                    source,
                    topic0,
                    admission.role.as_deref(),
                    admission.discovery_edge_kind.as_deref(),
                    admission.contract_instance_id,
                    manifest_declared_emitter,
                );
            }
        }

        for source in &self.manifests {
            for event in source.events.iter().filter(|event| {
                event.topic0.eq_ignore_ascii_case(topic0) && self.is_match_all(source, event)
            }) {
                let contract_instance_id = self
                    .admissions
                    .iter()
                    .filter(|admission| applies(admission, raw))
                    .map(|admission| admission.contract_instance_id)
                    .next()
                    .unwrap_or_else(|| contract_id(&raw.chain_id, &raw.emitting_address));
                candidates.push((
                    3,
                    0,
                    Selected {
                        source: source.clone(),
                        event: event.clone(),
                        contract_instance_id,
                        emitter_role: None,
                        match_all: true,
                        manifest_declared_emitter,
                    },
                ));
            }
        }
        select_unambiguous(raw, candidates)
    }

    pub(super) fn rule(
        &self,
        manifest_id: i64,
        edge_kind: &str,
        emitter_role: Option<&str>,
    ) -> Option<&DiscoveryRuleInput> {
        self.rules.iter().find(|rule| {
            rule.manifest_id == manifest_id
                && rule.edge_kind == edge_kind
                && rule
                    .from_role
                    .as_deref()
                    .is_none_or(|required| emitter_role.is_none_or(|actual| required == actual))
        })
    }

    pub(super) fn admit(&mut self, admission: AddressAdmissionInput) {
        if let (Some(edge_kind), Some(from), Some(observation_key)) = (
            admission.discovery_edge_kind.as_deref(),
            admission.discovery_from_contract_instance_id,
            admission.discovery_observation_key.as_deref(),
        ) {
            self.retire(edge_kind, from, observation_key);
        }
        self.admissions.push(admission);
    }

    pub(super) fn retire(&mut self, edge_kind: &str, from: Uuid, observation_key: &str) {
        self.admissions.retain(|existing| {
            existing.discovery_edge_kind.as_deref() != Some(edge_kind)
                || existing.discovery_from_contract_instance_id != Some(from)
                || existing.discovery_observation_key.as_deref() != Some(observation_key)
        });
    }

    pub(super) fn contract_instance_for_address(
        &self,
        address: &str,
        block_number: i64,
    ) -> anyhow::Result<Option<Uuid>> {
        let mut instances = self
            .admissions
            .iter()
            .filter(|admission| {
                admission.address.eq_ignore_ascii_case(address)
                    && admission
                        .active_from_block
                        .is_none_or(|from| block_number >= from)
                    && admission
                        .active_to_block
                        .is_none_or(|to| block_number <= to)
            })
            .map(|admission| admission.contract_instance_id)
            .collect::<BTreeSet<_>>();
        if instances.len() > 1 {
            bail!("address {address} has more than one active contract identity");
        }
        Ok(instances.pop_first())
    }

    pub(super) fn source(&self, manifest_id: i64) -> Option<&ManifestSource> {
        self.by_id
            .get(&manifest_id)
            .and_then(|index| self.manifests.get(*index))
    }

    pub(super) fn provenance(&self, manifest_id: i64) -> Option<&ManifestProvenance> {
        self.provenance_by_id.get(&manifest_id)
    }

    pub(super) fn provenance_ids(&self) -> imbl::OrdSet<i64> {
        self.provenance_by_id.keys().copied().collect()
    }

    pub(super) fn source_for_family(&self, source_family: &str) -> Option<&ManifestSource> {
        self.manifests
            .iter()
            .find(|source| source.source_family == source_family)
    }

    pub(super) fn correlation_address(&self, source_family: &str, name: &str) -> Option<&str> {
        self.source_for_family(source_family)?
            .correlation_addresses
            .get(name)
            .map(String::as_str)
    }

    pub(super) fn source_for_contract_instance(
        &self,
        contract_instance_id: Uuid,
    ) -> Option<&ManifestSource> {
        self.admissions
            .iter()
            .filter(|admission| admission.contract_instance_id == contract_instance_id)
            .filter_map(|admission| admission.source_manifest_id)
            .find_map(|manifest_id| self.source(manifest_id))
    }

    pub(super) fn declared_address_for_role(
        &self,
        source_family: &str,
        role: &str,
    ) -> Option<&str> {
        self.admissions.iter().find_map(|admission| {
            (admission.role.as_deref() == Some(role)
                && admission
                    .source_manifest_id
                    .and_then(|manifest_id| self.source(manifest_id))
                    .is_some_and(|source| source.source_family == source_family))
            .then_some(admission.address.as_str())
        })
    }

    pub(super) fn declared_start_block_for_role(
        &self,
        source_family: &str,
        role: &str,
    ) -> Option<i64> {
        self.admissions.iter().find_map(|admission| {
            (admission.role.as_deref() == Some(role)
                && admission
                    .source_manifest_id
                    .and_then(|manifest_id| self.source(manifest_id))
                    .is_some_and(|source| source.source_family == source_family))
            .then_some(admission.active_from_block)
            .flatten()
        })
    }

    pub(super) fn migration_registry_correlations(
        &self,
        address: &str,
        block_number: i64,
    ) -> Vec<MigrationRegistryCorrelation> {
        self.admissions
            .iter()
            .filter(|admission| {
                admission.discovery_edge_kind.as_deref()
                    == Some(MIGRATION_REGISTRY_ASSOCIATION_KIND)
                    && admission.address.eq_ignore_ascii_case(address)
                    && admission
                        .active_from_block
                        .is_none_or(|from| block_number >= from)
                    && admission
                        .active_to_block
                        .is_none_or(|to| block_number <= to)
            })
            .filter_map(|admission| admission.discovery_observation_key.as_deref())
            .filter_map(|encoded| {
                let parsed = serde_json::from_str::<Value>(encoded).ok();
                let id = parsed
                    .as_ref()
                    .and_then(|value| value.get("id"))
                    .and_then(Value::as_str)
                    .unwrap_or(encoded)
                    .to_owned();
                let evidence = parsed
                    .as_ref()
                    .and_then(|value| value.get("evidence"))
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                (!id.trim().is_empty()).then_some(MigrationRegistryCorrelation { id, evidence })
            })
            .map(|correlation| (correlation.id.clone(), correlation))
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .collect()
    }

    fn is_match_all(&self, source: &ManifestSource, event: &ManifestEvent) -> bool {
        protocol::is_match_all(source, event, &self.rules)
    }

    #[allow(clippy::too_many_arguments)]
    fn push_candidates(
        &self,
        output: &mut Vec<(u8, u8, Selected)>,
        rank: u8,
        source: &ManifestSource,
        topic0: &str,
        role: Option<&str>,
        discovery_kind: Option<&str>,
        instance: Uuid,
        manifest_declared_emitter: bool,
    ) {
        for event in source.events.iter().filter(|event| {
            event.topic0.eq_ignore_ascii_case(topic0)
                && (event.emitter_roles.is_empty()
                    || discovery_kind == Some("registry_announcement")
                    || role.is_some_and(|role| event.emitter_roles.iter().any(|item| item == role)))
        }) {
            let match_all = self.is_match_all(source, event);
            let required_rule = required_discovery_rule(source, event);
            let discovery_authority_rank =
                u8::from(required_rule.is_some_and(|edge_kind| {
                    self.rule(source.manifest_id, edge_kind, role).is_none()
                }));
            let emitter_role =
                if protocol::role_insensitivity_justification(&source.source_family, &event.name)
                    .is_some()
                {
                    None
                } else {
                    role.map(str::to_owned)
                };
            output.push((
                rank,
                discovery_authority_rank,
                Selected {
                    source: source.clone(),
                    event: event.clone(),
                    contract_instance_id: instance,
                    emitter_role,
                    match_all,
                    manifest_declared_emitter,
                },
            ));
        }
    }
}

fn applies(admission: &AddressAdmissionInput, raw: &RawLogInput) -> bool {
    admission
        .address
        .eq_ignore_ascii_case(&raw.emitting_address)
        && admission
            .active_from_block
            .is_none_or(|from| raw.block_number >= from)
        && admission
            .active_to_block
            .is_none_or(|to| raw.block_number <= to)
}

fn required_discovery_rule(source: &ManifestSource, event: &ManifestEvent) -> Option<&'static str> {
    match (source.source_family.as_str(), event.name.as_str()) {
        ("ens_v2_registry_l1", "RegistryCreated") => Some("registry_announcement"),
        ("ens_v2_registry_l1" | "ens_v2_root_l1", "SubregistryUpdated") => Some("subregistry"),
        ("ens_v2_registry_l1" | "ens_v2_root_l1", "ResolverUpdated") => Some("resolver"),
        _ => None,
    }
}

fn inferred_family(source_family: &str, edge_kind: Option<&str>) -> Option<&'static str> {
    match (edge_kind, source_family) {
        (Some("resolver"), "ens_v2_registry_l1" | "ens_v2_root_l1") => Some("ens_v2_resolver_l1"),
        (Some("resolver"), "basenames_base_registry") => Some("basenames_base_resolver"),
        (Some("registry_announcement"), "ens_v2_registry_l1") => Some("ens_v2_registry_l1"),
        _ => None,
    }
}

fn select_unambiguous(
    raw: &RawLogInput,
    mut candidates: Vec<(u8, u8, Selected)>,
) -> anyhow::Result<Option<Selected>> {
    let Some(rank) = candidates.iter().map(|candidate| candidate.0).min() else {
        return Ok(None);
    };
    candidates.retain(|candidate| candidate.0 == rank);
    let discovery_authority_rank = candidates
        .iter()
        .map(|candidate| candidate.1)
        .min()
        .expect("at least one candidate remains at the selected rank");
    candidates.retain(|candidate| candidate.1 == discovery_authority_rank);
    candidates
        .sort_by(|left, right| candidate_identity(&left.2).cmp(&candidate_identity(&right.2)));
    candidates.dedup_by(|left, right| {
        left.2.source.manifest_id == right.2.source.manifest_id
            && left.2.event.signature == right.2.event.signature
            && left.2.contract_instance_id == right.2.contract_instance_id
            && left.2.emitter_role == right.2.emitter_role
            && left.2.match_all == right.2.match_all
            && left.2.manifest_declared_emitter == right.2.manifest_declared_emitter
    });
    if candidates.len() > 1 {
        let sources = candidates
            .iter()
            .map(|candidate| {
                format!(
                    "{}:{} (role={}, instance={})",
                    candidate.2.source.source_family,
                    candidate.2.event.signature,
                    candidate.2.emitter_role.as_deref().unwrap_or("none"),
                    candidate.2.contract_instance_id,
                )
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .join(", ");
        bail!(
            "raw log {}:{} has ambiguous admitted adapters: {sources}",
            raw.block_hash,
            raw.log_index
        );
    }
    Ok(candidates.pop().map(|candidate| candidate.2))
}

fn candidate_identity(selected: &Selected) -> (i64, &str, Uuid, Option<&str>, bool, bool) {
    (
        selected.source.manifest_id,
        selected.event.signature.as_str(),
        selected.contract_instance_id,
        selected.emitter_role.as_deref(),
        selected.match_all,
        selected.manifest_declared_emitter,
    )
}
