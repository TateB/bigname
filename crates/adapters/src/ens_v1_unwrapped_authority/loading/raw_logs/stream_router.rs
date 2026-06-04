use std::collections::{BTreeSet, HashMap, HashSet};

use anyhow::Result;

use super::super::super::*;

pub(in crate::ens_v1_unwrapped_authority) struct AuthorityRawLogStreamSourceRouter<'a> {
    watched_emitters: Vec<RoutedActiveEmitter>,
    watched_emitters_by_address: HashMap<String, Vec<usize>>,
    watched_topic0s: HashSet<String>,
    generic_resolver_event_sources: &'a [GenericResolverEventSource],
    generic_topic0s: HashSet<String>,
}

#[derive(Clone, Copy)]
pub(super) enum AuthorityRawLogStreamSource<'a> {
    Watched(&'a RoutedActiveEmitter),
    Generic(&'a GenericResolverEventSource),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RoutedEmitterKey {
    source_manifest_id: i64,
    namespace: String,
    source_family: String,
    manifest_version: i64,
    normalizer_version: String,
    contract_role: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RoutedActiveEmitter {
    source_manifest_id: i64,
    namespace: String,
    source_family: String,
    manifest_version: i64,
    normalizer_version: String,
    contract_role: Option<String>,
    active_from_block_number: i64,
    active_to_block_number: i64,
}

impl<'a> AuthorityRawLogStreamSourceRouter<'a> {
    pub(in crate::ens_v1_unwrapped_authority) fn new(
        active_emitters: &'a [ActiveEmitter],
        generic_resolver_event_sources: &'a [GenericResolverEventSource],
        event_topics: &AuthorityEventTopics,
    ) -> Result<Self> {
        let mut source_emitters_by_address = HashMap::<&str, Vec<&ActiveEmitter>>::new();
        for emitter in active_emitters {
            source_emitters_by_address
                .entry(emitter.address.as_str())
                .or_default()
                .push(emitter);
        }

        let mut watched_emitters = Vec::<RoutedActiveEmitter>::new();
        let mut watched_emitters_by_address = HashMap::<String, Vec<usize>>::new();
        for (address, emitters) in source_emitters_by_address {
            append_routed_emitters_for_address(
                address,
                &emitters,
                &mut watched_emitters,
                &mut watched_emitters_by_address,
            );
        }

        let watched_topic0s = event_topics
            .authority_replay_event_topic0s()
            .into_iter()
            .map(|topic0| topic0.to_ascii_lowercase())
            .collect::<HashSet<_>>();

        let generic_topic0s = if generic_resolver_event_sources.is_empty() {
            HashSet::new()
        } else {
            event_topics
                .ens_resolver_event_topic0s()?
                .into_iter()
                .map(|topic0| topic0.to_ascii_lowercase())
                .collect::<HashSet<_>>()
        };

        Ok(Self {
            watched_emitters,
            watched_emitters_by_address,
            watched_topic0s,
            generic_resolver_event_sources,
            generic_topic0s,
        })
    }

    pub(super) fn source_candidates<'router>(
        &'router self,
        emitting_address: &str,
        topic0: &str,
        block_number: i64,
    ) -> Vec<AuthorityRawLogStreamSource<'router>> {
        let mut candidates = Vec::new();
        if self.watched_topic0s.contains(topic0) {
            if let Some(emitter_indexes) = self.watched_emitters_by_address.get(emitting_address) {
                candidates.extend(
                    emitter_indexes
                        .iter()
                        .copied()
                        .map(|index| &self.watched_emitters[index])
                        .filter(|emitter| emitter_active_at_block(emitter, block_number))
                        .map(AuthorityRawLogStreamSource::Watched),
                );
            }
        }
        if self.generic_topic0s.contains(topic0) {
            candidates.extend(
                self.generic_resolver_event_sources
                    .iter()
                    .filter(|source| generic_source_active_at_block(source, block_number))
                    .map(AuthorityRawLogStreamSource::Generic),
            );
        }

        candidates.sort_by(|left, right| {
            left.source_family()
                .cmp(right.source_family())
                .then(left.source_manifest_id().cmp(&right.source_manifest_id()))
        });
        let mut seen_source_families = HashSet::<String>::new();
        candidates
            .into_iter()
            .filter(|candidate| seen_source_families.insert(candidate.source_family().to_owned()))
            .collect()
    }

    pub(super) fn topic0_filters(&self) -> Vec<String> {
        self.watched_topic0s
            .iter()
            .chain(self.generic_topic0s.iter())
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}

fn append_routed_emitters_for_address(
    address: &str,
    emitters: &[&ActiveEmitter],
    watched_emitters: &mut Vec<RoutedActiveEmitter>,
    watched_emitters_by_address: &mut HashMap<String, Vec<usize>>,
) {
    let mut intervals_by_key = HashMap::<RoutedEmitterKey, Vec<(i64, i64)>>::new();
    for emitter in emitters {
        intervals_by_key
            .entry(RoutedEmitterKey::from_emitter(emitter))
            .or_default()
            .push(emitter_interval(emitter));
    }

    let mut address_indexes = Vec::new();
    for (key, mut intervals) in intervals_by_key {
        intervals.sort_unstable();
        let mut merged = Vec::<(i64, i64)>::new();
        for (from_block, to_block) in intervals {
            if let Some((_, last_to_block)) = merged.last_mut()
                && from_block <= last_to_block.saturating_add(1)
            {
                *last_to_block = (*last_to_block).max(to_block);
                continue;
            }
            merged.push((from_block, to_block));
        }

        for (from_block, to_block) in merged {
            let index = watched_emitters.len();
            watched_emitters.push(RoutedActiveEmitter {
                source_manifest_id: key.source_manifest_id,
                namespace: key.namespace.clone(),
                source_family: key.source_family.clone(),
                manifest_version: key.manifest_version,
                normalizer_version: key.normalizer_version.clone(),
                contract_role: key.contract_role.clone(),
                active_from_block_number: from_block,
                active_to_block_number: to_block,
            });
            address_indexes.push(index);
        }
    }

    address_indexes.sort_by(|left, right| {
        let left = &watched_emitters[*left];
        let right = &watched_emitters[*right];
        left.source_family
            .cmp(&right.source_family)
            .then(left.source_manifest_id.cmp(&right.source_manifest_id))
            .then(
                left.active_from_block_number
                    .cmp(&right.active_from_block_number),
            )
            .then(
                left.active_to_block_number
                    .cmp(&right.active_to_block_number),
            )
    });
    watched_emitters_by_address.insert(address.to_owned(), address_indexes);
}

impl RoutedEmitterKey {
    fn from_emitter(emitter: &ActiveEmitter) -> Self {
        Self {
            source_manifest_id: emitter.source_manifest_id,
            namespace: emitter.namespace.clone(),
            source_family: emitter.source_family.clone(),
            manifest_version: emitter.manifest_version,
            normalizer_version: emitter.normalizer_version.clone(),
            contract_role: emitter.contract_role.clone(),
        }
    }
}

fn emitter_interval(emitter: &ActiveEmitter) -> (i64, i64) {
    (
        emitter.active_from_block_number.unwrap_or(0),
        emitter.active_to_block_number.unwrap_or(i64::MAX),
    )
}

impl AuthorityRawLogStreamSource<'_> {
    pub(super) fn source_manifest_id(&self) -> i64 {
        match self {
            Self::Watched(emitter) => emitter.source_manifest_id,
            Self::Generic(source) => source.source_manifest_id,
        }
    }

    pub(super) fn namespace(&self) -> &str {
        match self {
            Self::Watched(emitter) => &emitter.namespace,
            Self::Generic(source) => &source.namespace,
        }
    }

    pub(super) fn source_family(&self) -> &str {
        match self {
            Self::Watched(emitter) => &emitter.source_family,
            Self::Generic(source) => &source.source_family,
        }
    }

    pub(super) fn manifest_version(&self) -> i64 {
        match self {
            Self::Watched(emitter) => emitter.manifest_version,
            Self::Generic(source) => source.manifest_version,
        }
    }

    pub(super) fn normalizer_version(&self) -> &str {
        match self {
            Self::Watched(emitter) => &emitter.normalizer_version,
            Self::Generic(source) => &source.normalizer_version,
        }
    }

    pub(super) fn contract_role(&self) -> Option<&str> {
        match self {
            Self::Watched(emitter) => emitter.contract_role.as_deref(),
            Self::Generic(_) => None,
        }
    }
}

fn emitter_active_at_block(emitter: &RoutedActiveEmitter, block_number: i64) -> bool {
    block_number >= emitter.active_from_block_number
        && block_number <= emitter.active_to_block_number
}

fn generic_source_active_at_block(source: &GenericResolverEventSource, block_number: i64) -> bool {
    source
        .effective_from_block
        .is_none_or(|from_block| block_number >= from_block)
        && source
            .effective_to_block
            .is_none_or(|to_block| block_number <= to_block)
}
