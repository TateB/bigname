use std::collections::{HashMap, HashSet};

use anyhow::Result;

use super::super::super::*;

pub(in crate::ens_v1_unwrapped_authority) struct AuthorityRawLogStreamSourceRouter<'a> {
    watched_emitters_by_address: HashMap<&'a str, Vec<&'a ActiveEmitter>>,
    watched_topic0s: HashSet<String>,
    generic_resolver_event_sources: &'a [GenericResolverEventSource],
    generic_topic0s: HashSet<String>,
}

#[derive(Clone, Copy)]
pub(super) enum AuthorityRawLogStreamSource<'a> {
    Watched(&'a ActiveEmitter),
    Generic(&'a GenericResolverEventSource),
}

impl<'a> AuthorityRawLogStreamSourceRouter<'a> {
    pub(in crate::ens_v1_unwrapped_authority) fn new(
        active_emitters: &'a [ActiveEmitter],
        generic_resolver_event_sources: &'a [GenericResolverEventSource],
        event_topics: &AuthorityEventTopics,
    ) -> Result<Self> {
        let mut watched_emitters_by_address = HashMap::<&str, Vec<&ActiveEmitter>>::new();
        for emitter in active_emitters {
            watched_emitters_by_address
                .entry(emitter.address.as_str())
                .or_default()
                .push(emitter);
        }
        for emitters in watched_emitters_by_address.values_mut() {
            emitters.sort_by(|left, right| {
                left.source_family
                    .cmp(&right.source_family)
                    .then(left.source_manifest_id.cmp(&right.source_manifest_id))
            });
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
            watched_emitters_by_address,
            watched_topic0s,
            generic_resolver_event_sources,
            generic_topic0s,
        })
    }

    pub(super) fn source_candidates(
        &self,
        emitting_address: &str,
        topic0: &str,
        block_number: i64,
    ) -> Vec<AuthorityRawLogStreamSource<'a>> {
        let mut candidates = Vec::new();
        if self.watched_topic0s.contains(topic0) {
            if let Some(emitters) = self.watched_emitters_by_address.get(emitting_address) {
                candidates.extend(
                    emitters
                        .iter()
                        .copied()
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

fn emitter_active_at_block(emitter: &ActiveEmitter, block_number: i64) -> bool {
    emitter
        .active_from_block_number
        .is_none_or(|from_block| block_number >= from_block)
        && emitter
            .active_to_block_number
            .is_none_or(|to_block| block_number <= to_block)
}

fn generic_source_active_at_block(source: &GenericResolverEventSource, block_number: i64) -> bool {
    source
        .effective_from_block
        .is_none_or(|from_block| block_number >= from_block)
        && source
            .effective_to_block
            .is_none_or(|to_block| block_number <= to_block)
}
