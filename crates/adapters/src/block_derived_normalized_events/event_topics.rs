use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use bigname_manifests::load_active_manifest_abi_events;
use sqlx::PgPool;

use crate::adapter_manifest::ActiveManifestEventTopic0sBySignature;

use super::constants::{
    BASENAMES_REGISTRAR_PREIMAGE_EVENT_SIGNATURES, ENS_V1_REGISTRAR_PREIMAGE_EVENT_SIGNATURES,
    ENS_V1_WRAPPER_PREIMAGE_EVENT_SIGNATURES, ENS_V2_REGISTRAR_PREIMAGE_EVENT_SIGNATURES,
    ENS_V2_REGISTRY_PREIMAGE_EVENT_SIGNATURES, ENS_V2_RESOLVER_PREIMAGE_EVENT_SIGNATURES,
    PREIMAGE_EVENT_SIGNATURES, SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
    SOURCE_FAMILY_ENS_V1_REGISTRAR_L1, SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
    SOURCE_FAMILY_ENS_V2_REGISTRAR_L1, SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
    SOURCE_FAMILY_ENS_V2_RESOLVER_L1, SOURCE_FAMILY_ENS_V2_ROOT_L1,
};
use super::types::{ActiveEmitter, WatchedRawLogRow};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct PreimageObservedEventTopics {
    by_manifest_id: HashMap<i64, ActiveManifestEventTopic0sBySignature>,
}

impl PreimageObservedEventTopics {
    #[cfg(test)]
    pub(super) fn from_manifest_topic0s(
        by_manifest_id: HashMap<i64, ActiveManifestEventTopic0sBySignature>,
    ) -> Self {
        Self { by_manifest_id }
    }

    pub(super) async fn load(pool: &PgPool, active_emitters: &[ActiveEmitter]) -> Result<Self> {
        let manifest_ids = active_emitters
            .iter()
            .filter(|emitter| is_manifest_preimage_source(&emitter.source_family))
            .map(|emitter| emitter.source_manifest_id)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        if manifest_ids.is_empty() {
            return Ok(Self::default());
        }

        let mut topic0s_by_manifest = HashMap::<i64, HashMap<String, String>>::new();
        for event in load_active_manifest_abi_events(pool, &manifest_ids)
            .await
            .context("failed to load active manifest ABI events for block-derived preimages")?
        {
            if !PREIMAGE_EVENT_SIGNATURES.contains(&event.canonical_signature.as_str()) {
                continue;
            }
            let topic0 = event.topic0.with_context(|| {
                format!(
                    "active manifest ABI event {} for block-derived preimages is anonymous",
                    event.canonical_signature
                )
            })?;
            let manifest_topics = topic0s_by_manifest.entry(event.manifest_id).or_default();
            match manifest_topics.get(&event.canonical_signature) {
                Some(existing) if existing != &topic0 => {
                    bail!(
                        "active manifest ABI event {} for block-derived preimages has conflicting topic0 values {} and {}",
                        event.canonical_signature,
                        existing,
                        topic0
                    );
                }
                Some(_) => {}
                None => {
                    manifest_topics.insert(event.canonical_signature, topic0);
                }
            }
        }

        let by_manifest_id = topic0s_by_manifest
            .into_iter()
            .map(|(manifest_id, topic0s)| {
                (
                    manifest_id,
                    ActiveManifestEventTopic0sBySignature::new(topic0s),
                )
            })
            .collect::<HashMap<_, _>>();

        for emitter in active_emitters
            .iter()
            .filter(|emitter| is_manifest_preimage_source(&emitter.source_family))
        {
            let topics = by_manifest_id
                .get(&emitter.source_manifest_id)
                .with_context(|| {
                    format!(
                        "missing active manifest ABI topics for block-derived {} manifest_id {}",
                        emitter.source_family, emitter.source_manifest_id
                    )
                })?;
            for signature in required_event_signatures(&emitter.source_family) {
                topics.topic0(signature).with_context(|| {
                    format!(
                        "active manifest ABI for block-derived {} manifest_id {} is missing required event {}",
                        emitter.source_family, emitter.source_manifest_id, signature
                    )
                })?;
            }
        }

        Ok(Self { by_manifest_id })
    }

    pub(super) fn query_topic0s(&self) -> Vec<String> {
        let mut topic0s = Vec::new();
        for manifest_topics in self.by_manifest_id.values() {
            for signature in PREIMAGE_EVENT_SIGNATURES {
                if let Ok(topic0) = manifest_topics.topic0(signature) {
                    topic0s.push(topic0.to_owned());
                }
            }
        }
        topic0s.sort();
        topic0s.dedup();
        topic0s
    }

    pub(super) fn matches(
        &self,
        raw_log: &WatchedRawLogRow,
        canonical_signature: &str,
        topic0: &str,
    ) -> Result<bool> {
        let topics = self
            .by_manifest_id
            .get(&raw_log.source_manifest_id)
            .with_context(|| {
                format!(
                    "missing active manifest ABI topics for block-derived {} manifest_id {}",
                    raw_log.source_family, raw_log.source_manifest_id
                )
            })?;
        topics.matches(canonical_signature, topic0)
    }
}

fn is_manifest_preimage_source(source_family: &str) -> bool {
    matches!(
        source_family,
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1
            | SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR
            | SOURCE_FAMILY_ENS_V1_WRAPPER_L1
            | SOURCE_FAMILY_ENS_V2_ROOT_L1
            | SOURCE_FAMILY_ENS_V2_REGISTRY_L1
            | SOURCE_FAMILY_ENS_V2_REGISTRAR_L1
            | SOURCE_FAMILY_ENS_V2_RESOLVER_L1
    )
}

fn required_event_signatures(source_family: &str) -> &'static [&'static str] {
    match source_family {
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1 => &ENS_V1_REGISTRAR_PREIMAGE_EVENT_SIGNATURES,
        SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR => &BASENAMES_REGISTRAR_PREIMAGE_EVENT_SIGNATURES,
        SOURCE_FAMILY_ENS_V1_WRAPPER_L1 => &ENS_V1_WRAPPER_PREIMAGE_EVENT_SIGNATURES,
        SOURCE_FAMILY_ENS_V2_ROOT_L1 | SOURCE_FAMILY_ENS_V2_REGISTRY_L1 => {
            &ENS_V2_REGISTRY_PREIMAGE_EVENT_SIGNATURES
        }
        SOURCE_FAMILY_ENS_V2_REGISTRAR_L1 => &ENS_V2_REGISTRAR_PREIMAGE_EVENT_SIGNATURES,
        SOURCE_FAMILY_ENS_V2_RESOLVER_L1 => &ENS_V2_RESOLVER_PREIMAGE_EVENT_SIGNATURES,
        _ => &[],
    }
}
