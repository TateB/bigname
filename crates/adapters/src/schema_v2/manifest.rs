use alloy_json_abi::Event;
use anyhow::{Context, bail};
use bigname_domain::normalization::ENS_NORMALIZER_VERSION;
use serde::Deserialize;
use std::collections::BTreeMap;

use super::model::ManifestInput;

#[derive(Clone, Debug)]
pub(super) struct ManifestSource {
    pub manifest_id: i64,
    pub manifest_version: i64,
    pub namespace: String,
    pub source_family: String,
    pub chain_id: String,
    pub deployment_label: String,
    pub correlation_addresses: BTreeMap<String, String>,
    pub events: Vec<ManifestEvent>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ManifestProvenance {
    pub manifest_id: i64,
    pub manifest_version: i64,
    pub namespace: String,
    pub source_family: String,
}

impl ManifestProvenance {
    pub(super) fn from_input(input: &ManifestInput) -> Self {
        Self {
            manifest_id: input.manifest_id,
            manifest_version: input.manifest_version,
            namespace: input.namespace.clone(),
            source_family: input.source_family.clone(),
        }
    }

    pub(super) fn from_source(source: &ManifestSource) -> Self {
        Self {
            manifest_id: source.manifest_id,
            manifest_version: source.manifest_version,
            namespace: source.namespace.clone(),
            source_family: source.source_family.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct ManifestEvent {
    pub name: String,
    pub signature: String,
    pub topic0: String,
    pub emitter_roles: Vec<String>,
    pub normalized_events: Vec<String>,
}

#[derive(Deserialize)]
struct StoredPayload {
    #[serde(default)]
    correlation_addresses: BTreeMap<String, String>,
    #[serde(default)]
    abi: StoredAbi,
}

#[derive(Default, Deserialize)]
struct StoredAbi {
    #[serde(default)]
    events: Vec<StoredEvent>,
}

#[derive(Deserialize)]
struct StoredEvent {
    name: String,
    fragment: String,
    #[serde(default)]
    emitter_roles: Vec<String>,
    #[serde(default)]
    normalized_events: Vec<String>,
}

pub(super) fn decode(input: ManifestInput) -> anyhow::Result<ManifestSource> {
    if input.normalizer_version != ENS_NORMALIZER_VERSION {
        bail!(
            "manifest {} declares normalizer version {}, but schema-v2 label flags use {}",
            input.manifest_id,
            input.normalizer_version,
            ENS_NORMALIZER_VERSION,
        );
    }
    let stored = serde_json::from_str::<StoredPayload>(&input.payload_json)
        .with_context(|| format!("manifest {} payload has no valid ABI", input.manifest_id))?;
    let events = stored
        .abi
        .events
        .into_iter()
        .map(|event| decode_event(input.manifest_id, event))
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(ManifestSource {
        manifest_id: input.manifest_id,
        manifest_version: input.manifest_version,
        namespace: input.namespace,
        source_family: input.source_family,
        chain_id: input.chain_id,
        deployment_label: input.deployment_label,
        correlation_addresses: stored.correlation_addresses,
        events,
    })
}

fn decode_event(manifest_id: i64, stored: StoredEvent) -> anyhow::Result<ManifestEvent> {
    let parsed = Event::parse(stored.fragment.trim()).with_context(|| {
        format!(
            "manifest {manifest_id} event {} has an invalid ABI fragment",
            stored.name
        )
    })?;
    if parsed.name != stored.name {
        bail!(
            "manifest {manifest_id} event {} has fragment name {}",
            stored.name,
            parsed.name
        );
    }
    if parsed.anonymous {
        bail!("manifest {manifest_id} event {} is anonymous", stored.name);
    }
    Ok(ManifestEvent {
        name: stored.name,
        signature: parsed.signature(),
        topic0: format!("{:#x}", parsed.selector()),
        emitter_roles: stored.emitter_roles,
        normalized_events: stored.normalized_events,
    })
}
