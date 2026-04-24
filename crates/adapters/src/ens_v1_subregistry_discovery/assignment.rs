use anyhow::{Context, Result};
use bigname_manifests::DiscoveryObservation;
use serde_json::json;

use super::{
    DERIVATION_KIND_ENS_V1_REGISTRY_RESOLVER_CHANGED, DERIVATION_KIND_ENS_V1_SUBREGISTRY_CHANGED,
    EVENT_KIND_RESOLVER_CHANGED, EVENT_KIND_SUBREGISTRY_CHANGED, RESOLVER_EDGE_KIND,
    SUBREGISTRY_EDGE_KIND,
    hex_topic::{
        ZERO_ADDRESS, child_node, decode_owner_address, new_owner_topic0, new_resolver_topic0,
        normalize_hex_32,
    },
    loader::RegistryRawLogRow,
};

#[derive(Clone, Debug)]
pub(super) struct ObservedRegistryAssignment {
    pub(super) observation_key: String,
    pub(super) observation: DiscoveryObservation,
    pub(super) raw_log: RegistryRawLogRow,
    pub(super) discovery_kind: RegistryDiscoveryKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RegistryDiscoveryKind {
    Subregistry,
    Resolver,
}

impl RegistryDiscoveryKind {
    pub(super) const fn edge_kind(self) -> &'static str {
        match self {
            Self::Subregistry => SUBREGISTRY_EDGE_KIND,
            Self::Resolver => RESOLVER_EDGE_KIND,
        }
    }

    pub(super) const fn event_kind(self) -> &'static str {
        match self {
            Self::Subregistry => EVENT_KIND_SUBREGISTRY_CHANGED,
            Self::Resolver => EVENT_KIND_RESOLVER_CHANGED,
        }
    }

    pub(super) const fn derivation_kind(self) -> &'static str {
        match self {
            Self::Subregistry => DERIVATION_KIND_ENS_V1_SUBREGISTRY_CHANGED,
            Self::Resolver => DERIVATION_KIND_ENS_V1_REGISTRY_RESOLVER_CHANGED,
        }
    }

    pub(super) const fn source_event(self) -> &'static str {
        match self {
            Self::Subregistry => "NewOwner",
            Self::Resolver => "NewResolver",
        }
    }
}

pub(super) fn build_registry_assignment(
    raw_log: &RegistryRawLogRow,
    chain: &str,
) -> Result<Option<ObservedRegistryAssignment>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };
    if topic0.eq_ignore_ascii_case(&new_owner_topic0()) {
        build_subregistry_assignment(raw_log, &ens_v1_subregistry_discovery_source(chain))
    } else if topic0.eq_ignore_ascii_case(&new_resolver_topic0()) {
        build_resolver_assignment(raw_log, &ens_v1_resolver_discovery_source(chain))
    } else {
        Ok(None)
    }
}

fn build_subregistry_assignment(
    raw_log: &RegistryRawLogRow,
    discovery_source: &str,
) -> Result<Option<ObservedRegistryAssignment>> {
    let parent_node = raw_log
        .topics
        .get(1)
        .context("NewOwner log is missing indexed parent node topic")?;
    let labelhash = raw_log
        .topics
        .get(2)
        .context("NewOwner log is missing indexed labelhash topic")?;
    let child_node = child_node(parent_node, labelhash)?;
    let owner = decode_owner_address(&raw_log.data).with_context(|| {
        format!(
            "failed to decode NewOwner owner payload for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;

    Ok(Some(ObservedRegistryAssignment {
        observation_key: child_node.clone(),
        observation: DiscoveryObservation {
            chain: raw_log.chain_id.clone(),
            from_address: raw_log.emitting_address.clone(),
            to_address: owner.clone(),
            edge_kind: SUBREGISTRY_EDGE_KIND.to_owned(),
            discovery_source: discovery_source.to_owned(),
            active_from_block_number: Some(raw_log.block_number),
            active_from_block_hash: Some(raw_log.block_hash.clone()),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: json!({
                "source": "raw_log",
                "source_event": "NewOwner",
                "observation_key": child_node,
                "parent_node": normalize_hex_32(parent_node)?,
                "labelhash": normalize_hex_32(labelhash)?,
                "owner": owner,
                "chain_id": raw_log.chain_id,
                "block_hash": raw_log.block_hash,
                "block_number": raw_log.block_number,
                "transaction_hash": raw_log.transaction_hash,
                "transaction_index": raw_log.transaction_index,
                "log_index": raw_log.log_index,
                "emitting_address": raw_log.emitting_address,
                "tombstone": owner == ZERO_ADDRESS,
            }),
        },
        raw_log: raw_log.clone(),
        discovery_kind: RegistryDiscoveryKind::Subregistry,
    }))
}

fn build_resolver_assignment(
    raw_log: &RegistryRawLogRow,
    discovery_source: &str,
) -> Result<Option<ObservedRegistryAssignment>> {
    let node = raw_log
        .topics
        .get(1)
        .context("NewResolver log is missing indexed node topic")?;
    let node = normalize_hex_32(node)?;
    let resolver = decode_owner_address(&raw_log.data).with_context(|| {
        format!(
            "failed to decode NewResolver resolver payload for chain {} block {} log {}",
            raw_log.chain_id, raw_log.block_hash, raw_log.log_index
        )
    })?;
    let observation_key = format!("resolver:{}:{node}", raw_log.emitting_address);

    Ok(Some(ObservedRegistryAssignment {
        observation_key: observation_key.clone(),
        observation: DiscoveryObservation {
            chain: raw_log.chain_id.clone(),
            from_address: raw_log.emitting_address.clone(),
            to_address: resolver.clone(),
            edge_kind: RESOLVER_EDGE_KIND.to_owned(),
            discovery_source: discovery_source.to_owned(),
            active_from_block_number: Some(raw_log.block_number),
            active_from_block_hash: Some(raw_log.block_hash.clone()),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: json!({
                "source": "raw_log",
                "source_event": "NewResolver",
                "observation_key": observation_key,
                "node": node,
                "resolver": resolver,
                "resolver_profile_supported": false,
                "resolver_profile_status": "unsupported",
                "resolver_profile_reason": "registry_resolver_discovery_does_not_admit_typed_resolver_profile",
                "chain_id": raw_log.chain_id,
                "block_hash": raw_log.block_hash,
                "block_number": raw_log.block_number,
                "transaction_hash": raw_log.transaction_hash,
                "transaction_index": raw_log.transaction_index,
                "log_index": raw_log.log_index,
                "emitting_address": raw_log.emitting_address,
                "tombstone": resolver == ZERO_ADDRESS,
            }),
        },
        raw_log: raw_log.clone(),
        discovery_kind: RegistryDiscoveryKind::Resolver,
    }))
}

pub(super) fn ens_v1_subregistry_discovery_source(chain: &str) -> String {
    format!("ens_v1_registry_new_owner:{chain}")
}

pub(super) fn ens_v1_resolver_discovery_source(chain: &str) -> String {
    format!("ens_v1_registry_resolver:{chain}")
}
