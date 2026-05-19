use anyhow::{Context, Result};
use bigname_manifests::DiscoveryObservation;
use serde_json::{Value, json};

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
    pub(super) discovery_source: String,
    pub(super) from_address: String,
    pub(super) to_address: String,
    pub(super) parent_node: Option<String>,
    pub(super) labelhash: Option<String>,
    pub(super) node: Option<String>,
    pub(super) migration_epoch_input: bool,
    pub(super) old_root_resolver_exception: bool,
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

impl ObservedRegistryAssignment {
    pub(super) fn discovery_observation(&self) -> Result<DiscoveryObservation> {
        let mut provenance = match self.discovery_kind {
            RegistryDiscoveryKind::Subregistry => json!({
                "source": "raw_log",
                "source_event": "NewOwner",
                "observation_key": self.observation_key,
                "parent_node": self
                    .parent_node
                    .as_deref()
                    .context("subregistry assignment is missing parent node")?,
                "labelhash": self
                    .labelhash
                    .as_deref()
                    .context("subregistry assignment is missing labelhash")?,
                "owner": self.to_address,
                "chain_id": self.raw_log.chain_id,
                "block_hash": self.raw_log.block_hash,
                "block_number": self.raw_log.block_number,
                "transaction_hash": self.raw_log.transaction_hash,
                "transaction_index": self.raw_log.transaction_index,
                "log_index": self.raw_log.log_index,
                "emitting_address": self.raw_log.emitting_address,
                "tombstone": self.to_address == ZERO_ADDRESS,
            }),
            RegistryDiscoveryKind::Resolver => json!({
                "source": "raw_log",
                "source_event": "NewResolver",
                "observation_key": self.observation_key,
                "node": self
                    .node
                    .as_deref()
                    .context("resolver assignment is missing node")?,
                "resolver": self.to_address,
                "resolver_profile_supported": false,
                "resolver_profile_status": "unsupported",
                "resolver_profile_reason": "registry_resolver_discovery_does_not_admit_typed_resolver_profile",
                "chain_id": self.raw_log.chain_id,
                "block_hash": self.raw_log.block_hash,
                "block_number": self.raw_log.block_number,
                "transaction_hash": self.raw_log.transaction_hash,
                "transaction_index": self.raw_log.transaction_index,
                "log_index": self.raw_log.log_index,
                "emitting_address": self.raw_log.emitting_address,
                "tombstone": self.to_address == ZERO_ADDRESS,
            }),
        };
        if self.migration_epoch_input {
            provenance["authority_from_address"] = Value::String(self.from_address.clone());
            provenance["ens_registry_old_migration_epoch_input"] = Value::Bool(true);
        }
        if self.old_root_resolver_exception {
            provenance["ens_registry_old_root_resolver_exception"] = Value::Bool(true);
        }

        Ok(DiscoveryObservation {
            chain: self.raw_log.chain_id.clone(),
            from_address: self.from_address.clone(),
            to_address: self.to_address.clone(),
            edge_kind: self.discovery_kind.edge_kind().to_owned(),
            discovery_source: self.discovery_source.clone(),
            active_from_block_number: Some(self.raw_log.block_number),
            active_from_block_hash: Some(self.raw_log.block_hash.clone()),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance,
        })
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
        discovery_source: discovery_source.to_owned(),
        from_address: raw_log.emitting_address.clone(),
        to_address: owner,
        parent_node: Some(normalize_hex_32(parent_node)?),
        labelhash: Some(normalize_hex_32(labelhash)?),
        node: None,
        migration_epoch_input: false,
        old_root_resolver_exception: false,
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
        discovery_source: discovery_source.to_owned(),
        from_address: raw_log.emitting_address.clone(),
        to_address: resolver,
        parent_node: None,
        labelhash: None,
        node: Some(node),
        migration_epoch_input: false,
        old_root_resolver_exception: false,
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
