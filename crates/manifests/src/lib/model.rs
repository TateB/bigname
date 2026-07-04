use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use anyhow::{Result, bail};
use serde::{Deserialize, Deserializer, Serialize, de};

use crate::REACHABLE_FROM_ROOT_ADMISSION;

#[path = "model/abi.rs"]
mod abi;
#[path = "model/watched.rs"]
mod watched;

pub use abi::{ParsedManifestAbiEvent, ParsedManifestAbiFunction};
pub use watched::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestRepository {
    pub(crate) root: PathBuf,
    pub(crate) manifests: Vec<LoadedManifest>,
    pub(crate) summary: ManifestLoadSummary,
}

impl ManifestRepository {
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn manifests(&self) -> &[LoadedManifest] {
        &self.manifests
    }

    pub fn summary(&self) -> &ManifestLoadSummary {
        &self.summary
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedManifest {
    pub path: PathBuf,
    pub relative_path: PathBuf,
    pub version_tag: String,
    pub manifest: SourceManifest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestLoadSummary {
    pub root: PathBuf,
    pub status: ManifestLoadStatus,
    pub namespace_count: usize,
    pub source_family_count: usize,
    pub manifest_count: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestLoadStatus {
    Loaded,
    Empty,
    MissingRoot,
    InvalidRoot,
}

impl ManifestLoadStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Loaded => "loaded",
            Self::Empty => "empty",
            Self::MissingRoot => "missing_root",
            Self::InvalidRoot => "invalid_root",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestSyncSummary {
    pub status: ManifestSyncStatus,
    pub synced_manifest_count: usize,
    pub active_manifest_count: usize,
    pub root_count: usize,
    pub contract_count: usize,
    pub capability_count: usize,
    pub discovery_rule_count: usize,
    pub removed_manifest_count: usize,
    pub cleared_discovery_edge_count: usize,
}

impl ManifestSyncSummary {
    pub(crate) fn skipped(status: ManifestSyncStatus) -> Self {
        Self {
            status,
            synced_manifest_count: 0,
            active_manifest_count: 0,
            root_count: 0,
            contract_count: 0,
            capability_count: 0,
            discovery_rule_count: 0,
            removed_manifest_count: 0,
            cleared_discovery_edge_count: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestSyncStatus {
    Synced,
    SkippedMissingRoot,
    SkippedInvalidRoot,
    SkippedPendingBaseRederiveReplay,
}

impl ManifestSyncStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Synced => "synced",
            Self::SkippedMissingRoot => "skipped_missing_root",
            Self::SkippedInvalidRoot => "skipped_invalid_root",
            Self::SkippedPendingBaseRederiveReplay => "skipped_pending_base_rederive_replay",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ActiveManifestVersion {
    pub manifest_version: u64,
    pub source_family: String,
    pub chain: String,
    pub deployment_epoch: String,
    pub normalizer_version: String,
    pub capability_flags: BTreeMap<String, CapabilityFlag>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceManifestSnapshot {
    pub manifests: Vec<ActiveManifestVersion>,
    pub last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceManifest {
    pub manifest_version: u64,
    pub namespace: String,
    pub source_family: String,
    pub chain: String,
    pub deployment_epoch: String,
    pub rollout_status: RolloutStatus,
    pub normalizer_version: String,
    pub capability_flags: BTreeMap<String, CapabilityFlag>,
    pub roots: Vec<ManifestRoot>,
    pub contracts: Vec<ManifestContract>,
    pub discovery_rules: Vec<DiscoveryRule>,
    #[serde(default, skip_serializing_if = "ManifestAbi::is_empty")]
    pub abi: ManifestAbi,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RolloutStatus {
    Draft,
    Shadow,
    Active,
    Deprecated,
}

impl RolloutStatus {
    pub const fn as_db_value(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Shadow => "shadow",
            Self::Active => "active",
            Self::Deprecated => "deprecated",
        }
    }

    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilitySupportStatus {
    Unsupported,
    Shadow,
    Supported,
}

impl CapabilitySupportStatus {
    pub const fn as_db_value(self) -> &'static str {
        match self {
            Self::Unsupported => "unsupported",
            Self::Shadow => "shadow",
            Self::Supported => "supported",
        }
    }

    pub(crate) fn from_db_value(value: &str) -> Result<Self> {
        match value {
            "unsupported" => Ok(Self::Unsupported),
            "shadow" => Ok(Self::Shadow),
            "supported" => Ok(Self::Supported),
            _ => bail!("unsupported capability status {value}"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CapabilityFlag {
    pub status: CapabilitySupportStatus,
    pub notes: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ManifestAbi {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<ManifestAbiEvent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub calls: Vec<ManifestAbiCall>,
}

impl ManifestAbi {
    pub fn is_empty(&self) -> bool {
        self.events.is_empty() && self.calls.is_empty()
    }

    pub fn event_topic0s(&self) -> Result<Vec<String>> {
        let mut topic0s = self
            .events
            .iter()
            .map(|event| event.parsed_event_view().map(|parsed| parsed.topic0()))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        topic0s.sort();
        topic0s.dedup();
        Ok(topic0s)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ManifestAbiEvent {
    pub name: String,
    pub fragment: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub emitter_roles: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub normalized_events: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<CapabilitySupportStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ManifestAbiCall {
    pub name: String,
    pub fragment: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_roles: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<CapabilitySupportStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ManifestRoot {
    pub name: String,
    pub address: String,
    pub code_hash: Option<String>,
    pub abi_ref: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_start_block")]
    pub start_block: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ManifestContract {
    pub role: String,
    pub address: String,
    pub proxy_kind: String,
    pub implementation: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_start_block")]
    pub start_block: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DiscoveryRule {
    pub edge_kind: String,
    pub from_role: String,
    #[serde(deserialize_with = "deserialize_authored_discovery_rule_admission")]
    pub admission: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct RawSourceManifest {
    manifest_version: u64,
    namespace: String,
    source_family: String,
    chain: String,
    deployment_epoch: String,
    rollout_status: RolloutStatus,
    normalizer_version: String,
    capability_flags: BTreeMap<String, RawCapabilityFlag>,
    roots: Vec<ManifestRoot>,
    contracts: Vec<ManifestContract>,
    discovery_rules: Vec<DiscoveryRule>,
    #[serde(default)]
    abi: ManifestAbi,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(untagged)]
enum RawCapabilityFlag {
    Status(CapabilitySupportStatus),
    Detailed(CapabilityFlag),
}

impl From<RawSourceManifest> for SourceManifest {
    fn from(value: RawSourceManifest) -> Self {
        Self {
            manifest_version: value.manifest_version,
            namespace: value.namespace,
            source_family: value.source_family,
            chain: value.chain,
            deployment_epoch: value.deployment_epoch,
            rollout_status: value.rollout_status,
            normalizer_version: value.normalizer_version,
            capability_flags: value
                .capability_flags
                .into_iter()
                .map(|(name, flag)| {
                    let flag = match flag {
                        RawCapabilityFlag::Status(status) => CapabilityFlag {
                            status,
                            notes: None,
                        },
                        RawCapabilityFlag::Detailed(flag) => flag,
                    };
                    (name, flag)
                })
                .collect(),
            roots: value.roots,
            contracts: value.contracts,
            discovery_rules: value.discovery_rules,
            abi: value.abi,
        }
    }
}

fn deserialize_optional_start_block<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<i64>::deserialize(deserializer)?
        .map(|start_block| {
            u64::try_from(start_block)
                .map_err(|_| de::Error::custom("start_block must be a non-negative integer"))
        })
        .transpose()
}

fn deserialize_authored_discovery_rule_admission<'de, D>(
    deserializer: D,
) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let admission = String::deserialize(deserializer)?;
    if admission == REACHABLE_FROM_ROOT_ADMISSION {
        Ok(admission)
    } else {
        Err(de::Error::custom(format!(
            "unsupported authored discovery_rules[].admission \"{admission}\"; expected \"{REACHABLE_FROM_ROOT_ADMISSION}\""
        )))
    }
}
