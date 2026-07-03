use std::hash::Hasher;

use alloy_primitives::keccak256;
use anyhow::{Result, bail};
use serde::Serialize;
use uuid::Uuid;

const COMPACT_SOURCE_IDENTITY_SELECTED_TARGET_THRESHOLD: usize = 10_000;

#[derive(Clone, Debug)]
pub struct WatchedContract {
    pub chain: String,
    pub source_family: String,
    pub address: String,
    pub contract_instance_id: Uuid,
    pub source: WatchedContractSource,
    pub source_manifest_id: Option<i64>,
    pub active_from_block_number: Option<i64>,
    pub active_to_block_number: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum WatchedContractSource {
    ManifestRoot,
    ManifestContract,
    DiscoveryEdge,
}

impl WatchedContractSource {
    pub(crate) fn from_db_value(value: &str) -> Result<Self> {
        match value {
            "manifest_root" => Ok(Self::ManifestRoot),
            "manifest_contract" => Ok(Self::ManifestContract),
            "discovery_edge" => Ok(Self::DiscoveryEdge),
            _ => bail!("unsupported watched contract source {value}"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchedContractSummary {
    pub unique_contract_count: usize,
    pub source_entry_count: usize,
    pub manifest_root_count: usize,
    pub manifest_contract_count: usize,
    pub discovery_edge_count: usize,
    pub chains: Vec<WatchedContractChainSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchedChainPlan {
    pub chain: String,
    pub addresses: Vec<String>,
    pub manifest_root_entry_count: usize,
    pub manifest_contract_entry_count: usize,
    pub discovery_edge_entry_count: usize,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct WatchedTargetIdentity {
    pub contract_instance_id: Uuid,
}

impl From<Uuid> for WatchedTargetIdentity {
    fn from(contract_instance_id: Uuid) -> Self {
        Self {
            contract_instance_id,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchedSourceSelectorKind {
    WholeActiveWatchedChain,
    SourceFamily,
    WatchedTargetSet,
}

impl WatchedSourceSelectorKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WholeActiveWatchedChain => "whole_active_watched_chain",
            Self::SourceFamily => "source_family",
            Self::WatchedTargetSet => "watched_target_set",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WatchedSourceSelector {
    WholeActiveWatchedChain,
    SourceFamily(String),
    WatchedTargetSet(Vec<WatchedTargetIdentity>),
}

impl WatchedSourceSelector {
    pub const fn kind(&self) -> WatchedSourceSelectorKind {
        match self {
            Self::WholeActiveWatchedChain => WatchedSourceSelectorKind::WholeActiveWatchedChain,
            Self::SourceFamily(_) => WatchedSourceSelectorKind::SourceFamily,
            Self::WatchedTargetSet(_) => WatchedSourceSelectorKind::WatchedTargetSet,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct WatchedBackfillTarget {
    pub source_family: String,
    pub contract_instance_id: Uuid,
    pub address: String,
    pub effective_from_block: i64,
    pub effective_to_block: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchedSourceSelectorPlan {
    pub chain: String,
    pub selector_kind: WatchedSourceSelectorKind,
    pub source_family: Option<String>,
    pub requested_watched_targets: Vec<WatchedTargetIdentity>,
    pub selected_targets: Vec<WatchedBackfillTarget>,
    pub watched_chain_plan: WatchedChainPlan,
}

impl WatchedSourceSelectorPlan {
    pub fn source_identity_payload(&self) -> serde_json::Value {
        let mut payload = self.source_identity_payload_without_hash();
        if let serde_json::Value::Object(fields) = &mut payload {
            fields.insert(
                "source_identity_hash".to_owned(),
                serde_json::Value::String(self.source_identity_hash()),
            );
        }
        payload
    }

    pub fn source_identity_hash(&self) -> String {
        let payload = serde_json::to_string(&self.source_identity_hash_payload_without_hash())
            .expect("watched source identity payload must be serializable");
        let mut hasher = StableFnv64::default();
        hasher.write(payload.as_bytes());
        format!("fnv1a64:{:016x}", hasher.finish())
    }

    fn source_identity_hash_payload_without_hash(&self) -> serde_json::Value {
        serde_json::json!({
            "selector_kind": self.selector_kind.as_str(),
            "source_family": self.source_family,
            "requested_watched_targets": self.requested_watched_targets,
            "selected_targets": self.selected_targets,
        })
    }

    fn source_identity_payload_without_hash(&self) -> serde_json::Value {
        if self.uses_compact_selected_targets_identity() {
            return serde_json::json!({
                "selector_kind": self.selector_kind.as_str(),
                "source_family": self.source_family,
                "requested_watched_targets": self.requested_watched_targets,
                "selected_target_count": self.selected_targets.len(),
                "selected_targets_digest_algorithm": "keccak256",
                "selected_targets_digest": selected_targets_digest(&self.selected_targets),
                "selected_targets_sample": selected_targets_sample(&self.selected_targets),
                "source_identity_payload_format": "selected_targets_digest_v1",
            });
        }

        self.source_identity_hash_payload_without_hash()
    }

    fn uses_compact_selected_targets_identity(&self) -> bool {
        self.selector_kind == WatchedSourceSelectorKind::WholeActiveWatchedChain
            && self.selected_targets.len() > COMPACT_SOURCE_IDENTITY_SELECTED_TARGET_THRESHOLD
    }
}

fn selected_targets_sample(selected_targets: &[WatchedBackfillTarget]) -> serde_json::Value {
    serde_json::json!({
        "first": selected_targets.first(),
        "last": selected_targets.last(),
    })
}

fn selected_targets_digest(selected_targets: &[WatchedBackfillTarget]) -> String {
    let value = serde_json::to_value(selected_targets)
        .expect("selected target identity digest input is serializable");
    json_digest(&canonical_json_value(value))
}

fn json_digest<T>(value: &T) -> String
where
    T: Serialize + ?Sized,
{
    let payload = serde_json::to_vec(value).expect("source identity digest input is serializable");
    format!("keccak256:{}", keccak256(payload))
}

fn canonical_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(canonical_json_value).collect())
        }
        serde_json::Value::Object(fields) => {
            let mut fields = fields
                .into_iter()
                .map(|(key, value)| (key, canonical_json_value(value)))
                .collect::<Vec<_>>();
            fields.sort_by(|left, right| left.0.cmp(&right.0));

            let mut sorted = serde_json::Map::new();
            for (key, value) in fields {
                sorted.insert(key, value);
            }
            serde_json::Value::Object(sorted)
        }
        value => value,
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ManifestBootstrapTarget {
    pub source_family: String,
    pub contract_instance_id: Uuid,
    pub address: String,
    pub effective_from_block: i64,
    pub effective_to_block: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolverProfileAdmission {
    pub chain: String,
    pub source_family: String,
    pub contract_instance_id: Uuid,
    pub address: String,
    pub source: WatchedContractSource,
    pub source_manifest_id: Option<i64>,
    pub active_from_block_number: Option<i64>,
    pub active_to_block_number: Option<i64>,
    pub profile: String,
    pub fact_family: String,
    pub status: String,
    pub admission_basis: String,
    pub observed_code_hash: Option<String>,
    pub matched_code_hash: Option<String>,
    pub matched_contract_instance_id: Option<Uuid>,
}

#[derive(Default)]
struct StableFnv64(u64);

impl Hasher for StableFnv64 {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        if self.0 == 0 {
            self.0 = 0xcbf29ce484222325;
        }

        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchedContractChainSummary {
    pub chain: String,
    pub unique_contract_count: usize,
    pub manifest_root_count: usize,
    pub manifest_contract_count: usize,
    pub discovery_edge_count: usize,
}
