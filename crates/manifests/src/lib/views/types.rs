use serde_json::Value;
use uuid::Uuid;

use crate::WatchedContractSource;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveManifestAbiEvent {
    pub manifest_id: i64,
    pub manifest_version: u64,
    pub namespace: String,
    pub source_family: String,
    pub chain: String,
    pub deployment_epoch: String,
    pub name: String,
    pub canonical_signature: String,
    pub topic0: Option<String>,
    pub emitter_roles: Vec<String>,
    pub normalized_events: Vec<String>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ManifestBootstrapSkippedTarget {
    pub source_family: String,
    pub contract_instance_id: Uuid,
    pub address: String,
    pub skip_reason: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ManifestDriftInputs {
    pub active_manifests: Vec<ManifestDriftActiveManifest>,
    pub declared_contracts: Vec<ManifestDeclaredContractDriftInput>,
    pub proxy_implementation_edges: Vec<ManifestProxyImplementationDriftEdge>,
    pub code_hash_observations: Vec<ManifestCodeHashObservation>,
    pub normalized_manifest_events: Vec<ManifestNormalizedEventInput>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ManifestDriftActiveManifest {
    pub manifest_id: i64,
    pub manifest_version: u64,
    pub namespace: String,
    pub source_family: String,
    pub chain: String,
    pub deployment_epoch: String,
    pub normalizer_version: String,
    pub file_path: String,
    pub manifest_payload: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestDeclaredContractDriftInput {
    pub manifest_id: i64,
    pub manifest_version: u64,
    pub namespace: String,
    pub source_family: String,
    pub chain: String,
    pub deployment_epoch: String,
    pub declaration_kind: String,
    pub declaration_name: String,
    pub contract_instance_id: Uuid,
    pub declared_address: String,
    pub code_hash: Option<String>,
    pub abi_ref: Option<String>,
    pub role: Option<String>,
    pub proxy_kind: Option<String>,
    pub implementation_contract_instance_id: Option<Uuid>,
    pub declared_implementation_address: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ManifestProxyImplementationDriftEdge {
    pub discovery_edge_id: i64,
    pub source_manifest_id: i64,
    pub manifest_version: u64,
    pub namespace: String,
    pub source_family: String,
    pub chain: String,
    pub proxy_contract_instance_id: Uuid,
    pub proxy_address: Option<String>,
    pub implementation_contract_instance_id: Uuid,
    pub implementation_address: Option<String>,
    pub declaration_name: Option<String>,
    pub role: Option<String>,
    pub proxy_kind: Option<String>,
    pub admission: String,
    pub active_from_block_number: Option<i64>,
    pub active_to_block_number: Option<i64>,
    pub provenance: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestCodeHashObservation {
    pub chain: String,
    pub source_family: String,
    pub contract_instance_id: Uuid,
    pub address: String,
    pub source: WatchedContractSource,
    pub source_manifest_id: Option<i64>,
    pub block_hash: String,
    pub block_number: i64,
    pub code_hash: String,
    pub code_byte_length: i64,
    pub canonicality_state: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ManifestNormalizedEventInput {
    pub normalized_event_id: i64,
    pub event_identity: String,
    pub namespace: String,
    pub logical_name_id: Option<String>,
    pub resource_id: Option<Uuid>,
    pub event_kind: String,
    pub source_family: String,
    pub manifest_version: u64,
    pub source_manifest_id: Option<i64>,
    pub chain_id: Option<String>,
    pub block_number: Option<i64>,
    pub block_hash: Option<String>,
    pub transaction_hash: Option<String>,
    pub log_index: Option<i64>,
    pub raw_fact_ref: Value,
    pub derivation_kind: String,
    pub canonicality_state: String,
    pub before_state: Value,
    pub after_state: Value,
}
