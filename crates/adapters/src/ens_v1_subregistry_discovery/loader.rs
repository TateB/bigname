use bigname_storage::CanonicalityState;
use sqlx::types::Uuid;

mod active_emitters;
mod edges;
mod raw_logs;

pub(super) use active_emitters::load_active_emitters;
pub(super) use edges::load_registry_edges_by_observation_point;
pub(super) use raw_logs::{
    RegistryRawLogPosition, load_registry_raw_log_checkpoint_page, load_registry_raw_logs,
    stream_registry_raw_logs,
};

#[derive(Clone, Debug)]
pub(super) struct RegistryRawLogRow {
    pub(super) chain_id: String,
    pub(super) block_hash: String,
    pub(super) block_number: i64,
    pub(super) transaction_hash: String,
    pub(super) transaction_index: i64,
    pub(super) log_index: i64,
    pub(super) emitting_address: String,
    pub(super) topics: Vec<String>,
    pub(super) data: Vec<u8>,
    pub(super) canonicality_state: CanonicalityState,
    pub(super) emitting_contract_instance_id: Uuid,
    pub(super) source_manifest_id: i64,
    pub(super) namespace: String,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
    pub(super) contract_role: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ActiveEmitter {
    pub(super) address: String,
    pub(super) contract_instance_id: Uuid,
    pub(super) source_manifest_id: i64,
    pub(super) namespace: String,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
    pub(super) contract_role: Option<String>,
    pub(super) active_from_block_number: Option<i64>,
    pub(super) active_to_block_number: Option<i64>,
    pub(super) source_rank: i32,
}

#[derive(Clone, Debug)]
pub(super) struct ActiveRegistryEdge {
    pub(super) observation_key: String,
    pub(super) discovery_source: String,
    pub(super) from_contract_instance_id: Uuid,
    pub(super) to_contract_instance_id: Uuid,
}
