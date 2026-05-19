use std::collections::BTreeMap;

use bigname_storage::CanonicalityState;

/// Sync summary for block-derived normalized events rebuilt from persisted raw payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockDerivedNormalizedEventSyncSummary {
    pub scanned_log_count: usize,
    pub matched_log_count: usize,
    pub total_synced_count: usize,
    pub total_inserted_count: usize,
    pub by_kind: BTreeMap<String, BlockDerivedNormalizedEventKindSyncSummary>,
}

/// Per-kind sync summary for logging.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockDerivedNormalizedEventKindSyncSummary {
    pub synced_count: usize,
    pub inserted_count: usize,
}

#[derive(Clone, Debug)]
pub(super) struct WatchedRawLogRow {
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
    pub(super) source_manifest_id: i64,
    pub(super) namespace: String,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ActiveEmitter {
    pub(super) address: String,
    pub(super) contract_instance_id: sqlx::types::Uuid,
    pub(super) source_manifest_id: i64,
    pub(super) namespace: String,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
    pub(super) source_rank: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RawLogSourceScopeTarget {
    pub(super) source_family: String,
    pub(super) address: String,
    pub(super) effective_from_block: i64,
    pub(super) effective_to_block: i64,
}

#[derive(Clone, Debug)]
pub(super) struct PreimageObservation {
    pub(super) dns_encoded_name: String,
    pub(super) decoded_name: Option<String>,
    pub(super) labelhashes: Vec<String>,
    pub(super) namehash: String,
}
