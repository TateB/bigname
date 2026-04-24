use sqlx::types::time::OffsetDateTime;

use crate::CanonicalityState;

/// Persisted exact block fact from a hash-scoped provider fetch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawBlock {
    pub chain_id: String,
    pub block_hash: String,
    pub parent_hash: Option<String>,
    pub block_number: i64,
    pub block_timestamp: OffsetDateTime,
    pub logs_bloom: Option<Vec<u8>>,
    pub transactions_root: Option<String>,
    pub receipts_root: Option<String>,
    pub state_root: Option<String>,
    pub canonicality_state: CanonicalityState,
}

/// Canonical raw log input for adapter-owned normalized-event replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawLogReplayInput {
    pub raw_log_id: i64,
    pub chain_id: String,
    pub block_hash: String,
    pub block_number: i64,
    pub parent_hash: Option<String>,
    pub block_timestamp: OffsetDateTime,
    pub lineage_canonicality_state: CanonicalityState,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub log_index: i64,
    pub emitting_address: String,
    pub topics: Vec<String>,
    pub data: Vec<u8>,
    pub raw_canonicality_state: CanonicalityState,
}
