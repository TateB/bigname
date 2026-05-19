use serde::{Deserialize, Serialize};
use sqlx::types::time::OffsetDateTime;

/// Persisted lineage snapshot for one chain block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainLineageBlock {
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

/// Persisted canonicality marker for a lineage row.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CanonicalityState {
    Observed,
    Canonical,
    Safe,
    Finalized,
    Orphaned,
}
