use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::CanonicalityState;

/// Persisted adapter-owned normalized event used to rebuild projections.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NormalizedEvent {
    pub event_identity: String,
    pub namespace: String,
    pub logical_name_id: Option<String>,
    pub resource_id: Option<Uuid>,
    pub event_kind: String,
    pub source_family: String,
    pub manifest_version: i64,
    pub source_manifest_id: Option<i64>,
    pub chain_id: Option<String>,
    pub block_number: Option<i64>,
    pub block_hash: Option<String>,
    pub transaction_hash: Option<String>,
    pub log_index: Option<i64>,
    pub raw_fact_ref: Value,
    pub derivation_kind: String,
    pub canonicality_state: CanonicalityState,
    pub before_state: Value,
    pub after_state: Value,
}
