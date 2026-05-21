use bigname_storage::CanonicalityState;
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RecordInventoryCurrentRebuildSummary {
    pub requested_resource_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RecordInventoryTextHydrationSummary {
    pub candidate_row_count: usize,
    pub candidate_entry_count: usize,
    pub hydrated_entry_count: usize,
    pub not_found_entry_count: usize,
    pub skipped_entry_count: usize,
    pub failed_entry_count: usize,
    pub updated_row_count: usize,
}

#[derive(Clone, Debug, sqlx::FromRow)]
pub(super) struct RelevantEvent {
    pub(super) normalized_event_id: i64,
    pub(super) logical_name_id: String,
    pub(super) resource_id: Uuid,
    pub(super) event_kind: String,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
    pub(super) source_manifest_id: Option<i64>,
    pub(super) chain_id: String,
    pub(super) block_number: i64,
    pub(super) block_hash: String,
    pub(super) log_index: Option<i64>,
    pub(super) block_timestamp: Option<OffsetDateTime>,
    pub(super) raw_fact_ref: Value,
    pub(super) canonicality_state: CanonicalityState,
    pub(super) after_state: Value,
    pub(super) emitting_address: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct RecordSelector {
    pub(super) record_key: String,
    pub(super) record_family: String,
    pub(super) selector_key: Option<String>,
}

#[derive(Clone, Debug)]
pub(super) struct ChainPositionCandidate {
    pub(super) slot: String,
    pub(super) chain_id: String,
    pub(super) block_number: i64,
    pub(super) block_hash: String,
    pub(super) timestamp: String,
}
