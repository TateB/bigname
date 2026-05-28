use bigname_storage::{CanonicalityState, SurfaceBindingKind};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

#[derive(Clone, Debug, sqlx::FromRow)]
pub(super) struct CurrentBindingSeed {
    pub(super) logical_name_id: String,
    pub(super) namespace: String,
    pub(super) canonical_display_name: String,
    pub(super) normalized_name: String,
    pub(super) namehash: String,
    pub(super) surface_chain_id: String,
    pub(super) surface_block_hash: String,
    pub(super) surface_block_number: i64,
    pub(super) surface_block_timestamp: Option<OffsetDateTime>,
    pub(super) surface_state: CanonicalityState,
    pub(super) surface_binding_id: Uuid,
    pub(super) resource_id: Uuid,
    pub(super) token_lineage_id: Option<Uuid>,
    pub(super) binding_kind: SurfaceBindingKind,
    pub(super) binding_chain_id: String,
    pub(super) binding_block_hash: String,
    pub(super) binding_block_number: i64,
    pub(super) binding_block_timestamp: Option<OffsetDateTime>,
    pub(super) binding_state: CanonicalityState,
    pub(super) resource_state: CanonicalityState,
    pub(super) token_lineage_state: Option<CanonicalityState>,
}

#[derive(Clone, Debug, sqlx::FromRow)]
pub(super) struct RelevantEvent {
    pub(super) normalized_event_id: i64,
    pub(super) resource_id: Option<Uuid>,
    pub(super) event_kind: String,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
    pub(super) source_manifest_id: Option<i64>,
    pub(super) chain_id: Option<String>,
    pub(super) block_number: Option<i64>,
    pub(super) block_hash: Option<String>,
    pub(super) block_timestamp: Option<OffsetDateTime>,
    pub(super) raw_fact_ref: Value,
    pub(super) canonicality_state: CanonicalityState,
    pub(super) after_state: Value,
}

#[derive(Clone, Debug, Default)]
pub(super) struct ProjectedRelations {
    pub(super) registrant: Option<String>,
    pub(super) token_holder: Option<String>,
    pub(super) effective_controller: Option<String>,
}

#[derive(Clone, Debug)]
pub(super) struct ChainPositionCandidate {
    pub(super) slot: String,
    pub(super) chain_id: String,
    pub(super) block_number: i64,
    pub(super) block_hash: String,
    pub(super) timestamp: OffsetDateTime,
}
