use bigname_storage::{CanonicalityState, HistoryEvent, SurfaceBindingKind};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

#[derive(Clone, Debug, sqlx::FromRow)]
pub(super) struct NameSurfaceSeed {
    pub(super) logical_name_id: String,
    pub(super) namespace: String,
    pub(super) canonical_display_name: String,
    pub(super) normalized_name: String,
    pub(super) namehash: String,
    pub(super) chain_id: String,
    pub(super) block_hash: String,
    pub(super) block_number: i64,
    pub(super) block_timestamp: Option<OffsetDateTime>,
    pub(super) canonicality_state: CanonicalityState,
}

#[derive(Clone, Debug, sqlx::FromRow)]
pub(super) struct CurrentBindingContext {
    pub(super) surface_binding_id: Uuid,
    pub(super) resource_id: Uuid,
    pub(super) token_lineage_id: Option<Uuid>,
    pub(super) binding_kind: SurfaceBindingKind,
    pub(super) chain_id: String,
    pub(super) block_hash: String,
    pub(super) block_number: i64,
    pub(super) block_timestamp: Option<OffsetDateTime>,
    pub(super) surface_binding_state: CanonicalityState,
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
    pub(super) source_manifest_version: Option<i64>,
    pub(super) source_manifest_namespace: Option<String>,
    pub(super) source_manifest_source_family: Option<String>,
    pub(super) source_manifest_chain: Option<String>,
    pub(super) source_manifest_deployment_epoch: Option<String>,
    pub(super) source_manifest_rollout_status: Option<String>,
    pub(super) exact_name_profile_status: Option<String>,
    pub(super) chain_id: Option<String>,
    pub(super) block_number: Option<i64>,
    pub(super) block_hash: Option<String>,
    pub(super) block_timestamp: Option<OffsetDateTime>,
    pub(super) raw_fact_ref: Value,
    pub(super) canonicality_state: CanonicalityState,
    pub(super) after_state: Value,
}

#[derive(Clone, Debug, Default)]
pub(super) struct ProjectedFacts {
    pub(super) registration_status: Option<String>,
    pub(super) authority_kind: Option<String>,
    pub(super) authority_key: Option<String>,
    pub(super) registrant: Option<String>,
    pub(super) expiry: Option<i64>,
    pub(super) released_at: Option<i64>,
    pub(super) registry_owner: Option<String>,
    pub(super) latest_registration_event_kind: Option<String>,
    pub(super) latest_control_event_kind: Option<String>,
    pub(super) control_status_substrate: Option<String>,
    pub(super) control_expiry_substrate: Option<i64>,
    pub(super) resolver_chain_id: Option<String>,
    pub(super) resolver_address: Option<String>,
    pub(super) latest_resolver_event_kind: Option<String>,
    pub(super) surface_head: Option<HistoryPointer>,
    pub(super) resource_head: Option<HistoryPointer>,
}

#[derive(Clone, Debug)]
pub(super) struct ChainPositionCandidate {
    pub(super) slot: String,
    pub(super) chain_id: String,
    pub(super) block_number: i64,
    pub(super) block_hash: String,
    pub(super) timestamp: OffsetDateTime,
}

#[derive(Clone, Debug)]
pub(super) struct SupplementalChainObservation {
    pub(super) candidate: ChainPositionCandidate,
    pub(super) canonicality_state: CanonicalityState,
}

#[derive(Clone, Debug)]
pub(super) struct SupportedResolutionProjection {
    pub(super) topology: Value,
    pub(super) manifest_versions: Vec<Value>,
}

#[derive(Clone, Debug)]
pub(super) struct BasenamesExecutionManifestVersion {
    pub(super) manifest_version: i64,
    pub(super) chain: String,
    pub(super) deployment_epoch: String,
    pub(super) contract_address: String,
}

#[derive(Clone, Debug)]
pub(super) struct WildcardSourceContext {
    pub(super) logical_name_id: String,
    pub(super) namespace: String,
    pub(super) normalized_name: String,
    pub(super) canonical_display_name: String,
    pub(super) namehash: String,
    pub(super) resource_id: Uuid,
    pub(super) resolver_event: RelevantEvent,
    pub(super) boundary_event: RelevantEvent,
    pub(super) matched_labels: Vec<String>,
}

impl WildcardSourceContext {
    pub(super) fn events(&self) -> impl Iterator<Item = &RelevantEvent> {
        let mut events = vec![&self.resolver_event];
        if self.boundary_event.normalized_event_id != self.resolver_event.normalized_event_id {
            events.push(&self.boundary_event);
        }
        events.into_iter()
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct HistoryHeads {
    pub(super) surface_head: Option<HistoryEvent>,
    pub(super) resource_head: Option<HistoryEvent>,
}

impl HistoryHeads {
    pub(super) fn iter(&self) -> impl Iterator<Item = &HistoryEvent> {
        self.surface_head.iter().chain(self.resource_head.iter())
    }
}

#[derive(Clone, Debug)]
pub(super) struct HistoryPointer {
    pub(super) normalized_event_id: i64,
    pub(super) event_kind: String,
    pub(super) chain_position: Value,
}
