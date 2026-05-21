use std::collections::BTreeMap;

use bigname_manifests::{ActiveManifestVersion, CapabilityFlag};
use bigname_storage::PrimaryNameCurrentRow;
use serde::{Deserialize, Serialize};
use sqlx::types::{JsonValue, time::OffsetDateTime};

use crate::pagination::HistoryPageResponse;

#[derive(Serialize)]
pub(crate) struct HealthResponse {
    pub(crate) service: &'static str,
    pub(crate) phase: &'static str,
    pub(crate) status: &'static str,
    pub(crate) process: HealthProcessResponse,
    pub(crate) database: HealthDatabaseResponse,
}

#[derive(Serialize)]
pub(crate) struct HealthProcessResponse {
    pub(crate) status: &'static str,
}

#[derive(Serialize)]
pub(crate) struct HealthDatabaseResponse {
    pub(crate) status: &'static str,
    pub(crate) reachable: bool,
    pub(crate) check: &'static str,
    pub(crate) error: Option<&'static str>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ForwardIdentityBatchInput {
    pub(crate) names: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ReverseIdentityBatchInput {
    pub(crate) inputs: Vec<ReverseIdentityBatchInputItem>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ReverseIdentityFeedInput {
    pub(crate) inputs: Vec<ReverseIdentityFeedInputItem>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ReverseIdentityBatchInputItem {
    pub(crate) address: String,
    pub(crate) coin_type: Option<u64>,
    pub(crate) roles: Option<String>,
    pub(crate) page_size: Option<u64>,
    pub(crate) page_cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ReverseIdentityFeedInputItem {
    pub(crate) address: String,
    pub(crate) coin_type: Option<u64>,
    pub(crate) roles: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct IdentityNameResponse {
    pub(crate) status: String,
    pub(crate) record: Option<NameRecordResponse>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ForwardIdentityBatchResponse {
    pub(crate) results: Vec<ForwardIdentityBatchResult>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ForwardIdentityBatchResult {
    pub(crate) input: ForwardIdentityBatchResultInput,
    pub(crate) record: Option<NameRecordResponse>,
    pub(crate) status: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ForwardIdentityBatchResultInput {
    pub(crate) name: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ReverseNamesResponse {
    pub(crate) input: ReverseNamesInputResponse,
    pub(crate) records: Vec<ReverseNameRecordResponse>,
    pub(crate) pagination: IdentityPaginationResponse,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ReverseIdentityBatchResponse {
    pub(crate) results: Vec<ReverseIdentityBatchResult>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ReverseIdentityFeedResponse {
    pub(crate) results: Vec<ReverseIdentityFeedResult>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ReverseIdentityBatchResult {
    pub(crate) input: ReverseNamesInputResponse,
    pub(crate) records: Vec<ReverseNameRecordResponse>,
    pub(crate) pagination: IdentityPaginationResponse,
    pub(crate) status: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ReverseIdentityFeedResult {
    pub(crate) input: ReverseNamesInputResponse,
    pub(crate) record: Option<IdentityFeedRecordResponse>,
    pub(crate) total_count: Option<u64>,
    pub(crate) status: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ReverseNamesInputResponse {
    pub(crate) address: String,
    pub(crate) coin_type: u64,
    pub(crate) roles: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct IdentityPaginationResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) next_page_cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) total_count: Option<u64>,
    pub(crate) has_more: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct NameRecordResponse {
    pub(crate) name: String,
    pub(crate) normalized_name: String,
    pub(crate) corrected_input_normalization: bool,
    pub(crate) namehash: String,
    pub(crate) owner_address: Option<String>,
    pub(crate) manager_address: Option<String>,
    pub(crate) primary_address: Option<String>,
    pub(crate) coin_type_addresses: BTreeMap<String, String>,
    pub(crate) text_records: BTreeMap<String, String>,
    pub(crate) resolver_address: Option<String>,
    pub(crate) expiration: Option<i64>,
    pub(crate) token_id: Option<String>,
    pub(crate) network: String,
    pub(crate) as_of: IdentityAsOfResponse,
    pub(crate) status: String,
    pub(crate) unsupported_fields: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ReverseNameRecordResponse {
    #[serde(flatten)]
    pub(crate) record: NameRecordResponse,
    pub(crate) is_primary: bool,
    pub(crate) relation_facets: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct IdentityFeedRecordResponse {
    pub(crate) name: String,
    pub(crate) normalized_name: String,
    pub(crate) namehash: String,
    pub(crate) namespace: String,
    pub(crate) network: String,
    pub(crate) is_primary: bool,
    pub(crate) relation_facets: Vec<String>,
    pub(crate) status: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct IdentityAsOfResponse {
    pub(crate) chain_positions: JsonValue,
    pub(crate) as_of_timestamp: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct IndexingStatusResponse {
    pub(crate) status: String,
    pub(crate) chains: BTreeMap<String, IndexingStatusChainResponse>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct IndexingStatusChainResponse {
    pub(crate) canonical_block: Option<i64>,
    pub(crate) safe_block: Option<i64>,
    pub(crate) finalized_block: Option<i64>,
    pub(crate) latest_projected_block: Option<i64>,
    pub(crate) latest_projected_timestamp: Option<String>,
    pub(crate) projection_lag_blocks: Option<i64>,
    pub(crate) projection_lag_seconds: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct NamespaceMetadataResponse {
    pub(crate) data: NamespaceMetadataData,
    pub(crate) declared_state: NamespaceMetadataDeclaredState,
    pub(crate) verified_state: Option<()>,
    pub(crate) provenance: NamespaceMetadataProvenance,
    pub(crate) coverage: CoverageResponse,
    pub(crate) chain_positions: BTreeMap<String, ChainPositionResponse>,
    pub(crate) consistency: String,
    pub(crate) last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct NamespaceMetadataData {
    pub(crate) namespace: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct NamespaceMetadataDeclaredState {
    pub(crate) active_manifest_count: usize,
    pub(crate) active_source_families: Vec<String>,
    pub(crate) chains: Vec<String>,
    pub(crate) normalizer_versions: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct NamespaceMetadataProvenance {
    pub(crate) normalized_event_ids: Vec<String>,
    pub(crate) raw_fact_refs: Vec<String>,
    pub(crate) manifest_versions: Vec<ManifestVersionRef>,
    pub(crate) execution_trace_id: Option<String>,
    pub(crate) derivation_kind: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct NamespaceManifestsResponse {
    pub(crate) data: NamespaceManifestsData,
    pub(crate) declared_state: NamespaceManifestsDeclaredState,
    pub(crate) verified_state: Option<()>,
    pub(crate) provenance: NamespaceManifestsProvenance,
    pub(crate) coverage: CoverageResponse,
    pub(crate) chain_positions: BTreeMap<String, ChainPositionResponse>,
    pub(crate) consistency: String,
    pub(crate) last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct NamespaceManifestsData {
    pub(crate) namespace: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct NamespaceManifestsDeclaredState {
    pub(crate) manifests: Vec<NamespaceManifestEntry>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct NamespaceManifestEntry {
    pub(crate) manifest_version: u64,
    pub(crate) source_family: String,
    pub(crate) chain: String,
    pub(crate) deployment_epoch: String,
    pub(crate) normalizer_version: String,
    pub(crate) capability_flags: BTreeMap<String, CapabilityFlag>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ManifestVersionRef {
    pub(crate) manifest_version: u64,
    pub(crate) source_family: String,
    pub(crate) chain: String,
    pub(crate) deployment_epoch: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct NamespaceManifestsProvenance {
    pub(crate) normalized_event_ids: Vec<String>,
    pub(crate) raw_fact_refs: Vec<String>,
    pub(crate) manifest_versions: Vec<ManifestVersionRef>,
    pub(crate) execution_trace_id: Option<String>,
    pub(crate) derivation_kind: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct NameResponse {
    pub(crate) data: JsonValue,
    pub(crate) declared_state: JsonValue,
    pub(crate) verified_state: Option<()>,
    pub(crate) provenance: JsonValue,
    pub(crate) coverage: JsonValue,
    pub(crate) chain_positions: JsonValue,
    pub(crate) consistency: String,
    pub(crate) last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ResolutionResponse {
    pub(crate) data: JsonValue,
    pub(crate) declared_state: Option<JsonValue>,
    pub(crate) verified_state: Option<JsonValue>,
    pub(crate) provenance: JsonValue,
    pub(crate) coverage: JsonValue,
    pub(crate) chain_positions: JsonValue,
    pub(crate) consistency: String,
    pub(crate) last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct PrimaryNameResponse {
    pub(crate) data: JsonValue,
    pub(crate) declared_state: Option<JsonValue>,
    pub(crate) verified_state: Option<JsonValue>,
    pub(crate) provenance: JsonValue,
    pub(crate) coverage: JsonValue,
    pub(crate) chain_positions: JsonValue,
    pub(crate) consistency: String,
    pub(crate) last_updated: String,
}

pub(crate) type ResolverResponse = NameResponse;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PrimaryNameTupleState {
    ProjectionUnavailable,
    TupleMissing,
    TuplePresent(PrimaryNameCurrentRow),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PrimaryNameLookupState {
    pub(crate) tuple_state: PrimaryNameTupleState,
    pub(crate) normalized_claim_name: Option<String>,
    pub(crate) persisted_verified: Option<PersistedPrimaryNameVerifiedReadback>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PersistedPrimaryNameVerifiedReadback {
    pub(crate) verified_primary_name: JsonValue,
    pub(crate) provenance: JsonValue,
    pub(crate) finished_at: OffsetDateTime,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct HistoryResponse {
    pub(crate) data: Vec<JsonValue>,
    pub(crate) declared_state: JsonValue,
    pub(crate) verified_state: Option<()>,
    pub(crate) provenance: JsonValue,
    pub(crate) coverage: CoverageResponse,
    pub(crate) chain_positions: JsonValue,
    pub(crate) page: HistoryPageResponse,
    pub(crate) consistency: String,
    pub(crate) last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ChildrenResponse {
    pub(crate) data: Vec<JsonValue>,
    pub(crate) declared_state: JsonValue,
    pub(crate) verified_state: Option<()>,
    pub(crate) provenance: JsonValue,
    pub(crate) coverage: CoverageResponse,
    pub(crate) chain_positions: JsonValue,
    pub(crate) page: HistoryPageResponse,
    pub(crate) consistency: String,
    pub(crate) last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct AddressNamesResponse {
    pub(crate) data: Vec<JsonValue>,
    pub(crate) declared_state: JsonValue,
    pub(crate) verified_state: Option<()>,
    pub(crate) provenance: JsonValue,
    pub(crate) coverage: CoverageResponse,
    pub(crate) chain_positions: JsonValue,
    pub(crate) page: HistoryPageResponse,
    pub(crate) consistency: String,
    pub(crate) last_updated: String,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct AddressNamesResponseSupplement {
    pub(crate) provenances: Vec<JsonValue>,
    pub(crate) chain_positions: Vec<JsonValue>,
    pub(crate) canonicality_summaries: Vec<JsonValue>,
    pub(crate) last_recomputed_at: Vec<OffsetDateTime>,
}

#[derive(Clone, Debug)]
pub(crate) struct AddressNameExpansionFacts {
    pub(crate) status: JsonValue,
    pub(crate) expiry: JsonValue,
    pub(crate) record_count: JsonValue,
}

impl Default for AddressNameExpansionFacts {
    fn default() -> Self {
        Self {
            status: JsonValue::Null,
            expiry: JsonValue::Null,
            record_count: JsonValue::Null,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ResourcePermissionsResponse {
    pub(crate) data: Vec<JsonValue>,
    pub(crate) declared_state: JsonValue,
    pub(crate) verified_state: Option<()>,
    pub(crate) provenance: JsonValue,
    pub(crate) coverage: CoverageResponse,
    pub(crate) chain_positions: JsonValue,
    pub(crate) page: HistoryPageResponse,
    pub(crate) consistency: String,
    pub(crate) last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct CoverageResponse {
    pub(crate) status: String,
    pub(crate) exhaustiveness: String,
    pub(crate) source_classes_considered: Vec<String>,
    pub(crate) enumeration_basis: String,
    pub(crate) unsupported_reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ChainPositionResponse {
    pub(crate) chain_id: String,
    pub(crate) block_number: i64,
    pub(crate) block_hash: String,
    pub(crate) timestamp: String,
}

impl From<ActiveManifestVersion> for NamespaceManifestEntry {
    fn from(value: ActiveManifestVersion) -> Self {
        Self {
            manifest_version: value.manifest_version,
            source_family: value.source_family,
            chain: value.chain,
            deployment_epoch: value.deployment_epoch,
            normalizer_version: value.normalizer_version,
            capability_flags: value.capability_flags,
        }
    }
}

impl From<&NamespaceManifestEntry> for ManifestVersionRef {
    fn from(value: &NamespaceManifestEntry) -> Self {
        Self {
            manifest_version: value.manifest_version,
            source_family: value.source_family.clone(),
            chain: value.chain.clone(),
            deployment_epoch: value.deployment_epoch.clone(),
        }
    }
}

impl From<&ActiveManifestVersion> for ManifestVersionRef {
    fn from(value: &ActiveManifestVersion) -> Self {
        Self {
            manifest_version: value.manifest_version,
            source_family: value.source_family.clone(),
            chain: value.chain.clone(),
            deployment_epoch: value.deployment_epoch.clone(),
        }
    }
}
