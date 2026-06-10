use std::collections::BTreeMap;

use bigname_manifests::{ActiveManifestVersion, CapabilityFlag};
use bigname_storage::PrimaryNameCurrentRow;
use serde::{Deserialize, Serialize};
use sqlx::types::{JsonValue, time::OffsetDateTime};

use crate::pagination::HistoryPageResponse;

fn json_value_is_null(value: &JsonValue) -> bool {
    value.is_null()
}

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

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IdentityLookupInput {
    pub(crate) profile: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) inputs: Vec<IdentityLookupInputItem>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IdentityLookupInputItem {
    pub(crate) id: String,
    pub(crate) kind: String,
    pub(crate) name: Option<String>,
    pub(crate) address: Option<String>,
    pub(crate) coin_type: Option<u64>,
    pub(crate) roles: Option<Vec<String>>,
    pub(crate) page_size: Option<u64>,
    pub(crate) cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct IdentityLookupResponse {
    pub(crate) results: Vec<IdentityLookupResult>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct IdentityLookupResult {
    pub(crate) id: String,
    pub(crate) kind: String,
    pub(crate) status: String,
    pub(crate) input: IdentityLookupResultInput,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) normalization: Option<NormalizationInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) record: Option<Option<NativeIdentityRecordResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) records: Option<Vec<NativeIdentityRecordResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) page: Option<IdentityLookupPageResponse>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct IdentityLookupResultInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) coin_type: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) roles: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct NormalizationInfo {
    pub(crate) changed: bool,
    pub(crate) input_name: String,
    pub(crate) reason: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct IdentityLookupPageResponse {
    pub(crate) next_cursor: Option<String>,
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
pub(crate) struct NativeIdentityRecordResponse {
    pub(crate) name: String,
    pub(crate) namespace: String,
    pub(crate) namehash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) owner_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) manager_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) primary_address: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) coin_type_addresses: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) text_records: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) resolver_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) expiration: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) token_id: Option<String>,
    pub(crate) network: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) is_primary: Option<bool>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) relation_facets: Vec<String>,
    pub(crate) status: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) unsupported_fields: Vec<String>,
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
pub(crate) struct PublicStatusResponse {
    pub(crate) data: IndexingStatusResponse,
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
    #[serde(skip_serializing_if = "Option::is_none")]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) execution_trace_id: Option<String>,
    pub(crate) derivation_kind: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct NameResponse {
    pub(crate) data: JsonValue,
    pub(crate) declared_state: JsonValue,
    pub(crate) verified_state: Option<()>,
    #[serde(default, skip_serializing_if = "json_value_is_null")]
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
    #[serde(default, skip_serializing_if = "json_value_is_null")]
    pub(crate) provenance: JsonValue,
    #[serde(default, skip_serializing_if = "json_value_is_null")]
    pub(crate) coverage: JsonValue,
    #[serde(default, skip_serializing_if = "json_value_is_null")]
    pub(crate) chain_positions: JsonValue,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub(crate) consistency: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub(crate) last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct PrimaryNameResponse {
    pub(crate) data: JsonValue,
    pub(crate) declared_state: Option<JsonValue>,
    pub(crate) verified_state: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "json_value_is_null")]
    pub(crate) provenance: JsonValue,
    pub(crate) coverage: JsonValue,
    pub(crate) chain_positions: JsonValue,
    pub(crate) consistency: String,
    pub(crate) last_updated: String,
}

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
    pub(crate) on_demand_claim: OnDemandPrimaryNameClaimState,
    pub(crate) on_demand_verified: OnDemandPrimaryNameVerificationState,
    pub(crate) persisted_verified: Option<PersistedPrimaryNameVerifiedReadback>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OnDemandPrimaryNameClaimState {
    NotAttempted,
    Unavailable,
    NotFound,
    Found(OnDemandPrimaryNameClaim),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OnDemandPrimaryNameClaim {
    pub(crate) normalized_name: String,
    pub(crate) resolver_address: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OnDemandPrimaryNameVerificationState {
    NotAttempted,
    Verified(JsonValue),
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
    #[serde(default, skip_serializing_if = "json_value_is_null")]
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
    #[serde(default, skip_serializing_if = "json_value_is_null")]
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
    #[serde(default, skip_serializing_if = "json_value_is_null")]
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
    #[serde(default, skip_serializing_if = "json_value_is_null")]
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
