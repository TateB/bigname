#![recursion_limit = "256"]

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    net::SocketAddr,
};

use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use bigname_manifests::{
    ActiveManifestVersion, CapabilityFlag, NamespaceManifestSnapshot,
    load_namespace_manifest_snapshot,
};
use bigname_storage::{
    AddressNameCurrentEntry, AddressNameRelation, AddressNamesCurrentDedupe, ChildrenCurrentRow,
    DatabaseConfig, ExecutionCacheKey, ExecutionOutcome, ExecutionTrace, HistoryEvent,
    HistoryScope, NameCurrentRow, PermissionScope, PermissionsCurrentRow, PrimaryNameClaimStatus,
    PrimaryNameCurrentRow, RecordInventoryCurrentRow, ResolverCurrentRow, SurfaceBindingKind,
    VERIFIED_PRIMARY_NAME_INVALIDATION_KEY, VERIFIED_PRIMARY_NAME_LOOKUP_KEY,
    VERIFIED_PRIMARY_NAME_REQUEST_TYPE, collapse_address_name_current_rows, load_address_history,
    load_address_names_current, load_children_current, load_execution_outcome,
    load_execution_trace, load_name_current, load_name_history, load_name_surface,
    load_permissions_current, load_primary_name_current_snapshot, load_record_inventory_current,
    load_resolver_current, load_resource, load_resource_history,
    load_surface_bindings_by_logical_name_id, load_surface_bindings_by_resource_id,
};
use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, json};
use sqlx::{
    PgPool, Row,
    types::{
        JsonValue, Uuid,
        time::{OffsetDateTime, UtcOffset},
    },
};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "bigname-api", about = "Bootstrap API process for bigname")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Serve(ServeArgs),
    PrintOpenapi,
}

#[derive(Args, Debug)]
struct ServeArgs {
    #[arg(long, env = "BIGNAME_API_BIND_ADDR", default_value = "127.0.0.1:3000")]
    bind_addr: SocketAddr,
    #[command(flatten)]
    database: DatabaseConfig,
}

#[derive(Clone)]
struct AppState {
    phase: &'static str,
    pool: PgPool,
}

#[derive(Clone, Copy)]
struct ApiRouteDefinition {
    id: ApiRouteId,
    path: &'static str,
    published_in_contract: bool,
}

#[derive(Clone, Copy)]
enum ApiRouteId {
    Health,
    AddressNames,
    AddressHistory,
    PrimaryNames,
    Coverage,
    ExplainSurfaceBinding,
    ExplainAuthorityControl,
    ExplainResolutionExecution,
    NamespaceMetadata,
    NameChildren,
    NameCurrent,
    ResolveCurrent,
    ResolutionCurrent,
    ResolverCurrent,
    NameHistory,
    ResourceHistory,
    ResourcePermissions,
    NamespaceManifests,
}

const API_ROUTE_DEFINITIONS: &[ApiRouteDefinition] = &[
    ApiRouteDefinition {
        id: ApiRouteId::Health,
        path: "/healthz",
        published_in_contract: false,
    },
    ApiRouteDefinition {
        id: ApiRouteId::AddressNames,
        path: "/v1/addresses/{address}/names",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::AddressHistory,
        path: "/v1/history/addresses/{address}",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::PrimaryNames,
        path: "/v1/primary-names/{address}",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::Coverage,
        path: "/v1/coverage/{namespace}/{name}",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::ExplainSurfaceBinding,
        path: "/v1/explain/names/{namespace}/{name}/surface-binding",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::ExplainAuthorityControl,
        path: "/v1/explain/names/{namespace}/{name}/authority-control",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::ExplainResolutionExecution,
        path: "/v1/explain/resolutions/{namespace}/{name}/execution",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::NamespaceMetadata,
        path: "/v1/namespaces/{namespace}",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::NameChildren,
        path: "/v1/names/{namespace}/{name}/children",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::NameCurrent,
        path: "/v1/names/{namespace}/{name}",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::ResolveCurrent,
        path: "/v1/resolve/{name}",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::ResolutionCurrent,
        path: "/v1/resolutions/{namespace}/{name}",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::ResolverCurrent,
        path: "/v1/resolvers/{chain_id}/{resolver_address}",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::NameHistory,
        path: "/v1/history/names/{namespace}/{name}",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::ResourceHistory,
        path: "/v1/history/resources/{resource_id}",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::ResourcePermissions,
        path: "/v1/resources/{resource_id}/permissions",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::NamespaceManifests,
        path: "/v1/manifests/{namespace}",
        published_in_contract: true,
    },
];

#[derive(Serialize)]
struct HealthResponse {
    service: &'static str,
    phase: &'static str,
    status: &'static str,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NamespaceMetadataResponse {
    data: NamespaceMetadataData,
    declared_state: NamespaceMetadataDeclaredState,
    verified_state: Option<()>,
    provenance: NamespaceMetadataProvenance,
    coverage: CoverageResponse,
    chain_positions: BTreeMap<String, ChainPositionResponse>,
    consistency: String,
    last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NamespaceMetadataData {
    namespace: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NamespaceMetadataDeclaredState {
    active_manifest_count: usize,
    active_source_families: Vec<String>,
    chains: Vec<String>,
    normalizer_versions: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NamespaceMetadataProvenance {
    normalized_event_ids: Vec<String>,
    raw_fact_refs: Vec<String>,
    manifest_versions: Vec<ManifestVersionRef>,
    execution_trace_id: Option<String>,
    derivation_kind: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NamespaceManifestsResponse {
    data: NamespaceManifestsData,
    declared_state: NamespaceManifestsDeclaredState,
    verified_state: Option<()>,
    provenance: NamespaceManifestsProvenance,
    coverage: CoverageResponse,
    chain_positions: BTreeMap<String, ChainPositionResponse>,
    consistency: String,
    last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NamespaceManifestsData {
    namespace: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NamespaceManifestsDeclaredState {
    manifests: Vec<NamespaceManifestEntry>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NamespaceManifestEntry {
    manifest_version: u64,
    source_family: String,
    chain: String,
    deployment_epoch: String,
    normalizer_version: String,
    capability_flags: BTreeMap<String, CapabilityFlag>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ManifestVersionRef {
    manifest_version: u64,
    source_family: String,
    chain: String,
    deployment_epoch: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NamespaceManifestsProvenance {
    normalized_event_ids: Vec<String>,
    raw_fact_refs: Vec<String>,
    manifest_versions: Vec<ManifestVersionRef>,
    execution_trace_id: Option<String>,
    derivation_kind: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NameResponse {
    data: JsonValue,
    declared_state: JsonValue,
    verified_state: Option<()>,
    provenance: JsonValue,
    coverage: JsonValue,
    chain_positions: JsonValue,
    consistency: String,
    last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ResolutionResponse {
    data: JsonValue,
    declared_state: Option<JsonValue>,
    verified_state: Option<JsonValue>,
    provenance: JsonValue,
    coverage: JsonValue,
    chain_positions: JsonValue,
    consistency: String,
    last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PrimaryNameResponse {
    data: JsonValue,
    declared_state: Option<JsonValue>,
    verified_state: Option<JsonValue>,
    provenance: JsonValue,
    coverage: JsonValue,
    chain_positions: JsonValue,
    consistency: String,
    last_updated: String,
}

type ResolverResponse = NameResponse;

#[derive(Clone, Debug, Default, Deserialize)]
struct HistoryQuery {
    scope: Option<String>,
    cursor: Option<String>,
    page_size: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct PermissionsQuery {
    subject: Option<String>,
    scope: Option<String>,
    cursor: Option<String>,
    page_size: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ChildrenQuery {
    surface_classes: Option<String>,
    include: Option<String>,
    cursor: Option<String>,
    page_size: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct AddressNamesQuery {
    namespace: Option<String>,
    relation: Option<String>,
    dedupe_by: Option<String>,
    include: Option<String>,
    cursor: Option<String>,
    page_size: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct AddressNamesIncludeOptions {
    role_summary: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct AddressHistoryQuery {
    namespace: Option<String>,
    relation: Option<String>,
    scope: Option<String>,
    cursor: Option<String>,
    page_size: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ResolutionQuery {
    mode: Option<String>,
    records: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ResolutionExecutionExplainQuery {
    records: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct PrimaryNameQuery {
    namespace: Option<String>,
    coin_type: Option<String>,
    mode: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResolutionMode {
    Declared,
    Verified,
    Both,
}

impl ResolutionMode {
    fn includes_declared(self) -> bool {
        matches!(self, Self::Declared | Self::Both)
    }

    fn includes_verified(self) -> bool {
        matches!(self, Self::Verified | Self::Both)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResolutionRecordKey {
    record_key: String,
    record_family: String,
    selector_key: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PrimaryNameTupleState {
    ProjectionUnavailable,
    TupleMissing,
    TuplePresent(PrimaryNameCurrentRow),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PrimaryNameLookupState {
    tuple_state: PrimaryNameTupleState,
    normalized_claim_name: Option<String>,
    persisted_verified: Option<PersistedPrimaryNameVerifiedReadback>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PersistedPrimaryNameVerifiedReadback {
    verified_primary_name: JsonValue,
    provenance: JsonValue,
    finished_at: OffsetDateTime,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct HistoryResponse {
    data: Vec<JsonValue>,
    declared_state: JsonValue,
    verified_state: Option<()>,
    provenance: JsonValue,
    coverage: CoverageResponse,
    chain_positions: JsonValue,
    page: HistoryPageResponse,
    consistency: String,
    last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ChildrenResponse {
    data: Vec<JsonValue>,
    declared_state: JsonValue,
    verified_state: Option<()>,
    provenance: JsonValue,
    coverage: CoverageResponse,
    chain_positions: JsonValue,
    page: HistoryPageResponse,
    consistency: String,
    last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct AddressNamesResponse {
    data: Vec<JsonValue>,
    declared_state: JsonValue,
    verified_state: Option<()>,
    provenance: JsonValue,
    coverage: CoverageResponse,
    chain_positions: JsonValue,
    page: HistoryPageResponse,
    consistency: String,
    last_updated: String,
}

#[derive(Clone, Debug, Default)]
struct AddressNamesResponseSupplement {
    provenances: Vec<JsonValue>,
    chain_positions: Vec<JsonValue>,
    canonicality_summaries: Vec<JsonValue>,
    last_recomputed_at: Vec<OffsetDateTime>,
}

#[derive(Clone, Debug)]
struct AddressNameExpansionFacts {
    status: JsonValue,
    expiry: JsonValue,
    record_count: JsonValue,
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
struct ResourcePermissionsResponse {
    data: Vec<JsonValue>,
    declared_state: JsonValue,
    verified_state: Option<()>,
    provenance: JsonValue,
    coverage: CoverageResponse,
    chain_positions: JsonValue,
    page: HistoryPageResponse,
    consistency: String,
    last_updated: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct HistoryPageResponse {
    cursor: Option<String>,
    next_cursor: Option<String>,
    page_size: u64,
    sort: String,
}

const DEFAULT_PAGE_SIZE: u64 = 50;
const MAX_PAGE_SIZE: u64 = 200;
const CURSOR_VERSION: u8 = 1;

#[derive(Clone, Debug)]
struct PaginationRequest {
    active: bool,
    cursor: Option<String>,
    page_size: u64,
}

#[derive(Clone, Debug)]
struct PaginationWindow {
    start: usize,
    end: usize,
    page: HistoryPageResponse,
}

#[derive(Clone, Debug)]
struct CursorSpec {
    route: &'static str,
    anchor: String,
    sort: &'static str,
    filters: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CursorEnvelope {
    version: u8,
    route: String,
    anchor: String,
    sort: String,
    filters: BTreeMap<String, String>,
    item: BTreeMap<String, String>,
}

impl CursorSpec {
    fn envelope(&self, item: BTreeMap<String, String>) -> CursorEnvelope {
        CursorEnvelope {
            version: CURSOR_VERSION,
            route: self.route.to_owned(),
            anchor: self.anchor.clone(),
            sort: self.sort.to_owned(),
            filters: self.filters.clone(),
            item,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CoverageResponse {
    status: String,
    exhaustiveness: String,
    source_classes_considered: Vec<String>,
    enumeration_basis: String,
    unsupported_reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ChainPositionResponse {
    chain_id: String,
    block_number: i64,
    block_hash: String,
    timestamp: String,
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ErrorResponse {
    error: ErrorBody,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ErrorBody {
    code: String,
    message: String,
    details: BTreeMap<String, String>,
}

struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn internal_error(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal_error",
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorResponse {
                error: ErrorBody {
                    code: self.code.to_owned(),
                    message: self.message,
                    details: BTreeMap::new(),
                },
            }),
        )
            .into_response()
    }
}

type ApiResult<T> = std::result::Result<T, ApiError>;

const PUBLIC_NAMESPACES: &[&str] = &["ens", "basenames"];
const VERIFIED_RESOLUTION_REQUEST_TYPE: &str = "verified_resolution";

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing("bigname-api");

    match Cli::parse().command {
        Command::Serve(args) => serve(args).await,
        Command::PrintOpenapi => {
            print!("{}", render_openapi_document());
            Ok(())
        }
    }
}

include!("openapi.rs");

include!("handlers.rs");

include!("responses.rs");

include!("support.rs");

#[cfg(test)]
mod tests;
