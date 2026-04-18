use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    net::SocketAddr,
};

use anyhow::{Context, Result};
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
    DatabaseConfig, HistoryEvent, HistoryScope, NameCurrentRow, PermissionScope,
    PermissionsCurrentRow, ResolverCurrentRow, SurfaceBindingKind,
    collapse_address_name_current_rows, load_address_history, load_address_names_current,
    load_children_current, load_name_current, load_name_history, load_name_surface,
    load_permissions_current, load_resolver_current, load_resource, load_resource_history,
    load_surface_bindings_by_logical_name_id, load_surface_bindings_by_resource_id,
};
use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, json};
use sqlx::{
    PgPool,
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
    Coverage,
    ExplainSurfaceBinding,
    ExplainAuthorityControl,
    NamespaceMetadata,
    NameChildren,
    NameCurrent,
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

impl ApiRouteDefinition {
    fn register(self, router: Router<AppState>) -> Router<AppState> {
        match self.id {
            ApiRouteId::Health => router.route(self.path, get(health)),
            ApiRouteId::AddressNames => router.route(self.path, get(address_names)),
            ApiRouteId::AddressHistory => router.route(self.path, get(address_history)),
            ApiRouteId::Coverage => router.route(self.path, get(coverage_current)),
            ApiRouteId::ExplainSurfaceBinding => {
                router.route(self.path, get(explain_surface_binding_current))
            }
            ApiRouteId::ExplainAuthorityControl => {
                router.route(self.path, get(explain_authority_control_current))
            }
            ApiRouteId::NamespaceMetadata => router.route(self.path, get(namespace_metadata)),
            ApiRouteId::NameChildren => router.route(self.path, get(name_children)),
            ApiRouteId::NameCurrent => router.route(self.path, get(name_current)),
            ApiRouteId::ResolutionCurrent => router.route(self.path, get(resolution_current)),
            ApiRouteId::ResolverCurrent => router.route(self.path, get(resolver_current)),
            ApiRouteId::NameHistory => router.route(self.path, get(name_history)),
            ApiRouteId::ResourceHistory => router.route(self.path, get(resource_history)),
            ApiRouteId::ResourcePermissions => router.route(self.path, get(resource_permissions)),
            ApiRouteId::NamespaceManifests => router.route(self.path, get(namespace_manifests)),
        }
    }

    fn openapi_path_item(self) -> Option<JsonValue> {
        self.published_in_contract
            .then(|| json!({ "get": self.id.openapi_operation() }))
    }
}

impl ApiRouteId {
    fn operation_id(self) -> &'static str {
        match self {
            Self::Health => "health",
            Self::AddressNames => "address_names",
            Self::AddressHistory => "address_history",
            Self::Coverage => "coverage_current",
            Self::ExplainSurfaceBinding => "explain_surface_binding_current",
            Self::ExplainAuthorityControl => "explain_authority_control_current",
            Self::NamespaceMetadata => "namespace_metadata",
            Self::NameChildren => "name_children",
            Self::NameCurrent => "name_current",
            Self::ResolutionCurrent => "resolution_current",
            Self::ResolverCurrent => "resolver_current",
            Self::NameHistory => "name_history",
            Self::ResourceHistory => "resource_history",
            Self::ResourcePermissions => "resource_permissions",
            Self::NamespaceManifests => "namespace_manifests",
        }
    }

    fn openapi_operation(self) -> JsonValue {
        match self {
            Self::Health => openapi_json_get_operation(
                self.operation_id(),
                "Health check",
                "Health",
                Vec::new(),
                "HealthResponse",
                false,
                false,
            ),
            Self::AddressNames => openapi_json_get_operation(
                self.operation_id(),
                "Address-to-surface collection",
                "Collections",
                vec![
                    address_path_parameter(),
                    namespace_query_parameter(),
                    relation_query_parameter(),
                    dedupe_by_query_parameter(),
                    csv_query_parameter(
                        "include",
                        "Optional collection expansions. `role_summary` is the only shipped expansion.",
                        json!({
                            "type": "string",
                            "enum": ["role_summary"],
                        }),
                    ),
                    cursor_query_parameter(),
                    page_size_query_parameter(),
                ],
                "CollectionResponse",
                true,
                false,
            ),
            Self::AddressHistory => openapi_json_get_operation(
                self.operation_id(),
                "Address activity across related surfaces and resources",
                "History",
                vec![
                    address_path_parameter(),
                    namespace_query_parameter(),
                    relation_query_parameter(),
                    history_scope_query_parameter(),
                    cursor_query_parameter(),
                    page_size_query_parameter(),
                ],
                "CollectionResponse",
                true,
                false,
            ),
            Self::Coverage => openapi_json_get_operation(
                self.operation_id(),
                "Single-name coverage and explain details",
                "Coverage",
                vec![namespace_path_parameter(), name_path_parameter()],
                "ExactNameResponse",
                false,
                true,
            ),
            Self::ExplainSurfaceBinding => openapi_json_get_operation(
                self.operation_id(),
                "Current surface-binding explain view for one exact name",
                "Explain",
                vec![namespace_path_parameter(), name_path_parameter()],
                "ExactNameResponse",
                false,
                true,
            ),
            Self::ExplainAuthorityControl => openapi_json_get_operation(
                self.operation_id(),
                "Current authority/control explain view for one exact name",
                "Explain",
                vec![namespace_path_parameter(), name_path_parameter()],
                "ExactNameResponse",
                false,
                true,
            ),
            Self::NamespaceMetadata => openapi_json_get_operation(
                self.operation_id(),
                "Namespace metadata and support status",
                "Namespaces",
                vec![namespace_path_parameter()],
                "NamespaceMetadataResponse",
                false,
                true,
            ),
            Self::NameChildren => openapi_json_get_operation(
                self.operation_id(),
                "Declared child collection by default",
                "Collections",
                vec![
                    namespace_path_parameter(),
                    name_path_parameter(),
                    csv_query_parameter(
                        "surface_classes",
                        "Requested child surface classes. Only `declared` is currently supported.",
                        json!({
                            "type": "string",
                            "default": "declared",
                        }),
                    ),
                    csv_query_parameter(
                        "include",
                        "Optional collection expansions. `counts` includes `declared_state.subname_count`.",
                        json!({
                            "type": "string",
                            "enum": ["counts"],
                        }),
                    ),
                    cursor_query_parameter(),
                    page_size_query_parameter(),
                ],
                "CollectionResponse",
                true,
                true,
            ),
            Self::NameCurrent => openapi_json_get_operation(
                self.operation_id(),
                "Exact name lookup",
                "Names",
                vec![namespace_path_parameter(), name_path_parameter()],
                "ExactNameResponse",
                false,
                true,
            ),
            Self::ResolutionCurrent => openapi_json_get_operation(
                self.operation_id(),
                "Resolution topology, inventory, and verified reads",
                "Resolution",
                vec![
                    namespace_path_parameter(),
                    name_path_parameter(),
                    resolution_mode_query_parameter(),
                    csv_query_parameter(
                        "records",
                        "Comma-separated record selectors. Required when `mode` is `verified` or `both`.",
                        json!({
                            "type": "string",
                        }),
                    ),
                ],
                "ResolutionResponse",
                true,
                true,
            ),
            Self::ResolverCurrent => openapi_json_get_operation(
                self.operation_id(),
                "Resolver overview",
                "Resolvers",
                vec![chain_id_path_parameter(), resolver_address_path_parameter()],
                "ResolverResponse",
                false,
                true,
            ),
            Self::NameHistory => openapi_json_get_operation(
                self.operation_id(),
                "Surface or combined history",
                "History",
                vec![
                    namespace_path_parameter(),
                    name_path_parameter(),
                    history_scope_query_parameter(),
                    cursor_query_parameter(),
                    page_size_query_parameter(),
                ],
                "CollectionResponse",
                true,
                true,
            ),
            Self::ResourceHistory => openapi_json_get_operation(
                self.operation_id(),
                "Resource history",
                "History",
                vec![
                    resource_id_path_parameter(),
                    history_scope_query_parameter(),
                    cursor_query_parameter(),
                    page_size_query_parameter(),
                ],
                "CollectionResponse",
                true,
                true,
            ),
            Self::ResourcePermissions => openapi_json_get_operation(
                self.operation_id(),
                "Resource-centric effective permissions",
                "Collections",
                vec![
                    resource_id_path_parameter(),
                    query_parameter(
                        "subject",
                        "Optional subject filter for the current effective permissions rows.",
                        json!({
                            "type": "string",
                        }),
                    ),
                    query_parameter(
                        "scope",
                        "Optional scope filter. Accepts `root`, `registry`, `resource`, `resolver:{chain_id}:{resolver_address}`, `record_manager:{chain_id}:{manager_address}`, `migration_derived:{resource_id}`, or `transport_derived:{transport}`.",
                        json!({
                            "type": "string",
                        }),
                    ),
                    cursor_query_parameter(),
                    page_size_query_parameter(),
                ],
                "CollectionResponse",
                true,
                false,
            ),
            Self::NamespaceManifests => openapi_json_get_operation(
                self.operation_id(),
                "Active manifest versions and capabilities",
                "Namespaces",
                vec![namespace_path_parameter()],
                "NamespaceManifestsResponse",
                false,
                true,
            ),
        }
    }
}

async fn serve(args: ServeArgs) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let state = AppState {
        phase: bigname_domain::bootstrap_phase(),
        pool,
    };
    let router = app_router(state);
    let listener = tokio::net::TcpListener::bind(args.bind_addr)
        .await
        .context("failed to bind the API listener")?;

    info!(
        service = "api",
        bind_addr = %args.bind_addr,
        phase = bigname_domain::bootstrap_phase(),
        "API booted"
    );

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal("api"))
        .await
        .context("API server exited unexpectedly")
}

fn app_router(state: AppState) -> Router {
    API_ROUTE_DEFINITIONS
        .iter()
        .copied()
        .fold(Router::new(), |router, route| route.register(router))
        .with_state(state)
}

fn render_openapi_document() -> String {
    let mut rendered =
        serde_json::to_string_pretty(&openapi_document()).expect("OpenAPI document must render");
    rendered.push('\n');
    rendered
}

fn openapi_document() -> JsonValue {
    let mut paths = JsonMap::new();
    for route in API_ROUTE_DEFINITIONS {
        if let Some(path_item) = route.openapi_path_item() {
            paths.insert(route.path.to_owned(), path_item);
        }
    }

    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "bigname API v1",
            "version": "phase-6",
            "description": "Machine-readable publication of the shipped Phase 6 public API surface derived from apps/api/src/main.rs.",
        },
        "paths": JsonValue::Object(paths),
        "components": openapi_components(),
    })
}

fn openapi_components() -> JsonValue {
    json!({
        "schemas": {
            "JsonObject": json_object_schema(),
            "NullValue": json!({ "type": "null" }),
            "Consistency": json!({
                "type": "string",
                "enum": ["head", "safe", "finalized"],
            }),
            "Provenance": json!({
                "type": "object",
                "required": [
                    "normalized_event_ids",
                    "raw_fact_refs",
                    "manifest_versions",
                    "execution_trace_id",
                    "derivation_kind",
                ],
                "properties": {
                    "normalized_event_ids": {
                        "type": "array",
                        "items": { "type": "string" },
                    },
                    "raw_fact_refs": {
                        "type": "array",
                        "items": {},
                    },
                    "manifest_versions": {
                        "type": "array",
                        "items": {},
                    },
                    "execution_trace_id": {
                        "type": ["string", "null"],
                    },
                    "derivation_kind": {
                        "type": "string",
                    },
                },
            }),
            "CoverageResponse": json!({
                "type": "object",
                "required": [
                    "status",
                    "exhaustiveness",
                    "source_classes_considered",
                    "enumeration_basis",
                    "unsupported_reason",
                ],
                "properties": {
                    "status": { "type": "string" },
                    "exhaustiveness": { "type": "string" },
                    "source_classes_considered": {
                        "type": "array",
                        "items": { "type": "string" },
                    },
                    "enumeration_basis": { "type": "string" },
                    "unsupported_reason": {
                        "type": ["string", "null"],
                    },
                },
            }),
            "ChainPositionResponse": json!({
                "type": "object",
                "required": ["chain_id", "block_number", "block_hash", "timestamp"],
                "properties": {
                    "chain_id": { "type": "string" },
                    "block_number": { "type": "integer" },
                    "block_hash": { "type": "string" },
                    "timestamp": {
                        "type": "string",
                        "format": "date-time",
                    },
                },
            }),
            "ChainPositions": json!({
                "type": "object",
                "additionalProperties": schema_ref("ChainPositionResponse"),
            }),
            "HistoryPageResponse": json!({
                "type": "object",
                "required": ["cursor", "next_cursor", "page_size", "sort"],
                "properties": {
                    "cursor": { "type": ["string", "null"] },
                    "next_cursor": { "type": ["string", "null"] },
                    "page_size": {
                        "type": "integer",
                        "minimum": 0,
                    },
                    "sort": { "type": "string" },
                },
            }),
            "ExactNameData": json!({
                "type": "object",
                "required": [
                    "logical_name_id",
                    "namespace",
                    "normalized_name",
                    "canonical_display_name",
                    "namehash",
                    "resource_id",
                    "token_lineage_id",
                    "binding_kind",
                ],
                "properties": {
                    "logical_name_id": { "type": "string" },
                    "namespace": { "type": "string" },
                    "normalized_name": { "type": "string" },
                    "canonical_display_name": { "type": "string" },
                    "namehash": { "type": "string" },
                    "resource_id": {
                        "type": ["string", "null"],
                        "format": "uuid",
                    },
                    "token_lineage_id": {
                        "type": ["string", "null"],
                        "format": "uuid",
                    },
                    "binding_kind": {
                        "type": ["string", "null"],
                    },
                },
            }),
            "ResolverData": json!({
                "type": "object",
                "required": ["chain_id", "resolver_address"],
                "properties": {
                    "chain_id": { "type": "string" },
                    "resolver_address": { "type": "string" },
                },
            }),
            "ExactNameResponse": declared_response_schema(
                schema_ref("ExactNameData"),
                schema_ref("JsonObject"),
            ),
            "ResolverResponse": declared_response_schema(
                schema_ref("ResolverData"),
                schema_ref("JsonObject"),
            ),
            "ResolutionResponse": json!({
                "type": "object",
                "required": [
                    "data",
                    "declared_state",
                    "verified_state",
                    "provenance",
                    "coverage",
                    "chain_positions",
                    "consistency",
                    "last_updated",
                ],
                "properties": {
                    "data": schema_ref("ExactNameData"),
                    "declared_state": {
                        "type": ["object", "null"],
                        "additionalProperties": true,
                    },
                    "verified_state": {
                        "type": ["object", "null"],
                        "additionalProperties": true,
                    },
                    "provenance": schema_ref("Provenance"),
                    "coverage": schema_ref("CoverageResponse"),
                    "chain_positions": schema_ref("ChainPositions"),
                    "consistency": schema_ref("Consistency"),
                    "last_updated": {
                        "type": "string",
                        "format": "date-time",
                    },
                },
            }),
            "CollectionResponse": paginated_declared_response_schema(
                json!({
                    "type": "array",
                    "items": schema_ref("JsonObject"),
                }),
                schema_ref("JsonObject"),
            ),
            "NamespaceData": json!({
                "type": "object",
                "required": ["namespace"],
                "properties": {
                    "namespace": {
                        "type": "string",
                        "enum": PUBLIC_NAMESPACES,
                    },
                },
            }),
            "NamespaceMetadataDeclaredState": json!({
                "type": "object",
                "required": [
                    "active_manifest_count",
                    "active_source_families",
                    "chains",
                    "normalizer_versions",
                ],
                "properties": {
                    "active_manifest_count": {
                        "type": "integer",
                        "minimum": 0,
                    },
                    "active_source_families": {
                        "type": "array",
                        "items": { "type": "string" },
                    },
                    "chains": {
                        "type": "array",
                        "items": { "type": "string" },
                    },
                    "normalizer_versions": {
                        "type": "array",
                        "items": { "type": "string" },
                    },
                },
            }),
            "NamespaceMetadataResponse": declared_response_schema(
                schema_ref("NamespaceData"),
                schema_ref("NamespaceMetadataDeclaredState"),
            ),
            "CapabilityFlag": json!({
                "type": "object",
                "required": ["status", "notes"],
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": ["unsupported", "shadow", "supported"],
                    },
                    "notes": {
                        "type": ["string", "null"],
                    },
                },
            }),
            "NamespaceManifestEntry": json!({
                "type": "object",
                "required": [
                    "manifest_version",
                    "source_family",
                    "chain",
                    "deployment_epoch",
                    "normalizer_version",
                    "capability_flags",
                ],
                "properties": {
                    "manifest_version": {
                        "type": "integer",
                        "minimum": 1,
                    },
                    "source_family": { "type": "string" },
                    "chain": { "type": "string" },
                    "deployment_epoch": { "type": "string" },
                    "normalizer_version": { "type": "string" },
                    "capability_flags": {
                        "type": "object",
                        "additionalProperties": schema_ref("CapabilityFlag"),
                    },
                },
            }),
            "NamespaceManifestsDeclaredState": json!({
                "type": "object",
                "required": ["manifests"],
                "properties": {
                    "manifests": {
                        "type": "array",
                        "items": schema_ref("NamespaceManifestEntry"),
                    },
                },
            }),
            "NamespaceManifestsResponse": declared_response_schema(
                schema_ref("NamespaceData"),
                schema_ref("NamespaceManifestsDeclaredState"),
            ),
            "HealthResponse": json!({
                "type": "object",
                "required": ["service", "phase", "status"],
                "properties": {
                    "service": { "type": "string" },
                    "phase": { "type": "string" },
                    "status": { "type": "string" },
                },
            }),
            "ErrorBody": json!({
                "type": "object",
                "required": ["code", "message", "details"],
                "properties": {
                    "code": { "type": "string" },
                    "message": { "type": "string" },
                    "details": {
                        "type": "object",
                        "additionalProperties": { "type": "string" },
                    },
                },
            }),
            "ErrorResponse": json!({
                "type": "object",
                "required": ["error"],
                "properties": {
                    "error": schema_ref("ErrorBody"),
                },
            }),
        },
    })
}

fn declared_response_schema(data_schema: JsonValue, declared_state_schema: JsonValue) -> JsonValue {
    json!({
        "type": "object",
        "required": [
            "data",
            "declared_state",
            "verified_state",
            "provenance",
            "coverage",
            "chain_positions",
            "consistency",
            "last_updated",
        ],
        "properties": {
            "data": data_schema,
            "declared_state": declared_state_schema,
            "verified_state": schema_ref("NullValue"),
            "provenance": schema_ref("Provenance"),
            "coverage": schema_ref("CoverageResponse"),
            "chain_positions": schema_ref("ChainPositions"),
            "consistency": schema_ref("Consistency"),
            "last_updated": {
                "type": "string",
                "format": "date-time",
            },
        },
    })
}

fn paginated_declared_response_schema(
    data_schema: JsonValue,
    declared_state_schema: JsonValue,
) -> JsonValue {
    let mut schema = declared_response_schema(data_schema, declared_state_schema);
    let object = schema
        .as_object_mut()
        .expect("declared response schema must be an object");
    object
        .get_mut("required")
        .and_then(JsonValue::as_array_mut)
        .expect("declared response schema must define required fields")
        .push(JsonValue::String("page".to_owned()));
    object
        .get_mut("properties")
        .and_then(JsonValue::as_object_mut)
        .expect("declared response schema must define properties")
        .insert("page".to_owned(), schema_ref("HistoryPageResponse"));
    schema
}

fn openapi_json_get_operation(
    operation_id: &'static str,
    summary: &'static str,
    tag: &'static str,
    parameters: Vec<JsonValue>,
    success_schema: &'static str,
    include_bad_request: bool,
    include_not_found: bool,
) -> JsonValue {
    let mut responses = JsonMap::new();
    responses.insert(
        "200".to_owned(),
        json_response("Successful response", success_schema),
    );
    if include_bad_request {
        responses.insert(
            "400".to_owned(),
            json_response("Invalid request", "ErrorResponse"),
        );
    }
    if include_not_found {
        responses.insert(
            "404".to_owned(),
            json_response("Requested resource was not found", "ErrorResponse"),
        );
    }
    responses.insert(
        "500".to_owned(),
        json_response("Internal error", "ErrorResponse"),
    );

    json!({
        "operationId": operation_id,
        "summary": summary,
        "tags": [tag],
        "parameters": parameters,
        "responses": JsonValue::Object(responses),
    })
}

fn json_response(description: &'static str, schema_name: &'static str) -> JsonValue {
    json!({
        "description": description,
        "content": {
            "application/json": {
                "schema": schema_ref(schema_name),
            },
        },
    })
}

fn schema_ref(schema_name: &str) -> JsonValue {
    json!({
        "$ref": format!("#/components/schemas/{schema_name}"),
    })
}

fn json_object_schema() -> JsonValue {
    json!({
        "type": "object",
        "additionalProperties": true,
    })
}

fn path_parameter(
    name: &'static str,
    description: impl Into<String>,
    schema: JsonValue,
) -> JsonValue {
    json!({
        "name": name,
        "in": "path",
        "required": true,
        "description": description.into(),
        "schema": schema,
    })
}

fn query_parameter(
    name: &'static str,
    description: impl Into<String>,
    schema: JsonValue,
) -> JsonValue {
    json!({
        "name": name,
        "in": "query",
        "required": false,
        "description": description.into(),
        "schema": schema,
    })
}

fn csv_query_parameter(
    name: &'static str,
    description: impl Into<String>,
    schema: JsonValue,
) -> JsonValue {
    let mut parameter = query_parameter(name, description, schema);
    let object = parameter
        .as_object_mut()
        .expect("query parameter helper must create an object");
    object.insert("style".to_owned(), JsonValue::String("form".to_owned()));
    object.insert("explode".to_owned(), JsonValue::Bool(false));
    parameter
}

fn namespace_path_parameter() -> JsonValue {
    path_parameter(
        "namespace",
        "Supported namespace identifier.",
        json!({
            "type": "string",
            "enum": PUBLIC_NAMESPACES,
        }),
    )
}

fn name_path_parameter() -> JsonValue {
    path_parameter(
        "name",
        "Normalized name within the namespace.",
        json!({
            "type": "string",
        }),
    )
}

fn address_path_parameter() -> JsonValue {
    path_parameter(
        "address",
        "Address anchor for the collection or history read. Addresses are normalized to lowercase.",
        json!({
            "type": "string",
        }),
    )
}

fn resource_id_path_parameter() -> JsonValue {
    path_parameter(
        "resource_id",
        "Resource identifier anchor.",
        json!({
            "type": "string",
            "format": "uuid",
        }),
    )
}

fn chain_id_path_parameter() -> JsonValue {
    path_parameter(
        "chain_id",
        "Resolver chain identifier.",
        json!({
            "type": "string",
        }),
    )
}

fn resolver_address_path_parameter() -> JsonValue {
    path_parameter(
        "resolver_address",
        "Resolver address anchor. Addresses are normalized to lowercase.",
        json!({
            "type": "string",
        }),
    )
}

fn namespace_query_parameter() -> JsonValue {
    query_parameter(
        "namespace",
        "Optional namespace filter.",
        json!({
            "type": "string",
            "enum": PUBLIC_NAMESPACES,
        }),
    )
}

fn relation_query_parameter() -> JsonValue {
    query_parameter(
        "relation",
        "Optional relation facet filter.",
        json!({
            "type": "string",
            "enum": ["registrant", "token_holder", "effective_controller"],
        }),
    )
}

fn dedupe_by_query_parameter() -> JsonValue {
    query_parameter(
        "dedupe_by",
        "Current collection dedupe basis.",
        json!({
            "type": "string",
            "enum": ["surface", "resource"],
            "default": "surface",
        }),
    )
}

fn history_scope_query_parameter() -> JsonValue {
    query_parameter(
        "scope",
        "History scope selector.",
        json!({
            "type": "string",
            "enum": ["surface", "resource", "both"],
            "default": "both",
        }),
    )
}

fn resolution_mode_query_parameter() -> JsonValue {
    query_parameter(
        "mode",
        "Resolution read mode.",
        json!({
            "type": "string",
            "enum": ["declared", "verified", "both"],
            "default": "declared",
        }),
    )
}

fn cursor_query_parameter() -> JsonValue {
    query_parameter(
        "cursor",
        "Replay-stable pagination cursor.",
        json!({
            "type": "string",
        }),
    )
}

fn page_size_query_parameter() -> JsonValue {
    query_parameter(
        "page_size",
        format!("Optional page size. When supplied it must be between 1 and {MAX_PAGE_SIZE}."),
        json!({
            "type": "integer",
            "minimum": 1,
            "maximum": MAX_PAGE_SIZE,
        }),
    )
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        service: "api",
        phase: state.phase,
        status: "ok",
    })
}

async fn namespace_metadata(
    Path(namespace): Path<String>,
    State(state): State<AppState>,
) -> ApiResult<Json<NamespaceMetadataResponse>> {
    ensure_public_namespace(&namespace)?;

    let snapshot = load_namespace_manifest_snapshot(&state.pool, &namespace)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                error = ?load_error,
                "failed to load namespace metadata"
            );
            ApiError::internal_error(format!(
                "failed to load namespace metadata for namespace {namespace}"
            ))
        })?;

    Ok(Json(build_namespace_metadata_response(namespace, snapshot)))
}

async fn namespace_manifests(
    Path(namespace): Path<String>,
    State(state): State<AppState>,
) -> ApiResult<Json<NamespaceManifestsResponse>> {
    ensure_public_namespace(&namespace)?;

    let snapshot = load_namespace_manifest_snapshot(&state.pool, &namespace)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                error = ?load_error,
                "failed to load manifest snapshot for namespace"
            );
            ApiError::internal_error(format!(
                "failed to load manifest snapshot for namespace {namespace}"
            ))
        })?;

    Ok(Json(build_namespace_manifests_response(
        namespace, snapshot,
    )))
}

async fn name_current(
    Path((namespace, name)): Path<(String, String)>,
    State(state): State<AppState>,
) -> ApiResult<Json<NameResponse>> {
    ensure_public_namespace(&namespace)?;

    let logical_name_id = format!("{namespace}:{name}");
    let row = load_name_current(&state.pool, &logical_name_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                name = %name,
                logical_name_id = %logical_name_id,
                error = ?load_error,
                "failed to load exact-name current projection"
            );
            ApiError::internal_error(format!(
                "failed to load current projection for name {namespace}/{name}"
            ))
        })?;

    let Some(row) = row else {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!("name {name} was not found in namespace {namespace}"),
        });
    };

    Ok(Json(build_name_response(row)))
}

async fn coverage_current(
    Path((namespace, name)): Path<(String, String)>,
    State(state): State<AppState>,
) -> ApiResult<Json<NameResponse>> {
    ensure_public_namespace(&namespace)?;

    let logical_name_id = format!("{namespace}:{name}");
    let row = load_name_current(&state.pool, &logical_name_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                name = %name,
                logical_name_id = %logical_name_id,
                error = ?load_error,
                "failed to load exact-name current projection for coverage route"
            );
            ApiError::internal_error(format!(
                "failed to load current projection for name {namespace}/{name}"
            ))
        })?;

    let Some(row) = row else {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!("name {name} was not found in namespace {namespace}"),
        });
    };

    Ok(Json(build_name_coverage_response(row)))
}

async fn explain_surface_binding_current(
    Path((namespace, name)): Path<(String, String)>,
    State(state): State<AppState>,
) -> ApiResult<Json<NameResponse>> {
    ensure_public_namespace(&namespace)?;

    let logical_name_id = format!("{namespace}:{name}");
    let row = load_name_current(&state.pool, &logical_name_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                name = %name,
                logical_name_id = %logical_name_id,
                error = ?load_error,
                "failed to load exact-name current projection for surface-binding explain route"
            );
            ApiError::internal_error(format!(
                "failed to load surface-binding explain projection for name {namespace}/{name}"
            ))
        })?;

    let Some(row) = row else {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!("name {name} was not found in namespace {namespace}"),
        });
    };

    Ok(Json(build_name_surface_binding_explain_response(row)))
}

async fn explain_authority_control_current(
    Path((namespace, name)): Path<(String, String)>,
    State(state): State<AppState>,
) -> ApiResult<Json<NameResponse>> {
    ensure_public_namespace(&namespace)?;

    let logical_name_id = format!("{namespace}:{name}");
    let row = load_name_current(&state.pool, &logical_name_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                name = %name,
                logical_name_id = %logical_name_id,
                error = ?load_error,
                "failed to load exact-name current projection for authority-control explain route"
            );
            ApiError::internal_error(format!(
                "failed to load authority-control explain projection for name {namespace}/{name}"
            ))
        })?;

    let Some(row) = row else {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!("name {name} was not found in namespace {namespace}"),
        });
    };

    Ok(Json(build_name_authority_control_explain_response(row)))
}

async fn resolution_current(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<ResolutionQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<ResolutionResponse>> {
    ensure_public_namespace(&namespace)?;

    let mode = parse_resolution_mode(query.mode.as_deref())?;
    let records = parse_resolution_record_keys(query.records.as_deref(), mode)?;
    let logical_name_id = format!("{namespace}:{name}");
    let row = load_name_current(&state.pool, &logical_name_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                name = %name,
                logical_name_id = %logical_name_id,
                mode = ?mode,
                records = ?records,
                error = ?load_error,
                "failed to load exact-name current projection for resolution route"
            );
            ApiError::internal_error(format!(
                "failed to load resolution projection for name {namespace}/{name}"
            ))
        })?;

    let Some(row) = row else {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!("name {name} was not found in namespace {namespace}"),
        });
    };

    Ok(Json(build_resolution_response(row, mode, &records)))
}

async fn resolver_current(
    Path((chain_id, resolver_address)): Path<(String, String)>,
    State(state): State<AppState>,
) -> ApiResult<Json<ResolverResponse>> {
    let normalized_address = normalize_address(&resolver_address);
    let row = load_resolver_current(&state.pool, &chain_id, &normalized_address)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                chain_id = %chain_id,
                resolver_address = %normalized_address,
                error = ?load_error,
                "failed to load resolver_current projection"
            );
            ApiError::internal_error(format!(
                "failed to load resolver projection for chain_id {chain_id} resolver_address {normalized_address}"
            ))
        })?;

    let Some(row) = row else {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!("resolver {normalized_address} was not found on chain {chain_id}"),
        });
    };

    Ok(Json(build_resolver_response(row)))
}

async fn address_names(
    Path(address): Path<String>,
    Query(query): Query<AddressNamesQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<AddressNamesResponse>> {
    let namespace = parse_address_names_namespace(query.namespace.as_deref())?;
    let relation = parse_address_name_relation(query.relation.as_deref())?;
    let dedupe_by = parse_address_names_dedupe_by(query.dedupe_by.as_deref())?;
    let include = parse_address_names_include(query.include.as_deref())?;
    let pagination = parse_pagination(query.cursor.as_deref(), query.page_size)?;
    let normalized_address = normalize_address(&address);

    let rows = load_address_names_current(
        &state.pool,
        &normalized_address,
        namespace.as_deref(),
        relation,
    )
    .await
    .map_err(|load_error| {
        error!(
            service = "api",
            address = %normalized_address,
            namespace = ?namespace,
            relation = relation.map(|value| value.as_str()),
            dedupe_by = address_names_dedupe_label(dedupe_by),
            error = ?load_error,
            "failed to load address_names_current rows"
        );
        ApiError::internal_error(format!(
            "failed to load current address-name collection for address {normalized_address}"
        ))
    })?;

    let entries = collapse_address_name_current_rows(&rows, dedupe_by);
    let mut filters = BTreeMap::new();
    if let Some(namespace) = namespace.as_ref() {
        filters.insert("namespace".to_owned(), namespace.clone());
    }
    if let Some(relation) = relation {
        filters.insert("relation".to_owned(), relation.as_str().to_owned());
    }
    filters.insert(
        "dedupe_by".to_owned(),
        address_names_dedupe_label(dedupe_by).to_owned(),
    );
    let page = paginate_window(
        &entries,
        &pagination,
        entries.len() as u64,
        &CursorSpec {
            route: "/v1/addresses/{address}/names",
            anchor: normalized_address.clone(),
            sort: "display_name_asc",
            filters,
        },
        address_name_cursor_fields,
    )?;
    let page_entries = &entries[page.start..page.end];
    let response = if include.role_summary {
        build_address_names_response_with_role_summary(
            &state.pool,
            &entries,
            page_entries,
            page.page,
        )
        .await?
    } else {
        let data = page_entries.iter().map(build_address_name_item).collect();
        build_address_names_response(
            &entries,
            data,
            AddressNamesResponseSupplement::default(),
            page.page,
        )
    };

    Ok(Json(response))
}

async fn name_children(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<ChildrenQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<ChildrenResponse>> {
    ensure_public_namespace(&namespace)?;

    let include_counts = parse_children_query(&query)?;
    let pagination = parse_pagination(query.cursor.as_deref(), query.page_size)?;
    let logical_name_id = format!("{namespace}:{name}");
    let surface = load_name_surface(&state.pool, &logical_name_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                name = %name,
                logical_name_id = %logical_name_id,
                error = ?load_error,
                "failed to load name surface for children route"
            );
            ApiError::internal_error(format!(
                "failed to load child collection for name {namespace}/{name}"
            ))
        })?;

    let Some(_surface) = surface else {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!("name {name} was not found in namespace {namespace}"),
        });
    };

    let rows = load_children_current(&state.pool, &logical_name_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                name = %name,
                logical_name_id = %logical_name_id,
                error = ?load_error,
                "failed to load children_current rows"
            );
            ApiError::internal_error(format!(
                "failed to load child collection for name {namespace}/{name}"
            ))
        })?;

    let page = paginate_window(
        &rows,
        &pagination,
        rows.len() as u64,
        &CursorSpec {
            route: "/v1/names/{namespace}/{name}/children",
            anchor: logical_name_id,
            sort: "display_name_asc",
            filters: BTreeMap::new(),
        },
        child_cursor_fields,
    )?;

    Ok(Json(build_children_response(
        &rows,
        &rows[page.start..page.end],
        include_counts,
        page.page,
    )))
}

async fn address_history(
    Path(address): Path<String>,
    Query(query): Query<AddressHistoryQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<HistoryResponse>> {
    let namespace = parse_address_names_namespace(query.namespace.as_deref())?;
    let relation = parse_address_name_relation(query.relation.as_deref())?;
    let scope = parse_history_scope(query.scope.as_deref())?;
    let pagination = parse_pagination(query.cursor.as_deref(), query.page_size)?;
    let normalized_address = normalize_address(&address);

    let rows = load_address_history(
        &state.pool,
        &normalized_address,
        namespace.as_deref(),
        relation,
        scope,
        true,
    )
    .await
    .map_err(|load_error| {
        error!(
            service = "api",
            address = %normalized_address,
            namespace = ?namespace,
            relation = relation.map(|value| value.as_str()),
            scope = scope.as_str(),
            error = ?load_error,
            "failed to load address history"
        );
        ApiError::internal_error(format!(
            "failed to load history for address {normalized_address}"
        ))
    })?;

    let mut filters = BTreeMap::new();
    filters.insert("scope".to_owned(), scope.as_str().to_owned());
    if let Some(namespace) = namespace.as_ref() {
        filters.insert("namespace".to_owned(), namespace.clone());
    }
    if let Some(relation) = relation {
        filters.insert("relation".to_owned(), relation.as_str().to_owned());
    }
    let page = paginate_window(
        &rows,
        &pagination,
        DEFAULT_PAGE_SIZE,
        &CursorSpec {
            route: "/v1/history/addresses/{address}",
            anchor: normalized_address.clone(),
            sort: "chain_position_desc",
            filters,
        },
        history_cursor_fields,
    )?;

    Ok(Json(build_history_response(
        &rows,
        &rows[page.start..page.end],
        scope,
        page.page,
    )))
}

async fn name_history(
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<HistoryQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<HistoryResponse>> {
    ensure_public_namespace(&namespace)?;

    let scope = parse_history_scope(query.scope.as_deref())?;
    let pagination = parse_pagination(query.cursor.as_deref(), query.page_size)?;
    let logical_name_id = format!("{namespace}:{name}");
    let surface = load_name_surface(&state.pool, &logical_name_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                name = %name,
                logical_name_id = %logical_name_id,
                error = ?load_error,
                "failed to load name surface for history route"
            );
            ApiError::internal_error(format!(
                "failed to load history for name {namespace}/{name}"
            ))
        })?;

    let Some(_surface) = surface else {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!("name {name} was not found in namespace {namespace}"),
        });
    };

    let resource_ids = resource_ids_for_name(&state.pool, &logical_name_id).await?;
    let rows = load_name_history(&state.pool, &logical_name_id, &resource_ids, scope, true)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = %namespace,
                name = %name,
                logical_name_id = %logical_name_id,
                resource_ids = ?resource_ids,
                scope = scope.as_str(),
                error = ?load_error,
                "failed to load name history"
            );
            ApiError::internal_error(format!(
                "failed to load history for name {namespace}/{name}"
            ))
        })?;

    let mut filters = BTreeMap::new();
    filters.insert("scope".to_owned(), scope.as_str().to_owned());
    let page = paginate_window(
        &rows,
        &pagination,
        DEFAULT_PAGE_SIZE,
        &CursorSpec {
            route: "/v1/history/names/{namespace}/{name}",
            anchor: logical_name_id,
            sort: "chain_position_desc",
            filters,
        },
        history_cursor_fields,
    )?;

    Ok(Json(build_history_response(
        &rows,
        &rows[page.start..page.end],
        scope,
        page.page,
    )))
}

async fn resource_history(
    Path(resource_id): Path<String>,
    Query(query): Query<HistoryQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<HistoryResponse>> {
    let scope = parse_history_scope(query.scope.as_deref())?;
    let pagination = parse_pagination(query.cursor.as_deref(), query.page_size)?;
    let resource_id = Uuid::parse_str(&resource_id).map_err(|_| ApiError {
        status: StatusCode::BAD_REQUEST,
        code: "invalid_input",
        message: "resource_id must be a UUID".to_owned(),
    })?;

    let resource = load_resource(&state.pool, resource_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                resource_id = %resource_id,
                error = ?load_error,
                "failed to load resource for history route"
            );
            ApiError::internal_error(format!("failed to load history for resource {resource_id}"))
        })?;

    let Some(_resource) = resource else {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!("resource {resource_id} was not found"),
        });
    };

    let logical_name_ids = logical_name_ids_for_resource(&state.pool, resource_id).await?;
    let rows = load_resource_history(&state.pool, resource_id, &logical_name_ids, scope, true)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                resource_id = %resource_id,
                logical_name_ids = ?logical_name_ids,
                scope = scope.as_str(),
                error = ?load_error,
                "failed to load resource history"
            );
            ApiError::internal_error(format!("failed to load history for resource {resource_id}"))
        })?;

    let mut filters = BTreeMap::new();
    filters.insert("scope".to_owned(), scope.as_str().to_owned());
    let page = paginate_window(
        &rows,
        &pagination,
        DEFAULT_PAGE_SIZE,
        &CursorSpec {
            route: "/v1/history/resources/{resource_id}",
            anchor: resource_id.to_string(),
            sort: "chain_position_desc",
            filters,
        },
        history_cursor_fields,
    )?;

    Ok(Json(build_history_response(
        &rows,
        &rows[page.start..page.end],
        scope,
        page.page,
    )))
}

async fn resource_permissions(
    Path(resource_id): Path<String>,
    Query(query): Query<PermissionsQuery>,
    State(state): State<AppState>,
) -> ApiResult<Json<ResourcePermissionsResponse>> {
    let resource_id = Uuid::parse_str(&resource_id).map_err(|_| ApiError {
        status: StatusCode::BAD_REQUEST,
        code: "invalid_input",
        message: "resource_id must be a UUID".to_owned(),
    })?;
    let subject = parse_permissions_subject(query.subject.as_deref());
    let scope = parse_permission_scope_filter(query.scope.as_deref())?;
    let pagination = parse_pagination(query.cursor.as_deref(), query.page_size)?;

    let rows =
        load_permissions_current(&state.pool, resource_id, subject.as_deref(), scope.as_ref())
            .await
            .map_err(|load_error| {
                error!(
                    service = "api",
                    resource_id = %resource_id,
                    subject = ?subject,
                    scope = scope.as_ref().map(PermissionScope::storage_key),
                    error = ?load_error,
                    "failed to load permissions_current rows"
                );
                ApiError::internal_error(format!(
                    "failed to load permissions for resource {resource_id}"
                ))
            })?;

    let mut filters = BTreeMap::new();
    if let Some(subject) = subject.as_ref() {
        filters.insert("subject".to_owned(), subject.clone());
    }
    if let Some(scope) = scope.as_ref() {
        filters.insert("scope".to_owned(), scope.storage_key());
    }
    let page = paginate_window(
        &rows,
        &pagination,
        rows.len() as u64,
        &CursorSpec {
            route: "/v1/resources/{resource_id}/permissions",
            anchor: resource_id.to_string(),
            sort: "subject_scope_asc",
            filters,
        },
        permission_cursor_fields,
    )?;

    Ok(Json(build_resource_permissions_response(
        &rows,
        &rows[page.start..page.end],
        page.page,
    )))
}

async fn build_address_names_response_with_role_summary(
    pool: &PgPool,
    entries: &[AddressNameCurrentEntry],
    page_entries: &[AddressNameCurrentEntry],
    page: HistoryPageResponse,
) -> ApiResult<AddressNamesResponse> {
    let mut data = Vec::with_capacity(page_entries.len());
    let mut supplement = AddressNamesResponseSupplement::default();
    let mut name_current_cache = BTreeMap::<String, Option<NameCurrentRow>>::new();
    let mut permissions_cache = BTreeMap::<Uuid, Vec<PermissionsCurrentRow>>::new();
    let mut children_cache = BTreeMap::<String, Vec<ChildrenCurrentRow>>::new();

    for entry in page_entries {
        let name_row = match name_current_cache.get(&entry.logical_name_id) {
            Some(row) => row.clone(),
            None => {
                let row = load_name_current(pool, &entry.logical_name_id)
                    .await
                    .map_err(|load_error| {
                        error!(
                            service = "api",
                            logical_name_id = %entry.logical_name_id,
                            error = ?load_error,
                            "failed to load name_current row for address role summary expansion"
                        );
                        ApiError::internal_error(format!(
                            "failed to load current projection for logical name {}",
                            entry.logical_name_id
                        ))
                    })?;
                name_current_cache.insert(entry.logical_name_id.clone(), row.clone());
                row
            }
        };

        let permissions = match permissions_cache.get(&entry.resource_id) {
            Some(rows) => rows.clone(),
            None => {
                let rows = load_permissions_current(pool, entry.resource_id, None, None)
                    .await
                    .map_err(|load_error| {
                        error!(
                            service = "api",
                            resource_id = %entry.resource_id,
                            error = ?load_error,
                            "failed to load permissions_current rows for address role summary expansion"
                        );
                        ApiError::internal_error(format!(
                            "failed to load permissions for resource {}",
                            entry.resource_id
                        ))
                    })?;
                permissions_cache.insert(entry.resource_id, rows.clone());
                rows
            }
        };

        let children = match children_cache.get(&entry.logical_name_id) {
            Some(rows) => rows.clone(),
            None => {
                let rows = load_children_current(pool, &entry.logical_name_id)
                    .await
                    .map_err(|load_error| {
                        error!(
                            service = "api",
                            logical_name_id = %entry.logical_name_id,
                            error = ?load_error,
                            "failed to load children_current rows for address role summary expansion"
                        );
                        ApiError::internal_error(format!(
                            "failed to load child collection for logical name {}",
                            entry.logical_name_id
                        ))
                    })?;
                children_cache.insert(entry.logical_name_id.clone(), rows.clone());
                rows
            }
        };

        if let Some(row) = name_row.as_ref() {
            supplement.push_name_current(row);
        }
        supplement.push_permissions(&permissions);
        supplement.push_children(&children);
        data.push(build_address_name_item_with_role_summary(
            entry,
            name_row.as_ref(),
            &permissions,
            &children,
        ));
    }

    Ok(build_address_names_response(
        entries, data, supplement, page,
    ))
}

fn build_namespace_metadata_response(
    namespace: String,
    snapshot: NamespaceManifestSnapshot,
) -> NamespaceMetadataResponse {
    let manifest_versions = snapshot
        .manifests
        .iter()
        .map(ManifestVersionRef::from)
        .collect::<Vec<_>>();

    NamespaceMetadataResponse {
        data: NamespaceMetadataData { namespace },
        declared_state: NamespaceMetadataDeclaredState {
            active_manifest_count: snapshot.manifests.len(),
            active_source_families: collect_unique(
                snapshot
                    .manifests
                    .iter()
                    .map(|manifest| manifest.source_family.clone()),
            ),
            chains: collect_unique(
                snapshot
                    .manifests
                    .iter()
                    .map(|manifest| manifest.chain.clone()),
            ),
            normalizer_versions: collect_unique(
                snapshot
                    .manifests
                    .iter()
                    .map(|manifest| manifest.normalizer_version.clone()),
            ),
        },
        verified_state: None,
        provenance: NamespaceMetadataProvenance {
            normalized_event_ids: Vec::new(),
            raw_fact_refs: Vec::new(),
            manifest_versions,
            execution_trace_id: None,
            derivation_kind: "declared".to_owned(),
        },
        coverage: CoverageResponse {
            status: "full".to_owned(),
            exhaustiveness: "authoritative".to_owned(),
            source_classes_considered: vec!["source_manifests".to_owned()],
            enumeration_basis: "active manifests for the requested namespace".to_owned(),
            unsupported_reason: None,
        },
        chain_positions: BTreeMap::new(),
        consistency: "head".to_owned(),
        last_updated: snapshot.last_updated,
    }
}

fn build_namespace_manifests_response(
    namespace: String,
    snapshot: NamespaceManifestSnapshot,
) -> NamespaceManifestsResponse {
    let manifests = snapshot
        .manifests
        .into_iter()
        .map(Into::into)
        .collect::<Vec<NamespaceManifestEntry>>();
    let manifest_versions = manifests.iter().map(ManifestVersionRef::from).collect();

    NamespaceManifestsResponse {
        data: NamespaceManifestsData { namespace },
        declared_state: NamespaceManifestsDeclaredState { manifests },
        verified_state: None,
        provenance: NamespaceManifestsProvenance {
            normalized_event_ids: Vec::new(),
            raw_fact_refs: Vec::new(),
            manifest_versions,
            execution_trace_id: None,
            derivation_kind: "declared".to_owned(),
        },
        coverage: CoverageResponse {
            status: "full".to_owned(),
            exhaustiveness: "authoritative".to_owned(),
            source_classes_considered: vec!["source_manifests".to_owned()],
            enumeration_basis: "active manifests for the requested namespace".to_owned(),
            unsupported_reason: None,
        },
        chain_positions: BTreeMap::new(),
        consistency: "head".to_owned(),
        last_updated: snapshot.last_updated,
    }
}

fn build_name_response(row: NameCurrentRow) -> NameResponse {
    let declared_state = build_name_declared_state(&row);

    build_name_declared_response(row, declared_state)
}

fn build_name_coverage_response(row: NameCurrentRow) -> NameResponse {
    let declared_state = build_name_coverage_declared_state(&row.coverage);

    build_name_declared_response(row, declared_state)
}

fn build_name_surface_binding_explain_response(row: NameCurrentRow) -> NameResponse {
    let declared_state = build_name_surface_binding_explain_declared_state(&row);

    build_name_declared_response(row, declared_state)
}

fn build_name_authority_control_explain_response(row: NameCurrentRow) -> NameResponse {
    let declared_state = build_name_authority_control_explain_declared_state(&row);

    build_name_declared_response(row, declared_state)
}

fn build_name_declared_response(row: NameCurrentRow, declared_state: JsonValue) -> NameResponse {
    NameResponse {
        data: build_name_data(&row),
        declared_state,
        verified_state: None,
        provenance: build_name_provenance(&row.provenance),
        coverage: build_name_coverage(&row.coverage),
        chain_positions: ensure_object(&row.chain_positions),
        consistency: canonicality_consistency(&row.canonicality_summary).to_owned(),
        last_updated: format_timestamp(row.last_recomputed_at),
    }
}

fn build_resolution_response(
    row: NameCurrentRow,
    mode: ResolutionMode,
    records: &[ResolutionRecordKey],
) -> ResolutionResponse {
    let data = build_name_data(&row);
    let declared_state = mode
        .includes_declared()
        .then(|| build_resolution_declared_state(&row));
    let verified_state = mode
        .includes_verified()
        .then(|| build_resolution_verified_state(records));
    let provenance = build_name_provenance(&row.provenance);
    let coverage = build_name_coverage(&row.coverage);
    let chain_positions = ensure_object(&row.chain_positions);
    let consistency = canonicality_consistency(&row.canonicality_summary).to_owned();
    let last_updated = format_timestamp(row.last_recomputed_at);

    ResolutionResponse {
        data,
        declared_state,
        verified_state,
        provenance,
        coverage,
        chain_positions,
        consistency,
        last_updated,
    }
}

fn build_resolver_response(row: ResolverCurrentRow) -> ResolverResponse {
    ResolverResponse {
        data: build_resolver_data(&row),
        declared_state: build_resolver_declared_state(&row.declared_summary),
        verified_state: None,
        provenance: build_name_provenance(&row.provenance),
        coverage: build_name_coverage(&row.coverage),
        chain_positions: ensure_object(&row.chain_positions),
        consistency: canonicality_consistency(&row.canonicality_summary).to_owned(),
        last_updated: format_timestamp(row.last_recomputed_at),
    }
}

fn build_children_response(
    rows: &[ChildrenCurrentRow],
    page_rows: &[ChildrenCurrentRow],
    include_counts: bool,
    page: HistoryPageResponse,
) -> ChildrenResponse {
    let last_updated = rows
        .iter()
        .map(|row| row.last_recomputed_at)
        .max()
        .map(format_timestamp)
        .unwrap_or_else(|| format_timestamp(OffsetDateTime::now_utc()));

    ChildrenResponse {
        data: page_rows.iter().map(build_child_item).collect(),
        declared_state: build_children_declared_state(rows.len(), include_counts),
        verified_state: None,
        provenance: build_children_provenance(rows),
        coverage: CoverageResponse {
            status: "full".to_owned(),
            exhaustiveness: "authoritative".to_owned(),
            source_classes_considered: vec!["declared".to_owned()],
            enumeration_basis: "declared_direct_children".to_owned(),
            unsupported_reason: None,
        },
        chain_positions: build_children_chain_positions(rows),
        page,
        consistency: collection_consistency(rows.iter().map(|row| &row.canonicality_summary))
            .to_owned(),
        last_updated,
    }
}

fn build_resolution_declared_state(row: &NameCurrentRow) -> JsonValue {
    let mut declared_state = empty_object();
    insert_value_field(
        &mut declared_state,
        "topology",
        build_resolution_topology(row),
    );
    insert_value_field(
        &mut declared_state,
        "record_inventory",
        unsupported_section("declared resolution record inventory is not yet projected"),
    );
    insert_value_field(
        &mut declared_state,
        "record_cache",
        unsupported_section("declared resolution record cache is not yet projected"),
    );
    declared_state
}

fn build_resolution_verified_state(records: &[ResolutionRecordKey]) -> JsonValue {
    let mut verified_state = empty_object();
    insert_value_field(
        &mut verified_state,
        "verified_queries",
        JsonValue::Array(
            records
                .iter()
                .map(build_resolution_verified_query)
                .collect(),
        ),
    );
    verified_state
}

fn build_resolution_verified_query(record: &ResolutionRecordKey) -> JsonValue {
    let mut query = empty_object();
    insert_string_field(&mut query, "record_key", record.record_key.clone());
    insert_string_field(&mut query, "status", "unsupported".to_owned());
    insert_string_field(
        &mut query,
        "unsupported_reason",
        "verified resolution entrypoint is not yet supported".to_owned(),
    );
    query
}

fn build_resolution_topology(row: &NameCurrentRow) -> JsonValue {
    if row.namespace != "ens"
        || row.binding_kind != Some(SurfaceBindingKind::DeclaredRegistryPath)
        || row.resource_id.is_none()
    {
        return unsupported_section("declared resolution topology is not yet projected");
    }

    let Some(resolver_summary) = provenance_field(&row.declared_summary, "resolver")
        .filter(|value| value.is_object())
        .filter(|value| !summary_is_unsupported(Some(value)))
    else {
        return unsupported_section("declared resolution topology is not yet projected");
    };

    let resolver_chain_id = string_field(provenance_field(resolver_summary, "chain_id"));
    let resolver_address = string_field(provenance_field(resolver_summary, "address"));
    if resolver_chain_id.is_some() != resolver_address.is_some() {
        return unsupported_section("declared resolution topology is not yet projected");
    }

    let Some(chain_position) = build_resolution_boundary_chain_position(row) else {
        return unsupported_section("declared resolution topology is not yet projected");
    };
    if !chain_position.chain_id.starts_with("ethereum") {
        return unsupported_section("declared resolution topology is not yet projected");
    }

    let registry_ref = build_resolution_name_ref(row);
    let resolver_hop = build_resolution_resolver_hop(
        row,
        resolver_chain_id,
        resolver_address,
        string_field(provenance_field(resolver_summary, "latest_event_kind")),
    );
    let boundary = build_resolution_version_boundary(row, &chain_position);

    let mut wildcard = empty_object();
    insert_value_field(&mut wildcard, "source", JsonValue::Null);
    insert_value_field(
        &mut wildcard,
        "matched_labels",
        JsonValue::Array(Vec::new()),
    );

    let mut alias = empty_object();
    insert_value_field(&mut alias, "final_target", JsonValue::Null);
    insert_value_field(&mut alias, "hops", JsonValue::Array(Vec::new()));

    let mut version_boundaries = empty_object();
    insert_value_field(
        &mut version_boundaries,
        "topology_version_boundary",
        boundary.clone(),
    );
    insert_value_field(&mut version_boundaries, "record_version_boundary", boundary);

    let mut transport = empty_object();
    insert_value_field(&mut transport, "source_chain_id", JsonValue::Null);
    insert_value_field(&mut transport, "target_chain_id", JsonValue::Null);
    insert_value_field(&mut transport, "contract_address", JsonValue::Null);
    insert_value_field(&mut transport, "latest_event_kind", JsonValue::Null);

    let mut topology = empty_object();
    insert_value_field(
        &mut topology,
        "registry_path",
        JsonValue::Array(vec![registry_ref]),
    );
    insert_value_field(
        &mut topology,
        "subregistry_path",
        JsonValue::Array(Vec::new()),
    );
    insert_value_field(
        &mut topology,
        "resolver_path",
        JsonValue::Array(vec![resolver_hop]),
    );
    insert_value_field(&mut topology, "wildcard", wildcard);
    insert_value_field(&mut topology, "alias", alias);
    insert_value_field(&mut topology, "version_boundaries", version_boundaries);
    insert_value_field(&mut topology, "transport", transport);
    topology
}

fn build_resolution_name_ref(row: &NameCurrentRow) -> JsonValue {
    let mut name_ref = empty_object();
    insert_string_field(
        &mut name_ref,
        "logical_name_id",
        row.logical_name_id.clone(),
    );
    insert_string_field(&mut name_ref, "namespace", row.namespace.clone());
    insert_string_field(
        &mut name_ref,
        "normalized_name",
        row.normalized_name.clone(),
    );
    insert_string_field(
        &mut name_ref,
        "canonical_display_name",
        row.canonical_display_name.clone(),
    );
    insert_string_field(&mut name_ref, "namehash", row.namehash.clone());
    insert_optional_string_field(
        &mut name_ref,
        "resource_id",
        row.resource_id.map(|value| value.to_string()),
    );
    insert_optional_string_field(
        &mut name_ref,
        "binding_kind",
        row.binding_kind.map(|value| value.as_str().to_owned()),
    );
    name_ref
}

fn build_resolution_resolver_hop(
    row: &NameCurrentRow,
    chain_id: Option<String>,
    address: Option<String>,
    latest_event_kind: Option<String>,
) -> JsonValue {
    let mut hop = empty_object();
    insert_string_field(&mut hop, "logical_name_id", row.logical_name_id.clone());
    insert_string_field(&mut hop, "namespace", row.namespace.clone());
    insert_string_field(&mut hop, "normalized_name", row.normalized_name.clone());
    insert_string_field(
        &mut hop,
        "canonical_display_name",
        row.canonical_display_name.clone(),
    );
    insert_optional_string_field(
        &mut hop,
        "resource_id",
        row.resource_id.map(|value| value.to_string()),
    );
    insert_nullable_string_field(&mut hop, "chain_id", chain_id);
    insert_nullable_string_field(&mut hop, "address", address);
    insert_nullable_string_field(&mut hop, "latest_event_kind", latest_event_kind);
    hop
}

fn build_resolution_version_boundary(
    row: &NameCurrentRow,
    chain_position: &ChainPositionResponse,
) -> JsonValue {
    let mut boundary = empty_object();
    insert_string_field(
        &mut boundary,
        "logical_name_id",
        row.logical_name_id.clone(),
    );
    insert_optional_string_field(
        &mut boundary,
        "resource_id",
        row.resource_id.map(|value| value.to_string()),
    );
    insert_value_field(&mut boundary, "normalized_event_id", JsonValue::Null);
    insert_value_field(&mut boundary, "event_kind", JsonValue::Null);
    insert_value_field(
        &mut boundary,
        "chain_position",
        serde_json::to_value(chain_position).expect("chain position must serialize"),
    );
    boundary
}

fn build_resolution_boundary_chain_position(row: &NameCurrentRow) -> Option<ChainPositionResponse> {
    let chain_positions = row.chain_positions.as_object()?;
    chain_positions
        .get("ethereum")
        .and_then(chain_position_from_value)
        .or_else(|| {
            let mut parsed = chain_positions
                .values()
                .filter_map(chain_position_from_value);
            let first = parsed.next()?;
            parsed.next().is_none().then_some(first)
        })
}

impl AddressNamesResponseSupplement {
    fn push_name_current(&mut self, row: &NameCurrentRow) {
        self.provenances.push(row.provenance.clone());
        self.chain_positions.push(row.chain_positions.clone());
        self.canonicality_summaries
            .push(row.canonicality_summary.clone());
        self.last_recomputed_at.push(row.last_recomputed_at);
    }

    fn push_permissions(&mut self, rows: &[PermissionsCurrentRow]) {
        self.provenances
            .extend(rows.iter().map(|row| row.provenance.clone()));
        self.chain_positions
            .extend(rows.iter().map(|row| row.chain_positions.clone()));
        self.canonicality_summaries
            .extend(rows.iter().map(|row| row.canonicality_summary.clone()));
        self.last_recomputed_at
            .extend(rows.iter().map(|row| row.last_recomputed_at));
    }

    fn push_children(&mut self, rows: &[ChildrenCurrentRow]) {
        self.provenances
            .extend(rows.iter().map(|row| row.provenance.clone()));
        self.chain_positions
            .extend(rows.iter().map(|row| row.chain_positions.clone()));
        self.canonicality_summaries
            .extend(rows.iter().map(|row| row.canonicality_summary.clone()));
        self.last_recomputed_at
            .extend(rows.iter().map(|row| row.last_recomputed_at));
    }
}

fn build_address_names_response(
    entries: &[AddressNameCurrentEntry],
    data: Vec<JsonValue>,
    supplement: AddressNamesResponseSupplement,
    page: HistoryPageResponse,
) -> AddressNamesResponse {
    let last_updated = entries
        .iter()
        .map(|entry| entry.last_recomputed_at)
        .chain(supplement.last_recomputed_at.iter().copied())
        .max()
        .map(format_timestamp)
        .unwrap_or_else(|| format_timestamp(OffsetDateTime::now_utc()));

    AddressNamesResponse {
        data,
        declared_state: empty_object(),
        verified_state: None,
        provenance: build_address_names_provenance(&entries, &supplement),
        coverage: CoverageResponse {
            status: "full".to_owned(),
            exhaustiveness: "authoritative".to_owned(),
            source_classes_considered: vec!["ensv1_registry_path".to_owned()],
            enumeration_basis: "surface_current_relations".to_owned(),
            unsupported_reason: None,
        },
        chain_positions: build_address_names_chain_positions(entries, &supplement),
        page,
        consistency: collection_consistency(
            entries
                .iter()
                .map(|entry| &entry.canonicality_summary)
                .chain(supplement.canonicality_summaries.iter()),
        )
        .to_owned(),
        last_updated,
    }
}

fn build_history_response(
    rows: &[HistoryEvent],
    page_rows: &[HistoryEvent],
    scope: HistoryScope,
    page: HistoryPageResponse,
) -> HistoryResponse {
    let last_updated = rows
        .iter()
        .filter_map(|row| row.block_timestamp)
        .max()
        .map(format_timestamp)
        .unwrap_or_else(|| format_timestamp(OffsetDateTime::now_utc()));

    HistoryResponse {
        data: page_rows.iter().map(build_history_item).collect(),
        declared_state: empty_object(),
        verified_state: None,
        provenance: build_history_provenance(rows),
        coverage: build_history_coverage(scope),
        chain_positions: build_history_chain_positions(rows),
        page,
        consistency: "head".to_owned(),
        last_updated,
    }
}

fn build_resource_permissions_response(
    rows: &[PermissionsCurrentRow],
    page_rows: &[PermissionsCurrentRow],
    page: HistoryPageResponse,
) -> ResourcePermissionsResponse {
    let last_updated = rows
        .iter()
        .map(|row| row.last_recomputed_at)
        .max()
        .map(format_timestamp)
        .unwrap_or_else(|| format_timestamp(OffsetDateTime::now_utc()));

    ResourcePermissionsResponse {
        data: page_rows.iter().map(build_permission_item).collect(),
        declared_state: empty_object(),
        verified_state: None,
        provenance: build_permissions_provenance(rows),
        coverage: build_permissions_coverage(rows),
        chain_positions: build_permissions_chain_positions(rows),
        page,
        consistency: collection_consistency(rows.iter().map(|row| &row.canonicality_summary))
            .to_owned(),
        last_updated,
    }
}

fn build_child_item(row: &ChildrenCurrentRow) -> JsonValue {
    let mut value = empty_object();
    insert_string_field(
        &mut value,
        "logical_name_id",
        row.child_logical_name_id.clone(),
    );
    insert_string_field(&mut value, "namespace", row.namespace.clone());
    insert_string_field(&mut value, "normalized_name", row.normalized_name.clone());
    insert_string_field(
        &mut value,
        "canonical_display_name",
        row.canonical_display_name.clone(),
    );
    insert_string_field(&mut value, "namehash", row.namehash.clone());
    insert_string_field(&mut value, "surface_class", row.surface_class.clone());
    value
}

fn build_permission_item(row: &PermissionsCurrentRow) -> JsonValue {
    let mut value = empty_object();
    insert_string_field(&mut value, "resource_id", row.resource_id.to_string());
    insert_string_field(&mut value, "subject", row.subject.clone());
    insert_value_field(
        &mut value,
        "scope",
        build_permission_scope_value(&row.scope),
    );
    insert_value_field(&mut value, "effective_powers", row.effective_powers.clone());
    insert_value_field(&mut value, "grant_source", row.grant_source.clone());
    insert_value_field(
        &mut value,
        "revocation_source",
        row.revocation_source.clone().unwrap_or(JsonValue::Null),
    );
    insert_value_field(&mut value, "inheritance_path", row.inheritance_path.clone());
    insert_value_field(
        &mut value,
        "transfer_behavior",
        row.transfer_behavior.clone(),
    );
    value
}

fn build_address_name_item(entry: &AddressNameCurrentEntry) -> JsonValue {
    let mut value = empty_object();
    insert_string_field(&mut value, "logical_name_id", entry.logical_name_id.clone());
    insert_string_field(&mut value, "namespace", entry.namespace.clone());
    insert_string_field(&mut value, "normalized_name", entry.normalized_name.clone());
    insert_string_field(
        &mut value,
        "canonical_display_name",
        entry.canonical_display_name.clone(),
    );
    insert_string_field(&mut value, "namehash", entry.namehash.clone());
    insert_string_field(&mut value, "resource_id", entry.resource_id.to_string());
    insert_string_field(
        &mut value,
        "binding_kind",
        entry.binding_kind.as_str().to_owned(),
    );
    insert_value_field(
        &mut value,
        "relation_facets",
        JsonValue::Array(
            entry
                .relations
                .iter()
                .map(|relation| JsonValue::String(relation.as_str().to_owned()))
                .collect(),
        ),
    );
    value
}

fn build_address_name_item_with_role_summary(
    entry: &AddressNameCurrentEntry,
    name_row: Option<&NameCurrentRow>,
    permissions: &[PermissionsCurrentRow],
    children: &[ChildrenCurrentRow],
) -> JsonValue {
    let mut value = build_address_name_item(entry);
    let facts = name_row
        .map(build_address_name_expansion_facts)
        .unwrap_or_default();

    insert_value_field(
        &mut value,
        "role_summary",
        build_address_name_role_summary(permissions),
    );
    insert_value_field(
        &mut value,
        "subname_count",
        JsonValue::Number((children.len() as u64).into()),
    );
    insert_value_field(&mut value, "record_count", facts.record_count);
    insert_value_field(&mut value, "status", facts.status);
    insert_value_field(&mut value, "expiry", facts.expiry);
    value
}

fn build_children_declared_state(child_count: usize, include_counts: bool) -> JsonValue {
    let mut declared_state = empty_object();
    if include_counts {
        insert_value_field(
            &mut declared_state,
            "subname_count",
            JsonValue::Number((child_count as u64).into()),
        );
    }
    declared_state
}

fn build_children_provenance(rows: &[ChildrenCurrentRow]) -> JsonValue {
    let mut value = empty_object();
    insert_value_field(
        &mut value,
        "normalized_event_ids",
        JsonValue::Array(
            collect_children_provenance_values(rows, "normalized_event_ids")
                .into_iter()
                .filter_map(|value| value_to_string(&value).map(JsonValue::String))
                .collect(),
        ),
    );
    insert_value_field(
        &mut value,
        "raw_fact_refs",
        JsonValue::Array(collect_children_provenance_values(rows, "raw_fact_refs")),
    );
    insert_value_field(
        &mut value,
        "manifest_versions",
        JsonValue::Array(collect_children_provenance_values(
            rows,
            "manifest_versions",
        )),
    );
    insert_value_field(&mut value, "execution_trace_id", JsonValue::Null);
    insert_string_field(
        &mut value,
        "derivation_kind",
        rows.iter()
            .filter_map(|row| string_field(provenance_field(&row.provenance, "derivation_kind")))
            .next()
            .unwrap_or_else(|| "declared".to_owned()),
    );
    value
}

fn build_address_names_provenance(
    entries: &[AddressNameCurrentEntry],
    supplement: &AddressNamesResponseSupplement,
) -> JsonValue {
    let provenances = entries
        .iter()
        .map(|entry| &entry.provenance)
        .chain(supplement.provenances.iter())
        .collect::<Vec<_>>();
    let mut value = empty_object();
    insert_value_field(
        &mut value,
        "normalized_event_ids",
        JsonValue::Array(
            collect_collection_provenance_values(&provenances, "normalized_event_ids")
                .into_iter()
                .filter_map(|value| value_to_string(&value).map(JsonValue::String))
                .collect(),
        ),
    );
    insert_value_field(
        &mut value,
        "raw_fact_refs",
        JsonValue::Array(collect_collection_provenance_values(
            &provenances,
            "raw_fact_refs",
        )),
    );
    insert_value_field(
        &mut value,
        "manifest_versions",
        JsonValue::Array(collect_collection_provenance_values(
            &provenances,
            "manifest_versions",
        )),
    );
    insert_value_field(&mut value, "execution_trace_id", JsonValue::Null);
    insert_string_field(
        &mut value,
        "derivation_kind",
        provenances
            .iter()
            .filter_map(|provenance| string_field(provenance_field(provenance, "derivation_kind")))
            .next()
            .unwrap_or_else(|| "declared".to_owned()),
    );
    value
}

fn collect_children_provenance_values(rows: &[ChildrenCurrentRow], key: &str) -> Vec<JsonValue> {
    let mut deduped = Vec::new();
    for row in rows {
        let Some(JsonValue::Array(values)) = provenance_field(&row.provenance, key) else {
            continue;
        };
        for value in values {
            if !deduped.contains(value) {
                deduped.push(value.clone());
            }
        }
    }
    deduped
}

fn collect_collection_provenance_values(provenances: &[&JsonValue], key: &str) -> Vec<JsonValue> {
    let mut deduped = Vec::new();
    for provenance in provenances {
        let Some(JsonValue::Array(values)) = provenance_field(provenance, key) else {
            continue;
        };
        for value in values {
            if !deduped.contains(value) {
                deduped.push(value.clone());
            }
        }
    }
    deduped
}

fn collect_permissions_provenance_values(
    rows: &[PermissionsCurrentRow],
    key: &str,
) -> Vec<JsonValue> {
    let mut deduped = Vec::new();
    for row in rows {
        let Some(JsonValue::Array(values)) = provenance_field(&row.provenance, key) else {
            continue;
        };
        for value in values {
            if !deduped.contains(value) {
                deduped.push(value.clone());
            }
        }
    }
    deduped
}

fn build_children_chain_positions(rows: &[ChildrenCurrentRow]) -> JsonValue {
    let mut chain_positions = BTreeMap::<String, ChainPositionResponse>::new();
    for row in rows {
        let Some(position_values) = row.chain_positions.as_object() else {
            continue;
        };

        for (slot, position_value) in position_values {
            let Some(candidate) = chain_position_from_value(position_value) else {
                continue;
            };
            merge_chain_position(&mut chain_positions, slot.clone(), candidate);
        }
    }

    serde_json::to_value(chain_positions).expect("children chain positions must serialize")
}

fn build_address_names_chain_positions(
    entries: &[AddressNameCurrentEntry],
    supplement: &AddressNamesResponseSupplement,
) -> JsonValue {
    let mut chain_positions = BTreeMap::<String, ChainPositionResponse>::new();
    for position_value in entries
        .iter()
        .map(|entry| &entry.chain_positions)
        .chain(supplement.chain_positions.iter())
    {
        let Some(position_values) = position_value.as_object() else {
            continue;
        };

        for (slot, position_value) in position_values {
            let Some(candidate) = chain_position_from_value(position_value) else {
                continue;
            };
            merge_chain_position(&mut chain_positions, slot.clone(), candidate);
        }
    }

    serde_json::to_value(chain_positions).expect("address names chain positions must serialize")
}

fn build_permissions_chain_positions(rows: &[PermissionsCurrentRow]) -> JsonValue {
    let mut chain_positions = BTreeMap::<String, ChainPositionResponse>::new();
    for row in rows {
        let Some(position_values) = row.chain_positions.as_object() else {
            continue;
        };

        for (slot, position_value) in position_values {
            let Some(candidate) = chain_position_from_value(position_value) else {
                continue;
            };
            merge_chain_position(&mut chain_positions, slot.clone(), candidate);
        }
    }

    serde_json::to_value(chain_positions).expect("permissions chain positions must serialize")
}

fn build_permissions_provenance(rows: &[PermissionsCurrentRow]) -> JsonValue {
    let mut value = empty_object();
    insert_value_field(
        &mut value,
        "normalized_event_ids",
        JsonValue::Array(
            collect_permissions_provenance_values(rows, "normalized_event_ids")
                .into_iter()
                .filter_map(|value| value_to_string(&value).map(JsonValue::String))
                .collect(),
        ),
    );
    insert_value_field(
        &mut value,
        "raw_fact_refs",
        JsonValue::Array(collect_permissions_provenance_values(rows, "raw_fact_refs")),
    );
    insert_value_field(
        &mut value,
        "manifest_versions",
        JsonValue::Array(collect_permissions_provenance_values(
            rows,
            "manifest_versions",
        )),
    );
    insert_value_field(&mut value, "execution_trace_id", JsonValue::Null);
    insert_string_field(
        &mut value,
        "derivation_kind",
        rows.iter()
            .filter_map(|row| string_field(provenance_field(&row.provenance, "derivation_kind")))
            .next()
            .unwrap_or_else(|| "declared".to_owned()),
    );
    value
}

fn build_permissions_coverage(rows: &[PermissionsCurrentRow]) -> CoverageResponse {
    let sample = rows.first().map(|row| &row.coverage);

    CoverageResponse {
        status: string_field(sample.and_then(|value| provenance_field(value, "status")))
            .unwrap_or_else(|| "full".to_owned()),
        exhaustiveness: string_field(
            sample.and_then(|value| provenance_field(value, "exhaustiveness")),
        )
        .unwrap_or_else(|| "authoritative".to_owned()),
        source_classes_considered: match sample
            .and_then(|value| provenance_field(value, "source_classes_considered"))
        {
            Some(JsonValue::Array(values)) => values.iter().filter_map(value_to_string).collect(),
            _ => vec!["permissions_current".to_owned()],
        },
        enumeration_basis: string_field(
            sample.and_then(|value| provenance_field(value, "enumeration_basis")),
        )
        .unwrap_or_else(|| "resource_permissions".to_owned()),
        unsupported_reason: string_field(
            sample.and_then(|value| provenance_field(value, "unsupported_reason")),
        ),
    }
}

fn build_permission_scope_value(scope: &PermissionScope) -> JsonValue {
    let mut value = empty_object();
    insert_string_field(&mut value, "kind", scope.kind().to_owned());
    insert_value_field(&mut value, "detail", scope.detail());
    value
}

fn build_address_name_role_summary(rows: &[PermissionsCurrentRow]) -> JsonValue {
    let mut subjects = BTreeMap::<String, Vec<&PermissionsCurrentRow>>::new();

    for row in rows {
        subjects.entry(row.subject.clone()).or_default().push(row);
    }

    json!({
        "subjects": subjects
            .into_iter()
            .map(|(subject, mut rows)| {
                rows.sort_by(|left, right| left.scope.storage_key().cmp(&right.scope.storage_key()));
                json!({
                    "subject": subject,
                    "scopes": rows
                        .into_iter()
                        .map(|row| {
                            json!({
                                "scope": build_permission_scope_value(&row.scope),
                                "effective_powers": row.effective_powers.clone(),
                            })
                        })
                        .collect::<Vec<_>>(),
                })
            })
            .collect::<Vec<_>>(),
    })
}

fn build_address_name_expansion_facts(row: &NameCurrentRow) -> AddressNameExpansionFacts {
    AddressNameExpansionFacts {
        status: supported_summary_field(
            provenance_field(&row.declared_summary, "control"),
            "status",
        ),
        expiry: supported_summary_field(
            provenance_field(&row.declared_summary, "control"),
            "expiry",
        ),
        record_count: supported_summary_field(
            provenance_field(&row.declared_summary, "record_inventory"),
            "count",
        ),
    }
}

fn supported_summary_field(section: Option<&JsonValue>, key: &str) -> JsonValue {
    if summary_is_unsupported(section) {
        return JsonValue::Null;
    }

    section
        .and_then(|value| provenance_field(value, key))
        .cloned()
        .unwrap_or(JsonValue::Null)
}

fn summary_is_unsupported(section: Option<&JsonValue>) -> bool {
    matches!(
        string_field(section.and_then(|value| provenance_field(value, "status"))).as_deref(),
        Some("unsupported")
    ) && string_field(section.and_then(|value| provenance_field(value, "unsupported_reason")))
        .is_some()
}

fn build_name_data(row: &NameCurrentRow) -> JsonValue {
    let mut data = empty_object();
    insert_string_field(&mut data, "logical_name_id", row.logical_name_id.clone());
    insert_string_field(&mut data, "namespace", row.namespace.clone());
    insert_string_field(&mut data, "normalized_name", row.normalized_name.clone());
    insert_string_field(
        &mut data,
        "canonical_display_name",
        row.canonical_display_name.clone(),
    );
    insert_string_field(&mut data, "namehash", row.namehash.clone());
    insert_optional_string_field(
        &mut data,
        "resource_id",
        row.resource_id.map(|value| value.to_string()),
    );
    insert_optional_string_field(
        &mut data,
        "token_lineage_id",
        row.token_lineage_id.map(|value| value.to_string()),
    );
    insert_optional_string_field(
        &mut data,
        "binding_kind",
        row.binding_kind.map(|value| value.as_str().to_owned()),
    );
    data
}

fn build_resolver_data(row: &ResolverCurrentRow) -> JsonValue {
    let mut data = empty_object();
    insert_string_field(&mut data, "chain_id", row.chain_id.clone());
    insert_string_field(&mut data, "resolver_address", row.resolver_address.clone());
    data
}

fn build_name_declared_state(row: &NameCurrentRow) -> JsonValue {
    let mut declared_state = empty_object();
    insert_value_field(
        &mut declared_state,
        "registration",
        declared_summary_section(
            &row.declared_summary,
            "registration",
            "declared registration summary is not yet projected",
        ),
    );
    insert_value_field(
        &mut declared_state,
        "authority",
        declared_authority_section(row),
    );
    insert_value_field(
        &mut declared_state,
        "control",
        declared_name_control_section(&row.declared_summary),
    );
    insert_value_field(
        &mut declared_state,
        "resolver",
        declared_summary_section(
            &row.declared_summary,
            "resolver",
            "declared resolver summary is not yet projected",
        ),
    );
    insert_value_field(
        &mut declared_state,
        "record_inventory",
        declared_summary_section(
            &row.declared_summary,
            "record_inventory",
            "declared record inventory summary is not yet projected",
        ),
    );
    insert_value_field(
        &mut declared_state,
        "history",
        declared_summary_section(
            &row.declared_summary,
            "history",
            "declared history pointers are not yet projected",
        ),
    );
    declared_state
}

fn build_resolver_declared_state(summary: &JsonValue) -> JsonValue {
    let mut declared_state = empty_object();
    insert_value_field(
        &mut declared_state,
        "bindings",
        declared_summary_section(
            summary,
            "bindings",
            "resolver bindings summary is not yet projected",
        ),
    );
    insert_value_field(
        &mut declared_state,
        "aliases",
        declared_summary_section(
            summary,
            "aliases",
            "resolver alias summary is not yet projected",
        ),
    );
    insert_value_field(
        &mut declared_state,
        "permissions",
        declared_summary_section(
            summary,
            "permissions",
            "resolver permissions summary is not yet projected",
        ),
    );
    insert_value_field(
        &mut declared_state,
        "role_holders",
        declared_summary_section(
            summary,
            "role_holders",
            "resolver role holder summary is not yet projected",
        ),
    );
    insert_value_field(
        &mut declared_state,
        "event_summary",
        declared_summary_section(
            summary,
            "event_summary",
            "resolver event summary is not yet projected",
        ),
    );
    declared_state
}

fn build_name_provenance(provenance: &JsonValue) -> JsonValue {
    let mut normalized = empty_object();
    insert_value_field(
        &mut normalized,
        "normalized_event_ids",
        array_value_strings(provenance_field(provenance, "normalized_event_ids")),
    );
    insert_value_field(
        &mut normalized,
        "raw_fact_refs",
        array_or_empty(provenance_field(provenance, "raw_fact_refs")),
    );
    insert_value_field(
        &mut normalized,
        "manifest_versions",
        array_or_empty(provenance_field(provenance, "manifest_versions")),
    );
    insert_nullable_string_field(
        &mut normalized,
        "execution_trace_id",
        string_field(provenance_field(provenance, "execution_trace_id")),
    );
    insert_string_field(
        &mut normalized,
        "derivation_kind",
        string_field(provenance_field(provenance, "derivation_kind"))
            .unwrap_or_else(|| "declared".to_owned()),
    );
    normalized
}

fn build_name_coverage(coverage: &JsonValue) -> JsonValue {
    let mut normalized = empty_object();
    insert_string_field(
        &mut normalized,
        "status",
        string_field(provenance_field(coverage, "status"))
            .unwrap_or_else(|| "unsupported".to_owned()),
    );
    insert_string_field(
        &mut normalized,
        "exhaustiveness",
        string_field(provenance_field(coverage, "exhaustiveness"))
            .unwrap_or_else(|| "not_applicable".to_owned()),
    );
    insert_value_field(
        &mut normalized,
        "source_classes_considered",
        array_or_empty(provenance_field(coverage, "source_classes_considered")),
    );
    insert_string_field(
        &mut normalized,
        "enumeration_basis",
        string_field(provenance_field(coverage, "enumeration_basis"))
            .unwrap_or_else(|| "exact_name".to_owned()),
    );
    insert_nullable_string_field(
        &mut normalized,
        "unsupported_reason",
        string_field(provenance_field(coverage, "unsupported_reason")),
    );
    normalized
}

fn build_name_coverage_declared_state(coverage: &JsonValue) -> JsonValue {
    let mut declared_state = empty_object();
    insert_string_field(
        &mut declared_state,
        "status",
        string_field(provenance_field(coverage, "status"))
            .unwrap_or_else(|| "unsupported".to_owned()),
    );
    insert_string_field(
        &mut declared_state,
        "exhaustiveness",
        string_field(provenance_field(coverage, "exhaustiveness"))
            .unwrap_or_else(|| "not_applicable".to_owned()),
    );
    insert_value_field(
        &mut declared_state,
        "source_classes_considered",
        array_or_empty(provenance_field(coverage, "source_classes_considered")),
    );
    insert_string_field(
        &mut declared_state,
        "enumeration_basis",
        string_field(provenance_field(coverage, "enumeration_basis"))
            .unwrap_or_else(|| "exact_name".to_owned()),
    );
    insert_nullable_string_field(
        &mut declared_state,
        "unsupported_reason",
        string_field(provenance_field(coverage, "unsupported_reason")),
    );
    declared_state
}

fn build_name_surface_binding_explain_declared_state(row: &NameCurrentRow) -> JsonValue {
    let mut declared_state = empty_object();
    insert_value_field(
        &mut declared_state,
        "surface_binding",
        build_name_surface_binding_explain_summary(row),
    );
    insert_value_field(
        &mut declared_state,
        "history",
        declared_summary_section(
            &row.declared_summary,
            "history",
            "declared history pointers are not yet projected",
        ),
    );
    declared_state
}

fn build_name_authority_control_explain_declared_state(row: &NameCurrentRow) -> JsonValue {
    let mut declared_state = empty_object();
    insert_value_field(
        &mut declared_state,
        "authority",
        declared_authority_section(row),
    );
    insert_value_field(
        &mut declared_state,
        "control",
        declared_name_control_section(&row.declared_summary),
    );
    declared_state
}

fn build_name_surface_binding_explain_summary(row: &NameCurrentRow) -> JsonValue {
    let has_binding_summary = row.surface_binding_id.is_some() || row.binding_kind.is_some();
    if !has_binding_summary {
        return unsupported_section("declared surface binding summary is not yet projected");
    }

    let mut surface_binding = empty_object();
    insert_optional_string_field(
        &mut surface_binding,
        "surface_binding_id",
        row.surface_binding_id.map(|value| value.to_string()),
    );
    insert_optional_string_field(
        &mut surface_binding,
        "binding_kind",
        row.binding_kind.map(|value| value.as_str().to_owned()),
    );
    surface_binding
}

fn declared_authority_section(row: &NameCurrentRow) -> JsonValue {
    if let Some(section) =
        provenance_field(&row.declared_summary, "authority").filter(|value| value.is_object())
    {
        return section.clone();
    }

    let has_binding_summary =
        row.resource_id.is_some() || row.token_lineage_id.is_some() || row.binding_kind.is_some();
    if !has_binding_summary {
        return unsupported_section("declared authority summary is not yet projected");
    }

    let mut authority = empty_object();
    insert_optional_string_field(
        &mut authority,
        "resource_id",
        row.resource_id.map(|value| value.to_string()),
    );
    insert_optional_string_field(
        &mut authority,
        "token_lineage_id",
        row.token_lineage_id.map(|value| value.to_string()),
    );
    insert_optional_string_field(
        &mut authority,
        "binding_kind",
        row.binding_kind.map(|value| value.as_str().to_owned()),
    );
    authority
}

fn declared_name_control_section(summary: &JsonValue) -> JsonValue {
    let Some(section) = provenance_field(summary, "control").filter(|value| value.is_object())
    else {
        return unsupported_section("declared control summary is not yet projected");
    };

    if summary_is_unsupported(Some(section)) {
        return section.clone();
    }

    let mut control = empty_object();
    insert_value_field(
        &mut control,
        "registrant",
        provenance_field(section, "registrant")
            .cloned()
            .unwrap_or(JsonValue::Null),
    );
    insert_value_field(
        &mut control,
        "registry_owner",
        provenance_field(section, "registry_owner")
            .cloned()
            .unwrap_or(JsonValue::Null),
    );
    insert_value_field(
        &mut control,
        "latest_event_kind",
        provenance_field(section, "latest_event_kind")
            .cloned()
            .unwrap_or(JsonValue::Null),
    );
    control
}

fn declared_summary_section(summary: &JsonValue, key: &str, unsupported_reason: &str) -> JsonValue {
    provenance_field(summary, key)
        .filter(|value| value.is_object())
        .cloned()
        .unwrap_or_else(|| unsupported_section(unsupported_reason))
}

fn canonicality_consistency(canonicality_summary: &JsonValue) -> &'static str {
    match string_field(provenance_field(canonicality_summary, "status")).as_deref() {
        Some("safe") => "safe",
        Some("finalized") => "finalized",
        _ => "head",
    }
}

fn collection_consistency<'a>(summaries: impl Iterator<Item = &'a JsonValue>) -> &'static str {
    let mut consistency = "finalized";
    let mut saw_any = false;

    for summary in summaries {
        saw_any = true;
        match canonicality_consistency(summary) {
            "head" => return "head",
            "safe" => consistency = "safe",
            "finalized" => {}
            _ => consistency = "head",
        }
    }

    if saw_any { consistency } else { "head" }
}

fn build_history_item(row: &HistoryEvent) -> JsonValue {
    let mut value = empty_object();
    insert_string_field(
        &mut value,
        "normalized_event_id",
        row.normalized_event_id.to_string(),
    );
    insert_string_field(&mut value, "event_identity", row.event_identity.clone());
    insert_string_field(&mut value, "namespace", row.namespace.clone());
    insert_optional_string_field(&mut value, "logical_name_id", row.logical_name_id.clone());
    insert_optional_string_field(
        &mut value,
        "resource_id",
        row.resource_id.map(|resource_id| resource_id.to_string()),
    );
    insert_string_field(&mut value, "event_kind", row.event_kind.clone());
    insert_string_field(&mut value, "source_family", row.source_family.clone());
    insert_value_field(
        &mut value,
        "manifest_version",
        JsonValue::Number(row.manifest_version.into()),
    );
    insert_value_field(
        &mut value,
        "source_manifest_id",
        row.source_manifest_id
            .map(|source_manifest_id| JsonValue::Number(source_manifest_id.into()))
            .unwrap_or(JsonValue::Null),
    );
    insert_value_field(
        &mut value,
        "chain_position",
        build_history_chain_position(row),
    );
    insert_nullable_string_field(&mut value, "transaction_hash", row.transaction_hash.clone());
    insert_value_field(
        &mut value,
        "log_index",
        row.log_index
            .map(|log_index| JsonValue::Number(log_index.into()))
            .unwrap_or(JsonValue::Null),
    );
    insert_value_field(&mut value, "raw_fact_ref", row.raw_fact_ref.clone());
    insert_string_field(&mut value, "derivation_kind", row.derivation_kind.clone());
    insert_string_field(
        &mut value,
        "canonicality_state",
        row.canonicality_state.as_str().to_owned(),
    );
    insert_value_field(&mut value, "before_state", row.before_state.clone());
    insert_value_field(&mut value, "after_state", row.after_state.clone());
    insert_value_field(&mut value, "provenance", ensure_object(&row.provenance));
    insert_value_field(&mut value, "coverage", build_name_coverage(&row.coverage));
    value
}

fn build_history_provenance(rows: &[HistoryEvent]) -> JsonValue {
    let mut value = empty_object();
    insert_value_field(
        &mut value,
        "normalized_event_ids",
        JsonValue::Array(
            rows.iter()
                .map(|row| JsonValue::String(row.normalized_event_id.to_string()))
                .collect(),
        ),
    );
    insert_value_field(
        &mut value,
        "raw_fact_refs",
        dedupe_json_values(rows.iter().map(|row| row.raw_fact_ref.clone())),
    );
    insert_value_field(
        &mut value,
        "manifest_versions",
        dedupe_json_values(rows.iter().map(history_manifest_version)),
    );
    insert_value_field(&mut value, "execution_trace_id", JsonValue::Null);
    insert_string_field(
        &mut value,
        "derivation_kind",
        "normalized_event_history".to_owned(),
    );
    value
}

fn build_history_coverage(scope: HistoryScope) -> CoverageResponse {
    CoverageResponse {
        status: "full".to_owned(),
        exhaustiveness: "authoritative".to_owned(),
        source_classes_considered: vec!["normalized_events".to_owned()],
        enumeration_basis: format!(
            "canonical normalized-event history for the requested {} scope",
            scope.as_str()
        ),
        unsupported_reason: None,
    }
}

fn build_history_chain_positions(rows: &[HistoryEvent]) -> JsonValue {
    let mut chain_positions = BTreeMap::<String, ChainPositionResponse>::new();
    for row in rows {
        let (Some(chain_id), Some(block_number), Some(block_hash), Some(timestamp)) = (
            row.chain_id.as_ref(),
            row.block_number,
            row.block_hash.as_ref(),
            row.block_timestamp,
        ) else {
            continue;
        };

        let key = chain_position_key(chain_id);
        let candidate = ChainPositionResponse {
            chain_id: chain_id.clone(),
            block_number,
            block_hash: block_hash.clone(),
            timestamp: format_timestamp(timestamp),
        };
        merge_chain_position(&mut chain_positions, key, candidate);
    }

    serde_json::to_value(chain_positions).expect("history chain positions must serialize")
}

fn chain_position_from_value(value: &JsonValue) -> Option<ChainPositionResponse> {
    Some(ChainPositionResponse {
        chain_id: string_field(provenance_field(value, "chain_id"))?,
        block_number: provenance_field(value, "block_number")?.as_i64()?,
        block_hash: string_field(provenance_field(value, "block_hash"))?,
        timestamp: string_field(provenance_field(value, "timestamp"))?,
    })
}

fn merge_chain_position(
    chain_positions: &mut BTreeMap<String, ChainPositionResponse>,
    key: String,
    candidate: ChainPositionResponse,
) {
    match chain_positions.get(&key) {
        Some(existing)
            if existing.block_number > candidate.block_number
                || (existing.block_number == candidate.block_number
                    && existing.block_hash >= candidate.block_hash) => {}
        _ => {
            chain_positions.insert(key, candidate);
        }
    }
}

fn build_history_chain_position(row: &HistoryEvent) -> JsonValue {
    match (
        row.chain_id.as_ref(),
        row.block_number,
        row.block_hash.as_ref(),
        row.block_timestamp,
    ) {
        (Some(chain_id), Some(block_number), Some(block_hash), Some(timestamp)) => json!({
            "chain_id": chain_id,
            "block_number": block_number,
            "block_hash": block_hash,
            "timestamp": format_timestamp(timestamp),
        }),
        _ => JsonValue::Null,
    }
}

fn dedupe_json_values(values: impl Iterator<Item = JsonValue>) -> JsonValue {
    let mut deduped = Vec::new();
    for value in values {
        if !deduped.contains(&value) {
            deduped.push(value);
        }
    }

    JsonValue::Array(deduped)
}

fn provenance_field<'a>(value: &'a JsonValue, key: &str) -> Option<&'a JsonValue> {
    value.as_object().and_then(|object| object.get(key))
}

fn string_field(value: Option<&JsonValue>) -> Option<String> {
    match value {
        Some(JsonValue::String(value)) => Some(value.clone()),
        Some(JsonValue::Number(value)) => Some(value.to_string()),
        Some(JsonValue::Bool(value)) => Some(value.to_string()),
        _ => None,
    }
}

fn array_or_empty(value: Option<&JsonValue>) -> JsonValue {
    match value {
        Some(JsonValue::Array(values)) => JsonValue::Array(values.clone()),
        _ => JsonValue::Array(Vec::new()),
    }
}

fn array_value_strings(value: Option<&JsonValue>) -> JsonValue {
    match value {
        Some(JsonValue::Array(values)) => JsonValue::Array(
            values
                .iter()
                .filter_map(|value| value_to_string(value).map(JsonValue::String))
                .collect(),
        ),
        _ => JsonValue::Array(Vec::new()),
    }
}

fn value_to_string(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(value) => Some(value.clone()),
        JsonValue::Number(value) => Some(value.to_string()),
        JsonValue::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn ensure_object(value: &JsonValue) -> JsonValue {
    value
        .as_object()
        .map(|_| value.clone())
        .unwrap_or_else(empty_object)
}

fn unsupported_section(unsupported_reason: &str) -> JsonValue {
    let mut value = empty_object();
    insert_string_field(&mut value, "status", "unsupported".to_owned());
    insert_string_field(
        &mut value,
        "unsupported_reason",
        unsupported_reason.to_owned(),
    );
    value
}

fn format_timestamp(value: OffsetDateTime) -> String {
    let value = value.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        value.year(),
        value.month() as u8,
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    )
}

fn empty_object() -> JsonValue {
    JsonValue::Object(Default::default())
}

fn insert_string_field(object: &mut JsonValue, key: &str, value: String) {
    object
        .as_object_mut()
        .expect("object helper must receive object")
        .insert(key.to_owned(), JsonValue::String(value));
}

fn insert_optional_string_field(object: &mut JsonValue, key: &str, value: Option<String>) {
    object
        .as_object_mut()
        .expect("object helper must receive object")
        .insert(
            key.to_owned(),
            value.map(JsonValue::String).unwrap_or(JsonValue::Null),
        );
}

fn insert_nullable_string_field(object: &mut JsonValue, key: &str, value: Option<String>) {
    insert_optional_string_field(object, key, value);
}

fn insert_value_field(object: &mut JsonValue, key: &str, value: JsonValue) {
    object
        .as_object_mut()
        .expect("object helper must receive object")
        .insert(key.to_owned(), value);
}

fn parse_history_scope(scope: Option<&str>) -> ApiResult<HistoryScope> {
    match scope.unwrap_or("both") {
        "surface" => Ok(HistoryScope::Surface),
        "resource" => Ok(HistoryScope::Resource),
        "both" => Ok(HistoryScope::Both),
        _ => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "scope must be one of: surface, resource, both".to_owned(),
        }),
    }
}

fn parse_resolution_mode(mode: Option<&str>) -> ApiResult<ResolutionMode> {
    match mode.unwrap_or("declared") {
        "declared" => Ok(ResolutionMode::Declared),
        "verified" => Ok(ResolutionMode::Verified),
        "both" => Ok(ResolutionMode::Both),
        _ => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "mode must be one of: declared, verified, both".to_owned(),
        }),
    }
}

fn parse_resolution_record_keys(
    records: Option<&str>,
    mode: ResolutionMode,
) -> ApiResult<Vec<ResolutionRecordKey>> {
    let Some(records) = records.map(str::trim).filter(|value| !value.is_empty()) else {
        return if mode.includes_verified() {
            Err(ApiError {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_input",
                message: "records is required when mode is verified or both".to_owned(),
            })
        } else {
            Ok(Vec::new())
        };
    };

    let mut parsed = Vec::new();
    let mut deduped = BTreeSet::new();

    for record_key in records.split(',').map(str::trim) {
        let Some(record) = parse_resolution_record_key(record_key) else {
            return Err(ApiError {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_input",
                message: "records must contain only valid record selectors".to_owned(),
            });
        };

        if mode.includes_verified() && !deduped.insert(record.record_key.clone()) {
            return Err(ApiError {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_input",
                message: "records must not contain duplicate selectors".to_owned(),
            });
        }

        parsed.push(record);
    }

    Ok(parsed)
}

fn parse_resolution_record_key(record_key: &str) -> Option<ResolutionRecordKey> {
    if record_key.is_empty()
        || record_key
            .chars()
            .any(|character| character.is_ascii_whitespace() || character == ',')
    {
        return None;
    }

    let is_valid_family = |family: &str| {
        !family.is_empty()
            && family.chars().all(|character| {
                character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
            })
    };

    match record_key.split_once(':') {
        None if is_valid_family(record_key) => Some(ResolutionRecordKey {
            record_key: record_key.to_owned(),
        }),
        Some((family, selector)) if is_valid_family(family) && !selector.is_empty() => {
            Some(ResolutionRecordKey {
                record_key: record_key.to_owned(),
            })
        }
        _ => None,
    }
}

fn parse_permissions_subject(subject: Option<&str>) -> Option<String> {
    subject
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn parse_permission_scope_filter(scope: Option<&str>) -> ApiResult<Option<PermissionScope>> {
    let Some(scope) = scope.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    if scope == "root" {
        return Ok(Some(PermissionScope::Root));
    }
    if scope == "registry" {
        return Ok(Some(PermissionScope::Registry));
    }
    if scope == "resource" {
        return Ok(Some(PermissionScope::Resource));
    }

    let mut parts = scope.split(':');
    let kind = parts.next().unwrap_or_default();
    let first = parts.next();
    let second = parts.next();
    let extra = parts.next();

    let parsed = match (kind, first, second, extra) {
        ("resolver", Some(chain_id), Some(resolver_address), None) => {
            Some(PermissionScope::Resolver {
                chain_id: chain_id.to_owned(),
                resolver_address: resolver_address.to_ascii_lowercase(),
            })
        }
        ("record_manager", Some(chain_id), Some(manager_address), None) => {
            Some(PermissionScope::RecordManager {
                chain_id: chain_id.to_owned(),
                manager_address: manager_address.to_ascii_lowercase(),
            })
        }
        ("migration_derived", Some(predecessor_resource_id), None, None) => {
            Some(PermissionScope::MigrationDerived {
                predecessor_resource_id: Uuid::parse_str(predecessor_resource_id).map_err(
                    |_| ApiError {
                        status: StatusCode::BAD_REQUEST,
                        code: "invalid_input",
                        message: "scope must use a valid permissions scope filter".to_owned(),
                    },
                )?,
            })
        }
        ("transport_derived", Some(transport), None, None) => {
            Some(PermissionScope::TransportDerived {
                transport: transport.to_owned(),
            })
        }
        _ => None,
    };

    parsed
        .ok_or(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "scope must use a valid permissions scope filter".to_owned(),
        })
        .map(Some)
}

fn parse_pagination(cursor: Option<&str>, page_size: Option<u64>) -> ApiResult<PaginationRequest> {
    let cursor = cursor
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let active = cursor.is_some() || page_size.is_some();

    let page_size = match page_size {
        None if !active => DEFAULT_PAGE_SIZE,
        None => DEFAULT_PAGE_SIZE,
        Some(value) if !(1..=MAX_PAGE_SIZE).contains(&value) => {
            return Err(ApiError {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_input",
                message: format!("page_size must be between 1 and {MAX_PAGE_SIZE}"),
            });
        }
        Some(value) => value,
    };

    Ok(PaginationRequest {
        active,
        cursor,
        page_size,
    })
}

fn paginate_window<T>(
    items: &[T],
    request: &PaginationRequest,
    unpaged_page_size: u64,
    spec: &CursorSpec,
    item_cursor_fields: impl Fn(&T) -> BTreeMap<String, String>,
) -> ApiResult<PaginationWindow> {
    if !request.active {
        return Ok(PaginationWindow {
            start: 0,
            end: items.len(),
            page: HistoryPageResponse {
                cursor: None,
                next_cursor: None,
                page_size: unpaged_page_size,
                sort: spec.sort.to_owned(),
            },
        });
    }

    let start = match request.cursor.as_deref() {
        None => 0,
        Some(cursor) => {
            let decoded = decode_cursor(cursor)?;
            validate_cursor(spec, &decoded)?;
            items
                .iter()
                .position(|item| item_cursor_fields(item) == decoded.item)
                .map(|index| index + 1)
                .ok_or_else(invalid_cursor_error)?
        }
    };
    let end = (start + request.page_size as usize).min(items.len());
    let next_cursor = if end < items.len() {
        Some(encode_cursor(
            &spec.envelope(item_cursor_fields(&items[end - 1])),
        ))
    } else {
        None
    };

    Ok(PaginationWindow {
        start,
        end,
        page: HistoryPageResponse {
            cursor: request.cursor.clone(),
            next_cursor,
            page_size: request.page_size,
            sort: spec.sort.to_owned(),
        },
    })
}

fn invalid_cursor_error() -> ApiError {
    ApiError {
        status: StatusCode::BAD_REQUEST,
        code: "invalid_input",
        message: "cursor must be a valid pagination cursor".to_owned(),
    }
}

fn validate_cursor(spec: &CursorSpec, cursor: &CursorEnvelope) -> ApiResult<()> {
    if cursor.version != CURSOR_VERSION
        || cursor.route != spec.route
        || cursor.anchor != spec.anchor
        || cursor.sort != spec.sort
        || cursor.filters != spec.filters
    {
        return Err(invalid_cursor_error());
    }

    Ok(())
}

fn decode_cursor(cursor: &str) -> ApiResult<CursorEnvelope> {
    let decoded = decode_hex(cursor).ok_or_else(invalid_cursor_error)?;
    serde_json::from_slice(&decoded).map_err(|_| invalid_cursor_error())
}

fn encode_cursor(cursor: &CursorEnvelope) -> String {
    encode_hex(&serde_json::to_vec(cursor).expect("cursor envelope must serialize for pagination"))
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("hex encoding must write into string");
    }
    encoded
}

fn decode_hex(value: &str) -> Option<Vec<u8>> {
    if value.len() % 2 != 0 {
        return None;
    }

    let mut decoded = Vec::with_capacity(value.len() / 2);
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let high = decode_hex_nibble(bytes[index])?;
        let low = decode_hex_nibble(bytes[index + 1])?;
        decoded.push((high << 4) | low);
        index += 2;
    }
    Some(decoded)
}

fn decode_hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn address_name_cursor_fields(entry: &AddressNameCurrentEntry) -> BTreeMap<String, String> {
    let mut item = BTreeMap::new();
    item.insert(
        "canonical_display_name".to_owned(),
        entry.canonical_display_name.clone(),
    );
    item.insert("logical_name_id".to_owned(), entry.logical_name_id.clone());
    item.insert("resource_id".to_owned(), entry.resource_id.to_string());
    item
}

fn child_cursor_fields(row: &ChildrenCurrentRow) -> BTreeMap<String, String> {
    let mut item = BTreeMap::new();
    item.insert(
        "canonical_display_name".to_owned(),
        row.canonical_display_name.clone(),
    );
    item.insert(
        "child_logical_name_id".to_owned(),
        row.child_logical_name_id.clone(),
    );
    item
}

fn permission_cursor_fields(row: &PermissionsCurrentRow) -> BTreeMap<String, String> {
    let mut item = BTreeMap::new();
    item.insert("subject".to_owned(), row.subject.clone());
    item.insert("scope".to_owned(), row.scope.storage_key());
    item
}

fn history_cursor_fields(row: &HistoryEvent) -> BTreeMap<String, String> {
    let mut item = BTreeMap::new();
    item.insert(
        "normalized_event_id".to_owned(),
        row.normalized_event_id.to_string(),
    );
    item.insert("event_identity".to_owned(), row.event_identity.clone());
    item
}

fn parse_children_query(query: &ChildrenQuery) -> ApiResult<bool> {
    parse_children_surface_classes(query.surface_classes.as_deref())?;
    parse_children_include_counts(query.include.as_deref())
}

fn parse_address_names_namespace(namespace: Option<&str>) -> ApiResult<Option<String>> {
    let Some(namespace) = namespace.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    if PUBLIC_NAMESPACES.contains(&namespace) {
        Ok(Some(namespace.to_owned()))
    } else {
        Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "namespace must be one of: ens, basenames".to_owned(),
        })
    }
}

fn parse_address_name_relation(relation: Option<&str>) -> ApiResult<Option<AddressNameRelation>> {
    match relation.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(None),
        Some("registrant") => Ok(Some(AddressNameRelation::Registrant)),
        Some("token_holder") => Ok(Some(AddressNameRelation::TokenHolder)),
        Some("effective_controller") => Ok(Some(AddressNameRelation::EffectiveController)),
        Some(_) => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "relation must be one of: registrant, token_holder, effective_controller"
                .to_owned(),
        }),
    }
}

fn parse_address_names_dedupe_by(dedupe_by: Option<&str>) -> ApiResult<AddressNamesCurrentDedupe> {
    match dedupe_by.unwrap_or("surface") {
        "surface" => Ok(AddressNamesCurrentDedupe::Surface),
        "resource" => Ok(AddressNamesCurrentDedupe::Resource),
        _ => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "dedupe_by must be one of: surface, resource".to_owned(),
        }),
    }
}

fn parse_address_names_include(include: Option<&str>) -> ApiResult<AddressNamesIncludeOptions> {
    let mut options = AddressNamesIncludeOptions::default();

    for value in include
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        match value {
            "role_summary" => options.role_summary = true,
            _ => {
                return Err(ApiError {
                    status: StatusCode::BAD_REQUEST,
                    code: "invalid_input",
                    message: "include must contain only role_summary".to_owned(),
                });
            }
        }
    }

    Ok(options)
}

fn parse_children_surface_classes(surface_classes: Option<&str>) -> ApiResult<()> {
    let mut requested_non_declared = false;

    for value in surface_classes
        .unwrap_or("declared")
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        match value {
            "declared" => {}
            "linked" | "alias" | "wildcard" => requested_non_declared = true,
            _ => {
                return Err(ApiError {
                    status: StatusCode::BAD_REQUEST,
                    code: "invalid_input",
                    message:
                        "surface_classes must contain only declared, linked, alias, or wildcard"
                            .to_owned(),
                });
            }
        }
    }

    if requested_non_declared {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "unsupported",
            message: "surface_classes other than declared are not yet supported".to_owned(),
        });
    }

    Ok(())
}

fn parse_children_include_counts(include: Option<&str>) -> ApiResult<bool> {
    let mut include_counts = false;

    for value in include
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        match value {
            "counts" => include_counts = true,
            _ => {
                return Err(ApiError {
                    status: StatusCode::BAD_REQUEST,
                    code: "invalid_input",
                    message: "include must contain only counts".to_owned(),
                });
            }
        }
    }

    Ok(include_counts)
}

fn normalize_address(address: &str) -> String {
    address.to_ascii_lowercase()
}

fn address_names_dedupe_label(dedupe_by: AddressNamesCurrentDedupe) -> &'static str {
    match dedupe_by {
        AddressNamesCurrentDedupe::Surface => "surface",
        AddressNamesCurrentDedupe::Resource => "resource",
    }
}

async fn resource_ids_for_name(pool: &PgPool, logical_name_id: &str) -> ApiResult<Vec<Uuid>> {
    let bindings = load_surface_bindings_by_logical_name_id(pool, logical_name_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                logical_name_id = %logical_name_id,
                error = ?load_error,
                "failed to load surface bindings for name history"
            );
            ApiError::internal_error(format!(
                "failed to load history bindings for logical name {logical_name_id}"
            ))
        })?;

    Ok(bindings
        .into_iter()
        .map(|binding| binding.resource_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

async fn logical_name_ids_for_resource(pool: &PgPool, resource_id: Uuid) -> ApiResult<Vec<String>> {
    let bindings = load_surface_bindings_by_resource_id(pool, resource_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                resource_id = %resource_id,
                error = ?load_error,
                "failed to load surface bindings for resource history"
            );
            ApiError::internal_error(format!(
                "failed to load history bindings for resource {resource_id}"
            ))
        })?;

    Ok(bindings
        .into_iter()
        .map(|binding| binding.logical_name_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

fn chain_position_key(chain_id: &str) -> String {
    match chain_id {
        "ethereum-mainnet" => "ethereum".to_owned(),
        "base-mainnet" => "base".to_owned(),
        other => other.to_owned(),
    }
}

fn history_manifest_version(row: &HistoryEvent) -> JsonValue {
    json!({
        "manifest_version": row.manifest_version,
        "source_family": row.source_family.clone(),
        "source_manifest_id": row.source_manifest_id,
    })
}

fn ensure_public_namespace(namespace: &str) -> ApiResult<()> {
    if PUBLIC_NAMESPACES.contains(&namespace) {
        Ok(())
    } else {
        Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!("namespace {namespace} is not supported"),
        })
    }
}

fn collect_unique(values: impl Iterator<Item = String>) -> Vec<String> {
    values.collect::<BTreeSet<_>>().into_iter().collect()
}

async fn shutdown_signal(service: &'static str) {
    match tokio::signal::ctrl_c().await {
        Ok(()) => info!(service = service, "shutdown signal received"),
        Err(error) => tracing::warn!(
            service = service,
            error = ?error,
            "failed to listen for shutdown signal"
        ),
    }
}

fn init_tracing(service: &'static str) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    if std::env::var_os("BIGNAME_LOG_JSON").is_some() {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .json()
            .with_target(false)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .compact()
            .with_target(false)
            .init();
    }

    info!(
        service = service,
        phase = bigname_domain::bootstrap_phase(),
        "logging configured"
    );
}

#[cfg(test)]
mod tests;
