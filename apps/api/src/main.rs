use std::{collections::BTreeMap, net::SocketAddr};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use bigname_manifests::{
    ActiveManifestVersion, CapabilityFlag, NamespaceManifestSnapshot,
    load_namespace_manifest_snapshot,
};
use bigname_storage::DatabaseConfig;
use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
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

#[derive(Serialize)]
struct HealthResponse {
    service: &'static str,
    phase: &'static str,
    status: &'static str,
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
    Router::new()
        .route("/healthz", get(health))
        .route("/v1/manifests/{namespace}", get(namespace_manifests))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        service: "api",
        phase: state.phase,
        status: "ok",
    })
}

async fn namespace_manifests(
    Path(namespace): Path<String>,
    State(state): State<AppState>,
) -> ApiResult<Json<NamespaceManifestsResponse>> {
    if !PUBLIC_NAMESPACES.contains(&namespace.as_str()) {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!("namespace {namespace} is not supported"),
        });
    }

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
mod tests {
    use std::{
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::Context;
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use bigname_storage::default_database_url;
    use serde::de::DeserializeOwned;
    use sqlx::{
        PgPool, Row,
        postgres::{PgConnectOptions, PgPoolOptions},
    };
    use tower::ServiceExt;

    use super::*;

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDatabase {
        admin_pool: PgPool,
        pool: PgPool,
        database_name: String,
    }

    impl TestDatabase {
        async fn new(initialize_manifest_schema: bool) -> Result<Self> {
            let database_url = std::env::var("BIGNAME_DATABASE_URL")
                .or_else(|_| std::env::var("DATABASE_URL"))
                .unwrap_or_else(|_| default_database_url().to_owned());
            let base_options = PgConnectOptions::from_str(&database_url)
                .context("failed to parse database URL for API tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_api_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone())
                .await
                .context("failed to connect admin pool for API tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect API test pool")?;

            if initialize_manifest_schema {
                sqlx::query(
                    r#"
                    CREATE TYPE manifest_rollout_status AS ENUM (
                        'draft',
                        'shadow',
                        'active',
                        'deprecated'
                    )
                    "#,
                )
                .execute(&pool)
                .await
                .context("failed to create manifest_rollout_status for API tests")?;
                sqlx::query(
                    r#"
                    CREATE TYPE capability_support_status AS ENUM (
                        'unsupported',
                        'shadow',
                        'supported'
                    )
                    "#,
                )
                .execute(&pool)
                .await
                .context("failed to create capability_support_status for API tests")?;
                sqlx::query(
                    r#"
                    CREATE TABLE manifest_versions (
                        manifest_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        manifest_version BIGINT NOT NULL CHECK (manifest_version > 0),
                        namespace TEXT NOT NULL,
                        source_family TEXT NOT NULL,
                        chain TEXT NOT NULL,
                        deployment_epoch TEXT NOT NULL,
                        rollout_status manifest_rollout_status NOT NULL,
                        normalizer_version TEXT NOT NULL,
                        file_path TEXT NOT NULL,
                        manifest_payload JSONB NOT NULL,
                        loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    "#,
                )
                .execute(&pool)
                .await
                .context("failed to create manifest_versions for API tests")?;
                sqlx::query(
                    r#"
                    CREATE TABLE manifest_capability_flags (
                        manifest_id BIGINT NOT NULL REFERENCES manifest_versions (manifest_id) ON DELETE CASCADE,
                        capability_name TEXT NOT NULL,
                        status capability_support_status NOT NULL,
                        notes TEXT,
                        PRIMARY KEY (manifest_id, capability_name)
                    )
                    "#,
                )
                .execute(&pool)
                .await
                .context("failed to create manifest_capability_flags for API tests")?;
            }

            Ok(Self {
                admin_pool,
                pool,
                database_name,
            })
        }

        fn app_state(&self) -> AppState {
            AppState {
                phase: "test",
                pool: self.pool.clone(),
            }
        }

        async fn insert_manifest(
            &self,
            namespace: &str,
            source_family: &str,
            chain: &str,
            deployment_epoch: &str,
            manifest_version: u64,
            rollout_status: &str,
            normalizer_version: &str,
        ) -> Result<i64> {
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let file_path =
                format!("tests/{namespace}/{source_family}/{manifest_version}-{sequence}.toml");

            sqlx::query(
                r#"
                INSERT INTO manifest_versions (
                    manifest_version,
                    namespace,
                    source_family,
                    chain,
                    deployment_epoch,
                    rollout_status,
                    normalizer_version,
                    file_path,
                    manifest_payload
                )
                VALUES ($1, $2, $3, $4, $5, $6::manifest_rollout_status, $7, $8, $9::jsonb)
                RETURNING manifest_id
                "#,
            )
            .bind(i64::try_from(manifest_version).context("manifest_version exceeds BIGINT")?)
            .bind(namespace)
            .bind(source_family)
            .bind(chain)
            .bind(deployment_epoch)
            .bind(rollout_status)
            .bind(normalizer_version)
            .bind(file_path)
            .bind("{}")
            .fetch_one(&self.pool)
            .await
            .context("failed to insert manifest_version for API test")?
            .try_get("manifest_id")
            .context("failed to read manifest_id for API test")
        }

        async fn insert_capability_flag(
            &self,
            manifest_id: i64,
            capability_name: &str,
            status: &str,
            notes: Option<&str>,
        ) -> Result<()> {
            sqlx::query(
                r#"
                INSERT INTO manifest_capability_flags (
                    manifest_id,
                    capability_name,
                    status,
                    notes
                )
                VALUES ($1, $2, $3::capability_support_status, $4)
                "#,
            )
            .bind(manifest_id)
            .bind(capability_name)
            .bind(status)
            .bind(notes)
            .execute(&self.pool)
            .await
            .context("failed to insert manifest capability flag for API test")?;

            Ok(())
        }

        async fn cleanup(self) -> Result<()> {
            self.pool.close().await;
            sqlx::query(&format!(
                r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
                self.database_name
            ))
            .execute(&self.admin_pool)
            .await
            .with_context(|| format!("failed to drop test database {}", self.database_name))?;
            self.admin_pool.close().await;
            Ok(())
        }
    }

    async fn read_json<T: DeserializeOwned>(response: Response) -> Result<T> {
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .context("failed to read API response body")?;
        serde_json::from_slice(&bytes).context("failed to decode API response JSON")
    }

    #[tokio::test]
    async fn get_namespace_manifests_returns_active_entries() -> Result<()> {
        let database = TestDatabase::new(true).await?;

        let ens_l1 = database
            .insert_manifest(
                "ens",
                "ens_v2_registry_l1",
                "ethereum-mainnet",
                "ens_v2",
                1,
                "active",
                "uts46-v1",
            )
            .await?;
        database
            .insert_capability_flag(ens_l1, "declared_children", "supported", None)
            .await?;
        database
            .insert_capability_flag(
                ens_l1,
                "verified_resolution",
                "shadow",
                Some("tracked but not yet served"),
            )
            .await?;

        let ens_l2 = database
            .insert_manifest(
                "ens",
                "ens_v2_registry_l2",
                "base-mainnet",
                "ens_v2_base",
                2,
                "active",
                "uts46-v2",
            )
            .await?;
        database
            .insert_capability_flag(ens_l2, "declared_children", "unsupported", Some("pending"))
            .await?;

        let ens_shadow = database
            .insert_manifest(
                "ens",
                "ens_shadow_registry",
                "ethereum-mainnet",
                "ens_shadow",
                3,
                "shadow",
                "uts46-v1",
            )
            .await?;
        database
            .insert_capability_flag(ens_shadow, "declared_children", "supported", None)
            .await?;

        let basenames = database
            .insert_manifest(
                "basenames",
                "base_registry",
                "base-mainnet",
                "basenames_v1",
                1,
                "active",
                "uts46-v1",
            )
            .await?;
        database
            .insert_capability_flag(basenames, "declared_children", "supported", None)
            .await?;

        let response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .uri("/v1/manifests/ens")
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("manifest request failed")?;

        assert_eq!(response.status(), StatusCode::OK);

        let payload: NamespaceManifestsResponse = read_json(response).await?;
        assert_eq!(payload.data.namespace, "ens");
        assert_eq!(payload.consistency, "head");
        assert!(payload.last_updated.ends_with('Z'));
        assert!(payload.verified_state.is_none());
        assert!(payload.chain_positions.is_empty());
        assert_eq!(payload.coverage.status, "full");
        assert_eq!(payload.coverage.exhaustiveness, "authoritative");
        assert_eq!(
            payload.coverage.source_classes_considered,
            vec!["source_manifests".to_owned()]
        );
        assert_eq!(
            payload.coverage.enumeration_basis,
            "active manifests for the requested namespace"
        );
        assert_eq!(payload.coverage.unsupported_reason, None);
        assert!(payload.provenance.normalized_event_ids.is_empty());
        assert!(payload.provenance.raw_fact_refs.is_empty());
        assert_eq!(payload.provenance.derivation_kind, "declared");
        assert_eq!(payload.provenance.execution_trace_id, None);
        assert_eq!(payload.provenance.manifest_versions.len(), 2);
        assert_eq!(payload.declared_state.manifests.len(), 2);

        assert_eq!(payload.declared_state.manifests[0].manifest_version, 1);
        assert_eq!(
            payload.declared_state.manifests[0].source_family,
            "ens_v2_registry_l1"
        );
        assert_eq!(
            payload.declared_state.manifests[0].chain,
            "ethereum-mainnet"
        );
        assert_eq!(
            payload.declared_state.manifests[0].deployment_epoch,
            "ens_v2"
        );
        assert_eq!(
            payload.declared_state.manifests[0].normalizer_version,
            "uts46-v1"
        );
        assert_eq!(
            payload.declared_state.manifests[0]
                .capability_flags
                .get("declared_children")
                .expect("declared_children capability")
                .status,
            bigname_manifests::CapabilitySupportStatus::Supported
        );
        assert_eq!(
            payload.declared_state.manifests[0]
                .capability_flags
                .get("verified_resolution")
                .expect("verified_resolution capability")
                .notes
                .as_deref(),
            Some("tracked but not yet served")
        );
        assert_eq!(
            payload.provenance.manifest_versions[0],
            ManifestVersionRef {
                manifest_version: 1,
                source_family: "ens_v2_registry_l1".to_owned(),
                chain: "ethereum-mainnet".to_owned(),
                deployment_epoch: "ens_v2".to_owned(),
            }
        );

        assert_eq!(payload.declared_state.manifests[1].manifest_version, 2);
        assert_eq!(
            payload.declared_state.manifests[1].source_family,
            "ens_v2_registry_l2"
        );
        assert_eq!(payload.declared_state.manifests[1].chain, "base-mainnet");
        assert_eq!(
            payload.declared_state.manifests[1].deployment_epoch,
            "ens_v2_base"
        );
        assert_eq!(
            payload.declared_state.manifests[1].normalizer_version,
            "uts46-v2"
        );
        assert_eq!(
            payload.declared_state.manifests[1]
                .capability_flags
                .get("declared_children")
                .expect("declared_children capability")
                .status,
            bigname_manifests::CapabilitySupportStatus::Unsupported
        );
        assert_eq!(
            payload.provenance.manifest_versions[1],
            ManifestVersionRef {
                manifest_version: 2,
                source_family: "ens_v2_registry_l2".to_owned(),
                chain: "base-mainnet".to_owned(),
                deployment_epoch: "ens_v2_base".to_owned(),
            }
        );

        database.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn get_namespace_manifests_returns_empty_list_when_namespace_has_no_active_entries()
    -> Result<()> {
        let database = TestDatabase::new(true).await?;

        let ens_shadow = database
            .insert_manifest(
                "ens",
                "ens_shadow_registry",
                "ethereum-mainnet",
                "ens_shadow",
                1,
                "shadow",
                "uts46-v1",
            )
            .await?;
        database
            .insert_capability_flag(ens_shadow, "declared_children", "supported", None)
            .await?;

        let response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .uri("/v1/manifests/ens")
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("manifest request failed")?;

        assert_eq!(response.status(), StatusCode::OK);

        let payload: NamespaceManifestsResponse = read_json(response).await?;
        assert_eq!(payload.data.namespace, "ens");
        assert!(payload.declared_state.manifests.is_empty());
        assert!(payload.provenance.manifest_versions.is_empty());
        assert_eq!(payload.coverage.status, "full");
        assert_eq!(payload.coverage.exhaustiveness, "authoritative");
        assert_eq!(
            payload.coverage.source_classes_considered,
            vec!["source_manifests".to_owned()]
        );
        assert_eq!(
            payload.coverage.enumeration_basis,
            "active manifests for the requested namespace"
        );
        assert_eq!(payload.provenance.derivation_kind, "declared");
        assert_eq!(payload.consistency, "head");
        assert!(payload.last_updated.ends_with('Z'));
        assert!(payload.verified_state.is_none());
        assert!(payload.chain_positions.is_empty());

        database.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn get_namespace_manifests_returns_internal_error_envelope_on_load_failure() -> Result<()>
    {
        let database = TestDatabase::new(false).await?;

        let response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .uri("/v1/manifests/ens")
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("manifest request failed")?;

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let payload: ErrorResponse = read_json(response).await?;
        assert_eq!(payload.error.code, "internal_error");
        assert_eq!(
            payload.error.message,
            "failed to load manifest snapshot for namespace ens"
        );
        assert!(payload.error.details.is_empty());

        database.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn get_namespace_manifests_returns_not_found_for_unknown_namespace() -> Result<()> {
        let database = TestDatabase::new(true).await?;

        let response = app_router(database.app_state())
            .oneshot(
                Request::builder()
                    .uri("/v1/manifests/unknown")
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("manifest request failed")?;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let payload: ErrorResponse = read_json(response).await?;
        assert_eq!(payload.error.code, "not_found");
        assert_eq!(payload.error.message, "namespace unknown is not supported");
        assert!(payload.error.details.is_empty());

        database.cleanup().await?;
        Ok(())
    }
}
