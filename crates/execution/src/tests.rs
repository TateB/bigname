use std::{
    path::PathBuf,
    process::Command,
    str::FromStr,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use serde_json::{Map, Value, json};
use sqlx::{
    ConnectOptions, PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::time::OffsetDateTime,
};
use tokio::time::{Duration, timeout};

use super::primary_name::{
    normalized_verified_primary_name_request_key, validate_verified_primary_request,
};
use super::revalidation::{
    build_requested_chain_positions_from_projection, normalize_alias_detail,
    normalize_transport_detail, normalize_wildcard_detail, selector_family_and_key,
};
use super::validation::{
    VerifiedQueryStatus, VerifiedQuerySummary, extract_requested_selectors,
    extract_supported_verified_queries, normalized_request_key, persisted_trace_detail_object,
    validate_direct_request,
};
use super::*;
use bigname_storage::{
    ChainLineageBlock, ExecutionCacheKey, ExecutionOutcome, ExecutionTrace, MIGRATOR,
    NameCurrentRow, NameSurface, NormalizedEvent, PrimaryNameClaimStatus, PrimaryNameCurrentRow,
    RawCallSnapshot, RecordInventoryCurrentRow, Resource,
    SupportedVerifiedResolutionRecordKey as SupportedVerifiedRecordKey, SurfaceBinding,
    SurfaceBindingKind, TokenLineage, default_database_url, upsert_chain_lineage_blocks,
    upsert_execution_outcome, upsert_execution_trace, upsert_name_current_rows,
    upsert_name_surfaces, upsert_primary_name_current_rows, upsert_record_inventory_current_rows,
    upsert_resources, upsert_surface_bindings, upsert_token_lineages,
};
use uuid::Uuid;

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);
static WORKER_CARGO_LOCK: Mutex<()> = Mutex::new(());

struct TestDatabase {
    admin_pool: PgPool,
    pool: PgPool,
    database_name: String,
}

impl TestDatabase {
    async fn new() -> Result<Self> {
        Self::new_with_max_connections(5).await
    }

    async fn new_with_max_connections(max_connections: u32) -> Result<Self> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for execution tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_execution_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for execution tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect execution test pool")?;

        MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for execution tests")?;

        Ok(Self {
            admin_pool,
            pool,
            database_name,
        })
    }

    fn pool(&self) -> &PgPool {
        &self.pool
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

fn raw_block(
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
    block_timestamp: i64,
) -> bigname_storage::RawBlock {
    bigname_storage::RawBlock {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: None,
        block_number,
        block_timestamp: timestamp(block_timestamp),
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn chain_lineage_block(
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
    block_timestamp: i64,
) -> ChainLineageBlock {
    ChainLineageBlock {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: Some(format!("0xparent{block_number:08x}")),
        block_number,
        block_timestamp: timestamp(block_timestamp),
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn logical_name_id_for_request(request: &PersistEnsExactNameVerifiedResolutionRequest) -> String {
    let requested_selectors =
        extract_requested_selectors(&request.trace).expect("request selectors must parse");
    format!(
        "{}:{}",
        request.trace.namespace, requested_selectors.surface
    )
}

async fn rebuild_name_current_projection(
    database: &TestDatabase,
    logical_name_id: &str,
) -> Result<()> {
    let database_url = std::env::var("BIGNAME_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| default_database_url().to_owned());
    let base_options = PgConnectOptions::from_str(&database_url)
        .context("failed to parse database URL for execution name_current rebuild")?;
    let rebuild_database_url = base_options
        .database(&database.database_name)
        .to_url_lossy()
        .to_string();
    let logical_name_id = logical_name_id.to_owned();
    let worker_manifest_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../apps/worker/Cargo.toml");

    tokio::task::spawn_blocking(move || -> Result<()> {
        let _guard = WORKER_CARGO_LOCK
            .lock()
            .expect("worker cargo lock must not be poisoned");
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
        let output = Command::new(cargo)
            .arg("run")
            .arg("--quiet")
            .arg("--manifest-path")
            .arg(worker_manifest_path)
            .arg("--")
            .arg("name-current")
            .arg("rebuild")
            .arg("--database-url")
            .arg(&rebuild_database_url)
            .arg("--logical-name-id")
            .arg(&logical_name_id)
            .output()
            .with_context(|| {
                format!(
                    "failed to invoke worker name_current rebuild for {logical_name_id}"
                )
            })?;

        if !output.status.success() {
            return Err(anyhow::anyhow!(
                "worker name_current rebuild failed for {logical_name_id}\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            ));
        }

        Ok(())
    })
    .await
    .context("execution name_current rebuild task panicked")??;

    Ok(())
}

fn requested_chain_positions_json_from_projection(chain_positions: &Value) -> Result<Value> {
    Ok(Value::Array(
        build_requested_chain_positions_from_projection(chain_positions)?
            .into_iter()
            .map(|position| {
                json!({
                    "chain_id": position.chain_id,
                    "block_number": position.block_number,
                    "block_hash": position.block_hash,
                })
            })
            .collect(),
    ))
}

fn append_cache_key_part(buffer: &mut String, value: &str) {
    use std::fmt::Write as _;

    write!(buffer, "{}:{value};", value.len())
        .expect("string write to cache-key buffer must succeed");
}

fn manifest_versions_for_request_from_projection(value: &Value) -> Result<Value> {
    let items = value
        .as_array()
        .context("rebuilt execution test manifest_versions must be an array")?;
    let mut sanitized = items
        .iter()
        .enumerate()
        .map(|(index, item)| -> Result<(String, Value)> {
            let object = item.as_object().with_context(|| {
                format!("rebuilt execution test manifest_versions[{index}] must be an object")
            })?;
            let mut manifest_version = Map::new();
            let mut identity_key = String::new();
            let source_manifest_id = object
                .get("source_manifest_id")
                .and_then(Value::as_i64)
                .filter(|value| *value > 0);
            append_cache_key_part(
                &mut identity_key,
                &source_manifest_id
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
            );
            if let Some(source_manifest_id) = source_manifest_id {
                manifest_version.insert(
                    "source_manifest_id".to_owned(),
                    Value::Number(source_manifest_id.into()),
                );
            }
            let source_family = object
                .get("source_family")
                .and_then(Value::as_str)
                .map(str::to_owned);
            append_cache_key_part(
                &mut identity_key,
                source_family.as_deref().unwrap_or_default(),
            );
            if let Some(source_family) = source_family {
                manifest_version.insert(
                    "source_family".to_owned(),
                    Value::String(source_family),
                );
            }
            let manifest_version_number = object
                .get("manifest_version")
                .and_then(Value::as_i64)
                .filter(|value| *value > 0)
                .with_context(|| {
                    format!(
                        "rebuilt execution test manifest_versions[{index}].manifest_version must be positive"
                    )
                })?;
            append_cache_key_part(&mut identity_key, &manifest_version_number.to_string());
            manifest_version.insert(
                "manifest_version".to_owned(),
                Value::Number(manifest_version_number.into()),
            );
            Ok((identity_key, Value::Object(manifest_version)))
        })
        .collect::<Result<Vec<_>>>()?;
    sanitized.sort_by(|left, right| left.0.cmp(&right.0));
    sanitized.dedup_by(|left, right| left.0 == right.0);
    Ok(Value::Array(
        sanitized
            .into_iter()
            .map(|(_, manifest_version)| manifest_version)
            .collect(),
    ))
}

fn sync_request_with_rebuilt_name_current(
    request: &mut PersistEnsExactNameVerifiedResolutionRequest,
    row: &NameCurrentRow,
) -> Result<()> {
    let topology = row
        .declared_summary
        .get("topology")
        .cloned()
        .context("rebuilt execution test row must project topology")?;
    let version_boundaries = topology
        .get("version_boundaries")
        .and_then(Value::as_object)
        .context("rebuilt execution test topology must include version_boundaries")?;
    request.outcome.cache_key.manifest_versions = manifest_versions_for_request_from_projection(
        row.provenance
            .get("manifest_versions")
            .unwrap_or(&Value::Null),
    )?;
    request.outcome.cache_key.requested_chain_positions =
        requested_chain_positions_json_from_projection(&row.chain_positions)?;
    request.outcome.cache_key.topology_version_boundary = version_boundaries
        .get("topology_version_boundary")
        .cloned()
        .context("rebuilt execution test topology must include topology boundary")?;
    request.outcome.cache_key.record_version_boundary = version_boundaries
        .get("record_version_boundary")
        .cloned()
        .context("rebuilt execution test topology must include record boundary")?;
    request.trace.chain_context["requested_positions"] =
        request.outcome.cache_key.requested_chain_positions.clone();
    let requested_positions = request
        .outcome
        .cache_key
        .requested_chain_positions
        .as_array()
        .cloned()
        .unwrap_or_default();
    for snapshot in &mut request.raw_call_snapshots {
        let requested_position = requested_positions
            .iter()
            .find(|position| {
                position
                    .get("chain_id")
                    .and_then(Value::as_str)
                    .is_some_and(|chain_id| chain_id == snapshot.chain_id)
            })
            .or_else(|| requested_positions.first());
        if let Some(requested_position) = requested_position {
            if let Some(chain_id) = requested_position.get("chain_id").and_then(Value::as_str) {
                snapshot.chain_id = chain_id.to_owned();
            }
            if let Some(block_number) = requested_position
                .get("block_number")
                .and_then(Value::as_i64)
            {
                snapshot.block_number = block_number;
            }
            if let Some(block_hash) = requested_position.get("block_hash").and_then(Value::as_str) {
                snapshot.block_hash = block_hash.to_owned();
            }
        }
    }

    if request.trace.namespace == ENS_NAMESPACE {
        request.trace.request_metadata["alias"] = topology["alias"].clone();
        request.trace.request_metadata["wildcard"] = topology["wildcard"].clone();
    }
    if request.trace.namespace == BASENAMES_NAMESPACE {
        request.trace.request_metadata["transport"] = topology["transport"].clone();
    }

    Ok(())
}

fn timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
}

fn requested_chain_positions() -> Value {
    json!([
        {
            "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
            "block_number": 21_000_000,
            "block_hash": "0xabc123"
        }
    ])
}

fn version_boundary(resource_id: Uuid) -> Value {
    json!({
        "logical_name_id": "ens:alice.eth",
        "resource_id": resource_id.to_string(),
        "normalized_event_id": 1_200,
        "event_kind": "RecordsChanged",
        "chain_position": {
            "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
            "block_number": 21_000_000,
            "block_hash": "0xabc123",
            "timestamp": "2024-06-01T00:00:17Z",
        }
    })
}

fn manifest_versions() -> Value {
    json!([
        {
            "source_family": ENS_EXECUTION_SOURCE_FAMILY,
            "manifest_version": 1
        },
        {
            "source_manifest_id": 7,
            "manifest_version": 3
        }
    ])
}

fn basenames_manifest_versions() -> Value {
    json!([
        {
            "source_family": BASENAMES_EXECUTION_SOURCE_FAMILY,
            "manifest_version": 2
        },
        {
            "source_manifest_id": 9,
            "manifest_version": 4
        }
    ])
}

fn basenames_requested_chain_positions() -> Value {
    json!([
        {
            "chain_id": BASE_MAINNET_CHAIN_ID,
            "block_number": 31_000_000,
            "block_hash": "0xbase123"
        },
        {
            "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
            "block_number": 21_000_000,
            "block_hash": "0xl1abc"
        }
    ])
}

fn basenames_version_boundary(resource_id: Uuid) -> Value {
    json!({
        "logical_name_id": "basenames:alice.base.eth",
        "resource_id": resource_id.to_string(),
        "normalized_event_id": 1_300,
        "event_kind": "RecordVersionChanged",
        "chain_position": {
            "chain_id": BASE_MAINNET_CHAIN_ID,
            "block_number": 31_000_000,
            "block_hash": "0xbase123",
            "timestamp": "2024-06-01T00:00:17Z",
        }
    })
}

fn raw_call_snapshot() -> RawCallSnapshot {
    RawCallSnapshot {
        chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
        block_hash: "0xabc123".to_owned(),
        block_number: 21_000_000,
        request_hash: "0xreq-a".to_owned(),
        request_payload: json!({
            "to": ENS_UNIVERSAL_RESOLVER_ADDRESS,
            "data": "0x9061b923"
        }),
        response_hash: "0xresp-a".to_owned(),
        response_payload: json!({
            "result": "0x00000000000000000000000000000000000000aa"
        }),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn alias_target(resource_id: Uuid) -> Value {
    json!({
        "logical_name_id": "ens:profile.alice.eth",
        "namespace": ENS_NAMESPACE,
        "normalized_name": "profile.alice.eth",
        "canonical_display_name": "Profile.alice.eth",
        "namehash": "namehash:profile.alice.eth",
        "resource_id": resource_id.to_string(),
        "binding_kind": RESOLVER_ALIAS_PATH_BINDING_KIND
    })
}

fn wildcard_source_ref(resource_id: Uuid) -> Value {
    json!({
        "logical_name_id": "ens:eth",
        "namespace": ENS_NAMESPACE,
        "normalized_name": "eth",
        "canonical_display_name": "Eth",
        "namehash": "namehash:eth",
        "resource_id": resource_id.to_string(),
        "binding_kind": OBSERVED_WILDCARD_PATH_BINDING_KIND
    })
}

fn boundary_resource_id(boundary: &Value) -> Uuid {
    let resource_id = boundary
        .get("resource_id")
        .and_then(Value::as_str)
        .expect("version boundary must include resource_id");
    Uuid::parse_str(resource_id).expect("version boundary resource_id must be a UUID")
}

fn first_boundary_manifest_version(manifest_versions: &Value) -> i64 {
    manifest_versions
        .as_array()
        .and_then(|items| items.first())
        .and_then(Value::as_object)
        .and_then(|item| item.get("manifest_version"))
        .and_then(Value::as_i64)
        .unwrap_or(1)
}

fn name_current_manifest_versions_for_request(
    request: &PersistEnsExactNameVerifiedResolutionRequest,
) -> Value {
    let mut manifest_versions = request.outcome.cache_key.manifest_versions.clone();
    if request.trace.namespace == BASENAMES_NAMESPACE
        && let Some(items) = manifest_versions.as_array_mut()
    {
        for item in items {
            let Some(object) = item.as_object_mut() else {
                continue;
            };
            if object
                .get("source_family")
                .and_then(Value::as_str)
                .is_some_and(|value| value == BASENAMES_EXECUTION_SOURCE_FAMILY)
                && object
                    .get("manifest_version")
                    .and_then(Value::as_i64)
                    .is_some_and(|value| value == 2)
            {
                object.insert("chain".to_owned(), json!(ETHEREUM_MAINNET_CHAIN_ID));
                object.insert("deployment_epoch".to_owned(), json!("basenames_v1"));
            }
        }
    }
    manifest_versions
}

fn projection_chain_positions_for_request(
    request: &PersistEnsExactNameVerifiedResolutionRequest,
) -> Value {
    let boundary_timestamps = [
        request
            .outcome
            .cache_key
            .topology_version_boundary
            .get("chain_position"),
        request
            .outcome
            .cache_key
            .record_version_boundary
            .get("chain_position"),
    ];
    let positions = request
        .outcome
        .cache_key
        .requested_chain_positions
        .as_array()
        .expect("requested_chain_positions must be an array");
    let mut chain_positions = Map::new();
    for position in positions {
        let chain_id = position
            .get("chain_id")
            .and_then(Value::as_str)
            .expect("requested chain position must include chain_id");
        let block_number = position
            .get("block_number")
            .and_then(Value::as_i64)
            .expect("requested chain position must include block_number");
        let block_hash = position
            .get("block_hash")
            .and_then(Value::as_str)
            .expect("requested chain position must include block_hash");
        let timestamp = boundary_timestamps
            .iter()
            .flatten()
            .find(|candidate| {
                candidate
                    .get("chain_id")
                    .and_then(Value::as_str)
                    .is_some_and(|value| value == chain_id)
                    && candidate
                        .get("block_hash")
                        .and_then(Value::as_str)
                        .is_some_and(|value| value == block_hash)
                    && candidate
                        .get("block_number")
                        .and_then(Value::as_i64)
                        .is_some_and(|value| value == block_number)
            })
            .and_then(|candidate| candidate.get("timestamp"))
            .and_then(Value::as_str)
            .unwrap_or("2026-04-23T00:00:00Z");
        let key = match chain_id {
            ETHEREUM_MAINNET_CHAIN_ID => "ethereum",
            BASE_MAINNET_CHAIN_ID => "base",
            other => other,
        };
        chain_positions.insert(
            key.to_owned(),
            json!({
                "chain_id": chain_id,
                "block_number": block_number,
                "block_hash": block_hash,
                "timestamp": timestamp
            }),
        );
    }
    Value::Object(chain_positions)
}

fn name_ref_for_request(
    request: &PersistEnsExactNameVerifiedResolutionRequest,
    resource_id: Uuid,
    binding_kind: SurfaceBindingKind,
) -> Value {
    let requested_selectors =
        extract_requested_selectors(&request.trace).expect("request selectors must parse");
    json!({
        "logical_name_id": format!("{}:{}", request.trace.namespace, requested_selectors.surface),
        "namespace": request.trace.namespace,
        "normalized_name": requested_selectors.surface,
        "canonical_display_name": requested_selectors.surface,
        "namehash": format!("namehash:{}", requested_selectors.surface),
        "resource_id": resource_id.to_string(),
        "binding_kind": binding_kind.as_str()
    })
}

fn resolver_path_for_request(
    request: &PersistEnsExactNameVerifiedResolutionRequest,
    resource_id: Uuid,
    binding_kind: SurfaceBindingKind,
    resolver_chain_id: &str,
    resolver_address: &str,
) -> Value {
    let wildcard = persisted_trace_detail_object(&request.trace, "wildcard");
    let hop = wildcard
        .as_ref()
        .and_then(|detail| detail.get("source"))
        .cloned()
        .unwrap_or_else(|| name_ref_for_request(request, resource_id, binding_kind));
    let mut hop = hop
        .as_object()
        .cloned()
        .expect("resolver_path hop must be an object");
    hop.insert("chain_id".to_owned(), json!(resolver_chain_id));
    hop.insert("address".to_owned(), json!(resolver_address));
    hop.insert("latest_event_kind".to_owned(), Value::Null);
    Value::Array(vec![Value::Object(hop)])
}

fn projected_topology_for_request(
    request: &PersistEnsExactNameVerifiedResolutionRequest,
    resource_id: Uuid,
    binding_kind: SurfaceBindingKind,
    resolver_chain_id: &str,
    resolver_address: &str,
) -> Value {
    let alias = normalize_alias_detail(
        persisted_trace_detail_object(&request.trace, "alias").as_ref(),
        &request.trace.namespace,
    )
    .expect("request alias detail must normalize");
    let wildcard = normalize_wildcard_detail(
        persisted_trace_detail_object(&request.trace, "wildcard").as_ref(),
        &request.trace.namespace,
    )
    .expect("request wildcard detail must normalize");
    let transport = normalize_transport_detail(
        persisted_trace_detail_object(&request.trace, "transport").as_ref(),
    )
    .expect("request transport detail must normalize");

    json!({
        "registry_path": [name_ref_for_request(request, resource_id, binding_kind)],
        "subregistry_path": [],
        "resolver_path": resolver_path_for_request(
            request,
            resource_id,
            binding_kind,
            resolver_chain_id,
            resolver_address,
        ),
        "wildcard": wildcard,
        "alias": alias,
        "version_boundaries": {
            "topology_version_boundary": request.outcome.cache_key.topology_version_boundary.clone(),
            "record_version_boundary": request.outcome.cache_key.record_version_boundary.clone(),
        },
        "transport": transport,
    })
}

fn binding_kind_for_request(
    request: &PersistEnsExactNameVerifiedResolutionRequest,
) -> SurfaceBindingKind {
    let requested_selectors =
        extract_requested_selectors(&request.trace).expect("request selectors must parse");
    match requested_selectors.binding_kind.as_deref() {
        None | Some(DECLARED_REGISTRY_PATH_BINDING_KIND) => {
            SurfaceBindingKind::DeclaredRegistryPath
        }
        Some(RESOLVER_ALIAS_PATH_BINDING_KIND) => SurfaceBindingKind::ResolverAliasPath,
        Some(OBSERVED_WILDCARD_PATH_BINDING_KIND) => SurfaceBindingKind::ObservedWildcardPath,
        Some(other) => panic!("unsupported binding_kind fixture {other}"),
    }
}

fn resolver_address_for_request(request: &PersistEnsExactNameVerifiedResolutionRequest) -> String {
    request
        .trace
        .request_metadata
        .get("transport")
        .and_then(|transport| transport.get("contract_address"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            request
                .trace
                .steps
                .iter()
                .find_map(|step| step.step_payload.get("resolver"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| match request.trace.namespace.as_str() {
            ENS_NAMESPACE => "0x0000000000000000000000000000000000000abc".to_owned(),
            BASENAMES_NAMESPACE => "0x0000000000000000000000000000000000000b60".to_owned(),
            other => panic!("unsupported namespace fixture {other}"),
        })
}

fn resolver_chain_id_for_request(request: &PersistEnsExactNameVerifiedResolutionRequest) -> String {
    match request.trace.namespace.as_str() {
        ENS_NAMESPACE => ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
        BASENAMES_NAMESPACE => BASE_MAINNET_CHAIN_ID.to_owned(),
        other => panic!("unsupported namespace fixture {other}"),
    }
}

fn token_lineage_for_request(token_lineage_id: Uuid, chain_id: &str) -> TokenLineage {
    TokenLineage {
        token_lineage_id,
        chain_id: chain_id.to_owned(),
        block_hash: "0xtrace-support".to_owned(),
        block_number: 21_000_000,
        provenance: json!({"source": "execution_test", "anchor": "token_lineage"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn resource_for_request(resource_id: Uuid, token_lineage_id: Uuid, chain_id: &str) -> Resource {
    Resource {
        resource_id,
        token_lineage_id: Some(token_lineage_id),
        chain_id: chain_id.to_owned(),
        block_hash: "0xtrace-support".to_owned(),
        block_number: 21_000_000,
        provenance: json!({"source": "execution_test", "anchor": "resource"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn name_surface_for_request(
    request: &PersistEnsExactNameVerifiedResolutionRequest,
    chain_id: &str,
) -> NameSurface {
    let requested_selectors =
        extract_requested_selectors(&request.trace).expect("request selectors must parse");
    NameSurface {
        logical_name_id: format!(
            "{}:{}",
            request.trace.namespace, requested_selectors.surface
        ),
        namespace: request.trace.namespace.clone(),
        input_name: requested_selectors.surface.clone(),
        canonical_display_name: requested_selectors.surface.clone(),
        normalized_name: requested_selectors.surface.clone(),
        dns_encoded_name: requested_selectors.surface.as_bytes().to_vec(),
        namehash: format!("namehash:{}", requested_selectors.surface),
        labelhashes: vec![format!("labelhash:{}", requested_selectors.surface)],
        normalizer_version: "uts46-v1".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: chain_id.to_owned(),
        block_hash: "0xtrace-support".to_owned(),
        block_number: 21_000_000,
        provenance: json!({"source": "execution_test", "anchor": "surface"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn surface_binding_for_request(
    request: &PersistEnsExactNameVerifiedResolutionRequest,
    surface_binding_id: Uuid,
    resource_id: Uuid,
    binding_kind: SurfaceBindingKind,
    chain_id: &str,
) -> SurfaceBinding {
    let requested_selectors =
        extract_requested_selectors(&request.trace).expect("request selectors must parse");
    SurfaceBinding {
        surface_binding_id,
        logical_name_id: format!(
            "{}:{}",
            request.trace.namespace, requested_selectors.surface
        ),
        resource_id,
        binding_kind,
        active_from: timestamp(1_717_171_700),
        active_to: None,
        chain_id: chain_id.to_owned(),
        block_hash: "0xtrace-support".to_owned(),
        block_number: 21_000_000,
        provenance: json!({"source": "execution_test", "anchor": "binding"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn supported_resolution_name_current_row(
    request: &PersistEnsExactNameVerifiedResolutionRequest,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
) -> NameCurrentRow {
    let requested_selectors =
        extract_requested_selectors(&request.trace).expect("request selectors must parse");
    let binding_kind = binding_kind_for_request(request);
    let resolver_chain_id = resolver_chain_id_for_request(request);
    let resolver_address = resolver_address_for_request(request);
    NameCurrentRow {
        logical_name_id: format!(
            "{}:{}",
            request.trace.namespace, requested_selectors.surface
        ),
        namespace: request.trace.namespace.clone(),
        canonical_display_name: requested_selectors.surface.clone(),
        normalized_name: requested_selectors.surface.clone(),
        namehash: format!("namehash:{}", requested_selectors.surface),
        surface_binding_id: Some(surface_binding_id),
        resource_id: Some(resource_id),
        token_lineage_id: Some(token_lineage_id),
        binding_kind: Some(binding_kind),
        declared_summary: json!({
            "registration": {
                "status": "active",
                "authority_kind": "registrar"
            },
            "resolver": {
                "chain_id": resolver_chain_id,
                "address": resolver_address,
                "latest_event_kind": "ResolverChanged"
            },
            "topology": projected_topology_for_request(
                request,
                resource_id,
                binding_kind,
                &resolver_chain_id_for_request(request),
                &resolver_address_for_request(request),
            )
        }),
        provenance: json!({
            "normalized_event_ids": [101, 102],
            "raw_fact_refs": [{
                "kind": "log",
                "chain_id": resolver_chain_id_for_request(request),
                "block_hash": "0xtrace-support"
            }],
            "manifest_versions": name_current_manifest_versions_for_request(request),
            "execution_trace_id": null,
            "derivation_kind": "projection_apply"
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": [format!("{}_supported_resolution", request.trace.namespace)],
            "unsupported_reason": null,
            "enumeration_basis": "exact_name"
        }),
        chain_positions: projection_chain_positions_for_request(request),
        canonicality_summary: {
            let mut chains = Map::new();
            for position in projection_chain_positions_for_request(request)
                .as_object()
                .expect("chain positions must be object")
                .values()
            {
                if let Some(chain_id) = position.get("chain_id").and_then(Value::as_str) {
                    chains.insert(chain_id.to_owned(), Value::String("finalized".to_owned()));
                }
            }
            json!({
                "status": "finalized",
                "chains": Value::Object(chains)
            })
        },
        manifest_version: first_boundary_manifest_version(
            &request.outcome.cache_key.manifest_versions,
        ),
        last_recomputed_at: timestamp(1_717_171_717),
    }
}

fn record_inventory_selector_entry(query: &VerifiedQuerySummary, cacheable: bool) -> Value {
    let (record_family, selector_key) = selector_family_and_key(&query.record_key, &query.selector);
    json!({
        "record_key": query.record_key,
        "record_family": record_family,
        "selector_key": selector_key,
        "cacheable": cacheable
    })
}

fn record_inventory_entry(query: &VerifiedQuerySummary) -> Value {
    let (record_family, selector_key) = selector_family_and_key(&query.record_key, &query.selector);
    let mut entry = json!({
        "record_key": query.record_key,
        "record_family": record_family,
        "selector_key": selector_key,
        "status": match query.status {
            VerifiedQueryStatus::Success => "success",
            VerifiedQueryStatus::NotFound => "not_found",
            VerifiedQueryStatus::ExecutionFailed => "unsupported",
        }
    });
    match query.status {
        VerifiedQueryStatus::Success => {
            let value = match &query.selector {
                SupportedVerifiedRecordKey::Addr { coin_type } => {
                    json!({
                        "coin_type": coin_type,
                        "value": query.value.as_deref().expect("success query must include value")
                    })
                }
                SupportedVerifiedRecordKey::Avatar
                | SupportedVerifiedRecordKey::Contenthash
                | SupportedVerifiedRecordKey::Text => json!({
                    "value": query.value.as_deref().expect("success query must include value")
                }),
            };
            entry
                .as_object_mut()
                .expect("entry must be object")
                .insert("value".to_owned(), value);
        }
        VerifiedQueryStatus::NotFound => {}
        VerifiedQueryStatus::ExecutionFailed => {
            entry.as_object_mut().expect("entry must be object").insert(
                "unsupported_reason".to_owned(),
                json!("value_not_retained_in_normalized_events"),
            );
        }
    }
    entry
}

fn supported_record_inventory_row_for_request(
    request: &PersistEnsExactNameVerifiedResolutionRequest,
    resource_id: Uuid,
) -> RecordInventoryCurrentRow {
    let mut queries =
        extract_supported_verified_queries(&request.outcome).expect("queries must parse");
    queries.sort_by(|left, right| left.record_key.cmp(&right.record_key));
    let selectors = queries
        .iter()
        .map(|query| {
            record_inventory_selector_entry(
                query,
                query.status != VerifiedQueryStatus::ExecutionFailed,
            )
        })
        .collect::<Vec<_>>();
    let entries = queries
        .iter()
        .filter(|query| query.status != VerifiedQueryStatus::ExecutionFailed)
        .map(record_inventory_entry)
        .collect::<Vec<_>>();
    let chain_id = request.outcome.cache_key.record_version_boundary["chain_position"]["chain_id"]
        .as_str()
        .unwrap_or(ETHEREUM_MAINNET_CHAIN_ID)
        .to_owned();
    let mut chain_positions = Map::new();
    chain_positions.insert(
        chain_id.clone(),
        request.outcome.cache_key.record_version_boundary["chain_position"].clone(),
    );
    let mut canonicality_chains = Map::new();
    canonicality_chains.insert(chain_id, Value::String("finalized".to_owned()));

    RecordInventoryCurrentRow {
        resource_id,
        record_version_boundary: request.outcome.cache_key.record_version_boundary.clone(),
        enumeration_basis: json!({
            "observed_selectors": true,
            "capability_declared_families": true,
            "globally_enumerable": false
        }),
        selectors: Value::Array(selectors),
        explicit_gaps: json!([]),
        unsupported_families: json!([]),
        last_change: Some(json!({
            "normalized_event_id": 1200,
            "event_kind": "RecordsChanged",
            "chain_position": request.outcome.cache_key.record_version_boundary["chain_position"].clone()
        })),
        entries: Value::Array(entries),
        provenance: json!({
            "normalized_event_ids": [1200],
            "derivation_kind": "record_inventory_current_rebuild"
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "enumeration_basis": "declared_record_inventory"
        }),
        chain_positions: Value::Object(chain_positions),
        canonicality_summary: json!({
            "status": "finalized",
            "chains": Value::Object(canonicality_chains)
        }),
        manifest_version: first_boundary_manifest_version(
            &request.outcome.cache_key.manifest_versions,
        ),
        last_recomputed_at: timestamp(1_717_171_719),
    }
}

async fn seed_alias_only_name_current_rebuild_inputs(
    database: &TestDatabase,
    request: &PersistEnsExactNameVerifiedResolutionRequest,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
) -> Result<()> {
    let logical_name_id = logical_name_id_for_request(request);
    bigname_storage::upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xsurface", 98, 1_717_171_698),
            raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xresource", 99, 1_717_171_699),
            raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xresolver", 101, 1_717_171_701),
            raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xalias", 102, 1_717_171_702),
            raw_block(
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xbinding-alias",
                103,
                1_717_171_703,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        database.pool(),
        &[name_surface_for_request(request, ETHEREUM_MAINNET_CHAIN_ID)],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        database.pool(),
        &[TokenLineage {
            token_lineage_id,
            chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
            block_hash: "0xresource".to_owned(),
            block_number: 99,
            provenance: json!({"seed": "execution_alias_token_lineage"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::upsert_resources(
        database.pool(),
        &[Resource {
            resource_id,
            token_lineage_id: Some(token_lineage_id),
            chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
            block_hash: "0xresource".to_owned(),
            block_number: 99,
            provenance: json!({"seed": "execution_alias_resource"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        database.pool(),
        &[SurfaceBinding {
            surface_binding_id,
            logical_name_id: logical_name_id.clone(),
            resource_id,
            binding_kind: SurfaceBindingKind::ResolverAliasPath,
            active_from: timestamp(1_717_171_703),
            active_to: None,
            chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
            block_hash: "0xbinding-alias".to_owned(),
            block_number: 103,
            provenance: json!({"seed": "execution_alias_binding"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::upsert_normalized_events(
        database.pool(),
        &[
            NormalizedEvent {
                event_identity: "execution:alias-resolver".to_owned(),
                namespace: ENS_NAMESPACE.to_owned(),
                logical_name_id: Some(logical_name_id.clone()),
                resource_id: Some(resource_id),
                event_kind: "ResolverChanged".to_owned(),
                source_family: "ens_v1_unwrapped_authority".to_owned(),
                manifest_version: 4,
                source_manifest_id: None,
                chain_id: Some(ETHEREUM_MAINNET_CHAIN_ID.to_owned()),
                block_number: Some(101),
                block_hash: Some("0xresolver".to_owned()),
                transaction_hash: Some("0xtxresolver".to_owned()),
                log_index: Some(0),
                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "execution:alias-resolver"}),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({}),
                after_state: json!({
                    "resolver": "0x0000000000000000000000000000000000000abc",
                    "namehash": "namehash:alice.eth",
                }),
            },
            NormalizedEvent {
                event_identity: "execution:alias-changed".to_owned(),
                namespace: ENS_NAMESPACE.to_owned(),
                logical_name_id: Some(logical_name_id.clone()),
                resource_id: Some(resource_id),
                event_kind: "AliasChanged".to_owned(),
                source_family: "ens_v2_resolver".to_owned(),
                manifest_version: 5,
                source_manifest_id: None,
                chain_id: Some(ETHEREUM_MAINNET_CHAIN_ID.to_owned()),
                block_number: Some(102),
                block_hash: Some("0xalias".to_owned()),
                transaction_hash: Some("0xtxalias".to_owned()),
                log_index: Some(0),
                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "execution:alias-changed"}),
                derivation_kind: "ens_v2_resolver".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({}),
                after_state: json!({
                    "active": true,
                    "alias_state": "active",
                    "to_name": "profile.alice.eth",
                    "to_logical_name_id": "ens:profile.alice.eth",
                    "to_normalized_name": "profile.alice.eth",
                    "to_canonical_display_name": "profile.alice.eth",
                    "to_namehash": "namehash:profile.alice.eth",
                    "to_resource_id": resource_id.to_string(),
                }),
            },
        ],
    )
    .await?;
    rebuild_name_current_projection(database, &logical_name_id).await
}

async fn seed_wildcard_name_current_rebuild_inputs(
    database: &TestDatabase,
    request: &PersistEnsExactNameVerifiedResolutionRequest,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
) -> Result<()> {
    let logical_name_id = logical_name_id_for_request(request);
    let wildcard = persisted_trace_detail_object(&request.trace, "wildcard")
        .context("wildcard-derived execution request must include wildcard detail")?;
    let source = wildcard
        .get("source")
        .cloned()
        .context("wildcard-derived execution request must include wildcard source")?;
    let source_logical_name_id = source
        .get("logical_name_id")
        .and_then(Value::as_str)
        .unwrap_or("ens:eth");
    let source_namespace = source
        .get("namespace")
        .and_then(Value::as_str)
        .unwrap_or(ENS_NAMESPACE);
    let source_normalized_name = source
        .get("normalized_name")
        .and_then(Value::as_str)
        .unwrap_or("eth");
    let source_canonical_display_name = source
        .get("canonical_display_name")
        .and_then(Value::as_str)
        .unwrap_or(source_normalized_name);
    let source_namehash = source
        .get("namehash")
        .and_then(Value::as_str)
        .unwrap_or("namehash:eth");
    let source_resource_id = source
        .get("resource_id")
        .and_then(Value::as_str)
        .map(Uuid::parse_str)
        .transpose()
        .context("wildcard source resource_id must be a UUID")?
        .unwrap_or(resource_id);
    let source_token_lineage_id = Uuid::from_u128(source_resource_id.as_u128() + 1);
    let source_surface_binding_id = Uuid::from_u128(source_resource_id.as_u128() + 2);
    bigname_storage::upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xsource-surface",
                96,
                1_717_171_696,
            ),
            raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xsurface", 98, 1_717_171_698),
            raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xresource", 99, 1_717_171_699),
            raw_block(
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xsource-resource",
                100,
                1_717_171_700,
            ),
            raw_block(ETHEREUM_MAINNET_CHAIN_ID, "0xresolver", 101, 1_717_171_701),
            raw_block(
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xsource-record-version",
                102,
                1_717_171_702,
            ),
            raw_block(
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xbinding-wildcard",
                103,
                1_717_171_703,
            ),
        ],
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        database.pool(),
        &[
            name_surface_for_request(request, ETHEREUM_MAINNET_CHAIN_ID),
            NameSurface {
                logical_name_id: source_logical_name_id.to_owned(),
                namespace: source_namespace.to_owned(),
                input_name: source_normalized_name.to_owned(),
                canonical_display_name: source_canonical_display_name.to_owned(),
                normalized_name: source_normalized_name.to_owned(),
                dns_encoded_name: source_normalized_name.as_bytes().to_vec(),
                namehash: source_namehash.to_owned(),
                labelhashes: vec![format!("labelhash:{source_normalized_name}")],
                normalizer_version: "uts46-v1".to_owned(),
                normalization_warnings: json!([]),
                normalization_errors: json!([]),
                chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
                block_hash: "0xsource-surface".to_owned(),
                block_number: 96,
                provenance: json!({"seed": "execution_wildcard_source_surface"}),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        database.pool(),
        &[
            TokenLineage {
                token_lineage_id,
                chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
                block_hash: "0xresource".to_owned(),
                block_number: 99,
                provenance: json!({"seed": "execution_wildcard_token_lineage"}),
                canonicality_state: CanonicalityState::Canonical,
            },
            TokenLineage {
                token_lineage_id: source_token_lineage_id,
                chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
                block_hash: "0xsource-resource".to_owned(),
                block_number: 100,
                provenance: json!({"seed": "execution_wildcard_source_token_lineage"}),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;
    bigname_storage::upsert_resources(
        database.pool(),
        &[
            Resource {
                resource_id,
                token_lineage_id: Some(token_lineage_id),
                chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
                block_hash: "0xresource".to_owned(),
                block_number: 99,
                provenance: json!({"seed": "execution_wildcard_resource"}),
                canonicality_state: CanonicalityState::Canonical,
            },
            Resource {
                resource_id: source_resource_id,
                token_lineage_id: Some(source_token_lineage_id),
                chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
                block_hash: "0xsource-resource".to_owned(),
                block_number: 100,
                provenance: json!({"seed": "execution_wildcard_source_resource"}),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        database.pool(),
        &[
            SurfaceBinding {
                surface_binding_id: source_surface_binding_id,
                logical_name_id: source_logical_name_id.to_owned(),
                resource_id: source_resource_id,
                binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                active_from: timestamp(1_717_171_700),
                active_to: None,
                chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
                block_hash: "0xsource-resource".to_owned(),
                block_number: 100,
                provenance: json!({"seed": "execution_wildcard_source_binding"}),
                canonicality_state: CanonicalityState::Canonical,
            },
            SurfaceBinding {
                surface_binding_id,
                logical_name_id: logical_name_id.clone(),
                resource_id,
                binding_kind: SurfaceBindingKind::ObservedWildcardPath,
                active_from: timestamp(1_717_171_703),
                active_to: None,
                chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
                block_hash: "0xbinding-wildcard".to_owned(),
                block_number: 103,
                provenance: json!({"seed": "execution_wildcard_binding"}),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;
    bigname_storage::upsert_normalized_events(
        database.pool(),
        &[
            NormalizedEvent {
                event_identity: "execution:wildcard-source-resolver".to_owned(),
                namespace: ENS_NAMESPACE.to_owned(),
                logical_name_id: Some(source_logical_name_id.to_owned()),
                resource_id: Some(source_resource_id),
                event_kind: "ResolverChanged".to_owned(),
                source_family: "ens_v1_unwrapped_authority".to_owned(),
                manifest_version: 4,
                source_manifest_id: None,
                chain_id: Some(ETHEREUM_MAINNET_CHAIN_ID.to_owned()),
                block_number: Some(101),
                block_hash: Some("0xresolver".to_owned()),
                transaction_hash: Some("0xtxwildcardsourceresolver".to_owned()),
                log_index: Some(0),
                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "execution:wildcard-source-resolver"}),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({}),
                after_state: json!({
                    "resolver": "0x0000000000000000000000000000000000000def",
                    "namehash": source_namehash,
                }),
            },
            NormalizedEvent {
                event_identity: "execution:wildcard-source-record-version".to_owned(),
                namespace: ENS_NAMESPACE.to_owned(),
                logical_name_id: Some(source_logical_name_id.to_owned()),
                resource_id: Some(source_resource_id),
                event_kind: "RecordVersionChanged".to_owned(),
                source_family: "ens_v1_unwrapped_authority".to_owned(),
                manifest_version: 4,
                source_manifest_id: None,
                chain_id: Some(ETHEREUM_MAINNET_CHAIN_ID.to_owned()),
                block_number: Some(102),
                block_hash: Some("0xsource-record-version".to_owned()),
                transaction_hash: Some("0xtxwildcardsourceversion".to_owned()),
                log_index: Some(0),
                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "execution:wildcard-source-record-version"}),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({"record_version": 6}),
                after_state: json!({"record_version": 7}),
            },
        ],
    )
    .await?;
    rebuild_name_current_projection(database, &logical_name_id).await
}

async fn insert_basenames_execution_manifest(pool: &PgPool) -> Result<i64> {
    let manifest_id = sqlx::query(
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
    .bind(2_i64)
    .bind(BASENAMES_NAMESPACE)
    .bind("basenames_execution")
    .bind(ETHEREUM_MAINNET_CHAIN_ID)
    .bind("basenames_v1")
    .bind("active")
    .bind("ensip15@2026-04-16")
    .bind("tests/execution/basenames_execution-v2.toml")
    .bind("{}")
    .fetch_one(pool)
    .await?
    .try_get("manifest_id")?;
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
    .bind("verified_resolution")
    .bind("supported")
    .bind(None::<String>)
    .execute(pool)
    .await?;
    insert_basenames_execution_manifest_contract(pool, manifest_id).await?;
    Ok(manifest_id)
}

async fn insert_basenames_execution_manifest_contract(
    pool: &PgPool,
    manifest_id: i64,
) -> Result<()> {
    let contract_instance_id = Uuid::from_u128(0x0b45_0000_0000_0000_0000_0000_0000_0002);
    sqlx::query(
        r#"
            INSERT INTO contract_instances (
                contract_instance_id,
                chain_id,
                contract_kind,
                provenance
            )
            VALUES ($1, $2, 'contract', $3::jsonb)
            ON CONFLICT (contract_instance_id) DO NOTHING
        "#,
    )
    .bind(contract_instance_id)
    .bind(ETHEREUM_MAINNET_CHAIN_ID)
    .bind(json!({"seed": "execution_basenames_execution"}))
    .execute(pool)
    .await
    .context("failed to insert Basenames execution contract_instance")?;

    sqlx::query(
        r#"
            INSERT INTO manifest_contract_instances (
                manifest_id,
                declaration_kind,
                declaration_name,
                contract_instance_id,
                declared_address,
                role,
                proxy_kind
            )
            VALUES (
                $1,
                'contract',
                'l1_resolver',
                $2,
                '0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31',
                'l1_resolver',
                'none'
            )
        "#,
    )
    .bind(manifest_id)
    .bind(contract_instance_id)
    .execute(pool)
    .await
    .context("failed to insert Basenames execution manifest_contract_instance")?;

    Ok(())
}

async fn insert_chain_checkpoint(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
) -> Result<()> {
    sqlx::query(
        r#"
            INSERT INTO chain_checkpoints (
                chain_id,
                finalized_block_hash,
                finalized_block_number
            )
            VALUES ($1, $2, $3)
            ON CONFLICT (chain_id)
            DO UPDATE SET
                finalized_block_hash = EXCLUDED.finalized_block_hash,
                finalized_block_number = EXCLUDED.finalized_block_number
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(block_number)
    .execute(pool)
    .await
    .with_context(|| format!("failed to insert chain checkpoint for {chain_id}"))?;

    Ok(())
}

async fn seed_basenames_name_current_rebuild_inputs(
    database: &TestDatabase,
    request: &PersistEnsExactNameVerifiedResolutionRequest,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
) -> Result<()> {
    let logical_name_id = logical_name_id_for_request(request);
    bigname_storage::upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(BASE_MAINNET_CHAIN_ID, "0xbase-surface", 98, 1_717_171_698),
            raw_block(BASE_MAINNET_CHAIN_ID, "0xbase-resource", 99, 1_717_171_699),
            raw_block(BASE_MAINNET_CHAIN_ID, "0xbase-grant", 101, 1_717_171_701),
            raw_block(
                BASE_MAINNET_CHAIN_ID,
                "0xbase-authority",
                102,
                1_717_171_702,
            ),
            raw_block(BASE_MAINNET_CHAIN_ID, "0xbase-resolver", 103, 1_717_171_703),
            raw_block(
                BASE_MAINNET_CHAIN_ID,
                "0xbase-binding-supported",
                104,
                1_717_171_704,
            ),
            raw_block(
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xbasenamesl1-compatible",
                21_000_099,
                1_717_171_704,
            ),
            raw_block(
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xbasenamesl1",
                21_000_100,
                1_776_387_700,
            ),
        ],
    )
    .await?;
    insert_chain_checkpoint(
        database.pool(),
        ETHEREUM_MAINNET_CHAIN_ID,
        "0xbasenamesl1",
        21_000_100,
    )
    .await?;
    bigname_storage::upsert_name_surfaces(
        database.pool(),
        &[NameSurface {
            logical_name_id: logical_name_id.clone(),
            namespace: BASENAMES_NAMESPACE.to_owned(),
            input_name: "alice.base.eth".to_owned(),
            canonical_display_name: "Alice.base.eth".to_owned(),
            normalized_name: "alice.base.eth".to_owned(),
            dns_encoded_name: b"alice.base.eth".to_vec(),
            namehash: "namehash:alice.base.eth".to_owned(),
            labelhashes: vec!["labelhash:alice.base.eth".to_owned()],
            normalizer_version: "ensip15@2026-04-16".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: BASE_MAINNET_CHAIN_ID.to_owned(),
            block_hash: "0xbase-surface".to_owned(),
            block_number: 98,
            provenance: json!({"seed": "execution_basenames_surface"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::upsert_token_lineages(
        database.pool(),
        &[TokenLineage {
            token_lineage_id,
            chain_id: BASE_MAINNET_CHAIN_ID.to_owned(),
            block_hash: "0xbase-resource".to_owned(),
            block_number: 99,
            provenance: json!({"seed": "execution_basenames_token_lineage"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::upsert_resources(
        database.pool(),
        &[Resource {
            resource_id,
            token_lineage_id: Some(token_lineage_id),
            chain_id: BASE_MAINNET_CHAIN_ID.to_owned(),
            block_hash: "0xbase-resource".to_owned(),
            block_number: 99,
            provenance: json!({"seed": "execution_basenames_resource"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::upsert_surface_bindings(
        database.pool(),
        &[SurfaceBinding {
            surface_binding_id,
            logical_name_id: logical_name_id.clone(),
            resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from: timestamp(1_717_171_704),
            active_to: None,
            chain_id: BASE_MAINNET_CHAIN_ID.to_owned(),
            block_hash: "0xbase-binding-supported".to_owned(),
            block_number: 104,
            provenance: json!({"seed": "execution_basenames_binding"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::upsert_normalized_events(
        database.pool(),
        &[
            NormalizedEvent {
                event_identity: "execution:basenames:grant".to_owned(),
                namespace: BASENAMES_NAMESPACE.to_owned(),
                logical_name_id: Some(logical_name_id.clone()),
                resource_id: Some(resource_id),
                event_kind: "RegistrationGranted".to_owned(),
                source_family: "basenames_base_registrar".to_owned(),
                manifest_version: 3,
                source_manifest_id: None,
                chain_id: Some(BASE_MAINNET_CHAIN_ID.to_owned()),
                block_number: Some(101),
                block_hash: Some("0xbase-grant".to_owned()),
                transaction_hash: Some("0xtxbasegrant".to_owned()),
                log_index: Some(0),
                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "execution:basenames:grant"}),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({}),
                after_state: json!({
                    "authority_kind": "registrar",
                    "authority_key": "registrar:base-mainnet:alice",
                    "registrant": "0x00000000000000000000000000000000000000aa",
                    "expiry": 1_900_000_000_i64,
                }),
            },
            NormalizedEvent {
                event_identity: "execution:basenames:authority".to_owned(),
                namespace: BASENAMES_NAMESPACE.to_owned(),
                logical_name_id: Some(logical_name_id.clone()),
                resource_id: Some(resource_id),
                event_kind: "AuthorityTransferred".to_owned(),
                source_family: "basenames_base_registry".to_owned(),
                manifest_version: 3,
                source_manifest_id: None,
                chain_id: Some(BASE_MAINNET_CHAIN_ID.to_owned()),
                block_number: Some(102),
                block_hash: Some("0xbase-authority".to_owned()),
                transaction_hash: Some("0xtxbaseauthority".to_owned()),
                log_index: Some(0),
                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "execution:basenames:authority"}),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({}),
                after_state: json!({
                    "owner": "0x00000000000000000000000000000000000000bb",
                }),
            },
            NormalizedEvent {
                event_identity: "execution:basenames:resolver".to_owned(),
                namespace: BASENAMES_NAMESPACE.to_owned(),
                logical_name_id: Some(logical_name_id.clone()),
                resource_id: Some(resource_id),
                event_kind: "ResolverChanged".to_owned(),
                source_family: "basenames_base_resolver".to_owned(),
                manifest_version: 4,
                source_manifest_id: None,
                chain_id: Some(BASE_MAINNET_CHAIN_ID.to_owned()),
                block_number: Some(103),
                block_hash: Some("0xbase-resolver".to_owned()),
                transaction_hash: Some("0xtxbaseresolver".to_owned()),
                log_index: Some(0),
                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "execution:basenames:resolver"}),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({}),
                after_state: json!({
                    "resolver": "0x0000000000000000000000000000000000000abc",
                    "namehash": "namehash:alice.base.eth",
                }),
            },
            NormalizedEvent {
                event_identity: "execution:basenames:record-version".to_owned(),
                namespace: BASENAMES_NAMESPACE.to_owned(),
                logical_name_id: Some(logical_name_id.clone()),
                resource_id: Some(resource_id),
                event_kind: "RecordVersionChanged".to_owned(),
                source_family: "basenames_base_resolver".to_owned(),
                manifest_version: 4,
                source_manifest_id: None,
                chain_id: Some(BASE_MAINNET_CHAIN_ID.to_owned()),
                block_number: Some(104),
                block_hash: Some("0xbase-binding-supported".to_owned()),
                transaction_hash: Some("0xtxbaserecordversion".to_owned()),
                log_index: Some(0),
                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "execution:basenames:record-version"}),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({
                    "record_version": 6,
                }),
                after_state: json!({
                    "record_version": 7,
                }),
            },
            NormalizedEvent {
                event_identity: "execution:basenames:addr".to_owned(),
                namespace: BASENAMES_NAMESPACE.to_owned(),
                logical_name_id: Some(logical_name_id.clone()),
                resource_id: Some(resource_id),
                event_kind: "RecordChanged".to_owned(),
                source_family: "basenames_base_resolver".to_owned(),
                manifest_version: 4,
                source_manifest_id: None,
                chain_id: Some(BASE_MAINNET_CHAIN_ID.to_owned()),
                block_number: Some(104),
                block_hash: Some("0xbase-binding-supported".to_owned()),
                transaction_hash: Some("0xtxbaseaddr".to_owned()),
                log_index: Some(1),
                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "execution:basenames:addr"}),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({}),
                after_state: json!({
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                }),
            },
            NormalizedEvent {
                event_identity: "execution:basenames:text".to_owned(),
                namespace: BASENAMES_NAMESPACE.to_owned(),
                logical_name_id: Some(logical_name_id.clone()),
                resource_id: Some(resource_id),
                event_kind: "RecordChanged".to_owned(),
                source_family: "basenames_base_resolver".to_owned(),
                manifest_version: 4,
                source_manifest_id: None,
                chain_id: Some(BASE_MAINNET_CHAIN_ID.to_owned()),
                block_number: Some(104),
                block_hash: Some("0xbase-binding-supported".to_owned()),
                transaction_hash: Some("0xtxbasetext".to_owned()),
                log_index: Some(2),
                raw_fact_ref: json!({"kind": "raw_log", "event_identity": "execution:basenames:text"}),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({}),
                after_state: json!({
                    "record_key": "text",
                    "record_family": "text",
                    "selector_key": null,
                }),
            },
        ],
    )
    .await?;
    insert_basenames_execution_manifest(database.pool()).await?;
    rebuild_name_current_projection(database, &logical_name_id).await
}

async fn seed_supported_resolution_storage_from_rebuild(
    database: &TestDatabase,
    request: &mut PersistEnsExactNameVerifiedResolutionRequest,
) -> Result<()> {
    let resource_id = boundary_resource_id(&request.outcome.cache_key.record_version_boundary);
    let token_lineage_id = Uuid::from_u128(resource_id.as_u128() + 1);
    let surface_binding_id = Uuid::from_u128(resource_id.as_u128() + 2);
    match (
        request.trace.namespace.as_str(),
        binding_kind_for_request(request),
    ) {
        (ENS_NAMESPACE, SurfaceBindingKind::ResolverAliasPath) => {
            seed_alias_only_name_current_rebuild_inputs(
                database,
                request,
                resource_id,
                token_lineage_id,
                surface_binding_id,
            )
            .await?;
        }
        (ENS_NAMESPACE, SurfaceBindingKind::ObservedWildcardPath) => {
            seed_wildcard_name_current_rebuild_inputs(
                database,
                request,
                resource_id,
                token_lineage_id,
                surface_binding_id,
            )
            .await?;
        }
        (BASENAMES_NAMESPACE, SurfaceBindingKind::DeclaredRegistryPath) => {
            seed_basenames_name_current_rebuild_inputs(
                database,
                request,
                resource_id,
                token_lineage_id,
                surface_binding_id,
            )
            .await?;
        }
        (namespace, binding_kind) => {
            bail!(
                "no rebuild-backed supported-resolution seed for namespace {namespace} binding_kind {}",
                binding_kind.as_str()
            );
        }
    }

    let logical_name_id = logical_name_id_for_request(request);
    let row = bigname_storage::load_name_current(database.pool(), &logical_name_id)
        .await?
        .with_context(|| {
            format!("rebuilt execution test name_current row missing for {logical_name_id}")
        })?;
    sync_request_with_rebuilt_name_current(request, &row)?;
    let record_resource_id =
        boundary_resource_id(&request.outcome.cache_key.record_version_boundary);
    upsert_record_inventory_current_rows(
        database.pool(),
        &[supported_record_inventory_row_for_request(
            request,
            record_resource_id,
        )],
    )
    .await?;
    Ok(())
}

async fn seed_supported_resolution_storage(
    database: &TestDatabase,
    request: &PersistEnsExactNameVerifiedResolutionRequest,
) -> Result<()> {
    let resource_id = boundary_resource_id(&request.outcome.cache_key.record_version_boundary);
    let token_lineage_id = Uuid::from_u128(resource_id.as_u128() + 1);
    let surface_binding_id = Uuid::from_u128(resource_id.as_u128() + 2);
    let primary_chain_id = request
        .outcome
        .cache_key
        .record_version_boundary
        .get("chain_position")
        .and_then(|chain_position| chain_position.get("chain_id"))
        .and_then(Value::as_str)
        .unwrap_or(ETHEREUM_MAINNET_CHAIN_ID);
    let binding_kind = binding_kind_for_request(request);

    upsert_token_lineages(
        database.pool(),
        &[token_lineage_for_request(
            token_lineage_id,
            primary_chain_id,
        )],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[resource_for_request(
            resource_id,
            token_lineage_id,
            primary_chain_id,
        )],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[name_surface_for_request(request, primary_chain_id)],
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &[surface_binding_for_request(
            request,
            surface_binding_id,
            resource_id,
            binding_kind,
            primary_chain_id,
        )],
    )
    .await?;
    upsert_name_current_rows(
        database.pool(),
        &[supported_resolution_name_current_row(
            request,
            resource_id,
            token_lineage_id,
            surface_binding_id,
        )],
    )
    .await?;
    upsert_record_inventory_current_rows(
        database.pool(),
        &[supported_record_inventory_row_for_request(
            request,
            resource_id,
        )],
    )
    .await?;
    Ok(())
}

fn basenames_transport_direct_request() -> PersistEnsExactNameVerifiedResolutionRequest {
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000031);
    let finished_at = timestamp(1_717_171_930);
    let request_key = "basenames:alice.base.eth:addr:60,text:com.twitter".to_owned();
    let verified_queries = json!([
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            }
        },
        {
            "record_key": "text:com.twitter",
            "status": "not_found",
            "failure_reason": "no_text_record"
        }
    ]);

    PersistEnsExactNameVerifiedResolutionRequest {
        raw_call_snapshots: Vec::new(),
        trace: ExecutionTrace {
            execution_trace_id,
            request_type: VERIFIED_RESOLUTION_REQUEST_TYPE.to_owned(),
            request_key: request_key.clone(),
            namespace: BASENAMES_NAMESPACE.to_owned(),
            chain_context: json!({
                "requested_positions": basenames_requested_chain_positions(),
                "topology_version_boundary": {
                    BASE_MAINNET_CHAIN_ID: 31_000_000
                }
            }),
            manifest_context: json!({
                "manifest_versions": basenames_manifest_versions(),
                "rollout_boundary": "supported"
            }),
            contracts_called: json!([
                {
                    "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
                    "contract_address": BASENAMES_L1_RESOLVER_ADDRESS,
                    "selector": "0x9061b923"
                }
            ]),
            gateway_digests: json!(["sha256:ccip-request", "sha256:ccip-response"]),
            final_payload: Some(json!({
                "verified_queries": verified_queries.clone()
            })),
            failure_payload: None,
            request_metadata: json!({
                "surface": "alice.base.eth",
                "record_keys": ["addr:60", "text:com.twitter"],
                "entrypoint": BASENAMES_L1_RESOLVER_ROLE,
                "contract_address": BASENAMES_L1_RESOLVER_ADDRESS,
                "transport": {
                    "source_chain_id": BASE_MAINNET_CHAIN_ID,
                    "target_chain_id": ETHEREUM_MAINNET_CHAIN_ID,
                    "contract_address": BASENAMES_L1_RESOLVER_ADDRESS,
                    "latest_event_kind": null
                }
            }),
            finished_at: Some(finished_at),
            steps: vec![
                ExecutionTraceStep {
                    step_index: 0,
                    step_kind: "load_declared_topology".to_owned(),
                    input_digest: Some("sha256:topology-input".to_owned()),
                    output_digest: Some("sha256:topology-output".to_owned()),
                    latency_ms: Some(4),
                    canonicality_dependency: json!({
                        BASE_MAINNET_CHAIN_ID: {
                            "block_hash": "0xbase123",
                            "block_number": 31_000_000,
                            "state": "canonical"
                        }
                    }),
                    step_payload: json!({
                        "entrypoint": BASENAMES_L1_RESOLVER_ROLE,
                        "resolver": "0x0000000000000000000000000000000000000abc"
                    }),
                },
                ExecutionTraceStep {
                    step_index: 1,
                    step_kind: "call_l1_resolver".to_owned(),
                    input_digest: Some("sha256:l1-resolver-input".to_owned()),
                    output_digest: Some("sha256:l1-resolver-output".to_owned()),
                    latency_ms: Some(18),
                    canonicality_dependency: json!({
                        ETHEREUM_MAINNET_CHAIN_ID: {
                            "block_hash": "0xl1abc",
                            "block_number": 21_000_000,
                            "state": "canonical"
                        }
                    }),
                    step_payload: json!({
                        "name": "alice.base.eth",
                        "record_count": 2
                    }),
                },
                ExecutionTraceStep {
                    step_index: 2,
                    step_kind: "ccip_offchain_lookup".to_owned(),
                    input_digest: Some("sha256:ccip-input".to_owned()),
                    output_digest: Some("sha256:ccip-output".to_owned()),
                    latency_ms: Some(32),
                    canonicality_dependency: json!({
                        ETHEREUM_MAINNET_CHAIN_ID: {
                            "block_hash": "0xl1abc",
                            "block_number": 21_000_000,
                            "state": "canonical"
                        }
                    }),
                    step_payload: json!({
                        "gateway_digest": "sha256:ccip-request"
                    }),
                },
                ExecutionTraceStep {
                    step_index: 3,
                    step_kind: "resolve_with_proof".to_owned(),
                    input_digest: Some("sha256:proof-input".to_owned()),
                    output_digest: Some("sha256:proof-output".to_owned()),
                    latency_ms: Some(11),
                    canonicality_dependency: json!({
                        ETHEREUM_MAINNET_CHAIN_ID: {
                            "block_hash": "0xl1abc",
                            "block_number": 21_000_000,
                            "state": "canonical"
                        }
                    }),
                    step_payload: json!({
                        "proof_kind": "signature"
                    }),
                },
            ],
        },
        outcome: ExecutionOutcome {
            cache_key: ExecutionCacheKey {
                request_key,
                requested_chain_positions: basenames_requested_chain_positions(),
                manifest_versions: basenames_manifest_versions(),
                topology_version_boundary: basenames_version_boundary(Uuid::from_u128(
                    0x0e7ec7ace0000000000000000000bab1,
                )),
                record_version_boundary: basenames_version_boundary(Uuid::from_u128(
                    0x0e7ec7ace0000000000000000000bab2,
                )),
            },
            execution_trace_id,
            request_type: VERIFIED_RESOLUTION_REQUEST_TYPE.to_owned(),
            namespace: BASENAMES_NAMESPACE.to_owned(),
            outcome_payload: Some(json!({
                "verified_queries": verified_queries
            })),
            failure_payload: None,
            finished_at,
        },
    }
}

fn success_request() -> PersistEnsExactNameVerifiedResolutionRequest {
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000011);
    let finished_at = timestamp(1_717_171_717);
    let request_key = "ens:alice.eth:addr:60".to_owned();
    PersistEnsExactNameVerifiedResolutionRequest {
        raw_call_snapshots: vec![raw_call_snapshot()],
        trace: ExecutionTrace {
            execution_trace_id,
            request_type: VERIFIED_RESOLUTION_REQUEST_TYPE.to_owned(),
            request_key: request_key.clone(),
            namespace: ENS_NAMESPACE.to_owned(),
            chain_context: json!({
                "requested_positions": requested_chain_positions(),
                "topology_version_boundary": {
                    ETHEREUM_MAINNET_CHAIN_ID: 21_000_000
                }
            }),
            manifest_context: json!({
                "manifest_versions": manifest_versions(),
                "rollout_boundary": "shadow"
            }),
            contracts_called: json!([
                {
                    "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
                    "contract_address": ENS_UNIVERSAL_RESOLVER_ADDRESS,
                    "selector": "0x9061b923"
                }
            ]),
            gateway_digests: json!([]),
            final_payload: Some(json!({
                "record_kind": "addr",
                "coin_type": 60,
                "value": "0x00000000000000000000000000000000000000aa"
            })),
            failure_payload: None,
            request_metadata: json!({
                "surface": "alice.eth",
                "record_key": "addr:60",
                "normalizer_version": "uts46-v1"
            }),
            finished_at: Some(finished_at),
            steps: vec![
                ExecutionTraceStep {
                    step_index: 0,
                    step_kind: "load_declared_topology".to_owned(),
                    input_digest: Some("sha256:topology-input".to_owned()),
                    output_digest: Some("sha256:topology-output".to_owned()),
                    latency_ms: Some(4),
                    canonicality_dependency: json!({
                        ETHEREUM_MAINNET_CHAIN_ID: {
                            "block_hash": "0xabc123",
                            "block_number": 21_000_000,
                            "state": "canonical"
                        }
                    }),
                    step_payload: json!({
                        "entrypoint": ENS_UNIVERSAL_RESOLVER_ROLE,
                        "resolver": ENS_UNIVERSAL_RESOLVER_ADDRESS
                    }),
                },
                ExecutionTraceStep {
                    step_index: 1,
                    step_kind: "call_universal_resolver".to_owned(),
                    input_digest: Some("sha256:resolver-input".to_owned()),
                    output_digest: Some("sha256:resolver-output".to_owned()),
                    latency_ms: Some(28),
                    canonicality_dependency: json!({
                        ETHEREUM_MAINNET_CHAIN_ID: {
                            "block_hash": "0xabc123",
                            "block_number": 21_000_000,
                            "state": "canonical"
                        }
                    }),
                    step_payload: json!({
                        "coin_type": 60,
                        "name": "alice.eth",
                        "resolved_address": "0x00000000000000000000000000000000000000aa"
                    }),
                },
            ],
        },
        outcome: ExecutionOutcome {
            cache_key: ExecutionCacheKey {
                request_key,
                requested_chain_positions: requested_chain_positions(),
                manifest_versions: manifest_versions(),
                topology_version_boundary: version_boundary(Uuid::from_u128(
                    0x0e7ec7ace0000000000000000000aaa1,
                )),
                record_version_boundary: version_boundary(Uuid::from_u128(
                    0x0e7ec7ace0000000000000000000aaa2,
                )),
            },
            execution_trace_id,
            request_type: VERIFIED_RESOLUTION_REQUEST_TYPE.to_owned(),
            namespace: ENS_NAMESPACE.to_owned(),
            outcome_payload: Some(json!({
                "verified_queries": [
                    {
                        "record_key": "addr:60",
                        "status": "success",
                        "value": {
                            "coin_type": "60",
                            "value": "0x00000000000000000000000000000000000000aa"
                        }
                    }
                ]
            })),
            failure_payload: None,
            finished_at,
        },
    }
}

fn execution_failed_request() -> PersistEnsExactNameVerifiedResolutionRequest {
    let mut request = success_request();
    request.raw_call_snapshots.clear();
    request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000012);
    request.trace.request_key = "ens:alice.eth:addr:60".to_owned();
    request.trace.final_payload = None;
    request.trace.failure_payload = Some(json!({
        "failure_reason": "resolver_call_reverted",
        "stage": "call_universal_resolver"
    }));
    request.trace.finished_at = Some(timestamp(1_717_171_800));
    request.outcome.execution_trace_id = request.trace.execution_trace_id;
    request.outcome.outcome_payload = Some(json!({
        "verified_queries": [
            {
                "record_key": "addr:60",
                "status": "execution_failed",
                "failure_reason": "resolver_call_reverted"
            }
        ]
    }));
    request.outcome.failure_payload = Some(json!({
        "failure_reason": "resolver_call_reverted",
        "reverted": true
    }));
    request.outcome.finished_at = request
        .trace
        .finished_at
        .expect("execution failed test trace must finish");
    request
}

fn contenthash_success_request() -> PersistEnsExactNameVerifiedResolutionRequest {
    let mut request = success_request();
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000018);
    let request_key = "ens:alice.eth:contenthash".to_owned();
    request.trace.execution_trace_id = execution_trace_id;
    request.trace.request_key = request_key.clone();
    request.trace.final_payload = Some(json!({
        "record_kind": "contenthash",
        "value": "ipfs://bafybeigdyrzt5sfp7udm7hu76fx4f2jv4jvgxk5csodx4d6vshv3zysn7u"
    }));
    request.trace.request_metadata = json!({
        "surface": "alice.eth",
        "record_key": "contenthash",
        "normalizer_version": "uts46-v1"
    });
    request.trace.steps[1].step_payload = json!({
        "name": "alice.eth",
        "contenthash": "ipfs://bafybeigdyrzt5sfp7udm7hu76fx4f2jv4jvgxk5csodx4d6vshv3zysn7u"
    });
    request.outcome.cache_key.request_key = request_key;
    request.outcome.execution_trace_id = execution_trace_id;
    request.outcome.outcome_payload = Some(json!({
        "verified_queries": [
            {
                "record_key": "contenthash",
                "status": "success",
                "value": {
                    "value": "ipfs://bafybeigdyrzt5sfp7udm7hu76fx4f2jv4jvgxk5csodx4d6vshv3zysn7u"
                }
            }
        ]
    }));
    request
}

fn avatar_success_request() -> PersistEnsExactNameVerifiedResolutionRequest {
    let mut request = success_request();
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000027);
    let request_key = "ens:alice.eth:avatar".to_owned();
    let avatar = "https://cdn.example.test/alice.png";
    request.trace.execution_trace_id = execution_trace_id;
    request.trace.request_key = request_key.clone();
    request.trace.final_payload = Some(json!({
        "record_kind": "avatar",
        "value": avatar
    }));
    request.trace.request_metadata = json!({
        "surface": "alice.eth",
        "record_key": "avatar",
        "normalizer_version": "uts46-v1"
    });
    request.trace.steps[1].step_payload = json!({
        "name": "alice.eth",
        "avatar": avatar
    });
    request.outcome.cache_key.request_key = request_key;
    request.outcome.execution_trace_id = execution_trace_id;
    request.outcome.outcome_payload = Some(json!({
        "verified_queries": [
            {
                "record_key": "avatar",
                "status": "success",
                "value": {
                    "value": avatar
                }
            }
        ]
    }));
    request
}

fn contenthash_not_found_request() -> PersistEnsExactNameVerifiedResolutionRequest {
    let mut request = contenthash_success_request();
    request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000019);
    request.trace.final_payload = Some(json!({
        "failure_reason": "no_contenthash_record"
    }));
    request.trace.finished_at = Some(timestamp(1_717_171_760));
    request.outcome.execution_trace_id = request.trace.execution_trace_id;
    request.outcome.outcome_payload = Some(json!({
        "verified_queries": [
            {
                "record_key": "contenthash",
                "status": "not_found",
                "failure_reason": "no_contenthash_record"
            }
        ]
    }));
    request.outcome.finished_at = request
        .trace
        .finished_at
        .expect("contenthash not_found test trace must finish");
    request
}

fn contenthash_execution_failed_request() -> PersistEnsExactNameVerifiedResolutionRequest {
    let mut request = contenthash_success_request();
    request.raw_call_snapshots.clear();
    request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000001a);
    request.trace.final_payload = None;
    request.trace.failure_payload = Some(json!({
        "failure_reason": "resolver_call_reverted",
        "stage": "call_universal_resolver"
    }));
    request.trace.finished_at = Some(timestamp(1_717_171_810));
    request.outcome.execution_trace_id = request.trace.execution_trace_id;
    request.outcome.outcome_payload = Some(json!({
        "verified_queries": [
            {
                "record_key": "contenthash",
                "status": "execution_failed",
                "failure_reason": "resolver_call_reverted"
            }
        ]
    }));
    request.outcome.failure_payload = Some(json!({
        "failure_reason": "resolver_call_reverted",
        "reverted": true
    }));
    request.outcome.finished_at = request
        .trace
        .finished_at
        .expect("contenthash execution_failed test trace must finish");
    request
}

fn multi_selector_request() -> PersistEnsExactNameVerifiedResolutionRequest {
    let mut request = success_request();
    let ordered_record_keys = vec![
        "addr:60".to_owned(),
        "addr:0".to_owned(),
        "addr:2".to_owned(),
    ];
    let request_key = normalized_request_key(ENS_NAMESPACE, "alice.eth", &ordered_record_keys);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000014);
    let finished_at = timestamp(1_717_171_900);
    let verified_queries = json!([
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            }
        },
        {
            "record_key": "addr:0",
            "status": "not_found",
            "failure_reason": "no_addr_record"
        },
        {
            "record_key": "addr:2",
            "status": "execution_failed",
            "failure_reason": "resolver_call_reverted"
        }
    ]);

    request.trace.execution_trace_id = execution_trace_id;
    request.trace.request_key = request_key.clone();
    request.trace.final_payload = Some(json!({
        "verified_queries": verified_queries.clone()
    }));
    request.trace.failure_payload = None;
    request.trace.request_metadata = json!({
        "surface": "alice.eth",
        "record_keys": ordered_record_keys,
        "normalizer_version": "uts46-v1"
    });
    request.trace.finished_at = Some(finished_at);
    request.outcome.cache_key.request_key = request_key;
    request.outcome.execution_trace_id = execution_trace_id;
    request.outcome.outcome_payload = Some(json!({
        "verified_queries": verified_queries
    }));
    request.outcome.failure_payload = None;
    request.outcome.finished_at = finished_at;
    request
}

fn contenthash_mixed_selector_request() -> PersistEnsExactNameVerifiedResolutionRequest {
    let mut request = contenthash_success_request();
    let ordered_record_keys = vec![
        "text:com.twitter".to_owned(),
        "contenthash".to_owned(),
        "addr:60".to_owned(),
    ];
    let request_key = normalized_request_key(ENS_NAMESPACE, "alice.eth", &ordered_record_keys);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000001b);
    let finished_at = timestamp(1_717_171_920);
    let verified_queries = json!([
        {
            "record_key": "text:com.twitter",
            "status": "not_found",
            "failure_reason": "no_text_record"
        },
        {
            "record_key": "contenthash",
            "status": "success",
            "value": {
                "value": "ipfs://bafybeigdyrzt5sfp7udm7hu76fx4f2jv4jvgxk5csodx4d6vshv3zysn7u"
            }
        },
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            }
        }
    ]);

    request.trace.execution_trace_id = execution_trace_id;
    request.trace.request_key = request_key.clone();
    request.trace.final_payload = Some(json!({
        "verified_queries": verified_queries.clone()
    }));
    request.trace.failure_payload = None;
    request.trace.request_metadata = json!({
        "surface": "alice.eth",
        "record_keys": ordered_record_keys,
        "normalizer_version": "uts46-v1"
    });
    request.trace.finished_at = Some(finished_at);
    request.outcome.cache_key.request_key = request_key;
    request.outcome.execution_trace_id = execution_trace_id;
    request.outcome.outcome_payload = Some(json!({
        "verified_queries": verified_queries
    }));
    request.outcome.failure_payload = None;
    request.outcome.finished_at = finished_at;
    request
}

fn avatar_mixed_selector_request() -> PersistEnsExactNameVerifiedResolutionRequest {
    let mut request = avatar_success_request();
    let ordered_record_keys = vec![
        "avatar".to_owned(),
        "text:com.twitter".to_owned(),
        "contenthash".to_owned(),
        "addr:60".to_owned(),
    ];
    let request_key = normalized_request_key(ENS_NAMESPACE, "alice.eth", &ordered_record_keys);
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000028);
    let finished_at = timestamp(1_717_171_930);
    let avatar = "https://cdn.example.test/alice.png";
    let contenthash = "ipfs://bafybeigdyrzt5sfp7udm7hu76fx4f2jv4jvgxk5csodx4d6vshv3zysn7u";
    let verified_queries = json!([
        {
            "record_key": "avatar",
            "status": "success",
            "value": {
                "value": avatar
            }
        },
        {
            "record_key": "text:com.twitter",
            "status": "not_found",
            "failure_reason": "no_text_record"
        },
        {
            "record_key": "contenthash",
            "status": "success",
            "value": {
                "value": contenthash
            }
        },
        {
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            }
        }
    ]);

    request.trace.execution_trace_id = execution_trace_id;
    request.trace.request_key = request_key.clone();
    request.trace.final_payload = Some(json!({
        "verified_queries": verified_queries.clone()
    }));
    request.trace.failure_payload = None;
    request.trace.request_metadata = json!({
        "surface": "alice.eth",
        "record_keys": ordered_record_keys,
        "normalizer_version": "uts46-v1"
    });
    request.trace.finished_at = Some(finished_at);
    request.outcome.cache_key.request_key = request_key;
    request.outcome.execution_trace_id = execution_trace_id;
    request.outcome.outcome_payload = Some(json!({
        "verified_queries": verified_queries
    }));
    request.outcome.failure_payload = None;
    request.outcome.finished_at = finished_at;
    request
}

fn alias_only_text_request() -> PersistEnsExactNameVerifiedResolutionRequest {
    let mut request = success_request();
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000001c);
    let request_key =
        normalized_request_key(ENS_NAMESPACE, "alice.eth", &["text:com.twitter".to_owned()]);
    let alias_target = alias_target(Uuid::from_u128(0x0e7ec7ace0000000000000000000aab3));

    request.trace.execution_trace_id = execution_trace_id;
    request.trace.request_key = request_key.clone();
    request.trace.final_payload = Some(json!({
        "record_kind": "text",
        "value": "@alice-via-alias"
    }));
    request.trace.request_metadata = json!({
        "surface": "alice.eth",
        "record_key": "text:com.twitter",
        "binding_kind": RESOLVER_ALIAS_PATH_BINDING_KIND,
        "alias": {
            "final_target": alias_target.clone(),
            "hops": [alias_target.clone()]
        },
        "normalizer_version": "uts46-v1"
    });
    request.trace.steps[1].step_payload = json!({
        "name": "alice.eth",
        "text_key": "com.twitter",
        "value": "@alice-via-alias"
    });
    request.outcome.cache_key.request_key = request_key;
    request.outcome.execution_trace_id = execution_trace_id;
    request.outcome.outcome_payload = Some(json!({
        "verified_queries": [
            {
                "record_key": "text:com.twitter",
                "status": "success",
                "value": {
                    "value": "@alice-via-alias"
                }
            }
        ]
    }));
    request
}

fn alias_only_avatar_request() -> PersistEnsExactNameVerifiedResolutionRequest {
    let mut request = avatar_success_request();
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000029);
    let request_key = normalized_request_key(ENS_NAMESPACE, "alice.eth", &["avatar".to_owned()]);
    let alias_target = alias_target(Uuid::from_u128(0x0e7ec7ace0000000000000000000aab5));
    let avatar = "https://cdn.example.test/alice-via-alias.png";

    request.trace.execution_trace_id = execution_trace_id;
    request.trace.request_key = request_key.clone();
    request.trace.final_payload = Some(json!({
        "record_kind": "avatar",
        "value": avatar
    }));
    request.trace.request_metadata = json!({
        "surface": "alice.eth",
        "record_key": "avatar",
        "binding_kind": RESOLVER_ALIAS_PATH_BINDING_KIND,
        "alias": {
            "final_target": alias_target.clone(),
            "hops": [alias_target.clone()]
        },
        "normalizer_version": "uts46-v1"
    });
    request.trace.steps[1].step_payload = json!({
        "name": "alice.eth",
        "avatar": avatar
    });
    request.outcome.cache_key.request_key = request_key;
    request.outcome.execution_trace_id = execution_trace_id;
    request.outcome.outcome_payload = Some(json!({
        "verified_queries": [
            {
                "record_key": "avatar",
                "status": "success",
                "value": {
                    "value": avatar
                }
            }
        ]
    }));
    request
}

#[tokio::test]
async fn persists_successful_direct_path_and_reads_back_storage_identity() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = success_request();
    seed_supported_resolution_storage(&database, &request).await?;

    let persisted =
        persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
    assert_eq!(
        persisted,
        PersistedVerifiedResolutionIdentity {
            execution_trace_id: request.trace.execution_trace_id,
            cache_key: request.outcome.cache_key.clone(),
        }
    );

    let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
        .await?
        .expect("execution trace must exist after persistence");
    assert_eq!(loaded_trace, request.trace);

    let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
        .await?
        .expect("execution outcome must exist after persistence");
    assert_eq!(loaded_outcome, request.outcome);

    let loaded_raw_calls = load_raw_call_snapshots_by_block_hash(
        database.pool(),
        ETHEREUM_MAINNET_CHAIN_ID,
        "0xabc123",
    )
    .await?;
    assert_eq!(loaded_raw_calls, request.raw_call_snapshots);

    let persisted_again =
        persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
    assert_eq!(persisted_again, persisted);
    assert_eq!(
        load_raw_call_snapshots_by_block_hash(
            database.pool(),
            ETHEREUM_MAINNET_CHAIN_ID,
            "0xabc123",
        )
        .await?,
        request.raw_call_snapshots
    );

    database.cleanup().await
}

#[tokio::test]
async fn persists_direct_path_for_later_selected_snapshot_without_recursive_ancestry() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let mut request = success_request();
    seed_supported_resolution_storage(&database, &request).await?;
    upsert_chain_lineage_blocks(
        database.pool(),
        &[
            ChainLineageBlock {
                chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
                block_hash: "0xabc123".to_owned(),
                parent_hash: None,
                block_number: 21_000_000,
                block_timestamp: timestamp(1_717_171_717),
                logs_bloom: None,
                transactions_root: None,
                receipts_root: None,
                state_root: None,
                canonicality_state: CanonicalityState::Finalized,
            },
            ChainLineageBlock {
                chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
                block_hash: "0xabc124".to_owned(),
                parent_hash: Some("0xnot-the-projected-parent".to_owned()),
                block_number: 21_000_001,
                block_timestamp: timestamp(1_717_171_729),
                logs_bloom: None,
                transactions_root: None,
                receipts_root: None,
                state_root: None,
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let selected_positions = json!([
        {
            "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
            "block_number": 21_000_001,
            "block_hash": "0xabc124"
        }
    ]);
    request.outcome.cache_key.requested_chain_positions = selected_positions.clone();
    request.trace.chain_context["requested_positions"] = selected_positions;
    for snapshot in &mut request.raw_call_snapshots {
        snapshot.block_hash = "0xabc124".to_owned();
        snapshot.block_number = 21_000_001;
    }
    for step in &mut request.trace.steps {
        step.canonicality_dependency[ETHEREUM_MAINNET_CHAIN_ID]["block_hash"] = json!("0xabc124");
        step.canonicality_dependency[ETHEREUM_MAINNET_CHAIN_ID]["block_number"] = json!(21_000_001);
    }

    let persisted =
        persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
    assert_eq!(persisted.cache_key, request.outcome.cache_key);
    assert_eq!(
        load_execution_outcome(database.pool(), &persisted.cache_key).await?,
        Some(request.outcome.clone())
    );
    assert_eq!(
        load_raw_call_snapshots_by_block_hash(
            database.pool(),
            ETHEREUM_MAINNET_CHAIN_ID,
            "0xabc124",
        )
        .await?,
        request.raw_call_snapshots
    );

    database.cleanup().await
}

#[tokio::test]
async fn persists_direct_path_when_name_projection_is_newer_than_record_inventory_projection()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let mut request = success_request();
    let selected_positions = json!([
        {
            "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
            "block_number": 21_000_001,
            "block_hash": "0xabc124"
        }
    ]);
    request.outcome.cache_key.requested_chain_positions = selected_positions.clone();
    request.trace.chain_context["requested_positions"] = selected_positions;
    for snapshot in &mut request.raw_call_snapshots {
        snapshot.block_hash = "0xabc124".to_owned();
        snapshot.block_number = 21_000_001;
    }
    for step in &mut request.trace.steps {
        step.canonicality_dependency[ETHEREUM_MAINNET_CHAIN_ID]["block_hash"] = json!("0xabc124");
        step.canonicality_dependency[ETHEREUM_MAINNET_CHAIN_ID]["block_number"] = json!(21_000_001);
    }

    seed_supported_resolution_storage(&database, &request).await?;
    upsert_chain_lineage_blocks(
        database.pool(),
        &[
            chain_lineage_block(
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xabc123",
                21_000_000,
                1_717_171_717,
            ),
            chain_lineage_block(
                ETHEREUM_MAINNET_CHAIN_ID,
                "0xabc124",
                21_000_001,
                1_717_171_729,
            ),
        ],
    )
    .await?;

    let persisted =
        persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
    assert_eq!(persisted.cache_key, request.outcome.cache_key);
    assert_eq!(
        load_execution_outcome(database.pool(), &persisted.cache_key).await?,
        Some(request.outcome.clone())
    );

    database.cleanup().await
}

#[tokio::test]
async fn persists_successful_direct_path_with_one_pool_connection() -> Result<()> {
    let database = TestDatabase::new_with_max_connections(1).await?;
    let request = success_request();
    seed_supported_resolution_storage(&database, &request).await?;

    let persisted = timeout(
        Duration::from_secs(5),
        persist_ens_exact_name_verified_resolution_direct(database.pool(), &request),
    )
    .await
    .expect("direct-path persistence should not block waiting for a second pool checkout")?;
    assert_eq!(
        persisted,
        PersistedVerifiedResolutionIdentity {
            execution_trace_id: request.trace.execution_trace_id,
            cache_key: request.outcome.cache_key.clone(),
        }
    );
    assert_eq!(
        load_execution_trace(database.pool(), persisted.execution_trace_id).await?,
        Some(request.trace.clone())
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &persisted.cache_key).await?,
        Some(request.outcome.clone())
    );

    database.cleanup().await
}

#[tokio::test]
async fn persists_avatar_success_direct_path_and_reads_back_storage_identity() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = avatar_success_request();
    seed_supported_resolution_storage(&database, &request).await?;

    let persisted =
        persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
    assert_eq!(
        persisted,
        PersistedVerifiedResolutionIdentity {
            execution_trace_id: request.trace.execution_trace_id,
            cache_key: request.outcome.cache_key.clone(),
        }
    );

    let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
        .await?
        .expect("execution trace must exist after avatar persistence");
    assert_eq!(loaded_trace, request.trace);

    let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
        .await?
        .expect("execution outcome must exist after avatar persistence");
    assert_eq!(loaded_outcome, request.outcome);

    database.cleanup().await
}

#[tokio::test]
async fn persists_multi_selector_direct_path_with_ordered_mixed_results() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = multi_selector_request();
    seed_supported_resolution_storage(&database, &request).await?;

    let persisted =
        persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
    assert_eq!(
        persisted,
        PersistedVerifiedResolutionIdentity {
            execution_trace_id: request.trace.execution_trace_id,
            cache_key: request.outcome.cache_key.clone(),
        }
    );
    assert_eq!(
        persisted.cache_key.request_key,
        "ens:alice.eth:addr:0,addr:2,addr:60"
    );

    let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
        .await?
        .expect("execution trace must exist after persistence");
    assert_eq!(loaded_trace, request.trace);

    let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
        .await?
        .expect("execution outcome must exist after persistence");
    assert_eq!(loaded_outcome, request.outcome);

    let loaded_verified_queries = loaded_outcome
        .outcome_payload
        .as_ref()
        .and_then(|payload| payload.get("verified_queries"))
        .and_then(Value::as_array)
        .expect("verified_queries must be present");
    let ordered_record_keys = loaded_verified_queries
        .iter()
        .filter_map(|query| query.get("record_key"))
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    assert_eq!(ordered_record_keys, vec!["addr:60", "addr:0", "addr:2"]);

    database.cleanup().await
}

#[tokio::test]
async fn persists_mixed_selector_direct_path_with_avatar_and_preserves_query_order() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = avatar_mixed_selector_request();
    seed_supported_resolution_storage(&database, &request).await?;

    let persisted =
        persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
    assert_eq!(
        persisted.cache_key.request_key,
        "ens:alice.eth:addr:60,avatar,contenthash,text:com.twitter"
    );

    let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
        .await?
        .expect("execution trace must exist after avatar mixed persistence");
    assert_eq!(loaded_trace, request.trace);

    let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
        .await?
        .expect("execution outcome must exist after avatar mixed persistence");
    assert_eq!(loaded_outcome, request.outcome);

    let loaded_verified_queries = loaded_outcome
        .outcome_payload
        .as_ref()
        .and_then(|payload| payload.get("verified_queries"))
        .and_then(Value::as_array)
        .expect("verified_queries must be present");
    let ordered_record_keys = loaded_verified_queries
        .iter()
        .filter_map(|query| query.get("record_key"))
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    assert_eq!(
        ordered_record_keys,
        vec!["avatar", "text:com.twitter", "contenthash", "addr:60"]
    );

    database.cleanup().await
}

#[tokio::test]
async fn persists_execution_failed_direct_path_without_raw_call_snapshots() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = execution_failed_request();
    seed_supported_resolution_storage(&database, &request).await?;

    let persisted =
        persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
    assert_eq!(
        persisted.execution_trace_id,
        request.trace.execution_trace_id
    );

    let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
        .await?
        .expect("execution trace must exist after persistence");
    assert_eq!(loaded_trace, request.trace);

    let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
        .await?
        .expect("execution outcome must exist after persistence");
    assert_eq!(loaded_outcome, request.outcome);

    assert!(
        load_raw_call_snapshots_by_block_hash(
            database.pool(),
            ETHEREUM_MAINNET_CHAIN_ID,
            "0xabc123",
        )
        .await?
        .is_empty(),
        "execution failed direct path fixture should not persist raw call snapshots"
    );

    database.cleanup().await
}

#[tokio::test]
async fn persists_contenthash_success_direct_path() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = contenthash_success_request();
    seed_supported_resolution_storage(&database, &request).await?;

    let persisted =
        persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
    assert_eq!(
        persisted,
        PersistedVerifiedResolutionIdentity {
            execution_trace_id: request.trace.execution_trace_id,
            cache_key: request.outcome.cache_key.clone(),
        }
    );

    let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
        .await?
        .expect("execution trace must exist after persistence");
    assert_eq!(loaded_trace, request.trace);

    let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
        .await?
        .expect("execution outcome must exist after persistence");
    assert_eq!(loaded_outcome, request.outcome);

    database.cleanup().await
}

#[tokio::test]
async fn persists_contenthash_not_found_direct_path() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = contenthash_not_found_request();
    seed_supported_resolution_storage(&database, &request).await?;

    let persisted =
        persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;

    let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
        .await?
        .expect("execution trace must exist after persistence");
    assert_eq!(loaded_trace, request.trace);

    let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
        .await?
        .expect("execution outcome must exist after persistence");
    assert_eq!(loaded_outcome, request.outcome);

    database.cleanup().await
}

#[tokio::test]
async fn persists_contenthash_execution_failed_direct_path_without_raw_call_snapshots() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let request = contenthash_execution_failed_request();
    seed_supported_resolution_storage(&database, &request).await?;

    let persisted =
        persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
    assert_eq!(
        persisted.execution_trace_id,
        request.trace.execution_trace_id
    );

    let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
        .await?
        .expect("execution trace must exist after persistence");
    assert_eq!(loaded_trace, request.trace);

    let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
        .await?
        .expect("execution outcome must exist after persistence");
    assert_eq!(loaded_outcome, request.outcome);

    assert!(
        load_raw_call_snapshots_by_block_hash(
            database.pool(),
            ETHEREUM_MAINNET_CHAIN_ID,
            "0xabc123",
        )
        .await?
        .is_empty(),
        "contenthash execution failed direct path fixture should not persist raw call snapshots"
    );

    database.cleanup().await
}

#[tokio::test]
async fn persists_mixed_selector_direct_path_with_contenthash_and_preserves_query_order()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let request = contenthash_mixed_selector_request();
    seed_supported_resolution_storage(&database, &request).await?;

    let persisted =
        persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
    assert_eq!(
        persisted.cache_key.request_key,
        "ens:alice.eth:addr:60,contenthash,text:com.twitter"
    );

    let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
        .await?
        .expect("execution trace must exist after persistence");
    assert_eq!(loaded_trace, request.trace);

    let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
        .await?
        .expect("execution outcome must exist after persistence");
    assert_eq!(loaded_outcome, request.outcome);

    let loaded_verified_queries = loaded_outcome
        .outcome_payload
        .as_ref()
        .and_then(|payload| payload.get("verified_queries"))
        .and_then(Value::as_array)
        .expect("verified_queries must be present");
    let ordered_record_keys = loaded_verified_queries
        .iter()
        .filter_map(|query| query.get("record_key"))
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    assert_eq!(
        ordered_record_keys,
        vec!["text:com.twitter", "contenthash", "addr:60"]
    );

    database.cleanup().await
}

#[tokio::test]
async fn persists_exact_surface_alias_only_path_with_resolver_alias_binding() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut request = alias_only_text_request();
    seed_supported_resolution_storage_from_rebuild(&database, &mut request).await?;

    let persisted =
        persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
    assert_eq!(
        persisted,
        PersistedVerifiedResolutionIdentity {
            execution_trace_id: request.trace.execution_trace_id,
            cache_key: request.outcome.cache_key.clone(),
        }
    );

    let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
        .await?
        .expect("execution trace must exist after alias-only persistence");
    assert_eq!(loaded_trace, request.trace);

    let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
        .await?
        .expect("execution outcome must exist after alias-only persistence");
    assert_eq!(loaded_outcome, request.outcome);

    database.cleanup().await
}

#[tokio::test]
async fn persists_exact_surface_alias_only_avatar_path_with_resolver_alias_binding() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = alias_only_avatar_request();
    seed_supported_resolution_storage(&database, &request).await?;

    let persisted =
        persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
    assert_eq!(
        persisted,
        PersistedVerifiedResolutionIdentity {
            execution_trace_id: request.trace.execution_trace_id,
            cache_key: request.outcome.cache_key.clone(),
        }
    );

    let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
        .await?
        .expect("execution trace must exist after alias-only avatar persistence");
    assert_eq!(loaded_trace, request.trace);

    let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
        .await?
        .expect("execution outcome must exist after alias-only avatar persistence");
    assert_eq!(loaded_outcome, request.outcome);

    database.cleanup().await
}

#[tokio::test]
async fn rolls_back_raw_calls_and_trace_when_outcome_write_fails() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = success_request();
    seed_supported_resolution_storage(&database, &request).await?;

    let mut conflicting_trace = request.trace.clone();
    conflicting_trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000016);
    conflicting_trace.request_type = "verified_primary_name".to_owned();
    conflicting_trace.final_payload = Some(json!({
        "verified_primary_name": {
            "status": "success",
            "name": "alice.eth"
        }
    }));
    upsert_execution_trace(database.pool(), &conflicting_trace).await?;

    let mut conflicting_outcome = request.outcome.clone();
    conflicting_outcome.execution_trace_id = conflicting_trace.execution_trace_id;
    conflicting_outcome.request_type = conflicting_trace.request_type.clone();
    conflicting_outcome.namespace = "basenames".to_owned();
    conflicting_outcome.outcome_payload = Some(json!({
        "verified_primary_name": {
            "status": "success",
            "name": "alice.eth"
        }
    }));
    upsert_execution_outcome(database.pool(), &conflicting_outcome).await?;

    let error = persist_ens_exact_name_verified_resolution_direct(database.pool(), &request)
        .await
        .expect_err("conflicting cache identity must roll back the whole direct-path write");
    assert!(
        error
            .to_string()
            .contains("execution outcome cache identity mismatch"),
        "unexpected error: {error:#}"
    );
    assert!(
        load_execution_trace(database.pool(), request.trace.execution_trace_id)
            .await?
            .is_none(),
        "failed direct-path persistence must not leave a trace row behind"
    );
    assert!(
        load_raw_call_snapshots_by_block_hash(
            database.pool(),
            ETHEREUM_MAINNET_CHAIN_ID,
            "0xabc123",
        )
        .await?
        .is_empty(),
        "failed direct-path persistence must not leave raw call snapshots behind"
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &request.outcome.cache_key).await?,
        Some(conflicting_outcome),
        "the pre-existing conflicting outcome must remain untouched"
    );

    database.cleanup().await
}

#[tokio::test]
async fn persists_trace_only_when_storage_revalidation_fails_closed() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = success_request();

    let error = persist_ens_exact_name_verified_resolution_direct(database.pool(), &request)
        .await
        .expect_err("missing projection inputs must fail during storage revalidation");
    assert!(
        error
            .to_string()
            .contains("supported outcome persistence failed closed after storage revalidation"),
        "unexpected error: {error:#}"
    );
    assert_eq!(
        load_execution_trace(database.pool(), request.trace.execution_trace_id).await?,
        Some(request.trace.clone())
    );
    assert!(
        load_execution_outcome(database.pool(), &request.outcome.cache_key)
            .await?
            .is_none(),
        "revalidation failure must not persist outcome rows"
    );
    assert!(
        load_raw_call_snapshots_by_block_hash(
            database.pool(),
            ETHEREUM_MAINNET_CHAIN_ID,
            "0xabc123",
        )
        .await?
        .is_empty(),
        "revalidation failure must not persist raw call snapshots"
    );

    database.cleanup().await
}

#[test]
fn validates_stable_request_key_normalization_for_multi_selector_requests() -> Result<()> {
    let request = multi_selector_request();
    validate_direct_request(&request)?;
    assert_eq!(
        request.trace.request_key,
        "ens:alice.eth:addr:0,addr:2,addr:60"
    );

    let mut unnormalized_request = request.clone();
    unnormalized_request.trace.request_key = "ens:alice.eth:addr:60,addr:0,addr:2".to_owned();
    unnormalized_request.outcome.cache_key.request_key =
        unnormalized_request.trace.request_key.clone();

    let error = validate_direct_request(&unnormalized_request)
        .expect_err("unnormalized request_key must be rejected");
    assert!(
        error
            .to_string()
            .contains("does not match expected ens:alice.eth:addr:0,addr:2,addr:60"),
        "unexpected error: {error:#}"
    );

    Ok(())
}

#[tokio::test]
async fn rejects_duplicate_selectors_before_writing_any_storage() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut request = multi_selector_request();
    let duplicate_record_keys = vec!["addr:60".to_owned(), "addr:60".to_owned()];
    let request_key = normalized_request_key(ENS_NAMESPACE, "alice.eth", &duplicate_record_keys);
    request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000015);
    request.trace.request_key = request_key.clone();
    request.trace.request_metadata = json!({
        "surface": "alice.eth",
        "record_keys": duplicate_record_keys,
        "normalizer_version": "uts46-v1"
    });
    request.trace.final_payload = Some(json!({
        "verified_queries": [
            {
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000aa"
                }
            },
            {
                "record_key": "addr:60",
                "status": "not_found",
                "failure_reason": "no_addr_record"
            }
        ]
    }));
    request.outcome.execution_trace_id = request.trace.execution_trace_id;
    request.outcome.cache_key.request_key = request_key;
    request.outcome.outcome_payload = request.trace.final_payload.clone();

    let error = persist_ens_exact_name_verified_resolution_direct(database.pool(), &request)
        .await
        .expect_err("duplicate selectors must be rejected");
    assert!(
        error
            .to_string()
            .contains("must not contain duplicate selectors"),
        "unexpected error: {error:#}"
    );
    assert!(
        load_execution_trace(database.pool(), request.trace.execution_trace_id)
            .await?
            .is_none(),
        "rejected request must not persist trace rows"
    );
    assert!(
        load_execution_outcome(database.pool(), &request.outcome.cache_key)
            .await?
            .is_none(),
        "rejected request must not persist outcome rows"
    );
    assert!(
        load_raw_call_snapshots_by_block_hash(
            database.pool(),
            ETHEREUM_MAINNET_CHAIN_ID,
            "0xabc123",
        )
        .await?
        .is_empty(),
        "rejected request must not persist raw call snapshots"
    );

    database.cleanup().await
}

#[tokio::test]
async fn persists_text_selector_results_before_writing_storage() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut request = multi_selector_request();
    let ordered_record_keys = vec!["addr:60".to_owned(), "text:com.twitter".to_owned()];
    let request_key = normalized_request_key(ENS_NAMESPACE, "alice.eth", &ordered_record_keys);
    request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000013);
    request.trace.request_key = request_key.clone();
    request.trace.request_metadata = json!({
        "surface": "alice.eth",
        "record_keys": ordered_record_keys,
        "normalizer_version": "uts46-v1"
    });
    request.trace.final_payload = Some(json!({
        "verified_queries": [
            {
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000aa"
                }
            },
            {
                "record_key": "text:com.twitter",
                "status": "success",
                "value": {
                    "value": "@alice"
                }
            }
        ]
    }));
    request.outcome.execution_trace_id = request.trace.execution_trace_id;
    request.outcome.cache_key.request_key = request_key;
    request.outcome.outcome_payload = request.trace.final_payload.clone();
    seed_supported_resolution_storage(&database, &request).await?;

    let persisted =
        persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
    assert!(
        load_execution_trace(database.pool(), persisted.execution_trace_id)
            .await?
            .is_some(),
        "supported text selector must persist trace rows"
    );
    assert!(
        load_execution_outcome(database.pool(), &persisted.cache_key)
            .await?
            .is_some(),
        "supported text selector must persist outcome rows"
    );

    database.cleanup().await
}

#[tokio::test]
async fn rejects_still_unsupported_selector_before_writing_any_storage() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut request = multi_selector_request();
    let ordered_record_keys = vec!["addr:60".to_owned(), "abi".to_owned()];
    let request_key = normalized_request_key(ENS_NAMESPACE, "alice.eth", &ordered_record_keys);
    request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000017);
    request.trace.request_key = request_key.clone();
    request.trace.request_metadata = json!({
        "surface": "alice.eth",
        "record_keys": ordered_record_keys,
        "normalizer_version": "uts46-v1"
    });
    request.trace.final_payload = Some(json!({
        "verified_queries": [
            {
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000aa"
                }
            },
            {
                "record_key": "abi",
                "status": "success",
                "value": {
                    "value": "0x1234"
                }
            }
        ]
    }));
    request.outcome.execution_trace_id = request.trace.execution_trace_id;
    request.outcome.cache_key.request_key = request_key;
    request.outcome.outcome_payload = request.trace.final_payload.clone();

    let error = persist_ens_exact_name_verified_resolution_direct(database.pool(), &request)
        .await
        .expect_err("unsupported selector must be rejected");
    assert!(
        error.to_string().contains(
            "only supports addr:<coin_type>, avatar, contenthash, and text:<key> selectors"
        ),
        "unexpected error: {error:#}"
    );
    assert!(
        load_execution_outcome(database.pool(), &request.outcome.cache_key)
            .await?
            .is_none(),
        "rejected request must not persist outcome rows"
    );
    assert!(
        load_raw_call_snapshots_by_block_hash(
            database.pool(),
            ETHEREUM_MAINNET_CHAIN_ID,
            "0xabc123",
        )
        .await?
        .is_empty(),
        "rejected request must not persist raw call snapshots"
    );

    database.cleanup().await
}

#[tokio::test]
async fn rejects_basenames_path_before_writing_any_storage() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut request = avatar_success_request();
    request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000002a);
    request.outcome.execution_trace_id = request.trace.execution_trace_id;
    request.trace.steps[1].step_kind = "call_basenames_resolver".to_owned();

    let error = persist_ens_exact_name_verified_resolution_direct(database.pool(), &request)
        .await
        .expect_err("Basenames execution paths must remain unsupported");
    assert!(
        error
            .to_string()
            .contains("must not persist non-direct step call_basenames_resolver"),
        "unexpected error: {error:#}"
    );
    assert!(
        load_execution_trace(database.pool(), request.trace.execution_trace_id)
            .await?
            .is_none(),
        "rejected Basenames path must not persist trace rows"
    );
    assert!(
        load_execution_outcome(database.pool(), &request.outcome.cache_key)
            .await?
            .is_none(),
        "rejected Basenames path must not persist outcome rows"
    );

    database.cleanup().await
}

#[tokio::test]
async fn rejects_linked_subregistry_binding_before_writing_any_storage() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut request = success_request();
    request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000001d);
    request.outcome.execution_trace_id = request.trace.execution_trace_id;
    request.trace.request_metadata = json!({
        "surface": "alice.eth",
        "record_key": "addr:60",
        "binding_kind": LINKED_SUBREGISTRY_PATH_BINDING_KIND,
        "normalizer_version": "uts46-v1"
    });

    let error = persist_ens_exact_name_verified_resolution_direct(database.pool(), &request)
        .await
        .expect_err("linked-subregistry path must remain unsupported");
    assert!(
        error
            .to_string()
            .contains("must not persist non-alias ancestor-selected binding_kind"),
        "unexpected error: {error:#}"
    );
    assert!(
        load_execution_trace(database.pool(), request.trace.execution_trace_id)
            .await?
            .is_none(),
        "rejected linked-subregistry request must not persist trace rows"
    );
    assert!(
        load_execution_outcome(database.pool(), &request.outcome.cache_key)
            .await?
            .is_none(),
        "rejected linked-subregistry request must not persist outcome rows"
    );

    database.cleanup().await
}

#[tokio::test]
async fn rejects_wildcard_derived_alias_only_path_before_writing_any_storage() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut request = alias_only_text_request();
    request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000001e);
    request.outcome.execution_trace_id = request.trace.execution_trace_id;
    request.trace.request_metadata["wildcard"] = json!({
        "source": alias_target(Uuid::from_u128(0x0e7ec7ace0000000000000000000aab4)),
        "matched_labels": ["profile"]
    });

    let error = persist_ens_exact_name_verified_resolution_direct(database.pool(), &request)
        .await
        .expect_err("wildcard-derived alias-only path must remain unsupported");
    assert!(
        error
            .to_string()
            .contains("only supports wildcard.source=null with matched_labels=[]"),
        "unexpected error: {error:#}"
    );
    assert!(
        load_execution_trace(database.pool(), request.trace.execution_trace_id)
            .await?
            .is_none(),
        "rejected wildcard-derived request must not persist trace rows"
    );
    assert!(
        load_execution_outcome(database.pool(), &request.outcome.cache_key)
            .await?
            .is_none(),
        "rejected wildcard-derived request must not persist outcome rows"
    );

    database.cleanup().await
}

#[tokio::test]
async fn persists_exact_surface_wildcard_derived_path_with_observed_wildcard_binding() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let mut request = success_request();
    let wildcard_source = wildcard_source_ref(Uuid::from_u128(0x0e7ec7ace0000000000000000000aab6));

    request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000030);
    request.trace.request_key = "ens:alice.eth:addr:60".to_owned();
    request.trace.request_metadata = json!({
        "surface": "alice.eth",
        "record_key": "addr:60",
        "binding_kind": OBSERVED_WILDCARD_PATH_BINDING_KIND,
        "wildcard": {
            "source": wildcard_source.clone(),
            "matched_labels": ["alice"]
        },
        "normalizer_version": "uts46-v1"
    });
    request.trace.steps.push(ExecutionTraceStep {
        step_index: 2,
        step_kind: "call_wildcard_resolver".to_owned(),
        input_digest: Some("sha256:wildcard-input".to_owned()),
        output_digest: Some("sha256:wildcard-output".to_owned()),
        latency_ms: Some(19),
        canonicality_dependency: json!({
            ETHEREUM_MAINNET_CHAIN_ID: {
                "block_hash": "0xabc123",
                "block_number": 21_000_000,
                "state": "canonical"
            }
        }),
        step_payload: json!({
            "name": "alice.eth",
            "wildcard": {
                "source": wildcard_source,
                "matched_labels": ["alice"]
            }
        }),
    });
    request.outcome.execution_trace_id = request.trace.execution_trace_id;
    request.outcome.cache_key.request_key = request.trace.request_key.clone();
    request.outcome.finished_at = request
        .trace
        .finished_at
        .expect("wildcard-derived test trace must finish");
    seed_supported_resolution_storage_from_rebuild(&database, &mut request).await?;

    let persisted =
        persist_ens_exact_name_verified_resolution_direct(database.pool(), &request).await?;
    assert_eq!(
        persisted,
        PersistedVerifiedResolutionIdentity {
            execution_trace_id: request.trace.execution_trace_id,
            cache_key: request.outcome.cache_key.clone(),
        }
    );

    let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
        .await?
        .expect("execution trace must exist after wildcard-derived persistence");
    assert_eq!(loaded_trace, request.trace);

    let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
        .await?
        .expect("execution outcome must exist after wildcard-derived persistence");
    assert_eq!(loaded_outcome, request.outcome);

    database.cleanup().await
}

#[tokio::test]
async fn rejects_transport_assisted_alias_only_path_before_writing_any_storage() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut request = alias_only_text_request();
    request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000001f);
    request.outcome.execution_trace_id = request.trace.execution_trace_id;
    request.trace.request_metadata["transport"] = json!({
        "source_chain_id": ETHEREUM_MAINNET_CHAIN_ID,
        "target_chain_id": "base",
        "contract_address": "0x0000000000000000000000000000000000000bad",
        "latest_event_kind": "TransportConfigured"
    });

    let error = persist_ens_exact_name_verified_resolution_direct(database.pool(), &request)
        .await
        .expect_err("transport-assisted alias-only path must remain unsupported");
    assert!(
        error
            .to_string()
            .contains("transport-assisted persisted requests remain unsupported"),
        "unexpected error: {error:#}"
    );
    assert!(
        load_execution_trace(database.pool(), request.trace.execution_trace_id)
            .await?
            .is_none(),
        "rejected transport-assisted request must not persist trace rows"
    );
    assert!(
        load_execution_outcome(database.pool(), &request.outcome.cache_key)
            .await?
            .is_none(),
        "rejected transport-assisted request must not persist outcome rows"
    );

    database.cleanup().await
}

#[tokio::test]
async fn persists_basenames_transport_assisted_direct_path() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut request = basenames_transport_direct_request();
    seed_supported_resolution_storage_from_rebuild(&database, &mut request).await?;

    let persisted = persist_basenames_exact_name_verified_resolution_transport_direct(
        database.pool(),
        &request,
    )
    .await?;
    assert_eq!(
        persisted,
        PersistedVerifiedResolutionIdentity {
            execution_trace_id: request.trace.execution_trace_id,
            cache_key: request.outcome.cache_key.clone(),
        }
    );

    let loaded_trace = load_execution_trace(database.pool(), persisted.execution_trace_id)
        .await?
        .expect("execution trace must exist after Basenames transport persistence");
    assert_eq!(loaded_trace, request.trace);

    let loaded_outcome = load_execution_outcome(database.pool(), &persisted.cache_key)
        .await?
        .expect("execution outcome must exist after Basenames transport persistence");
    assert_eq!(loaded_outcome, request.outcome);

    database.cleanup().await
}

#[tokio::test]
async fn persists_basenames_transport_direct_with_one_pool_connection() -> Result<()> {
    let database = TestDatabase::new_with_max_connections(1).await?;
    let request = basenames_transport_direct_request();
    seed_supported_resolution_storage(&database, &request).await?;

    let persisted = timeout(
        Duration::from_secs(5),
        persist_basenames_exact_name_verified_resolution_transport_direct(
            database.pool(),
            &request,
        ),
    )
    .await
    .expect(
        "Basenames transport-direct persistence should not block waiting for a second pool checkout",
    )?;
    assert_eq!(
        persisted,
        PersistedVerifiedResolutionIdentity {
            execution_trace_id: request.trace.execution_trace_id,
            cache_key: request.outcome.cache_key.clone(),
        }
    );
    assert_eq!(
        load_execution_trace(database.pool(), persisted.execution_trace_id).await?,
        Some(request.trace.clone())
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &persisted.cache_key).await?,
        Some(request.outcome.clone())
    );

    database.cleanup().await
}

#[tokio::test]
async fn rejects_basenames_transport_direct_with_wrong_transport_contract() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut request = basenames_transport_direct_request();
    request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000032);
    request.outcome.execution_trace_id = request.trace.execution_trace_id;
    request.trace.request_metadata["transport"]["contract_address"] =
        json!("0x0000000000000000000000000000000000000bad");

    let error = persist_basenames_exact_name_verified_resolution_transport_direct(
        database.pool(),
        &request,
    )
    .await
    .expect_err("Basenames transport-direct persistence must reject out-of-class transport");
    assert!(
        error
            .to_string()
            .contains("must use transport base-mainnet -> ethereum-mainnet"),
        "unexpected error: {error:#}"
    );
    assert!(
        load_execution_trace(database.pool(), request.trace.execution_trace_id)
            .await?
            .is_none(),
        "rejected Basenames transport request must not persist trace rows"
    );
    assert!(
        load_execution_outcome(database.pool(), &request.outcome.cache_key)
            .await?
            .is_none(),
        "rejected Basenames transport request must not persist outcome rows"
    );

    database.cleanup().await
}

#[tokio::test]
async fn rejects_basenames_transport_direct_without_ethereum_position() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut request = basenames_transport_direct_request();
    request.trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000035);
    request.outcome.execution_trace_id = request.trace.execution_trace_id;
    request.trace.chain_context = json!({
        "requested_positions": [{
            "chain_id": BASE_MAINNET_CHAIN_ID,
            "block_number": 31_000_000,
            "block_hash": "0xbase123"
        }]
    });
    request.outcome.cache_key.requested_chain_positions =
        request.trace.chain_context["requested_positions"].clone();

    let error = persist_basenames_exact_name_verified_resolution_transport_direct(
        database.pool(),
        &request,
    )
    .await
    .expect_err("Basenames transport-direct persistence must require Ethereum position");
    assert!(
        error
            .to_string()
            .contains("must include exactly two chain positions")
            || error
                .to_string()
                .contains("must include chain_id ethereum-mainnet"),
        "unexpected error: {error:#}"
    );
    assert!(
        load_execution_trace(database.pool(), request.trace.execution_trace_id)
            .await?
            .is_none(),
        "rejected Basenames request missing Ethereum position must not persist trace rows"
    );
    assert!(
        load_execution_outcome(database.pool(), &request.outcome.cache_key)
            .await?
            .is_none(),
        "rejected Basenames request missing Ethereum position must not persist outcome rows"
    );

    database.cleanup().await
}

fn primary_name_anchor_row_for_namespace(
    namespace: &str,
    address: &str,
    coin_type: &str,
    claim_status: PrimaryNameClaimStatus,
) -> PrimaryNameCurrentRow {
    PrimaryNameCurrentRow {
        address: address.to_ascii_lowercase(),
        namespace: namespace.to_owned(),
        coin_type: coin_type.to_owned(),
        claim_status,
        raw_claim_name: (claim_status == PrimaryNameClaimStatus::InvalidName)
            .then(|| "bad name".to_owned()),
        claim_provenance: match namespace {
            ENS_NAMESPACE => json!({
                "source_family": "ens_v1_reverse_l1",
                "contract_role": "reverse_registrar",
                "contract_instance_id": "00000000-0000-0000-0000-000000000123",
                "emitting_address": "0x00000000000000000000000000000000000000ad"
            }),
            BASENAMES_NAMESPACE => json!({
                "source_family": "basenames_base_primary",
                "contract_role": "reverse_registrar",
                "contract_instance_id": "00000000-0000-0000-0000-000000000124",
                "emitting_address": "0x00000000000000000000000000000000000000ae"
            }),
            other => panic!("unsupported primary-name test namespace {other}"),
        },
    }
}

async fn insert_primary_name_anchor(
    database: &TestDatabase,
    address: &str,
    coin_type: &str,
    claim_status: PrimaryNameClaimStatus,
) -> Result<()> {
    upsert_primary_name_current_rows(
        database.pool(),
        &[primary_name_anchor_row_for_namespace(
            ENS_NAMESPACE,
            address,
            coin_type,
            claim_status,
        )],
    )
    .await?;
    Ok(())
}

async fn insert_basenames_primary_name_anchor(
    database: &TestDatabase,
    address: &str,
    coin_type: &str,
    claim_status: PrimaryNameClaimStatus,
) -> Result<()> {
    upsert_primary_name_current_rows(
        database.pool(),
        &[primary_name_anchor_row_for_namespace(
            BASENAMES_NAMESPACE,
            address,
            coin_type,
            claim_status,
        )],
    )
    .await?;
    Ok(())
}

fn verified_primary_name_ref_for_namespace(namespace: &str, name: &str) -> Value {
    json!({
        "logical_name_id": format!("{namespace}:{name}"),
        "namespace": namespace,
        "normalized_name": name,
        "canonical_display_name": name,
        "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
        "resource_id": "00000000-0000-0000-0000-000000000456",
        "binding_kind": "declared_registry_path"
    })
}

fn verified_primary_name_ref(name: &str) -> Value {
    verified_primary_name_ref_for_namespace(ENS_NAMESPACE, name)
}

fn verified_primary_request_for_namespace(
    namespace: &str,
    execution_trace_id: Uuid,
    normalized_address: &str,
    coin_type: &str,
    verified_primary_name: Value,
) -> PersistEnsVerifiedPrimaryNameRequest {
    let request_key =
        normalized_verified_primary_name_request_key(namespace, normalized_address, coin_type);
    let finished_at = timestamp(1_717_172_100);
    let manifest_versions = match namespace {
        ENS_NAMESPACE => manifest_versions(),
        BASENAMES_NAMESPACE => basenames_manifest_versions(),
        other => panic!("unsupported verified-primary test namespace {other}"),
    };
    let contracts_called = match namespace {
        ENS_NAMESPACE => json!([
            {
                "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
                "contract_address": ENS_UNIVERSAL_RESOLVER_ADDRESS,
                "selector": "0x9061b923"
            }
        ]),
        BASENAMES_NAMESPACE => json!([
            {
                "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
                "contract_address": BASENAMES_L1_RESOLVER_ADDRESS,
                "selector": "0x9061b923"
            }
        ]),
        other => panic!("unsupported verified-primary test namespace {other}"),
    };
    let gateway_digests = match namespace {
        ENS_NAMESPACE => json!([]),
        BASENAMES_NAMESPACE => json!(["sha256:basenames-verified-primary"]),
        other => panic!("unsupported verified-primary test namespace {other}"),
    };
    let steps = match namespace {
        ENS_NAMESPACE => vec![
            ExecutionTraceStep {
                step_index: 0,
                step_kind: "load_primary_name_claim".to_owned(),
                input_digest: Some("sha256:claim-input".to_owned()),
                output_digest: Some("sha256:claim-output".to_owned()),
                latency_ms: Some(2),
                canonicality_dependency: json!({
                    ETHEREUM_MAINNET_CHAIN_ID: {
                        "block_hash": "0xabc123",
                        "block_number": 21_000_000,
                        "state": "canonical"
                    }
                }),
                step_payload: json!({
                    "address": normalized_address,
                    "coin_type": coin_type
                }),
            },
            ExecutionTraceStep {
                step_index: 1,
                step_kind: "normalize_claimed_name".to_owned(),
                input_digest: Some("sha256:normalize-input".to_owned()),
                output_digest: Some("sha256:normalize-output".to_owned()),
                latency_ms: Some(1),
                canonicality_dependency: json!({
                    ETHEREUM_MAINNET_CHAIN_ID: {
                        "block_hash": "0xabc123",
                        "block_number": 21_000_000,
                        "state": "canonical"
                    }
                }),
                step_payload: json!({
                    "normalizer_version": "uts46-v1"
                }),
            },
            ExecutionTraceStep {
                step_index: 2,
                step_kind: "call_universal_resolver".to_owned(),
                input_digest: Some("sha256:resolver-input".to_owned()),
                output_digest: Some("sha256:resolver-output".to_owned()),
                latency_ms: Some(14),
                canonicality_dependency: json!({
                    ETHEREUM_MAINNET_CHAIN_ID: {
                        "block_hash": "0xabc123",
                        "block_number": 21_000_000,
                        "state": "canonical"
                    }
                }),
                step_payload: json!({
                    "name": "alice.eth",
                    "coin_type": coin_type
                }),
            },
        ],
        BASENAMES_NAMESPACE => vec![
            ExecutionTraceStep {
                step_index: 0,
                step_kind: "load_primary_name_claim".to_owned(),
                input_digest: Some("sha256:claim-input".to_owned()),
                output_digest: Some("sha256:claim-output".to_owned()),
                latency_ms: Some(2),
                canonicality_dependency: json!({
                    ETHEREUM_MAINNET_CHAIN_ID: {
                        "block_hash": "0xabc123",
                        "block_number": 21_000_000,
                        "state": "canonical"
                    }
                }),
                step_payload: json!({
                    "address": normalized_address,
                    "coin_type": coin_type
                }),
            },
            ExecutionTraceStep {
                step_index: 1,
                step_kind: "call_l1_resolver".to_owned(),
                input_digest: Some("sha256:l1-input".to_owned()),
                output_digest: Some("sha256:l1-output".to_owned()),
                latency_ms: Some(21),
                canonicality_dependency: json!({
                    ETHEREUM_MAINNET_CHAIN_ID: {
                        "block_hash": "0xabc123",
                        "block_number": 21_000_000,
                        "state": "canonical"
                    }
                }),
                step_payload: json!({
                    "address": normalized_address,
                    "coin_type": coin_type
                }),
            },
            ExecutionTraceStep {
                step_index: 2,
                step_kind: "complete_offchain_lookup".to_owned(),
                input_digest: Some("sha256:gateway-input".to_owned()),
                output_digest: Some("sha256:gateway-output".to_owned()),
                latency_ms: Some(33),
                canonicality_dependency: json!({
                    ETHEREUM_MAINNET_CHAIN_ID: {
                        "block_hash": "0xabc123",
                        "block_number": 21_000_000,
                        "state": "canonical"
                    }
                }),
                step_payload: json!({
                    "gateway": "https://basenames.example.test"
                }),
            },
        ],
        other => panic!("unsupported verified-primary test namespace {other}"),
    };
    PersistEnsVerifiedPrimaryNameRequest {
        trace: ExecutionTrace {
            execution_trace_id,
            request_type: VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
            request_key: request_key.clone(),
            namespace: namespace.to_owned(),
            chain_context: json!({
                "requested_positions": requested_chain_positions(),
                "topology_version_boundary": {
                    ETHEREUM_MAINNET_CHAIN_ID: 21_000_000
                }
            }),
            manifest_context: json!({
                "manifest_versions": manifest_versions.clone(),
                "rollout_boundary": "shadow"
            }),
            contracts_called,
            gateway_digests,
            final_payload: Some(json!({
                "verified_primary_name": verified_primary_name.clone()
            })),
            failure_payload: None,
            request_metadata: json!({
                "normalized_address": normalized_address,
                "coin_type": coin_type,
                "namespace": namespace
            }),
            finished_at: Some(finished_at),
            steps,
        },
        outcome: ExecutionOutcome {
            cache_key: ExecutionCacheKey {
                request_key,
                requested_chain_positions: requested_chain_positions(),
                manifest_versions,
                topology_version_boundary: version_boundary(Uuid::from_u128(
                    0x0e7ec7ace0000000000000000000bbb1,
                )),
                record_version_boundary: version_boundary(Uuid::from_u128(
                    0x0e7ec7ace0000000000000000000bbb2,
                )),
            },
            execution_trace_id,
            request_type: VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
            namespace: namespace.to_owned(),
            outcome_payload: Some(json!({
                "verified_primary_name": verified_primary_name
            })),
            failure_payload: None,
            finished_at,
        },
    }
}

fn verified_primary_request(
    execution_trace_id: Uuid,
    normalized_address: &str,
    coin_type: &str,
    verified_primary_name: Value,
) -> PersistEnsVerifiedPrimaryNameRequest {
    verified_primary_request_for_namespace(
        ENS_NAMESPACE,
        execution_trace_id,
        normalized_address,
        coin_type,
        verified_primary_name,
    )
}

fn expected_verified_primary_readback_provenance(
    request: &PersistEnsVerifiedPrimaryNameRequest,
) -> VerifiedPrimaryNameReadbackProvenance {
    VerifiedPrimaryNameReadbackProvenance {
        execution_trace_id: request.trace.execution_trace_id,
        manifest_versions: request.outcome.cache_key.manifest_versions.clone(),
    }
}

fn verified_primary_success_request() -> PersistEnsVerifiedPrimaryNameRequest {
    verified_primary_request(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000021),
        "0x00000000000000000000000000000000000000aa",
        "60",
        json!({
            "status": "success",
            "name": verified_primary_name_ref("alice.eth")
        }),
    )
}

fn verified_primary_mismatch_request() -> PersistEnsVerifiedPrimaryNameRequest {
    verified_primary_request(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000022),
        "0x00000000000000000000000000000000000000ab",
        "60",
        json!({
            "status": "mismatch",
            "name": verified_primary_name_ref("alice.eth"),
            "failure_reason": "resolved_target_mismatch"
        }),
    )
}

fn verified_primary_not_found_request() -> PersistEnsVerifiedPrimaryNameRequest {
    let mut request = verified_primary_request(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000023),
        "0x00000000000000000000000000000000000000ac",
        "60",
        json!({
            "status": "not_found"
        }),
    );
    request.trace.contracts_called = json!([]);
    request.trace.steps = vec![ExecutionTraceStep {
        step_index: 0,
        step_kind: "load_primary_name_claim".to_owned(),
        input_digest: Some("sha256:claim-input".to_owned()),
        output_digest: Some("sha256:claim-output".to_owned()),
        latency_ms: Some(2),
        canonicality_dependency: json!({
            ETHEREUM_MAINNET_CHAIN_ID: {
                "block_hash": "0xabc123",
                "block_number": 21_000_000,
                "state": "canonical"
            }
        }),
        step_payload: json!({
            "address": "0x00000000000000000000000000000000000000ac",
            "coin_type": "60"
        }),
    }];
    request
}

fn verified_primary_invalid_name_request() -> PersistEnsVerifiedPrimaryNameRequest {
    let mut request = verified_primary_request(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000024),
        "0x00000000000000000000000000000000000000ad",
        "60",
        json!({
            "status": "invalid_name",
            "failure_reason": "claim_name_not_normalizable"
        }),
    );
    request.trace.contracts_called = json!([]);
    request.trace.steps = vec![
        ExecutionTraceStep {
            step_index: 0,
            step_kind: "load_primary_name_claim".to_owned(),
            input_digest: Some("sha256:claim-input".to_owned()),
            output_digest: Some("sha256:claim-output".to_owned()),
            latency_ms: Some(2),
            canonicality_dependency: json!({
                ETHEREUM_MAINNET_CHAIN_ID: {
                    "block_hash": "0xabc123",
                    "block_number": 21_000_000,
                    "state": "canonical"
                }
            }),
            step_payload: json!({
                "address": "0x00000000000000000000000000000000000000ad",
                "coin_type": "60"
            }),
        },
        ExecutionTraceStep {
            step_index: 1,
            step_kind: "normalize_claimed_name".to_owned(),
            input_digest: Some("sha256:normalize-input".to_owned()),
            output_digest: Some("sha256:normalize-output".to_owned()),
            latency_ms: Some(1),
            canonicality_dependency: json!({
                ETHEREUM_MAINNET_CHAIN_ID: {
                    "block_hash": "0xabc123",
                    "block_number": 21_000_000,
                    "state": "canonical"
                }
            }),
            step_payload: json!({
                "normalizer_version": "uts46-v1",
                "error": "label_has_whitespace"
            }),
        },
    ];
    request
}

fn verified_primary_execution_failed_request() -> PersistEnsVerifiedPrimaryNameRequest {
    let mut request = verified_primary_request(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000025),
        "0x00000000000000000000000000000000000000ae",
        "60",
        json!({
            "status": "execution_failed",
            "failure_reason": "resolver_call_reverted"
        }),
    );
    request.trace.final_payload = None;
    request.trace.failure_payload = Some(json!({
        "failure_reason": "resolver_call_reverted",
        "stage": "call_universal_resolver"
    }));
    request.outcome.failure_payload = Some(json!({
        "failure_reason": "resolver_call_reverted",
        "reverted": true
    }));
    request
}

fn verified_primary_basenames_success_request() -> PersistEnsVerifiedPrimaryNameRequest {
    verified_primary_request_for_namespace(
        BASENAMES_NAMESPACE,
        Uuid::from_u128(0x0e7ec7ace00000000000000000000027),
        "0x00000000000000000000000000000000000000ba",
        "60",
        json!({
            "status": "success",
            "name": verified_primary_name_ref_for_namespace(
                BASENAMES_NAMESPACE,
                "alice.base.eth"
            )
        }),
    )
}

fn verified_primary_basenames_not_found_request() -> PersistEnsVerifiedPrimaryNameRequest {
    let mut request = verified_primary_request_for_namespace(
        BASENAMES_NAMESPACE,
        Uuid::from_u128(0x0e7ec7ace00000000000000000000028),
        "0x00000000000000000000000000000000000000bb",
        "60",
        json!({
            "status": "not_found"
        }),
    );
    request.trace.contracts_called = json!([]);
    request.trace.gateway_digests = json!([]);
    request.trace.steps = vec![ExecutionTraceStep {
        step_index: 0,
        step_kind: "load_primary_name_claim".to_owned(),
        input_digest: Some("sha256:claim-input".to_owned()),
        output_digest: Some("sha256:claim-output".to_owned()),
        latency_ms: Some(2),
        canonicality_dependency: json!({
            ETHEREUM_MAINNET_CHAIN_ID: {
                "block_hash": "0xabc123",
                "block_number": 21_000_000,
                "state": "canonical"
            }
        }),
        step_payload: json!({
            "address": "0x00000000000000000000000000000000000000bb",
            "coin_type": "60"
        }),
    }];
    request
}

fn verified_primary_basenames_invalid_name_request() -> PersistEnsVerifiedPrimaryNameRequest {
    let mut request = verified_primary_request_for_namespace(
        BASENAMES_NAMESPACE,
        Uuid::from_u128(0x0e7ec7ace00000000000000000000029),
        "0x00000000000000000000000000000000000000bc",
        "60",
        json!({
            "status": "invalid_name",
            "failure_reason": "claim_name_not_normalizable"
        }),
    );
    request.trace.contracts_called = json!([]);
    request.trace.gateway_digests = json!([]);
    request.trace.steps = vec![
        ExecutionTraceStep {
            step_index: 0,
            step_kind: "load_primary_name_claim".to_owned(),
            input_digest: Some("sha256:claim-input".to_owned()),
            output_digest: Some("sha256:claim-output".to_owned()),
            latency_ms: Some(2),
            canonicality_dependency: json!({
                ETHEREUM_MAINNET_CHAIN_ID: {
                    "block_hash": "0xabc123",
                    "block_number": 21_000_000,
                    "state": "canonical"
                }
            }),
            step_payload: json!({
                "address": "0x00000000000000000000000000000000000000bc",
                "coin_type": "60"
            }),
        },
        ExecutionTraceStep {
            step_index: 1,
            step_kind: "normalize_claimed_name".to_owned(),
            input_digest: Some("sha256:normalize-input".to_owned()),
            output_digest: Some("sha256:normalize-output".to_owned()),
            latency_ms: Some(1),
            canonicality_dependency: json!({
                ETHEREUM_MAINNET_CHAIN_ID: {
                    "block_hash": "0xabc123",
                    "block_number": 21_000_000,
                    "state": "canonical"
                }
            }),
            step_payload: json!({
                "normalizer_version": "uts46-v1",
                "error": "label_has_whitespace"
            }),
        },
    ];
    request
}

#[tokio::test]
async fn persists_verified_primary_success_and_reads_back() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = verified_primary_success_request();
    insert_primary_name_anchor(
        &database,
        "0x00000000000000000000000000000000000000aa",
        "60",
        PrimaryNameClaimStatus::Success,
    )
    .await?;

    let persisted = persist_ens_verified_primary_name(database.pool(), &request).await?;
    assert_eq!(
        persisted,
        PersistedVerifiedPrimaryNameIdentity {
            execution_trace_id: request.trace.execution_trace_id,
            cache_key: request.outcome.cache_key.clone(),
        }
    );

    let loaded = load_persisted_ens_verified_primary_name(database.pool(), &persisted.cache_key)
        .await?
        .expect("verified-primary readback must exist");
    assert_eq!(loaded.execution_trace_id, request.trace.execution_trace_id);
    assert_eq!(
        loaded.provenance,
        expected_verified_primary_readback_provenance(&request)
    );
    assert_eq!(
        loaded.verified_primary_name,
        json!({
            "status": "success",
            "name": verified_primary_name_ref("alice.eth")
        })
    );
    assert_eq!(loaded.trace, request.trace);
    assert_eq!(loaded.outcome, request.outcome);

    database.cleanup().await
}

#[tokio::test]
async fn persists_basenames_verified_primary_success_and_reads_back() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = verified_primary_basenames_success_request();
    insert_basenames_primary_name_anchor(
        &database,
        "0x00000000000000000000000000000000000000ba",
        "60",
        PrimaryNameClaimStatus::Success,
    )
    .await?;

    let persisted = persist_ens_verified_primary_name(database.pool(), &request).await?;
    let loaded = load_persisted_ens_verified_primary_name(database.pool(), &persisted.cache_key)
        .await?
        .expect("Basenames verified-primary readback must exist");
    assert_eq!(
        loaded.provenance,
        expected_verified_primary_readback_provenance(&request)
    );
    assert_eq!(
        loaded.verified_primary_name,
        json!({
            "status": "success",
            "name": verified_primary_name_ref_for_namespace(
                BASENAMES_NAMESPACE,
                "alice.base.eth"
            )
        })
    );
    assert_eq!(loaded.trace.namespace, BASENAMES_NAMESPACE);
    assert_eq!(loaded.outcome.namespace, BASENAMES_NAMESPACE);

    database.cleanup().await
}

#[tokio::test]
async fn persists_basenames_verified_primary_not_found_without_l1_resolver_call() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = verified_primary_basenames_not_found_request();
    insert_basenames_primary_name_anchor(
        &database,
        "0x00000000000000000000000000000000000000bb",
        "60",
        PrimaryNameClaimStatus::NotFound,
    )
    .await?;

    let persisted = persist_ens_verified_primary_name(database.pool(), &request).await?;
    let loaded = load_persisted_ens_verified_primary_name(database.pool(), &persisted.cache_key)
        .await?
        .expect("Basenames not_found readback must exist");
    assert_eq!(
        loaded.provenance,
        expected_verified_primary_readback_provenance(&request)
    );
    assert_eq!(
        loaded.verified_primary_name,
        json!({ "status": "not_found" })
    );
    assert_eq!(loaded.trace.contracts_called, json!([]));
    assert_eq!(loaded.trace.gateway_digests, json!([]));

    database.cleanup().await
}

#[tokio::test]
async fn persists_basenames_verified_primary_invalid_name_without_l1_resolver_call() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = verified_primary_basenames_invalid_name_request();
    insert_basenames_primary_name_anchor(
        &database,
        "0x00000000000000000000000000000000000000bc",
        "60",
        PrimaryNameClaimStatus::InvalidName,
    )
    .await?;

    let persisted = persist_ens_verified_primary_name(database.pool(), &request).await?;
    let loaded = load_persisted_ens_verified_primary_name(database.pool(), &persisted.cache_key)
        .await?
        .expect("Basenames invalid_name readback must exist");
    assert_eq!(
        loaded.provenance,
        expected_verified_primary_readback_provenance(&request)
    );
    assert_eq!(
        loaded.verified_primary_name,
        json!({
            "status": "invalid_name",
            "failure_reason": "claim_name_not_normalizable"
        })
    );
    assert_eq!(loaded.trace.contracts_called, json!([]));
    assert_eq!(loaded.trace.gateway_digests, json!([]));

    database.cleanup().await
}

#[tokio::test]
async fn persists_verified_primary_mismatch_and_reads_back() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = verified_primary_mismatch_request();
    insert_primary_name_anchor(
        &database,
        "0x00000000000000000000000000000000000000ab",
        "60",
        PrimaryNameClaimStatus::Success,
    )
    .await?;

    let persisted = persist_ens_verified_primary_name(database.pool(), &request).await?;
    let loaded = load_persisted_ens_verified_primary_name(database.pool(), &persisted.cache_key)
        .await?
        .expect("mismatch readback must exist");
    assert_eq!(
        loaded.provenance,
        expected_verified_primary_readback_provenance(&request)
    );
    assert_eq!(
        loaded.verified_primary_name,
        json!({
            "status": "mismatch",
            "name": verified_primary_name_ref("alice.eth"),
            "failure_reason": "resolved_target_mismatch"
        })
    );

    database.cleanup().await
}

#[tokio::test]
async fn persists_verified_primary_not_found_without_resolver_call() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = verified_primary_not_found_request();
    insert_primary_name_anchor(
        &database,
        "0x00000000000000000000000000000000000000ac",
        "60",
        PrimaryNameClaimStatus::NotFound,
    )
    .await?;

    let persisted = persist_ens_verified_primary_name(database.pool(), &request).await?;
    let loaded = load_persisted_ens_verified_primary_name(database.pool(), &persisted.cache_key)
        .await?
        .expect("not_found readback must exist");
    assert_eq!(
        loaded.provenance,
        expected_verified_primary_readback_provenance(&request)
    );
    assert_eq!(
        loaded.verified_primary_name,
        json!({ "status": "not_found" })
    );

    database.cleanup().await
}

#[tokio::test]
async fn persists_verified_primary_invalid_name_without_resolver_call() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = verified_primary_invalid_name_request();
    insert_primary_name_anchor(
        &database,
        "0x00000000000000000000000000000000000000ad",
        "60",
        PrimaryNameClaimStatus::InvalidName,
    )
    .await?;

    let persisted = persist_ens_verified_primary_name(database.pool(), &request).await?;
    let loaded = load_persisted_ens_verified_primary_name(database.pool(), &persisted.cache_key)
        .await?
        .expect("invalid_name readback must exist");
    assert_eq!(
        loaded.provenance,
        expected_verified_primary_readback_provenance(&request)
    );
    assert_eq!(
        loaded.verified_primary_name,
        json!({
            "status": "invalid_name",
            "failure_reason": "claim_name_not_normalizable"
        })
    );

    database.cleanup().await
}

#[tokio::test]
async fn persists_verified_primary_execution_failed_with_failure_payloads() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = verified_primary_execution_failed_request();
    insert_primary_name_anchor(
        &database,
        "0x00000000000000000000000000000000000000ae",
        "60",
        PrimaryNameClaimStatus::Success,
    )
    .await?;

    let persisted = persist_ens_verified_primary_name(database.pool(), &request).await?;
    let loaded = load_persisted_ens_verified_primary_name(database.pool(), &persisted.cache_key)
        .await?
        .expect("execution_failed readback must exist");
    assert_eq!(
        loaded.provenance,
        expected_verified_primary_readback_provenance(&request)
    );
    assert_eq!(
        loaded.verified_primary_name,
        json!({
            "status": "execution_failed",
            "failure_reason": "resolver_call_reverted"
        })
    );
    assert_eq!(loaded.trace.failure_payload, request.trace.failure_payload);
    assert_eq!(
        loaded.outcome.failure_payload,
        request.outcome.failure_payload
    );

    database.cleanup().await
}

#[tokio::test]
async fn verified_primary_readback_provenance_remains_execution_local() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut request = verified_primary_success_request();
    let metadata = request
        .trace
        .request_metadata
        .as_object_mut()
        .expect("verified-primary request_metadata must be an object");
    metadata.insert(
        "verified_primary_name_lookup".to_owned(),
        json!({
            "address": "0x00000000000000000000000000000000000000aa",
            "namespace": ENS_NAMESPACE,
            "coin_type": "60",
        }),
    );
    metadata.insert(
        "verified_primary_name_invalidation".to_owned(),
        json!({
            "claim_status": "success",
            "primary_claim_source": {
                "seed": "claim-side-only"
            },
        }),
    );
    insert_primary_name_anchor(
        &database,
        "0x00000000000000000000000000000000000000aa",
        "60",
        PrimaryNameClaimStatus::Success,
    )
    .await?;

    let persisted = persist_ens_verified_primary_name(database.pool(), &request).await?;
    let loaded = load_persisted_ens_verified_primary_name(database.pool(), &persisted.cache_key)
        .await?
        .expect("verified-primary readback must exist");
    assert_eq!(
        loaded.provenance,
        expected_verified_primary_readback_provenance(&request)
    );

    database.cleanup().await
}

#[tokio::test]
async fn rejects_verified_primary_without_primary_name_anchor() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = verified_primary_success_request();

    let error = persist_ens_verified_primary_name(database.pool(), &request)
        .await
        .expect_err("missing tuple anchor must be rejected");
    assert!(
        error
            .to_string()
            .contains("requires primary_names_current anchor"),
        "unexpected error: {error:#}"
    );
    assert!(
        load_execution_trace(database.pool(), request.trace.execution_trace_id)
            .await?
            .is_none(),
        "rejected verified-primary request must not persist trace rows"
    );
    assert!(
        load_execution_outcome(database.pool(), &request.outcome.cache_key)
            .await?
            .is_none(),
        "rejected verified-primary request must not persist outcome rows"
    );

    database.cleanup().await
}

#[tokio::test]
async fn verified_primary_readback_returns_none_when_anchor_is_missing() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = verified_primary_success_request();
    upsert_execution_trace(database.pool(), &request.trace).await?;
    upsert_execution_outcome(database.pool(), &request.outcome).await?;

    assert!(
        load_persisted_ens_verified_primary_name(database.pool(), &request.outcome.cache_key)
            .await?
            .is_none(),
        "readback must stay gated on primary_names_current tuple presence"
    );

    database.cleanup().await
}

#[test]
fn rejects_unnormalized_verified_primary_request_key() -> Result<()> {
    let mut request = verified_primary_success_request();
    request.trace.request_key = "ens:0x00000000000000000000000000000000000000AA:60".to_owned();
    request.outcome.cache_key.request_key = request.trace.request_key.clone();

    let error = validate_verified_primary_request(&request)
        .expect_err("unnormalized verified-primary request_key must be rejected");
    assert!(
        error
            .to_string()
            .contains("does not match expected ens:0x00000000000000000000000000000000000000aa:60"),
        "unexpected error: {error:#}"
    );

    Ok(())
}

#[tokio::test]
async fn rolls_back_verified_primary_trace_when_outcome_write_fails() -> Result<()> {
    let database = TestDatabase::new().await?;
    let request = verified_primary_success_request();
    insert_primary_name_anchor(
        &database,
        "0x00000000000000000000000000000000000000aa",
        "60",
        PrimaryNameClaimStatus::Success,
    )
    .await?;

    let mut conflicting_trace = request.trace.clone();
    conflicting_trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000026);
    conflicting_trace.request_type = VERIFIED_RESOLUTION_REQUEST_TYPE.to_owned();
    conflicting_trace.request_key = "ens:alice.eth:addr:60".to_owned();
    conflicting_trace.final_payload = Some(json!({
        "verified_queries": [{
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            }
        }]
    }));
    conflicting_trace.request_metadata = json!({
        "surface": "alice.eth",
        "record_key": "addr:60",
        "normalizer_version": "uts46-v1"
    });
    upsert_execution_trace(database.pool(), &conflicting_trace).await?;

    let mut conflicting_outcome = request.outcome.clone();
    conflicting_outcome.execution_trace_id = conflicting_trace.execution_trace_id;
    conflicting_outcome.request_type = conflicting_trace.request_type.clone();
    conflicting_outcome.namespace = "basenames".to_owned();
    conflicting_outcome.outcome_payload = Some(json!({
        "verified_queries": [{
            "record_key": "addr:60",
            "status": "success",
            "value": {
                "coin_type": "60",
                "value": "0x00000000000000000000000000000000000000aa"
            }
        }]
    }));
    upsert_execution_outcome(database.pool(), &conflicting_outcome).await?;

    let error = persist_ens_verified_primary_name(database.pool(), &request)
        .await
        .expect_err("conflicting cache identity must roll back verified-primary writes");
    assert!(
        error
            .to_string()
            .contains("execution outcome cache identity mismatch"),
        "unexpected error: {error:#}"
    );
    assert!(
        load_execution_trace(database.pool(), request.trace.execution_trace_id)
            .await?
            .is_none(),
        "failed verified-primary persistence must not leave a trace row behind"
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &request.outcome.cache_key).await?,
        Some(conflicting_outcome),
        "the pre-existing conflicting outcome must remain untouched"
    );

    database.cleanup().await
}
