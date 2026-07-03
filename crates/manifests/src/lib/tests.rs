use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use bigname_storage::default_database_url;
use sqlx::{
    PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
    query_scalar,
};
use uuid::Uuid;

use super::*;

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);
const TEST_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new() -> Result<Self> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "bigname-manifests-tests-{}-{unique}-{sequence}",
            std::process::id(),
        ));
        fs::create_dir_all(&path)
            .with_context(|| format!("failed to create test directory {}", path.display()))?;
        Ok(Self { path })
    }

    fn write_manifest(
        &self,
        namespace: &str,
        source_family: &str,
        version_tag: &str,
        contents: &str,
    ) -> Result<PathBuf> {
        let directory = self.path.join(namespace).join(source_family);
        fs::create_dir_all(&directory)
            .with_context(|| format!("failed to create {}", directory.display()))?;
        let path = directory.join(format!("{version_tag}.toml"));
        fs::write(&path, contents)
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(path)
    }

    fn write_manifest_for_chain_combo(
        &self,
        chain_combo: &str,
        namespace: &str,
        source_family: &str,
        version_tag: &str,
        contents: &str,
    ) -> Result<PathBuf> {
        let directory = self
            .path
            .join(chain_combo)
            .join(namespace)
            .join(source_family);
        fs::create_dir_all(&directory)
            .with_context(|| format!("failed to create {}", directory.display()))?;
        let path = directory.join(format!("{version_tag}.toml"));
        fs::write(&path, contents)
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

struct TestDatabase {
    admin_pool: PgPool,
    pool: PgPool,
    database_name: String,
}

impl TestDatabase {
    async fn new() -> Result<Self> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for manifest integration tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_manifests_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for manifest integration tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect manifest integration test pool")?;

        TEST_MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for manifest integration tests")?;

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

fn manifest_contents(
    rollout_status: &str,
    root_address: &str,
    contract_address: &str,
    implementation: Option<&str>,
) -> String {
    let implementation = implementation
        .map(|value| format!("implementation = \"{value}\"\n"))
        .unwrap_or_default();
    format!(
        r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v2_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v2"
rollout_status = "{rollout_status}"
normalizer_version = "ensip15@ens-normalize-0.1.1"

[capability_flags]
declared_children = "supported"

[[roots]]
name = "RootRegistry"
address = "{root_address}"

[[contracts]]
role = "registry"
address = "{contract_address}"
proxy_kind = "erc1967"
{implementation}

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"
"#
    )
}

fn start_block_manifest_contents(
    root_start_block: Option<i64>,
    contract_start_block: Option<i64>,
    omitted_contract_address: &str,
) -> String {
    let root_start_block = root_start_block
        .map(|start_block| format!("start_block = {start_block}\n"))
        .unwrap_or_default();
    let contract_start_block = contract_start_block
        .map(|start_block| format!("start_block = {start_block}\n"))
        .unwrap_or_default();
    format!(
        r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v2_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v2"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"

[capability_flags]
declared_children = "supported"

[[roots]]
name = "RootRegistry"
address = "0x0000000000000000000000000000000000000001"
{root_start_block}

[[contracts]]
role = "registry"
address = "0x0000000000000000000000000000000000000002"
proxy_kind = "none"
{contract_start_block}

[[contracts]]
role = "omitted_start"
address = "{omitted_contract_address}"
proxy_kind = "none"

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"
"#
    )
}

fn simple_contract_start_block_manifest_contents() -> String {
    r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v1_reverse_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v1"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"
roots = []
discovery_rules = []

[capability_flags]

[[contracts]]
role = "reverse_registrar"
address = "0x0000000000000000000000000000000000000042"
proxy_kind = "none"
start_block = 4242
"#
    .to_owned()
}

fn abi_manifest_contents() -> String {
    r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v2_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v2"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"

[capability_flags]
declared_children = "supported"

[[roots]]
name = "RootRegistry"
address = "0x0000000000000000000000000000000000000001"

[[contracts]]
role = "registry"
address = "0x0000000000000000000000000000000000000002"
proxy_kind = "none"

[[abi.events]]
name = "SubregistryUpdated"
fragment = "event SubregistryUpdated(uint256 indexed node, address registry, address sender)"
emitter_roles = ["registry"]
normalized_events = ["SubregistryChanged"]
status = "supported"
notes = "adapter-owned registry resource link input"

[[abi.calls]]
name = "resolver"
fragment = "function resolver(bytes32 node) view returns (address)"
target_roles = ["registry"]
status = "shadow"

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"
"#
    .to_owned()
}

fn registry_manifest_contents(rollout_status: &str) -> String {
    format!(
        r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v1_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v1"
rollout_status = "{rollout_status}"
normalizer_version = "ensip15@ens-normalize-0.1.1"

[capability_flags]
declared_children = {{ status = "supported", notes = "registry-controlled child surfaces are authoritative inputs" }}

[[roots]]
name = "ENSRegistry"
address = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E"

[[contracts]]
role = "registry"
address = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E"
proxy_kind = "none"

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"
"#
    )
}

fn execution_manifest_contents(rollout_status: &str) -> String {
    format!(
        r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_execution"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v1"
rollout_status = "{rollout_status}"
normalizer_version = "ensip15@ens-normalize-0.1.1"
roots = []
discovery_rules = []

[capability_flags]
verified_resolution = {{ status = "shadow", notes = "shadow execution traces and cache ownership are tracked before public verified-resolution reads ship" }}

[[contracts]]
role = "universal_resolver"
address = "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe"
proxy_kind = "none"
"#
    )
}

fn checked_in_manifest_contents(
    namespace: &str,
    source_family: &str,
    version_tag: &str,
) -> Result<String> {
    for chain_combo in ["ethereum", "base"] {
        let path = checked_in_manifest_root("manifests/mainnet")
            .join(chain_combo)
            .join(namespace)
            .join(source_family)
            .join(format!("{version_tag}.toml"));
        if path.exists() {
            return fs::read_to_string(&path)
                .with_context(|| format!("failed to read checked-in manifest {}", path.display()));
        }
    }

    bail!(
        "failed to find checked-in manifest {namespace}/{source_family}/{version_tag}.toml in mainnet chain-combo roots"
    );
}

fn checked_in_manifest_root(profile_root: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(profile_root)
}

async fn load_single_contract_instance_for_address(
    pool: &PgPool,
    chain: &str,
    address: &str,
) -> Result<Uuid> {
    query_scalar::<_, Uuid>(
        r#"
            SELECT contract_instance_id
            FROM contract_instance_addresses
            WHERE chain_id = $1
              AND address = $2
            ORDER BY (deactivated_at IS NULL) DESC, admitted_at DESC
            LIMIT 1
            "#,
    )
    .bind(chain)
    .bind(normalize_address(address))
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to load contract instance for {chain} {address}"))
}

async fn load_manifest_rollout_statuses(
    pool: &PgPool,
    namespace: &str,
) -> Result<Vec<(String, String)>> {
    let rows = sqlx::query(
        r#"
        SELECT source_family, rollout_status::TEXT AS rollout_status
        FROM manifest_versions
        WHERE namespace = $1
        ORDER BY source_family, chain, deployment_epoch, manifest_version
        "#,
    )
    .bind(namespace)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load manifest rollout statuses for {namespace}"))?;

    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get("source_family")
                    .context("failed to read source_family")?,
                row.try_get("rollout_status")
                    .context("failed to read rollout_status")?,
            ))
        })
        .collect()
}

async fn load_capability_flags_for_source_family(
    pool: &PgPool,
    namespace: &str,
    source_family: &str,
) -> Result<BTreeMap<String, CapabilityFlag>> {
    let rows = sqlx::query(
        r#"
        SELECT mcf.capability_name, mcf.status::TEXT AS status, mcf.notes
        FROM manifest_versions mv
        JOIN manifest_capability_flags mcf ON mcf.manifest_id = mv.manifest_id
        WHERE mv.namespace = $1
          AND mv.source_family = $2
        ORDER BY mcf.capability_name
        "#,
    )
    .bind(namespace)
    .bind(source_family)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load capability flags for {namespace}/{source_family}"))?;

    rows.into_iter()
        .map(|row| {
            let capability_name = row
                .try_get::<String, _>("capability_name")
                .context("failed to read capability_name")?;
            let status = row
                .try_get::<String, _>("status")
                .context("failed to read capability status")?;
            let notes = row.try_get("notes").context("failed to read notes")?;
            Ok((
                capability_name,
                CapabilityFlag {
                    status: CapabilitySupportStatus::from_db_value(&status)?,
                    notes,
                },
            ))
        })
        .collect()
}

async fn load_capability_flags_for_source_family_version(
    pool: &PgPool,
    namespace: &str,
    source_family: &str,
    manifest_version: i64,
) -> Result<BTreeMap<String, CapabilityFlag>> {
    let rows = sqlx::query(
        r#"
        SELECT mcf.capability_name, mcf.status::TEXT AS status, mcf.notes
        FROM manifest_versions mv
        JOIN manifest_capability_flags mcf ON mcf.manifest_id = mv.manifest_id
        WHERE mv.namespace = $1
          AND mv.source_family = $2
          AND mv.manifest_version = $3
        ORDER BY mcf.capability_name
        "#,
    )
    .bind(namespace)
    .bind(source_family)
    .bind(manifest_version)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load capability flags for {namespace}/{source_family} v{manifest_version}"
        )
    })?;

    rows.into_iter()
        .map(|row| {
            let capability_name = row
                .try_get::<String, _>("capability_name")
                .context("failed to read capability_name")?;
            let status = row
                .try_get::<String, _>("status")
                .context("failed to read capability status")?;
            let notes = row.try_get("notes").context("failed to read notes")?;
            Ok((
                capability_name,
                CapabilityFlag {
                    status: CapabilitySupportStatus::from_db_value(&status)?,
                    notes,
                },
            ))
        })
        .collect()
}

async fn active_manifest_id_for_source_family(
    pool: &PgPool,
    namespace: &str,
    source_family: &str,
) -> Result<i64> {
    query_scalar::<_, i64>(
        r#"
        SELECT manifest_id
        FROM manifest_versions
        WHERE namespace = $1
          AND source_family = $2
          AND rollout_status = 'active'
        ORDER BY manifest_version DESC
        LIMIT 1
        "#,
    )
    .bind(namespace)
    .bind(source_family)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to load active manifest_id for {namespace}/{source_family}"))
}

struct RawCodeHashObservation<'a> {
    chain: &'a str,
    block_hash: &'a str,
    block_number: i64,
    contract_address: &'a str,
    code_hash: &'a str,
    code_byte_length: i64,
    canonicality_state: &'a str,
}

async fn insert_raw_code_hash_observation(
    pool: &PgPool,
    observation: RawCodeHashObservation<'_>,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO raw_code_hashes (
            chain_id,
            block_hash,
            block_number,
            contract_address,
            code_hash,
            code_byte_length,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7::canonicality_state)
        "#,
    )
    .bind(observation.chain)
    .bind(observation.block_hash)
    .bind(observation.block_number)
    .bind(normalize_address(observation.contract_address))
    .bind(observation.code_hash)
    .bind(observation.code_byte_length)
    .bind(observation.canonicality_state)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to insert raw code hash for {}/{}",
            observation.chain, observation.contract_address
        )
    })?;

    Ok(())
}

async fn insert_manifest_normalized_event(
    pool: &PgPool,
    event_identity: &str,
    event_kind: &str,
    source_family: &str,
    manifest_version: i64,
    source_manifest_id: Option<i64>,
    canonicality_state: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            event_kind,
            source_family,
            manifest_version,
            source_manifest_id,
            raw_fact_ref,
            derivation_kind,
            canonicality_state,
            before_state,
            after_state
        )
        VALUES (
            $1,
            'ens',
            $2,
            $3,
            $4,
            $5,
            $6::jsonb,
            'manifest_sync',
            $7::canonicality_state,
            $8::jsonb,
            $9::jsonb
        )
        "#,
    )
    .bind(event_identity)
    .bind(event_kind)
    .bind(source_family)
    .bind(manifest_version)
    .bind(source_manifest_id)
    .bind(serde_json::json!({ "event_identity": event_identity }).to_string())
    .bind(canonicality_state)
    .bind(serde_json::json!({ "before": event_identity }).to_string())
    .bind(serde_json::json!({ "after": event_identity }).to_string())
    .execute(pool)
    .await
    .with_context(|| format!("failed to insert normalized event {event_identity}"))?;

    Ok(())
}

fn watched_contract_for_test(
    chain: &str,
    source_family: &str,
    address: &str,
    contract_instance_id: Uuid,
    source: WatchedContractSource,
    active_from_block_number: Option<i64>,
    active_to_block_number: Option<i64>,
) -> WatchedContract {
    WatchedContract {
        chain: chain.to_owned(),
        source_family: source_family.to_owned(),
        address: normalize_address(address),
        contract_instance_id,
        source,
        source_manifest_id: Some(1),
        active_from_block_number,
        active_to_block_number,
    }
}

#[test]
fn source_family_selector_filters_targets_and_builds_chain_plan() -> Result<()> {
    let registry_a = Uuid::from_u128(1);
    let registry_b = Uuid::from_u128(2);
    let registrar = Uuid::from_u128(3);
    let other_chain_registry = Uuid::from_u128(4);
    let watched_contracts = vec![
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v2_registrar_l1",
            "0x0000000000000000000000000000000000000002",
            registrar,
            WatchedContractSource::ManifestContract,
            None,
            None,
        ),
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v2_registry_l1",
            "0x0000000000000000000000000000000000000001",
            registry_a,
            WatchedContractSource::ManifestRoot,
            None,
            None,
        ),
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v2_registry_l1",
            "0x0000000000000000000000000000000000000003",
            registry_b,
            WatchedContractSource::DiscoveryEdge,
            Some(90),
            Some(150),
        ),
        watched_contract_for_test(
            "base-mainnet",
            "ens_v2_registry_l1",
            "0x0000000000000000000000000000000000000004",
            other_chain_registry,
            WatchedContractSource::ManifestContract,
            None,
            None,
        ),
    ];

    let plan = resolve_watched_source_selector(
        &watched_contracts,
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v2_registry_l1".to_owned()),
        100,
        120,
    )?;

    assert_eq!(plan.selector_kind, WatchedSourceSelectorKind::SourceFamily);
    assert_eq!(plan.source_family.as_deref(), Some("ens_v2_registry_l1"));
    assert_eq!(
        plan.watched_chain_plan,
        WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x0000000000000000000000000000000000000001".to_owned(),
                "0x0000000000000000000000000000000000000003".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 0,
            discovery_edge_entry_count: 1,
        }
    );
    assert_eq!(
        plan.selected_targets,
        vec![
            WatchedBackfillTarget {
                source_family: "ens_v2_registry_l1".to_owned(),
                contract_instance_id: registry_a,
                address: "0x0000000000000000000000000000000000000001".to_owned(),
                effective_from_block: 100,
                effective_to_block: 120,
            },
            WatchedBackfillTarget {
                source_family: "ens_v2_registry_l1".to_owned(),
                contract_instance_id: registry_b,
                address: "0x0000000000000000000000000000000000000003".to_owned(),
                effective_from_block: 100,
                effective_to_block: 120,
            },
        ]
    );

    Ok(())
}

#[test]
fn large_whole_active_source_identity_uses_compact_selected_target_digest() -> Result<()> {
    let selected_targets = (0..10_001)
        .map(|index| WatchedBackfillTarget {
            source_family: "basenames_base_registry".to_owned(),
            contract_instance_id: Uuid::from_u128(index as u128 + 1),
            address: format!("0x{index:040x}"),
            effective_from_block: index as i64,
            effective_to_block: index as i64 + 10,
        })
        .collect::<Vec<_>>();
    let plan = WatchedSourceSelectorPlan {
        chain: "base-mainnet".to_owned(),
        selector_kind: WatchedSourceSelectorKind::WholeActiveWatchedChain,
        source_family: None,
        requested_watched_targets: Vec::new(),
        selected_targets,
        watched_chain_plan: WatchedChainPlan {
            chain: "base-mainnet".to_owned(),
            addresses: Vec::new(),
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 0,
            discovery_edge_entry_count: 0,
        },
    };

    let payload = plan.source_identity_payload();

    assert_eq!(
        payload
            .get("source_identity_payload_format")
            .and_then(serde_json::Value::as_str),
        Some("selected_targets_digest_v1")
    );
    assert!(payload.get("selected_targets").is_none());
    assert_eq!(
        payload
            .get("selected_target_count")
            .and_then(serde_json::Value::as_u64),
        Some(plan.selected_targets.len() as u64)
    );
    assert!(
        payload
            .get("selected_targets_digest")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|digest| digest.starts_with("keccak256:0x"))
    );
    assert!(
        payload
            .get("source_identity_hash")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|digest| digest.starts_with("fnv1a64:"))
    );
    assert_eq!(plan.source_identity_payload(), payload);

    let mut drifted_plan = plan.clone();
    drifted_plan
        .selected_targets
        .last_mut()
        .expect("test plan has selected targets")
        .effective_to_block += 1;
    let drifted_payload = drifted_plan.source_identity_payload();
    assert_ne!(
        drifted_payload.get("selected_targets_digest"),
        payload.get("selected_targets_digest")
    );
    assert_ne!(
        drifted_payload.get("source_identity_hash"),
        payload.get("source_identity_hash")
    );

    Ok(())
}

#[test]
fn watched_selector_dynamic_resolver_backfill() -> Result<()> {
    let ens_resolver_a = Uuid::from_u128(30);
    let ens_resolver_b = Uuid::from_u128(10);
    let ens_closed_resolver = Uuid::from_u128(20);
    let ens_future_resolver = Uuid::from_u128(40);
    let ens_registry = Uuid::from_u128(50);
    let basenames_resolver = Uuid::from_u128(60);
    let watched_contracts = vec![
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v1_resolver_l1",
            "0x0000000000000000000000000000000000000030",
            ens_resolver_a,
            WatchedContractSource::DiscoveryEdge,
            Some(110),
            Some(180),
        ),
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v1_resolver_l1",
            "0x0000000000000000000000000000000000000010",
            ens_resolver_b,
            WatchedContractSource::DiscoveryEdge,
            Some(90),
            Some(150),
        ),
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v1_resolver_l1",
            "0x0000000000000000000000000000000000000020",
            ens_closed_resolver,
            WatchedContractSource::DiscoveryEdge,
            Some(10),
            Some(99),
        ),
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v1_resolver_l1",
            "0x0000000000000000000000000000000000000040",
            ens_future_resolver,
            WatchedContractSource::DiscoveryEdge,
            Some(181),
            None,
        ),
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v1_registry_l1",
            "0x0000000000000000000000000000000000000050",
            ens_registry,
            WatchedContractSource::ManifestRoot,
            None,
            None,
        ),
        watched_contract_for_test(
            "base-mainnet",
            "basenames_base_resolver",
            "0x0000000000000000000000000000000000000060",
            basenames_resolver,
            WatchedContractSource::DiscoveryEdge,
            Some(500),
            Some(700),
        ),
    ];

    let ens_plan = resolve_watched_source_selector(
        &watched_contracts,
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v1_resolver_l1".to_owned()),
        100,
        175,
    )?;
    assert_eq!(
        ens_plan.selected_targets,
        vec![
            WatchedBackfillTarget {
                source_family: "ens_v1_resolver_l1".to_owned(),
                contract_instance_id: ens_resolver_b,
                address: "0x0000000000000000000000000000000000000010".to_owned(),
                effective_from_block: 100,
                effective_to_block: 150,
            },
            WatchedBackfillTarget {
                source_family: "ens_v1_resolver_l1".to_owned(),
                contract_instance_id: ens_resolver_a,
                address: "0x0000000000000000000000000000000000000030".to_owned(),
                effective_from_block: 110,
                effective_to_block: 175,
            },
        ]
    );
    let mut sorted_targets = ens_plan.selected_targets.clone();
    sorted_targets.sort();
    assert_eq!(ens_plan.selected_targets, sorted_targets);

    let basenames_plan = resolve_watched_source_selector(
        &watched_contracts,
        "base-mainnet",
        WatchedSourceSelector::SourceFamily("basenames_base_resolver".to_owned()),
        600,
        650,
    )?;
    assert_eq!(
        basenames_plan.selected_targets,
        vec![WatchedBackfillTarget {
            source_family: "basenames_base_resolver".to_owned(),
            contract_instance_id: basenames_resolver,
            address: "0x0000000000000000000000000000000000000060".to_owned(),
            effective_from_block: 600,
            effective_to_block: 650,
        }]
    );

    let empty_family_error = resolve_watched_source_selector(
        &watched_contracts,
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily(String::new()),
        100,
        175,
    )
    .expect_err("empty source-family selector must fail before job creation");
    assert!(
        empty_family_error
            .to_string()
            .contains("source_family  found no active watched targets"),
        "unexpected empty source-family error: {empty_family_error:#}"
    );

    let unknown_family_error = resolve_watched_source_selector(
        &watched_contracts,
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("unknown_resolver_family".to_owned()),
        100,
        175,
    )
    .expect_err("unknown source-family selector must fail before job creation");
    assert!(
        unknown_family_error
            .to_string()
            .contains("source_family unknown_resolver_family found no active watched targets"),
        "unexpected unknown source-family error: {unknown_family_error:#}"
    );

    Ok(())
}

#[test]
fn watched_selector_preserves_duplicate_identity_effective_ranges() -> Result<()> {
    let resolver = Uuid::from_u128(70);
    let resolver_address = "0x0000000000000000000000000000000000000070";
    let watched_contracts = vec![
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v1_resolver_l1",
            resolver_address,
            resolver,
            WatchedContractSource::ManifestContract,
            None,
            None,
        ),
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v1_resolver_l1",
            resolver_address,
            resolver,
            WatchedContractSource::DiscoveryEdge,
            Some(123),
            None,
        ),
    ];

    let plan = resolve_watched_source_selector(
        &watched_contracts,
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v1_resolver_l1".to_owned()),
        100,
        200,
    )?;

    assert_eq!(
        plan.watched_chain_plan,
        WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![normalize_address(resolver_address)],
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 1,
        }
    );
    assert_eq!(
        plan.selected_targets,
        vec![
            WatchedBackfillTarget {
                source_family: "ens_v1_resolver_l1".to_owned(),
                contract_instance_id: resolver,
                address: normalize_address(resolver_address),
                effective_from_block: 100,
                effective_to_block: 200,
            },
            WatchedBackfillTarget {
                source_family: "ens_v1_resolver_l1".to_owned(),
                contract_instance_id: resolver,
                address: normalize_address(resolver_address),
                effective_from_block: 123,
                effective_to_block: 200,
            },
        ]
    );

    Ok(())
}

#[test]
fn explicit_watched_target_set_is_normalized_sorted_and_validated() -> Result<()> {
    let registry = Uuid::from_u128(30);
    let registrar = Uuid::from_u128(10);
    let resolver = Uuid::from_u128(20);
    let watched_contracts = vec![
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v2_registry_l1",
            "0x0000000000000000000000000000000000000030",
            registry,
            WatchedContractSource::ManifestContract,
            Some(25),
            Some(125),
        ),
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v2_registrar_l1",
            "0x0000000000000000000000000000000000000010",
            registrar,
            WatchedContractSource::ManifestContract,
            None,
            None,
        ),
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v2_resolver_l1",
            "0x0000000000000000000000000000000000000020",
            resolver,
            WatchedContractSource::DiscoveryEdge,
            Some(110),
            Some(190),
        ),
    ];

    let plan = resolve_watched_source_selector(
        &watched_contracts,
        "ethereum-mainnet",
        WatchedSourceSelector::WatchedTargetSet(vec![
            registry.into(),
            registrar.into(),
            registry.into(),
        ]),
        100,
        150,
    )?;

    assert_eq!(
        plan.requested_watched_targets,
        vec![
            WatchedTargetIdentity {
                contract_instance_id: registrar,
            },
            WatchedTargetIdentity {
                contract_instance_id: registry,
            },
        ]
    );
    assert_eq!(
        plan.selected_targets,
        vec![
            WatchedBackfillTarget {
                source_family: "ens_v2_registrar_l1".to_owned(),
                contract_instance_id: registrar,
                address: "0x0000000000000000000000000000000000000010".to_owned(),
                effective_from_block: 100,
                effective_to_block: 150,
            },
            WatchedBackfillTarget {
                source_family: "ens_v2_registry_l1".to_owned(),
                contract_instance_id: registry,
                address: "0x0000000000000000000000000000000000000030".to_owned(),
                effective_from_block: 100,
                effective_to_block: 125,
            },
        ]
    );
    assert_eq!(
        plan.source_identity_payload()["selector_kind"],
        "watched_target_set"
    );

    let error = resolve_watched_source_selector(
        &watched_contracts,
        "ethereum-mainnet",
        WatchedSourceSelector::WatchedTargetSet(vec![Uuid::from_u128(99).into()]),
        100,
        150,
    )
    .expect_err("unknown explicit watched target must fail");
    assert!(
        error
            .to_string()
            .contains("is not active for chain ethereum-mainnet"),
        "unexpected explicit target validation error: {error:#}"
    );

    Ok(())
}

#[test]
fn watched_selector_validation_prevents_cross_chain_leakage() {
    let registry = Uuid::from_u128(40);
    let watched_contracts = vec![watched_contract_for_test(
        "ethereum-sepolia",
        "ens_v2_registry_l1",
        "0x0000000000000000000000000000000000000040",
        registry,
        WatchedContractSource::ManifestContract,
        None,
        None,
    )];

    let family_error = resolve_watched_source_selector(
        &watched_contracts,
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v2_registry_l1".to_owned()),
        1,
        10,
    )
    .expect_err("source-family selector must not leak targets from another chain");
    assert!(
        family_error
            .to_string()
            .contains("found no active watched targets for chain ethereum-mainnet"),
        "unexpected source-family validation error: {family_error:#}"
    );

    let target_error = resolve_watched_source_selector(
        &watched_contracts,
        "ethereum-mainnet",
        WatchedSourceSelector::WatchedTargetSet(vec![registry.into()]),
        1,
        10,
    )
    .expect_err("explicit selector must not leak targets from another chain");
    assert!(
        target_error
            .to_string()
            .contains("is not active for chain ethereum-mainnet"),
        "unexpected explicit target validation error: {target_error:#}"
    );
}

#[test]
fn watched_selector_rejects_conflicting_target_metadata() {
    let registry = Uuid::from_u128(50);
    let watched_contracts = vec![
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v2_registry_l1",
            "0x0000000000000000000000000000000000000050",
            registry,
            WatchedContractSource::ManifestRoot,
            None,
            None,
        ),
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v2_registry_l1",
            "0x0000000000000000000000000000000000000051",
            registry,
            WatchedContractSource::ManifestContract,
            None,
            None,
        ),
    ];

    let error = resolve_watched_source_selector(
        &watched_contracts,
        "ethereum-mainnet",
        WatchedSourceSelector::WholeActiveWatchedChain,
        1,
        10,
    )
    .expect_err("conflicting metadata for one target identity must fail");
    assert!(
        error.to_string().contains("source identity conflict"),
        "unexpected conflict error: {error:#}"
    );
}

#[test]
fn reports_missing_root() -> Result<()> {
    let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "bigname-manifests-missing-{}-{}-{sequence}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos()
    ));

    let repository = load_repository(&root)?;

    assert_eq!(repository.summary().status, ManifestLoadStatus::MissingRoot);
    assert!(repository.manifests().is_empty());

    Ok(())
}

#[test]
fn loads_valid_repository_manifest() -> Result<()> {
    let test_dir = TestDir::new()?;
    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x00000000000000000000000000000000000000AA",
            Some("0x00000000000000000000000000000000000000DD"),
        ),
    )?;

    let repository = load_repository(&test_dir.path)?;

    assert_eq!(repository.summary().status, ManifestLoadStatus::Loaded);
    assert_eq!(repository.summary().namespace_count, 1);
    assert_eq!(repository.summary().source_family_count, 1);
    assert_eq!(repository.summary().manifest_count, 1);
    assert_eq!(repository.manifests().len(), 1);
    assert_eq!(repository.manifests()[0].version_tag, "v1");
    assert_eq!(repository.manifests()[0].manifest.namespace, "ens");
    assert!(repository.manifests()[0].manifest.abi.is_empty());

    Ok(())
}

#[test]
fn loads_chain_combo_repository_manifests() -> Result<()> {
    let test_dir = TestDir::new()?;
    test_dir.write_manifest_for_chain_combo(
        "ethereum",
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x00000000000000000000000000000000000000AA",
            Some("0x00000000000000000000000000000000000000DD"),
        ),
    )?;

    let repository = load_repository(&test_dir.path)?;

    assert_eq!(repository.summary().status, ManifestLoadStatus::Loaded);
    assert_eq!(repository.summary().namespace_count, 1);
    assert_eq!(repository.summary().source_family_count, 1);
    assert_eq!(repository.summary().manifest_count, 1);
    assert_eq!(
        repository.manifests()[0].relative_path,
        PathBuf::from("ethereum/ens/ens_v2_registry_l1/v1.toml")
    );

    Ok(())
}

#[test]
fn parses_optional_start_block_on_roots_and_contracts() -> Result<()> {
    let test_dir = TestDir::new()?;
    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &start_block_manifest_contents(
            Some(12_345),
            Some(23_456),
            "0x0000000000000000000000000000000000000003",
        ),
    )?;

    let repository = load_repository(&test_dir.path)?;
    let manifest = &repository.manifests()[0].manifest;

    assert_eq!(manifest.roots[0].start_block, Some(12_345));
    assert_eq!(manifest.contracts[0].start_block, Some(23_456));
    assert_eq!(manifest.contracts[1].start_block, None);

    Ok(())
}

#[test]
fn loads_manifest_abi_fragments() -> Result<()> {
    let test_dir = TestDir::new()?;
    test_dir.write_manifest("ens", "ens_v2_registry_l1", "v1", &abi_manifest_contents())?;

    let repository = load_repository(&test_dir.path)?;
    let abi = &repository.manifests()[0].manifest.abi;

    assert_eq!(abi.events.len(), 1);
    assert_eq!(abi.events[0].name, "SubregistryUpdated");
    assert_eq!(abi.events[0].emitter_roles, ["registry"]);
    assert_eq!(abi.events[0].normalized_events, ["SubregistryChanged"]);
    assert_eq!(
        abi.events[0].status,
        Some(CapabilitySupportStatus::Supported)
    );
    assert_eq!(abi.calls.len(), 1);
    assert_eq!(abi.calls[0].name, "resolver");
    assert_eq!(abi.calls[0].target_roles, ["registry"]);
    assert_eq!(abi.calls[0].status, Some(CapabilitySupportStatus::Shadow));
    let parsed_event = abi.events[0].parsed_event_view()?;
    assert_eq!(
        parsed_event.canonical_signature(),
        "SubregistryUpdated(uint256,address,address)"
    );
    assert!(
        parsed_event
            .topic0()
            .is_some_and(|topic0| topic0.starts_with("0x") && topic0.len() == 66)
    );
    let parsed_call = abi.calls[0].parsed_function_view()?;
    assert_eq!(parsed_call.canonical_signature(), "resolver(bytes32)");
    assert_eq!(parsed_call.selector().len(), 10);

    Ok(())
}

#[test]
fn normalize_address_uses_alloy_for_standard_hex_without_tightening_fallbacks() {
    assert_eq!(
        normalize_address("0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E"),
        "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e"
    );
    assert_eq!(normalize_address("NOT-A-HEX-ADDRESS"), "not-a-hex-address");
    assert_eq!(normalize_address("0xABC"), "0xabc");
    assert_eq!(
        normalize_address("00000000000000000000000000000000000000AA"),
        "00000000000000000000000000000000000000aa"
    );
}

#[test]
fn rejects_invalid_manifest_abi_fragment() -> Result<()> {
    let test_dir = TestDir::new()?;
    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &abi_manifest_contents().replacen("event SubregistryUpdated", "SubregistryUpdated", 1),
    )?;

    let error =
        load_repository(&test_dir.path).expect_err("non-event ABI fragment must fail validation");
    assert!(
        error.to_string().contains("must use an event fragment"),
        "unexpected error: {error:#}"
    );

    Ok(())
}

#[test]
fn rejects_manifest_abi_unknown_roles() -> Result<()> {
    let test_dir = TestDir::new()?;
    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &abi_manifest_contents().replacen(
            r#"emitter_roles = ["registry"]"#,
            r#"emitter_roles = ["missing_registry"]"#,
            1,
        ),
    )?;

    let error =
        load_repository(&test_dir.path).expect_err("unknown ABI emitter role must fail validation");
    assert!(
        error.to_string().contains("unknown emitter role"),
        "unexpected error: {error:#}"
    );

    Ok(())
}

#[test]
fn rejects_negative_start_block_values() -> Result<()> {
    let test_dir = TestDir::new()?;
    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &start_block_manifest_contents(
            Some(-1),
            Some(23_456),
            "0x0000000000000000000000000000000000000003",
        ),
    )?;

    let error = load_repository(&test_dir.path)
        .expect_err("negative start_block must fail manifest parsing");
    assert!(
        error.to_string().contains("failed to parse manifest TOML"),
        "unexpected error: {error:#}"
    );
    assert!(
        format!("{error:#}").contains("start_block must be a non-negative integer"),
        "unexpected error: {error:#}"
    );

    Ok(())
}

#[test]
fn rejects_unsupported_authored_discovery_rule_admission_literals() -> Result<()> {
    let test_dir = TestDir::new()?;
    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x0000000000000000000000000000000000000002",
            Some("0x0000000000000000000000000000000000000003"),
        )
        .replacen(
            "admission = \"reachable_from_root\"",
            "admission = \"manifest_declared\"",
            1,
        ),
    )?;

    let error = load_repository(&test_dir.path)
        .expect_err("unsupported discovery_rules[].admission must fail manifest parsing");
    assert!(
        error.to_string().contains("failed to parse manifest TOML"),
        "unexpected error: {error:#}"
    );
    assert!(
        format!("{error:#}")
            .contains("unsupported authored discovery_rules[].admission \"manifest_declared\""),
        "unexpected error: {error:#}"
    );

    Ok(())
}

#[test]
fn rejects_namespace_mismatch() -> Result<()> {
    let test_dir = TestDir::new()?;
    let path = test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        r#"
manifest_version = 1
namespace = "basenames"
source_family = "ens_v2_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v2"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"

[capability_flags]
declared_children = "supported"

[[roots]]
name = "RootRegistry"
address = "0x0000000000000000000000000000000000000000"

[[contracts]]
role = "registry"
address = "0x0000000000000000000000000000000000000000"
proxy_kind = "none"

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"
"#,
    )?;

    let error = load_repository(&test_dir.path).expect_err("namespace mismatch must fail");
    assert!(
        error.to_string().contains("does not match directory"),
        "unexpected error for {}: {error:#}",
        path.display()
    );

    Ok(())
}

#[test]
fn rejects_chain_combo_mismatch() -> Result<()> {
    let test_dir = TestDir::new()?;
    let path = test_dir.write_manifest_for_chain_combo(
        "base",
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x00000000000000000000000000000000000000AA",
            Some("0x00000000000000000000000000000000000000DD"),
        ),
    )?;

    let error = load_repository(&test_dir.path).expect_err("chain combo mismatch must fail");
    assert!(
        error.to_string().contains("does not match chain directory"),
        "unexpected error for {}: {error:#}",
        path.display()
    );

    Ok(())
}

#[test]
fn rejects_manifest_version_tag_mismatch() -> Result<()> {
    let test_dir = TestDir::new()?;
    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v2",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x00000000000000000000000000000000000000AA",
            Some("0x00000000000000000000000000000000000000DD"),
        ),
    )?;

    let error = load_repository(&test_dir.path).expect_err("version tag mismatch must fail");
    assert!(
        error
            .to_string()
            .contains("manifest_version 1 does not match version tag v2"),
        "unexpected error: {error:#}"
    );

    Ok(())
}

#[test]
fn rejects_duplicate_contract_roles() -> Result<()> {
    let test_dir = TestDir::new()?;
    let contents = manifest_contents(
        "active",
        "0x0000000000000000000000000000000000000001",
        "0x00000000000000000000000000000000000000AA",
        Some("0x00000000000000000000000000000000000000DD"),
    ) + r#"
[[contracts]]
role = "registry"
address = "0x00000000000000000000000000000000000000BB"
proxy_kind = "none"
"#;
    test_dir.write_manifest("ens", "ens_v2_registry_l1", "v1", &contents)?;

    let error = load_repository(&test_dir.path).expect_err("duplicate roles must fail");
    assert!(
        error
            .to_string()
            .contains("duplicates contract role registry"),
        "unexpected error: {error:#}"
    );

    Ok(())
}

#[test]
fn rejects_unsupported_normalizer_version() -> Result<()> {
    let test_dir = TestDir::new()?;
    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x00000000000000000000000000000000000000AA",
            Some("0x00000000000000000000000000000000000000DD"),
        )
        .replacen(
            "normalizer_version = \"ensip15@ens-normalize-0.1.1\"",
            "normalizer_version = \"ensip15@unknown\"",
            1,
        ),
    )?;

    let error = load_repository(&test_dir.path).expect_err("unsupported normalizer must fail");
    assert!(
        error
            .to_string()
            .contains("unsupported normalizer_version ensip15@unknown"),
        "unexpected error: {error:#}"
    );

    Ok(())
}

#[test]
fn checked_in_sepolia_manifests_load_as_alternate_profile() -> Result<()> {
    let main_repository = load_repository(checked_in_manifest_root("manifests/mainnet"))?;
    let sepolia_repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;

    assert_eq!(
        sepolia_repository.summary().status,
        ManifestLoadStatus::Loaded
    );
    assert_eq!(sepolia_repository.summary().namespace_count, 1);
    assert_eq!(sepolia_repository.summary().source_family_count, 4);
    assert_eq!(sepolia_repository.summary().manifest_count, 5);

    let sepolia_source_versions = sepolia_repository
        .manifests()
        .iter()
        .map(|loaded_manifest| {
            (
                loaded_manifest.manifest.source_family.as_str(),
                loaded_manifest.version_tag.as_str(),
                loaded_manifest.manifest.manifest_version,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        sepolia_source_versions,
        vec![
            ("ens_v2_registrar_l1", "v1", 1),
            ("ens_v2_registrar_l1", "v2", 2),
            ("ens_v2_registry_l1", "v1", 1),
            ("ens_v2_resolver_l1", "v1", 1),
            ("ens_v2_root_l1", "v1", 1),
        ]
    );
    assert!(!main_repository.manifests().iter().any(|loaded_manifest| {
        loaded_manifest
            .relative_path
            .starts_with("ethereum/ens/ens_v2_root_l1")
    }));
    assert!(
        !sepolia_repository
            .manifests()
            .iter()
            .any(|loaded_manifest| {
                loaded_manifest
                    .relative_path
                    .starts_with("ethereum/ens/ens_v1_registry_l1")
            })
    );

    for loaded_manifest in sepolia_repository.manifests() {
        assert_eq!(loaded_manifest.manifest.namespace, "ens");
        assert_eq!(loaded_manifest.manifest.chain, "ethereum-sepolia");
        assert_eq!(
            loaded_manifest.manifest.deployment_epoch,
            "ens_v2_sepolia_dev"
        );
        if loaded_manifest.manifest.source_family == "ens_v2_registrar_l1"
            && loaded_manifest.manifest.manifest_version == 1
        {
            assert_eq!(
                loaded_manifest.manifest.rollout_status,
                RolloutStatus::Deprecated
            );
        } else {
            assert_eq!(
                loaded_manifest.manifest.rollout_status,
                RolloutStatus::Active
            );
        }
    }

    let manifests_by_source_family_version = sepolia_repository
        .manifests()
        .iter()
        .map(|loaded_manifest| {
            (
                (
                    loaded_manifest.manifest.source_family.as_str(),
                    loaded_manifest.manifest.manifest_version,
                ),
                &loaded_manifest.manifest,
            )
        })
        .collect::<BTreeMap<_, _>>();

    let root_manifest = manifests_by_source_family_version[&("ens_v2_root_l1", 1)];
    assert_eq!(root_manifest.roots.len(), 1);
    assert_eq!(root_manifest.roots[0].name, "RootRegistry");
    assert_eq!(
        normalize_address(&root_manifest.roots[0].address),
        "0x3a3e15a5d27ff6f05c844313312f2e72096d3ed3"
    );
    assert_eq!(root_manifest.contracts.len(), 1);
    assert_eq!(root_manifest.contracts[0].role, "root_registry");
    assert_eq!(
        normalize_address(&root_manifest.contracts[0].address),
        "0x3a3e15a5d27ff6f05c844313312f2e72096d3ed3"
    );

    let registry_manifest = manifests_by_source_family_version[&("ens_v2_registry_l1", 1)];
    assert_eq!(registry_manifest.roots.len(), 1);
    assert_eq!(registry_manifest.roots[0].name, "ETHRegistry");
    assert_eq!(
        normalize_address(&registry_manifest.roots[0].address),
        "0x796fff2e907449be8d5921bcc215b1b76d89d080"
    );
    assert_eq!(registry_manifest.contracts.len(), 1);
    assert_eq!(registry_manifest.contracts[0].role, "registry");
    assert_eq!(
        normalize_address(&registry_manifest.contracts[0].address),
        "0x796fff2e907449be8d5921bcc215b1b76d89d080"
    );
    assert_eq!(
        registry_manifest
            .abi
            .events
            .iter()
            .map(|event| event.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "LabelRegistered",
            "LabelReserved",
            "LabelUnregistered",
            "ExpiryUpdated",
            "SubregistryUpdated",
            "ResolverUpdated",
            "TokenResource",
            "EACRolesChanged",
            "TokenRegenerated",
            "ParentUpdated",
        ]
    );

    let registrar_manifest_v1 = manifests_by_source_family_version[&("ens_v2_registrar_l1", 1)];
    assert_eq!(
        registrar_manifest_v1.rollout_status,
        RolloutStatus::Deprecated
    );
    assert_eq!(
        registrar_manifest_v1.capability_flags["exact_name_profile"].status,
        CapabilitySupportStatus::Shadow
    );

    let registrar_manifest = manifests_by_source_family_version[&("ens_v2_registrar_l1", 2)];
    assert_eq!(registrar_manifest.roots.len(), 1);
    assert_eq!(registrar_manifest.roots[0].name, "ETHRegistrar");
    assert_eq!(
        normalize_address(&registrar_manifest.roots[0].address),
        "0x68586418353b771cf2425ed14a07512aa880c532"
    );
    assert_eq!(registrar_manifest.contracts.len(), 1);
    assert_eq!(registrar_manifest.contracts[0].role, "registrar");
    assert_eq!(
        normalize_address(&registrar_manifest.contracts[0].address),
        "0x68586418353b771cf2425ed14a07512aa880c532"
    );
    assert_eq!(
        registrar_manifest.capability_flags["exact_name_profile"].status,
        CapabilitySupportStatus::Supported
    );
    assert_eq!(
        registrar_manifest
            .abi
            .events
            .iter()
            .map(|event| event.name.as_str())
            .collect::<Vec<_>>(),
        vec!["NameRegistered", "NameRenewed"]
    );

    let resolver_manifest = manifests_by_source_family_version[&("ens_v2_resolver_l1", 1)];
    assert!(resolver_manifest.roots.is_empty());
    assert!(resolver_manifest.contracts.is_empty());
    assert!(resolver_manifest.discovery_rules.is_empty());
    assert_eq!(
        resolver_manifest
            .abi
            .events
            .iter()
            .map(|event| event.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "AddressChanged",
            "TextChanged",
            "ContenthashChanged",
            "NameChanged",
            "VersionChanged",
            "AliasChanged",
            "NamedResource",
            "NamedTextResource",
            "NamedAddrResource",
            "EACRolesChanged",
        ]
    );

    let admitted_addresses = sepolia_repository
        .manifests()
        .iter()
        .flat_map(|loaded_manifest| {
            loaded_manifest
                .manifest
                .roots
                .iter()
                .map(|root| root.address.as_str())
                .chain(
                    loaded_manifest
                        .manifest
                        .contracts
                        .iter()
                        .map(|contract| contract.address.as_str()),
                )
        })
        .map(normalize_address)
        .collect::<Vec<_>>();
    assert!(!admitted_addresses.contains(&"0xe566a1fbaf30ff7c39828fe99f955fc55544cb9c".to_owned()));

    Ok(())
}

#[tokio::test]
async fn syncing_sepolia_profile_replaces_main_profile_without_mixing() -> Result<()> {
    let database = TestDatabase::new().await?;
    let main_repository = load_repository(checked_in_manifest_root("manifests/mainnet"))?;
    let sepolia_repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;

    assert_eq!(main_repository.summary().status, ManifestLoadStatus::Loaded);
    assert_eq!(
        sepolia_repository.summary().status,
        ManifestLoadStatus::Loaded
    );
    sync_repository(database.pool(), &main_repository).await?;

    let summary = sync_repository(database.pool(), &sepolia_repository).await?;
    assert_eq!(summary.status, ManifestSyncStatus::Synced);
    assert_eq!(summary.synced_manifest_count, 5);
    assert_eq!(summary.active_manifest_count, 4);
    assert_eq!(summary.root_count, 4);
    assert_eq!(summary.contract_count, 4);
    assert_eq!(summary.capability_count, 5);
    assert_eq!(summary.discovery_rule_count, 3);
    assert_eq!(
        summary.removed_manifest_count,
        main_repository.manifests().len()
    );

    assert_eq!(
        load_manifest_rollout_statuses(database.pool(), "ens").await?,
        vec![
            ("ens_v2_registrar_l1".to_owned(), "deprecated".to_owned()),
            ("ens_v2_registrar_l1".to_owned(), "active".to_owned()),
            ("ens_v2_registry_l1".to_owned(), "active".to_owned()),
            ("ens_v2_resolver_l1".to_owned(), "active".to_owned()),
            ("ens_v2_root_l1".to_owned(), "active".to_owned()),
        ]
    );
    assert_eq!(
        load_capability_flags_for_source_family(database.pool(), "ens", "ens_v2_registry_l1")
            .await?,
        BTreeMap::from([(
            "declared_children".to_owned(),
            CapabilityFlag {
                status: CapabilitySupportStatus::Supported,
                notes: Some(
                    "sepolia-dev registry and discovered user registries are authoritative declared child inputs within the selected profile"
                        .to_owned(),
                ),
            },
        )])
    );
    assert_eq!(
        load_capability_flags_for_source_family_version(
            database.pool(),
            "ens",
            "ens_v2_registrar_l1",
            1
        )
        .await?,
        BTreeMap::from([
            (
                "exact_name_profile".to_owned(),
                CapabilityFlag {
                    status: CapabilitySupportStatus::Shadow,
                    notes: Some(
                        "sepolia-dev registrar lifecycle facts are admitted before product reads depend on them"
                            .to_owned(),
                    ),
                },
            ),
            (
                "name_history".to_owned(),
                CapabilityFlag {
                    status: CapabilitySupportStatus::Shadow,
                    notes: Some("sepolia-dev registrar history remains downstream work".to_owned()),
                },
            ),
        ])
    );
    assert_eq!(
        load_capability_flags_for_source_family_version(
            database.pool(),
            "ens",
            "ens_v2_registrar_l1",
            2
        )
        .await?,
        BTreeMap::from([
            (
                "exact_name_profile".to_owned(),
                CapabilityFlag {
                    status: CapabilitySupportStatus::Supported,
                    notes: Some(
                        "selected sepolia-dev exact-name profile reads are supported from admitted ETHRegistry and ETHRegistrar sources only"
                            .to_owned(),
                    ),
                },
            ),
            (
                "name_history".to_owned(),
                CapabilityFlag {
                    status: CapabilitySupportStatus::Shadow,
                    notes: Some("sepolia-dev registrar history remains downstream work".to_owned()),
                },
            ),
        ])
    );
    assert_eq!(
        load_capability_flags_for_source_family(database.pool(), "ens", "ens_v2_root_l1").await?,
        BTreeMap::new()
    );
    assert_eq!(
        load_capability_flags_for_source_family(database.pool(), "ens", "ens_v2_resolver_l1")
            .await?,
        BTreeMap::new()
    );
    assert!(
        load_manifest_rollout_statuses(database.pool(), "basenames")
            .await?
            .is_empty()
    );

    let active_manifests = load_active_manifests_for_namespace(database.pool(), "ens").await?;
    assert_eq!(active_manifests.len(), 4);
    assert!(
        active_manifests
            .iter()
            .all(|manifest| manifest.chain == "ethereum-sepolia")
    );
    assert!(
        active_manifests
            .iter()
            .all(|manifest| manifest.deployment_epoch == "ens_v2_sepolia_dev")
    );
    assert!(
        !active_manifests
            .iter()
            .any(|manifest| manifest.source_family.starts_with("ens_v1_"))
    );
    assert!(active_manifests.iter().any(|manifest| {
        manifest.source_family == "ens_v2_registrar_l1"
            && manifest.manifest_version == 2
            && manifest.capability_flags["exact_name_profile"].status
                == CapabilitySupportStatus::Supported
    }));

    let watched_contracts = load_watched_contracts(database.pool()).await?;
    assert!(
        watched_contracts
            .iter()
            .all(|contract| contract.chain == "ethereum-sepolia")
    );
    assert!(!watched_contracts.iter().any(|contract| {
        contract.address == normalize_address("0xe566a1fbaf30ff7c39828fe99f955fc55544cb9c")
    }));
    assert!(!watched_contracts.iter().any(|contract| {
        contract.address == normalize_address("0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E")
    }));

    assert_eq!(
        load_watched_chain_plan(database.pool()).await?,
        vec![WatchedChainPlan {
            chain: "ethereum-sepolia".to_owned(),
            addresses: vec![
                "0x3a3e15a5d27ff6f05c844313312f2e72096d3ed3".to_owned(),
                "0x68586418353b771cf2425ed14a07512aa880c532".to_owned(),
                "0x796fff2e907449be8d5921bcc215b1b76d89d080".to_owned(),
            ],
            manifest_root_entry_count: 3,
            manifest_contract_entry_count: 3,
            discovery_edge_entry_count: 0,
        }]
    );

    let watched_summary = load_watched_contract_summary(database.pool()).await?;
    assert_eq!(watched_summary.unique_contract_count, 3);
    assert_eq!(watched_summary.source_entry_count, 6);
    assert_eq!(watched_summary.manifest_root_count, 3);
    assert_eq!(watched_summary.manifest_contract_count, 3);
    assert_eq!(watched_summary.discovery_edge_count, 0);

    let admission_state = load_discovery_admission_state(database.pool()).await?;
    assert_eq!(admission_state.active_manifest_count, 4);
    assert_eq!(admission_state.active_root_count, 3);
    assert_eq!(admission_state.active_contract_count, 3);
    assert_eq!(admission_state.active_rule_count, 3);
    assert!(admission_state.has_authoritative_address(
        "ethereum-sepolia",
        "0x3a3e15a5d27ff6f05c844313312f2e72096d3ed3"
    ));
    assert!(admission_state.has_authoritative_address(
        "ethereum-sepolia",
        "0x796fff2e907449be8d5921bcc215b1b76d89d080"
    ));
    assert!(admission_state.has_authoritative_address(
        "ethereum-sepolia",
        "0x68586418353b771cf2425ed14a07512aa880c532"
    ));
    assert!(!admission_state.has_authoritative_address(
        "ethereum-sepolia",
        "0xe566a1fbaf30ff7c39828fe99f955fc55544cb9c"
    ));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn active_manifest_abi_events_derive_topics_from_payload() -> Result<()> {
    let database = TestDatabase::new().await?;
    let sepolia_repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &sepolia_repository).await?;

    let registry_manifest_id =
        active_manifest_id_for_source_family(database.pool(), "ens", "ens_v2_registry_l1").await?;
    let events = load_active_manifest_abi_events(database.pool(), &[registry_manifest_id]).await?;

    assert_eq!(events.len(), 10);
    let label_registered = events
        .iter()
        .find(|event| event.name == "LabelRegistered")
        .expect("registry manifest must declare LabelRegistered ABI");
    assert_eq!(label_registered.manifest_id, registry_manifest_id);
    assert_eq!(label_registered.source_family, "ens_v2_registry_l1");
    assert_eq!(
        label_registered.canonical_signature,
        "LabelRegistered(uint256,bytes32,string,address,uint64,address)"
    );
    assert!(
        label_registered
            .topic0
            .as_ref()
            .is_some_and(|topic0| topic0.starts_with("0x") && topic0.len() == 66)
    );
    assert_eq!(label_registered.emitter_roles, ["registry"]);
    assert_eq!(label_registered.normalized_events, ["RegistrationGranted"]);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn syncs_start_blocks_into_watch_plan_and_bootstrap_targets() -> Result<()> {
    let database = TestDatabase::new().await?;
    let test_dir = TestDir::new()?;
    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &start_block_manifest_contents(
            Some(120),
            Some(100),
            "0x0000000000000000000000000000000000000003",
        ),
    )?;
    let repository = load_repository(&test_dir.path)?;

    sync_repository(database.pool(), &repository).await?;
    sqlx::query(
        r#"
        UPDATE contract_instance_addresses
        SET active_to_block_number = 160
        WHERE chain_id = 'ethereum-mainnet'
          AND address = $1
        "#,
    )
    .bind("0x0000000000000000000000000000000000000002")
    .execute(database.pool())
    .await
    .context("failed to constrain registry active range")?;

    let watched_contracts = load_watched_contracts(database.pool()).await?;
    let root = watched_contracts
        .iter()
        .find(|contract| contract.address == "0x0000000000000000000000000000000000000001")
        .expect("root target must be watched");
    let registry = watched_contracts
        .iter()
        .find(|contract| contract.address == "0x0000000000000000000000000000000000000002")
        .expect("registry target must be watched");
    let unknown_start = watched_contracts
        .iter()
        .find(|contract| contract.address == "0x0000000000000000000000000000000000000003")
        .expect("unknown-start target must be watched");

    assert_eq!(root.active_from_block_number, Some(120));
    assert_eq!(root.active_to_block_number, None);
    assert_eq!(registry.active_from_block_number, Some(100));
    assert_eq!(registry.active_to_block_number, Some(160));
    assert_eq!(unknown_start.active_from_block_number, None);
    assert_eq!(unknown_start.active_to_block_number, None);

    let stored_registry_start = query_scalar::<_, Option<i64>>(
        r#"
        SELECT active_from_block_number
        FROM contract_instance_addresses
        WHERE chain_id = 'ethereum-mainnet'
          AND address = $1
          AND deactivated_at IS NULL
        "#,
    )
    .bind("0x0000000000000000000000000000000000000002")
    .fetch_one(database.pool())
    .await
    .context("failed to load stored registry start")?;
    assert_eq!(stored_registry_start, Some(100));

    let selector_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v2_registry_l1".to_owned()),
        50,
        180,
    )
    .await?;
    assert!(
        selector_plan
            .selected_targets
            .contains(&WatchedBackfillTarget {
                source_family: "ens_v2_registry_l1".to_owned(),
                contract_instance_id: root.contract_instance_id,
                address: root.address.clone(),
                effective_from_block: 120,
                effective_to_block: 180,
            })
    );
    assert!(
        selector_plan
            .selected_targets
            .contains(&WatchedBackfillTarget {
                source_family: "ens_v2_registry_l1".to_owned(),
                contract_instance_id: registry.contract_instance_id,
                address: registry.address.clone(),
                effective_from_block: 100,
                effective_to_block: 160,
            })
    );

    let bootstrap_targets =
        load_manifest_declared_bootstrap_targets(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(bootstrap_targets.len(), 2);
    assert!(bootstrap_targets.contains(&ManifestBootstrapTarget {
        source_family: "ens_v2_registry_l1".to_owned(),
        contract_instance_id: root.contract_instance_id,
        address: root.address.clone(),
        effective_from_block: 120,
        effective_to_block: None,
    }));
    assert!(bootstrap_targets.contains(&ManifestBootstrapTarget {
        source_family: "ens_v2_registry_l1".to_owned(),
        contract_instance_id: registry.contract_instance_id,
        address: registry.address.clone(),
        effective_from_block: 100,
        effective_to_block: Some(160),
    }));
    assert!(
        !bootstrap_targets
            .iter()
            .any(|target| target.address == unknown_start.address)
    );
    let skipped_targets =
        load_manifest_skipped_bootstrap_targets(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(
        skipped_targets,
        vec![ManifestBootstrapSkippedTarget {
            source_family: "ens_v2_registry_l1".to_owned(),
            contract_instance_id: unknown_start.contract_instance_id,
            address: unknown_start.address.clone(),
            skip_reason: "unknown_start".to_owned(),
        }]
    );
    let mut sorted_targets = bootstrap_targets.clone();
    sorted_targets.sort();
    assert_eq!(bootstrap_targets, sorted_targets);
    assert!(
        load_manifest_declared_bootstrap_targets(database.pool(), "base-mainnet")
            .await?
            .is_empty()
    );
    assert!(
        load_manifest_skipped_bootstrap_targets(database.pool(), "base-mainnet")
            .await?
            .is_empty()
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn simple_contract_start_block_persists_to_active_address_row() -> Result<()> {
    let database = TestDatabase::new().await?;
    let test_dir = TestDir::new()?;
    test_dir.write_manifest(
        "ens",
        "ens_v1_reverse_l1",
        "v1",
        &simple_contract_start_block_manifest_contents(),
    )?;

    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let active_from_block_number = query_scalar::<_, Option<i64>>(
        r#"
        SELECT active_from_block_number
        FROM contract_instance_addresses
        WHERE chain_id = 'ethereum-mainnet'
          AND address = $1
          AND deactivated_at IS NULL
        "#,
    )
    .bind("0x0000000000000000000000000000000000000042")
    .fetch_one(database.pool())
    .await
    .context("failed to load simple contract active start block")?;
    assert_eq!(active_from_block_number, Some(4242));

    let watched_contracts = load_watched_contracts(database.pool()).await?;
    let watched_contract = watched_contracts
        .iter()
        .find(|contract| contract.address == "0x0000000000000000000000000000000000000042")
        .expect("simple contract must enter the watched plan");
    assert_eq!(watched_contract.active_from_block_number, Some(4242));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn rejects_conflicting_active_start_blocks_for_same_contract_instance() -> Result<()> {
    let database = TestDatabase::new().await?;
    let test_dir = TestDir::new()?;
    let v1 = start_block_manifest_contents(
        Some(100),
        None,
        "0x0000000000000000000000000000000000000003",
    );
    let v2 = v1
        .replacen("manifest_version = 1", "manifest_version = 2", 1)
        .replacen("start_block = 100", "start_block = 200", 1);
    test_dir.write_manifest("ens", "ens_v2_registry_l1", "v1", &v1)?;
    test_dir.write_manifest("ens", "ens_v2_registry_l1", "v2", &v2)?;
    let repository = load_repository(&test_dir.path)?;

    let error = sync_repository(database.pool(), &repository)
        .await
        .expect_err("conflicting active start blocks must fail sync");
    assert!(
        error
            .to_string()
            .contains("conflicting start_block declarations"),
        "unexpected conflict error: {error:#}"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn checked_in_registry_manifests_admit_resolver_discovery() -> Result<()> {
    for case in [
        (
            "ens",
            "ens_v1_registry_l1",
            "ens_v1_resolver_l1",
            "v3",
            3_u64,
            9_usize,
            "ethereum-mainnet",
            "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            "0xF29100983E058B709F3D539b0c765937B804AC15",
            22_764_828,
            22_764_928,
            22_764_850,
            [
                "(upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f)",
                "(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89 @ ens_v1@91c966f)",
                "(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L174 @ ens_v1@91c966f)",
            ],
        ),
        (
            "basenames",
            "basenames_base_registry",
            "basenames_base_resolver",
            "v2",
            2_u64,
            2_usize,
            "base-mainnet",
            "0xb94704422c2a1e396835a571837aa5ae53285a95",
            "0xC6d566A56A1aFf6508b41f6c90ff131615583BCD",
            100,
            200,
            123,
            [
                "(upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc)",
                "(upstream: .refs/basenames/src/L2/Registry.sol:L113 @ basenames@1809bbc)",
                "(upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)",
            ],
        ),
    ] {
        let (
            namespace,
            source_family,
            resolver_source_family,
            registry_version_tag,
            registry_manifest_version,
            expected_contract_count,
            chain,
            registry_address,
            resolver_address,
            resolver_range_start,
            resolver_range_end,
            resolver_discovery_from,
            citations,
        ) = case;
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;
        let manifest =
            checked_in_manifest_contents(namespace, source_family, registry_version_tag)?;

        for citation in citations {
            assert!(
                manifest.contains(citation),
                "{namespace}/{source_family}/{registry_version_tag} manifest is missing upstream citation {citation}"
            );
        }

        let resolver_manifest =
            checked_in_manifest_contents(namespace, resolver_source_family, "v1")?;
        test_dir.write_manifest(namespace, source_family, registry_version_tag, &manifest)?;
        test_dir.write_manifest(namespace, resolver_source_family, "v1", &resolver_manifest)?;
        let repository = load_repository(&test_dir.path)?;
        assert_eq!(repository.summary().status, ManifestLoadStatus::Loaded);
        assert_eq!(repository.manifests().len(), 2);

        let loaded_manifest = &repository
            .manifests()
            .iter()
            .find(|loaded_manifest| {
                loaded_manifest.manifest.source_family == source_family
                    && loaded_manifest.manifest.manifest_version == registry_manifest_version
            })
            .expect("registry manifest must load")
            .manifest;
        assert_eq!(loaded_manifest.manifest_version, registry_manifest_version);
        assert_eq!(loaded_manifest.rollout_status, RolloutStatus::Active);
        assert_eq!(
            loaded_manifest.discovery_rules,
            vec![
                DiscoveryRule {
                    edge_kind: "subregistry".to_owned(),
                    from_role: "registry".to_owned(),
                    admission: "reachable_from_root".to_owned(),
                },
                DiscoveryRule {
                    edge_kind: "resolver".to_owned(),
                    from_role: "registry".to_owned(),
                    admission: "reachable_from_root".to_owned(),
                },
            ]
        );

        let summary = sync_repository(database.pool(), &repository).await?;
        assert_eq!(summary.status, ManifestSyncStatus::Synced);
        assert_eq!(summary.synced_manifest_count, 2);
        assert_eq!(summary.active_manifest_count, 2);
        assert_eq!(summary.root_count, 1);
        assert_eq!(summary.contract_count, expected_contract_count);
        assert_eq!(summary.capability_count, 1);
        assert_eq!(summary.discovery_rule_count, 2);

        let active_manifests =
            load_active_manifests_for_namespace(database.pool(), namespace).await?;
        assert_eq!(active_manifests.len(), 2);
        assert!(active_manifests.iter().any(|manifest| {
            manifest.source_family == source_family
                && manifest.manifest_version == registry_manifest_version
        }));
        assert!(active_manifests.iter().any(|manifest| {
            manifest.source_family == resolver_source_family && manifest.manifest_version == 1
        }));
        let registry_manifest_id =
            active_manifest_id_for_source_family(database.pool(), namespace, source_family).await?;
        let resolver_manifest_id = active_manifest_id_for_source_family(
            database.pool(),
            namespace,
            resolver_source_family,
        )
        .await?;

        let admission_state = load_discovery_admission_state(database.pool()).await?;
        assert_eq!(admission_state.active_manifest_count, 2);
        assert_eq!(admission_state.active_rule_count, 2);
        assert!(admission_state.has_authoritative_address(chain, registry_address));
        assert!(admission_state.has_authoritative_address(chain, resolver_address));

        let persistence_summary = persist_discovery_observation(
            database.pool(),
            &DiscoveryObservation {
                chain: chain.to_owned(),
                from_address: registry_address.to_owned(),
                to_address: resolver_address.to_owned(),
                edge_kind: "resolver".to_owned(),
                discovery_source: "registry_resolver_observation".to_owned(),
                active_from_block_number: Some(resolver_discovery_from),
                active_from_block_hash: Some(
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                ),
                active_to_block_number: None,
                active_to_block_hash: None,
                provenance: serde_json::json!({
                    "provider": "unit-test",
                    "kind": "resolver",
                }),
            },
        )
        .await?;
        assert_eq!(persistence_summary.admitted_edge_count, 1);
        assert_eq!(persistence_summary.inserted_edge_count, 1);
        assert_eq!(persistence_summary.admitted_edges[0].edge_kind, "resolver");
        assert_eq!(
            persistence_summary.admitted_edges[0].admission,
            "reachable_from_root"
        );
        assert_eq!(persistence_summary.admitted_edges[0].from_role, "registry");
        assert_eq!(
            persistence_summary.admitted_edges[0].source_manifest_id,
            registry_manifest_id
        );

        let resolver_address = normalize_address(resolver_address);
        let watched_contracts = load_watched_contracts(database.pool()).await?;
        assert!(watched_contracts.iter().any(|contract| {
            contract.chain == chain
                && contract.source_family == resolver_source_family
                && contract.address == resolver_address
                && contract.source == WatchedContractSource::DiscoveryEdge
                && contract.source_manifest_id == Some(resolver_manifest_id)
        }));
        let resolver_contract_instance_id = persistence_summary.admitted_edges[0]
            .to_contract_instance_id
            .expect("resolver discovery must admit a target contract instance");
        let resolver_source_plan = load_watched_source_selector_plan(
            database.pool(),
            chain,
            WatchedSourceSelector::SourceFamily(resolver_source_family.to_owned()),
            resolver_range_start,
            resolver_range_end,
        )
        .await?;
        assert!(resolver_source_plan.selected_targets.iter().any(|target| {
            target.source_family == resolver_source_family
                && target.contract_instance_id == resolver_contract_instance_id
                && target.address == resolver_address
                && target.effective_from_block == resolver_range_start
                && target.effective_to_block == resolver_range_end
        }));
        assert!(resolver_source_plan.selected_targets.iter().any(|target| {
            target.source_family == resolver_source_family
                && target.contract_instance_id == resolver_contract_instance_id
                && target.address == resolver_address
                && target.effective_from_block == resolver_discovery_from
                && target.effective_to_block == resolver_range_end
        }));
        let discovery_edge = sqlx::query(
            r#"
            SELECT source_manifest_id, provenance
            FROM discovery_edges
            WHERE edge_kind = 'resolver'
              AND deactivated_at IS NULL
            "#,
        )
        .fetch_one(database.pool())
        .await?;
        assert_eq!(
            discovery_edge
                .try_get::<Option<i64>, _>("source_manifest_id")?
                .expect("resolver discovery edge must retain source manifest provenance"),
            registry_manifest_id
        );
        assert!(
            !discovery_edge
                .try_get::<serde_json::Value, _>("provenance")?
                .as_object()
                .expect("resolver discovery provenance must be an object")
                .contains_key(PROPAGATED_ROLE_PROVENANCE_FIELD)
        );

        database.cleanup().await?;
    }

    Ok(())
}

#[tokio::test]
async fn checked_in_ens_registry_v3_admits_current_and_old_registry_targets() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    let registry_v2_manifest = checked_in_manifest_contents("ens", "ens_v1_registry_l1", "v2")?;
    let registry_v3_manifest = checked_in_manifest_contents("ens", "ens_v1_registry_l1", "v3")?;

    for citation in [
        "(upstream: .refs/ens_subgraph/subgraph.yaml:L15 @ ens_subgraph@723f1b6)",
        "(upstream: .refs/ens_subgraph/subgraph.yaml:L39 @ ens_subgraph@723f1b6)",
        "(upstream: .refs/ens_subgraph/subgraph.yaml:L42 @ ens_subgraph@723f1b6)",
        "(upstream: .refs/ens_subgraph/subgraph.yaml:L44 @ ens_subgraph@723f1b6)",
    ] {
        assert!(
            registry_v3_manifest.contains(citation),
            "ENSv1 registry v3 manifest is missing upstream citation {citation}"
        );
    }

    test_dir.write_manifest("ens", "ens_v1_registry_l1", "v2", &registry_v2_manifest)?;
    test_dir.write_manifest("ens", "ens_v1_registry_l1", "v3", &registry_v3_manifest)?;

    let repository = load_repository(&test_dir.path)?;
    assert_eq!(repository.summary().status, ManifestLoadStatus::Loaded);
    assert_eq!(repository.manifests().len(), 2);

    let manifests_by_version = repository
        .manifests()
        .iter()
        .map(|loaded_manifest| {
            (
                loaded_manifest.manifest.manifest_version,
                &loaded_manifest.manifest,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let registry_v2 = manifests_by_version[&2_u64];
    let registry_v3 = manifests_by_version[&3_u64];

    assert_eq!(registry_v2.rollout_status, RolloutStatus::Deprecated);
    assert_eq!(registry_v3.rollout_status, RolloutStatus::Active);
    assert_eq!(registry_v3.roots.len(), 1);
    assert_eq!(registry_v3.roots[0].start_block, Some(9_380_380));
    assert_eq!(registry_v3.contracts.len(), 2);

    let current_contract = registry_v3
        .contracts
        .iter()
        .find(|contract| contract.role == "registry")
        .expect("current registry role must be present");
    let old_contract = registry_v3
        .contracts
        .iter()
        .find(|contract| contract.role == "registry_old")
        .expect("old registry role must be present");
    assert_eq!(
        normalize_address(&current_contract.address),
        "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e"
    );
    assert_eq!(current_contract.start_block, Some(9_380_380));
    assert_eq!(
        normalize_address(&old_contract.address),
        "0x314159265dd8dbb310642f98f50c066173c1259b"
    );
    assert_eq!(old_contract.start_block, Some(3_327_417));
    assert_eq!(
        registry_v3.capability_flags, registry_v2.capability_flags,
        "v3 must not change ENSv1 registry capability flags"
    );
    let registry_v3_capability_flags = registry_v3.capability_flags.clone();

    let summary = sync_repository(database.pool(), &repository).await?;
    assert_eq!(summary.status, ManifestSyncStatus::Synced);
    assert_eq!(summary.synced_manifest_count, 2);
    assert_eq!(summary.active_manifest_count, 1);
    assert_eq!(summary.root_count, 2);
    assert_eq!(summary.contract_count, 3);
    assert_eq!(summary.capability_count, 2);
    assert_eq!(summary.discovery_rule_count, 4);

    assert_eq!(
        load_manifest_rollout_statuses(database.pool(), "ens").await?,
        vec![
            ("ens_v1_registry_l1".to_owned(), "deprecated".to_owned()),
            ("ens_v1_registry_l1".to_owned(), "active".to_owned()),
        ]
    );
    assert_eq!(
        load_capability_flags_for_source_family_version(
            database.pool(),
            "ens",
            "ens_v1_registry_l1",
            3
        )
        .await?,
        load_capability_flags_for_source_family_version(
            database.pool(),
            "ens",
            "ens_v1_registry_l1",
            2
        )
        .await?
    );

    let active_manifests = load_active_manifests_for_namespace(database.pool(), "ens").await?;
    assert_eq!(active_manifests.len(), 1);
    assert_eq!(active_manifests[0].source_family, "ens_v1_registry_l1");
    assert_eq!(active_manifests[0].manifest_version, 3);
    assert_eq!(
        active_manifests[0].capability_flags,
        registry_v3_capability_flags
    );

    let current_registry = normalize_address("0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E");
    let old_registry = normalize_address("0x314159265dd8dbb310642f98f50c066173c1259b");
    let watched_contracts = load_watched_contracts(database.pool()).await?;
    assert_eq!(watched_contracts.len(), 3);

    let watched_current_root = watched_contracts
        .iter()
        .find(|contract| {
            contract.address == current_registry
                && contract.source == WatchedContractSource::ManifestRoot
        })
        .expect("current registry root must be watched");
    let watched_current_contract = watched_contracts
        .iter()
        .find(|contract| {
            contract.address == current_registry
                && contract.source == WatchedContractSource::ManifestContract
        })
        .expect("current registry contract must be watched");
    let watched_old_contract = watched_contracts
        .iter()
        .find(|contract| {
            contract.address == old_registry
                && contract.source == WatchedContractSource::ManifestContract
        })
        .expect("old registry contract must be watched");

    assert_eq!(
        watched_current_root.contract_instance_id,
        watched_current_contract.contract_instance_id
    );
    assert_ne!(
        watched_current_contract.contract_instance_id,
        watched_old_contract.contract_instance_id
    );
    assert_eq!(
        watched_current_contract.active_from_block_number,
        Some(9_380_380)
    );
    assert_eq!(
        watched_old_contract.active_from_block_number,
        Some(3_327_417)
    );

    let current_contract_instance_id = watched_current_contract.contract_instance_id;
    let old_contract_instance_id = watched_old_contract.contract_instance_id;

    assert_eq!(
        load_watched_chain_plan(database.pool()).await?,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![current_registry.clone(), old_registry.clone()],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 2,
            discovery_edge_entry_count: 0,
        }]
    );

    let selector_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v1_registry_l1".to_owned()),
        0,
        10_000_000,
    )
    .await?;
    assert_eq!(selector_plan.selected_targets.len(), 2);
    assert!(
        selector_plan
            .selected_targets
            .contains(&WatchedBackfillTarget {
                source_family: "ens_v1_registry_l1".to_owned(),
                contract_instance_id: current_contract_instance_id,
                address: current_registry.clone(),
                effective_from_block: 9_380_380,
                effective_to_block: 10_000_000,
            })
    );
    assert!(
        selector_plan
            .selected_targets
            .contains(&WatchedBackfillTarget {
                source_family: "ens_v1_registry_l1".to_owned(),
                contract_instance_id: old_contract_instance_id,
                address: old_registry.clone(),
                effective_from_block: 3_327_417,
                effective_to_block: 10_000_000,
            })
    );

    let bootstrap_targets =
        load_manifest_declared_bootstrap_targets(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(bootstrap_targets.len(), 2);
    assert!(bootstrap_targets.contains(&ManifestBootstrapTarget {
        source_family: "ens_v1_registry_l1".to_owned(),
        contract_instance_id: current_contract_instance_id,
        address: current_registry,
        effective_from_block: 9_380_380,
        effective_to_block: None,
    }));
    assert!(bootstrap_targets.contains(&ManifestBootstrapTarget {
        source_family: "ens_v1_registry_l1".to_owned(),
        contract_instance_id: old_contract_instance_id,
        address: old_registry,
        effective_from_block: 3_327_417,
        effective_to_block: None,
    }));
    assert!(
        load_manifest_skipped_bootstrap_targets(database.pool(), "ethereum-mainnet")
            .await?
            .is_empty()
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn ens_v1_resolver_public_resolver_profile_admission_keeps_unknowns_watch_only() -> Result<()>
{
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    let registry_manifest = checked_in_manifest_contents("ens", "ens_v1_registry_l1", "v3")?;
    let resolver_manifest = checked_in_manifest_contents("ens", "ens_v1_resolver_l1", "v1")?;
    test_dir.write_manifest("ens", "ens_v1_registry_l1", "v3", &registry_manifest)?;
    test_dir.write_manifest("ens", "ens_v1_resolver_l1", "v1", &resolver_manifest)?;

    let repository = load_repository(&test_dir.path)?;
    let summary = sync_repository(database.pool(), &repository).await?;
    assert_eq!(summary.status, ManifestSyncStatus::Synced);
    assert_eq!(summary.active_manifest_count, 2);

    let registry_address = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E";
    let public_resolver_seed_address = "0xF29100983E058B709F3D539b0c765937B804AC15";
    let supported_resolver_address = "0x0000000000000000000000000000000000000201";
    let pending_resolver_address = "0x0000000000000000000000000000000000000202";
    let unsupported_resolver_address = "0x0000000000000000000000000000000000000203";
    let public_resolver_code_hash = "keccak256:ens-v1-public-resolver-compatible";
    let unsupported_code_hash = "keccak256:unknown-resolver";

    let seed_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        public_resolver_seed_address,
    )
    .await?;

    let supported_summary = persist_discovery_observation(
        database.pool(),
        &DiscoveryObservation {
            chain: "ethereum-mainnet".to_owned(),
            from_address: registry_address.to_owned(),
            to_address: supported_resolver_address.to_owned(),
            edge_kind: "resolver".to_owned(),
            discovery_source: "registry_resolver_observation".to_owned(),
            active_from_block_number: Some(100),
            active_from_block_hash: Some(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "kind": "resolver",
                "case": "public-resolver-code-hash-match",
            }),
        },
    )
    .await?;
    let pending_summary = persist_discovery_observation(
        database.pool(),
        &DiscoveryObservation {
            chain: "ethereum-mainnet".to_owned(),
            from_address: registry_address.to_owned(),
            to_address: pending_resolver_address.to_owned(),
            edge_kind: "resolver".to_owned(),
            discovery_source: "registry_resolver_observation".to_owned(),
            active_from_block_number: Some(110),
            active_from_block_hash: Some(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
            ),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "kind": "resolver",
                "case": "pending-code-hash",
            }),
        },
    )
    .await?;
    let unsupported_summary = persist_discovery_observation(
        database.pool(),
        &DiscoveryObservation {
            chain: "ethereum-mainnet".to_owned(),
            from_address: registry_address.to_owned(),
            to_address: unsupported_resolver_address.to_owned(),
            edge_kind: "resolver".to_owned(),
            discovery_source: "registry_resolver_observation".to_owned(),
            active_from_block_number: Some(120),
            active_from_block_hash: Some(
                "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
            ),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "kind": "resolver",
                "case": "unsupported-code-hash",
            }),
        },
    )
    .await?;

    assert_eq!(supported_summary.admitted_edge_count, 1);
    assert_eq!(pending_summary.admitted_edge_count, 1);
    assert_eq!(unsupported_summary.admitted_edge_count, 1);
    let supported_contract_instance_id = supported_summary.admitted_edges[0]
        .to_contract_instance_id
        .expect("supported resolver discovery must admit a target");
    let pending_contract_instance_id = pending_summary.admitted_edges[0]
        .to_contract_instance_id
        .expect("pending resolver discovery must admit a target");
    let unsupported_contract_instance_id = unsupported_summary.admitted_edges[0]
        .to_contract_instance_id
        .expect("unsupported resolver discovery must admit a target");

    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "ethereum-mainnet",
            block_hash: "0x1111111111111111111111111111111111111111111111111111111111111111",
            block_number: 100,
            contract_address: public_resolver_seed_address,
            code_hash: public_resolver_code_hash,
            code_byte_length: 1,
            canonicality_state: "canonical",
        },
    )
    .await?;
    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "ethereum-mainnet",
            block_hash: "0x2222222222222222222222222222222222222222222222222222222222222222",
            block_number: 110,
            contract_address: supported_resolver_address,
            code_hash: public_resolver_code_hash,
            code_byte_length: 1,
            canonicality_state: "canonical",
        },
    )
    .await?;
    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "ethereum-mainnet",
            block_hash: "0x3333333333333333333333333333333333333333333333333333333333333333",
            block_number: 120,
            contract_address: unsupported_resolver_address,
            code_hash: unsupported_code_hash,
            code_byte_length: 1,
            canonicality_state: "canonical",
        },
    )
    .await?;

    let watched_contracts = load_watched_contracts(database.pool()).await?;
    for (address, contract_instance_id) in [
        (supported_resolver_address, supported_contract_instance_id),
        (pending_resolver_address, pending_contract_instance_id),
        (
            unsupported_resolver_address,
            unsupported_contract_instance_id,
        ),
    ] {
        let address = normalize_address(address);
        assert!(
            watched_contracts.iter().any(|contract| {
                contract.source_family == "ens_v1_resolver_l1"
                    && contract.address == address
                    && contract.contract_instance_id == contract_instance_id
                    && contract.source == WatchedContractSource::DiscoveryEdge
            }),
            "dynamic resolver {address} must remain an admitted watch target"
        );
    }

    let admissions = load_ens_v1_public_resolver_profile_admissions(database.pool()).await?;
    assert_eq!(admissions.len(), 118);

    assert_ens_v1_profile_admission_rows_with_statuses(
        &admissions,
        EnsV1ProfileAdmissionStatusExpectation {
            address: public_resolver_seed_address,
            profile: "public_resolver_compatible",
            fact_statuses: latest_public_resolver_fact_statuses(),
            admission_basis: "manifest_public_resolver_seed",
            contract_instance_id: seed_contract_instance_id,
            observed_code_hash: Some(public_resolver_code_hash),
            matched_code_hash: Some(public_resolver_code_hash),
            matched_contract_instance_id: Some(seed_contract_instance_id),
        },
    );
    assert_ens_v1_profile_admission_rows_with_statuses(
        &admissions,
        EnsV1ProfileAdmissionStatusExpectation {
            address: supported_resolver_address,
            profile: "public_resolver_compatible",
            fact_statuses: latest_public_resolver_fact_statuses(),
            admission_basis: "code_hash_match",
            contract_instance_id: supported_contract_instance_id,
            observed_code_hash: Some(public_resolver_code_hash),
            matched_code_hash: Some(public_resolver_code_hash),
            matched_contract_instance_id: Some(seed_contract_instance_id),
        },
    );
    assert_profile_admission_rows(
        &admissions,
        EnsV1ProfileAdmissionExpectation {
            address: pending_resolver_address,
            profile: "public_resolver_compatible",
            fact_families: default_public_resolver_fact_families(),
            status: "pending",
            admission_basis: "code_hash_pending",
            contract_instance_id: pending_contract_instance_id,
            observed_code_hash: None,
            matched_code_hash: None,
            matched_contract_instance_id: None,
        },
    );
    assert_profile_admission_rows(
        &admissions,
        EnsV1ProfileAdmissionExpectation {
            address: unsupported_resolver_address,
            profile: "public_resolver_compatible",
            fact_families: default_public_resolver_fact_families(),
            status: "unsupported",
            admission_basis: "code_hash_mismatch",
            contract_instance_id: unsupported_contract_instance_id,
            observed_code_hash: Some(unsupported_code_hash),
            matched_code_hash: None,
            matched_contract_instance_id: None,
        },
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn scoped_resolver_profile_rejects_unadmitted_code_hash_target() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    test_dir.write_manifest(
        "ens",
        "ens_v1_registry_l1",
        "v3",
        &checked_in_manifest_contents("ens", "ens_v1_registry_l1", "v3")?,
    )?;
    test_dir.write_manifest(
        "ens",
        "ens_v1_resolver_l1",
        "v1",
        &checked_in_manifest_contents("ens", "ens_v1_resolver_l1", "v1")?,
    )?;

    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let registry_address = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E";
    let public_resolver_seed_address = "0xF29100983E058B709F3D539b0c765937B804AC15";
    let admitted_resolver_address = "0x0000000000000000000000000000000000000241";
    let unadmitted_resolver_address = "0x0000000000000000000000000000000000000242";
    let public_resolver_code_hash = "keccak256:ens-v1-scoped-public-resolver-compatible";

    let seed_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        public_resolver_seed_address,
    )
    .await?;
    let admitted_summary = persist_discovery_observation(
        database.pool(),
        &DiscoveryObservation {
            chain: "ethereum-mainnet".to_owned(),
            from_address: registry_address.to_owned(),
            to_address: admitted_resolver_address.to_owned(),
            edge_kind: "resolver".to_owned(),
            discovery_source: "registry_resolver_observation".to_owned(),
            active_from_block_number: Some(140),
            active_from_block_hash: Some(
                "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_owned(),
            ),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "kind": "scoped-supported-resolver",
            }),
        },
    )
    .await?;
    let admitted_contract_instance_id = admitted_summary.admitted_edges[0]
        .to_contract_instance_id
        .expect("admitted resolver discovery must create a target");

    for (block_number, address) in [
        (100, public_resolver_seed_address),
        (140, admitted_resolver_address),
        (150, unadmitted_resolver_address),
    ] {
        let block_hash = format!("0x{block_number:064x}");
        insert_raw_code_hash_observation(
            database.pool(),
            RawCodeHashObservation {
                chain: "ethereum-mainnet",
                block_hash: &block_hash,
                block_number,
                contract_address: address,
                code_hash: public_resolver_code_hash,
                code_byte_length: 1,
                canonicality_state: "canonical",
            },
        )
        .await?;
    }

    let admissions = load_ens_v1_public_resolver_profile_admissions_for_targets(
        database.pool(),
        &[
            (
                "ethereum-mainnet".to_owned(),
                admitted_resolver_address.to_owned(),
            ),
            (
                "ethereum-mainnet".to_owned(),
                unadmitted_resolver_address.to_owned(),
            ),
        ],
    )
    .await?;

    assert_eq!(admissions.len(), 14);
    assert_ens_v1_profile_admission_rows_with_statuses(
        &admissions,
        EnsV1ProfileAdmissionStatusExpectation {
            address: admitted_resolver_address,
            profile: "public_resolver_compatible",
            fact_statuses: latest_public_resolver_fact_statuses(),
            admission_basis: "code_hash_match",
            contract_instance_id: admitted_contract_instance_id,
            observed_code_hash: Some(public_resolver_code_hash),
            matched_code_hash: Some(public_resolver_code_hash),
            matched_contract_instance_id: Some(seed_contract_instance_id),
        },
    );
    assert!(
        admissions
            .iter()
            .all(|admission| admission.address != normalize_address(unadmitted_resolver_address)),
        "unadmitted target must not graduate to a scoped resolver profile"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn ens_v1_known_legacy_resolver_profile_does_not_flatten_latest_capabilities() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    test_dir.write_manifest(
        "ens",
        "ens_v1_resolver_l1",
        "v1",
        &checked_in_manifest_contents("ens", "ens_v1_resolver_l1", "v1")?,
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let legacy_resolver_address = "0x4976fb03C32e5B8cfe2b6cCB31c09Ba78EBaBa41";
    let legacy_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        legacy_resolver_address,
    )
    .await?;
    let admissions = load_ens_v1_public_resolver_profile_admissions(database.pool()).await?;
    let legacy_rows = admissions
        .iter()
        .filter(|admission| admission.address == normalize_address(legacy_resolver_address))
        .collect::<Vec<_>>();
    assert_eq!(legacy_rows.len(), 14);
    assert!(
        legacy_rows
            .iter()
            .all(|row| row.profile == "public_resolver_legacy_multicoin_dns")
    );
    assert!(legacy_rows.iter().all(
        |row| row.contract_instance_id == legacy_contract_instance_id
            && row.source == WatchedContractSource::ManifestContract
            && row.admission_basis == "first_party_known_resolver_admission"
    ));
    let statuses = legacy_rows
        .iter()
        .map(|row| (row.fact_family.as_str(), row.status.as_str()))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(statuses["resolver_record"], "unsupported");
    assert_eq!(statuses["resolver_record:addr"], "supported");
    assert_eq!(statuses["resolver_record:multicoin_addr"], "supported");
    assert_eq!(statuses["resolver_record:name"], "supported");
    assert_eq!(statuses["resolver_record:text"], "supported");
    assert_eq!(statuses["resolver_record:abi"], "supported");
    assert_eq!(statuses["resolver_record:contenthash"], "supported");
    assert_eq!(statuses["resolver_record:dns"], "supported");
    assert_eq!(statuses["resolver_record:interface"], "supported");
    assert_eq!(statuses["resolver_record:data"], "unsupported");
    assert_eq!(statuses["resolver_authorization"], "supported");
    assert_eq!(statuses["resolver_record_version"], "unsupported");
    assert_eq!(
        statuses["resolver_feature:name_wrapper_aware"],
        "unsupported"
    );
    assert_eq!(
        statuses["resolver_feature:default_coin_type"],
        "unsupported"
    );
    assert!(
        legacy_rows
            .iter()
            .all(|row| row.observed_code_hash.is_none())
    );

    database.cleanup().await?;
    Ok(())
}

struct ProfileAdmissionExpectation<'a> {
    address: &'a str,
    chain: &'a str,
    source_family: &'a str,
    profile: &'a str,
    fact_families: BTreeSet<&'a str>,
    status: &'a str,
    admission_basis: &'a str,
    contract_instance_id: Uuid,
    observed_code_hash: Option<&'a str>,
    matched_code_hash: Option<&'a str>,
    matched_contract_instance_id: Option<Uuid>,
}

struct EnsV1ProfileAdmissionExpectation<'a> {
    address: &'a str,
    profile: &'a str,
    fact_families: BTreeSet<&'a str>,
    status: &'a str,
    admission_basis: &'a str,
    contract_instance_id: Uuid,
    observed_code_hash: Option<&'a str>,
    matched_code_hash: Option<&'a str>,
    matched_contract_instance_id: Option<Uuid>,
}

struct EnsV1ProfileAdmissionStatusExpectation<'a> {
    address: &'a str,
    profile: &'a str,
    fact_statuses: BTreeMap<&'a str, &'a str>,
    admission_basis: &'a str,
    contract_instance_id: Uuid,
    observed_code_hash: Option<&'a str>,
    matched_code_hash: Option<&'a str>,
    matched_contract_instance_id: Option<Uuid>,
}

fn assert_profile_admission_rows(
    admissions: &[ResolverProfileAdmission],
    expectation: EnsV1ProfileAdmissionExpectation<'_>,
) {
    assert_profile_admission_rows_for_profile(
        admissions,
        ProfileAdmissionExpectation {
            address: expectation.address,
            chain: "ethereum-mainnet",
            source_family: "ens_v1_resolver_l1",
            profile: expectation.profile,
            fact_families: expectation.fact_families,
            status: expectation.status,
            admission_basis: expectation.admission_basis,
            contract_instance_id: expectation.contract_instance_id,
            observed_code_hash: expectation.observed_code_hash,
            matched_code_hash: expectation.matched_code_hash,
            matched_contract_instance_id: expectation.matched_contract_instance_id,
        },
    );
}

fn assert_ens_v1_profile_admission_rows_with_statuses(
    admissions: &[ResolverProfileAdmission],
    expectation: EnsV1ProfileAdmissionStatusExpectation<'_>,
) {
    assert_profile_admission_rows_with_statuses(
        admissions,
        "ethereum-mainnet",
        "ens_v1_resolver_l1",
        expectation.address,
        expectation.profile,
        expectation.fact_statuses,
        expectation.admission_basis,
        expectation.contract_instance_id,
        expectation.observed_code_hash,
        expectation.matched_code_hash,
        expectation.matched_contract_instance_id,
    );
}

#[allow(clippy::too_many_arguments)]
fn assert_profile_admission_rows_with_statuses(
    admissions: &[ResolverProfileAdmission],
    chain: &str,
    source_family: &str,
    address: &str,
    profile: &str,
    fact_statuses: BTreeMap<&str, &str>,
    admission_basis: &str,
    contract_instance_id: Uuid,
    observed_code_hash: Option<&str>,
    matched_code_hash: Option<&str>,
    matched_contract_instance_id: Option<Uuid>,
) {
    let address = normalize_address(address);
    let rows = admissions
        .iter()
        .filter(|admission| admission.address == address)
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), fact_statuses.len());
    assert_eq!(
        rows.iter()
            .map(|admission| admission.fact_family.as_str())
            .collect::<BTreeSet<_>>(),
        fact_statuses.keys().copied().collect::<BTreeSet<_>>()
    );

    for row in rows {
        assert_eq!(row.chain, chain);
        assert_eq!(row.source_family, source_family);
        assert_eq!(row.contract_instance_id, contract_instance_id);
        assert_eq!(row.profile, profile);
        assert_eq!(row.status.as_str(), fact_statuses[row.fact_family.as_str()]);
        assert_eq!(row.admission_basis, admission_basis);
        assert_eq!(row.observed_code_hash.as_deref(), observed_code_hash);
        assert_eq!(row.matched_code_hash.as_deref(), matched_code_hash);
        assert_eq!(
            row.matched_contract_instance_id,
            matched_contract_instance_id
        );
    }
}

fn assert_profile_admission_rows_for_profile(
    admissions: &[ResolverProfileAdmission],
    expectation: ProfileAdmissionExpectation<'_>,
) {
    let address = normalize_address(expectation.address);
    let rows = admissions
        .iter()
        .filter(|admission| admission.address == address)
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), expectation.fact_families.len());
    assert_eq!(
        rows.iter()
            .map(|admission| admission.fact_family.as_str())
            .collect::<BTreeSet<_>>(),
        expectation.fact_families
    );

    for row in rows {
        assert_eq!(row.chain, expectation.chain);
        assert_eq!(row.source_family, expectation.source_family);
        assert_eq!(row.contract_instance_id, expectation.contract_instance_id);
        assert_eq!(row.profile, expectation.profile);
        assert_eq!(row.status, expectation.status);
        assert_eq!(row.admission_basis, expectation.admission_basis);
        assert_eq!(
            row.observed_code_hash.as_deref(),
            expectation.observed_code_hash
        );
        assert_eq!(
            row.matched_code_hash.as_deref(),
            expectation.matched_code_hash
        );
        assert_eq!(
            row.matched_contract_instance_id,
            expectation.matched_contract_instance_id
        );
    }
}

fn default_public_resolver_fact_families() -> BTreeSet<&'static str> {
    BTreeSet::from([
        "resolver_authorization",
        "resolver_record",
        "resolver_record_version",
    ])
}

fn latest_public_resolver_fact_families() -> BTreeSet<&'static str> {
    BTreeSet::from([
        "resolver_authorization",
        "resolver_feature:default_coin_type",
        "resolver_feature:name_wrapper_aware",
        "resolver_record",
        "resolver_record:abi",
        "resolver_record:addr",
        "resolver_record:contenthash",
        "resolver_record:data",
        "resolver_record:dns",
        "resolver_record:interface",
        "resolver_record:multicoin_addr",
        "resolver_record:name",
        "resolver_record:text",
        "resolver_record_version",
    ])
}

fn latest_public_resolver_fact_statuses() -> BTreeMap<&'static str, &'static str> {
    latest_public_resolver_fact_families()
        .into_iter()
        .map(|fact_family| {
            let status = if fact_family == "resolver_record:data" {
                "unsupported"
            } else {
                "supported"
            };
            (fact_family, status)
        })
        .collect()
}

#[tokio::test]
async fn basenames_l2_resolver_profile_admission_keeps_unknowns_watch_only() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    let registry_manifest =
        checked_in_manifest_contents("basenames", "basenames_base_registry", "v2")?;
    let resolver_manifest =
        checked_in_manifest_contents("basenames", "basenames_base_resolver", "v1")?;

    for citation in [
        "(upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc)",
        "(upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc)",
        "(upstream: .refs/basenames/src/L2/L2Resolver.sol:L29 @ basenames@1809bbc)",
        "(upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc)",
        "(upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc)",
        "(upstream: .refs/basenames/src/L2/L2Resolver.sol:L209 @ basenames@1809bbc)",
        "(upstream: .refs/basenames/src/L2/L2Resolver.sol:L225 @ basenames@1809bbc)",
    ] {
        assert!(
            resolver_manifest.contains(citation),
            "Basenames resolver manifest is missing upstream citation {citation}"
        );
    }
    assert!(!resolver_manifest.contains("public_resolver_compatible"));
    assert!(!resolver_manifest.contains("record-version"));

    test_dir.write_manifest(
        "basenames",
        "basenames_base_registry",
        "v2",
        &registry_manifest,
    )?;
    test_dir.write_manifest(
        "basenames",
        "basenames_base_resolver",
        "v1",
        &resolver_manifest,
    )?;

    let repository = load_repository(&test_dir.path)?;
    let summary = sync_repository(database.pool(), &repository).await?;
    assert_eq!(summary.status, ManifestSyncStatus::Synced);
    assert_eq!(summary.active_manifest_count, 2);

    let registry_address = "0xb94704422c2a1e396835a571837aa5ae53285a95";
    let l2_resolver_seed_address = "0xC6d566A56A1aFf6508b41f6c90ff131615583BCD";
    let supported_resolver_address = "0x0000000000000000000000000000000000000301";
    let pending_resolver_address = "0x0000000000000000000000000000000000000302";
    let unsupported_resolver_address = "0x0000000000000000000000000000000000000303";
    let l2_resolver_code_hash = "keccak256:basenames-l2-resolver-compatible";
    let unsupported_code_hash = "keccak256:basenames-unknown-resolver";

    let seed_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "base-mainnet",
        l2_resolver_seed_address,
    )
    .await?;

    let supported_summary = persist_discovery_observation(
        database.pool(),
        &DiscoveryObservation {
            chain: "base-mainnet".to_owned(),
            from_address: registry_address.to_owned(),
            to_address: supported_resolver_address.to_owned(),
            edge_kind: "resolver".to_owned(),
            discovery_source: "registry_resolver_observation".to_owned(),
            active_from_block_number: Some(100),
            active_from_block_hash: Some(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "kind": "resolver",
                "case": "l2-resolver-code-hash-match",
            }),
        },
    )
    .await?;
    let pending_summary = persist_discovery_observation(
        database.pool(),
        &DiscoveryObservation {
            chain: "base-mainnet".to_owned(),
            from_address: registry_address.to_owned(),
            to_address: pending_resolver_address.to_owned(),
            edge_kind: "resolver".to_owned(),
            discovery_source: "registry_resolver_observation".to_owned(),
            active_from_block_number: Some(110),
            active_from_block_hash: Some(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
            ),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "kind": "resolver",
                "case": "pending-code-hash",
            }),
        },
    )
    .await?;
    let unsupported_summary = persist_discovery_observation(
        database.pool(),
        &DiscoveryObservation {
            chain: "base-mainnet".to_owned(),
            from_address: registry_address.to_owned(),
            to_address: unsupported_resolver_address.to_owned(),
            edge_kind: "resolver".to_owned(),
            discovery_source: "registry_resolver_observation".to_owned(),
            active_from_block_number: Some(120),
            active_from_block_hash: Some(
                "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
            ),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "kind": "resolver",
                "case": "unsupported-code-hash",
            }),
        },
    )
    .await?;

    assert_eq!(supported_summary.admitted_edge_count, 1);
    assert_eq!(pending_summary.admitted_edge_count, 1);
    assert_eq!(unsupported_summary.admitted_edge_count, 1);
    let supported_contract_instance_id = supported_summary.admitted_edges[0]
        .to_contract_instance_id
        .expect("supported resolver discovery must admit a target");
    let pending_contract_instance_id = pending_summary.admitted_edges[0]
        .to_contract_instance_id
        .expect("pending resolver discovery must admit a target");
    let unsupported_contract_instance_id = unsupported_summary.admitted_edges[0]
        .to_contract_instance_id
        .expect("unsupported resolver discovery must admit a target");

    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "base-mainnet",
            block_hash: "0x1111111111111111111111111111111111111111111111111111111111111111",
            block_number: 100,
            contract_address: l2_resolver_seed_address,
            code_hash: l2_resolver_code_hash,
            code_byte_length: 1,
            canonicality_state: "canonical",
        },
    )
    .await?;
    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "base-mainnet",
            block_hash: "0x2222222222222222222222222222222222222222222222222222222222222222",
            block_number: 110,
            contract_address: supported_resolver_address,
            code_hash: l2_resolver_code_hash,
            code_byte_length: 1,
            canonicality_state: "canonical",
        },
    )
    .await?;
    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "base-mainnet",
            block_hash: "0x3333333333333333333333333333333333333333333333333333333333333333",
            block_number: 120,
            contract_address: unsupported_resolver_address,
            code_hash: unsupported_code_hash,
            code_byte_length: 1,
            canonicality_state: "canonical",
        },
    )
    .await?;

    let watched_contracts = load_watched_contracts(database.pool()).await?;
    for (address, contract_instance_id) in [
        (supported_resolver_address, supported_contract_instance_id),
        (pending_resolver_address, pending_contract_instance_id),
        (
            unsupported_resolver_address,
            unsupported_contract_instance_id,
        ),
    ] {
        let address = normalize_address(address);
        assert!(
            watched_contracts.iter().any(|contract| {
                contract.source_family == "basenames_base_resolver"
                    && contract.address == address
                    && contract.contract_instance_id == contract_instance_id
                    && contract.source == WatchedContractSource::DiscoveryEdge
            }),
            "dynamic Basenames resolver {address} must remain an admitted watch target"
        );
    }

    let admissions = load_basenames_l2_resolver_profile_admissions(database.pool()).await?;
    assert_eq!(admissions.len(), 8);
    assert!(
        admissions
            .iter()
            .all(|admission| admission.profile != "public_resolver_compatible")
    );
    assert!(
        admissions
            .iter()
            .all(|admission| admission.fact_family != "resolver_record_version")
    );
    let fact_families = BTreeSet::from(["resolver_authorization", "resolver_record"]);

    assert_profile_admission_rows_for_profile(
        &admissions,
        ProfileAdmissionExpectation {
            address: l2_resolver_seed_address,
            chain: "base-mainnet",
            source_family: "basenames_base_resolver",
            profile: "l2_resolver_compatible",
            fact_families: fact_families.clone(),
            status: "supported",
            admission_basis: "manifest_l2_resolver_seed",
            contract_instance_id: seed_contract_instance_id,
            observed_code_hash: Some(l2_resolver_code_hash),
            matched_code_hash: Some(l2_resolver_code_hash),
            matched_contract_instance_id: Some(seed_contract_instance_id),
        },
    );
    assert_profile_admission_rows_for_profile(
        &admissions,
        ProfileAdmissionExpectation {
            address: supported_resolver_address,
            chain: "base-mainnet",
            source_family: "basenames_base_resolver",
            profile: "l2_resolver_compatible",
            fact_families: fact_families.clone(),
            status: "supported",
            admission_basis: "code_hash_match",
            contract_instance_id: supported_contract_instance_id,
            observed_code_hash: Some(l2_resolver_code_hash),
            matched_code_hash: Some(l2_resolver_code_hash),
            matched_contract_instance_id: Some(seed_contract_instance_id),
        },
    );
    assert_profile_admission_rows_for_profile(
        &admissions,
        ProfileAdmissionExpectation {
            address: pending_resolver_address,
            chain: "base-mainnet",
            source_family: "basenames_base_resolver",
            profile: "l2_resolver_compatible",
            fact_families: fact_families.clone(),
            status: "pending",
            admission_basis: "code_hash_pending",
            contract_instance_id: pending_contract_instance_id,
            observed_code_hash: None,
            matched_code_hash: None,
            matched_contract_instance_id: None,
        },
    );
    assert_profile_admission_rows_for_profile(
        &admissions,
        ProfileAdmissionExpectation {
            address: unsupported_resolver_address,
            chain: "base-mainnet",
            source_family: "basenames_base_resolver",
            profile: "l2_resolver_compatible",
            fact_families,
            status: "unsupported",
            admission_basis: "code_hash_mismatch",
            contract_instance_id: unsupported_contract_instance_id,
            observed_code_hash: Some(unsupported_code_hash),
            matched_code_hash: None,
            matched_contract_instance_id: None,
        },
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn dynamic_resolver_backfill_selector_loads_edge_address_intersections() -> Result<()> {
    for case in [
        (
            "ens",
            "ens_v1_registry_l1",
            "ens_v1_resolver_l1",
            "ethereum-mainnet",
            "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            "0x0000000000000000000000000000000000000101",
            "0x0000000000000000000000000000000000000102",
            "0x0000000000000000000000000000000000000103",
        ),
        (
            "basenames",
            "basenames_base_registry",
            "basenames_base_resolver",
            "base-mainnet",
            "0xb94704422c2a1e396835a571837aa5ae53285a95",
            "0x0000000000000000000000000000000000000201",
            "0x0000000000000000000000000000000000000202",
            "0x0000000000000000000000000000000000000203",
        ),
    ] {
        let (
            namespace,
            registry_source_family,
            resolver_source_family,
            chain,
            registry_address,
            selected_resolver_address,
            closed_resolver_address,
            deactivated_resolver_address,
        ) = case;
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;
        let registry_version_tag = if registry_source_family == "ens_v1_registry_l1" {
            "v3"
        } else {
            "v2"
        };

        test_dir.write_manifest(
            namespace,
            registry_source_family,
            registry_version_tag,
            &checked_in_manifest_contents(namespace, registry_source_family, registry_version_tag)?,
        )?;
        test_dir.write_manifest(
            namespace,
            resolver_source_family,
            "v1",
            &checked_in_manifest_contents(namespace, resolver_source_family, "v1")?,
        )?;
        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

        let selected_summary = persist_discovery_observation(
            database.pool(),
            &DiscoveryObservation {
                chain: chain.to_owned(),
                from_address: registry_address.to_owned(),
                to_address: selected_resolver_address.to_owned(),
                edge_kind: "resolver".to_owned(),
                discovery_source: "dynamic-resolver-backfill-selector-test".to_owned(),
                active_from_block_number: Some(100),
                active_from_block_hash: Some(
                    "0x1111111111111111111111111111111111111111111111111111111111111111".to_owned(),
                ),
                active_to_block_number: Some(220),
                active_to_block_hash: Some(
                    "0x2222222222222222222222222222222222222222222222222222222222222222".to_owned(),
                ),
                provenance: serde_json::json!({
                    "provider": "unit-test",
                    "kind": "selected-resolver",
                }),
            },
        )
        .await?;
        let selected_contract_instance_id = selected_summary.admitted_edges[0]
            .to_contract_instance_id
            .expect("selected resolver discovery must admit a target contract instance");
        sqlx::query(
            r#"
            UPDATE contract_instance_addresses
            SET active_from_block_number = 150,
                active_to_block_number = 190
            WHERE contract_instance_id = $1
              AND deactivated_at IS NULL
            "#,
        )
        .bind(selected_contract_instance_id)
        .execute(database.pool())
        .await?;

        let closed_summary = persist_discovery_observation(
            database.pool(),
            &DiscoveryObservation {
                chain: chain.to_owned(),
                from_address: registry_address.to_owned(),
                to_address: closed_resolver_address.to_owned(),
                edge_kind: "resolver".to_owned(),
                discovery_source: "dynamic-resolver-backfill-selector-test".to_owned(),
                active_from_block_number: Some(20),
                active_from_block_hash: Some(
                    "0x3333333333333333333333333333333333333333333333333333333333333333".to_owned(),
                ),
                active_to_block_number: Some(90),
                active_to_block_hash: Some(
                    "0x4444444444444444444444444444444444444444444444444444444444444444".to_owned(),
                ),
                provenance: serde_json::json!({
                    "provider": "unit-test",
                    "kind": "closed-resolver",
                }),
            },
        )
        .await?;
        let closed_contract_instance_id = closed_summary.admitted_edges[0]
            .to_contract_instance_id
            .expect("closed resolver discovery must admit a target contract instance");

        let deactivated_summary = persist_discovery_observation(
            database.pool(),
            &DiscoveryObservation {
                chain: chain.to_owned(),
                from_address: registry_address.to_owned(),
                to_address: deactivated_resolver_address.to_owned(),
                edge_kind: "resolver".to_owned(),
                discovery_source: "dynamic-resolver-backfill-selector-test".to_owned(),
                active_from_block_number: Some(150),
                active_from_block_hash: Some(
                    "0x5555555555555555555555555555555555555555555555555555555555555555".to_owned(),
                ),
                active_to_block_number: Some(190),
                active_to_block_hash: Some(
                    "0x6666666666666666666666666666666666666666666666666666666666666666".to_owned(),
                ),
                provenance: serde_json::json!({
                    "provider": "unit-test",
                    "kind": "deactivated-resolver",
                }),
            },
        )
        .await?;
        let deactivated_contract_instance_id = deactivated_summary.admitted_edges[0]
            .to_contract_instance_id
            .expect("deactivated resolver discovery must admit a target contract instance");
        sqlx::query(
            r#"
            UPDATE discovery_edges
            SET deactivated_at = now()
            WHERE to_contract_instance_id = $1
              AND deactivated_at IS NULL
            "#,
        )
        .bind(deactivated_contract_instance_id)
        .execute(database.pool())
        .await?;

        let watched_contracts = load_watched_contracts(database.pool()).await?;
        let selected_watched_contract = watched_contracts
            .iter()
            .find(|contract| contract.contract_instance_id == selected_contract_instance_id)
            .expect("selected resolver discovery target must be in the watched plan");
        assert_eq!(selected_watched_contract.chain, chain);
        assert_eq!(
            selected_watched_contract.source_family,
            resolver_source_family
        );
        assert_eq!(
            selected_watched_contract.address,
            normalize_address(selected_resolver_address)
        );
        assert_eq!(
            selected_watched_contract.source,
            WatchedContractSource::DiscoveryEdge
        );
        assert_eq!(
            selected_watched_contract.active_from_block_number,
            Some(150)
        );
        assert_eq!(selected_watched_contract.active_to_block_number, Some(190));
        assert!(
            watched_contracts
                .iter()
                .all(|contract| contract.contract_instance_id != deactivated_contract_instance_id),
            "deactivated resolver discovery edge must not remain in the watched plan"
        );

        let selected_plan = load_watched_source_selector_plan(
            database.pool(),
            chain,
            WatchedSourceSelector::SourceFamily(resolver_source_family.to_owned()),
            120,
            175,
        )
        .await?;
        let mut sorted_targets = selected_plan.selected_targets.clone();
        sorted_targets.sort();
        assert_eq!(selected_plan.selected_targets, sorted_targets);
        assert_eq!(
            selected_plan
                .selected_targets
                .iter()
                .find(|target| target.contract_instance_id == selected_contract_instance_id),
            Some(&WatchedBackfillTarget {
                source_family: resolver_source_family.to_owned(),
                contract_instance_id: selected_contract_instance_id,
                address: normalize_address(selected_resolver_address),
                effective_from_block: 150,
                effective_to_block: 175,
            })
        );
        assert!(
            selected_plan
                .selected_targets
                .iter()
                .all(
                    |target| target.contract_instance_id != closed_contract_instance_id
                        && target.contract_instance_id != deactivated_contract_instance_id
                ),
            "closed and deactivated resolver discovery targets must not be selected"
        );

        for (range_start, range_end) in [(100, 149), (191, 220)] {
            let out_of_range_plan = load_watched_source_selector_plan(
                database.pool(),
                chain,
                WatchedSourceSelector::SourceFamily(resolver_source_family.to_owned()),
                range_start,
                range_end,
            )
            .await;
            if namespace == "ens" {
                let error = out_of_range_plan
                    .expect_err("ENS low historical resolver ranges have no active targets");
                assert!(
                    error.to_string().contains(
                        "watched source selector source_family ens_v1_resolver_l1 found no active watched targets"
                    ),
                    "unexpected ENS out-of-range selector error: {error:#}"
                );
            } else {
                let out_of_range_plan = out_of_range_plan?;
                assert!(
                    out_of_range_plan
                        .selected_targets
                        .iter()
                        .all(|target| target.contract_instance_id != selected_contract_instance_id),
                    "resolver target must not be selected outside the edge/address intersection"
                );
            }
        }

        database.cleanup().await?;
    }

    Ok(())
}

#[tokio::test]
async fn resolver_discovery_edges_do_not_become_transitive_registry_parents() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v1_registry_l1",
        "v3",
        &checked_in_manifest_contents("ens", "ens_v1_registry_l1", "v3")?,
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let registry_address = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E";
    let resolver_address = "0x00000000000000000000000000000000000000CC";
    let child_address = "0x00000000000000000000000000000000000000DD";
    let summary = reconcile_discovery_observations(
        database.pool(),
        "unit-test-registry-observations",
        &[
            DiscoveryObservation {
                chain: "ethereum-mainnet".to_owned(),
                from_address: registry_address.to_owned(),
                to_address: resolver_address.to_owned(),
                edge_kind: "resolver".to_owned(),
                discovery_source: "unit-test-registry-observations".to_owned(),
                active_from_block_number: Some(123),
                active_from_block_hash: Some(
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                ),
                active_to_block_number: None,
                active_to_block_hash: None,
                provenance: serde_json::json!({
                    "provider": "unit-test",
                    "observation_key": "resolver-edge",
                }),
            },
            DiscoveryObservation {
                chain: "ethereum-mainnet".to_owned(),
                from_address: resolver_address.to_owned(),
                to_address: child_address.to_owned(),
                edge_kind: "subregistry".to_owned(),
                discovery_source: "unit-test-registry-observations".to_owned(),
                active_from_block_number: Some(124),
                active_from_block_hash: Some(
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                ),
                active_to_block_number: None,
                active_to_block_hash: None,
                provenance: serde_json::json!({
                    "provider": "unit-test",
                    "observation_key": "resolver-as-parent",
                }),
            },
        ],
    )
    .await?;

    assert_eq!(summary.active_edge_count, 1);
    assert_eq!(summary.admitted_edge_count, 1);
    assert_eq!(summary.inserted_edge_count, 1);
    assert_eq!(summary.admitted_edges[0].edge_kind, "resolver");
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE edge_kind = 'subregistry' AND deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert!(
        !sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT provenance FROM discovery_edges WHERE edge_kind = 'resolver' AND deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?
        .as_object()
        .expect("resolver discovery provenance must be an object")
        .contains_key(PROPAGATED_ROLE_PROVENANCE_FIELD)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn scoped_discovery_reconciliation_keeps_unrelated_active_addresses() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v1_registry_l1",
        "v3",
        &checked_in_manifest_contents("ens", "ens_v1_registry_l1", "v3")?,
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let discovery_source = "unit-test-scoped-registry-observations";
    let registry_address = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E";
    let first_child_address = "0x0000000000000000000000000000000000000101";
    let stale_child_address = "0x0000000000000000000000000000000000000102";
    let replacement_child_address = "0x0000000000000000000000000000000000000103";
    let observation =
        |observation_key: &str, to_address: &str, block_number: i64| DiscoveryObservation {
            chain: "ethereum-mainnet".to_owned(),
            from_address: registry_address.to_owned(),
            to_address: to_address.to_owned(),
            edge_kind: "subregistry".to_owned(),
            discovery_source: discovery_source.to_owned(),
            active_from_block_number: Some(block_number),
            active_from_block_hash: Some(format!("0x{block_number:064x}")),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "observation_key": observation_key,
            }),
        };

    let initial_summary = reconcile_discovery_observations(
        database.pool(),
        discovery_source,
        &[
            observation("edge-a", first_child_address, 100),
            observation("edge-b", stale_child_address, 101),
        ],
    )
    .await?;
    assert_eq!(initial_summary.inserted_edge_count, 2);

    let first_child_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        first_child_address,
    )
    .await?;
    let stale_child_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        stale_child_address,
    )
    .await?;

    sqlx::query(
        r#"
        UPDATE discovery_edges
        SET deactivated_at = now()
        WHERE to_contract_instance_id = $1
          AND deactivated_at IS NULL
        "#,
    )
    .bind(stale_child_id)
    .execute(database.pool())
    .await?;

    let scoped_summary = reconcile_scoped_discovery_observations(
        database.pool(),
        discovery_source,
        &[observation("edge-a", replacement_child_address, 102)],
    )
    .await?;
    assert_eq!(scoped_summary.inserted_edge_count, 1);
    assert_eq!(scoped_summary.deactivated_edge_count, 1);

    let replacement_child_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        replacement_child_address,
    )
    .await?;
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM contract_instance_addresses WHERE contract_instance_id = $1 AND deactivated_at IS NULL"
        )
        .bind(first_child_id)
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM contract_instance_addresses WHERE contract_instance_id = $1 AND deactivated_at IS NULL"
        )
        .bind(replacement_child_id)
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM contract_instance_addresses WHERE contract_instance_id = $1 AND deactivated_at IS NULL"
        )
        .bind(stale_child_id)
        .fetch_one(database.pool())
        .await?,
        1
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reuses_contract_instance_ids_across_inactive_gaps() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x00000000000000000000000000000000000000AA",
            Some("0x00000000000000000000000000000000000000DD"),
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    let first_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
    )
    .await?;

    let empty_dir = TestDir::new()?;
    sync_repository(database.pool(), &load_repository(&empty_dir.path)?).await?;
    assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM contract_instance_addresses WHERE contract_instance_id = $1 AND deactivated_at IS NULL"
            )
            .bind(first_contract_instance_id)
            .fetch_one(database.pool())
            .await?,
            0
        );

    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    let reused_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
    )
    .await?;

    assert_eq!(first_contract_instance_id, reused_contract_instance_id);
    assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM contract_instance_addresses WHERE contract_instance_id = $1"
            )
            .bind(first_contract_instance_id)
            .fetch_one(database.pool())
            .await?,
            2
        );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn removing_manifest_deactivates_active_discovery_edges_from_that_source() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v1_registry_l1",
        "v1",
        &registry_manifest_contents("active"),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let persistence_summary = persist_discovery_observation(
        database.pool(),
        &DiscoveryObservation {
            chain: "ethereum-mainnet".to_owned(),
            from_address: "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E".to_owned(),
            to_address: "0x00000000000000000000000000000000000000CC".to_owned(),
            edge_kind: "subregistry".to_owned(),
            discovery_source: "manifest-removal-cleanup-test".to_owned(),
            active_from_block_number: Some(123),
            active_from_block_hash: Some(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "kind": "subregistry",
            }),
        },
    )
    .await?;
    let child_contract_instance_id = persistence_summary.admitted_edges[0]
        .to_contract_instance_id
        .expect("persisted discovery edge must admit a target contract instance");

    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );

    let empty_dir = TestDir::new()?;
    let summary = sync_repository(database.pool(), &load_repository(&empty_dir.path)?).await?;
    assert_eq!(summary.removed_manifest_count, 1);
    assert_eq!(summary.cleared_discovery_edge_count, 1);
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM contract_instance_addresses WHERE contract_instance_id = $1 AND deactivated_at IS NULL"
        )
        .bind(child_contract_instance_id)
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn persist_discovery_observation_ignores_zero_address_targets() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v1_registry_l1",
        "v1",
        &registry_manifest_contents("active"),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let summary = persist_discovery_observation(
        database.pool(),
        &DiscoveryObservation {
            chain: "ethereum-mainnet".to_owned(),
            from_address: "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E".to_owned(),
            to_address: ZERO_ADDRESS.to_owned(),
            edge_kind: "subregistry".to_owned(),
            discovery_source: "zero-address-discovery-test".to_owned(),
            active_from_block_number: Some(123),
            active_from_block_hash: Some(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "kind": "subregistry",
            }),
        },
    )
    .await?;

    assert_eq!(summary.admitted_edge_count, 0);
    assert_eq!(summary.inserted_edge_count, 0);
    assert!(summary.admitted_edges.is_empty());
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn sync_repository_rejects_invalid_proxy_shape_declarations() -> Result<()> {
    let cases = [
        (
            "proxy_kind_none_with_implementation",
            manifest_contents(
                "active",
                "0x0000000000000000000000000000000000000001",
                "0x00000000000000000000000000000000000000AA",
                Some("0x00000000000000000000000000000000000000DD"),
            )
            .replacen("proxy_kind = \"erc1967\"", "proxy_kind = \"none\"", 1),
            "cannot declare implementation when proxy_kind = \"none\"",
        ),
        (
            "proxied_contract_without_implementation",
            manifest_contents(
                "active",
                "0x0000000000000000000000000000000000000001",
                "0x00000000000000000000000000000000000000AA",
                None,
            ),
            "must declare implementation when proxy_kind = \"erc1967\"",
        ),
    ];

    for (case_name, contents, expected_error) in cases {
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;
        test_dir.write_manifest("ens", "ens_v2_registry_l1", "v1", &contents)?;

        let repository = load_repository(&test_dir.path)?;
        let error = sync_repository(database.pool(), &repository)
            .await
            .expect_err("invalid proxy shape must fail manifest sync");
        assert!(
            error.to_string().contains(expected_error),
            "unexpected sync error for {case_name}: {error:#}"
        );

        database.cleanup().await?;
    }

    Ok(())
}

#[tokio::test]
async fn shadow_execution_family_persists_without_entering_active_views() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v1_registry_l1",
        "v1",
        &registry_manifest_contents("active"),
    )?;
    test_dir.write_manifest(
        "ens",
        "ens_execution",
        "v1",
        &execution_manifest_contents("shadow"),
    )?;

    let summary = sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    assert_eq!(summary.status, ManifestSyncStatus::Synced);
    assert_eq!(summary.synced_manifest_count, 2);
    assert_eq!(summary.active_manifest_count, 1);
    assert_eq!(summary.root_count, 1);
    assert_eq!(summary.contract_count, 2);
    assert_eq!(summary.capability_count, 2);
    assert_eq!(summary.discovery_rule_count, 1);

    assert_eq!(
        load_manifest_rollout_statuses(database.pool(), "ens").await?,
        vec![
            ("ens_execution".to_owned(), "shadow".to_owned()),
            ("ens_v1_registry_l1".to_owned(), "active".to_owned()),
        ]
    );

    assert_eq!(
        load_capability_flags_for_source_family(database.pool(), "ens", "ens_v1_registry_l1")
            .await?,
        BTreeMap::from([(
            "declared_children".to_owned(),
            CapabilityFlag {
                status: CapabilitySupportStatus::Supported,
                notes: Some(
                    "registry-controlled child surfaces are authoritative inputs".to_owned(),
                ),
            },
        )])
    );
    assert_eq!(
        load_capability_flags_for_source_family(database.pool(), "ens", "ens_execution").await?,
        BTreeMap::from([(
            "verified_resolution".to_owned(),
            CapabilityFlag {
                status: CapabilitySupportStatus::Shadow,
                notes: Some(
                    "shadow execution traces and cache ownership are tracked before public verified-resolution reads ship"
                        .to_owned(),
                ),
            },
        )])
    );

    let active_manifests = load_active_manifests_for_namespace(database.pool(), "ens").await?;
    assert_eq!(active_manifests.len(), 1);
    assert_eq!(active_manifests[0].source_family, "ens_v1_registry_l1");
    assert!(
        !active_manifests[0]
            .capability_flags
            .contains_key("verified_resolution")
    );

    let watched_contracts = load_watched_contracts(database.pool()).await?;
    assert!(!watched_contracts.iter().any(|contract| {
        contract.address == normalize_address("0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe")
    }));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn active_execution_family_is_admitted_with_owned_capability_and_watch_target() -> Result<()>
{
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v1_registry_l1",
        "v1",
        &registry_manifest_contents("active"),
    )?;
    test_dir.write_manifest(
        "ens",
        "ens_execution",
        "v1",
        &execution_manifest_contents("active"),
    )?;

    let summary = sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    assert_eq!(summary.status, ManifestSyncStatus::Synced);
    assert_eq!(summary.synced_manifest_count, 2);
    assert_eq!(summary.active_manifest_count, 2);
    assert_eq!(summary.root_count, 1);
    assert_eq!(summary.contract_count, 2);
    assert_eq!(summary.capability_count, 2);
    assert_eq!(summary.discovery_rule_count, 1);

    let active_manifests = load_active_manifests_for_namespace(database.pool(), "ens").await?;
    assert_eq!(
        active_manifests
            .iter()
            .map(|manifest| manifest.source_family.as_str())
            .collect::<Vec<_>>(),
        vec!["ens_execution", "ens_v1_registry_l1"]
    );
    assert_eq!(
        active_manifests[0].capability_flags,
        BTreeMap::from([(
            "verified_resolution".to_owned(),
            CapabilityFlag {
                status: CapabilitySupportStatus::Shadow,
                notes: Some(
                    "shadow execution traces and cache ownership are tracked before public verified-resolution reads ship"
                        .to_owned(),
                ),
            },
        )])
    );
    assert_eq!(
        active_manifests[1].capability_flags,
        BTreeMap::from([(
            "declared_children".to_owned(),
            CapabilityFlag {
                status: CapabilitySupportStatus::Supported,
                notes: Some(
                    "registry-controlled child surfaces are authoritative inputs".to_owned(),
                ),
            },
        )])
    );

    let watched_contracts = load_watched_contracts(database.pool()).await?;
    assert!(watched_contracts.iter().any(|contract| {
        contract.address == normalize_address("0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe")
            && contract.source == WatchedContractSource::ManifestContract
    }));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn checked_in_reverse_manifest_is_admitted_as_authoritative_watch_target() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v1_reverse_l1",
        "v1",
        &checked_in_manifest_contents("ens", "ens_v1_reverse_l1", "v1")?,
    )?;

    let repository = load_repository(&test_dir.path)?;
    assert_eq!(repository.summary().status, ManifestLoadStatus::Loaded);
    assert_eq!(repository.manifests().len(), 1);
    assert_eq!(
        repository.manifests()[0].manifest.source_family,
        "ens_v1_reverse_l1"
    );

    let summary = sync_repository(database.pool(), &repository).await?;
    assert_eq!(summary.status, ManifestSyncStatus::Synced);
    assert_eq!(summary.active_manifest_count, 1);
    assert_eq!(summary.root_count, 0);
    assert_eq!(summary.contract_count, 1);
    assert_eq!(summary.capability_count, 0);
    assert_eq!(summary.discovery_rule_count, 0);

    assert_eq!(
        load_manifest_rollout_statuses(database.pool(), "ens").await?,
        vec![("ens_v1_reverse_l1".to_owned(), "active".to_owned())]
    );
    assert_eq!(
        load_capability_flags_for_source_family(database.pool(), "ens", "ens_v1_reverse_l1")
            .await?,
        BTreeMap::new()
    );

    let active_manifests = load_active_manifests_for_namespace(database.pool(), "ens").await?;
    assert_eq!(active_manifests.len(), 1);
    assert_eq!(active_manifests[0].source_family, "ens_v1_reverse_l1");
    assert!(active_manifests[0].capability_flags.is_empty());

    let reverse_registrar = normalize_address("0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb");
    let watched_contracts = load_watched_contracts(database.pool()).await?;
    assert_eq!(watched_contracts.len(), 1);
    assert!(watched_contracts.iter().any(|contract| {
        contract.address == reverse_registrar
            && contract.source == WatchedContractSource::ManifestContract
    }));

    assert_eq!(
        load_watched_chain_plan(database.pool()).await?,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![reverse_registrar.clone()],
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 0,
        }]
    );

    let admission_state = load_discovery_admission_state(database.pool()).await?;
    assert!(admission_state.has_authoritative_address("ethereum-mainnet", &reverse_registrar));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn checked_in_wrapper_and_resolver_manifests_admit_phase4_input_families() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    let wrapper_manifest = checked_in_manifest_contents("ens", "ens_v1_wrapper_l1", "v1")?;
    let resolver_manifest = checked_in_manifest_contents("ens", "ens_v1_resolver_l1", "v1")?;

    for citation in [
        "(upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L2 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L35 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L37 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L38 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L54 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L80 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L90 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L102 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L138 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L140 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L479 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L500 @ ens_v1@91c966f)",
    ] {
        assert!(
            wrapper_manifest.contains(citation),
            "wrapper manifest is missing upstream citation {citation}"
        );
    }
    for citation in [
        "(upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L2 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L5 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L6 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L7 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L8 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L9 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L10 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L11 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L12 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L13 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L31 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L66 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L75 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L114 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L131 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L150 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L17 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L23 @ ens_v1@91c966f)",
    ] {
        assert!(
            resolver_manifest.contains(citation),
            "resolver manifest is missing upstream citation {citation}"
        );
    }
    for admission_basis in [
        "first-party app known-resolver admissions supplied for this change",
        "(upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L32 @ ens_app_v3@7175858)",
        "public_resolver_4976fb03",
        "0x4976fb03C32e5B8cfe2b6cCB31c09Ba78EBaBa41",
        "no VersionableResolver",
        "no default coin-type fallback claim",
    ] {
        assert!(
            resolver_manifest.contains(admission_basis),
            "resolver manifest is missing admission basis {admission_basis}"
        );
    }

    test_dir.write_manifest("ens", "ens_v1_wrapper_l1", "v1", &wrapper_manifest)?;
    test_dir.write_manifest("ens", "ens_v1_resolver_l1", "v1", &resolver_manifest)?;

    let repository = load_repository(&test_dir.path)?;
    assert_eq!(repository.summary().status, ManifestLoadStatus::Loaded);
    assert_eq!(repository.manifests().len(), 2);

    let manifests_by_source_family = repository
        .manifests()
        .iter()
        .map(|loaded_manifest| {
            (
                loaded_manifest.manifest.source_family.as_str(),
                &loaded_manifest.manifest,
            )
        })
        .collect::<BTreeMap<_, _>>();

    let wrapper = manifests_by_source_family["ens_v1_wrapper_l1"];
    assert_eq!(wrapper.chain, "ethereum-mainnet");
    assert_eq!(wrapper.deployment_epoch, "ens_v1");
    assert_eq!(wrapper.rollout_status, RolloutStatus::Active);
    assert!(wrapper.roots.is_empty());
    assert!(wrapper.discovery_rules.is_empty());
    assert!(wrapper.capability_flags.is_empty());
    assert_eq!(wrapper.contracts.len(), 1);
    assert_eq!(wrapper.contracts[0].role, "name_wrapper");
    assert_eq!(
        normalize_address(&wrapper.contracts[0].address),
        "0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401"
    );
    assert_eq!(wrapper.contracts[0].proxy_kind, "none");

    let resolver = manifests_by_source_family["ens_v1_resolver_l1"];
    assert_eq!(resolver.chain, "ethereum-mainnet");
    assert_eq!(resolver.deployment_epoch, "ens_v1");
    assert_eq!(resolver.rollout_status, RolloutStatus::Active);
    assert!(resolver.roots.is_empty());
    assert!(resolver.discovery_rules.is_empty());
    assert!(resolver.capability_flags.is_empty());
    assert_eq!(resolver.contracts.len(), 7);
    let resolver_contracts = resolver
        .contracts
        .iter()
        .map(|contract| (contract.role.as_str(), normalize_address(&contract.address)))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        resolver_contracts["public_resolver"],
        "0xf29100983e058b709f3d539b0c765937b804ac15"
    );
    assert_eq!(
        resolver_contracts["public_resolver_4976fb03"],
        "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41"
    );
    assert!(
        resolver
            .contracts
            .iter()
            .all(|contract| contract.proxy_kind == "none")
    );

    let summary = sync_repository(database.pool(), &repository).await?;
    assert_eq!(summary.status, ManifestSyncStatus::Synced);
    assert_eq!(summary.synced_manifest_count, 2);
    assert_eq!(summary.active_manifest_count, 2);
    assert_eq!(summary.root_count, 0);
    assert_eq!(summary.contract_count, 8);
    assert_eq!(summary.capability_count, 0);
    assert_eq!(summary.discovery_rule_count, 0);

    assert_eq!(
        load_manifest_rollout_statuses(database.pool(), "ens").await?,
        vec![
            ("ens_v1_resolver_l1".to_owned(), "active".to_owned()),
            ("ens_v1_wrapper_l1".to_owned(), "active".to_owned()),
        ]
    );
    assert_eq!(
        load_capability_flags_for_source_family(database.pool(), "ens", "ens_v1_wrapper_l1")
            .await?,
        BTreeMap::new()
    );
    assert_eq!(
        load_capability_flags_for_source_family(database.pool(), "ens", "ens_v1_resolver_l1")
            .await?,
        BTreeMap::new()
    );

    let active_manifests = load_active_manifests_for_namespace(database.pool(), "ens").await?;
    assert_eq!(
        active_manifests
            .iter()
            .map(|manifest| manifest.source_family.as_str())
            .collect::<Vec<_>>(),
        vec!["ens_v1_resolver_l1", "ens_v1_wrapper_l1"]
    );
    assert!(
        active_manifests
            .iter()
            .all(|manifest| manifest.capability_flags.is_empty())
    );

    let wrapper_address = normalize_address("0xD4416b13d2b3a9aBae7AcD5D6C2BbDBE25686401");
    let resolver_addresses = BTreeSet::from([
        normalize_address("0xF29100983E058B709F3D539b0c765937B804AC15"),
        normalize_address("0x231b0Ee14048e9dCcD1d247744d114a4EB5E8E63"),
        normalize_address("0x4976fb03C32e5B8cfe2b6cCB31c09Ba78EBaBa41"),
        normalize_address("0xDaaF96c344f63131acadD0Ea35170E7892d3dfBA"),
        normalize_address("0x226159d592E2b063810a10Ebf6dcbADA94Ed68b8"),
        normalize_address("0x5FfC014343cd971B7eb70732021E26C35B744cc4"),
        normalize_address("0x1da022710dF5002339274AaDEe8D58218e9D6AB5"),
    ]);
    let watched_contracts = load_watched_contracts(database.pool()).await?;
    assert_eq!(watched_contracts.len(), 8);
    assert!(watched_contracts.iter().any(|contract| {
        contract.source_family == "ens_v1_wrapper_l1"
            && contract.address == wrapper_address
            && contract.source == WatchedContractSource::ManifestContract
    }));
    let watched_resolver_addresses = watched_contracts
        .iter()
        .filter(|contract| contract.source_family == "ens_v1_resolver_l1")
        .inspect(|contract| assert_eq!(contract.source, WatchedContractSource::ManifestContract))
        .map(|contract| contract.address.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(watched_resolver_addresses, resolver_addresses);

    let chain_plan = load_watched_chain_plan(database.pool()).await?;
    assert_eq!(chain_plan.len(), 1);
    assert_eq!(chain_plan[0].chain, "ethereum-mainnet");
    let expected_plan_addresses = resolver_addresses
        .iter()
        .cloned()
        .chain(std::iter::once(wrapper_address.clone()))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        chain_plan[0]
            .addresses
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        expected_plan_addresses
    );
    assert_eq!(chain_plan[0].manifest_root_entry_count, 0);
    assert_eq!(chain_plan[0].manifest_contract_entry_count, 8);
    assert_eq!(chain_plan[0].discovery_edge_entry_count, 0);

    let admission_state = load_discovery_admission_state(database.pool()).await?;
    assert!(admission_state.has_authoritative_address("ethereum-mainnet", &wrapper_address));
    for resolver_address in resolver_addresses {
        assert!(admission_state.has_authoritative_address("ethereum-mainnet", &resolver_address));
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn checked_in_basenames_manifests_reuse_l1_resolver_address_across_active_families()
-> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    for source_family in [
        "basenames_base_primary",
        "basenames_base_registrar",
        "basenames_base_registry",
        "basenames_base_resolver",
        "basenames_execution",
        "basenames_l1_compat",
    ] {
        test_dir.write_manifest(
            "basenames",
            source_family,
            "v1",
            &checked_in_manifest_contents("basenames", source_family, "v1")?,
        )?;
    }
    test_dir.write_manifest(
        "basenames",
        "basenames_execution",
        "v2",
        &checked_in_manifest_contents("basenames", "basenames_execution", "v2")?,
    )?;
    test_dir.write_manifest(
        "basenames",
        "basenames_base_registry",
        "v2",
        &checked_in_manifest_contents("basenames", "basenames_base_registry", "v2")?,
    )?;

    let repository = load_repository(&test_dir.path)?;
    assert_eq!(repository.summary().status, ManifestLoadStatus::Loaded);
    assert_eq!(repository.manifests().len(), 8);
    assert!(
        !repository
            .manifests()
            .iter()
            .any(|manifest| { manifest.manifest.source_family == "basenames_offchain" })
    );

    let summary = sync_repository(database.pool(), &repository).await?;
    assert_eq!(summary.status, ManifestSyncStatus::Synced);
    assert_eq!(summary.synced_manifest_count, 8);
    assert_eq!(summary.active_manifest_count, 6);
    assert_eq!(summary.contract_count, 10);

    let active_manifests =
        load_active_manifests_for_namespace(database.pool(), "basenames").await?;
    assert_eq!(active_manifests.len(), 6);
    assert!(active_manifests.iter().any(|manifest| {
        manifest.source_family == "basenames_l1_compat" && manifest.manifest_version == 1
    }));
    assert!(active_manifests.iter().any(|manifest| {
        manifest.source_family == "basenames_execution" && manifest.manifest_version == 2
    }));
    assert!(active_manifests.iter().any(|manifest| {
        manifest.source_family == "basenames_base_registry" && manifest.manifest_version == 2
    }));

    let shared_l1_resolver = normalize_address("0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31");
    let shared_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        &shared_l1_resolver,
    )
    .await?;

    assert_eq!(
        query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM contract_instance_addresses
            WHERE chain_id = $1
              AND address = $2
              AND deactivated_at IS NULL
            "#
        )
        .bind("ethereum-mainnet")
        .bind(&shared_l1_resolver)
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM manifest_versions mv
            JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
            WHERE mv.namespace = 'basenames'
              AND mv.chain = 'ethereum-mainnet'
              AND mv.source_family IN ('basenames_l1_compat', 'basenames_execution')
              AND mci.declaration_kind = 'contract'
              AND mci.declared_address = $1
              AND mci.contract_instance_id = $2
            "#
        )
        .bind(&shared_l1_resolver)
        .bind(shared_contract_instance_id)
        .fetch_one(database.pool())
        .await?,
        3
    );
    assert_eq!(
        query_scalar::<_, i64>(
            r#"
            SELECT COUNT(DISTINCT mci.contract_instance_id)::BIGINT
            FROM manifest_versions mv
            JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
            WHERE mv.namespace = 'basenames'
              AND mv.chain = 'ethereum-mainnet'
              AND mv.source_family IN ('basenames_l1_compat', 'basenames_execution')
              AND mci.declaration_kind = 'contract'
              AND mci.declared_address = $1
            "#
        )
        .bind(&shared_l1_resolver)
        .fetch_one(database.pool())
        .await?,
        1
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn basenames_execution_promotion_prefers_later_active_manifest_version() -> Result<()> {
    let database = TestDatabase::new().await?;
    let test_dir = TestDir::new()?;
    test_dir.write_manifest(
        "basenames",
        "basenames_execution",
        "v1",
        &checked_in_manifest_contents("basenames", "basenames_execution", "v1")?,
    )?;
    test_dir.write_manifest(
        "basenames",
        "basenames_execution",
        "v2",
        &checked_in_manifest_contents("basenames", "basenames_execution", "v2")?,
    )?;

    let summary = sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    assert_eq!(summary.status, ManifestSyncStatus::Synced);
    assert_eq!(summary.synced_manifest_count, 2);
    assert_eq!(summary.active_manifest_count, 1);
    assert_eq!(summary.contract_count, 2);
    assert_eq!(summary.capability_count, 2);

    let active_manifests =
        load_active_manifests_for_namespace(database.pool(), "basenames").await?;
    assert_eq!(active_manifests.len(), 1);
    assert_eq!(active_manifests[0].source_family, "basenames_execution");
    assert_eq!(active_manifests[0].manifest_version, 2);
    assert_eq!(
        active_manifests[0].capability_flags,
        BTreeMap::from([(
            "verified_resolution".to_owned(),
            CapabilityFlag {
                status: CapabilitySupportStatus::Supported,
                notes: Some(
                    "supports the frozen exact-surface transport-assisted direct-path public Basenames verified-resolution class"
                        .to_owned(),
                ),
            },
        )])
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn keeps_proxy_instance_stable_across_implementation_churn() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x00000000000000000000000000000000000000AA",
            Some("0x00000000000000000000000000000000000000DD"),
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let proxy_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
    )
    .await?;
    let first_implementation_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000dd",
    )
    .await?;

    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x00000000000000000000000000000000000000AA",
            Some("0x00000000000000000000000000000000000000EE"),
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let proxy_after_churn = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
    )
    .await?;
    let second_implementation_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000ee",
    )
    .await?;

    assert_eq!(proxy_contract_instance_id, proxy_after_churn);
    assert_ne!(first_implementation_id, second_implementation_id);
    assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
            )
            .bind(MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE)
            .fetch_one(database.pool())
            .await?,
            1
        );
    assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM contract_instance_addresses WHERE contract_instance_id = $1 AND deactivated_at IS NULL"
            )
            .bind(first_implementation_id)
            .fetch_one(database.pool())
            .await?,
            0
        );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn manifest_drift_views_materialize_active_alert_inputs() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    let root_address = "0x0000000000000000000000000000000000000001";
    let proxy_address = "0x00000000000000000000000000000000000000AA";
    let first_implementation = "0x00000000000000000000000000000000000000DD";
    let second_implementation = "0x00000000000000000000000000000000000000EE";
    let inactive_execution = "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe";

    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            root_address,
            proxy_address,
            Some(first_implementation),
        ),
    )?;
    test_dir.write_manifest(
        "ens",
        "ens_execution",
        "v1",
        &execution_manifest_contents("shadow"),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let proxy_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        proxy_address,
    )
    .await?;
    let first_implementation_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        first_implementation,
    )
    .await?;

    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            root_address,
            proxy_address,
            Some(second_implementation),
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let proxy_after_churn = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        proxy_address,
    )
    .await?;
    let second_implementation_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        second_implementation,
    )
    .await?;
    assert_eq!(proxy_contract_instance_id, proxy_after_churn);
    assert_ne!(first_implementation_id, second_implementation_id);

    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "ethereum-mainnet",
            block_hash: "0x1000000000000000000000000000000000000000000000000000000000000000",
            block_number: 100,
            contract_address: root_address,
            code_hash: "0xroot",
            code_byte_length: 32,
            canonicality_state: "canonical",
        },
    )
    .await?;
    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "ethereum-mainnet",
            block_hash: "0x1010000000000000000000000000000000000000000000000000000000000000",
            block_number: 101,
            contract_address: proxy_address,
            code_hash: "0xproxy-old",
            code_byte_length: 64,
            canonicality_state: "canonical",
        },
    )
    .await?;
    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "ethereum-mainnet",
            block_hash: "0x1020000000000000000000000000000000000000000000000000000000000000",
            block_number: 102,
            contract_address: proxy_address,
            code_hash: "0xproxy-current",
            code_byte_length: 65,
            canonicality_state: "finalized",
        },
    )
    .await?;
    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "ethereum-mainnet",
            block_hash: "0x1030000000000000000000000000000000000000000000000000000000000000",
            block_number: 103,
            contract_address: second_implementation,
            code_hash: "0ximpl-current",
            code_byte_length: 96,
            canonicality_state: "safe",
        },
    )
    .await?;
    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "ethereum-mainnet",
            block_hash: "0x1040000000000000000000000000000000000000000000000000000000000000",
            block_number: 104,
            contract_address: first_implementation,
            code_hash: "0ximpl-stale",
            code_byte_length: 96,
            canonicality_state: "finalized",
        },
    )
    .await?;
    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "ethereum-mainnet",
            block_hash: "0x1050000000000000000000000000000000000000000000000000000000000000",
            block_number: 105,
            contract_address: inactive_execution,
            code_hash: "0xinactive",
            code_byte_length: 128,
            canonicality_state: "finalized",
        },
    )
    .await?;
    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "ethereum-mainnet",
            block_hash: "0x1060000000000000000000000000000000000000000000000000000000000000",
            block_number: 106,
            contract_address: root_address,
            code_hash: "0xorphan-root",
            code_byte_length: 33,
            canonicality_state: "orphaned",
        },
    )
    .await?;

    let manifest_id =
        active_manifest_id_for_source_family(database.pool(), "ens", "ens_v2_registry_l1").await?;
    insert_manifest_normalized_event(
        database.pool(),
        "manifest:registry:source",
        "SourceManifestUpdated",
        "ens_v2_registry_l1",
        1,
        Some(manifest_id),
        "finalized",
    )
    .await?;
    insert_manifest_normalized_event(
        database.pool(),
        "manifest:registry:proxy",
        "ProxyImplementationChanged",
        "ens_v2_registry_l1",
        1,
        Some(manifest_id),
        "canonical",
    )
    .await?;
    insert_manifest_normalized_event(
        database.pool(),
        "manifest:registry:capability",
        "CapabilityChanged",
        "ens_v2_registry_l1",
        1,
        Some(manifest_id),
        "safe",
    )
    .await?;
    insert_manifest_normalized_event(
        database.pool(),
        "manifest:registry:orphan",
        "SourceManifestUpdated",
        "ens_v2_registry_l1",
        1,
        Some(manifest_id),
        "orphaned",
    )
    .await?;
    insert_manifest_normalized_event(
        database.pool(),
        "manifest:registry:not-alert-material",
        "NameRecordChanged",
        "ens_v2_registry_l1",
        1,
        Some(manifest_id),
        "finalized",
    )
    .await?;

    let drift_inputs = load_manifest_drift_inputs(database.pool()).await?;
    assert_eq!(drift_inputs.active_manifests.len(), 1);
    assert_eq!(
        drift_inputs.active_manifests[0].source_family,
        "ens_v2_registry_l1"
    );
    assert_eq!(
        drift_inputs.active_manifests[0].manifest_payload["rollout_status"],
        "active"
    );
    assert_eq!(
        drift_inputs.active_manifests[0].manifest_payload["source_family"],
        "ens_v2_registry_l1"
    );

    assert_eq!(drift_inputs.declared_contracts.len(), 2);
    assert!(drift_inputs.declared_contracts.iter().any(|entry| {
        entry.declaration_kind == DECLARATION_KIND_ROOT
            && entry.declaration_name == "RootRegistry"
            && entry.declared_address == normalize_address(root_address)
            && entry.implementation_contract_instance_id.is_none()
    }));
    let declared_proxy = drift_inputs
        .declared_contracts
        .iter()
        .find(|entry| entry.declaration_kind == DECLARATION_KIND_CONTRACT)
        .expect("active manifest contract declaration must be present");
    assert_eq!(declared_proxy.declaration_name, "registry");
    assert_eq!(
        declared_proxy.contract_instance_id,
        proxy_contract_instance_id
    );
    assert_eq!(
        declared_proxy.declared_address,
        normalize_address(proxy_address)
    );
    assert_eq!(declared_proxy.proxy_kind.as_deref(), Some("erc1967"));
    assert_eq!(
        declared_proxy.implementation_contract_instance_id,
        Some(second_implementation_id)
    );
    assert_eq!(
        declared_proxy.declared_implementation_address.as_deref(),
        Some(normalize_address(second_implementation).as_str())
    );

    assert_eq!(drift_inputs.proxy_implementation_edges.len(), 1);
    let proxy_edge = &drift_inputs.proxy_implementation_edges[0];
    assert_eq!(
        proxy_edge.proxy_contract_instance_id,
        proxy_contract_instance_id
    );
    assert_eq!(
        proxy_edge.implementation_contract_instance_id,
        second_implementation_id
    );
    assert_eq!(
        proxy_edge.proxy_address.as_deref(),
        Some(normalize_address(proxy_address).as_str())
    );
    assert_eq!(
        proxy_edge.implementation_address.as_deref(),
        Some(normalize_address(second_implementation).as_str())
    );
    assert_eq!(proxy_edge.proxy_kind.as_deref(), Some("erc1967"));
    assert_eq!(
        proxy_edge.admission,
        MANIFEST_PROXY_IMPLEMENTATION_ADMISSION
    );

    let code_hashes_by_address = drift_inputs
        .code_hash_observations
        .iter()
        .map(|observation| (observation.address.as_str(), observation))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(code_hashes_by_address.len(), 3);
    assert_eq!(
        code_hashes_by_address[normalize_address(root_address).as_str()].code_hash,
        "0xroot"
    );
    assert_eq!(
        code_hashes_by_address[normalize_address(proxy_address).as_str()].code_hash,
        "0xproxy-current"
    );
    assert_eq!(
        code_hashes_by_address[normalize_address(second_implementation).as_str()].code_hash,
        "0ximpl-current"
    );
    assert!(!code_hashes_by_address.contains_key(normalize_address(first_implementation).as_str()));
    assert!(!code_hashes_by_address.contains_key(normalize_address(inactive_execution).as_str()));

    assert_eq!(
        drift_inputs
            .normalized_manifest_events
            .iter()
            .map(|event| event.event_kind.as_str())
            .collect::<Vec<_>>(),
        vec![
            "CapabilityChanged",
            "ProxyImplementationChanged",
            "SourceManifestUpdated",
        ]
    );
    assert!(
        drift_inputs
            .normalized_manifest_events
            .iter()
            .all(|event| event.canonicality_state != "orphaned")
    );

    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM manifest_versions WHERE namespace = 'ens'"
        )
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
        )
        .bind(MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE)
        .fetch_one(database.pool())
        .await?,
        1
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn rotates_successor_addresses_and_persists_migration_continuity() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x00000000000000000000000000000000000000AA",
            Some("0x00000000000000000000000000000000000000DD"),
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let original_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
    )
    .await?;

    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x00000000000000000000000000000000000000BB",
            Some("0x00000000000000000000000000000000000000DD"),
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let successor_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000bb",
    )
    .await?;
    assert_ne!(
        original_contract_instance_id,
        successor_contract_instance_id
    );
    assert_eq!(
        query_scalar::<_, i64>(
            r#"
                SELECT COUNT(*)::BIGINT
                FROM discovery_edges
                WHERE discovery_source = $1
                  AND edge_kind = $2
                  AND from_contract_instance_id = $3
                  AND to_contract_instance_id = $4
                  AND deactivated_at IS NULL
                "#
        )
        .bind(MANIFEST_SUCCESSOR_DISCOVERY_SOURCE)
        .bind(MANIFEST_SUCCESSOR_EDGE_KIND)
        .bind(original_contract_instance_id)
        .bind(successor_contract_instance_id)
        .fetch_one(database.pool())
        .await?,
        1
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn watched_plan_does_not_expand_migration_edges() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x00000000000000000000000000000000000000AA",
            Some("0x00000000000000000000000000000000000000DD"),
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x00000000000000000000000000000000000000BB",
            Some("0x00000000000000000000000000000000000000DD"),
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE edge_kind = $1 AND deactivated_at IS NULL"
            )
            .bind(MANIFEST_SUCCESSOR_EDGE_KIND)
            .fetch_one(database.pool())
            .await?,
            1
        );

    let watched_summary = load_watched_contract_summary(database.pool()).await?;
    assert_eq!(watched_summary.unique_contract_count, 3);
    assert_eq!(watched_summary.manifest_root_count, 1);
    assert_eq!(watched_summary.manifest_contract_count, 1);
    assert_eq!(watched_summary.discovery_edge_count, 1);

    let watched_chain_plan = load_watched_chain_plan(database.pool()).await?;
    assert_eq!(
        watched_chain_plan,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x0000000000000000000000000000000000000001".to_owned(),
                "0x00000000000000000000000000000000000000bb".to_owned(),
                "0x00000000000000000000000000000000000000dd".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 1,
        }]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn rebuilds_watched_plan_from_active_contract_instance_address_ranges() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x00000000000000000000000000000000000000AA",
            Some("0x00000000000000000000000000000000000000DD"),
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let persistence_summary = persist_discovery_observation(
        database.pool(),
        &DiscoveryObservation {
            chain: "ethereum-mainnet".to_owned(),
            from_address: "0x00000000000000000000000000000000000000AA".to_owned(),
            to_address: "0x00000000000000000000000000000000000000CC".to_owned(),
            edge_kind: "subregistry".to_owned(),
            discovery_source: "unit-test".to_owned(),
            active_from_block_number: Some(123),
            active_from_block_hash: Some(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "kind": "subregistry",
            }),
        },
    )
    .await?;
    assert_eq!(persistence_summary.admitted_edge_count, 1);
    assert_eq!(persistence_summary.inserted_edge_count, 1);
    assert!(
        persistence_summary.admitted_edges[0]
            .to_contract_instance_id
            .is_some()
    );

    let watched_contracts = load_watched_contracts(database.pool()).await?;
    assert_eq!(watched_contracts.len(), 4);
    assert!(watched_contracts.iter().any(|contract| {
        contract.address == "0x0000000000000000000000000000000000000001"
            && contract.source == WatchedContractSource::ManifestRoot
    }));
    assert!(watched_contracts.iter().any(|contract| {
        contract.address == "0x00000000000000000000000000000000000000aa"
            && contract.source == WatchedContractSource::ManifestContract
    }));
    assert!(watched_contracts.iter().any(|contract| {
        contract.address == "0x00000000000000000000000000000000000000dd"
            && contract.source == WatchedContractSource::DiscoveryEdge
            && contract.source_family == "ens_v2_registry_l1"
    }));
    assert!(watched_contracts.iter().any(|contract| {
        contract.address == "0x00000000000000000000000000000000000000cc"
            && contract.source == WatchedContractSource::DiscoveryEdge
            && contract.source_family == "ens_v2_registry_l1"
    }));

    let watched_summary = load_watched_contract_summary(database.pool()).await?;
    assert_eq!(watched_summary.unique_contract_count, 4);
    assert_eq!(watched_summary.manifest_root_count, 1);
    assert_eq!(watched_summary.manifest_contract_count, 1);
    assert_eq!(watched_summary.discovery_edge_count, 2);

    let watched_chain_plan = load_watched_chain_plan(database.pool()).await?;
    assert_eq!(
        watched_chain_plan,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x0000000000000000000000000000000000000001".to_owned(),
                "0x00000000000000000000000000000000000000aa".to_owned(),
                "0x00000000000000000000000000000000000000cc".to_owned(),
                "0x00000000000000000000000000000000000000dd".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 2,
        }]
    );

    let selected_source_family_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v2_registry_l1".to_owned()),
        100,
        200,
    )
    .await?;
    assert_eq!(
        selected_source_family_plan.watched_chain_plan,
        watched_chain_plan[0]
    );
    assert_eq!(selected_source_family_plan.selected_targets.len(), 4);
    assert!(
        selected_source_family_plan
            .selected_targets
            .iter()
            .all(|target| target.source_family == "ens_v2_registry_l1")
    );
    let mut sorted_targets = selected_source_family_plan.selected_targets.clone();
    sorted_targets.sort();
    assert_eq!(selected_source_family_plan.selected_targets, sorted_targets);
    assert_eq!(
        selected_source_family_plan
            .selected_targets
            .iter()
            .find(|target| { target.address == "0x00000000000000000000000000000000000000cc" })
            .map(|target| (target.effective_from_block, target.effective_to_block)),
        Some((123, 200))
    );

    database.cleanup().await?;
    Ok(())
}
