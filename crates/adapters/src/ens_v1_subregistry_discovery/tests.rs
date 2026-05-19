use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use alloy_primitives::keccak256;
use anyhow::{Context, Result};
use bigname_manifests::{
    WatchedChainPlan, WatchedContractSource, WatchedSourceSelector, load_repository,
    load_watched_chain_plan, load_watched_contract_summary, load_watched_contracts,
    load_watched_source_selector_plan, sync_repository,
};
use bigname_storage::{
    CanonicalityState, RawBlock, RawLog, default_database_url, load_normalized_events_by_namespace,
    upsert_raw_blocks, upsert_raw_logs,
};
use sqlx::{
    PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
    query_scalar,
    types::{Uuid, time::OffsetDateTime},
};

use super::{
    hex_topic::{
        ZERO_ADDRESS, ZERO_NODE, child_node, hex_string, new_owner_topic0, new_resolver_topic0,
        new_ttl_topic0, normalize_address, normalize_hex_32, registry_transfer_topic0,
    },
    *,
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);
const TEST_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new() -> Result<Self> {
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "bigname-ensv1-subregistry-{}-{}-{sequence}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos()
        ));
        fs::create_dir_all(&root)
            .with_context(|| format!("failed to create test directory {}", root.display()))?;
        Ok(Self { path: root })
    }

    fn write_manifest(
        &self,
        namespace: &str,
        source_family: &str,
        version: &str,
        contents: &str,
    ) -> Result<PathBuf> {
        let directory = self.path.join(namespace).join(source_family);
        fs::create_dir_all(&directory).with_context(|| {
            format!(
                "failed to create manifest directory {}",
                directory.display()
            )
        })?;
        let path = directory.join(format!("{version}.toml"));
        fs::write(&path, contents)
            .with_context(|| format!("failed to write manifest {}", path.display()))?;
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
        let connect_options: PgConnectOptions = database_url.parse().with_context(|| {
            "failed to parse database URL for ENSv1 subregistry adapter tests".to_owned()
        })?;
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options.clone().database("postgres"))
            .await
            .context("failed to connect admin test database pool")?;

        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_ensv1_subregistry_{}_{}_{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos(),
            sequence
        );
        sqlx::query(&format!(r#"CREATE DATABASE "{database_name}""#))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let mut database_options = connect_options.database(&database_name);
        database_options = database_options.application_name("bigname-ensv1-subregistry-tests");
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(database_options)
            .await
            .with_context(|| format!("failed to connect test database {database_name}"))?;

        TEST_MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for ENSv1 subregistry adapter tests")?;

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

fn manifest_contents(include_discovery_rule: bool) -> String {
    manifest_contents_for_registry(
        "ens",
        ENS_V1_REGISTRY_SOURCE_FAMILY,
        "ethereum-mainnet",
        "ens_v1",
        "ENSRegistry",
        "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
        include_discovery_rule,
    )
}

fn manifest_contents_with_old_registry(
    current_registry_address: &str,
    old_registry_address: &str,
    current_start_block: i64,
    old_start_block: i64,
) -> String {
    format!(
        r#"
manifest_version = 3
namespace = "ens"
source_family = "{ENS_V1_REGISTRY_SOURCE_FAMILY}"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v1"
rollout_status = "active"
normalizer_version = "uts46-v1"

[capability_flags]
declared_children = "supported"

[[roots]]
name = "ENSRegistry"
address = "{current_registry_address}"
start_block = {current_start_block}

[[contracts]]
role = "registry"
address = "{current_registry_address}"
proxy_kind = "none"
start_block = {current_start_block}

[[contracts]]
role = "registry_old"
address = "{old_registry_address}"
proxy_kind = "none"
start_block = {old_start_block}

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"

[[discovery_rules]]
edge_kind = "resolver"
from_role = "registry"
admission = "reachable_from_root"
"#
    )
}

fn manifest_contents_for_registry(
    namespace: &str,
    source_family: &str,
    chain: &str,
    deployment_epoch: &str,
    root_name: &str,
    root_address: &str,
    include_discovery_rule: bool,
) -> String {
    let discovery_rule = if include_discovery_rule {
        r#"
[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"

[[discovery_rules]]
edge_kind = "resolver"
from_role = "registry"
admission = "reachable_from_root"
"#
    } else {
        r#"
[[discovery_rules]]
edge_kind = "subregistry"
from_role = "wrapper"
admission = "reachable_from_root"
"#
    };
    format!(
        r#"
manifest_version = 1
namespace = "{namespace}"
source_family = "{source_family}"
chain = "{chain}"
deployment_epoch = "{deployment_epoch}"
rollout_status = "active"
normalizer_version = "uts46-v1"

[capability_flags]
declared_children = "supported"

[[roots]]
name = "{root_name}"
address = "{root_address}"

[[contracts]]
role = "registry"
address = "{root_address}"
proxy_kind = "none"
{discovery_rule}
"#
    )
}

fn resolver_manifest_contents_for_family(
    namespace: &str,
    source_family: &str,
    chain: &str,
    deployment_epoch: &str,
) -> String {
    format!(
        r#"
manifest_version = 1
namespace = "{namespace}"
source_family = "{source_family}"
chain = "{chain}"
deployment_epoch = "{deployment_epoch}"
rollout_status = "active"
normalizer_version = "uts46-v1"
roots = []
contracts = []
discovery_rules = []

[capability_flags]
"#
    )
}

async fn insert_raw_new_owner_log(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
    emitting_address: &str,
    owner: &str,
    canonicality_state: CanonicalityState,
) -> Result<()> {
    insert_raw_new_owner_log_with_key(
        pool,
        RawNewOwnerLog {
            chain_id,
            block_hash,
            block_number,
            emitting_address,
            owner,
            parent_node: ZERO_NODE,
            label: "eth",
            canonicality_state,
        },
    )
    .await
}

struct RawNewOwnerLog<'a> {
    chain_id: &'a str,
    block_hash: &'a str,
    block_number: i64,
    emitting_address: &'a str,
    owner: &'a str,
    parent_node: &'a str,
    label: &'a str,
    canonicality_state: CanonicalityState,
}

async fn insert_raw_new_owner_log_with_key(pool: &PgPool, log: RawNewOwnerLog<'_>) -> Result<()> {
    upsert_raw_blocks(
        pool,
        &[RawBlock {
            chain_id: log.chain_id.to_owned(),
            block_hash: log.block_hash.to_owned(),
            parent_hash: None,
            block_number: log.block_number,
            block_timestamp: OffsetDateTime::UNIX_EPOCH,
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state: log.canonicality_state,
        }],
    )
    .await?;

    upsert_raw_logs(
        pool,
        &[RawLog {
            chain_id: log.chain_id.to_owned(),
            block_hash: log.block_hash.to_owned(),
            block_number: log.block_number,
            transaction_hash: format!("0xtx{:02x}", log.block_number),
            transaction_index: 0,
            log_index: 0,
            emitting_address: log.emitting_address.to_owned(),
            topics: vec![
                new_owner_topic0(),
                log.parent_node.to_owned(),
                labelhash_hex(log.label),
            ],
            data: encode_new_owner_log_data(log.owner),
            canonicality_state: log.canonicality_state,
        }],
    )
    .await?;

    Ok(())
}

struct RawNewResolverLog<'a> {
    chain_id: &'a str,
    block_hash: &'a str,
    block_number: i64,
    emitting_address: &'a str,
    resolver: &'a str,
    node: &'a str,
    canonicality_state: CanonicalityState,
}

async fn insert_raw_new_resolver_log(pool: &PgPool, log: RawNewResolverLog<'_>) -> Result<()> {
    upsert_raw_blocks(
        pool,
        &[RawBlock {
            chain_id: log.chain_id.to_owned(),
            block_hash: log.block_hash.to_owned(),
            parent_hash: None,
            block_number: log.block_number,
            block_timestamp: OffsetDateTime::UNIX_EPOCH,
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state: log.canonicality_state,
        }],
    )
    .await?;

    upsert_raw_logs(
        pool,
        &[RawLog {
            chain_id: log.chain_id.to_owned(),
            block_hash: log.block_hash.to_owned(),
            block_number: log.block_number,
            transaction_hash: format!("0xtx{:02x}", log.block_number),
            transaction_index: 0,
            log_index: 0,
            emitting_address: log.emitting_address.to_owned(),
            topics: vec![new_resolver_topic0(), normalize_hex_32(log.node)?],
            data: encode_registry_new_resolver_log_data(log.resolver),
            canonicality_state: log.canonicality_state,
        }],
    )
    .await?;

    Ok(())
}

struct RawRegistryTransferLog<'a> {
    chain_id: &'a str,
    block_hash: &'a str,
    block_number: i64,
    emitting_address: &'a str,
    owner: &'a str,
    node: &'a str,
    canonicality_state: CanonicalityState,
}

async fn insert_raw_registry_transfer_log(
    pool: &PgPool,
    log: RawRegistryTransferLog<'_>,
) -> Result<()> {
    upsert_raw_blocks(
        pool,
        &[RawBlock {
            chain_id: log.chain_id.to_owned(),
            block_hash: log.block_hash.to_owned(),
            parent_hash: None,
            block_number: log.block_number,
            block_timestamp: OffsetDateTime::UNIX_EPOCH,
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state: log.canonicality_state,
        }],
    )
    .await?;

    upsert_raw_logs(
        pool,
        &[RawLog {
            chain_id: log.chain_id.to_owned(),
            block_hash: log.block_hash.to_owned(),
            block_number: log.block_number,
            transaction_hash: format!("0xtx{:02x}", log.block_number),
            transaction_index: 0,
            log_index: 0,
            emitting_address: log.emitting_address.to_owned(),
            topics: vec![registry_transfer_topic0(), normalize_hex_32(log.node)?],
            data: encode_new_owner_log_data(log.owner),
            canonicality_state: log.canonicality_state,
        }],
    )
    .await?;

    Ok(())
}

struct RawNewTtlLog<'a> {
    chain_id: &'a str,
    block_hash: &'a str,
    block_number: i64,
    emitting_address: &'a str,
    node: &'a str,
    ttl: u64,
    canonicality_state: CanonicalityState,
}

async fn insert_raw_new_ttl_log(pool: &PgPool, log: RawNewTtlLog<'_>) -> Result<()> {
    upsert_raw_blocks(
        pool,
        &[RawBlock {
            chain_id: log.chain_id.to_owned(),
            block_hash: log.block_hash.to_owned(),
            parent_hash: None,
            block_number: log.block_number,
            block_timestamp: OffsetDateTime::UNIX_EPOCH,
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state: log.canonicality_state,
        }],
    )
    .await?;

    upsert_raw_logs(
        pool,
        &[RawLog {
            chain_id: log.chain_id.to_owned(),
            block_hash: log.block_hash.to_owned(),
            block_number: log.block_number,
            transaction_hash: format!("0xtx{:02x}", log.block_number),
            transaction_index: 0,
            log_index: 0,
            emitting_address: log.emitting_address.to_owned(),
            topics: vec![new_ttl_topic0(), normalize_hex_32(log.node)?],
            data: abi_word_u64(log.ttl).to_vec(),
            canonicality_state: log.canonicality_state,
        }],
    )
    .await?;

    Ok(())
}

async fn load_contract_instance_for_address(
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
        "#,
    )
    .bind(namespace)
    .bind(source_family)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to load active manifest_id for {source_family}"))
}

fn labelhash_hex(label: &str) -> String {
    format!("0x{}", hex_string(keccak256(label.as_bytes())))
}

fn base_eth_node() -> Result<String> {
    child_node(
        &child_node(ZERO_NODE, &labelhash_hex("eth"))?,
        &labelhash_hex("base"),
    )
}

fn encode_new_owner_log_data(owner: &str) -> Vec<u8> {
    abi_word_address(owner).to_vec()
}

fn encode_registry_new_resolver_log_data(resolver: &str) -> Vec<u8> {
    abi_word_address(resolver).to_vec()
}

fn abi_word_u64(value: u64) -> [u8; 32] {
    let mut word = [0u8; 32];
    word[24..].copy_from_slice(&value.to_be_bytes());
    word
}

fn abi_word_address(value: &str) -> [u8; 32] {
    let value = value.strip_prefix("0x").unwrap_or(value);
    assert_eq!(value.len(), 40, "test address must be 20 bytes");
    let mut word = [0u8; 32];
    for (index, chunk) in value.as_bytes().chunks(2).enumerate() {
        let hex = std::str::from_utf8(chunk).expect("test address chunk must be utf-8");
        word[12 + index] =
            u8::from_str_radix(hex, 16).expect("test address chunk must be valid hex");
    }
    word
}

#[tokio::test]
async fn canonical_new_owner_log_persists_one_active_subregistry_edge_and_expands_watch_plan()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(true))?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    insert_raw_new_owner_log(
        database.pool(),
        "ethereum-mainnet",
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        42,
        "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
        "0x00000000000000000000000000000000000000CC",
        CanonicalityState::Canonical,
    )
    .await?;

    let summary = sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(
        summary,
        EnsV1SubregistryDiscoverySyncSummary {
            scanned_log_count: 1,
            matched_log_count: 1,
            active_observation_count: 1,
            active_edge_count: 1,
            admitted_edge_count: 1,
            inserted_edge_count: 1,
            deactivated_edge_count: 0,
            total_normalized_event_count: 1,
            total_normalized_event_inserted_count: 1,
        }
    );

    let discovery_source = ens_v1_subregistry_discovery_source("ethereum-mainnet");
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
        )
        .bind(&discovery_source)
        .fetch_one(database.pool())
        .await?,
        1
    );
    let discovered_contract_instance_id = load_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000cc",
    )
    .await?;
    assert_eq!(
        query_scalar::<_, Uuid>(
            "SELECT to_contract_instance_id FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
        )
        .bind(&discovery_source)
        .fetch_one(database.pool())
        .await?,
        discovered_contract_instance_id
    );
    let normalized_events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
    assert_eq!(normalized_events.len(), 1);
    assert_eq!(
        normalized_events[0].event_kind,
        EVENT_KIND_SUBREGISTRY_CHANGED
    );
    assert_eq!(
        normalized_events[0].block_hash.as_deref(),
        Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    );
    assert_eq!(
        normalized_events[0].after_state["owner"].as_str(),
        Some("0x00000000000000000000000000000000000000cc")
    );
    assert_eq!(
        normalized_events[0].after_state["tombstone"].as_bool(),
        Some(false)
    );
    assert_eq!(
        normalized_events[0].after_state["to_contract_instance_id"].as_str(),
        Some(discovered_contract_instance_id.to_string().as_str())
    );
    sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(
        load_normalized_events_by_namespace(database.pool(), "ens")
            .await?
            .len(),
        1
    );

    let watched_summary = load_watched_contract_summary(database.pool()).await?;
    assert_eq!(watched_summary.unique_contract_count, 2);
    assert_eq!(watched_summary.manifest_root_count, 1);
    assert_eq!(watched_summary.manifest_contract_count, 1);
    assert_eq!(watched_summary.discovery_edge_count, 1);

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    assert_eq!(
        watched_plan,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x00000000000000000000000000000000000000cc".to_owned(),
                "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 1,
        }]
    );
    database.cleanup().await
}

#[tokio::test]
async fn basenames_finalized_new_owner_log_emits_basenames_subregistry_event_idempotently()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "basenames",
        BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
        "v1",
        &manifest_contents_for_registry(
            "basenames",
            BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
            "base-mainnet",
            "basenames_v1",
            "BasenamesRegistry",
            "0x00000000000000000000000000000000000000bb",
            true,
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    insert_raw_new_owner_log_with_key(
        database.pool(),
        RawNewOwnerLog {
            chain_id: "base-mainnet",
            block_hash: "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            block_number: 42,
            emitting_address: "0x00000000000000000000000000000000000000bb",
            owner: "0x00000000000000000000000000000000000000cc",
            parent_node: &base_eth_node()?,
            label: "alice",
            canonicality_state: CanonicalityState::Finalized,
        },
    )
    .await?;

    let first = sync_ens_v1_subregistry_discovery(database.pool(), "base-mainnet").await?;
    assert_eq!(
        first,
        EnsV1SubregistryDiscoverySyncSummary {
            scanned_log_count: 1,
            matched_log_count: 1,
            active_observation_count: 1,
            active_edge_count: 1,
            admitted_edge_count: 1,
            inserted_edge_count: 1,
            deactivated_edge_count: 0,
            total_normalized_event_count: 1,
            total_normalized_event_inserted_count: 1,
        }
    );

    let second = sync_ens_v1_subregistry_discovery(database.pool(), "base-mainnet").await?;
    assert_eq!(
        second,
        EnsV1SubregistryDiscoverySyncSummary {
            scanned_log_count: 1,
            matched_log_count: 1,
            active_observation_count: 1,
            active_edge_count: 1,
            admitted_edge_count: 1,
            inserted_edge_count: 0,
            deactivated_edge_count: 0,
            total_normalized_event_count: 1,
            total_normalized_event_inserted_count: 0,
        }
    );

    let discovery_source = ens_v1_subregistry_discovery_source("base-mainnet");
    let parent_node = base_eth_node()?;
    let discovered_contract_instance_id = load_contract_instance_for_address(
        database.pool(),
        "base-mainnet",
        "0x00000000000000000000000000000000000000cc",
    )
    .await?;
    let normalized_events =
        load_normalized_events_by_namespace(database.pool(), "basenames").await?;
    assert_eq!(normalized_events.len(), 1);
    assert_eq!(normalized_events[0].namespace, "basenames");
    assert_eq!(
        normalized_events[0].canonicality_state,
        CanonicalityState::Finalized
    );
    assert_eq!(
        normalized_events[0].after_state["parent_node"].as_str(),
        Some(parent_node.as_str())
    );
    assert_eq!(
        normalized_events[0].after_state["active_edge"].as_bool(),
        Some(true)
    );
    assert_eq!(
        normalized_events[0].after_state["to_contract_instance_id"].as_str(),
        Some(discovered_contract_instance_id.to_string().as_str())
    );
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
        )
        .bind(&discovery_source)
        .fetch_one(database.pool())
        .await?,
        1
    );

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    assert_eq!(
        watched_plan,
        vec![WatchedChainPlan {
            chain: "base-mainnet".to_owned(),
            addresses: vec![
                "0x00000000000000000000000000000000000000bb".to_owned(),
                "0x00000000000000000000000000000000000000cc".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 1,
        }]
    );
    database.cleanup().await
}

#[tokio::test]
async fn canonical_new_resolver_log_persists_resolver_edge_without_profile_support() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(true))?;
    test_dir.write_manifest(
        "ens",
        ENS_V1_RESOLVER_SOURCE_FAMILY,
        "v1",
        &resolver_manifest_contents_for_family(
            "ens",
            ENS_V1_RESOLVER_SOURCE_FAMILY,
            "ethereum-mainnet",
            "ens_v1",
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    let registry_manifest_id =
        active_manifest_id_for_source_family(database.pool(), "ens", ENS_V1_REGISTRY_SOURCE_FAMILY)
            .await?;
    let resolver_manifest_id =
        active_manifest_id_for_source_family(database.pool(), "ens", ENS_V1_RESOLVER_SOURCE_FAMILY)
            .await?;
    let node = child_node(ZERO_NODE, &labelhash_hex("eth"))?;
    insert_raw_new_resolver_log(
        database.pool(),
        RawNewResolverLog {
            chain_id: "ethereum-mainnet",
            block_hash: "0x9999999999999999999999999999999999999999999999999999999999999999",
            block_number: 58,
            emitting_address: "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            resolver: "0x00000000000000000000000000000000000000CC",
            node: &node,
            canonicality_state: CanonicalityState::Canonical,
        },
    )
    .await?;

    let summary = sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(
        summary,
        EnsV1SubregistryDiscoverySyncSummary {
            scanned_log_count: 1,
            matched_log_count: 1,
            active_observation_count: 1,
            active_edge_count: 1,
            admitted_edge_count: 1,
            inserted_edge_count: 1,
            deactivated_edge_count: 0,
            total_normalized_event_count: 1,
            total_normalized_event_inserted_count: 1,
        }
    );

    let discovery_source = ens_v1_resolver_discovery_source("ethereum-mainnet");
    let discovered_contract_instance_id = load_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000cc",
    )
    .await?;
    let discovery_edge = sqlx::query(
        r#"
        SELECT to_contract_instance_id, source_manifest_id, provenance
        FROM discovery_edges
        WHERE discovery_source = $1
          AND edge_kind = 'resolver'
          AND deactivated_at IS NULL
        "#,
    )
    .bind(&discovery_source)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        discovery_edge.try_get::<Uuid, _>("to_contract_instance_id")?,
        discovered_contract_instance_id
    );
    assert_eq!(
        discovery_edge
            .try_get::<Option<i64>, _>("source_manifest_id")?
            .expect("resolver edge must retain registry source manifest provenance"),
        registry_manifest_id
    );
    assert!(
        !discovery_edge
            .try_get::<serde_json::Value, _>("provenance")?
            .as_object()
            .expect("resolver discovery provenance must be an object")
            .contains_key("propagated_role")
    );

    let normalized_events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
    assert_eq!(normalized_events.len(), 1);
    assert_eq!(normalized_events[0].event_kind, EVENT_KIND_RESOLVER_CHANGED);
    assert_eq!(
        normalized_events[0].source_family,
        ENS_V1_REGISTRY_SOURCE_FAMILY
    );
    assert_eq!(
        normalized_events[0].derivation_kind,
        DERIVATION_KIND_ENS_V1_REGISTRY_RESOLVER_CHANGED
    );
    assert_eq!(
        normalized_events[0].after_state["resolver"].as_str(),
        Some("0x00000000000000000000000000000000000000cc")
    );
    assert_eq!(
        normalized_events[0].after_state["resolver_profile_supported"].as_bool(),
        Some(false)
    );
    assert_eq!(
        normalized_events[0].after_state["resolver_profile_status"].as_str(),
        Some("unsupported")
    );
    assert_eq!(
        normalized_events[0].after_state["active_edge"].as_bool(),
        Some(true)
    );
    assert_eq!(
        normalized_events[0].after_state["to_contract_instance_id"].as_str(),
        Some(discovered_contract_instance_id.to_string().as_str())
    );

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    assert_eq!(
        watched_plan,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x00000000000000000000000000000000000000cc".to_owned(),
                "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 1,
        }]
    );
    let watched_contracts = load_watched_contracts(database.pool()).await?;
    assert!(watched_contracts.iter().any(|contract| {
        contract.chain == "ethereum-mainnet"
            && contract.address == "0x00000000000000000000000000000000000000cc"
            && contract.source == WatchedContractSource::DiscoveryEdge
            && contract.source_family == ENS_V1_RESOLVER_SOURCE_FAMILY
            && contract.source_manifest_id == Some(resolver_manifest_id)
    }));
    let resolver_source_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily(ENS_V1_RESOLVER_SOURCE_FAMILY.to_owned()),
        58,
        58,
    )
    .await?;
    assert_eq!(resolver_source_plan.selected_targets.len(), 1);
    assert_eq!(
        resolver_source_plan.selected_targets[0].source_family,
        ENS_V1_RESOLVER_SOURCE_FAMILY
    );
    assert_eq!(
        resolver_source_plan.selected_targets[0].contract_instance_id,
        discovered_contract_instance_id
    );

    database.cleanup().await
}

#[tokio::test]
async fn block_hash_replay_sync_skips_discovery_reconciliation_and_unselected_registry_logs()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(true))?;
    test_dir.write_manifest(
        "ens",
        ENS_V1_RESOLVER_SOURCE_FAMILY,
        "v1",
        &resolver_manifest_contents_for_family(
            "ens",
            ENS_V1_RESOLVER_SOURCE_FAMILY,
            "ethereum-mainnet",
            "ens_v1",
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    insert_raw_new_resolver_log(
        database.pool(),
        RawNewResolverLog {
            chain_id: "ethereum-mainnet",
            block_hash: "0x4242424242424242424242424242424242424242424242424242424242424242",
            block_number: 42,
            emitting_address: "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            resolver: "0x00000000000000000000000000000000000000cc",
            node: ZERO_NODE,
            canonicality_state: CanonicalityState::Canonical,
        },
    )
    .await?;
    insert_raw_new_resolver_log(
        database.pool(),
        RawNewResolverLog {
            chain_id: "ethereum-mainnet",
            block_hash: "0x4343434343434343434343434343434343434343434343434343434343434343",
            block_number: 43,
            emitting_address: "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            resolver: "0x00000000000000000000000000000000000000dd",
            node: &labelhash_hex("unselected"),
            canonicality_state: CanonicalityState::Canonical,
        },
    )
    .await?;

    let summary =
        EnsV1SubregistryDiscoverySyncSummary::sync_for_block_hashes_without_discovery_reconciliation(
            database.pool(),
            "ethereum-mainnet",
            &["0x4242424242424242424242424242424242424242424242424242424242424242"
                .to_owned()],
        )
        .await?;
    assert_eq!(
        summary,
        EnsV1SubregistryDiscoverySyncSummary {
            scanned_log_count: 1,
            matched_log_count: 1,
            active_observation_count: 1,
            active_edge_count: 0,
            admitted_edge_count: 0,
            inserted_edge_count: 0,
            deactivated_edge_count: 0,
            total_normalized_event_count: 0,
            total_normalized_event_inserted_count: 0,
        }
    );
    assert_eq!(
        query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM discovery_edges")
            .fetch_one(database.pool())
            .await?,
        0
    );
    assert!(
        load_normalized_events_by_namespace(database.pool(), "ens")
            .await?
            .is_empty()
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_registry_old_seeds_before_current_and_later_old_new_owner_is_suppressed() -> Result<()>
{
    let _permit = crate::acquire_test_db_permit().await;
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    let current_registry = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E";
    let old_registry = "0x314159265dd8dbb310642f98f50c066173c1259b";

    test_dir.write_manifest(
        "ens",
        "ens_v1_registry_l1",
        "v3",
        &manifest_contents_with_old_registry(current_registry, old_registry, 10, 1),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    insert_raw_new_owner_log_with_key(
        database.pool(),
        RawNewOwnerLog {
            chain_id: "ethereum-mainnet",
            block_hash: "0x0101010101010101010101010101010101010101010101010101010101010101",
            block_number: 9,
            emitting_address: old_registry,
            owner: "0x00000000000000000000000000000000000000aa",
            parent_node: ZERO_NODE,
            label: "eth",
            canonicality_state: CanonicalityState::Canonical,
        },
    )
    .await?;

    let seeded = sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(seeded.scanned_log_count, 1);
    assert_eq!(seeded.matched_log_count, 1);
    assert_eq!(
        query_scalar::<_, String>(
            "SELECT address FROM contract_instance_addresses WHERE contract_instance_id = (
                SELECT to_contract_instance_id FROM discovery_edges
                WHERE discovery_source = $1 AND deactivated_at IS NULL
                LIMIT 1
            )"
        )
        .bind(ens_v1_subregistry_discovery_source("ethereum-mainnet"))
        .fetch_one(database.pool())
        .await?,
        "0x00000000000000000000000000000000000000aa".to_owned()
    );

    insert_raw_new_owner_log_with_key(
        database.pool(),
        RawNewOwnerLog {
            chain_id: "ethereum-mainnet",
            block_hash: "0x0202020202020202020202020202020202020202020202020202020202020202",
            block_number: 10,
            emitting_address: current_registry,
            owner: "0x00000000000000000000000000000000000000bb",
            parent_node: ZERO_NODE,
            label: "eth",
            canonicality_state: CanonicalityState::Canonical,
        },
    )
    .await?;
    insert_raw_new_owner_log_with_key(
        database.pool(),
        RawNewOwnerLog {
            chain_id: "ethereum-mainnet",
            block_hash: "0x0303030303030303030303030303030303030303030303030303030303030303",
            block_number: 11,
            emitting_address: old_registry,
            owner: "0x00000000000000000000000000000000000000cc",
            parent_node: ZERO_NODE,
            label: "eth",
            canonicality_state: CanonicalityState::Canonical,
        },
    )
    .await?;

    let guarded = sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(guarded.scanned_log_count, 3);
    assert_eq!(guarded.matched_log_count, 2);
    assert_eq!(
        query_scalar::<_, String>(
            "SELECT address FROM contract_instance_addresses WHERE contract_instance_id = (
                SELECT to_contract_instance_id FROM discovery_edges
                WHERE discovery_source = $1 AND deactivated_at IS NULL
                LIMIT 1
            )"
        )
        .bind(ens_v1_subregistry_discovery_source("ethereum-mainnet"))
        .fetch_one(database.pool())
        .await?,
        "0x00000000000000000000000000000000000000bb".to_owned()
    );
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges de
             JOIN contract_instance_addresses cia
               ON cia.contract_instance_id = de.to_contract_instance_id
              AND cia.address = '0x00000000000000000000000000000000000000cc'
             WHERE de.deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );

    let replay = sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(replay.inserted_edge_count, 0);
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE event_kind = 'SubregistryChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        2
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_registry_old_non_root_resolver_transfer_and_ttl_after_migration_are_suppressed()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    let current_registry = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E";
    let old_registry = "0x314159265dd8dbb310642f98f50c066173c1259b";

    test_dir.write_manifest(
        "ens",
        "ens_v1_registry_l1",
        "v3",
        &manifest_contents_with_old_registry(current_registry, old_registry, 10, 1),
    )?;
    test_dir.write_manifest(
        "ens",
        ENS_V1_RESOLVER_SOURCE_FAMILY,
        "v1",
        &resolver_manifest_contents_for_family(
            "ens",
            ENS_V1_RESOLVER_SOURCE_FAMILY,
            "ethereum-mainnet",
            "ens_v1",
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let node = child_node(ZERO_NODE, &labelhash_hex("eth"))?;
    insert_raw_new_owner_log_with_key(
        database.pool(),
        RawNewOwnerLog {
            chain_id: "ethereum-mainnet",
            block_hash: "0x1111111111111111111111111111111111111111111111111111111111111111",
            block_number: 10,
            emitting_address: current_registry,
            owner: "0x00000000000000000000000000000000000000bb",
            parent_node: ZERO_NODE,
            label: "eth",
            canonicality_state: CanonicalityState::Safe,
        },
    )
    .await?;
    insert_raw_new_resolver_log(
        database.pool(),
        RawNewResolverLog {
            chain_id: "ethereum-mainnet",
            block_hash: "0x1212121212121212121212121212121212121212121212121212121212121212",
            block_number: 11,
            emitting_address: old_registry,
            resolver: "0x00000000000000000000000000000000000000dd",
            node: &node,
            canonicality_state: CanonicalityState::Canonical,
        },
    )
    .await?;
    insert_raw_registry_transfer_log(
        database.pool(),
        RawRegistryTransferLog {
            chain_id: "ethereum-mainnet",
            block_hash: "0x1313131313131313131313131313131313131313131313131313131313131313",
            block_number: 12,
            emitting_address: old_registry,
            owner: "0x00000000000000000000000000000000000000ee",
            node: &node,
            canonicality_state: CanonicalityState::Finalized,
        },
    )
    .await?;
    insert_raw_new_ttl_log(
        database.pool(),
        RawNewTtlLog {
            chain_id: "ethereum-mainnet",
            block_hash: "0x1414141414141414141414141414141414141414141414141414141414141414",
            block_number: 13,
            emitting_address: old_registry,
            node: &node,
            ttl: 3600,
            canonicality_state: CanonicalityState::Canonical,
        },
    )
    .await?;

    let summary = sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.scanned_log_count, 4);
    assert_eq!(summary.matched_log_count, 1);
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE edge_kind = 'resolver' AND deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_registry_old_root_resolver_exception_feeds_current_registry_resolver_discovery()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    let current_registry = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E";
    let old_registry = "0x314159265dd8dbb310642f98f50c066173c1259b";

    test_dir.write_manifest(
        "ens",
        "ens_v1_registry_l1",
        "v3",
        &manifest_contents_with_old_registry(current_registry, old_registry, 10, 1),
    )?;
    test_dir.write_manifest(
        "ens",
        ENS_V1_RESOLVER_SOURCE_FAMILY,
        "v1",
        &resolver_manifest_contents_for_family(
            "ens",
            ENS_V1_RESOLVER_SOURCE_FAMILY,
            "ethereum-mainnet",
            "ens_v1",
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    insert_raw_new_resolver_log(
        database.pool(),
        RawNewResolverLog {
            chain_id: "ethereum-mainnet",
            block_hash: "0x2121212121212121212121212121212121212121212121212121212121212121",
            block_number: 11,
            emitting_address: old_registry,
            resolver: "0x00000000000000000000000000000000000000dd",
            node: ZERO_NODE,
            canonicality_state: CanonicalityState::Canonical,
        },
    )
    .await?;

    let summary = sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.scanned_log_count, 1);
    assert_eq!(summary.matched_log_count, 1);
    assert_eq!(summary.active_edge_count, 1);

    let current_registry_instance =
        load_contract_instance_for_address(database.pool(), "ethereum-mainnet", current_registry)
            .await?;
    let resolver_instance = load_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000dd",
    )
    .await?;
    let discovery_edge = sqlx::query(
        "SELECT from_contract_instance_id, to_contract_instance_id, provenance
         FROM discovery_edges
         WHERE edge_kind = 'resolver' AND deactivated_at IS NULL",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        discovery_edge.try_get::<Uuid, _>("from_contract_instance_id")?,
        current_registry_instance
    );
    assert_eq!(
        discovery_edge.try_get::<Uuid, _>("to_contract_instance_id")?,
        resolver_instance
    );
    let provenance = discovery_edge.try_get::<serde_json::Value, _>("provenance")?;
    assert_eq!(
        provenance["ens_registry_old_root_resolver_exception"].as_bool(),
        Some(true)
    );
    assert_eq!(
        provenance["emitting_address"].as_str(),
        Some("0x314159265dd8dbb310642f98f50c066173c1259b")
    );

    let normalized_events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
    assert_eq!(normalized_events.len(), 1);
    assert_eq!(normalized_events[0].event_kind, EVENT_KIND_RESOLVER_CHANGED);
    assert_eq!(
        normalized_events[0].after_state["node"].as_str(),
        Some(ZERO_NODE)
    );
    assert_eq!(
        normalized_events[0].after_state["emitting_address"].as_str(),
        Some("0x314159265dd8dbb310642f98f50c066173c1259b")
    );
    assert_eq!(
        normalized_events[0].after_state["from_contract_instance_id"].as_str(),
        Some(current_registry_instance.to_string().as_str())
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_registry_old_raw_loading_respects_manifest_start_blocks() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    let current_registry = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E";
    let old_registry = "0x314159265dd8dbb310642f98f50c066173c1259b";

    test_dir.write_manifest(
        "ens",
        "ens_v1_registry_l1",
        "v3",
        &manifest_contents_with_old_registry(current_registry, old_registry, 100, 50),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    insert_raw_new_owner_log_with_key(
        database.pool(),
        RawNewOwnerLog {
            chain_id: "ethereum-mainnet",
            block_hash: "0x3131313131313131313131313131313131313131313131313131313131313131",
            block_number: 49,
            emitting_address: old_registry,
            owner: "0x00000000000000000000000000000000000000aa",
            parent_node: ZERO_NODE,
            label: "eth",
            canonicality_state: CanonicalityState::Canonical,
        },
    )
    .await?;
    insert_raw_new_owner_log_with_key(
        database.pool(),
        RawNewOwnerLog {
            chain_id: "ethereum-mainnet",
            block_hash: "0x3232323232323232323232323232323232323232323232323232323232323232",
            block_number: 50,
            emitting_address: old_registry,
            owner: "0x00000000000000000000000000000000000000bb",
            parent_node: ZERO_NODE,
            label: "eth",
            canonicality_state: CanonicalityState::Safe,
        },
    )
    .await?;
    insert_raw_new_owner_log_with_key(
        database.pool(),
        RawNewOwnerLog {
            chain_id: "ethereum-mainnet",
            block_hash: "0x3333333333333333333333333333333333333333333333333333333333333333",
            block_number: 99,
            emitting_address: current_registry,
            owner: "0x00000000000000000000000000000000000000cc",
            parent_node: ZERO_NODE,
            label: "eth",
            canonicality_state: CanonicalityState::Finalized,
        },
    )
    .await?;
    insert_raw_new_owner_log_with_key(
        database.pool(),
        RawNewOwnerLog {
            chain_id: "ethereum-mainnet",
            block_hash: "0x3434343434343434343434343434343434343434343434343434343434343434",
            block_number: 100,
            emitting_address: current_registry,
            owner: "0x00000000000000000000000000000000000000dd",
            parent_node: ZERO_NODE,
            label: "eth",
            canonicality_state: CanonicalityState::Canonical,
        },
    )
    .await?;

    let summary = sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.scanned_log_count, 2);
    assert_eq!(summary.matched_log_count, 2);
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE block_number IN (49, 99)"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn new_resolver_target_is_not_registry_discovery_emitter() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(true))?;
    test_dir.write_manifest(
        "ens",
        ENS_V1_RESOLVER_SOURCE_FAMILY,
        "v1",
        &resolver_manifest_contents_for_family(
            "ens",
            ENS_V1_RESOLVER_SOURCE_FAMILY,
            "ethereum-mainnet",
            "ens_v1",
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let node = child_node(ZERO_NODE, &labelhash_hex("eth"))?;
    let resolver_address = "0x00000000000000000000000000000000000000CC";
    insert_raw_new_resolver_log(
        database.pool(),
        RawNewResolverLog {
            chain_id: "ethereum-mainnet",
            block_hash: "0x8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a",
            block_number: 70,
            emitting_address: "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            resolver: resolver_address,
            node: &node,
            canonicality_state: CanonicalityState::Canonical,
        },
    )
    .await?;
    insert_raw_new_owner_log(
        database.pool(),
        "ethereum-mainnet",
        "0x8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b",
        71,
        resolver_address,
        "0x00000000000000000000000000000000000000DD",
        CanonicalityState::Canonical,
    )
    .await?;

    let first = sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(first.scanned_log_count, 1);
    assert_eq!(first.active_edge_count, 1);
    assert_eq!(first.inserted_edge_count, 1);

    let second = sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(second.scanned_log_count, 1);
    assert_eq!(second.active_edge_count, 1);
    assert_eq!(second.inserted_edge_count, 0);
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE edge_kind = 'subregistry' AND deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE edge_kind = 'resolver' AND deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );

    database.cleanup().await
}

#[tokio::test]
async fn basenames_new_resolver_log_admits_resolver_watch_target_without_profile_support()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "basenames",
        BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
        "v1",
        &manifest_contents_for_registry(
            "basenames",
            BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
            "base-mainnet",
            "basenames_v1",
            "BasenamesRegistry",
            "0x00000000000000000000000000000000000000bb",
            true,
        ),
    )?;
    test_dir.write_manifest(
        "basenames",
        BASENAMES_BASE_RESOLVER_SOURCE_FAMILY,
        "v1",
        &resolver_manifest_contents_for_family(
            "basenames",
            BASENAMES_BASE_RESOLVER_SOURCE_FAMILY,
            "base-mainnet",
            "basenames_v1",
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    let registry_manifest_id = active_manifest_id_for_source_family(
        database.pool(),
        "basenames",
        BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
    )
    .await?;
    let resolver_manifest_id = active_manifest_id_for_source_family(
        database.pool(),
        "basenames",
        BASENAMES_BASE_RESOLVER_SOURCE_FAMILY,
    )
    .await?;
    let node = child_node(&base_eth_node()?, &labelhash_hex("alice"))?;
    insert_raw_new_resolver_log(
        database.pool(),
        RawNewResolverLog {
            chain_id: "base-mainnet",
            block_hash: "0x9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a",
            block_number: 59,
            emitting_address: "0x00000000000000000000000000000000000000bb",
            resolver: "0x00000000000000000000000000000000000000cc",
            node: &node,
            canonicality_state: CanonicalityState::Finalized,
        },
    )
    .await?;

    let summary = sync_ens_v1_subregistry_discovery(database.pool(), "base-mainnet").await?;
    assert_eq!(summary.scanned_log_count, 1);
    assert_eq!(summary.matched_log_count, 1);
    assert_eq!(summary.active_observation_count, 1);
    assert_eq!(summary.active_edge_count, 1);
    assert_eq!(summary.admitted_edge_count, 1);
    assert_eq!(summary.inserted_edge_count, 1);

    let normalized_events =
        load_normalized_events_by_namespace(database.pool(), "basenames").await?;
    assert_eq!(normalized_events.len(), 1);
    assert_eq!(normalized_events[0].namespace, "basenames");
    assert_eq!(normalized_events[0].event_kind, EVENT_KIND_RESOLVER_CHANGED);
    assert_eq!(
        normalized_events[0].canonicality_state,
        CanonicalityState::Finalized
    );
    assert_eq!(
        normalized_events[0].source_family,
        BASENAMES_BASE_REGISTRY_SOURCE_FAMILY
    );
    assert_eq!(
        normalized_events[0].after_state["node"].as_str(),
        Some(node.as_str())
    );
    assert_eq!(
        normalized_events[0].after_state["resolver_profile_supported"].as_bool(),
        Some(false)
    );

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    assert_eq!(
        watched_plan,
        vec![WatchedChainPlan {
            chain: "base-mainnet".to_owned(),
            addresses: vec![
                "0x00000000000000000000000000000000000000bb".to_owned(),
                "0x00000000000000000000000000000000000000cc".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 1,
        }]
    );
    let discovered_contract_instance_id = load_contract_instance_for_address(
        database.pool(),
        "base-mainnet",
        "0x00000000000000000000000000000000000000cc",
    )
    .await?;
    let discovery_edge = sqlx::query(
        r#"
        SELECT to_contract_instance_id, source_manifest_id, provenance
        FROM discovery_edges
        WHERE discovery_source = $1
          AND edge_kind = 'resolver'
          AND deactivated_at IS NULL
        "#,
    )
    .bind(ens_v1_resolver_discovery_source("base-mainnet"))
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        discovery_edge.try_get::<Uuid, _>("to_contract_instance_id")?,
        discovered_contract_instance_id
    );
    assert_eq!(
        discovery_edge
            .try_get::<Option<i64>, _>("source_manifest_id")?
            .expect("resolver edge must retain registry source manifest provenance"),
        registry_manifest_id
    );
    assert!(
        !discovery_edge
            .try_get::<serde_json::Value, _>("provenance")?
            .as_object()
            .expect("resolver discovery provenance must be an object")
            .contains_key("propagated_role")
    );
    let watched_contracts = load_watched_contracts(database.pool()).await?;
    assert!(watched_contracts.iter().any(|contract| {
        contract.chain == "base-mainnet"
            && contract.address == "0x00000000000000000000000000000000000000cc"
            && contract.source == WatchedContractSource::DiscoveryEdge
            && contract.source_family == BASENAMES_BASE_RESOLVER_SOURCE_FAMILY
            && contract.source_manifest_id == Some(resolver_manifest_id)
    }));
    let resolver_source_plan = load_watched_source_selector_plan(
        database.pool(),
        "base-mainnet",
        WatchedSourceSelector::SourceFamily(BASENAMES_BASE_RESOLVER_SOURCE_FAMILY.to_owned()),
        59,
        59,
    )
    .await?;
    assert_eq!(resolver_source_plan.selected_targets.len(), 1);
    assert_eq!(
        resolver_source_plan.selected_targets[0].source_family,
        BASENAMES_BASE_RESOLVER_SOURCE_FAMILY
    );
    assert_eq!(
        resolver_source_plan.selected_targets[0].contract_instance_id,
        discovered_contract_instance_id
    );

    database.cleanup().await
}

#[tokio::test]
async fn zero_resolver_update_closes_resolver_edge_without_closing_subregistry_edge() -> Result<()>
{
    let _permit = crate::acquire_test_db_permit().await;
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(true))?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    let node = child_node(ZERO_NODE, &labelhash_hex("eth"))?;
    insert_raw_new_owner_log(
        database.pool(),
        "ethereum-mainnet",
        "0x9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b9b",
        60,
        "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
        "0x00000000000000000000000000000000000000DD",
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_new_resolver_log(
        database.pool(),
        RawNewResolverLog {
            chain_id: "ethereum-mainnet",
            block_hash: "0x9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c",
            block_number: 61,
            emitting_address: "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            resolver: "0x00000000000000000000000000000000000000CC",
            node: &node,
            canonicality_state: CanonicalityState::Canonical,
        },
    )
    .await?;
    sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;

    insert_raw_new_resolver_log(
        database.pool(),
        RawNewResolverLog {
            chain_id: "ethereum-mainnet",
            block_hash: "0x9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d",
            block_number: 62,
            emitting_address: "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            resolver: ZERO_ADDRESS,
            node: &node,
            canonicality_state: CanonicalityState::Canonical,
        },
    )
    .await?;

    let summary = sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.active_observation_count, 1);
    assert_eq!(summary.active_edge_count, 1);
    assert_eq!(summary.inserted_edge_count, 0);
    assert_eq!(summary.deactivated_edge_count, 1);
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE edge_kind = 'subregistry' AND deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE edge_kind = 'resolver' AND deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );

    let resolver_tombstone = sqlx::query(
        r#"
        SELECT active_to_block_number, active_to_block_hash
        FROM discovery_edges
        WHERE edge_kind = 'resolver'
          AND deactivated_at IS NOT NULL
        ORDER BY discovery_edge_id DESC
        LIMIT 1
        "#,
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        resolver_tombstone.try_get::<Option<i64>, _>("active_to_block_number")?,
        Some(62)
    );
    assert_eq!(
        resolver_tombstone.try_get::<Option<String>, _>("active_to_block_hash")?,
        Some("0x9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d".to_owned())
    );
    let normalized_events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
    assert_eq!(normalized_events.len(), 3);
    assert_eq!(normalized_events[2].event_kind, EVENT_KIND_RESOLVER_CHANGED);
    assert!(normalized_events[2].after_state["resolver"].is_null());
    assert_eq!(
        normalized_events[2].after_state["raw_resolver"].as_str(),
        Some(ZERO_ADDRESS)
    );
    assert_eq!(
        normalized_events[2].after_state["active_edge"].as_bool(),
        Some(false)
    );

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    assert_eq!(
        watched_plan,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x00000000000000000000000000000000000000dd".to_owned(),
                "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 1,
        }]
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_subregistry_discovery_extends_transitively_from_discovered_subregistries()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(true))?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    insert_raw_new_owner_log(
        database.pool(),
        "ethereum-mainnet",
        "0x1111111111111111111111111111111111111111111111111111111111111111",
        50,
        "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
        "0x00000000000000000000000000000000000000CC",
        CanonicalityState::Canonical,
    )
    .await?;
    sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;

    let first_child_node = child_node(ZERO_NODE, &labelhash_hex("eth"))?;
    insert_raw_new_owner_log_with_key(
        database.pool(),
        RawNewOwnerLog {
            chain_id: "ethereum-mainnet",
            block_hash: "0x2222222222222222222222222222222222222222222222222222222222222222",
            block_number: 51,
            emitting_address: "0x00000000000000000000000000000000000000CC",
            owner: "0x00000000000000000000000000000000000000DD",
            parent_node: &first_child_node,
            label: "sub",
            canonicality_state: CanonicalityState::Canonical,
        },
    )
    .await?;

    let summary = sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.active_observation_count, 2);
    assert_eq!(summary.active_edge_count, 2);
    assert_eq!(summary.admitted_edge_count, 2);
    assert_eq!(summary.inserted_edge_count, 1);
    assert_eq!(summary.deactivated_edge_count, 0);

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    assert_eq!(
        watched_plan,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x00000000000000000000000000000000000000cc".to_owned(),
                "0x00000000000000000000000000000000000000dd".to_owned(),
                "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 2,
        }]
    );
    let normalized_events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
    assert_eq!(normalized_events.len(), 2);
    assert_eq!(
        normalized_events[1].after_state["emitting_address"].as_str(),
        Some("0x00000000000000000000000000000000000000cc")
    );
    assert_eq!(
        normalized_events[1].after_state["owner"].as_str(),
        Some("0x00000000000000000000000000000000000000dd")
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_subregistry_discovery_accepts_finalized_logs() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(true))?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    insert_raw_new_owner_log(
        database.pool(),
        "ethereum-mainnet",
        "0x3333333333333333333333333333333333333333333333333333333333333333",
        52,
        "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
        "0x00000000000000000000000000000000000000EE",
        CanonicalityState::Finalized,
    )
    .await?;

    let summary = sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.scanned_log_count, 1);
    assert_eq!(summary.matched_log_count, 1);
    assert_eq!(summary.active_edge_count, 1);
    assert_eq!(summary.admitted_edge_count, 1);

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_subregistry_discovery_skips_observed_and_orphaned_logs() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(true))?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    insert_raw_new_owner_log(
        database.pool(),
        "ethereum-mainnet",
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        43,
        "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
        "0x00000000000000000000000000000000000000CC",
        CanonicalityState::Observed,
    )
    .await?;
    insert_raw_new_owner_log(
        database.pool(),
        "ethereum-mainnet",
        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        44,
        "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
        "0x00000000000000000000000000000000000000DD",
        CanonicalityState::Orphaned,
    )
    .await?;

    let summary = sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.scanned_log_count, 0);
    assert_eq!(summary.matched_log_count, 0);
    assert_eq!(summary.active_observation_count, 0);
    assert_eq!(summary.active_edge_count, 0);
    assert_eq!(summary.admitted_edge_count, 0);

    let discovery_source = ens_v1_subregistry_discovery_source("ethereum-mainnet");
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
        )
        .bind(&discovery_source)
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_subregistry_discovery_clears_zero_owner_edges_deterministically() -> Result<()>
{
    let _permit = crate::acquire_test_db_permit().await;
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(true))?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    insert_raw_new_owner_log(
        database.pool(),
        "ethereum-mainnet",
        "0x4444444444444444444444444444444444444444444444444444444444444444",
        53,
        "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
        "0x00000000000000000000000000000000000000CC",
        CanonicalityState::Canonical,
    )
    .await?;
    sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;

    insert_raw_new_owner_log(
        database.pool(),
        "ethereum-mainnet",
        "0x5555555555555555555555555555555555555555555555555555555555555555",
        54,
        "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
        ZERO_ADDRESS,
        CanonicalityState::Canonical,
    )
    .await?;

    let summary = sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.active_observation_count, 0);
    assert_eq!(summary.active_edge_count, 0);
    assert_eq!(summary.inserted_edge_count, 0);
    assert_eq!(summary.deactivated_edge_count, 1);
    let normalized_events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
    assert_eq!(normalized_events.len(), 2);
    assert_eq!(
        normalized_events[1].event_kind,
        EVENT_KIND_SUBREGISTRY_CHANGED
    );
    assert_eq!(
        normalized_events[1].after_state["owner"].as_str(),
        Some(ZERO_ADDRESS)
    );
    assert_eq!(
        normalized_events[1].after_state["tombstone"].as_bool(),
        Some(true)
    );

    let discovery_source = ens_v1_subregistry_discovery_source("ethereum-mainnet");
    let cleared_edge = sqlx::query(
        r#"
        SELECT active_to_block_number, active_to_block_hash
        FROM discovery_edges
        WHERE discovery_source = $1
        ORDER BY discovery_edge_id DESC
        LIMIT 1
        "#,
    )
    .bind(&discovery_source)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        cleared_edge.try_get::<Option<i64>, _>("active_to_block_number")?,
        Some(54)
    );
    assert_eq!(
        cleared_edge.try_get::<Option<String>, _>("active_to_block_hash")?,
        Some("0x5555555555555555555555555555555555555555555555555555555555555555".to_owned())
    );

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    assert_eq!(
        watched_plan,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec!["0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned()],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 0,
        }]
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_subregistry_discovery_cascades_descendant_teardown_in_same_sync() -> Result<()>
{
    let _permit = crate::acquire_test_db_permit().await;
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(true))?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    insert_raw_new_owner_log(
        database.pool(),
        "ethereum-mainnet",
        "0x6666666666666666666666666666666666666666666666666666666666666666",
        55,
        "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
        "0x00000000000000000000000000000000000000CC",
        CanonicalityState::Canonical,
    )
    .await?;
    sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;

    let first_child_node = child_node(ZERO_NODE, &labelhash_hex("eth"))?;
    insert_raw_new_owner_log_with_key(
        database.pool(),
        RawNewOwnerLog {
            chain_id: "ethereum-mainnet",
            block_hash: "0x7777777777777777777777777777777777777777777777777777777777777777",
            block_number: 56,
            emitting_address: "0x00000000000000000000000000000000000000CC",
            owner: "0x00000000000000000000000000000000000000DD",
            parent_node: &first_child_node,
            label: "sub",
            canonicality_state: CanonicalityState::Canonical,
        },
    )
    .await?;
    sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;

    insert_raw_new_owner_log(
        database.pool(),
        "ethereum-mainnet",
        "0x8888888888888888888888888888888888888888888888888888888888888888",
        57,
        "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
        ZERO_ADDRESS,
        CanonicalityState::Canonical,
    )
    .await?;

    let summary = sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.active_observation_count, 1);
    assert_eq!(summary.active_edge_count, 0);
    assert_eq!(summary.inserted_edge_count, 0);
    assert_eq!(summary.deactivated_edge_count, 2);
    assert_eq!(
        load_normalized_events_by_namespace(database.pool(), "ens")
            .await?
            .len(),
        3
    );

    let discovery_source = ens_v1_subregistry_discovery_source("ethereum-mainnet");
    let ended_edges = sqlx::query(
        r#"
        SELECT active_to_block_number, active_to_block_hash
        FROM discovery_edges
        WHERE discovery_source = $1
          AND deactivated_at IS NOT NULL
        ORDER BY discovery_edge_id
        "#,
    )
    .bind(&discovery_source)
    .fetch_all(database.pool())
    .await?;
    assert_eq!(ended_edges.len(), 2);
    for edge in ended_edges {
        assert_eq!(
            edge.try_get::<Option<i64>, _>("active_to_block_number")?,
            Some(57)
        );
        assert_eq!(
            edge.try_get::<Option<String>, _>("active_to_block_hash")?,
            Some("0x8888888888888888888888888888888888888888888888888888888888888888".to_owned())
        );
    }

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    assert_eq!(
        watched_plan,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec!["0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned()],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 0,
        }]
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_subregistry_discovery_reconciles_reassigned_children_to_one_active_edge()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(true))?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    insert_raw_new_owner_log(
        database.pool(),
        "ethereum-mainnet",
        "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        46,
        "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
        "0x00000000000000000000000000000000000000CC",
        CanonicalityState::Canonical,
    )
    .await?;

    let first_summary =
        sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(first_summary.active_edge_count, 1);
    assert_eq!(first_summary.inserted_edge_count, 1);
    assert_eq!(first_summary.deactivated_edge_count, 0);

    insert_raw_new_owner_log(
        database.pool(),
        "ethereum-mainnet",
        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        47,
        "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
        "0x00000000000000000000000000000000000000DD",
        CanonicalityState::Canonical,
    )
    .await?;

    let second_summary =
        sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(second_summary.scanned_log_count, 2);
    assert_eq!(second_summary.matched_log_count, 2);
    assert_eq!(second_summary.active_observation_count, 1);
    assert_eq!(second_summary.active_edge_count, 1);
    assert_eq!(second_summary.admitted_edge_count, 1);
    assert_eq!(second_summary.inserted_edge_count, 1);
    assert_eq!(second_summary.deactivated_edge_count, 1);

    let discovery_source = ens_v1_subregistry_discovery_source("ethereum-mainnet");
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
        )
        .bind(&discovery_source)
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1"
        )
        .bind(&discovery_source)
        .fetch_one(database.pool())
        .await?,
        2
    );
    let deactivated_edge = sqlx::query(
        r#"
        SELECT active_to_block_number, active_to_block_hash
        FROM discovery_edges
        WHERE discovery_source = $1
          AND deactivated_at IS NOT NULL
        ORDER BY discovery_edge_id DESC
        LIMIT 1
        "#,
    )
    .bind(&discovery_source)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        deactivated_edge.try_get::<Option<i64>, _>("active_to_block_number")?,
        Some(47)
    );
    assert_eq!(
        deactivated_edge.try_get::<Option<String>, _>("active_to_block_hash")?,
        Some("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_owned())
    );

    let active_to_contract_instance_id = query_scalar::<_, Uuid>(
        "SELECT to_contract_instance_id FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
    )
    .bind(&discovery_source)
    .fetch_one(database.pool())
    .await?;
    let reassigned_contract_instance_id = load_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000dd",
    )
    .await?;
    assert_eq!(
        active_to_contract_instance_id,
        reassigned_contract_instance_id
    );

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    assert_eq!(
        watched_plan,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x00000000000000000000000000000000000000dd".to_owned(),
                "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 1,
        }]
    );

    database.cleanup().await
}

#[tokio::test]
async fn source_scoped_subregistry_discovery_reconciles_touched_assignments_only() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    let registry_address = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E";

    test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(true))?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    insert_raw_new_owner_log(
        database.pool(),
        "ethereum-mainnet",
        "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        46,
        registry_address,
        "0x00000000000000000000000000000000000000CC",
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_new_owner_log_with_key(
        database.pool(),
        RawNewOwnerLog {
            chain_id: "ethereum-mainnet",
            block_hash: "0xabababababababababababababababababababababababababababababababab",
            block_number: 47,
            emitting_address: registry_address,
            owner: "0x00000000000000000000000000000000000000EE",
            parent_node: ZERO_NODE,
            label: "alice",
            canonicality_state: CanonicalityState::Canonical,
        },
    )
    .await?;
    sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;

    insert_raw_new_owner_log(
        database.pool(),
        "ethereum-mainnet",
        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        48,
        registry_address,
        "0x00000000000000000000000000000000000000DD",
        CanonicalityState::Canonical,
    )
    .await?;

    let source_scope = vec![(
        ENS_V1_REGISTRY_SOURCE_FAMILY.to_owned(),
        registry_address.to_owned(),
        0,
        i64::MAX,
    )];
    let summary = EnsV1SubregistryDiscoverySyncSummary::sync_for_block_hashes_with_source_scope(
        database.pool(),
        "ethereum-mainnet",
        &["0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_owned()],
        &source_scope,
    )
    .await?;
    assert_eq!(summary.scanned_log_count, 1);
    assert_eq!(summary.matched_log_count, 1);
    assert_eq!(summary.active_observation_count, 1);
    assert_eq!(summary.active_edge_count, 1);
    assert_eq!(summary.admitted_edge_count, 1);
    assert_eq!(summary.inserted_edge_count, 1);
    assert_eq!(summary.deactivated_edge_count, 1);

    let discovery_source = ens_v1_subregistry_discovery_source("ethereum-mainnet");
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
        )
        .bind(&discovery_source)
        .fetch_one(database.pool())
        .await?,
        2
    );
    let eth_node = child_node(ZERO_NODE, &labelhash_hex("eth"))?;
    let alice_node = child_node(ZERO_NODE, &labelhash_hex("alice"))?;
    let active_eth_owner = query_scalar::<_, String>(
        r#"
        SELECT cia.address
        FROM discovery_edges de
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = de.to_contract_instance_id
         AND cia.deactivated_at IS NULL
        WHERE de.discovery_source = $1
          AND de.provenance ->> 'observation_key' = $2
          AND de.deactivated_at IS NULL
        "#,
    )
    .bind(&discovery_source)
    .bind(&eth_node)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        active_eth_owner,
        "0x00000000000000000000000000000000000000dd"
    );
    let active_alice_owner = query_scalar::<_, String>(
        r#"
        SELECT cia.address
        FROM discovery_edges de
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = de.to_contract_instance_id
         AND cia.deactivated_at IS NULL
        WHERE de.discovery_source = $1
          AND de.provenance ->> 'observation_key' = $2
          AND de.deactivated_at IS NULL
        "#,
    )
    .bind(&discovery_source)
    .bind(&alice_node)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        active_alice_owner,
        "0x00000000000000000000000000000000000000ee"
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_subregistry_discovery_respects_manifest_discovery_rules() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &manifest_contents(false))?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    insert_raw_new_owner_log(
        database.pool(),
        "ethereum-mainnet",
        "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
        45,
        "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
        "0x00000000000000000000000000000000000000CC",
        CanonicalityState::Canonical,
    )
    .await?;

    let summary = sync_ens_v1_subregistry_discovery(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.scanned_log_count, 1);
    assert_eq!(summary.matched_log_count, 1);
    assert_eq!(summary.active_observation_count, 1);
    assert_eq!(summary.active_edge_count, 0);
    assert_eq!(summary.admitted_edge_count, 0);
    assert_eq!(summary.inserted_edge_count, 0);
    assert!(
        load_normalized_events_by_namespace(database.pool(), "ens")
            .await?
            .is_empty()
    );

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    assert_eq!(
        watched_plan,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec!["0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned()],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 0,
        }]
    );

    database.cleanup().await
}
