use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use bigname_storage::{
    NormalizedEvent, RawBlock, RawCodeHash, RawLog, default_database_url, load_name_surface,
    load_normalized_event_counts_by_kind, load_surface_bindings_by_logical_name_id,
    upsert_normalized_events, upsert_raw_blocks, upsert_raw_code_hashes, upsert_raw_logs,
};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::time::OffsetDateTime,
};

use super::*;

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);
const BASE_NATIVE_COIN_TYPE: &str = "2147492101";

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
            .context("failed to parse database URL for ENSv1 unwrapped authority tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!("bn_ad_ua_{}_{}_{}", std::process::id(), sequence, unique);

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for ENSv1 unwrapped authority tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect test pool for ENSv1 unwrapped authority tests")?;

        bigname_storage::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for ENSv1 unwrapped authority tests")?;

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

async fn delete_normalized_events_in_block_range_for_test(
    pool: &PgPool,
    logical_name_id: &str,
    min_block_number_inclusive: Option<i64>,
    max_block_number_exclusive: Option<i64>,
) -> Result<()> {
    // These tests intentionally remove normalized rows to prove replay preloads
    // from durable identity rows. Keep the test-only delete compatible with the
    // projection apply FK without relaxing the production no-delete boundary.
    let has_projection_change_log = sqlx::query_scalar::<_, bool>(
        "SELECT to_regclass('public.projection_normalized_event_changes') IS NOT NULL",
    )
    .fetch_one(pool)
    .await
    .context("failed to inspect projection change-log table for replay preload test")?;

    if has_projection_change_log {
        sqlx::query(
            r#"
            DELETE FROM projection_normalized_event_changes changes
            USING normalized_events events
            WHERE changes.normalized_event_id = events.normalized_event_id
              AND events.logical_name_id = $1
              AND ($2::BIGINT IS NULL OR events.block_number >= $2)
              AND ($3::BIGINT IS NULL OR events.block_number < $3)
            "#,
        )
        .bind(logical_name_id)
        .bind(min_block_number_inclusive)
        .bind(max_block_number_exclusive)
        .execute(pool)
        .await
        .context("failed to delete projection change-log rows for replay preload test")?;
    }

    sqlx::query(
        r#"
        DELETE FROM normalized_events events
        WHERE events.logical_name_id = $1
          AND ($2::BIGINT IS NULL OR events.block_number >= $2)
          AND ($3::BIGINT IS NULL OR events.block_number < $3)
        "#,
    )
    .bind(logical_name_id)
    .bind(min_block_number_inclusive)
    .bind(max_block_number_exclusive)
    .execute(pool)
    .await
    .context("failed to delete normalized events for replay preload test")?;

    Ok(())
}

struct ManifestVersionSeed<'a> {
    manifest_version: i64,
    namespace: &'a str,
    source_family: &'a str,
    chain: &'a str,
    deployment_epoch: &'a str,
    rollout_status: &'a str,
    normalizer_version: &'a str,
    file_path: &'a str,
}

fn manifest_payload_for_seed(seed: &ManifestVersionSeed<'_>) -> Value {
    json!({
        "manifest_version": seed.manifest_version,
        "namespace": seed.namespace,
        "source_family": seed.source_family,
        "chain": seed.chain,
        "deployment_epoch": seed.deployment_epoch,
        "rollout_status": seed.rollout_status,
        "normalizer_version": seed.normalizer_version,
        "capability_flags": {},
        "roots": [],
        "contracts": [],
        "discovery_rules": [],
        "abi": {
            "events": manifest_abi_events_for_source_family(seed.source_family),
        },
    })
}

fn manifest_abi_events_for_source_family(source_family: &str) -> Vec<Value> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1 => vec![
            abi_event(
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 cost, uint256 expires)",
            ),
            abi_event(
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 baseCost, uint256 premium, uint256 expires)",
            ),
            abi_event(
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 baseCost, uint256 premium, uint256 expires, bytes32 referrer)",
            ),
            abi_event(
                "NameRenewed",
                "event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires)",
            ),
            abi_event(
                "NameRenewed",
                "event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires, bytes32 referrer)",
            ),
            abi_event(
                "Transfer",
                "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
            ),
        ],
        SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR => vec![
            abi_event(
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            ),
            abi_event(
                "NameRenewed",
                "event NameRenewed(string name, bytes32 indexed label, uint256 expires)",
            ),
            abi_event(
                "Transfer",
                "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
            ),
        ],
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1 | SOURCE_FAMILY_BASENAMES_BASE_REGISTRY => vec![
            abi_event(
                "NewOwner",
                "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
            ),
            abi_event(
                "Transfer",
                "event Transfer(bytes32 indexed node, address owner)",
            ),
            abi_event(
                "NewResolver",
                "event NewResolver(bytes32 indexed node, address resolver)",
            ),
            abi_event("NewTTL", "event NewTTL(bytes32 indexed node, uint64 ttl)"),
        ],
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1 => vec![
            abi_event(
                "ABIChanged",
                "event ABIChanged(bytes32 indexed node, uint256 indexed contentType)",
            ),
            abi_event(
                "AddrChanged",
                "event AddrChanged(bytes32 indexed node, address a)",
            ),
            abi_event(
                "AddressChanged",
                "event AddressChanged(bytes32 indexed node, uint256 coinType, bytes newAddress)",
            ),
            abi_event(
                "ContentChanged",
                "event ContentChanged(bytes32 indexed node, bytes32 hash)",
            ),
            abi_event(
                "ContenthashChanged",
                "event ContenthashChanged(bytes32 indexed node, bytes hash)",
            ),
            abi_event(
                "DNSRecordChanged",
                "event DNSRecordChanged(bytes32 indexed node, bytes name, uint16 resource, bytes record)",
            ),
            abi_event(
                "DNSRecordDeleted",
                "event DNSRecordDeleted(bytes32 indexed node, bytes name, uint16 resource)",
            ),
            abi_event(
                "DNSZonehashChanged",
                "event DNSZonehashChanged(bytes32 indexed node, bytes lastzonehash, bytes zonehash)",
            ),
            abi_event(
                "DataChanged",
                "event DataChanged(bytes32 indexed node, string indexed indexedKey, string key, bytes indexed indexedData)",
            ),
            abi_event(
                "InterfaceChanged",
                "event InterfaceChanged(bytes32 indexed node, bytes4 indexed interfaceID, address implementer)",
            ),
            abi_event(
                "NameChanged",
                "event NameChanged(bytes32 indexed node, string name)",
            ),
            abi_event(
                "TextChanged",
                "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key)",
            ),
            abi_event(
                "TextChanged",
                "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
            ),
            abi_event(
                "VersionChanged",
                "event VersionChanged(bytes32 indexed node, uint64 newVersion)",
            ),
        ],
        SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => vec![
            abi_event(
                "AddrChanged",
                "event AddrChanged(bytes32 indexed node, address a)",
            ),
            abi_event(
                "AddressChanged",
                "event AddressChanged(bytes32 indexed node, uint256 coinType, bytes newAddress)",
            ),
            abi_event(
                "NameChanged",
                "event NameChanged(bytes32 indexed node, string name)",
            ),
            abi_event(
                "TextChanged",
                "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
            ),
            abi_event(
                "VersionChanged",
                "event VersionChanged(bytes32 indexed node, uint64 newVersion)",
            ),
        ],
        SOURCE_FAMILY_ENS_V1_WRAPPER_L1 => vec![
            abi_event(
                "NameWrapped",
                "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
            ),
            abi_event(
                "NameUnwrapped",
                "event NameUnwrapped(bytes32 indexed node, address owner)",
            ),
            abi_event(
                "FusesSet",
                "event FusesSet(bytes32 indexed node, uint32 fuses)",
            ),
            abi_event(
                "ExpiryExtended",
                "event ExpiryExtended(bytes32 indexed node, uint64 expiry)",
            ),
            abi_event(
                "TransferSingle",
                "event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)",
            ),
            abi_event(
                "TransferBatch",
                "event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)",
            ),
        ],
        _ => Vec::new(),
    }
}

fn abi_event(name: &str, fragment: &str) -> Value {
    json!({
        "name": name,
        "fragment": fragment,
    })
}

async fn insert_manifest_version(pool: &PgPool, seed: ManifestVersionSeed<'_>) -> Result<i64> {
    let manifest_payload = manifest_payload_for_seed(&seed);
    sqlx::query_scalar(
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
    .bind(seed.manifest_version)
    .bind(seed.namespace)
    .bind(seed.source_family)
    .bind(seed.chain)
    .bind(seed.deployment_epoch)
    .bind(seed.rollout_status)
    .bind(seed.normalizer_version)
    .bind(seed.file_path)
    .bind(manifest_payload.to_string())
    .fetch_one(pool)
    .await
    .context("failed to insert manifest version")
}

struct ManifestContractInstanceSeed<'a> {
    manifest_id: i64,
    declaration_kind: &'a str,
    declaration_name: &'a str,
    contract_instance_id: Uuid,
    declared_address: &'a str,
    role: Option<&'a str>,
    proxy_kind: Option<&'a str>,
}

async fn insert_manifest_contract_instance(
    pool: &PgPool,
    seed: ManifestContractInstanceSeed<'_>,
) -> Result<()> {
    sqlx::query(
        r#"
            INSERT INTO manifest_contract_instances (
                manifest_id,
                declaration_kind,
                declaration_name,
                contract_instance_id,
                declared_address,
                code_hash,
                abi_ref,
                role,
                proxy_kind,
                implementation_contract_instance_id,
                declared_implementation_address
            )
            VALUES ($1, $2, $3, $4, $5, NULL, NULL, $6, $7, NULL, NULL)
            "#,
    )
    .bind(seed.manifest_id)
    .bind(seed.declaration_kind)
    .bind(seed.declaration_name)
    .bind(seed.contract_instance_id)
    .bind(seed.declared_address)
    .bind(seed.role)
    .bind(seed.proxy_kind)
    .execute(pool)
    .await
    .context("failed to insert manifest contract instance")?;
    Ok(())
}

async fn insert_contract_instance(
    pool: &PgPool,
    contract_instance_id: Uuid,
    chain_id: &str,
    contract_kind: &str,
) -> Result<()> {
    sqlx::query(
        r#"
            INSERT INTO contract_instances (
                contract_instance_id,
                chain_id,
                contract_kind,
                provenance
            )
            VALUES ($1, $2, $3, $4::jsonb)
            "#,
    )
    .bind(contract_instance_id)
    .bind(chain_id)
    .bind(contract_kind)
    .bind("{}")
    .execute(pool)
    .await
    .context("failed to insert contract instance")?;
    Ok(())
}

async fn insert_contract_instance_address(
    pool: &PgPool,
    contract_instance_id: Uuid,
    chain_id: &str,
    address: &str,
    source_manifest_id: i64,
) -> Result<()> {
    sqlx::query(
        r#"
            INSERT INTO contract_instance_addresses (
                contract_instance_id,
                chain_id,
                address,
                source_manifest_id,
                provenance
            )
            VALUES ($1, $2, $3, $4, $5::jsonb)
            "#,
    )
    .bind(contract_instance_id)
    .bind(chain_id)
    .bind(address)
    .bind(source_manifest_id)
    .bind("{}")
    .execute(pool)
    .await
    .context("failed to insert contract-instance address")?;
    Ok(())
}

struct ActiveDiscoveryEdgeSeed<'a> {
    chain_id: &'a str,
    edge_kind: &'a str,
    from_contract_instance_id: Uuid,
    to_contract_instance_id: Uuid,
    source_manifest_id: i64,
    active_from_block_number: Option<i64>,
    active_to_block_number: Option<i64>,
}

async fn insert_active_discovery_edge_with_range(
    pool: &PgPool,
    seed: ActiveDiscoveryEdgeSeed<'_>,
) -> Result<()> {
    let discovery_source = format!(
        "test:{}:{}:{}:{:?}:{:?}",
        seed.edge_kind,
        seed.from_contract_instance_id,
        seed.to_contract_instance_id,
        seed.active_from_block_number,
        seed.active_to_block_number,
    );
    sqlx::query(
        r#"
            INSERT INTO discovery_edges (
                chain_id,
                edge_kind,
                from_contract_instance_id,
                to_contract_instance_id,
                discovery_source,
                source_manifest_id,
                admission,
                active_from_block_number,
                active_to_block_number,
                provenance
            )
            VALUES ($1, $2, $3, $4, $5, $6, 'test', $7, $8, $9::jsonb)
            "#,
    )
    .bind(seed.chain_id)
    .bind(seed.edge_kind)
    .bind(seed.from_contract_instance_id)
    .bind(seed.to_contract_instance_id)
    .bind(discovery_source)
    .bind(seed.source_manifest_id)
    .bind(seed.active_from_block_number)
    .bind(seed.active_to_block_number)
    .bind("{}")
    .execute(pool)
    .await
    .context("failed to insert discovery edge")?;
    Ok(())
}

async fn insert_active_contract_fixture(
    pool: &PgPool,
    source_family: &str,
    declaration_name: &str,
    address: &str,
    role: Option<&str>,
    file_path: &str,
) -> Result<i64> {
    insert_active_contract_fixture_with_manifest(
        pool,
        ActiveContractFixtureSeed {
            namespace: "ens",
            source_family,
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v1",
            declaration_name,
            address,
            role,
            file_path,
        },
    )
    .await
}

struct ActiveContractFixtureSeed<'a> {
    namespace: &'a str,
    source_family: &'a str,
    chain: &'a str,
    deployment_epoch: &'a str,
    declaration_name: &'a str,
    address: &'a str,
    role: Option<&'a str>,
    file_path: &'a str,
}

async fn insert_active_contract_fixture_with_manifest(
    pool: &PgPool,
    seed: ActiveContractFixtureSeed<'_>,
) -> Result<i64> {
    let manifest_id = insert_manifest_version(
        pool,
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: seed.namespace,
            source_family: seed.source_family,
            chain: seed.chain,
            deployment_epoch: seed.deployment_epoch,
            rollout_status: "active",
            normalizer_version: "ensip15@ens-normalize-0.1.1",
            file_path: seed.file_path,
        },
    )
    .await?;
    let contract_instance_id = Uuid::new_v4();
    insert_contract_instance(pool, contract_instance_id, seed.chain, "contract").await?;
    insert_manifest_contract_instance(
        pool,
        ManifestContractInstanceSeed {
            manifest_id,
            declaration_kind: "contract",
            declaration_name: seed.declaration_name,
            contract_instance_id,
            declared_address: seed.address,
            role: seed.role,
            proxy_kind: Some("none"),
        },
    )
    .await?;
    insert_contract_instance_address(
        pool,
        contract_instance_id,
        seed.chain,
        seed.address,
        manifest_id,
    )
    .await?;
    Ok(manifest_id)
}

async fn insert_ens_registry_current_and_old_fixture(
    pool: &PgPool,
    current_registry_address: &str,
    old_registry_address: &str,
) -> Result<i64> {
    let manifest_id = insert_manifest_version(
        pool,
        ManifestVersionSeed {
            manifest_version: 3,
            namespace: "ens",
            source_family: SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v1",
            rollout_status: "active",
            normalizer_version: "ensip15@ens-normalize-0.1.1",
            file_path: "manifests/ens/ens_v1_registry_l1/v3.toml",
        },
    )
    .await?;
    for (declaration_name, address, role) in [
        ("registry", current_registry_address, "registry"),
        ("registry_old", old_registry_address, "registry_old"),
    ] {
        let contract_instance_id = Uuid::new_v4();
        insert_contract_instance(pool, contract_instance_id, "ethereum-mainnet", "contract")
            .await?;
        insert_manifest_contract_instance(
            pool,
            ManifestContractInstanceSeed {
                manifest_id,
                declaration_kind: "contract",
                declaration_name,
                contract_instance_id,
                declared_address: address,
                role: Some(role),
                proxy_kind: Some("none"),
            },
        )
        .await?;
        insert_contract_instance_address(
            pool,
            contract_instance_id,
            "ethereum-mainnet",
            address,
            manifest_id,
        )
        .await?;
    }
    Ok(manifest_id)
}

fn raw_block(
    block_hash: &str,
    parent_hash: Option<&str>,
    block_number: i64,
    timestamp: i64,
) -> RawBlock {
    raw_block_on_chain(
        "ethereum-mainnet",
        block_hash,
        parent_hash,
        block_number,
        timestamp,
    )
}

fn raw_block_on_chain(
    chain_id: &str,
    block_hash: &str,
    parent_hash: Option<&str>,
    block_number: i64,
    timestamp: i64,
) -> RawBlock {
    RawBlock {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: parent_hash.map(str::to_owned),
        block_number,
        block_timestamp: OffsetDateTime::from_unix_timestamp(timestamp)
            .expect("test block timestamp must be valid"),
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn raw_block_snapshot(block_number: i64, timestamp: i64) -> RawBlockSnapshot {
    RawBlockSnapshot {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: format!("0x{block_number:064x}"),
        block_number,
        block_timestamp: OffsetDateTime::from_unix_timestamp(timestamp)
            .expect("test block timestamp must be valid"),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn raw_code_hash_for_address(address: &str, code_hash: &str) -> RawCodeHash {
    raw_code_hash_for_address_on_chain("ethereum-mainnet", address, code_hash)
}

fn raw_code_hash_for_address_on_chain(
    chain_id: &str,
    address: &str,
    code_hash: &str,
) -> RawCodeHash {
    RawCodeHash {
        chain_id: chain_id.to_owned(),
        block_hash: "0x9999999999999999999999999999999999999999999999999999999999999999".to_owned(),
        block_number: 41,
        contract_address: address.to_owned(),
        code_hash: code_hash.to_owned(),
        code_byte_length: 5,
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn abi_word_u64(value: u64) -> [u8; 32] {
    let mut word = [0u8; 32];
    word[24..].copy_from_slice(&value.to_be_bytes());
    word
}

fn abi_word_address(address: &str) -> [u8; 32] {
    let normalized = address.trim_start_matches("0x");
    assert_eq!(normalized.len(), 40, "address must be 20 bytes");
    let mut word = [0u8; 32];
    for (index, chunk) in normalized.as_bytes().chunks(2).enumerate() {
        let value = std::str::from_utf8(chunk).expect("hex address chunk must be utf-8");
        word[12 + index] = u8::from_str_radix(value, 16).expect("address must be hex");
    }
    word
}

fn abi_word_fixed_bytes(value: &[u8]) -> [u8; 32] {
    assert!(value.len() <= 32, "fixed bytes value must fit in one word");
    let mut word = [0u8; 32];
    word[..value.len()].copy_from_slice(value);
    word
}

fn encode_registrar_name_registered_log_data(label: &str, expiry_unix: i64) -> Vec<u8> {
    encode_controller_label_event_log_data(label, &[1, expiry_unix as u64])
}

fn encode_basenames_registrar_name_registered_log_data(label: &str, expiry_unix: i64) -> Vec<u8> {
    encode_controller_label_event_log_data(label, &[expiry_unix as u64])
}

fn encode_controller_label_event_log_data(
    label: &str,
    static_words_after_offset: &[u64],
) -> Vec<u8> {
    encode_controller_label_bytes_event_log_data(label.as_bytes(), static_words_after_offset)
}

fn encode_controller_label_bytes_event_log_data(
    label_bytes: &[u8],
    static_words_after_offset: &[u64],
) -> Vec<u8> {
    let mut output = Vec::new();

    let string_offset = 32 * (1 + static_words_after_offset.len());
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(string_offset).expect("test ABI offset must fit in u64"),
    ));
    for word in static_words_after_offset {
        output.extend_from_slice(&abi_word_u64(*word));
    }
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(label_bytes.len()).expect("test label length must fit in u64"),
    ));
    output.extend_from_slice(label_bytes);

    let padded_length = label_bytes.len().div_ceil(32) * 32;
    output.resize(string_offset + 32 + padded_length, 0);
    output
}

fn encode_registry_new_resolver_log_data(resolver: &str) -> Vec<u8> {
    abi_word_address(resolver).to_vec()
}

fn encode_dynamic_string_log_data(value: &str) -> Vec<u8> {
    encode_dynamic_bytes_log_data(value.as_bytes())
}

fn encode_dynamic_bytes_log_data(value_bytes: &[u8]) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(&abi_word_u64(32));
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(value_bytes.len()).expect("test dynamic bytes length must fit in u64"),
    ));
    output.extend_from_slice(value_bytes);
    let padded_length = value_bytes.len().div_ceil(32) * 32;
    output.resize(64 + padded_length, 0);
    output
}

fn encode_two_dynamic_string_log_data(first: &str, second: &str) -> Vec<u8> {
    let first_bytes = first.as_bytes();
    let second_bytes = second.as_bytes();
    let first_padded_length = first_bytes.len().div_ceil(32) * 32;
    let second_padded_length = second_bytes.len().div_ceil(32) * 32;
    let first_offset = 64;
    let second_offset = first_offset + 32 + first_padded_length;
    let mut output = Vec::new();
    output.extend_from_slice(&abi_word_u64(first_offset as u64));
    output.extend_from_slice(&abi_word_u64(second_offset as u64));
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(first_bytes.len()).expect("test string length must fit in u64"),
    ));
    output.extend_from_slice(first_bytes);
    output.resize(first_offset + 32 + first_padded_length, 0);
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(second_bytes.len()).expect("test string length must fit in u64"),
    ));
    output.extend_from_slice(second_bytes);
    output.resize(second_offset + 32 + second_padded_length, 0);
    output
}

fn encode_resolver_addr_changed_log_data(address: &str) -> Vec<u8> {
    abi_word_address(address).to_vec()
}

fn encode_resolver_address_changed_log_data(coin_type: u64, address_bytes: &[u8]) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(&abi_word_u64(coin_type));
    output.extend_from_slice(&abi_word_u64(64));
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(address_bytes.len()).expect("test address length must fit in u64"),
    ));
    output.extend_from_slice(address_bytes);
    let padded_length = address_bytes.len().div_ceil(32) * 32;
    output.resize(96 + padded_length, 0);
    output
}

fn encode_resolver_version_changed_log_data(version: u64) -> Vec<u8> {
    abi_word_u64(version).to_vec()
}

fn dns_encoded_name(labels: &[&str]) -> Vec<u8> {
    let mut output = Vec::new();
    for label in labels {
        output.push(u8::try_from(label.len()).expect("test label length must fit in u8"));
        output.extend_from_slice(label.as_bytes());
    }
    output.push(0);
    output
}

fn encode_name_wrapped_log_data(
    dns_name: &[u8],
    owner: &str,
    fuses: u64,
    expiry_unix: u64,
) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(&abi_word_u64(128));
    output.extend_from_slice(&abi_word_address(owner));
    output.extend_from_slice(&abi_word_u64(fuses));
    output.extend_from_slice(&abi_word_u64(expiry_unix));
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(dns_name.len()).expect("test DNS name length must fit in u64"),
    ));
    output.extend_from_slice(dns_name);
    let padded_length = dns_name.len().div_ceil(32) * 32;
    output.resize(160 + padded_length, 0);
    output
}

fn encode_name_unwrapped_log_data(owner: &str) -> Vec<u8> {
    abi_word_address(owner).to_vec()
}

fn encode_fuses_set_log_data(fuses: u64) -> Vec<u8> {
    abi_word_u64(fuses).to_vec()
}

fn encode_expiry_extended_log_data(expiry_unix: u64) -> Vec<u8> {
    abi_word_u64(expiry_unix).to_vec()
}

fn hex_32_word(value: &str) -> [u8; 32] {
    let normalized = value.trim_start_matches("0x");
    assert_eq!(normalized.len(), 64, "word must be 32 bytes");
    let mut word = [0u8; 32];
    for (index, chunk) in normalized.as_bytes().chunks(2).enumerate() {
        let value = std::str::from_utf8(chunk).expect("hex word chunk must be utf-8");
        word[index] = u8::from_str_radix(value, 16).expect("word must be hex");
    }
    word
}

fn encode_transfer_single_log_data(token_id: &str, value: u64) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(&hex_32_word(token_id));
    output.extend_from_slice(&abi_word_u64(value));
    output
}

fn transfer_batch_topic0_for_test() -> String {
    keccak256_hex(b"TransferBatch(address,address,address,uint256[],uint256[])")
}

fn encode_transfer_batch_log_data(token_ids: &[String], values: &[u64]) -> Vec<u8> {
    assert_eq!(
        token_ids.len(),
        values.len(),
        "batch ids and values must have the same length"
    );
    let ids_offset = 64_u64;
    let values_offset = ids_offset + 32 + u64::try_from(token_ids.len()).unwrap() * 32;
    let mut output = Vec::new();
    output.extend_from_slice(&abi_word_u64(ids_offset));
    output.extend_from_slice(&abi_word_u64(values_offset));
    output.extend_from_slice(&abi_word_u64(u64::try_from(token_ids.len()).unwrap()));
    for token_id in token_ids {
        output.extend_from_slice(&hex_32_word(token_id));
    }
    output.extend_from_slice(&abi_word_u64(u64::try_from(values.len()).unwrap()));
    for value in values {
        output.extend_from_slice(&abi_word_u64(*value));
    }
    output
}

fn reverse_claim_event(
    source_manifest_id: i64,
    block_hash: &str,
    transaction_hash: &str,
    log_index: i64,
    claimed_address: &str,
    reverse_node: &str,
    reverse_name: &str,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!(
            "{DERIVATION_KIND_ENS_V1_REVERSE_CLAIM}:{EVENT_KIND_REVERSE_CHANGED}:{block_hash}:{transaction_hash}:{log_index}:{claimed_address}"
        ),
        namespace: "ens".to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_REVERSE_CHANGED.to_owned(),
        source_family: "ens_v1_reverse_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: Some(source_manifest_id),
        chain_id: Some("ethereum-mainnet".to_owned()),
        block_number: Some(42),
        block_hash: Some(block_hash.to_owned()),
        transaction_hash: Some(transaction_hash.to_owned()),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "ethereum-mainnet",
            "block_hash": block_hash,
            "block_number": 42,
            "transaction_hash": transaction_hash,
            "transaction_index": 0,
            "log_index": log_index,
            "emitting_address": "0x00000000000000000000000000000000000000ad",
        }),
        derivation_kind: DERIVATION_KIND_ENS_V1_REVERSE_CLAIM.to_owned(),
        canonicality_state: CanonicalityState::Canonical,
        before_state: json!({}),
        after_state: json!({
            "source_event": "ReverseClaimed",
            "address": claimed_address,
            "coin_type": ENS_NATIVE_COIN_TYPE,
            "namespace": "ens",
            "reverse_namespace": "ens",
            "reverse_label": claimed_address.trim_start_matches("0x").to_ascii_lowercase(),
            "reverse_name": reverse_name,
            "reverse_node": reverse_node,
            "claim_provenance": {
                "source_family": "ens_v1_reverse_l1",
                "contract_role": CONTRACT_ROLE_REVERSE_REGISTRAR,
                "contract_instance_id": Uuid::from_u128(0x44).to_string(),
                "emitting_address": "0x00000000000000000000000000000000000000ad",
            },
        }),
    }
}

fn basenames_reverse_claim_event(
    source_manifest_id: i64,
    block_hash: &str,
    transaction_hash: &str,
    log_index: i64,
    claimed_address: &str,
    reverse_node: &str,
    reverse_name: &str,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!(
            "{DERIVATION_KIND_ENS_V1_REVERSE_CLAIM}:{EVENT_KIND_REVERSE_CHANGED}:{block_hash}:{transaction_hash}:{log_index}:{claimed_address}:basenames"
        ),
        namespace: "basenames".to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_REVERSE_CHANGED.to_owned(),
        source_family: "basenames_base_primary".to_owned(),
        manifest_version: 1,
        source_manifest_id: Some(source_manifest_id),
        chain_id: Some("base-mainnet".to_owned()),
        block_number: Some(42),
        block_hash: Some(block_hash.to_owned()),
        transaction_hash: Some(transaction_hash.to_owned()),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "base-mainnet",
            "block_hash": block_hash,
            "block_number": 42,
            "transaction_hash": transaction_hash,
            "transaction_index": 0,
            "log_index": log_index,
            "emitting_address": "0x00000000000000000000000000000000000000ad",
        }),
        derivation_kind: DERIVATION_KIND_ENS_V1_REVERSE_CLAIM.to_owned(),
        canonicality_state: CanonicalityState::Canonical,
        before_state: json!({}),
        after_state: json!({
            "source_event": "NameForAddrChanged",
            "address": claimed_address,
            "coin_type": BASE_NATIVE_COIN_TYPE,
            "namespace": "basenames",
            "reverse_namespace": "basenames",
            "reverse_label": claimed_address.trim_start_matches("0x").to_ascii_lowercase(),
            "reverse_name": reverse_name,
            "reverse_node": reverse_node,
            "claim_provenance": {
                "source_family": "basenames_base_primary",
                "contract_role": CONTRACT_ROLE_REVERSE_REGISTRAR,
                "contract_instance_id": Uuid::from_u128(0x45).to_string(),
                "emitting_address": "0x00000000000000000000000000000000000000ad",
            },
        }),
    }
}

fn reverse_label_for_address(address: &str) -> String {
    address.trim_start_matches("0x").to_ascii_lowercase()
}

fn reverse_node_for_address(address: &str) -> String {
    let reverse_label = reverse_label_for_address(address);
    namehash_hex(&[
        reverse_label.into_bytes(),
        b"addr".to_vec(),
        b"reverse".to_vec(),
    ])
}

fn base_reverse_name_for_address(address: &str) -> String {
    format!("{}.80002105.reverse", reverse_label_for_address(address))
}

fn base_reverse_node_for_address(address: &str) -> String {
    const BASE_REVERSE_NODE: &str =
        "0x08d9b0993eb8c4da57c37a4b84a6e384c2623114ff4e9370ed51c9b8935109ba";

    let label_hash = keccak256_hex(reverse_label_for_address(address).as_bytes());
    child_namehash_hex(BASE_REVERSE_NODE, &label_hash)
        .expect("Basenames reverse node test derivation must be valid")
}

fn resolver_raw_log(
    emitting_address: &str,
    topics: Vec<String>,
    data: Vec<u8>,
    log_index: i64,
) -> AuthorityRawLogRow {
    AuthorityRawLogRow {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        block_number: 42,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_042)
            .expect("test timestamp must be valid"),
        transaction_hash: "0xtxaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .to_owned(),
        transaction_index: 0,
        log_index,
        emitting_address: emitting_address.to_owned(),
        topics,
        data,
        canonicality_state: CanonicalityState::Canonical,
        source_manifest_id: 3,
        namespace: "ens".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned(),
        manifest_version: 1,
        normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
        contract_role: Some("public_resolver".to_owned()),
    }
}

fn wrapper_raw_log(topics: Vec<String>, data: Vec<u8>, log_index: i64) -> AuthorityRawLogRow {
    AuthorityRawLogRow {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        block_number: 42,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_042)
            .expect("test timestamp must be valid"),
        transaction_hash: "0xtxaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .to_owned(),
        transaction_index: 0,
        log_index,
        emitting_address: "0x00000000000000000000000000000000000000dd".to_owned(),
        topics,
        data,
        canonicality_state: CanonicalityState::Canonical,
        source_manifest_id: 4,
        namespace: "ens".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_WRAPPER_L1.to_owned(),
        manifest_version: 1,
        normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
        contract_role: Some("name_wrapper".to_owned()),
    }
}

fn registrar_raw_log(topics: Vec<String>, data: Vec<u8>, log_index: i64) -> AuthorityRawLogRow {
    AuthorityRawLogRow {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        block_number: 42,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_042)
            .expect("test timestamp must be valid"),
        transaction_hash: "0xtxaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .to_owned(),
        transaction_index: 0,
        log_index,
        emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
        topics,
        data,
        canonicality_state: CanonicalityState::Canonical,
        source_manifest_id: 1,
        namespace: "ens".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRAR_L1.to_owned(),
        manifest_version: 1,
        normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
        contract_role: Some("registrar_controller".to_owned()),
    }
}

#[test]
fn build_authority_observation_decodes_registrar_controller_event_generations() -> Result<()> {
    let event_topics = AuthorityEventTopics::for_tests();
    let owner_topic = hex_string(&abi_word_address(
        "0x0000000000000000000000000000000000000001",
    ));
    let labelhash = keccak256_hex(b"alice");

    let wrapped = build_authority_observation(
        &registrar_raw_log(
            vec![
                wrapped_name_registered_topic0(),
                labelhash.clone(),
                owner_topic.clone(),
            ],
            encode_controller_label_event_log_data("alice", &[1, 2, 1_800_000_000]),
            0,
        ),
        &event_topics,
    )?
    .context("wrapped controller NameRegistered observation should decode")?;
    assert!(matches!(
        wrapped,
        AuthorityObservation::RegistrationGranted(NameRegistrationObservation {
            expiry,
            ..
        }) if expiry.unix_timestamp() == 1_800_000_000
    ));

    let unwrapped = build_authority_observation(
        &registrar_raw_log(
            vec![
                unwrapped_name_registered_topic0(),
                labelhash.clone(),
                owner_topic,
            ],
            encode_controller_label_event_log_data("alice", &[1, 2, 1_900_000_000, 3]),
            1,
        ),
        &event_topics,
    )?
    .context("unwrapped controller NameRegistered observation should decode")?;
    assert!(matches!(
        unwrapped,
        AuthorityObservation::RegistrationGranted(NameRegistrationObservation {
            expiry,
            ..
        }) if expiry.unix_timestamp() == 1_900_000_000
    ));

    let renewed = build_authority_observation(
        &registrar_raw_log(
            vec![unwrapped_name_renewed_topic0(), labelhash],
            encode_controller_label_event_log_data("alice", &[1, 2_000_000_000, 3]),
            2,
        ),
        &event_topics,
    )?
    .context("unwrapped controller NameRenewed observation should decode")?;
    assert!(matches!(
        renewed,
        AuthorityObservation::RegistrationRenewed(NameRenewalObservation {
            expiry,
            ..
        }) if expiry.unix_timestamp() == 2_000_000_000
    ));

    Ok(())
}

#[test]
fn build_authority_observation_keeps_registry_new_owner_for_known_subname_parent() -> Result<()> {
    let event_topics = AuthorityEventTopics::for_tests();
    let parent = observe_registrar_eth_name_with_version("taytems", ENS_NORMALIZER_VERSION)?;
    let child_labelhash = keccak256_hex(b"cold");
    let child_namehash = child_namehash_hex(&parent.namehash, &child_labelhash)?;
    let mut raw_log = registrar_raw_log(
        vec![
            new_owner_topic0(),
            parent.namehash.clone(),
            child_labelhash.clone(),
        ],
        abi_word_address("0x0000000000000000000000000000000000000002").to_vec(),
        0,
    );
    raw_log.source_family = SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned();
    raw_log.source_manifest_id = 2;
    raw_log.contract_role = Some("registry".to_owned());

    let observation = build_authority_observation(&raw_log, &event_topics)?
        .context("non-root registry NewOwner should remain observable for child surfaces")?;

    assert!(matches!(
        observation,
        AuthorityObservation::RegistryOwnerChanged(RegistryOwnerObservation {
            parent_node,
            labelhash,
            namehash,
            ..
        }) if parent_node == Some(parent.namehash)
            && labelhash == child_labelhash
            && namehash == Some(child_namehash)
    ));

    Ok(())
}

#[test]
fn build_authority_observation_decodes_registry_transfer_owner_change() -> Result<()> {
    let event_topics = AuthorityEventTopics::for_tests();
    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let owner = "0x0000000000000000000000000000000000000002";
    let mut raw_log = registrar_raw_log(
        vec![registry_transfer_topic0(), alice.namehash.clone()],
        abi_word_address(owner).to_vec(),
        0,
    );
    raw_log.source_family = SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned();
    raw_log.source_manifest_id = 2;
    raw_log.contract_role = Some("registry".to_owned());

    let observation = build_authority_observation(&raw_log, &event_topics)?
        .context("registry Transfer should decode as an owner change")?;

    assert_eq!(
        observation,
        AuthorityObservation::RegistryOwnerChanged(RegistryOwnerObservation {
            parent_node: None,
            labelhash: String::new(),
            namehash: Some(alice.namehash),
            owner: owner.to_owned(),
            reference: raw_log.reference(),
        })
    );

    Ok(())
}

#[test]
fn registry_migration_guard_suppresses_old_registry_transfer_for_migrated_node() -> Result<()> {
    let event_topics = AuthorityEventTopics::for_tests();
    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let mut raw_log = registrar_raw_log(
        vec![registry_transfer_topic0(), alice.namehash.clone()],
        abi_word_address("0x0000000000000000000000000000000000000002").to_vec(),
        0,
    );
    raw_log.source_family = SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned();
    raw_log.source_manifest_id = 2;
    raw_log.contract_role = Some(CONTRACT_ROLE_REGISTRY_OLD.to_owned());

    let action = registry_migration_guard_action(&raw_log, &event_topics)?;
    let mut migrated_nodes = MigratedRegistryNodes::empty();
    migrated_nodes.insert(alice.namehash);

    assert!(action.suppressed_by(&migrated_nodes));

    Ok(())
}

#[test]
fn build_authority_observation_skips_oversized_registrar_labels() -> Result<()> {
    let event_topics = AuthorityEventTopics::for_tests();
    let owner_topic = hex_string(&abi_word_address(
        "0x0000000000000000000000000000000000000001",
    ));
    let oversized_label = "a".repeat(usize::from(u8::MAX) + 1);
    let labelhash = keccak256_hex(oversized_label.as_bytes());

    let registered = build_authority_observation(
        &registrar_raw_log(
            vec![
                wrapped_name_registered_topic0(),
                labelhash.clone(),
                owner_topic,
            ],
            encode_controller_label_event_log_data(&oversized_label, &[1, 2, 1_800_000_000]),
            0,
        ),
        &event_topics,
    )?;
    assert_eq!(registered, None);

    let renewed = build_authority_observation(
        &registrar_raw_log(
            vec![unwrapped_name_renewed_topic0(), labelhash],
            encode_controller_label_event_log_data(&oversized_label, &[1, 2_000_000_000, 3]),
            1,
        ),
        &event_topics,
    )?;
    assert_eq!(renewed, None);

    Ok(())
}

#[test]
fn build_authority_observation_skips_non_utf8_registrar_labels() -> Result<()> {
    let event_topics = AuthorityEventTopics::for_tests();
    let owner_topic = hex_string(&abi_word_address(
        "0x0000000000000000000000000000000000000001",
    ));
    let invalid_label = [0xff, b'a', b'l', b'i', b'c', b'e'];
    let labelhash = keccak256_hex(&invalid_label);

    let registered = build_authority_observation(
        &registrar_raw_log(
            vec![
                wrapped_name_registered_topic0(),
                labelhash.clone(),
                owner_topic,
            ],
            encode_controller_label_bytes_event_log_data(&invalid_label, &[1, 2, 1_800_000_000]),
            0,
        ),
        &event_topics,
    )?;
    assert_eq!(registered, None);

    let renewed = build_authority_observation(
        &registrar_raw_log(
            vec![unwrapped_name_renewed_topic0(), labelhash],
            encode_controller_label_bytes_event_log_data(&invalid_label, &[1, 2_000_000_000, 3]),
            1,
        ),
        &event_topics,
    )?;
    assert_eq!(renewed, None);

    Ok(())
}

#[test]
fn build_authority_observation_skips_unnormalizable_registrar_labels() -> Result<()> {
    let event_topics = AuthorityEventTopics::for_tests();
    let owner_topic = hex_string(&abi_word_address(
        "0x0000000000000000000000000000000000000001",
    ));
    let invalid_label = "Ni\u{200d}ck";
    let labelhash = keccak256_hex(invalid_label.as_bytes());

    let registered = build_authority_observation(
        &registrar_raw_log(
            vec![
                wrapped_name_registered_topic0(),
                labelhash.clone(),
                owner_topic,
            ],
            encode_controller_label_event_log_data(invalid_label, &[1, 2, 1_800_000_000]),
            0,
        ),
        &event_topics,
    )?;
    assert_eq!(registered, None);

    let renewed = build_authority_observation(
        &registrar_raw_log(
            vec![unwrapped_name_renewed_topic0(), labelhash],
            encode_controller_label_event_log_data(invalid_label, &[1, 2_000_000_000, 3]),
            1,
        ),
        &event_topics,
    )?;
    assert_eq!(renewed, None);

    Ok(())
}

#[test]
fn build_authority_observation_skips_malformed_registrar_label_payloads() -> Result<()> {
    let event_topics = AuthorityEventTopics::for_tests();
    let owner_topic = hex_string(&abi_word_address(
        "0x0000000000000000000000000000000000000001",
    ));
    let labelhash = keccak256_hex(b"alice");
    let malformed_dynamic_label = abi_word_u64(96).to_vec();

    let registered = build_authority_observation(
        &registrar_raw_log(
            vec![
                wrapped_name_registered_topic0(),
                labelhash.clone(),
                owner_topic,
            ],
            malformed_dynamic_label.clone(),
            0,
        ),
        &event_topics,
    )?;
    assert_eq!(registered, None);

    let renewed = build_authority_observation(
        &registrar_raw_log(
            vec![unwrapped_name_renewed_topic0(), labelhash],
            malformed_dynamic_label,
            1,
        ),
        &event_topics,
    )?;
    assert_eq!(renewed, None);

    Ok(())
}

#[test]
fn build_authority_observation_skips_registrar_labelhash_mismatches() -> Result<()> {
    let event_topics = AuthorityEventTopics::for_tests();
    let owner_topic = hex_string(&abi_word_address(
        "0x0000000000000000000000000000000000000001",
    ));
    let mismatched_labelhash = keccak256_hex(b"bob");

    let registered = build_authority_observation(
        &registrar_raw_log(
            vec![
                wrapped_name_registered_topic0(),
                mismatched_labelhash.clone(),
                owner_topic,
            ],
            encode_controller_label_event_log_data("alice", &[1, 2, 1_800_000_000]),
            0,
        ),
        &event_topics,
    )?;
    assert_eq!(registered, None);

    let renewed = build_authority_observation(
        &registrar_raw_log(
            vec![unwrapped_name_renewed_topic0(), mismatched_labelhash],
            encode_controller_label_event_log_data("alice", &[1, 2_000_000_000, 3]),
            1,
        ),
        &event_topics,
    )?;
    assert_eq!(renewed, None);

    Ok(())
}

#[test]
fn canonical_block_index_finds_first_block_after_timestamp() -> Result<()> {
    let index = CanonicalBlockIndex {
        blocks: vec![
            raw_block_snapshot(100, 1_700_000_000),
            raw_block_snapshot(101, 1_700_000_012),
            raw_block_snapshot(102, 1_700_000_024),
        ],
    };

    let before_first = index
        .first_block_after(OffsetDateTime::from_unix_timestamp(1_699_999_999)?, "ens")
        .context("timestamp before first block should resolve to first block")?;
    assert_eq!(before_first.block_number, 100);
    assert_eq!(before_first.namespace, "ens");

    let exact = index
        .first_block_after(OffsetDateTime::from_unix_timestamp(1_700_000_012)?, "ens")
        .context("exact timestamp should resolve to the following block")?;
    assert_eq!(exact.block_number, 102);

    let between = index
        .first_block_after(OffsetDateTime::from_unix_timestamp(1_700_000_013)?, "ens")
        .context("between timestamps should resolve to the next block")?;
    assert_eq!(between.block_number, 102);

    assert!(
        index
            .first_block_after(OffsetDateTime::from_unix_timestamp(1_700_000_024)?, "ens",)
            .is_none()
    );

    Ok(())
}

#[test]
fn preload_registrar_history_recovers_binding_authority_provenance() -> Result<()> {
    let labelhash = keccak256_hex(b"alice");
    let expiry = OffsetDateTime::from_unix_timestamp(1_800_000_000)?;
    let release = release_after_grace(expiry)?;
    let block_hash = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let boundary_ref = BoundaryRef {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number: 100,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_000)?,
        canonicality_state: CanonicalityState::Canonical,
        namespace: "ens".to_owned(),
    };
    let release_block = RawBlockSnapshot {
        chain_id: boundary_ref.chain_id.clone(),
        block_hash: "0x2222222222222222222222222222222222222222222222222222222222222222".to_owned(),
        block_number: 200,
        block_timestamp: OffsetDateTime::from_unix_timestamp(release.unix_timestamp() + 1)?,
        canonicality_state: CanonicalityState::Canonical,
    };
    let mut history = empty_preloaded_history(labelhash.clone(), None);

    preload_registrar_history(
        &mut history,
        &json!({
            "authority_kind": "registrar",
            "authority_key": format!("registrar:ethereum-mainnet:10:{labelhash}:{block_hash}:7"),
            "source_event": "surface_binding_authority",
        }),
        &boundary_ref,
        Uuid::nil(),
        Some(release),
        None,
        &CanonicalBlockIndex {
            blocks: vec![release_block],
        },
    )?;

    let lease = history
        .current_registration
        .context("preloaded registrar lease should be restored")?;
    assert_eq!(lease.labelhash, labelhash);
    assert_eq!(lease.expiry, expiry);
    assert_eq!(lease.registrant, ZERO_ADDRESS);
    assert_eq!(
        lease
            .release_ref
            .context("release boundary should be restored")?
            .block_number,
        200
    );
    assert!(history.open_binding.is_some());

    Ok(())
}

#[test]
fn preload_registry_history_recovers_binding_manifest_provenance() -> Result<()> {
    let name = observe_registrar_eth_name_with_version("swagalicious", ENS_NORMALIZER_VERSION)?;
    let labelhash = name.labelhashes[0].clone();
    let authority_key = format!("registry-only:ethereum-mainnet:{}", name.namehash);
    let registry_ref = BoundaryRef {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xfe229b2d07f38e80af623be1ffc42905ffdad689e829cad1938006151bac4209".to_owned(),
        block_number: 19_614_072,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_712_617_091)?,
        canonicality_state: CanonicalityState::Finalized,
        namespace: "ens".to_owned(),
    };
    let next_ref = BoundaryRef {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0x3d14edd987841b80984b4fe216090112fe670013032e3e2b282fcb932c8b72a5".to_owned(),
        block_number: 21_782_838,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_738_778_911)?,
        canonicality_state: CanonicalityState::Finalized,
        namespace: "ens".to_owned(),
    };
    let surface_binding_id = Uuid::from_u128(0x31337);
    let resource_id = deterministic_uuid(&format!("resource:{authority_key}"));
    let mut history = empty_preloaded_history(labelhash.clone(), Some(name));
    let namehash = history.namehash.clone();

    preload_registry_history(
        &mut history,
        &json!({
            "authority_kind": "registry_only",
            "authority_key": authority_key,
            "binding_source_family": SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
            "binding_manifest_version": 3,
            "binding_manifest_id": 13,
            "logical_name_id": "ens:swagalicious.eth",
            "namehash": namehash,
            "labelhash": labelhash,
            "current_registry_owner": "0x8e8db5ccef88cca9d624701db544989c996e3216",
        }),
        &registry_ref,
        surface_binding_id,
        resource_id,
        None,
    );

    let before_anchor = history
        .open_binding
        .as_ref()
        .map(|binding| binding.authority.clone());
    transition_authority(
        &mut history,
        before_anchor,
        None,
        &next_ref,
        next_ref.block_timestamp,
    )?;

    let surface_unbound = history
        .events
        .iter()
        .find(|event| event.event_kind == EVENT_KIND_SURFACE_UNBOUND)
        .context("preloaded registry-only binding should emit SurfaceUnbound")?;
    assert_eq!(surface_unbound.manifest_version, 3);
    assert_eq!(surface_unbound.source_manifest_id, Some(13));

    Ok(())
}

#[test]
fn fresh_registry_anchor_uses_basenames_registry_family_for_surface_unbound() -> Result<()> {
    let name = observe_registrar_name_with_version(
        "based1",
        AuthorityProfile::Basenames,
        ENS_NORMALIZER_VERSION,
    )?;
    let labelhash = name.labelhashes[0].clone();
    let registry_ref = ObservationRef {
        chain_id: "base-mainnet".to_owned(),
        block_hash: "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
        block_number: 46_606_106,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_760_000_000)?,
        transaction_hash: Some(
            "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_owned(),
        ),
        transaction_index: Some(0),
        log_index: Some(7),
        canonicality_state: CanonicalityState::Finalized,
        namespace: AuthorityProfile::Basenames.namespace().to_owned(),
        source_manifest_id: 202,
        source_family: SOURCE_FAMILY_BASENAMES_BASE_REGISTRY.to_owned(),
        manifest_version: 2,
    };
    let boundary_ref = BoundaryRef {
        chain_id: registry_ref.chain_id.clone(),
        block_hash: "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_owned(),
        block_number: 46_606_107,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_760_000_012)?,
        canonicality_state: CanonicalityState::Finalized,
        namespace: AuthorityProfile::Basenames.namespace().to_owned(),
    };
    let mut history = empty_preloaded_history(labelhash.clone(), Some(name));
    history.current_registry_owner = Some("0x0000000000000000000000000000000000000202".to_owned());
    history.latest_registry_owner_ref = Some(registry_ref);

    let anchor = registry_anchor_for_history(&history, "base-mainnet", &labelhash)
        .context("fresh registry owner should produce a registry-only anchor")?;
    assert_eq!(
        anchor.binding_source_family,
        SOURCE_FAMILY_BASENAMES_BASE_REGISTRY
    );
    history.open_binding = Some(OpenBinding {
        surface_binding_id: Uuid::from_u128(0x51321),
        authority: anchor.clone(),
        active_from: OffsetDateTime::from_unix_timestamp(1_760_000_000)?,
        anchor_ref: boundary_ref.clone(),
    });

    transition_authority(
        &mut history,
        Some(anchor),
        None,
        &boundary_ref,
        boundary_ref.block_timestamp,
    )?;

    let surface_unbound = history
        .events
        .iter()
        .find(|event| event.event_kind == EVENT_KIND_SURFACE_UNBOUND)
        .context("fresh Basenames registry binding should emit SurfaceUnbound")?;
    assert_eq!(
        surface_unbound.source_family,
        SOURCE_FAMILY_BASENAMES_BASE_REGISTRY
    );

    Ok(())
}

#[test]
fn preload_registry_history_uses_basenames_registry_family_for_basenames_boundaries() -> Result<()>
{
    for provenance in [
        json!({
            "authority_kind": "registry_only",
            "authority_key": "registry-only:base-mainnet:0xmissing-source-family",
            "logical_name_id": "basenames:based1.base.eth",
        }),
        json!({
            "authority_kind": "registry_only",
            "authority_key": "registry-only:base-mainnet:0xstale-source-family",
            "binding_source_family": SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
            "logical_name_id": "basenames:based1.base.eth",
        }),
    ] {
        let name = observe_registrar_name_with_version(
            "based1",
            AuthorityProfile::Basenames,
            ENS_NORMALIZER_VERSION,
        )?;
        let labelhash = name.labelhashes[0].clone();
        let registry_ref = BoundaryRef {
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            block_number: 46_606_106,
            block_timestamp: OffsetDateTime::from_unix_timestamp(1_760_000_000)?,
            canonicality_state: CanonicalityState::Finalized,
            namespace: "basenames".to_owned(),
        };
        let next_ref = BoundaryRef {
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                .to_owned(),
            block_number: 46_606_107,
            block_timestamp: OffsetDateTime::from_unix_timestamp(1_760_000_012)?,
            canonicality_state: CanonicalityState::Finalized,
            namespace: "basenames".to_owned(),
        };
        let mut history = empty_preloaded_history(labelhash, Some(name));

        preload_registry_history(
            &mut history,
            &provenance,
            &registry_ref,
            Uuid::from_u128(0x41321),
            Uuid::from_u128(0x51321),
            None,
        );

        let before_anchor = history
            .open_binding
            .as_ref()
            .map(|binding| binding.authority.clone());
        transition_authority(
            &mut history,
            before_anchor,
            None,
            &next_ref,
            next_ref.block_timestamp,
        )?;

        let surface_unbound = history
            .events
            .iter()
            .find(|event| event.event_kind == EVENT_KIND_SURFACE_UNBOUND)
            .context("preloaded Basenames registry binding should emit SurfaceUnbound")?;
        assert_eq!(
            surface_unbound.source_family,
            SOURCE_FAMILY_BASENAMES_BASE_REGISTRY
        );
    }

    Ok(())
}

#[test]
fn transition_authority_emits_surface_unbound_for_zero_length_binding() -> Result<()> {
    let name = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let labelhash = name.labelhashes[0].clone();
    let effective_time = OffsetDateTime::from_unix_timestamp(1_700_000_050)?;
    let reference = BoundaryRef {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0x7777777777777777777777777777777777777777777777777777777777777777".to_owned(),
        block_number: 50,
        block_timestamp: effective_time,
        canonicality_state: CanonicalityState::Canonical,
        namespace: "ens".to_owned(),
    };
    let before_anchor = AuthorityAnchor {
        kind: AuthorityKind::Registrar,
        authority_key: format!(
            "registrar:ethereum-mainnet:1:{labelhash}:{}:0",
            reference.block_hash
        ),
        resource_id: Uuid::from_u128(0x3301),
        token_lineage_id: Some(Uuid::from_u128(0x3302)),
        binding_source_family: SOURCE_FAMILY_ENS_V1_REGISTRAR_L1.to_owned(),
        binding_manifest_version: 1,
        binding_manifest_id: 1,
    };
    let after_anchor = AuthorityAnchor {
        kind: AuthorityKind::RegistryOnly,
        authority_key: format!("registry-only:ethereum-mainnet:{}", name.namehash),
        resource_id: Uuid::from_u128(0x3303),
        token_lineage_id: None,
        binding_source_family: SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned(),
        binding_manifest_version: 3,
        binding_manifest_id: 2,
    };
    let mut history = empty_preloaded_history(labelhash, Some(name));
    history.open_binding = Some(OpenBinding {
        surface_binding_id: Uuid::from_u128(0x3304),
        authority: before_anchor.clone(),
        active_from: effective_time,
        anchor_ref: reference.clone(),
    });

    transition_authority(
        &mut history,
        Some(before_anchor.clone()),
        Some(after_anchor),
        &reference,
        effective_time,
    )?;

    assert!(history.bindings.is_empty());
    let surface_unbound = history
        .events
        .iter()
        .find(|event| event.event_kind == EVENT_KIND_SURFACE_UNBOUND)
        .context("zero-length binding should still emit SurfaceUnbound")?;
    assert_eq!(surface_unbound.resource_id, Some(before_anchor.resource_id));
    assert_eq!(
        surface_unbound.after_state.get("active_to"),
        Some(&json!(effective_time.unix_timestamp()))
    );

    Ok(())
}

#[test]
fn preload_registrar_history_prefers_selected_replay_authority() -> Result<()> {
    let labelhash = keccak256_hex(b"beyonce");
    let active_block_hash = "0xc21ecb3c75618295892c04d9e3ee4303818f8948b981b01fed25c20c222362e8";
    let selected_block_hash = "0x89f845f7fb05afb691159bb6269de1aaf50bc9e6f035a9ff2b321f0db1813e59";
    let active_authority_key =
        format!("registrar:ethereum-mainnet:10:{labelhash}:{active_block_hash}:149");
    let selected_authority_key =
        format!("registrar:ethereum-mainnet:10:{labelhash}:{selected_block_hash}:126");
    let boundary_ref = BoundaryRef {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: active_block_hash.to_owned(),
        block_number: 25_004_504,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_777_691_531)?,
        canonicality_state: CanonicalityState::Finalized,
        namespace: "ens".to_owned(),
    };
    let selected_start_ref = ObservationRef {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: selected_block_hash.to_owned(),
        block_number: 9_818_673,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_586_178_402)?,
        transaction_hash: None,
        transaction_index: None,
        log_index: Some(126),
        canonicality_state: CanonicalityState::Finalized,
        namespace: "ens".to_owned(),
        source_manifest_id: 10,
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRAR_L1.to_owned(),
        manifest_version: 1,
    };
    let mut history = empty_preloaded_history(labelhash.clone(), None);

    preload_registrar_history(
        &mut history,
        &json!({
            "authority_kind": "registrar",
            "authority_key": active_authority_key,
            "labelhash": labelhash,
        }),
        &boundary_ref,
        Uuid::nil(),
        None,
        Some(&PreloadedRegistrarState {
            expiry: Some(OffsetDateTime::from_unix_timestamp(1_777_850_208)?),
            registrant: None,
            authority_key: Some(selected_authority_key.clone()),
            labelhash: None,
            start_ref: Some(selected_start_ref),
        }),
        &CanonicalBlockIndex { blocks: Vec::new() },
    )?;

    let lease = history
        .current_registration
        .context("selected registrar authority should be preloaded")?;
    assert_eq!(lease.authority_key, selected_authority_key);
    assert_eq!(lease.start_ref.block_hash, selected_block_hash);
    assert_eq!(
        history
            .open_binding
            .map(|binding| binding.authority.resource_id),
        Some(deterministic_uuid(&format!(
            "resource:{selected_authority_key}"
        )))
    );

    Ok(())
}

#[test]
fn selected_registrar_preload_restores_released_lease_for_renewal_replay() -> Result<()> {
    let name = observe_registrar_eth_name_with_version("beyonce", ENS_NORMALIZER_VERSION)?;
    let labelhash = name.labelhashes[0].clone();
    let original_block_hash = "0x89f845f7fb05afb691159bb6269de1aaf50bc9e6f035a9ff2b321f0db1813e59";
    let original_authority_key =
        format!("registrar:ethereum-mainnet:10:{labelhash}:{original_block_hash}:126");
    let start_ref = ObservationRef {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: original_block_hash.to_owned(),
        block_number: 9_818_673,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_586_178_402)?,
        transaction_hash: None,
        transaction_index: None,
        log_index: Some(126),
        canonicality_state: CanonicalityState::Finalized,
        namespace: "ens".to_owned(),
        source_manifest_id: 10,
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRAR_L1.to_owned(),
        manifest_version: 1,
    };
    let registry_ref = BoundaryRef {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xdbc12fa03fec3d2173f369d4541b0c264b6c30055b7d5746fa3da6232b84b9f2".to_owned(),
        block_number: 25_004_424,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_777_690_571)?,
        canonicality_state: CanonicalityState::Finalized,
        namespace: "ens".to_owned(),
    };
    let renewal_ref = ObservationRef {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xc21ecb3c75618295892c04d9e3ee4303818f8948b981b01fed25c20c222362e8".to_owned(),
        block_number: 25_004_504,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_777_691_531)?,
        transaction_hash: Some(
            "0xd6daf4412b9ea2a119917068c12b4392492112365ccf0fff6bf508880386da47".to_owned(),
        ),
        transaction_index: None,
        log_index: Some(149),
        canonicality_state: CanonicalityState::Finalized,
        namespace: "ens".to_owned(),
        source_manifest_id: 10,
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRAR_L1.to_owned(),
        manifest_version: 1,
    };
    let before_expiry = OffsetDateTime::from_unix_timestamp(1_777_850_208)?;
    let after_expiry = OffsetDateTime::from_unix_timestamp(1_809_386_208)?;
    let namehash = name.namehash.clone();
    let mut history = empty_preloaded_history(labelhash.clone(), Some(name));
    let registry_anchor = AuthorityAnchor {
        kind: AuthorityKind::RegistryOnly,
        authority_key: format!("registry-only:ethereum-mainnet:{namehash}"),
        resource_id: deterministic_uuid(&format!(
            "resource:registry-only:ethereum-mainnet:{namehash}"
        )),
        token_lineage_id: None,
        binding_source_family: SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned(),
        binding_manifest_version: 1,
        binding_manifest_id: 1,
    };
    history.open_binding = Some(OpenBinding {
        surface_binding_id: Uuid::nil(),
        authority: registry_anchor,
        active_from: registry_ref.block_timestamp,
        anchor_ref: registry_ref,
    });

    preload_selected_registrar_lease(
        &mut history,
        Some(&PreloadedRegistrarState {
            expiry: Some(before_expiry),
            registrant: Some("0x2a1ee6d0d13a7a37ba04717ff234eac30fd6b394".to_owned()),
            authority_key: Some(original_authority_key.clone()),
            labelhash: Some(labelhash.clone()),
            start_ref: Some(start_ref),
        }),
        &CanonicalBlockIndex { blocks: Vec::new() },
    )?;
    apply_registration_renewed(
        &mut history,
        NameRenewalObservation {
            label: "beyonce".to_owned(),
            labelhash: labelhash.clone(),
            expiry: after_expiry,
            reference: renewal_ref,
        },
        &CanonicalBlockIndex { blocks: Vec::new() },
    )?;

    let renewal = history
        .events
        .iter()
        .find(|event| event.event_kind == EVENT_KIND_REGISTRATION_RENEWED)
        .context("renewal event should be emitted")?;
    assert_eq!(
        renewal.resource_id,
        Some(deterministic_uuid(&format!(
            "resource:{original_authority_key}"
        )))
    );
    assert_eq!(
        renewal.before_state["expiry"],
        json!(before_expiry.unix_timestamp())
    );
    assert_eq!(
        renewal.after_state["expiry"],
        json!(after_expiry.unix_timestamp())
    );
    assert!(
        !history
            .events
            .iter()
            .any(|event| event.event_kind == EVENT_KIND_REGISTRATION_GRANTED)
    );

    Ok(())
}

#[test]
fn wrapper_wrap_after_released_registrar_uses_registry_authority_before_state() -> Result<()> {
    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let labelhash = alice.labelhashes[0].clone();
    let registration_ref = registrar_raw_log(Vec::new(), Vec::new(), 0).reference();
    let registry_ref = AuthorityRawLogRow {
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned(),
        source_manifest_id: 2,
        contract_role: Some("registry".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 1)
    }
    .reference();
    let wrapper_ref = AuthorityRawLogRow {
        block_number: 44,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_200)?,
        log_index: 543,
        source_family: SOURCE_FAMILY_ENS_V1_WRAPPER_L1.to_owned(),
        source_manifest_id: 4,
        contract_role: Some("name_wrapper".to_owned()),
        ..wrapper_raw_log(Vec::new(), Vec::new(), 543)
    }
    .reference();
    let release_ref = BoundaryRef {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
        block_number: 43,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_100)?,
        canonicality_state: CanonicalityState::Canonical,
        namespace: "ens".to_owned(),
    };
    let mut history = empty_preloaded_history(labelhash.clone(), Some(alice.clone()));
    history.current_registration = Some(RegistrationLease {
        authority_key: format!(
            "registrar:ethereum-mainnet:1:{labelhash}:{}:0",
            registration_ref.block_hash
        ),
        labelhash,
        registrant: "0x0000000000000000000000000000000000000001".to_owned(),
        expiry: OffsetDateTime::from_unix_timestamp(1_690_000_000)?,
        release_ref: Some(release_ref),
        start_ref: registration_ref,
    });
    history.current_registry_owner = Some("0x0000000000000000000000000000000000000002".to_owned());
    history.latest_registry_owner_ref = Some(registry_ref);

    apply_wrapper_name_wrapped(
        &mut history,
        WrapperNameWrappedObservation {
            name: alice,
            owner: "0x0000000000000000000000000000000000000003".to_owned(),
            fuses: 0,
            expiry: OffsetDateTime::from_unix_timestamp(1_800_000_000)?,
            reference: wrapper_ref,
        },
    )?;

    let wrapped_event = history
        .events
        .iter()
        .find(|event| event.event_kind == EVENT_KIND_TOKEN_CONTROL_TRANSFERRED)
        .context("wrapper transfer event should be emitted")?;
    assert_eq!(
        wrapped_event.before_state.get("authority_kind"),
        Some(&json!("registry_only"))
    );

    Ok(())
}

#[test]
fn finalize_history_keeps_registry_resource_anchor_from_latest_owner_ref() -> Result<()> {
    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let labelhash = alice.labelhashes[0].clone();
    let registry_ref = AuthorityRawLogRow {
        block_number: 44,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_044)?,
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned(),
        source_manifest_id: 2,
        contract_role: Some("registry".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 44)
    }
    .reference();
    let head_ref = BoundaryRef {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
        block_number: 45,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_045)?,
        canonicality_state: CanonicalityState::Canonical,
        namespace: "ens".to_owned(),
    };
    let mut history = empty_preloaded_history(labelhash, Some(alice));
    history.current_registry_owner = Some("0x0000000000000000000000000000000000000002".to_owned());
    history.latest_registry_owner_ref = Some(registry_ref.clone());

    let finalized = finalize_history(history, &head_ref)?;

    assert_eq!(
        finalized.registry_resource_anchor,
        Some(registry_ref.as_boundary_ref())
    );
    assert!(
        finalized
            .bindings
            .iter()
            .any(|segment| segment.authority.kind == AuthorityKind::RegistryOnly)
    );

    Ok(())
}

#[test]
fn registry_only_authority_key_uses_full_namehash_for_subnames() -> Result<()> {
    let reference = AuthorityRawLogRow {
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned(),
        source_manifest_id: 2,
        contract_role: Some("registry".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 44)
    }
    .reference();
    let cold_eth =
        observe_text_name_with_reference("cold.eth", &reference, ENS_NORMALIZER_VERSION)?;
    let cold_highwind =
        observe_text_name_with_reference("cold.highwind.eth", &reference, ENS_NORMALIZER_VERSION)?;
    assert_eq!(cold_eth.labelhashes[0], cold_highwind.labelhashes[0]);

    let mut cold_eth_history =
        empty_preloaded_history(cold_eth.labelhashes[0].clone(), Some(cold_eth.clone()));
    cold_eth_history.current_registry_owner =
        Some("0x0000000000000000000000000000000000000001".to_owned());
    cold_eth_history.latest_registry_owner_ref = Some(reference.clone());
    let mut cold_highwind_history = empty_preloaded_history(
        cold_highwind.labelhashes[0].clone(),
        Some(cold_highwind.clone()),
    );
    cold_highwind_history.current_registry_owner =
        Some("0x0000000000000000000000000000000000000002".to_owned());
    cold_highwind_history.latest_registry_owner_ref = Some(reference);

    let cold_eth_anchor = registry_anchor_for_history(
        &cold_eth_history,
        "ethereum-mainnet",
        &cold_eth.labelhashes[0],
    )
    .context("cold.eth should have a registry-only anchor")?;
    let cold_highwind_anchor = registry_anchor_for_history(
        &cold_highwind_history,
        "ethereum-mainnet",
        &cold_highwind.labelhashes[0],
    )
    .context("cold.highwind.eth should have a registry-only anchor")?;

    assert_ne!(
        cold_eth_anchor.authority_key,
        cold_highwind_anchor.authority_key
    );
    assert_ne!(
        cold_eth_anchor.resource_id,
        cold_highwind_anchor.resource_id
    );
    assert!(cold_eth_anchor.authority_key.ends_with(&cold_eth.namehash));
    assert!(
        cold_highwind_anchor
            .authority_key
            .ends_with(&cold_highwind.namehash)
    );

    Ok(())
}

#[test]
fn registrar_grant_after_wrapper_wrap_keeps_wrapper_as_active_record_authority() -> Result<()> {
    let snakegame = observe_registrar_eth_name_with_version("snakegame", ENS_NORMALIZER_VERSION)?;
    let labelhash = snakegame.labelhashes[0].clone();
    let owner = "0x0000000000000000000000000000000000000001";
    let resolver = "0x00000000000000000000000000000000000000cc";
    let wrapper_ref = wrapper_raw_log(Vec::new(), Vec::new(), 424).reference();
    let registrar_ref = registrar_raw_log(Vec::new(), Vec::new(), 426).reference();
    let record_ref = AuthorityRawLogRow {
        block_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
        block_number: 43,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_043)?,
        log_index: 197,
        ..resolver_raw_log(resolver, Vec::new(), Vec::new(), 197)
    }
    .reference();
    let mut history = empty_preloaded_history(labelhash.clone(), Some(snakegame.clone()));
    history.current_resolver = Some(resolver.to_owned());

    apply_wrapper_name_wrapped(
        &mut history,
        WrapperNameWrappedObservation {
            name: snakegame.clone(),
            owner: owner.to_owned(),
            fuses: 0,
            expiry: OffsetDateTime::from_unix_timestamp(1_800_000_000)?,
            reference: wrapper_ref,
        },
    )?;
    let wrapper_anchor = active_anchor_for_history(&history, "ethereum-mainnet")
        .context("wrapper should be active after wrap")?;
    assert_eq!(wrapper_anchor.kind, AuthorityKind::Wrapper);

    apply_registration_granted(
        &mut history,
        NameRegistrationObservation {
            label: "snakegame".to_owned(),
            labelhash,
            registrant: owner.to_owned(),
            expiry: OffsetDateTime::from_unix_timestamp(1_800_000_000)?,
            reference: registrar_ref,
        },
        &CanonicalBlockIndex { blocks: Vec::new() },
    )?;

    let active_anchor = active_anchor_for_history(&history, "ethereum-mainnet")
        .context("wrapper should remain the active authority")?;
    let registrar_resource_id =
        build_registrar_anchor(history.current_registration.as_ref().unwrap()).resource_id;
    assert_eq!(active_anchor.kind, AuthorityKind::Wrapper);
    assert_eq!(active_anchor.resource_id, wrapper_anchor.resource_id);
    assert_ne!(active_anchor.resource_id, registrar_resource_id);
    assert_eq!(
        history
            .events
            .iter()
            .filter(|event| event.event_kind == EVENT_KIND_SURFACE_BOUND)
            .count(),
        1
    );

    apply_record_changed(
        &mut history,
        RecordChangeObservation {
            namehash: snakegame.namehash,
            resolver: resolver.to_owned(),
            selector: RecordSelector {
                record_key: "contenthash".to_owned(),
                record_family: "contenthash".to_owned(),
                selector_key: None,
            },
            value: Some(json!({
                "encoding": "hex",
                "bytes": "0xe30101",
            })),
            raw_name: None,
            reference: record_ref,
        },
    )?;

    let record_event = history
        .events
        .iter()
        .rev()
        .find(|event| event.event_kind == EVENT_KIND_RECORD_CHANGED)
        .context("record event should be emitted")?;
    assert_eq!(record_event.resource_id, Some(wrapper_anchor.resource_id));

    Ok(())
}

#[test]
fn registration_granted_before_state_carries_previous_registrant() -> Result<()> {
    let name = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let labelhash = name.labelhashes[0].clone();
    let previous_registrant = "0x0000000000000000000000000000000000000001";
    let next_registrant = "0x0000000000000000000000000000000000000002";
    let previous_ref = AuthorityRawLogRow {
        block_hash: "0x5151515151515151515151515151515151515151515151515151515151515151".to_owned(),
        block_number: 51,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_051)?,
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRAR_L1.to_owned(),
        source_manifest_id: 1,
        contract_role: Some("registrar_controller".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 0)
    }
    .reference();
    let grant_ref = AuthorityRawLogRow {
        block_hash: "0x5252525252525252525252525252525252525252525252525252525252525252".to_owned(),
        block_number: 52,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_052)?,
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRAR_L1.to_owned(),
        source_manifest_id: 1,
        contract_role: Some("registrar_controller".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 1)
    }
    .reference();
    let mut history = empty_preloaded_history(labelhash.clone(), Some(name));
    history.current_registration = Some(RegistrationLease {
        authority_key: format!(
            "registrar:ethereum-mainnet:1:{labelhash}:{}:0",
            previous_ref.block_hash
        ),
        labelhash: labelhash.clone(),
        registrant: previous_registrant.to_owned(),
        expiry: OffsetDateTime::from_unix_timestamp(1_800_000_000)?,
        release_ref: None,
        start_ref: previous_ref,
    });

    apply_registration_granted(
        &mut history,
        NameRegistrationObservation {
            label: "alice".to_owned(),
            labelhash,
            registrant: next_registrant.to_owned(),
            expiry: OffsetDateTime::from_unix_timestamp(1_900_000_000)?,
            reference: grant_ref,
        },
        &CanonicalBlockIndex { blocks: Vec::new() },
    )?;

    let grant_event = history
        .events
        .iter()
        .find(|event| event.event_kind == EVENT_KIND_REGISTRATION_GRANTED)
        .context("RegistrationGranted event should be emitted")?;
    assert_eq!(
        grant_event.before_state.get("registrant"),
        Some(&json!(previous_registrant))
    );

    Ok(())
}

#[test]
fn registry_owner_divergence_supersedes_live_registrar_before_wrap() -> Result<()> {
    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let labelhash = alice.labelhashes[0].clone();
    let registrant = "0x0000000000000000000000000000000000000001";
    let registry_owner = "0x0000000000000000000000000000000000000002";
    let wrapper_owner = "0x0000000000000000000000000000000000000003";
    let registration_ref = AuthorityRawLogRow {
        block_number: 42,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_042)?,
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRAR_L1.to_owned(),
        source_manifest_id: 1,
        contract_role: Some("registrar_controller".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 0)
    }
    .reference();
    let registry_ref = AuthorityRawLogRow {
        block_number: 43,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_043)?,
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned(),
        source_manifest_id: 2,
        contract_role: Some("registry".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 1)
    }
    .reference();
    let wrapper_ref = AuthorityRawLogRow {
        block_number: 44,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_044)?,
        source_family: SOURCE_FAMILY_ENS_V1_WRAPPER_L1.to_owned(),
        source_manifest_id: 4,
        contract_role: Some("name_wrapper".to_owned()),
        ..wrapper_raw_log(Vec::new(), Vec::new(), 2)
    }
    .reference();
    let mut history = empty_preloaded_history(labelhash.clone(), Some(alice.clone()));

    apply_registration_granted(
        &mut history,
        NameRegistrationObservation {
            label: "alice".to_owned(),
            labelhash: labelhash.clone(),
            registrant: registrant.to_owned(),
            expiry: OffsetDateTime::from_unix_timestamp(1_800_000_000)?,
            reference: registration_ref,
        },
        &CanonicalBlockIndex { blocks: Vec::new() },
    )?;
    apply_registry_owner_changed(
        &mut history,
        RegistryOwnerObservation {
            parent_node: None,
            labelhash,
            namehash: None,
            owner: registry_owner.to_owned(),
            reference: registry_ref,
        },
    )?;
    assert!(history.current_registration.is_none());
    assert!(history.superseded_registration.is_some());
    let registry_anchor = active_anchor_for_history(&history, "ethereum-mainnet")
        .context("registry-only authority should be active")?;
    assert_eq!(registry_anchor.kind, AuthorityKind::RegistryOnly);
    let registry_transfer = history
        .events
        .iter()
        .find(|event| {
            event.event_kind == EVENT_KIND_AUTHORITY_TRANSFERRED
                && event.resource_id == Some(registry_anchor.resource_id)
        })
        .context("registry owner transfer should be emitted for current registry-only authority")?;
    assert_eq!(
        registry_transfer.after_state.get("owner"),
        Some(&json!(registry_owner))
    );
    let registry_permission = history
        .events
        .iter()
        .rev()
        .find(|event| {
            event.event_kind == EVENT_KIND_PERMISSION_CHANGED
                && event
                    .after_state
                    .pointer("/scope/kind")
                    .and_then(Value::as_str)
                    == Some("resource")
                && event
                    .after_state
                    .pointer("/grant_source/authority_kind")
                    .and_then(Value::as_str)
                    == Some("registry_only")
                && event
                    .after_state
                    .pointer("/grant_source/authority_key")
                    .and_then(Value::as_str)
                    == Some(registry_anchor.authority_key.as_str())
        })
        .context("registry owner grant should use the registry-only authority")?;
    assert_eq!(
        registry_permission.resource_id,
        Some(registry_anchor.resource_id)
    );

    apply_wrapper_name_wrapped(
        &mut history,
        WrapperNameWrappedObservation {
            name: alice,
            owner: wrapper_owner.to_owned(),
            fuses: 0,
            expiry: OffsetDateTime::from_unix_timestamp(1_800_000_000)?,
            reference: wrapper_ref,
        },
    )?;

    let wrapped_event = history
        .events
        .iter()
        .find(|event| event.event_kind == EVENT_KIND_TOKEN_CONTROL_TRANSFERRED)
        .context("wrapper transfer event should be emitted")?;
    assert_eq!(
        wrapped_event.before_state.get("authority_kind"),
        Some(&json!("registry_only"))
    );

    Ok(())
}

#[test]
fn registrar_renewal_keeps_registry_owner_authority_after_divergence() -> Result<()> {
    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let labelhash = alice.labelhashes[0].clone();
    let registrant = "0x0000000000000000000000000000000000000001";
    let registry_owner = "0x0000000000000000000000000000000000000002";
    let registration_ref = AuthorityRawLogRow {
        block_number: 42,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_042)?,
        block_hash: "0x1000000000000000000000000000000000000000000000000000000000000042".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRAR_L1.to_owned(),
        source_manifest_id: 1,
        contract_role: Some("registrar_controller".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 0)
    }
    .reference();
    let registry_ref = AuthorityRawLogRow {
        block_number: 43,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_043)?,
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned(),
        source_manifest_id: 2,
        contract_role: Some("registry".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 1)
    }
    .reference();
    let renewal_ref = AuthorityRawLogRow {
        block_number: 44,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_044)?,
        block_hash: "0x1000000000000000000000000000000000000000000000000000000000000044".to_owned(),
        transaction_hash: "0x2000000000000000000000000000000000000000000000000000000000000044"
            .to_owned(),
        log_index: 44,
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRAR_L1.to_owned(),
        source_manifest_id: 1,
        contract_role: Some("registrar_controller".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 2)
    }
    .reference();
    let mut history = empty_preloaded_history(labelhash.clone(), Some(alice.clone()));
    let before_expiry = OffsetDateTime::from_unix_timestamp(1_800_000_000)?;
    let after_expiry = OffsetDateTime::from_unix_timestamp(1_900_000_000)?;

    apply_registration_granted(
        &mut history,
        NameRegistrationObservation {
            label: "alice".to_owned(),
            labelhash: labelhash.clone(),
            registrant: registrant.to_owned(),
            expiry: before_expiry,
            reference: registration_ref.clone(),
        },
        &CanonicalBlockIndex { blocks: Vec::new() },
    )?;
    let registrar_resource_id = deterministic_uuid(&format!(
        "resource:registrar:ethereum-mainnet:1:{labelhash}:{}:0",
        registration_ref.block_hash
    ));
    apply_registry_owner_changed(
        &mut history,
        RegistryOwnerObservation {
            parent_node: None,
            labelhash: labelhash.clone(),
            namehash: None,
            owner: registry_owner.to_owned(),
            reference: registry_ref,
        },
    )?;

    apply_registration_renewed(
        &mut history,
        NameRenewalObservation {
            label: "alice".to_owned(),
            labelhash,
            expiry: after_expiry,
            reference: renewal_ref,
        },
        &CanonicalBlockIndex { blocks: Vec::new() },
    )?;

    let renewal = history
        .events
        .iter()
        .find(|event| event.event_kind == EVENT_KIND_REGISTRATION_RENEWED)
        .context("renewal event should be emitted")?;
    assert_eq!(renewal.resource_id, Some(registrar_resource_id));
    assert_eq!(
        renewal.before_state["expiry"],
        json!(before_expiry.unix_timestamp())
    );
    assert!(
        !history
            .events
            .iter()
            .any(|event| event.event_kind == EVENT_KIND_REGISTRATION_GRANTED
                && event.block_number == Some(44))
    );
    assert!(
        history.current_registration.is_none(),
        "renewal must not restore registrar authority while registry owner still diverges"
    );
    assert!(
        history
            .superseded_registration
            .as_ref()
            .is_some_and(|lease| lease.expiry == after_expiry),
        "renewal should extend the superseded registrar lease for audit/history"
    );
    let active_anchor = active_anchor_for_history(&history, "ethereum-mainnet")
        .context("registry-only authority should remain active after renewal")?;
    assert_eq!(active_anchor.kind, AuthorityKind::RegistryOnly);
    assert_eq!(
        history.current_registry_owner.as_deref(),
        Some(registry_owner)
    );

    Ok(())
}

#[test]
fn registry_transfer_restores_superseded_registrar_matching_owner_before_release() -> Result<()> {
    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let labelhash = alice.labelhashes[0].clone();
    let registrant = "0x0000000000000000000000000000000000000001";
    let registry_owner = "0x0000000000000000000000000000000000000002";
    let resolver = "0x0000000000000000000000000000000000000003";
    let registration_ref = AuthorityRawLogRow {
        block_number: 42,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_042)?,
        block_hash: "0x1000000000000000000000000000000000000000000000000000000000000042".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRAR_L1.to_owned(),
        source_manifest_id: 1,
        contract_role: Some("registrar_controller".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 0)
    }
    .reference();
    let resolver_ref = AuthorityRawLogRow {
        block_number: 43,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_043)?,
        block_hash: "0x1000000000000000000000000000000000000000000000000000000000000043".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned(),
        source_manifest_id: 2,
        contract_role: Some("registry".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 1)
    }
    .reference();
    let registry_ref = AuthorityRawLogRow {
        block_number: 44,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_044)?,
        block_hash: "0x1000000000000000000000000000000000000000000000000000000000000044".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned(),
        source_manifest_id: 2,
        contract_role: Some("registry".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 2)
    }
    .reference();
    let transfer_ref = AuthorityRawLogRow {
        block_number: 45,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_045)?,
        block_hash: "0x1000000000000000000000000000000000000000000000000000000000000045".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned(),
        source_manifest_id: 2,
        contract_role: Some("registry".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 3)
    }
    .reference();
    let record_ref = AuthorityRawLogRow {
        block_number: 46,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_046)?,
        block_hash: "0x1000000000000000000000000000000000000000000000000000000000000046".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned(),
        source_manifest_id: 3,
        contract_role: Some("public_resolver".to_owned()),
        ..resolver_raw_log(
            "0x00000000000000000000000000000000000000bb",
            Vec::new(),
            Vec::new(),
            4,
        )
    }
    .reference();
    let mut history = empty_preloaded_history(labelhash.clone(), Some(alice.clone()));

    apply_registration_granted(
        &mut history,
        NameRegistrationObservation {
            label: "alice".to_owned(),
            labelhash: labelhash.clone(),
            registrant: registrant.to_owned(),
            expiry: OffsetDateTime::from_unix_timestamp(1_800_000_000)?,
            reference: registration_ref,
        },
        &CanonicalBlockIndex { blocks: Vec::new() },
    )?;
    let registrar_anchor = build_registrar_anchor(history.current_registration.as_ref().unwrap());
    apply_resolver_changed(
        &mut history,
        ResolverObservation {
            namehash: alice.namehash.clone(),
            resolver: resolver.to_owned(),
            reference: resolver_ref,
        },
    )?;
    apply_registry_owner_changed(
        &mut history,
        RegistryOwnerObservation {
            parent_node: None,
            labelhash: labelhash.clone(),
            namehash: None,
            owner: registry_owner.to_owned(),
            reference: registry_ref,
        },
    )?;
    assert!(history.current_registration.is_none());
    assert!(history.superseded_registration.is_some());
    let registry_anchor = active_anchor_for_history(&history, "ethereum-mainnet")
        .context("registry-only authority should be active")?;
    assert_eq!(registry_anchor.kind, AuthorityKind::RegistryOnly);

    apply_registry_owner_changed(
        &mut history,
        RegistryOwnerObservation {
            parent_node: None,
            labelhash: String::new(),
            namehash: Some(alice.namehash.clone()),
            owner: registrant.to_owned(),
            reference: transfer_ref,
        },
    )?;

    assert!(history.current_registration.is_some());
    assert!(history.superseded_registration.is_none());
    let active_anchor = active_anchor_for_history(&history, "ethereum-mainnet")
        .context("registrar authority should be restored")?;
    assert_eq!(active_anchor.kind, AuthorityKind::Registrar);
    assert_eq!(active_anchor.resource_id, registrar_anchor.resource_id);
    assert_eq!(
        active_anchor.token_lineage_id,
        registrar_anchor.token_lineage_id
    );
    let restore_epoch = history
        .events
        .iter()
        .rev()
        .find(|event| {
            event.event_kind == EVENT_KIND_AUTHORITY_EPOCH_CHANGED && event.block_number == Some(45)
        })
        .context("registry transfer should restore the registrar authority epoch")?;
    assert_eq!(
        restore_epoch.before_state.get("authority_kind"),
        Some(&json!("registry_only"))
    );
    assert_eq!(
        restore_epoch.after_state.get("authority_kind"),
        Some(&json!("registrar"))
    );
    let registry_revokes = history
        .events
        .iter()
        .filter(|event| {
            event.event_kind == EVENT_KIND_PERMISSION_CHANGED
                && event.block_number == Some(45)
                && event.resource_id == Some(registry_anchor.resource_id)
                && event.after_state.get("subject").and_then(Value::as_str) == Some(registry_owner)
                && event
                    .after_state
                    .get("effective_powers")
                    .and_then(Value::as_array)
                    .is_some_and(Vec::is_empty)
        })
        .count();
    assert_eq!(registry_revokes, 2);

    apply_record_changed(
        &mut history,
        RecordChangeObservation {
            namehash: alice.namehash,
            resolver: resolver.to_owned(),
            selector: RecordSelector {
                record_key: "addr:60".to_owned(),
                record_family: "addr".to_owned(),
                selector_key: Some("60".to_owned()),
            },
            value: Some(json!(registrant)),
            raw_name: None,
            reference: record_ref,
        },
    )?;
    let record = history
        .events
        .iter()
        .rev()
        .find(|event| event.event_kind == EVENT_KIND_RECORD_CHANGED)
        .context("record event should be emitted")?;
    assert_eq!(record.resource_id, Some(registrar_anchor.resource_id));

    Ok(())
}

#[test]
fn registry_new_owner_reclaim_restores_registrar_before_release() -> Result<()> {
    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let labelhash = alice.labelhashes[0].clone();
    let registrant = "0x0000000000000000000000000000000000000001";
    let registry_owner = "0x0000000000000000000000000000000000000002";
    let registration_ref = AuthorityRawLogRow {
        block_number: 42,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_042)?,
        block_hash: "0x2000000000000000000000000000000000000000000000000000000000000042".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRAR_L1.to_owned(),
        source_manifest_id: 1,
        contract_role: Some("registrar_controller".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 0)
    }
    .reference();
    let registry_ref = AuthorityRawLogRow {
        block_number: 43,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_043)?,
        block_hash: "0x2000000000000000000000000000000000000000000000000000000000000043".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned(),
        source_manifest_id: 2,
        contract_role: Some("registry".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 1)
    }
    .reference();
    let reclaim_ref = AuthorityRawLogRow {
        block_number: 44,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_044)?,
        block_hash: "0x2000000000000000000000000000000000000000000000000000000000000044".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned(),
        source_manifest_id: 2,
        contract_role: Some("registry".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 2)
    }
    .reference();
    let mut history = empty_preloaded_history(labelhash.clone(), Some(alice.clone()));

    apply_registration_granted(
        &mut history,
        NameRegistrationObservation {
            label: "alice".to_owned(),
            labelhash: labelhash.clone(),
            registrant: registrant.to_owned(),
            expiry: OffsetDateTime::from_unix_timestamp(1_800_000_000)?,
            reference: registration_ref,
        },
        &CanonicalBlockIndex { blocks: Vec::new() },
    )?;
    let registrar_anchor = build_registrar_anchor(history.current_registration.as_ref().unwrap());
    apply_registry_owner_changed(
        &mut history,
        RegistryOwnerObservation {
            parent_node: None,
            labelhash: labelhash.clone(),
            namehash: None,
            owner: registry_owner.to_owned(),
            reference: registry_ref,
        },
    )?;
    assert!(history.current_registration.is_none());
    assert!(history.superseded_registration.is_some());

    apply_registry_owner_changed(
        &mut history,
        RegistryOwnerObservation {
            parent_node: Some(eth_node()),
            labelhash,
            namehash: Some(alice.namehash),
            owner: registrant.to_owned(),
            reference: reclaim_ref,
        },
    )?;

    assert!(history.current_registration.is_some());
    assert!(history.superseded_registration.is_none());
    let active_anchor = active_anchor_for_history(&history, "ethereum-mainnet")
        .context("registrar authority should be restored")?;
    assert_eq!(active_anchor.kind, AuthorityKind::Registrar);
    assert_eq!(active_anchor.resource_id, registrar_anchor.resource_id);
    assert_eq!(
        active_anchor.token_lineage_id,
        registrar_anchor.token_lineage_id
    );
    let restore_epoch = history
        .events
        .iter()
        .rev()
        .find(|event| {
            event.event_kind == EVENT_KIND_AUTHORITY_EPOCH_CHANGED && event.block_number == Some(44)
        })
        .context("registry NewOwner reclaim should restore the registrar authority epoch")?;
    assert_eq!(
        restore_epoch.before_state.get("authority_kind"),
        Some(&json!("registry_only"))
    );
    assert_eq!(
        restore_epoch.after_state.get("authority_kind"),
        Some(&json!("registrar"))
    );

    Ok(())
}

#[test]
fn token_transfer_restores_superseded_registrar_matching_registry_owner() -> Result<()> {
    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let labelhash = alice.labelhashes[0].clone();
    let registrant = "0x0000000000000000000000000000000000000001";
    let registry_owner = "0x0000000000000000000000000000000000000002";
    let resolver = "0x0000000000000000000000000000000000000003";
    let registration_ref = AuthorityRawLogRow {
        block_number: 42,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_042)?,
        block_hash: "0x1000000000000000000000000000000000000000000000000000000000000042".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRAR_L1.to_owned(),
        source_manifest_id: 1,
        contract_role: Some("registrar_controller".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 0)
    }
    .reference();
    let resolver_ref = AuthorityRawLogRow {
        block_number: 43,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_043)?,
        block_hash: "0x1000000000000000000000000000000000000000000000000000000000000043".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned(),
        source_manifest_id: 2,
        contract_role: Some("registry".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 1)
    }
    .reference();
    let registry_ref = AuthorityRawLogRow {
        block_number: 44,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_044)?,
        block_hash: "0x1000000000000000000000000000000000000000000000000000000000000044".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned(),
        source_manifest_id: 2,
        contract_role: Some("registry".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 2)
    }
    .reference();
    let transfer_ref = AuthorityRawLogRow {
        block_number: 45,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_045)?,
        block_hash: "0x1000000000000000000000000000000000000000000000000000000000000045".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRAR_L1.to_owned(),
        source_manifest_id: 1,
        contract_role: Some("registrar_controller".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 3)
    }
    .reference();
    let record_ref = AuthorityRawLogRow {
        block_number: 46,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_046)?,
        block_hash: "0x1000000000000000000000000000000000000000000000000000000000000046".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned(),
        source_manifest_id: 3,
        contract_role: Some("public_resolver".to_owned()),
        ..resolver_raw_log(
            "0x00000000000000000000000000000000000000bb",
            Vec::new(),
            Vec::new(),
            4,
        )
    }
    .reference();
    let mut history = empty_preloaded_history(labelhash.clone(), Some(alice.clone()));

    apply_registration_granted(
        &mut history,
        NameRegistrationObservation {
            label: "alice".to_owned(),
            labelhash: labelhash.clone(),
            registrant: registrant.to_owned(),
            expiry: OffsetDateTime::from_unix_timestamp(1_800_000_000)?,
            reference: registration_ref,
        },
        &CanonicalBlockIndex { blocks: Vec::new() },
    )?;
    let registrar_resource_id =
        build_registrar_anchor(history.current_registration.as_ref().unwrap()).resource_id;
    apply_resolver_changed(
        &mut history,
        ResolverObservation {
            namehash: alice.namehash.clone(),
            resolver: resolver.to_owned(),
            reference: resolver_ref,
        },
    )?;
    apply_registry_owner_changed(
        &mut history,
        RegistryOwnerObservation {
            parent_node: None,
            labelhash: labelhash.clone(),
            namehash: None,
            owner: registry_owner.to_owned(),
            reference: registry_ref,
        },
    )?;
    assert!(history.current_registration.is_none());
    assert!(history.superseded_registration.is_some());
    let registry_anchor = active_anchor_for_history(&history, "ethereum-mainnet")
        .context("registry-only authority should be active")?;
    assert_eq!(registry_anchor.kind, AuthorityKind::RegistryOnly);

    apply_token_transferred(
        &mut history,
        TokenTransferObservation {
            labelhash,
            from_address: registrant.to_owned(),
            to_address: registry_owner.to_owned(),
            reference: transfer_ref,
        },
    )?;

    assert!(history.current_registration.is_some());
    assert!(history.superseded_registration.is_none());
    let active_anchor = active_anchor_for_history(&history, "ethereum-mainnet")
        .context("registrar authority should be restored")?;
    assert_eq!(active_anchor.kind, AuthorityKind::Registrar);
    assert_eq!(active_anchor.resource_id, registrar_resource_id);
    let restore_epoch = history
        .events
        .iter()
        .rev()
        .find(|event| {
            event.event_kind == EVENT_KIND_AUTHORITY_EPOCH_CHANGED && event.block_number == Some(45)
        })
        .context("token transfer should restore the registrar authority epoch")?;
    assert_eq!(
        restore_epoch.before_state.get("authority_kind"),
        Some(&json!("registry_only"))
    );
    assert_eq!(
        restore_epoch.after_state.get("authority_kind"),
        Some(&json!("registrar"))
    );
    let registry_revokes = history
        .events
        .iter()
        .filter(|event| {
            event.event_kind == EVENT_KIND_PERMISSION_CHANGED
                && event.block_number == Some(45)
                && event.resource_id == Some(registry_anchor.resource_id)
                && event.after_state.get("subject").and_then(Value::as_str) == Some(registry_owner)
                && event
                    .after_state
                    .get("effective_powers")
                    .and_then(Value::as_array)
                    .is_some_and(Vec::is_empty)
        })
        .collect::<Vec<_>>();
    assert_eq!(registry_revokes.len(), 2);
    assert!(registry_revokes.iter().any(|event| {
        event
            .after_state
            .pointer("/scope/kind")
            .and_then(Value::as_str)
            == Some("resource")
    }));
    assert!(registry_revokes.iter().any(|event| {
        event
            .after_state
            .pointer("/scope/kind")
            .and_then(Value::as_str)
            == Some("resolver")
    }));

    apply_record_changed(
        &mut history,
        RecordChangeObservation {
            namehash: alice.namehash,
            resolver: resolver.to_owned(),
            selector: RecordSelector {
                record_key: "addr:60".to_owned(),
                record_family: "addr".to_owned(),
                selector_key: Some("60".to_owned()),
            },
            value: Some(json!(registry_owner)),
            raw_name: None,
            reference: record_ref,
        },
    )?;
    let record = history
        .events
        .iter()
        .rev()
        .find(|event| event.event_kind == EVENT_KIND_RECORD_CHANGED)
        .context("record event should be emitted")?;
    assert_eq!(record.resource_id, Some(registrar_resource_id));

    Ok(())
}

#[test]
fn token_transfer_away_from_registry_owner_promotes_registry_only_authority() -> Result<()> {
    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let labelhash = alice.labelhashes[0].clone();
    let registry_owner = "0x0000000000000000000000000000000000000002";
    let token_holder = "0x0000000000000000000000000000000000000003";
    let resolver = "0x0000000000000000000000000000000000000004";
    let registration_ref = AuthorityRawLogRow {
        block_number: 42,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_042)?,
        block_hash: "0x1000000000000000000000000000000000000000000000000000000000000042".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRAR_L1.to_owned(),
        source_manifest_id: 1,
        contract_role: Some("registrar_controller".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 0)
    }
    .reference();
    let registry_ref = AuthorityRawLogRow {
        block_number: 43,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_043)?,
        block_hash: "0x1000000000000000000000000000000000000000000000000000000000000043".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned(),
        source_manifest_id: 2,
        contract_role: Some("registry".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 1)
    }
    .reference();
    let resolver_ref = AuthorityRawLogRow {
        block_number: 44,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_044)?,
        block_hash: "0x1000000000000000000000000000000000000000000000000000000000000044".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned(),
        source_manifest_id: 2,
        contract_role: Some("registry".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 2)
    }
    .reference();
    let transfer_ref = AuthorityRawLogRow {
        block_number: 45,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_045)?,
        block_hash: "0x1000000000000000000000000000000000000000000000000000000000000045".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRAR_L1.to_owned(),
        source_manifest_id: 1,
        contract_role: Some("registrar_controller".to_owned()),
        ..registrar_raw_log(Vec::new(), Vec::new(), 3)
    }
    .reference();
    let mut history = empty_preloaded_history(labelhash.clone(), Some(alice.clone()));

    apply_registration_granted(
        &mut history,
        NameRegistrationObservation {
            label: "alice".to_owned(),
            labelhash: labelhash.clone(),
            registrant: registry_owner.to_owned(),
            expiry: OffsetDateTime::from_unix_timestamp(1_800_000_000)?,
            reference: registration_ref,
        },
        &CanonicalBlockIndex { blocks: Vec::new() },
    )?;
    let registrar_resource_id =
        build_registrar_anchor(history.current_registration.as_ref().unwrap()).resource_id;
    apply_registry_owner_changed(
        &mut history,
        RegistryOwnerObservation {
            parent_node: None,
            labelhash: labelhash.clone(),
            namehash: None,
            owner: registry_owner.to_owned(),
            reference: registry_ref,
        },
    )?;
    apply_resolver_changed(
        &mut history,
        ResolverObservation {
            namehash: alice.namehash,
            resolver: resolver.to_owned(),
            reference: resolver_ref,
        },
    )?;

    apply_token_transferred(
        &mut history,
        TokenTransferObservation {
            labelhash,
            from_address: registry_owner.to_owned(),
            to_address: token_holder.to_owned(),
            reference: transfer_ref,
        },
    )?;

    assert!(history.current_registration.is_none());
    assert!(
        history
            .superseded_registration
            .as_ref()
            .is_some_and(|lease| lease.registrant == token_holder)
    );
    let registry_anchor = active_anchor_for_history(&history, "ethereum-mainnet")
        .context("registry-only authority should become active after token divergence")?;
    assert_eq!(registry_anchor.kind, AuthorityKind::RegistryOnly);
    assert_ne!(registry_anchor.resource_id, registrar_resource_id);

    let active_registry_owner_event = history
        .events
        .iter()
        .find(|event| {
            event.event_kind == EVENT_KIND_AUTHORITY_TRANSFERRED
                && event.resource_id == Some(registrar_resource_id)
                && event.after_state.get("owner").and_then(Value::as_str) == Some(registry_owner)
        })
        .context("registry owner should be visible while registrar authority is active")?;
    assert!(
        active_registry_owner_event
            .event_identity
            .contains("registry-active-transfer")
    );

    let divergence_epoch = history
        .events
        .iter()
        .rev()
        .find(|event| {
            event.event_kind == EVENT_KIND_AUTHORITY_EPOCH_CHANGED && event.block_number == Some(45)
        })
        .context("token divergence should switch to registry-only authority")?;
    assert_eq!(
        divergence_epoch.after_state.get("authority_kind"),
        Some(&json!("registry_only"))
    );
    assert_eq!(
        divergence_epoch.after_state.get("registry_owner"),
        Some(&json!(registry_owner))
    );

    let registry_grants = history
        .events
        .iter()
        .filter(|event| {
            event.event_kind == EVENT_KIND_PERMISSION_CHANGED
                && event.block_number == Some(45)
                && event.resource_id == Some(registry_anchor.resource_id)
                && event.after_state.get("subject").and_then(Value::as_str) == Some(registry_owner)
                && event
                    .after_state
                    .get("effective_powers")
                    .and_then(Value::as_array)
                    .is_some_and(|powers| !powers.is_empty())
        })
        .collect::<Vec<_>>();
    assert_eq!(registry_grants.len(), 2);
    assert!(registry_grants.iter().any(|event| {
        event
            .after_state
            .pointer("/scope/kind")
            .and_then(Value::as_str)
            == Some("resource")
    }));
    assert!(registry_grants.iter().any(|event| {
        event
            .after_state
            .pointer("/scope/kind")
            .and_then(Value::as_str)
            == Some("resolver")
    }));

    let registrar_owner_revokes = history
        .events
        .iter()
        .filter(|event| {
            event.event_kind == EVENT_KIND_PERMISSION_CHANGED
                && event.block_number == Some(45)
                && event.resource_id == Some(registrar_resource_id)
                && event.after_state.get("subject").and_then(Value::as_str) == Some(registry_owner)
                && event
                    .after_state
                    .get("effective_powers")
                    .and_then(Value::as_array)
                    .is_some_and(Vec::is_empty)
        })
        .count();
    assert_eq!(registrar_owner_revokes, 0);

    Ok(())
}

#[test]
fn registrar_renewal_observation_replaces_same_label_subdomain_preload_name() -> Result<()> {
    let labelhash = keccak256_hex(b"palimpsest");
    let raw_log = registrar_raw_log(
        vec![unwrapped_name_renewed_topic0(), labelhash.clone()],
        encode_controller_label_event_log_data("palimpsest", &[1, 1_814_474_767, 3]),
        373,
    );
    let reference = raw_log.reference();
    let wrong_name = observe_text_name_with_reference(
        "palimpsest.holer.eth",
        &reference,
        ENS_NORMALIZER_VERSION,
    )?;
    let mut history = empty_preloaded_history(labelhash.clone(), Some(wrong_name));
    history.current_registration = Some(RegistrationLease {
        authority_key: format!(
            "registrar:ethereum-mainnet:1:{labelhash}:{}:7",
            reference.block_hash
        ),
        labelhash: labelhash.clone(),
        registrant: ZERO_ADDRESS.to_owned(),
        expiry: OffsetDateTime::from_unix_timestamp(1_782_938_767)?,
        release_ref: None,
        start_ref: reference.clone(),
    });

    apply_registration_renewed(
        &mut history,
        NameRenewalObservation {
            label: "palimpsest".to_owned(),
            labelhash,
            expiry: OffsetDateTime::from_unix_timestamp(1_814_474_767)?,
            reference,
        },
        &CanonicalBlockIndex { blocks: Vec::new() },
    )?;

    assert_eq!(
        history
            .name
            .as_ref()
            .map(|name| name.logical_name_id.as_str()),
        Some("ens:palimpsest.eth")
    );
    assert!(history.events.iter().any(|event| {
        event.event_kind == EVENT_KIND_REGISTRATION_RENEWED
            && event.logical_name_id.as_deref() == Some("ens:palimpsest.eth")
    }));
    assert!(
        !history
            .events
            .iter()
            .any(|event| { event.logical_name_id.as_deref() == Some("ens:palimpsest.holer.eth") })
    );

    Ok(())
}

#[test]
fn build_authority_observation_skips_malformed_resolver_text_payloads() -> Result<()> {
    let event_topics = AuthorityEventTopics::for_tests();
    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let resolver_address = "0x00000000000000000000000000000000000000cc";

    let malformed = build_authority_observation(
        &resolver_raw_log(
            resolver_address,
            vec![
                text_changed_topic0(),
                alice.namehash.clone(),
                keccak256_hex(b"com.twitter"),
                keccak256_hex(b"ignored-value"),
            ],
            Vec::new(),
            0,
        ),
        &event_topics,
    )?;
    assert_eq!(malformed, None);

    let mismatched_indexed_key = build_authority_observation(
        &resolver_raw_log(
            resolver_address,
            vec![
                text_changed_topic0(),
                alice.namehash,
                keccak256_hex(b"not-com.twitter"),
            ],
            encode_two_dynamic_string_log_data("com.twitter", "alice-twitter"),
            1,
        ),
        &event_topics,
    )?;
    assert_eq!(mismatched_indexed_key, None);

    Ok(())
}

#[test]
fn build_authority_observation_decodes_resolver_record_logs() -> Result<()> {
    let event_topics = AuthorityEventTopics::for_tests();
    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let resolver_address = "0x00000000000000000000000000000000000000cc";

    let legacy_text_observation = build_authority_observation(
        &resolver_raw_log(
            resolver_address,
            vec![
                text_changed_topic0(),
                alice.namehash.clone(),
                keccak256_hex(b"com.twitter"),
            ],
            encode_dynamic_string_log_data("com.twitter"),
            0,
        ),
        &event_topics,
    )?
    .context("legacy TextChanged observation should decode")?;
    assert_eq!(
        legacy_text_observation,
        AuthorityObservation::RecordChanged(RecordChangeObservation {
            namehash: alice.namehash.clone(),
            resolver: resolver_address.to_owned(),
            selector: RecordSelector {
                record_key: "text:com.twitter".to_owned(),
                record_family: "text".to_owned(),
                selector_key: Some("com.twitter".to_owned()),
            },
            value: None,
            raw_name: None,
            reference: resolver_raw_log(resolver_address, Vec::new(), Vec::new(), 0).reference(),
        })
    );

    let text_with_value_observation = build_authority_observation(
        &resolver_raw_log(
            resolver_address,
            vec![
                text_changed_with_value_topic0(),
                alice.namehash.clone(),
                keccak256_hex(b"com.twitter"),
            ],
            encode_two_dynamic_string_log_data("com.twitter", "alice-twitter"),
            1,
        ),
        &event_topics,
    )?
    .context("TextChanged observation with value should decode")?;
    assert_eq!(
        text_with_value_observation,
        AuthorityObservation::RecordChanged(RecordChangeObservation {
            namehash: alice.namehash.clone(),
            resolver: resolver_address.to_owned(),
            selector: RecordSelector {
                record_key: "text:com.twitter".to_owned(),
                record_family: "text".to_owned(),
                selector_key: Some("com.twitter".to_owned()),
            },
            value: Some(json!("alice-twitter")),
            raw_name: None,
            reference: resolver_raw_log(resolver_address, Vec::new(), Vec::new(), 1).reference(),
        })
    );

    let name_observation = build_authority_observation(
        &resolver_raw_log(
            resolver_address,
            vec![name_changed_topic0(), alice.namehash.clone()],
            encode_dynamic_string_log_data("alice.eth"),
            2,
        ),
        &event_topics,
    )?
    .context("NameChanged observation should decode")?;
    assert_eq!(
        name_observation,
        AuthorityObservation::RecordChanged(RecordChangeObservation {
            namehash: alice.namehash.clone(),
            resolver: resolver_address.to_owned(),
            selector: RecordSelector {
                record_key: "name".to_owned(),
                record_family: "name".to_owned(),
                selector_key: None,
            },
            value: Some(json!("alice.eth")),
            raw_name: Some("alice.eth".to_owned()),
            reference: resolver_raw_log(resolver_address, Vec::new(), Vec::new(), 2).reference(),
        })
    );

    let addr_observation = build_authority_observation(
        &resolver_raw_log(
            resolver_address,
            vec![addr_changed_topic0(), alice.namehash.clone()],
            encode_resolver_addr_changed_log_data("0x00000000000000000000000000000000000000aa"),
            3,
        ),
        &event_topics,
    )?
    .context("AddrChanged observation should decode")?;
    assert_eq!(
        addr_observation,
        AuthorityObservation::RecordChanged(RecordChangeObservation {
            namehash: alice.namehash.clone(),
            resolver: resolver_address.to_owned(),
            selector: RecordSelector {
                record_key: "addr:60".to_owned(),
                record_family: "addr".to_owned(),
                selector_key: Some("60".to_owned()),
            },
            value: Some(json!("0x00000000000000000000000000000000000000aa")),
            raw_name: None,
            reference: resolver_raw_log(resolver_address, Vec::new(), Vec::new(), 3).reference(),
        })
    );

    let multicoin_addr_observation = build_authority_observation(
        &resolver_raw_log(
            resolver_address,
            vec![address_changed_topic0(), alice.namehash.clone()],
            encode_resolver_address_changed_log_data(61, &[0xde, 0xad, 0xbe, 0xef]),
            4,
        ),
        &event_topics,
    )?
    .context("AddressChanged observation should decode")?;
    assert_eq!(
        multicoin_addr_observation,
        AuthorityObservation::RecordChanged(RecordChangeObservation {
            namehash: alice.namehash.clone(),
            resolver: resolver_address.to_owned(),
            selector: RecordSelector {
                record_key: "addr:61".to_owned(),
                record_family: "addr".to_owned(),
                selector_key: Some("61".to_owned()),
            },
            value: Some(json!({
                "encoding": "hex",
                "bytes": "0xdeadbeef",
            })),
            raw_name: None,
            reference: resolver_raw_log(resolver_address, Vec::new(), Vec::new(), 4).reference(),
        })
    );

    let data_hash = keccak256_hex(&[0xde, 0xad, 0xbe, 0xef]);
    let data_observation = build_authority_observation(
        &resolver_raw_log(
            resolver_address,
            vec![
                data_changed_topic0(),
                alice.namehash.clone(),
                keccak256_hex(b"avatar"),
                data_hash.clone(),
            ],
            encode_dynamic_string_log_data("avatar"),
            5,
        ),
        &event_topics,
    )?
    .context("DataChanged observation should decode")?;
    assert_eq!(
        data_observation,
        AuthorityObservation::RecordChanged(RecordChangeObservation {
            namehash: alice.namehash.clone(),
            resolver: resolver_address.to_owned(),
            selector: RecordSelector {
                record_key: "data:avatar".to_owned(),
                record_family: "data".to_owned(),
                selector_key: Some("avatar".to_owned()),
            },
            value: Some(json!({ "indexed_data_hash": data_hash })),
            raw_name: None,
            reference: resolver_raw_log(resolver_address, Vec::new(), Vec::new(), 5).reference(),
        })
    );

    let contenthash_bytes = [0xe3, 0x01, 0x01];
    let contenthash_observation = build_authority_observation(
        &resolver_raw_log(
            resolver_address,
            vec![
                keccak256_hex(CONTENTHASH_CHANGED_SIGNATURE.as_bytes()),
                alice.namehash.clone(),
            ],
            encode_dynamic_bytes_log_data(&contenthash_bytes),
            6,
        ),
        &event_topics,
    )?
    .context("ContenthashChanged observation should decode")?;
    assert_eq!(
        contenthash_observation,
        AuthorityObservation::RecordChanged(RecordChangeObservation {
            namehash: alice.namehash.clone(),
            resolver: resolver_address.to_owned(),
            selector: RecordSelector {
                record_key: "contenthash".to_owned(),
                record_family: "contenthash".to_owned(),
                selector_key: None,
            },
            value: Some(json!({
                "encoding": "hex",
                "bytes": "0xe30101",
            })),
            raw_name: None,
            reference: resolver_raw_log(resolver_address, Vec::new(), Vec::new(), 6).reference(),
        })
    );

    let abi_observation = build_authority_observation(
        &resolver_raw_log(
            resolver_address,
            vec![
                keccak256_hex(ABI_CHANGED_SIGNATURE.as_bytes()),
                alice.namehash.clone(),
                hex_string(abi_word_u64(2)),
            ],
            Vec::new(),
            7,
        ),
        &event_topics,
    )?
    .context("ABIChanged observation should decode")?;
    assert_eq!(
        abi_observation,
        AuthorityObservation::RecordChanged(RecordChangeObservation {
            namehash: alice.namehash.clone(),
            resolver: resolver_address.to_owned(),
            selector: RecordSelector {
                record_key: "abi:2".to_owned(),
                record_family: "abi".to_owned(),
                selector_key: Some("2".to_owned()),
            },
            value: Some(json!(2)),
            raw_name: None,
            reference: resolver_raw_log(resolver_address, Vec::new(), Vec::new(), 7).reference(),
        })
    );

    let interface_observation = build_authority_observation(
        &resolver_raw_log(
            resolver_address,
            vec![
                keccak256_hex(INTERFACE_CHANGED_SIGNATURE.as_bytes()),
                alice.namehash.clone(),
                hex_string(abi_word_fixed_bytes(&[0x01, 0xff, 0xc9, 0xa7])),
            ],
            abi_word_address("0x00000000000000000000000000000000000000dd").to_vec(),
            8,
        ),
        &event_topics,
    )?
    .context("InterfaceChanged observation should decode")?;
    assert_eq!(
        interface_observation,
        AuthorityObservation::RecordChanged(RecordChangeObservation {
            namehash: alice.namehash.clone(),
            resolver: resolver_address.to_owned(),
            selector: RecordSelector {
                record_key: "interface:0x01ffc9a7".to_owned(),
                record_family: "interface".to_owned(),
                selector_key: Some("0x01ffc9a7".to_owned()),
            },
            value: Some(json!("0x00000000000000000000000000000000000000dd")),
            raw_name: None,
            reference: resolver_raw_log(resolver_address, Vec::new(), Vec::new(), 8).reference(),
        })
    );

    let pubkey_observation = build_authority_observation(
        &resolver_raw_log(
            resolver_address,
            vec![pubkey_changed_topic0(), alice.namehash.clone()],
            vec![0; 64],
            9,
        ),
        &event_topics,
    )?;
    assert_eq!(pubkey_observation, None);

    let record_version_observation = build_authority_observation(
        &resolver_raw_log(
            resolver_address,
            vec![version_changed_topic0(), alice.namehash.clone()],
            encode_resolver_version_changed_log_data(7),
            10,
        ),
        &event_topics,
    )?
    .context("VersionChanged observation should decode")?;
    assert_eq!(
        record_version_observation,
        AuthorityObservation::RecordVersionChanged(RecordVersionObservation {
            namehash: alice.namehash,
            resolver: resolver_address.to_owned(),
            record_version: 7,
            reference: resolver_raw_log(resolver_address, Vec::new(), Vec::new(), 10).reference(),
        })
    );

    Ok(())
}

#[test]
fn build_authority_observation_decodes_wrapper_logs() -> Result<()> {
    let event_topics = AuthorityEventTopics::for_tests();
    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let dns_name = dns_encoded_name(&["alice", "eth"]);
    let owner = "0x0000000000000000000000000000000000000001";

    let wrapped_observation = build_authority_observation(
        &wrapper_raw_log(
            vec![name_wrapped_topic0(), alice.namehash.clone()],
            encode_name_wrapped_log_data(&dns_name, owner, 8, 1_800_000_000),
            0,
        ),
        &event_topics,
    )?
    .context("NameWrapped observation should decode")?;
    assert_eq!(
        wrapped_observation,
        AuthorityObservation::WrapperNameWrapped(WrapperNameWrappedObservation {
            name: alice.clone(),
            owner: owner.to_owned(),
            fuses: 8,
            expiry: OffsetDateTime::from_unix_timestamp(1_800_000_000)?,
            reference: wrapper_raw_log(Vec::new(), Vec::new(), 0).reference(),
        })
    );

    let cased_dns_name = dns_encoded_name(&["Sean", "decashed", "com"]);
    let cased_namehash = namehash_hex(&[b"Sean".to_vec(), b"decashed".to_vec(), b"com".to_vec()]);
    assert_ne!(
        cased_namehash,
        namehash_hex(&[b"sean".to_vec(), b"decashed".to_vec(), b"com".to_vec(),])
    );
    let cased_wrapped_observation = build_authority_observation(
        &wrapper_raw_log(
            vec![name_wrapped_topic0(), cased_namehash.clone()],
            encode_name_wrapped_log_data(&cased_dns_name, owner, 0, 0),
            99,
        ),
        &event_topics,
    )?;
    assert_eq!(cased_wrapped_observation, None);

    let nul_label_dns_name = vec![3, b'b', 0, b'd', 3, b'e', b't', b'h', 0];
    assert_eq!(
        build_authority_observation(
            &wrapper_raw_log(
                vec![
                    name_wrapped_topic0(),
                    namehash_hex(&[b"b\0d".to_vec(), b"eth".to_vec()])
                ],
                encode_name_wrapped_log_data(&nul_label_dns_name, owner, 0, 0),
                98,
            ),
            &event_topics,
        )?,
        None
    );

    let unwrapped_observation = build_authority_observation(
        &wrapper_raw_log(
            vec![name_unwrapped_topic0(), alice.namehash.clone()],
            encode_name_unwrapped_log_data("0x0000000000000000000000000000000000000002"),
            1,
        ),
        &event_topics,
    )?
    .context("NameUnwrapped observation should decode")?;
    assert_eq!(
        unwrapped_observation,
        AuthorityObservation::WrapperNameUnwrapped(WrapperNameUnwrappedObservation {
            namehash: alice.namehash.clone(),
            owner: "0x0000000000000000000000000000000000000002".to_owned(),
            reference: wrapper_raw_log(Vec::new(), Vec::new(), 1).reference(),
        })
    );

    let fuses_observation = build_authority_observation(
        &wrapper_raw_log(
            vec![fuses_set_topic0(), alice.namehash.clone()],
            encode_fuses_set_log_data(10),
            2,
        ),
        &event_topics,
    )?
    .context("FusesSet observation should decode")?;
    assert_eq!(
        fuses_observation,
        AuthorityObservation::WrapperFusesSet(WrapperFusesObservation {
            namehash: alice.namehash.clone(),
            fuses: 10,
            reference: wrapper_raw_log(Vec::new(), Vec::new(), 2).reference(),
        })
    );

    let expiry_observation = build_authority_observation(
        &wrapper_raw_log(
            vec![expiry_extended_topic0(), alice.namehash.clone()],
            encode_expiry_extended_log_data(1_800_000_100),
            3,
        ),
        &event_topics,
    )?
    .context("ExpiryExtended observation should decode")?;
    assert_eq!(
        expiry_observation,
        AuthorityObservation::WrapperExpiryExtended(WrapperExpiryObservation {
            namehash: alice.namehash.clone(),
            expiry: OffsetDateTime::from_unix_timestamp(1_800_000_100)?,
            reference: wrapper_raw_log(Vec::new(), Vec::new(), 3).reference(),
        })
    );

    let transfer_observation = build_authority_observation(
        &wrapper_raw_log(
            vec![
                transfer_single_topic0(),
                hex_string(&abi_word_address(
                    "0x00000000000000000000000000000000000000ff",
                )),
                hex_string(&abi_word_address(
                    "0x0000000000000000000000000000000000000001",
                )),
                hex_string(&abi_word_address(
                    "0x0000000000000000000000000000000000000002",
                )),
            ],
            encode_transfer_single_log_data(&alice.namehash, 1),
            4,
        ),
        &event_topics,
    )?
    .context("TransferSingle observation should decode")?;
    assert_eq!(
        transfer_observation,
        AuthorityObservation::WrapperTokenTransferred(WrapperTokenTransferObservation {
            namehash: alice.namehash,
            from_address: "0x0000000000000000000000000000000000000001".to_owned(),
            to_address: "0x0000000000000000000000000000000000000002".to_owned(),
            value: 1,
            transfer_index: None,
            reference: wrapper_raw_log(Vec::new(), Vec::new(), 4).reference(),
        })
    );

    Ok(())
}

#[tokio::test]
async fn sync_ens_v1_unwrapped_authority_persists_registrar_identity_rows_idempotently()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "ens",
            source_family: SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v1",
            rollout_status: "active",
            normalizer_version: "ensip15@ens-normalize-0.1.1",
            file_path: "manifests/ens/ens_v1_registrar_l1/v1.toml",
        },
    )
    .await?;
    let contract_instance_id = Uuid::new_v4();
    insert_contract_instance(
        database.pool(),
        contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        ManifestContractInstanceSeed {
            manifest_id,
            declaration_kind: "contract",
            declaration_name: "registrar",
            contract_instance_id,
            declared_address: "0x00000000000000000000000000000000000000aa",
            role: Some("registrar"),
            proxy_kind: Some("none"),
        },
    )
    .await?;
    insert_contract_instance_address(
        database.pool(),
        contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
        manifest_id,
    )
    .await?;
    upsert_raw_blocks(
        database.pool(),
        &[raw_block(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            42,
            1_700_000_042,
        )],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[RawLog {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            block_number: 42,
            transaction_hash: "0xtxaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            transaction_index: 0,
            log_index: 0,
            emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
            topics: vec![
                name_registered_topic0(),
                keccak256_hex(b"alice"),
                hex_string(&abi_word_address(
                    "0x0000000000000000000000000000000000000001",
                )),
            ],
            data: encode_registrar_name_registered_log_data("alice", 1_700_010_000),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;

    let checkpoint_context = crate::ens_v1_subregistry_discovery::ReplayAdapterCheckpointContext {
        deployment_profile: "test".to_owned(),
        cursor_kind: "raw_fact_normalized_events".to_owned(),
        range_start_block_number: 42,
        target_block_number: 42,
    };
    let first = sync_ens_v1_unwrapped_authority_with_replay_checkpoint_and_log_limit(
        database.pool(),
        "ethereum-mainnet",
        &checkpoint_context,
        100_000,
    )
    .await?;
    assert_eq!(first.scanned_log_count, 1);
    assert_eq!(first.matched_log_count, 1);
    assert_eq!(first.total_name_surface_count, 1);
    assert_eq!(first.total_resource_count, 1);
    assert_eq!(first.total_surface_binding_count, 1);
    assert_eq!(first.total_normalized_event_count, 5);
    assert_eq!(
        first.by_kind.get(EVENT_KIND_REGISTRATION_GRANTED),
        Some(&1_usize)
    );
    assert_eq!(first.by_kind.get(EVENT_KIND_EXPIRY_CHANGED), Some(&1_usize));
    assert_eq!(
        first.by_kind.get(EVENT_KIND_PERMISSION_CHANGED),
        Some(&1_usize)
    );
    assert_eq!(first.by_kind.get(EVENT_KIND_SURFACE_BOUND), Some(&1_usize));
    assert_eq!(
        first.by_kind.get(EVENT_KIND_AUTHORITY_EPOCH_CHANGED),
        Some(&1_usize)
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT status FROM normalized_replay_adapter_checkpoints WHERE adapter = 'ens_v1_unwrapped_authority'"
        )
        .fetch_one(database.pool())
        .await?,
        "completed"
    );
    assert!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_replay_adapter_checkpoint_items WHERE adapter = 'ens_v1_unwrapped_authority'"
        )
        .fetch_one(database.pool())
        .await?
            > 0
    );

    let second = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(second.scanned_log_count, 1);
    assert_eq!(second.matched_log_count, 1);
    assert_eq!(second.total_name_surface_count, 1);
    assert_eq!(second.total_resource_count, 1);
    assert_eq!(second.total_surface_binding_count, 1);
    assert_eq!(second.total_normalized_event_count, 5);

    assert!(
        load_name_surface(database.pool(), "ens:alice.eth")
            .await?
            .is_some()
    );
    let bindings =
        load_surface_bindings_by_logical_name_id(database.pool(), "ens:alice.eth").await?;
    assert_eq!(bindings.len(), 1);
    assert_eq!(
        bindings[0].binding_kind,
        SurfaceBindingKind::DeclaredRegistryPath
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM token_lineages")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM resources")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        5
    );
    assert_eq!(
        load_normalized_event_counts_by_kind(database.pool(), "ens").await?,
        BTreeMap::from([
            (EVENT_KIND_AUTHORITY_EPOCH_CHANGED.to_owned(), 1_usize),
            (EVENT_KIND_EXPIRY_CHANGED.to_owned(), 1_usize),
            (EVENT_KIND_PERMISSION_CHANGED.to_owned(), 1_usize),
            (EVENT_KIND_REGISTRATION_GRANTED.to_owned(), 1_usize),
            (EVENT_KIND_SURFACE_BOUND.to_owned(), 1_usize),
        ])
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_unwrapped_authority_replay_checkpoint_honors_latched_target_block()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let registrar_address = "0x00000000000000000000000000000000000000aa";
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                Some("0x9999999999999999999999999999999999999999999999999999999999999999"),
                42,
                1_700_000_042,
            ),
            raw_block(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                43,
                1_700_000_043,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
                block_number: 42,
                transaction_hash:
                    "0xtxaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    keccak256_hex(b"alice"),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000001",
                    )),
                ],
                data: encode_registrar_name_registered_log_data("alice", 1_700_010_000),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .to_owned(),
                block_number: 43,
                transaction_hash:
                    "0xtxbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    keccak256_hex(b"bob"),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000002",
                    )),
                ],
                data: encode_registrar_name_registered_log_data("bob", 1_700_010_000),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let checkpoint_context = crate::ens_v1_subregistry_discovery::ReplayAdapterCheckpointContext {
        deployment_profile: "test".to_owned(),
        cursor_kind: "raw_fact_normalized_events".to_owned(),
        range_start_block_number: 42,
        target_block_number: 42,
    };
    let summary = sync_ens_v1_unwrapped_authority_with_replay_checkpoint_and_log_limit(
        database.pool(),
        "ethereum-mainnet",
        &checkpoint_context,
        100_000,
    )
    .await?;

    assert_eq!(summary.scanned_log_count, 1);
    assert_eq!(summary.matched_log_count, 1);
    assert_eq!(summary.total_name_surface_count, 1);
    assert_eq!(summary.total_normalized_event_count, 5);
    assert!(
        load_name_surface(database.pool(), "ens:alice.eth")
            .await?
            .is_some()
    );
    assert!(
        load_name_surface(database.pool(), "ens:bob.eth")
            .await?
            .is_none()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE(MAX(block_number), 0) FROM normalized_events"
        )
        .fetch_one(database.pool())
        .await?,
        42
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_unwrapped_authority_materializes_replaced_registration_identity_rows()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let registrar_address = "0x00000000000000000000000000000000000000aa";
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                Some("0x9999999999999999999999999999999999999999999999999999999999999999"),
                42,
                1_700_000_042,
            ),
            raw_block(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                43,
                1_700_000_043,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
                block_number: 42,
                transaction_hash:
                    "0xtxaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    keccak256_hex(b"alice"),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000001",
                    )),
                ],
                data: encode_registrar_name_registered_log_data("alice", 1_700_010_000),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .to_owned(),
                block_number: 43,
                transaction_hash:
                    "0xtxbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    keccak256_hex(b"alice"),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000002",
                    )),
                ],
                data: encode_registrar_name_registered_log_data("alice", 1_700_020_000),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let summary = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.scanned_log_count, 2);
    assert_eq!(summary.matched_log_count, 2);
    assert_eq!(summary.total_name_surface_count, 1);
    assert_eq!(summary.total_resource_count, 2);
    assert_eq!(summary.total_surface_binding_count, 2);
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_REGISTRATION_GRANTED),
        Some(&2_usize)
    );
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_SURFACE_BOUND),
        Some(&2_usize)
    );
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_SURFACE_UNBOUND),
        Some(&1_usize)
    );

    let bindings =
        load_surface_bindings_by_logical_name_id(database.pool(), "ens:alice.eth").await?;
    assert_eq!(bindings.len(), 2);
    assert!(bindings[0].active_to.is_some());
    assert!(bindings[1].active_to.is_none());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM token_lineages")
            .fetch_one(database.pool())
            .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM resources")
            .fetch_one(database.pool())
            .await?,
        2
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_unwrapped_authority_renews_after_due_release_on_new_authority() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let registrar_address = "0x00000000000000000000000000000000000000aa";
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;

    let labelhash = keccak256_hex(b"alice");
    let registration_expiry = 1_700_010_000;
    let renewal_block_timestamp =
        release_after_grace(OffsetDateTime::from_unix_timestamp(registration_expiry)?)?
            .unix_timestamp()
            + 1;
    let renewal_expiry = renewal_block_timestamp + 86_400;
    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                Some("0x9999999999999999999999999999999999999999999999999999999999999999"),
                42,
                1_700_000_042,
            ),
            raw_block(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                43,
                renewal_block_timestamp,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
                block_number: 42,
                transaction_hash:
                    "0xtxaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    labelhash.clone(),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000001",
                    )),
                ],
                data: encode_registrar_name_registered_log_data("alice", registration_expiry),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .to_owned(),
                block_number: 43,
                transaction_hash:
                    "0xtxbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: registrar_address.to_owned(),
                topics: vec![unwrapped_name_renewed_topic0(), labelhash],
                data: encode_controller_label_event_log_data(
                    "alice",
                    &[1, renewal_expiry as u64, 3],
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let summary = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.scanned_log_count, 2);
    assert_eq!(summary.matched_log_count, 2);
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_REGISTRATION_RELEASED),
        Some(&1_usize)
    );
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_REGISTRATION_GRANTED),
        Some(&2_usize)
    );
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_REGISTRATION_RENEWED),
        Some(&1_usize)
    );

    let renewal_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'RegistrationRenewed'
           AND block_number = 43
           AND log_index = 1",
    )
    .fetch_one(database.pool())
    .await?;
    let reopened_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'RegistrationGranted'
           AND block_number = 43
           AND log_index = 1",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(renewal_resource_id, reopened_resource_id);

    let renewed_before_state = sqlx::query_scalar::<_, Value>(
        "SELECT before_state FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'RegistrationRenewed'
           AND block_number = 43
           AND log_index = 1",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        renewed_before_state["expiry"].as_i64(),
        Some(renewal_expiry)
    );

    let bindings =
        load_surface_bindings_by_logical_name_id(database.pool(), "ens:alice.eth").await?;
    assert_eq!(bindings.len(), 2);
    assert!(bindings[0].active_to.is_some());
    assert_eq!(bindings[1].resource_id, reopened_resource_id);

    database.cleanup().await
}

#[tokio::test]
async fn full_replay_defers_same_transaction_logs_before_last_registry_owner_until_registration()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;
    let registrar_address = "0x00000000000000000000000000000000000000aa";
    let registry_address = "0x00000000000000000000000000000000000000bb";
    let resolver_address = "0x00000000000000000000000000000000000000cc";
    let transient_owner = "0x00000000000000000000000000000000000000dd";
    let final_owner = "0x00000000000000000000000000000000000000ee";
    let block_hash = "0x8989898989898989898989898989898989898989898989898989898989898989";
    let transaction_hash = "0xtx89898989898989898989898989898989898989898989898989898989898989";

    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "registry",
        registry_address,
        Some("registry"),
        "manifests/ens/ens_v1_registry_l1/v3.toml",
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    upsert_raw_blocks(
        database.pool(),
        &[raw_block(
            block_hash,
            Some("0x8888888888888888888888888888888888888888888888888888888888888888"),
            89,
            1_700_000_089,
        )],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 89,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), alice.labelhashes[0].clone()],
                data: abi_word_address(transient_owner).to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 89,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(resolver_address),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 89,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 2,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), alice.labelhashes[0].clone()],
                data: abi_word_address(final_owner).to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 89,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 3,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    alice.labelhashes[0].clone(),
                    hex_string(&abi_word_address(final_owner)),
                ],
                data: encode_registrar_name_registered_log_data("alice", 1_800_000_000),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let checkpoint_context = crate::ens_v1_subregistry_discovery::ReplayAdapterCheckpointContext {
        deployment_profile: "test".to_owned(),
        cursor_kind: "raw_fact_normalized_events".to_owned(),
        range_start_block_number: 89,
        target_block_number: 89,
    };
    let summary = sync_ens_v1_unwrapped_authority_with_replay_checkpoint_and_log_limit(
        database.pool(),
        "ethereum-mainnet",
        &checkpoint_context,
        100_000,
    )
    .await?;
    assert_eq!(summary.scanned_log_count, 4);
    assert_eq!(summary.matched_log_count, 4);

    let (registration_resource_id, registration_before_state) = sqlx::query_as::<_, (Uuid, Value)>(
        "SELECT resource_id, before_state
             FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND event_kind = 'RegistrationGranted'",
    )
    .fetch_one(database.pool())
    .await?;
    let resolver_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id
         FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'ResolverChanged'",
    )
    .fetch_one(database.pool())
    .await?;
    assert!(
        registration_before_state["authority_kind"].is_null(),
        "same-transaction registry setup should be deferred until after the registration"
    );
    assert_eq!(resolver_resource_id, registration_resource_id);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND event_kind IN ('AuthorityTransferred', 'PermissionChanged')
               AND resource_id <> $1"
        )
        .bind(registration_resource_id)
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn block_hash_replay_preloads_open_registrar_before_renewal() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let registrar_address = "0x00000000000000000000000000000000000000aa";
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;

    let labelhash = keccak256_hex(b"alice");
    let grant_block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let renewal_block_hash = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let future_head_block_hash =
        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    let registration_expiry = 1_700_010_000;
    let renewal_expiry = 1_800_010_000;
    let release_boundary_timestamp =
        release_after_grace(OffsetDateTime::from_unix_timestamp(registration_expiry)?)?
            .unix_timestamp();
    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(
                grant_block_hash,
                Some("0x9999999999999999999999999999999999999999999999999999999999999999"),
                42,
                1_700_000_042,
            ),
            raw_block(
                renewal_block_hash,
                Some(grant_block_hash),
                43,
                1_700_000_043,
            ),
            raw_block(
                future_head_block_hash,
                Some(renewal_block_hash),
                44,
                release_boundary_timestamp + 1,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: grant_block_hash.to_owned(),
                block_number: 42,
                transaction_hash:
                    "0xtxaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    labelhash.clone(),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000001",
                    )),
                ],
                data: encode_registrar_name_registered_log_data("alice", registration_expiry),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: renewal_block_hash.to_owned(),
                block_number: 43,
                transaction_hash:
                    "0xtxbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: registrar_address.to_owned(),
                topics: vec![unwrapped_name_renewed_topic0(), labelhash],
                data: encode_controller_label_event_log_data(
                    "alice",
                    &[1, renewal_expiry as u64, 3],
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let first = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[grant_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(first.matched_log_count, 1);
    let grant_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'RegistrationGranted'
           AND block_number = 42",
    )
    .fetch_one(database.pool())
    .await?;
    let premature_release_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'RegistrationReleased'",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(premature_release_count, 0);

    let second = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[renewal_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(second.matched_log_count, 1);

    let (renewal_resource_id, renewal_before_state) = sqlx::query_as::<_, (Uuid, Value)>(
        "SELECT resource_id, before_state FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'RegistrationRenewed'
           AND block_number = 43
           AND log_index = 1",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(renewal_resource_id, grant_resource_id);
    assert_eq!(
        renewal_before_state["expiry"].as_i64(),
        Some(registration_expiry)
    );

    let bindings =
        load_surface_bindings_by_logical_name_id(database.pool(), "ens:alice.eth").await?;
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].resource_id, grant_resource_id);
    assert!(bindings[0].active_to.is_none());

    delete_normalized_events_in_block_range_for_test(
        database.pool(),
        "ens:alice.eth",
        None,
        Some(43),
    )
    .await?;

    let third = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[renewal_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(third.matched_log_count, 1);
    assert_eq!(third.total_normalized_event_inserted_count, 0);
    let replayed_renewal_before_state = sqlx::query_scalar::<_, Value>(
        "SELECT before_state FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'RegistrationRenewed'
           AND block_number = 43
           AND log_index = 1",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        replayed_renewal_before_state["expiry"].as_i64(),
        Some(registration_expiry)
    );

    database.cleanup().await
}

#[tokio::test]
async fn block_hash_replay_preload_ignores_prior_registration_epoch() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let registrar_address = "0x00000000000000000000000000000000000000aa";
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;

    let labelhash = keccak256_hex(b"alice");
    let old_grant_block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let current_grant_block_hash =
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let renewal_block_hash = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    let old_expiry = 1_700_010_000;
    let current_grant_timestamp =
        release_after_grace(OffsetDateTime::from_unix_timestamp(old_expiry)?)?.unix_timestamp() + 1;
    let current_expiry = current_grant_timestamp + 86_400;
    let renewal_expiry = current_expiry + 86_400;

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(
                old_grant_block_hash,
                Some("0x9999999999999999999999999999999999999999999999999999999999999999"),
                42,
                1_700_000_042,
            ),
            raw_block(
                current_grant_block_hash,
                Some(old_grant_block_hash),
                43,
                current_grant_timestamp,
            ),
            raw_block(
                renewal_block_hash,
                Some(current_grant_block_hash),
                44,
                current_grant_timestamp + 1,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: old_grant_block_hash.to_owned(),
                block_number: 42,
                transaction_hash:
                    "0xtxaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    labelhash.clone(),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000001",
                    )),
                ],
                data: encode_registrar_name_registered_log_data("alice", old_expiry),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: current_grant_block_hash.to_owned(),
                block_number: 43,
                transaction_hash:
                    "0xtxbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    labelhash.clone(),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000002",
                    )),
                ],
                data: encode_registrar_name_registered_log_data("alice", current_expiry),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: renewal_block_hash.to_owned(),
                block_number: 44,
                transaction_hash:
                    "0xtxcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
                transaction_index: 0,
                log_index: 2,
                emitting_address: registrar_address.to_owned(),
                topics: vec![unwrapped_name_renewed_topic0(), labelhash],
                data: encode_controller_label_event_log_data(
                    "alice",
                    &[1, renewal_expiry as u64, 3],
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let seeded = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[
            old_grant_block_hash.to_owned(),
            current_grant_block_hash.to_owned(),
        ],
    )
    .await?;
    assert_eq!(seeded.matched_log_count, 2);
    assert_eq!(
        seeded.by_kind.get(EVENT_KIND_REGISTRATION_GRANTED),
        Some(&2_usize)
    );

    let current_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id FROM surface_bindings
         WHERE logical_name_id = 'ens:alice.eth'
           AND block_number = 43",
    )
    .fetch_one(database.pool())
    .await?;

    delete_normalized_events_in_block_range_for_test(
        database.pool(),
        "ens:alice.eth",
        Some(43),
        Some(44),
    )
    .await?;

    let replayed = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[renewal_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(replayed.matched_log_count, 1);

    let (renewal_resource_id, renewal_before_state) = sqlx::query_as::<_, (Uuid, Value)>(
        "SELECT resource_id, before_state FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'RegistrationRenewed'
           AND block_number = 44
           AND log_index = 2",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(renewal_resource_id, current_resource_id);
    assert_eq!(
        renewal_before_state["expiry"].as_i64(),
        Some(current_expiry)
    );

    database.cleanup().await
}

#[tokio::test]
async fn block_hash_replay_does_not_preload_wrapper_expiry_as_registrar_expiry() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let registrar_address = "0x00000000000000000000000000000000000000aa";
    let wrapper_address = "0x00000000000000000000000000000000000000dd";
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
        "name_wrapper",
        wrapper_address,
        Some("name_wrapper"),
        "manifests/ens/ens_v1_wrapper_l1/v1.toml",
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let dns_name = dns_encoded_name(&["alice", "eth"]);
    let wrap_block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let grant_block_hash = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let wrap_tx_hash = "0xtxaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let grant_tx_hash = "0xtxbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let wrapper_expiry = 1_721_829_493;
    let registrar_expiry = 1_780_713_287;
    let registrant = "0x0000000000000000000000000000000000000001";
    let wrapped_owner = "0x0000000000000000000000000000000000000002";

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(
                wrap_block_hash,
                Some("0x9999999999999999999999999999999999999999999999999999999999999999"),
                42,
                1_700_000_042,
            ),
            raw_block(grant_block_hash, Some(wrap_block_hash), 43, 1_700_000_043),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: wrap_block_hash.to_owned(),
                block_number: 42,
                transaction_hash: wrap_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: wrapper_address.to_owned(),
                topics: vec![name_wrapped_topic0(), alice.namehash.clone()],
                data: encode_name_wrapped_log_data(
                    &dns_name,
                    wrapped_owner,
                    0,
                    wrapper_expiry as u64,
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: grant_block_hash.to_owned(),
                block_number: 43,
                transaction_hash: grant_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    alice.labelhashes[0].clone(),
                    hex_string(&abi_word_address(registrant)),
                ],
                data: encode_registrar_name_registered_log_data("alice", registrar_expiry),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: grant_block_hash.to_owned(),
                block_number: 43,
                transaction_hash: grant_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    transfer_topic0(),
                    hex_string(&abi_word_address(ZERO_ADDRESS)),
                    hex_string(&abi_word_address(registrant)),
                    alice.labelhashes[0].clone(),
                ],
                data: Vec::new(),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let seeded = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[wrap_block_hash.to_owned(), grant_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(seeded.matched_log_count, 3);

    let replayed = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[grant_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(replayed.matched_log_count, 2);
    assert_eq!(replayed.total_normalized_event_inserted_count, 0);

    let registrar_expiry_before_state = sqlx::query_scalar::<_, Value>(
        "SELECT before_state FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'ExpiryChanged'
           AND block_number = 43
           AND log_index = 0",
    )
    .fetch_one(database.pool())
    .await?;
    assert!(registrar_expiry_before_state["expiry"].is_null());

    database.cleanup().await
}

#[tokio::test]
async fn block_hash_replay_settles_expired_registrar_under_wrapper_before_new_grant() -> Result<()>
{
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let registrar_address = "0x00000000000000000000000000000000000000aa";
    let wrapper_address = "0x00000000000000000000000000000000000000dd";
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
        "name_wrapper",
        wrapper_address,
        Some("name_wrapper"),
        "manifests/ens/ens_v1_wrapper_l1/v1.toml",
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let dns_name = dns_encoded_name(&["alice", "eth"]);
    let old_grant_block_hash = "0x9191919191919191919191919191919191919191919191919191919191919191";
    let wrap_block_hash = "0x9292929292929292929292929292929292929292929292929292929292929292";
    let release_block_hash = "0x9494949494949494949494949494949494949494949494949494949494949494";
    let new_grant_block_hash = "0x9393939393939393939393939393939393939393939393939393939393939393";
    let old_expiry = 1_700_010_000;
    let grace_boundary = release_after_grace(OffsetDateTime::from_unix_timestamp(old_expiry)?)?;
    let release_boundary =
        OffsetDateTime::from_unix_timestamp(grace_boundary.unix_timestamp() + 1)?;
    let release_timestamp = release_boundary.unix_timestamp();
    let new_grant_timestamp = release_timestamp + 10;
    let wrapper_expiry = release_timestamp + 31_536_000;
    let new_expiry = release_timestamp + 63_072_000;
    let old_registrant = "0x0000000000000000000000000000000000000001";
    let wrapped_owner = "0x0000000000000000000000000000000000000002";
    let new_registrant = "0x0000000000000000000000000000000000000003";

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(
                old_grant_block_hash,
                Some("0x9090909090909090909090909090909090909090909090909090909090909090"),
                42,
                1_700_000_042,
            ),
            raw_block(
                wrap_block_hash,
                Some(old_grant_block_hash),
                43,
                1_700_000_043,
            ),
            raw_block(
                release_block_hash,
                Some(wrap_block_hash),
                44,
                release_timestamp,
            ),
            raw_block(
                new_grant_block_hash,
                Some(release_block_hash),
                45,
                new_grant_timestamp,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: old_grant_block_hash.to_owned(),
                block_number: 42,
                transaction_hash:
                    "0xtx91919191919191919191919191919191919191919191919191919191919191".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    alice.labelhashes[0].clone(),
                    hex_string(&abi_word_address(old_registrant)),
                ],
                data: encode_registrar_name_registered_log_data("alice", old_expiry),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: wrap_block_hash.to_owned(),
                block_number: 43,
                transaction_hash:
                    "0xtx92929292929292929292929292929292929292929292929292929292929292".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: wrapper_address.to_owned(),
                topics: vec![name_wrapped_topic0(), alice.namehash.clone()],
                data: encode_name_wrapped_log_data(
                    &dns_name,
                    wrapped_owner,
                    0,
                    wrapper_expiry as u64,
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: new_grant_block_hash.to_owned(),
                block_number: 45,
                transaction_hash:
                    "0xtx93939393939393939393939393939393939393939393939393939393939393".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    alice.labelhashes[0].clone(),
                    hex_string(&abi_word_address(new_registrant)),
                ],
                data: encode_registrar_name_registered_log_data("alice", new_expiry),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let seeded = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[old_grant_block_hash.to_owned(), wrap_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(seeded.matched_log_count, 2);

    let replayed = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[new_grant_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(replayed.matched_log_count, 1);

    let expiry_before_state = sqlx::query_scalar::<_, Value>(
        "SELECT before_state FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'ExpiryChanged'
           AND block_number = 45
           AND log_index = 0",
    )
    .fetch_one(database.pool())
    .await?;
    assert!(expiry_before_state["expiry"].is_null());

    let new_surface_bound_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'SurfaceBound'
           AND block_number = 45",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(new_surface_bound_count, 1);

    let new_registrar_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id
         FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'RegistrationGranted'
           AND block_number = 45",
    )
    .fetch_one(database.pool())
    .await?;
    let active_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id
         FROM surface_bindings
         WHERE logical_name_id = 'ens:alice.eth'
           AND active_to IS NULL",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(active_resource_id, new_registrar_resource_id);

    let wrapper_active_to = sqlx::query_scalar::<_, Option<OffsetDateTime>>(
        "SELECT binding.active_to
         FROM surface_bindings binding
         JOIN resources resource USING (resource_id)
         WHERE binding.logical_name_id = 'ens:alice.eth'
           AND resource.provenance->>'authority_kind' = 'wrapper'",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(wrapper_active_to, Some(release_boundary));

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_unwrapped_authority_finalizes_stale_wrapper_release_without_later_log()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let registrar_address = "0x00000000000000000000000000000000000000aa";
    let wrapper_address = "0x00000000000000000000000000000000000000dd";
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
        "name_wrapper",
        wrapper_address,
        Some("name_wrapper"),
        "manifests/ens/ens_v1_wrapper_l1/v1.toml",
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let dns_name = dns_encoded_name(&["alice", "eth"]);
    let grant_block_hash = "0xa191919191919191919191919191919191919191919191919191919191919191";
    let wrap_block_hash = "0xa292929292929292929292929292929292929292929292929292929292929292";
    let release_block_hash = "0xa393939393939393939393939393939393939393939393939393939393939393";
    let expiry = 1_700_010_000;
    let grace_boundary = release_after_grace(OffsetDateTime::from_unix_timestamp(expiry)?)?;
    let release_boundary =
        OffsetDateTime::from_unix_timestamp(grace_boundary.unix_timestamp() + 1)?;
    let release_timestamp = release_boundary.unix_timestamp();
    let registrant = "0x0000000000000000000000000000000000000001";
    let wrapped_owner = "0x0000000000000000000000000000000000000002";

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(
                grant_block_hash,
                Some("0xa090909090909090909090909090909090909090909090909090909090909090"),
                42,
                1_700_000_042,
            ),
            raw_block(wrap_block_hash, Some(grant_block_hash), 43, 1_700_000_043),
            raw_block(
                release_block_hash,
                Some(wrap_block_hash),
                44,
                release_timestamp,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: grant_block_hash.to_owned(),
                block_number: 42,
                transaction_hash:
                    "0xtxa1919191919191919191919191919191919191919191919191919191919191".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    alice.labelhashes[0].clone(),
                    hex_string(&abi_word_address(registrant)),
                ],
                data: encode_registrar_name_registered_log_data("alice", expiry),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: wrap_block_hash.to_owned(),
                block_number: 43,
                transaction_hash:
                    "0xtxa2929292929292929292929292929292929292929292929292929292929292".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: wrapper_address.to_owned(),
                topics: vec![name_wrapped_topic0(), alice.namehash.clone()],
                data: encode_name_wrapped_log_data(
                    &dns_name,
                    wrapped_owner,
                    0,
                    (release_timestamp + 31_536_000) as u64,
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let summary = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.matched_log_count, 2);

    let wrapper_active_to = sqlx::query_scalar::<_, Option<OffsetDateTime>>(
        "SELECT binding.active_to
         FROM surface_bindings binding
         JOIN resources resource USING (resource_id)
         WHERE binding.logical_name_id = 'ens:alice.eth'
           AND resource.provenance->>'authority_kind' = 'wrapper'",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(wrapper_active_to, Some(release_boundary));

    database.cleanup().await
}

#[tokio::test]
async fn block_hash_replay_clears_stale_wrapper_before_same_tx_grant_and_renewal_without_wrap()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let registrar_address = "0x00000000000000000000000000000000000000aa";
    let wrapper_address = "0x00000000000000000000000000000000000000dd";
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
        "name_wrapper",
        wrapper_address,
        Some("name_wrapper"),
        "manifests/ens/ens_v1_wrapper_l1/v1.toml",
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let dns_name = dns_encoded_name(&["alice", "eth"]);
    let wrap_block_hash = "0xb292929292929292929292929292929292929292929292929292929292929292";
    let grant_block_hash = "0xb393939393939393939393939393939393939393939393939393939393939393";
    let wrapped_owner = "0x0000000000000000000000000000000000000002";
    let new_registrant = "0x0000000000000000000000000000000000000003";
    let grant_expiry = 1_900_000_000;
    let renewal_expiry = grant_expiry + 31_536_000;
    let grant_tx_hash = "0xtxb3939393939393939393939393939393939393939393939393939393939393";

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(
                wrap_block_hash,
                Some("0xb191919191919191919191919191919191919191919191919191919191919191"),
                43,
                1_700_000_043,
            ),
            raw_block(grant_block_hash, Some(wrap_block_hash), 44, 1_700_000_044),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: wrap_block_hash.to_owned(),
                block_number: 43,
                transaction_hash:
                    "0xtxb2929292929292929292929292929292929292929292929292929292929292".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: wrapper_address.to_owned(),
                topics: vec![name_wrapped_topic0(), alice.namehash.clone()],
                data: encode_name_wrapped_log_data(&dns_name, wrapped_owner, 0, 1_800_000_000),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: grant_block_hash.to_owned(),
                block_number: 44,
                transaction_hash: grant_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    alice.labelhashes[0].clone(),
                    hex_string(&abi_word_address(new_registrant)),
                ],
                data: encode_registrar_name_registered_log_data("alice", grant_expiry),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: grant_block_hash.to_owned(),
                block_number: 44,
                transaction_hash: grant_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    unwrapped_name_renewed_topic0(),
                    alice.labelhashes[0].clone(),
                ],
                data: encode_controller_label_event_log_data(
                    "alice",
                    &[1, renewal_expiry as u64, 3],
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let seeded = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[wrap_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(seeded.matched_log_count, 1);

    let replayed = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[grant_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(replayed.matched_log_count, 2);

    let granted_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id
         FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'RegistrationGranted'
           AND block_number = 44",
    )
    .fetch_one(database.pool())
    .await?;
    let active_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id
         FROM surface_bindings
         WHERE logical_name_id = 'ens:alice.eth'
           AND active_to IS NULL",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(active_resource_id, granted_resource_id);

    database.cleanup().await
}

#[tokio::test]
async fn block_hash_replay_keeps_wrapper_on_grace_boundary_renewal() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let registrar_address = "0x00000000000000000000000000000000000000aa";
    let wrapper_address = "0x00000000000000000000000000000000000000dd";
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
        "name_wrapper",
        wrapper_address,
        Some("name_wrapper"),
        "manifests/ens/ens_v1_wrapper_l1/v1.toml",
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let dns_name = dns_encoded_name(&["alice", "eth"]);
    let grant_block_hash = "0xc191919191919191919191919191919191919191919191919191919191919191";
    let wrap_block_hash = "0xc292929292929292929292929292929292929292929292929292929292929292";
    let renewal_block_hash = "0xc393939393939393939393939393939393939393939393939393939393939393";
    let expiry = 1_700_010_000;
    let grace_boundary = release_after_grace(OffsetDateTime::from_unix_timestamp(expiry)?)?;
    let registrant = "0x0000000000000000000000000000000000000001";
    let wrapped_owner = "0x0000000000000000000000000000000000000002";
    let renewal_expiry = grace_boundary.unix_timestamp() + 31_536_000;

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(
                grant_block_hash,
                Some("0xc090909090909090909090909090909090909090909090909090909090909090"),
                42,
                1_700_000_042,
            ),
            raw_block(wrap_block_hash, Some(grant_block_hash), 43, 1_700_000_043),
            raw_block(
                renewal_block_hash,
                Some(wrap_block_hash),
                44,
                grace_boundary.unix_timestamp(),
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: grant_block_hash.to_owned(),
                block_number: 42,
                transaction_hash:
                    "0xtxc1919191919191919191919191919191919191919191919191919191919191".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    alice.labelhashes[0].clone(),
                    hex_string(&abi_word_address(registrant)),
                ],
                data: encode_registrar_name_registered_log_data("alice", expiry),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: wrap_block_hash.to_owned(),
                block_number: 43,
                transaction_hash:
                    "0xtxc2929292929292929292929292929292929292929292929292929292929292".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: wrapper_address.to_owned(),
                topics: vec![name_wrapped_topic0(), alice.namehash.clone()],
                data: encode_name_wrapped_log_data(
                    &dns_name,
                    wrapped_owner,
                    0,
                    renewal_expiry as u64,
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: renewal_block_hash.to_owned(),
                block_number: 44,
                transaction_hash:
                    "0xtxc3939393939393939393939393939393939393939393939393939393939393".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    unwrapped_name_renewed_topic0(),
                    alice.labelhashes[0].clone(),
                ],
                data: encode_controller_label_event_log_data(
                    "alice",
                    &[1, renewal_expiry as u64, 3],
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let seeded = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[grant_block_hash.to_owned(), wrap_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(seeded.matched_log_count, 2);

    let replayed = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[renewal_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(replayed.matched_log_count, 1);

    let release_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT
         FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'RegistrationReleased'
           AND block_number = 44",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(release_count, 0);

    let wrapper_active_to = sqlx::query_scalar::<_, Option<OffsetDateTime>>(
        "SELECT binding.active_to
         FROM surface_bindings binding
         JOIN resources resource USING (resource_id)
         WHERE binding.logical_name_id = 'ens:alice.eth'
           AND resource.provenance->>'authority_kind' = 'wrapper'",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(wrapper_active_to, None);

    database.cleanup().await
}

#[tokio::test]
async fn block_hash_replay_preloads_registrar_lease_for_wrapped_name_renewal() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let registrar_address = "0x00000000000000000000000000000000000000aa";
    let wrapper_address = "0x00000000000000000000000000000000000000dd";
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
        "name_wrapper",
        wrapper_address,
        Some("name_wrapper"),
        "manifests/ens/ens_v1_wrapper_l1/v1.toml",
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let dns_name = dns_encoded_name(&["alice", "eth"]);
    let wrap_block_hash = "0x8181818181818181818181818181818181818181818181818181818181818181";
    let grant_block_hash = "0x8282828282828282828282828282828282828282828282828282828282828282";
    let first_renewal_block_hash =
        "0x8383838383838383838383838383838383838383838383838383838383838383";
    let second_renewal_block_hash =
        "0x8484848484848484848484848484848484848484848484848484848484848484";
    let registrant = "0x0000000000000000000000000000000000000001";
    let wrapped_owner = "0x0000000000000000000000000000000000000002";
    let grant_expiry = 1_700_010_000;
    let first_renewal_expiry = 1_700_096_400;
    let second_renewal_expiry = 1_700_182_800;

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(
                wrap_block_hash,
                Some("0x8080808080808080808080808080808080808080808080808080808080808080"),
                42,
                1_700_000_042,
            ),
            raw_block(grant_block_hash, Some(wrap_block_hash), 43, 1_700_000_043),
            raw_block(
                first_renewal_block_hash,
                Some(grant_block_hash),
                44,
                1_700_000_044,
            ),
            raw_block(
                second_renewal_block_hash,
                Some(first_renewal_block_hash),
                45,
                1_700_000_045,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: wrap_block_hash.to_owned(),
                block_number: 42,
                transaction_hash:
                    "0xtx81818181818181818181818181818181818181818181818181818181818181".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: wrapper_address.to_owned(),
                topics: vec![name_wrapped_topic0(), alice.namehash.clone()],
                data: encode_name_wrapped_log_data(
                    &dns_name,
                    wrapped_owner,
                    0,
                    second_renewal_expiry as u64,
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: grant_block_hash.to_owned(),
                block_number: 43,
                transaction_hash:
                    "0xtx82828282828282828282828282828282828282828282828282828282828282".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    alice.labelhashes[0].clone(),
                    hex_string(&abi_word_address(registrant)),
                ],
                data: encode_registrar_name_registered_log_data("alice", grant_expiry),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: first_renewal_block_hash.to_owned(),
                block_number: 44,
                transaction_hash:
                    "0xtx83838383838383838383838383838383838383838383838383838383838383".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    unwrapped_name_renewed_topic0(),
                    alice.labelhashes[0].clone(),
                ],
                data: encode_controller_label_event_log_data(
                    "alice",
                    &[1, first_renewal_expiry as u64, 3],
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: second_renewal_block_hash.to_owned(),
                block_number: 45,
                transaction_hash:
                    "0xtx84848484848484848484848484848484848484848484848484848484848484".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    unwrapped_name_renewed_topic0(),
                    alice.labelhashes[0].clone(),
                ],
                data: encode_controller_label_event_log_data(
                    "alice",
                    &[1, second_renewal_expiry as u64, 3],
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let seeded = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[
            wrap_block_hash.to_owned(),
            grant_block_hash.to_owned(),
            first_renewal_block_hash.to_owned(),
        ],
    )
    .await?;
    assert_eq!(seeded.matched_log_count, 3);

    let registrar_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'RegistrationRenewed'
           AND block_number = 44",
    )
    .fetch_one(database.pool())
    .await?;

    let incremental = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[second_renewal_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(incremental.matched_log_count, 1);

    let unexpected_grant_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'RegistrationGranted'
           AND block_number = 45",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(unexpected_grant_count, 0);

    let (renewal_resource_id, renewal_before_state) = sqlx::query_as::<_, (Uuid, Value)>(
        "SELECT resource_id, before_state FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'RegistrationRenewed'
           AND block_number = 45",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(renewal_resource_id, registrar_resource_id);
    assert_eq!(
        renewal_before_state["expiry"].as_i64(),
        Some(first_renewal_expiry)
    );

    let replayed = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[
            first_renewal_block_hash.to_owned(),
            second_renewal_block_hash.to_owned(),
        ],
    )
    .await?;
    assert_eq!(replayed.matched_log_count, 2);
    assert_eq!(replayed.total_normalized_event_inserted_count, 0);

    database.cleanup().await
}

#[tokio::test]
async fn ens_registry_old_new_owner_after_current_migration_does_not_replace_registry_owner()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;
    let registrar_address = "0x00000000000000000000000000000000000000aa";
    let current_registry = "0x00000000000000000000000000000000000000bb";
    let old_registry = "0x00000000000000000000000000000000000000bc";

    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_ens_registry_current_and_old_fixture(database.pool(), current_registry, old_registry)
        .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let registration_expiry = 1_700_000_100;
    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(
                "0x5151515151515151515151515151515151515151515151515151515151515151",
                None,
                51,
                1_700_000_051,
            ),
            raw_block(
                "0x5252525252525252525252525252525252525252525252525252525252525252",
                None,
                52,
                1_700_000_052,
            ),
            raw_block(
                "0x5353535353535353535353535353535353535353535353535353535353535353",
                None,
                53,
                1_700_000_053,
            ),
            raw_block(
                "0x5454545454545454545454545454545454545454545454545454545454545454",
                None,
                54,
                1_700_000_054,
            ),
            raw_block(
                "0x5555555555555555555555555555555555555555555555555555555555555555",
                None,
                55,
                release_after_grace(OffsetDateTime::from_unix_timestamp(registration_expiry)?)?
                    .unix_timestamp(),
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x5151515151515151515151515151515151515151515151515151515151515151"
                    .to_owned(),
                block_number: 51,
                transaction_hash:
                    "0xtx51515151515151515151515151515151515151515151515151515151515151".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: old_registry.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), keccak256_hex(b"alice")],
                data: abi_word_address("0x0000000000000000000000000000000000000001").to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x5252525252525252525252525252525252525252525252525252525252525252"
                    .to_owned(),
                block_number: 52,
                transaction_hash:
                    "0xtx52525252525252525252525252525252525252525252525252525252525252".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: current_registry.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), keccak256_hex(b"alice")],
                data: abi_word_address("0x0000000000000000000000000000000000000002").to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x5353535353535353535353535353535353535353535353535353535353535353"
                    .to_owned(),
                block_number: 53,
                transaction_hash:
                    "0xtx53535353535353535353535353535353535353535353535353535353535353".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: old_registry.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), keccak256_hex(b"alice")],
                data: abi_word_address("0x0000000000000000000000000000000000000003").to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x5454545454545454545454545454545454545454545454545454545454545454"
                    .to_owned(),
                block_number: 54,
                transaction_hash:
                    "0xtx54545454545454545454545454545454545454545454545454545454545454".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    alice.labelhashes[0].clone(),
                    hex_string(&abi_word_address(
                        "0x00000000000000000000000000000000000000aa",
                    )),
                ],
                data: encode_registrar_name_registered_log_data("alice", registration_expiry),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let summary = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.scanned_log_count, 4);
    assert_eq!(summary.matched_log_count, 3);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT provenance->>'current_registry_owner'
             FROM resources
             WHERE provenance->>'authority_kind' = 'registry_only'
             LIMIT 1"
        )
        .fetch_one(database.pool())
        .await?,
        "0x0000000000000000000000000000000000000002".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM raw_logs")
            .fetch_one(database.pool())
            .await?,
        4
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_registry_old_non_root_resolver_transfer_and_ttl_after_migration_emit_no_topology()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;
    let registrar_address = "0x00000000000000000000000000000000000000aa";
    let current_registry = "0x00000000000000000000000000000000000000bb";
    let old_registry = "0x00000000000000000000000000000000000000bc";

    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_ens_registry_current_and_old_fixture(database.pool(), current_registry, old_registry)
        .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(
                "0x6161616161616161616161616161616161616161616161616161616161616161",
                None,
                61,
                1_700_000_061,
            ),
            raw_block(
                "0x6262626262626262626262626262626262626262626262626262626262626262",
                None,
                62,
                1_700_000_062,
            ),
            raw_block(
                "0x6363636363636363636363636363636363636363636363636363636363636363",
                None,
                63,
                1_700_000_063,
            ),
            raw_block(
                "0x6464646464646464646464646464646464646464646464646464646464646464",
                None,
                64,
                1_700_000_064,
            ),
            raw_block(
                "0x6565656565656565656565656565656565656565656565656565656565656565",
                None,
                65,
                1_700_000_065,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x6161616161616161616161616161616161616161616161616161616161616161"
                    .to_owned(),
                block_number: 61,
                transaction_hash:
                    "0xtx61616161616161616161616161616161616161616161616161616161616161".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    alice.labelhashes[0].clone(),
                    hex_string(&abi_word_address(
                        "0x00000000000000000000000000000000000000aa",
                    )),
                ],
                data: encode_registrar_name_registered_log_data("alice", 1_800_000_000),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x6262626262626262626262626262626262626262626262626262626262626262"
                    .to_owned(),
                block_number: 62,
                transaction_hash:
                    "0xtx62626262626262626262626262626262626262626262626262626262626262".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: current_registry.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), alice.labelhashes[0].clone()],
                data: abi_word_address("0x0000000000000000000000000000000000000002").to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x6363636363636363636363636363636363636363636363636363636363636363"
                    .to_owned(),
                block_number: 63,
                transaction_hash:
                    "0xtx63636363636363636363636363636363636363636363636363636363636363".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: old_registry.to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(
                    "0x00000000000000000000000000000000000000dd",
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x6464646464646464646464646464646464646464646464646464646464646464"
                    .to_owned(),
                block_number: 64,
                transaction_hash:
                    "0xtx64646464646464646464646464646464646464646464646464646464646464".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: old_registry.to_owned(),
                topics: vec![registry_transfer_topic0(), alice.namehash.clone()],
                data: abi_word_address("0x00000000000000000000000000000000000000ee").to_vec(),
                canonicality_state: CanonicalityState::Safe,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x6565656565656565656565656565656565656565656565656565656565656565"
                    .to_owned(),
                block_number: 65,
                transaction_hash:
                    "0xtx65656565656565656565656565656565656565656565656565656565656565".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: old_registry.to_owned(),
                topics: vec![new_ttl_topic0(), alice.namehash.clone()],
                data: abi_word_u64(3600).to_vec(),
                canonicality_state: CanonicalityState::Finalized,
            },
        ],
    )
    .await?;

    let summary = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.scanned_log_count, 5);
    assert_eq!(summary.matched_log_count, 2);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM raw_logs")
            .fetch_one(database.pool())
            .await?,
        5
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_registry_old_block_hash_replay_preloads_current_migration_markers() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;
    let registrar_address = "0x00000000000000000000000000000000000000aa";
    let current_registry = "0x00000000000000000000000000000000000000bb";
    let old_registry = "0x00000000000000000000000000000000000000bc";
    let migration_block_hash = "0x7272727272727272727272727272727272727272727272727272727272727272";
    let selected_block_hash = "0x7373737373737373737373737373737373737373737373737373737373737373";

    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_ens_registry_current_and_old_fixture(database.pool(), current_registry, old_registry)
        .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(migration_block_hash, None, 72, 1_700_000_072),
            raw_block(
                selected_block_hash,
                Some(migration_block_hash),
                73,
                1_700_000_073,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: migration_block_hash.to_owned(),
                block_number: 72,
                transaction_hash:
                    "0xtx72727272727272727272727272727272727272727272727272727272727272".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: current_registry.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), alice.labelhashes[0].clone()],
                data: abi_word_address("0x0000000000000000000000000000000000000002").to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: selected_block_hash.to_owned(),
                block_number: 73,
                transaction_hash:
                    "0xtx73737373737373737373737373737373737373737373737373737373737373".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    alice.labelhashes[0].clone(),
                    hex_string(&abi_word_address(
                        "0x00000000000000000000000000000000000000aa",
                    )),
                ],
                data: encode_registrar_name_registered_log_data("alice", 1_800_000_000),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: selected_block_hash.to_owned(),
                block_number: 73,
                transaction_hash:
                    "0xtx73737373737373737373737373737373737373737373737373737373737373".to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: old_registry.to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(
                    "0x00000000000000000000000000000000000000dd",
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let summary = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[selected_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(summary.scanned_log_count, 2);
    assert_eq!(summary.matched_log_count, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_events
             WHERE event_kind = 'ResolverChanged'
               AND raw_fact_ref->>'emitting_address' = $1"
        )
        .bind(old_registry)
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM raw_logs")
            .fetch_one(database.pool())
            .await?,
        3
    );

    database.cleanup().await
}

#[tokio::test]
async fn block_hash_replay_defers_preloaded_same_transaction_namehash_logs_until_registration()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;
    let registrar_address = "0x00000000000000000000000000000000000000aa";
    let registry_address = "0x00000000000000000000000000000000000000bb";
    let resolver_address = "0x00000000000000000000000000000000000000cc";
    let registry_owner = "0x00000000000000000000000000000000000000dd";
    let registrant = "0x00000000000000000000000000000000000000ee";
    let block_hash = "0x7474747474747474747474747474747474747474747474747474747474747474";
    let transaction_hash = "0xtx74747474747474747474747474747474747474747474747474747474747474";

    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "registry",
        registry_address,
        Some("registry"),
        "manifests/ens/ens_v1_registry_l1/v3.toml",
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    upsert_raw_blocks(
        database.pool(),
        &[raw_block(
            block_hash,
            Some("0x7373737373737373737373737373737373737373737373737373737373737373"),
            74,
            1_700_000_074,
        )],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 74,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), alice.labelhashes[0].clone()],
                data: abi_word_address(registry_owner).to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 74,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(resolver_address),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 74,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 2,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    alice.labelhashes[0].clone(),
                    hex_string(&abi_word_address(registrant)),
                ],
                data: encode_registrar_name_registered_log_data("alice", 1_800_000_000),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let first = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[block_hash.to_owned()],
    )
    .await?;
    assert_eq!(first.scanned_log_count, 3);
    assert_eq!(first.matched_log_count, 3);

    let (registration_resource_id, registration_before_state) = sqlx::query_as::<_, (Uuid, Value)>(
        "SELECT resource_id, before_state
         FROM normalized_events
         WHERE event_kind = 'RegistrationGranted'",
    )
    .fetch_one(database.pool())
    .await?;
    let resolver_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id FROM normalized_events WHERE event_kind = 'ResolverChanged'",
    )
    .fetch_one(database.pool())
    .await?;
    let registry_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id FROM resources WHERE provenance->>'authority_kind' = 'registry_only'",
    )
    .fetch_one(database.pool())
    .await?;
    assert!(registration_before_state["authority_kind"].is_null());
    assert_ne!(resolver_resource_id, registration_resource_id);
    assert_eq!(resolver_resource_id, registry_resource_id);

    let second = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[block_hash.to_owned()],
    )
    .await?;
    assert_eq!(second.scanned_log_count, 3);
    assert_eq!(second.matched_log_count, 3);
    assert_eq!(second.total_normalized_event_inserted_count, 0);

    database.cleanup().await
}

#[tokio::test]
async fn block_hash_replay_preserves_same_transaction_registry_resolver_before_reregistration()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;
    let registrar_address = "0x00000000000000000000000000000000000000aa";
    let registry_address = "0x00000000000000000000000000000000000000bb";
    let old_resolver = "0x00000000000000000000000000000000000000cc";
    let new_resolver = "0x00000000000000000000000000000000000000dd";
    let registry_owner = "0x00000000000000000000000000000000000000ee";
    let old_registrant = "0x0000000000000000000000000000000000000001";
    let new_registrant = "0x0000000000000000000000000000000000000002";
    let grant_block_hash = "0x7575757575757575757575757575757575757575757575757575757575757575";
    let resolver_block_hash = "0x7676767676767676767676767676767676767676767676767676767676767676";
    let selected_block_hash = "0x7777777777777777777777777777777777777777777777777777777777777777";
    let later_block_hash = "0x7878787878787878787878787878787878787878787878787878787878787878";
    let selected_tx_hash = "0xtx77777777777777777777777777777777777777777777777777777777777777";
    let later_tx_hash = "0xtx78787878787878787878787878787878787878787878787878787878787878";

    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "registry",
        registry_address,
        Some("registry"),
        "manifests/ens/ens_v1_registry_l1/v3.toml",
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let old_expiry = 1_700_000_100;
    let release_timestamp =
        release_after_grace(OffsetDateTime::from_unix_timestamp(old_expiry)?)?.unix_timestamp();
    let new_expiry = release_timestamp + 86_400;

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(
                grant_block_hash,
                Some("0x7474747474747474747474747474747474747474747474747474747474747474"),
                75,
                1_700_000_075,
            ),
            raw_block(
                resolver_block_hash,
                Some(grant_block_hash),
                76,
                1_700_000_076,
            ),
            raw_block(
                selected_block_hash,
                Some(resolver_block_hash),
                77,
                release_timestamp,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: grant_block_hash.to_owned(),
                block_number: 75,
                transaction_hash:
                    "0xtx75757575757575757575757575757575757575757575757575757575757575".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    alice.labelhashes[0].clone(),
                    hex_string(&abi_word_address(old_registrant)),
                ],
                data: encode_registrar_name_registered_log_data("alice", old_expiry),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: resolver_block_hash.to_owned(),
                block_number: 76,
                transaction_hash:
                    "0xtx76767676767676767676767676767676767676767676767676767676767676".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(old_resolver),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: selected_block_hash.to_owned(),
                block_number: 77,
                transaction_hash: selected_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), alice.labelhashes[0].clone()],
                data: abi_word_address(registry_owner).to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: selected_block_hash.to_owned(),
                block_number: 77,
                transaction_hash: selected_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(new_resolver),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: selected_block_hash.to_owned(),
                block_number: 77,
                transaction_hash: selected_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 2,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    alice.labelhashes[0].clone(),
                    hex_string(&abi_word_address(new_registrant)),
                ],
                data: encode_registrar_name_registered_log_data("alice", new_expiry),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let seeded = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[grant_block_hash.to_owned(), resolver_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(seeded.matched_log_count, 2);

    let first = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[selected_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(first.matched_log_count, 3);

    let registry_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id
         FROM resources
         WHERE provenance->>'authority_kind' = 'registry_only'",
    )
    .fetch_one(database.pool())
    .await?;
    let registration_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id
         FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'RegistrationGranted'
           AND block_number = 77",
    )
    .fetch_one(database.pool())
    .await?;
    let (resolver_resource_id, resolver_before_state) = sqlx::query_as::<_, (Uuid, Value)>(
        "SELECT resource_id, before_state
         FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'ResolverChanged'
           AND block_number = 77
           AND log_index = 1",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(resolver_resource_id, registry_resource_id);
    assert_ne!(resolver_resource_id, registration_resource_id);
    assert_eq!(
        resolver_before_state["resolver"].as_str(),
        Some(old_resolver)
    );

    let replayed = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[selected_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(replayed.matched_log_count, 3);
    assert_eq!(replayed.total_normalized_event_inserted_count, 0);

    let bob = observe_registrar_eth_name_with_version("bob", ENS_NORMALIZER_VERSION)?;
    upsert_raw_blocks(
        database.pool(),
        &[raw_block(
            later_block_hash,
            Some(selected_block_hash),
            78,
            release_timestamp + 12,
        )],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[RawLog {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: later_block_hash.to_owned(),
            block_number: 78,
            transaction_hash: later_tx_hash.to_owned(),
            transaction_index: 0,
            log_index: 0,
            emitting_address: registrar_address.to_owned(),
            topics: vec![
                name_registered_topic0(),
                bob.labelhashes[0].clone(),
                hex_string(&abi_word_address(new_registrant)),
            ],
            data: encode_registrar_name_registered_log_data("bob", new_expiry),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;

    let broad_replay = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[selected_block_hash.to_owned(), later_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(broad_replay.matched_log_count, 4);

    let resolver_resource_id_after_broad_replay = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id
         FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'ResolverChanged'
           AND block_number = 77
           AND log_index = 1",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        resolver_resource_id_after_broad_replay,
        registry_resource_id
    );

    database.cleanup().await
}

#[tokio::test]
async fn block_hash_replay_preloads_latent_registry_resolver_before_same_tx_registration()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;
    let registrar_address = "0x00000000000000000000000000000000000000aa";
    let registry_address = "0x00000000000000000000000000000000000000bb";
    let old_resolver = "0x00000000000000000000000000000000000000cc";
    let new_resolver = "0x00000000000000000000000000000000000000dd";
    let registry_owner = "0x00000000000000000000000000000000000000ee";
    let registrant = "0x0000000000000000000000000000000000000001";
    let resolver_block_hash = "0x7979797979797979797979797979797979797979797979797979797979797979";
    let selected_block_hash = "0x7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a";
    let resolver_tx_hash = "0xtx79797979797979797979797979797979797979797979797979797979797979";
    let selected_tx_hash = "0xtx7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a";

    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "registry",
        registry_address,
        Some("registry"),
        "manifests/ens/ens_v1_registry_l1/v3.toml",
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(
                resolver_block_hash,
                Some("0x7878787878787878787878787878787878787878787878787878787878787878"),
                79,
                1_700_000_079,
            ),
            raw_block(
                selected_block_hash,
                Some(resolver_block_hash),
                80,
                1_700_000_080,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: resolver_block_hash.to_owned(),
                block_number: 79,
                transaction_hash: resolver_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(old_resolver),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: selected_block_hash.to_owned(),
                block_number: 80,
                transaction_hash: selected_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), alice.labelhashes[0].clone()],
                data: abi_word_address(registry_owner).to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: selected_block_hash.to_owned(),
                block_number: 80,
                transaction_hash: selected_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(new_resolver),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: selected_block_hash.to_owned(),
                block_number: 80,
                transaction_hash: selected_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 2,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    alice.labelhashes[0].clone(),
                    hex_string(&abi_word_address(registrant)),
                ],
                data: encode_registrar_name_registered_log_data("alice", 1_800_000_000),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let first = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[selected_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(first.scanned_log_count, 3);
    assert_eq!(first.matched_log_count, 3);

    let registry_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id
         FROM resources
         WHERE provenance->>'authority_kind' = 'registry_only'",
    )
    .fetch_one(database.pool())
    .await?;
    let registration_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id
         FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'RegistrationGranted'
           AND block_number = 80",
    )
    .fetch_one(database.pool())
    .await?;
    let (resolver_resource_id, resolver_before_state) = sqlx::query_as::<_, (Uuid, Value)>(
        "SELECT resource_id, before_state
         FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'ResolverChanged'
           AND block_number = 80
           AND log_index = 1",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(resolver_resource_id, registry_resource_id);
    assert_ne!(resolver_resource_id, registration_resource_id);
    assert_eq!(
        resolver_before_state["resolver"].as_str(),
        Some(old_resolver)
    );

    let replayed = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[selected_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(replayed.scanned_log_count, 3);
    assert_eq!(replayed.matched_log_count, 3);
    assert_eq!(replayed.total_normalized_event_inserted_count, 0);

    database.cleanup().await
}

#[tokio::test]
async fn block_hash_replay_preloads_registry_owner_at_boundary_not_resource_head() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;
    let registrar_address = "0x00000000000000000000000000000000000000aa";
    let registry_address = "0x00000000000000000000000000000000000000bb";
    let registrant = "0x0000000000000000000000000000000000000011";
    let first_registry_owner = "0x0000000000000000000000000000000000000022";
    let selected_registry_owner = "0x0000000000000000000000000000000000000033";
    let later_registry_owner = "0x0000000000000000000000000000000000000044";
    let registration_block_hash =
        "0x7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b";
    let first_owner_block_hash =
        "0x7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c";
    let selected_block_hash = "0x7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d";
    let later_block_hash = "0x7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e";

    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "registry",
        registry_address,
        Some("registry"),
        "manifests/ens/ens_v1_registry_l1/v3.toml",
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(registration_block_hash, None, 81, 1_700_000_081),
            raw_block(
                first_owner_block_hash,
                Some(registration_block_hash),
                82,
                1_700_000_082,
            ),
            raw_block(
                selected_block_hash,
                Some(first_owner_block_hash),
                83,
                1_700_000_083,
            ),
            raw_block(
                later_block_hash,
                Some(selected_block_hash),
                84,
                1_700_000_084,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: registration_block_hash.to_owned(),
                block_number: 81,
                transaction_hash: "0xtx7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b"
                    .to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    alice.labelhashes[0].clone(),
                    hex_string(&abi_word_address(registrant)),
                ],
                data: encode_registrar_name_registered_log_data("alice", 1_800_000_000),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: first_owner_block_hash.to_owned(),
                block_number: 82,
                transaction_hash: "0xtx7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c"
                    .to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), alice.labelhashes[0].clone()],
                data: abi_word_address(first_registry_owner).to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: selected_block_hash.to_owned(),
                block_number: 83,
                transaction_hash: "0xtx7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d"
                    .to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), alice.labelhashes[0].clone()],
                data: abi_word_address(selected_registry_owner).to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: later_block_hash.to_owned(),
                block_number: 84,
                transaction_hash: "0xtx7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e"
                    .to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), alice.labelhashes[0].clone()],
                data: abi_word_address(later_registry_owner).to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let seeded = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(seeded.matched_log_count, 4);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT provenance->>'current_registry_owner'
             FROM resources
             WHERE provenance->>'authority_kind' = 'registry_only'"
        )
        .fetch_one(database.pool())
        .await?,
        later_registry_owner.to_owned()
    );

    let replayed = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[selected_block_hash.to_owned(), later_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(replayed.matched_log_count, 2);
    assert_eq!(replayed.total_normalized_event_inserted_count, 0);

    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT before_state->>'owner'
             FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND event_kind = 'AuthorityTransferred'
               AND block_number = 83"
        )
        .fetch_one(database.pool())
        .await?,
        first_registry_owner.to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT provenance->>'current_registry_owner'
             FROM resources
             WHERE provenance->>'authority_kind' = 'registry_only'"
        )
        .fetch_one(database.pool())
        .await?,
        later_registry_owner.to_owned()
    );

    database.cleanup().await
}

#[tokio::test]
async fn block_hash_replay_preloads_diverged_registrar_as_superseded_before_registry_convergence()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;
    let registrar_address = "0x00000000000000000000000000000000000000aa";
    let registry_address = "0x00000000000000000000000000000000000000bb";
    let registrant = "0x0000000000000000000000000000000000000001";
    let diverged_registry_owner = "0x0000000000000000000000000000000000000002";
    let resolver = "0x00000000000000000000000000000000000000cc";
    let registration_block_hash =
        "0xc100000000000000000000000000000000000000000000000000000000000001";
    let divergence_block_hash =
        "0xc100000000000000000000000000000000000000000000000000000000000002";
    let resolver_block_hash = "0xc100000000000000000000000000000000000000000000000000000000000003";
    let convergence_block_hash =
        "0xc100000000000000000000000000000000000000000000000000000000000004";

    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "registry",
        registry_address,
        Some("registry"),
        "manifests/ens/ens_v1_registry_l1/v3.toml",
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let labelhash = alice.labelhashes[0].clone();
    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(registration_block_hash, None, 91, 1_700_000_091),
            raw_block(
                divergence_block_hash,
                Some(registration_block_hash),
                92,
                1_700_000_092,
            ),
            raw_block(
                resolver_block_hash,
                Some(divergence_block_hash),
                93,
                1_700_000_093,
            ),
            raw_block(
                convergence_block_hash,
                Some(resolver_block_hash),
                94,
                1_700_000_094,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: registration_block_hash.to_owned(),
                block_number: 91,
                transaction_hash:
                    "0xtxc1000000000000000000000000000000000000000000000000000000000001".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    labelhash.clone(),
                    hex_string(&abi_word_address(registrant)),
                ],
                data: encode_registrar_name_registered_log_data("alice", 1_800_000_000),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: divergence_block_hash.to_owned(),
                block_number: 92,
                transaction_hash:
                    "0xtxc1000000000000000000000000000000000000000000000000000000000002".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), labelhash.clone()],
                data: abi_word_address(diverged_registry_owner).to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: resolver_block_hash.to_owned(),
                block_number: 93,
                transaction_hash:
                    "0xtxc1000000000000000000000000000000000000000000000000000000000003".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(resolver),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: convergence_block_hash.to_owned(),
                block_number: 94,
                transaction_hash:
                    "0xtxc1000000000000000000000000000000000000000000000000000000000004".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), labelhash],
                data: abi_word_address(registrant).to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let seeded = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[
            registration_block_hash.to_owned(),
            divergence_block_hash.to_owned(),
        ],
    )
    .await?;
    assert_eq!(seeded.matched_log_count, 2);

    let (registrar_resource_id, registrar_lineage_id) = sqlx::query_as::<_, (Uuid, Option<Uuid>)>(
        "SELECT resource_id, token_lineage_id
         FROM resources
         WHERE provenance->>'authority_kind' = 'registrar'",
    )
    .fetch_one(database.pool())
    .await?;
    let registry_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id
         FROM resources
         WHERE provenance->>'authority_kind' = 'registry_only'",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        sqlx::query_scalar::<_, Uuid>(
            "SELECT resource_id
             FROM surface_bindings
             WHERE logical_name_id = 'ens:alice.eth'
               AND active_to IS NULL",
        )
        .fetch_one(database.pool())
        .await?,
        registry_resource_id
    );

    let resolver_replayed = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[resolver_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(resolver_replayed.matched_log_count, 1);
    let resolver_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id
         FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'ResolverChanged'
           AND block_number = 93",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(resolver_resource_id, registry_resource_id);
    assert_ne!(resolver_resource_id, registrar_resource_id);

    let convergence_replayed = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[convergence_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(convergence_replayed.matched_log_count, 1);

    let active_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id
         FROM surface_bindings
         WHERE logical_name_id = 'ens:alice.eth'
           AND active_to IS NULL",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(active_resource_id, registrar_resource_id);
    assert_eq!(
        sqlx::query_scalar::<_, Option<Uuid>>(
            "SELECT token_lineage_id
             FROM resources
             WHERE resource_id = $1",
        )
        .bind(active_resource_id)
        .fetch_one(database.pool())
        .await?,
        registrar_lineage_id
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM surface_bindings
             WHERE logical_name_id = 'ens:alice.eth'
               AND resource_id = $1
               AND active_to IS NOT NULL",
        )
        .bind(registry_resource_id)
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND event_kind = 'PermissionChanged'
               AND block_number = 94
               AND resource_id = $1
               AND after_state->>'subject' = $2
               AND jsonb_array_length(after_state->'effective_powers') = 0",
        )
        .bind(registry_resource_id)
        .bind(diverged_registry_owner)
        .fetch_one(database.pool())
        .await?,
        2
    );
    let (before_authority_kind, after_authority_kind) = sqlx::query_as::<_, (String, String)>(
        "SELECT before_state->>'authority_kind', after_state->>'authority_kind'
         FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'AuthorityEpochChanged'
           AND block_number = 94",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(before_authority_kind, "registry_only");
    assert_eq!(after_authority_kind, "registrar");
    assert_eq!(
        sqlx::query_scalar::<_, Uuid>(
            "SELECT resource_id
             FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND event_kind = 'AuthorityTransferred'
               AND block_number = 94
               AND after_state->>'owner' = $1",
        )
        .bind(registrant)
        .fetch_one(database.pool())
        .await?,
        registrar_resource_id
    );

    database.cleanup().await
}

#[tokio::test]
async fn block_hash_replay_does_not_restore_released_superseded_registrar_on_convergence()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;
    let registrar_address = "0x00000000000000000000000000000000000000aa";
    let registry_address = "0x00000000000000000000000000000000000000bb";
    let registrant = "0x0000000000000000000000000000000000000001";
    let diverged_registry_owner = "0x0000000000000000000000000000000000000002";
    let registration_block_hash =
        "0xd100000000000000000000000000000000000000000000000000000000000001";
    let divergence_block_hash =
        "0xd100000000000000000000000000000000000000000000000000000000000002";
    let convergence_block_hash =
        "0xd100000000000000000000000000000000000000000000000000000000000003";

    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "registry",
        registry_address,
        Some("registry"),
        "manifests/ens/ens_v1_registry_l1/v3.toml",
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let labelhash = alice.labelhashes[0].clone();
    let registration_expiry = 1_700_000_100;
    let release_timestamp =
        release_after_grace(OffsetDateTime::from_unix_timestamp(registration_expiry)?)?
            .unix_timestamp();
    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(registration_block_hash, None, 101, 1_700_000_001),
            raw_block(
                divergence_block_hash,
                Some(registration_block_hash),
                102,
                1_700_000_002,
            ),
            raw_block(
                convergence_block_hash,
                Some(divergence_block_hash),
                103,
                release_timestamp + 10,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: registration_block_hash.to_owned(),
                block_number: 101,
                transaction_hash:
                    "0xtxd1000000000000000000000000000000000000000000000000000000000001".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    labelhash.clone(),
                    hex_string(&abi_word_address(registrant)),
                ],
                data: encode_registrar_name_registered_log_data("alice", registration_expiry),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: divergence_block_hash.to_owned(),
                block_number: 102,
                transaction_hash:
                    "0xtxd1000000000000000000000000000000000000000000000000000000000002".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), labelhash.clone()],
                data: abi_word_address(diverged_registry_owner).to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: convergence_block_hash.to_owned(),
                block_number: 103,
                transaction_hash:
                    "0xtxd1000000000000000000000000000000000000000000000000000000000003".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), labelhash],
                data: abi_word_address(registrant).to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let seeded = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[
            registration_block_hash.to_owned(),
            divergence_block_hash.to_owned(),
        ],
    )
    .await?;
    assert_eq!(seeded.matched_log_count, 2);

    let registry_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id
         FROM resources
         WHERE provenance->>'authority_kind' = 'registry_only'",
    )
    .fetch_one(database.pool())
    .await?;
    let replayed = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[convergence_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(replayed.matched_log_count, 1);

    let active_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id
         FROM surface_bindings
         WHERE logical_name_id = 'ens:alice.eth'
           AND active_to IS NULL",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(active_resource_id, registry_resource_id);
    assert_eq!(
        sqlx::query_scalar::<_, Uuid>(
            "SELECT resource_id
             FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND event_kind = 'AuthorityTransferred'
               AND block_number = 103",
        )
        .fetch_one(database.pool())
        .await?,
        registry_resource_id
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND event_kind = 'AuthorityEpochChanged'
               AND block_number = 103
               AND after_state->>'authority_kind' = 'registrar'",
        )
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn block_hash_replay_renews_preloaded_superseded_registrar_while_diverged() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;
    let registrar_address = "0x00000000000000000000000000000000000000aa";
    let registry_address = "0x00000000000000000000000000000000000000bb";
    let registrant = "0x0000000000000000000000000000000000000001";
    let diverged_registry_owner = "0x0000000000000000000000000000000000000002";
    let registration_block_hash =
        "0xd200000000000000000000000000000000000000000000000000000000000001";
    let divergence_block_hash =
        "0xd200000000000000000000000000000000000000000000000000000000000002";
    let renewal_block_hash = "0xd200000000000000000000000000000000000000000000000000000000000003";
    let registration_expiry = 1_800_000_000;
    let renewal_expiry = 1_900_000_000;

    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "registry",
        registry_address,
        Some("registry"),
        "manifests/ens/ens_v1_registry_l1/v3.toml",
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let labelhash = alice.labelhashes[0].clone();
    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(registration_block_hash, None, 111, 1_700_000_111),
            raw_block(
                divergence_block_hash,
                Some(registration_block_hash),
                112,
                1_700_000_112,
            ),
            raw_block(
                renewal_block_hash,
                Some(divergence_block_hash),
                113,
                1_700_000_113,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: registration_block_hash.to_owned(),
                block_number: 111,
                transaction_hash:
                    "0xtxd2000000000000000000000000000000000000000000000000000000000001".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    labelhash.clone(),
                    hex_string(&abi_word_address(registrant)),
                ],
                data: encode_registrar_name_registered_log_data("alice", registration_expiry),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: divergence_block_hash.to_owned(),
                block_number: 112,
                transaction_hash:
                    "0xtxd2000000000000000000000000000000000000000000000000000000000002".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), labelhash.clone()],
                data: abi_word_address(diverged_registry_owner).to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: renewal_block_hash.to_owned(),
                block_number: 113,
                transaction_hash:
                    "0xtxd2000000000000000000000000000000000000000000000000000000000003".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![unwrapped_name_renewed_topic0(), labelhash],
                data: encode_controller_label_event_log_data(
                    "alice",
                    &[1, renewal_expiry as u64, 3],
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let seeded = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[
            registration_block_hash.to_owned(),
            divergence_block_hash.to_owned(),
        ],
    )
    .await?;
    assert_eq!(seeded.matched_log_count, 2);

    let registrar_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id
         FROM resources
         WHERE provenance->>'authority_kind' = 'registrar'",
    )
    .fetch_one(database.pool())
    .await?;
    let registry_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id
         FROM resources
         WHERE provenance->>'authority_kind' = 'registry_only'",
    )
    .fetch_one(database.pool())
    .await?;
    let replayed = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[renewal_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(replayed.matched_log_count, 1);

    let (renewal_resource_id, renewal_before_state) = sqlx::query_as::<_, (Uuid, Value)>(
        "SELECT resource_id, before_state
         FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'RegistrationRenewed'
           AND block_number = 113",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(renewal_resource_id, registrar_resource_id);
    assert_eq!(
        renewal_before_state["expiry"].as_i64(),
        Some(registration_expiry)
    );
    assert_eq!(
        sqlx::query_scalar::<_, Uuid>(
            "SELECT resource_id
             FROM surface_bindings
             WHERE logical_name_id = 'ens:alice.eth'
               AND active_to IS NULL",
        )
        .fetch_one(database.pool())
        .await?,
        registry_resource_id
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND event_kind = 'AuthorityEpochChanged'
               AND block_number = 113
               AND after_state->>'authority_kind' = 'registrar'",
        )
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn block_hash_replay_wraps_after_preloaded_registry_divergence() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let registrar_address = "0x00000000000000000000000000000000000000aa";
    let registry_address = "0x00000000000000000000000000000000000000bb";
    let wrapper_address = "0x00000000000000000000000000000000000000dd";
    let registrant = "0x0000000000000000000000000000000000000001";
    let diverged_registry_owner = "0x0000000000000000000000000000000000000002";
    let wrapped_owner = "0x0000000000000000000000000000000000000003";
    let registration_block_hash =
        "0xd300000000000000000000000000000000000000000000000000000000000001";
    let divergence_block_hash =
        "0xd300000000000000000000000000000000000000000000000000000000000002";
    let wrap_block_hash = "0xd300000000000000000000000000000000000000000000000000000000000003";
    let wrap_tx_hash = "0xtxd3000000000000000000000000000000000000000000000000000000000003";

    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "registry",
        registry_address,
        Some("registry"),
        "manifests/ens/ens_v1_registry_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
        "name_wrapper",
        wrapper_address,
        Some("name_wrapper"),
        "manifests/ens/ens_v1_wrapper_l1/v1.toml",
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let labelhash = alice.labelhashes[0].clone();
    let dns_name = dns_encoded_name(&["alice", "eth"]);
    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(registration_block_hash, None, 121, 1_700_000_121),
            raw_block(
                divergence_block_hash,
                Some(registration_block_hash),
                122,
                1_700_000_122,
            ),
            raw_block(
                wrap_block_hash,
                Some(divergence_block_hash),
                123,
                1_700_000_123,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: registration_block_hash.to_owned(),
                block_number: 121,
                transaction_hash:
                    "0xtxd3000000000000000000000000000000000000000000000000000000000001".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    labelhash.clone(),
                    hex_string(&abi_word_address(registrant)),
                ],
                data: encode_registrar_name_registered_log_data("alice", 1_800_000_000),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: divergence_block_hash.to_owned(),
                block_number: 122,
                transaction_hash:
                    "0xtxd3000000000000000000000000000000000000000000000000000000000002".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), labelhash.clone()],
                data: abi_word_address(diverged_registry_owner).to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: wrap_block_hash.to_owned(),
                block_number: 123,
                transaction_hash: wrap_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    transfer_topic0(),
                    hex_string(&abi_word_address(registrant)),
                    hex_string(&abi_word_address(wrapper_address)),
                    labelhash.clone(),
                ],
                data: Vec::new(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: wrap_block_hash.to_owned(),
                block_number: 123,
                transaction_hash: wrap_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), labelhash],
                data: abi_word_address(wrapper_address).to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: wrap_block_hash.to_owned(),
                block_number: 123,
                transaction_hash: wrap_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 2,
                emitting_address: wrapper_address.to_owned(),
                topics: vec![name_wrapped_topic0(), alice.namehash.clone()],
                data: encode_name_wrapped_log_data(&dns_name, wrapped_owner, 0, 1_800_777_600),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let seeded = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[
            registration_block_hash.to_owned(),
            divergence_block_hash.to_owned(),
        ],
    )
    .await?;
    assert_eq!(seeded.matched_log_count, 2);

    let registrar_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id
         FROM resources
         WHERE provenance->>'authority_kind' = 'registrar'",
    )
    .fetch_one(database.pool())
    .await?;
    let replayed = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[wrap_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(replayed.matched_log_count, 3);

    let wrapper_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id
         FROM resources
         WHERE provenance->>'authority_kind' = 'wrapper'",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        sqlx::query_scalar::<_, Uuid>(
            "SELECT resource_id
             FROM surface_bindings
             WHERE logical_name_id = 'ens:alice.eth'
               AND active_to IS NULL",
        )
        .fetch_one(database.pool())
        .await?,
        wrapper_resource_id
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND event_kind = 'AuthorityEpochChanged'
               AND block_number = 123
               AND before_state->>'authority_kind' = 'registry_only'
               AND after_state->>'authority_kind' = 'registrar'",
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND event_kind = 'AuthorityEpochChanged'
               AND block_number = 123
               AND before_state->>'authority_kind' = 'registrar'
               AND after_state->>'authority_kind' = 'wrapper'",
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, Uuid>(
            "SELECT resource_id
             FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND event_kind = 'AuthorityTransferred'
               AND block_number = 123
               AND log_index = 1",
        )
        .fetch_one(database.pool())
        .await?,
        registrar_resource_id
    );

    database.cleanup().await
}

#[tokio::test]
async fn block_hash_replay_prefers_same_block_resolver_log_over_authority_boundary() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;
    let registrar_address = "0x00000000000000000000000000000000000000aa";
    let registry_address = "0x00000000000000000000000000000000000000bb";
    let old_resolver = "0x00000000000000000000000000000000000000cc";
    let new_resolver = "0x00000000000000000000000000000000000000dd";
    let registry_owner = "0x00000000000000000000000000000000000000ee";
    let old_registrant = "0x0000000000000000000000000000000000000001";
    let new_registrant = "0x0000000000000000000000000000000000000002";
    let registry_block_hash = "0x8181818181818181818181818181818181818181818181818181818181818181";
    let resolver_block_hash = "0x8282828282828282828282828282828282828282828282828282828282828282";
    let registration_block_hash =
        "0x8383838383838383838383838383838383838383838383838383838383838383";
    let transfer_block_hash = "0x8484848484848484848484848484848484848484848484848484848484848484";
    let registration_tx_hash = "0xtx83838383838383838383838383838383838383838383838383838383838383";
    let transfer_tx_hash = "0xtx84848484848484848484848484848484848484848484848484848484848484";

    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "registry",
        registry_address,
        Some("registry"),
        "manifests/ens/ens_v1_registry_l1/v3.toml",
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(
                registry_block_hash,
                Some("0x8080808080808080808080808080808080808080808080808080808080808080"),
                81,
                1_700_000_081,
            ),
            raw_block(
                resolver_block_hash,
                Some(registry_block_hash),
                82,
                1_700_000_082,
            ),
            raw_block(
                registration_block_hash,
                Some(resolver_block_hash),
                83,
                1_700_000_083,
            ),
            raw_block(
                transfer_block_hash,
                Some(registration_block_hash),
                84,
                1_700_000_084,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: registry_block_hash.to_owned(),
                block_number: 81,
                transaction_hash:
                    "0xtx81818181818181818181818181818181818181818181818181818181818181".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), alice.labelhashes[0].clone()],
                data: abi_word_address(registry_owner).to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: resolver_block_hash.to_owned(),
                block_number: 82,
                transaction_hash:
                    "0xtx82828282828282828282828282828282828282828282828282828282828282".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(old_resolver),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: registration_block_hash.to_owned(),
                block_number: 83,
                transaction_hash: registration_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    alice.labelhashes[0].clone(),
                    hex_string(&abi_word_address(old_registrant)),
                ],
                data: encode_registrar_name_registered_log_data("alice", 1_800_000_000),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: registration_block_hash.to_owned(),
                block_number: 83,
                transaction_hash: registration_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(new_resolver),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: transfer_block_hash.to_owned(),
                block_number: 84,
                transaction_hash: transfer_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    transfer_topic0(),
                    hex_string(&abi_word_address(old_registrant)),
                    hex_string(&abi_word_address(new_registrant)),
                    alice.labelhashes[0].clone(),
                ],
                data: Vec::new(),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let seeded = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(seeded.matched_log_count, 5);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND block_number = 84
               AND event_kind = 'PermissionChanged'
               AND after_state->'scope'->>'kind' = 'resolver'
               AND after_state->'scope'->>'resolver_address' = $1",
        )
        .bind(new_resolver)
        .fetch_one(database.pool())
        .await?,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_events event
             JOIN resources resource
               ON resource.resource_id = event.resource_id
              AND resource.provenance->>'authority_kind' = 'registry_only'
             WHERE event.logical_name_id = 'ens:alice.eth'
               AND event.block_number = 84
               AND event.event_kind = 'PermissionChanged'
               AND event.after_state->'scope'->>'kind' = 'resolver'
               AND event.after_state->'scope'->>'resolver_address' = $1
               AND event.after_state->>'subject' = $2",
        )
        .bind(new_resolver)
        .bind(registry_owner)
        .fetch_one(database.pool())
        .await?,
        1
    );

    delete_normalized_events_in_block_range_for_test(
        database.pool(),
        "ens:alice.eth",
        Some(84),
        Some(85),
    )
    .await?;

    let replayed = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[transfer_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(replayed.matched_log_count, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND block_number = 84
               AND event_kind = 'PermissionChanged'
               AND after_state->'scope'->>'kind' = 'resolver'
               AND after_state->'scope'->>'resolver_address' = $1",
        )
        .bind(new_resolver)
        .fetch_one(database.pool())
        .await?,
        2
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_unwrapped_authority_emits_resolver_changed_idempotently() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let registrar_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "ens",
            source_family: SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v1",
            rollout_status: "active",
            normalizer_version: "ensip15@ens-normalize-0.1.1",
            file_path: "manifests/ens/ens_v1_registrar_l1/v1.toml",
        },
    )
    .await?;
    let registrar_contract_instance_id = Uuid::new_v4();
    insert_contract_instance(
        database.pool(),
        registrar_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        ManifestContractInstanceSeed {
            manifest_id: registrar_manifest_id,
            declaration_kind: "contract",
            declaration_name: "registrar",
            contract_instance_id: registrar_contract_instance_id,
            declared_address: "0x00000000000000000000000000000000000000aa",
            role: Some("registrar"),
            proxy_kind: Some("none"),
        },
    )
    .await?;
    insert_contract_instance_address(
        database.pool(),
        registrar_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
        registrar_manifest_id,
    )
    .await?;

    let registry_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "ens",
            source_family: SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v1",
            rollout_status: "active",
            normalizer_version: "ensip15@ens-normalize-0.1.1",
            file_path: "manifests/ens/ens_v1_registry_l1/v1.toml",
        },
    )
    .await?;
    let registry_contract_instance_id = Uuid::new_v4();
    insert_contract_instance(
        database.pool(),
        registry_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        ManifestContractInstanceSeed {
            manifest_id: registry_manifest_id,
            declaration_kind: "contract",
            declaration_name: "registry",
            contract_instance_id: registry_contract_instance_id,
            declared_address: "0x00000000000000000000000000000000000000bb",
            role: Some("registry"),
            proxy_kind: Some("none"),
        },
    )
    .await?;
    insert_contract_instance_address(
        database.pool(),
        registry_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000bb",
        registry_manifest_id,
    )
    .await?;

    let block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let transaction_hash = "0xtxaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let block_timestamp = 1_700_000_042;
    upsert_raw_blocks(
        database.pool(),
        &[raw_block(
            block_hash,
            Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            42,
            block_timestamp,
        )],
    )
    .await?;
    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: "0x00000000000000000000000000000000000000bb".to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(
                    "0x00000000000000000000000000000000000000cc",
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    keccak256_hex(b"alice"),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000001",
                    )),
                ],
                data: encode_registrar_name_registered_log_data("alice", 1_700_010_000),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let first = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(first.scanned_log_count, 2);
    assert_eq!(first.matched_log_count, 2);
    assert_eq!(first.total_name_surface_count, 1);
    assert_eq!(first.total_resource_count, 1);
    assert_eq!(first.total_surface_binding_count, 1);
    assert_eq!(first.total_normalized_event_count, 7);
    assert_eq!(
        first.by_kind.get(EVENT_KIND_RESOLVER_CHANGED),
        Some(&1_usize)
    );
    assert_eq!(
        first.by_kind.get(EVENT_KIND_PERMISSION_CHANGED),
        Some(&2_usize)
    );

    let expected_identity = format!(
        "{}:{}:resolver:{}:{}:{}",
        DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
        EVENT_KIND_RESOLVER_CHANGED,
        block_hash,
        transaction_hash,
        0
    );
    let resolver_event_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id FROM normalized_events WHERE event_kind = 'ResolverChanged'",
    )
    .fetch_one(database.pool())
    .await?;
    let authority_resource_id =
        sqlx::query_scalar::<_, Uuid>("SELECT resource_id FROM resources LIMIT 1")
            .fetch_one(database.pool())
            .await?;
    assert_eq!(resolver_event_resource_id, authority_resource_id);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PermissionChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PermissionChanged' AND resource_id = $1"
            )
            .bind(authority_resource_id)
            .fetch_one(database.pool())
            .await?,
            2
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->'scope'->>'kind' FROM normalized_events WHERE event_kind = 'PermissionChanged' AND after_state->'scope'->>'kind' = 'resource' LIMIT 1"
            )
            .fetch_one(database.pool())
            .await?,
            "resource".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->'scope'->>'kind' FROM normalized_events WHERE event_kind = 'PermissionChanged' AND after_state->'scope'->>'kind' = 'resolver' LIMIT 1"
            )
            .fetch_one(database.pool())
            .await?,
            "resolver".to_owned()
        );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT event_identity FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        expected_identity
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT logical_name_id FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens:alice.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT source_family FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned()
    );
    assert_eq!(
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT before_state->>'resolver' FROM normalized_events WHERE event_kind = 'ResolverChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            None
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->>'resolver' FROM normalized_events WHERE event_kind = 'ResolverChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            "0x00000000000000000000000000000000000000cc".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->>'namehash' FROM normalized_events WHERE event_kind = 'ResolverChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            alice.namehash.clone()
        );

    let second = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(second.scanned_log_count, 2);
    assert_eq!(second.matched_log_count, 2);
    assert_eq!(second.total_normalized_event_count, 7);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        7
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_unwrapped_authority_carries_resolver_to_registry_release_binding() -> Result<()>
{
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;
    let registrar_address = "0x00000000000000000000000000000000000000aa";
    let registry_address = "0x00000000000000000000000000000000000000bb";
    let resolver_address = "0x00000000000000000000000000000000000000cc";

    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "registry",
        registry_address,
        Some("registry"),
        "manifests/ens/ens_v1_registry_l1/v3.toml",
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let registration_expiry = 1_700_000_100;
    let release_timestamp =
        release_after_grace(OffsetDateTime::from_unix_timestamp(registration_expiry)?)?
            .unix_timestamp()
            + 1;
    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(
                "0x6161616161616161616161616161616161616161616161616161616161616161",
                None,
                61,
                1_700_000_061,
            ),
            raw_block(
                "0x6262626262626262626262626262626262626262626262626262626262626262",
                None,
                62,
                1_700_000_062,
            ),
            raw_block(
                "0x6363636363636363636363636363636363636363636363636363636363636363",
                None,
                63,
                1_700_000_063,
            ),
            raw_block(
                "0x6464646464646464646464646464646464646464646464646464646464646464",
                None,
                64,
                release_timestamp,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x6161616161616161616161616161616161616161616161616161616161616161"
                    .to_owned(),
                block_number: 61,
                transaction_hash:
                    "0xtx61616161616161616161616161616161616161616161616161616161616161".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), alice.labelhashes[0].clone()],
                data: abi_word_address("0x0000000000000000000000000000000000000002").to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x6262626262626262626262626262626262626262626262626262626262626262"
                    .to_owned(),
                block_number: 62,
                transaction_hash:
                    "0xtx62626262626262626262626262626262626262626262626262626262626262".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(resolver_address),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x6363636363636363636363636363636363636363636363636363636363636363"
                    .to_owned(),
                block_number: 63,
                transaction_hash:
                    "0xtx63636363636363636363636363636363636363636363636363636363636363".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    alice.labelhashes[0].clone(),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000003",
                    )),
                ],
                data: encode_registrar_name_registered_log_data("alice", registration_expiry),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let summary = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.scanned_log_count, 3);
    assert_eq!(summary.matched_log_count, 3);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events
             WHERE event_kind = 'ResolverChanged'
               AND logical_name_id = 'ens:alice.eth'"
        )
        .fetch_one(database.pool())
        .await?,
        3
    );
    let registrar_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id FROM resources
         WHERE provenance->>'authority_kind' = 'registrar'
         LIMIT 1",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'resolver'
             FROM normalized_events
             WHERE event_kind = 'ResolverChanged'
               AND resource_id = $1"
        )
        .bind(registrar_resource_id)
        .fetch_one(database.pool())
        .await?,
        resolver_address.to_owned()
    );
    let registry_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id FROM resources
         WHERE provenance->>'authority_kind' = 'registry_only'
         LIMIT 1",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'resolver'
             FROM normalized_events
             WHERE event_kind = 'ResolverChanged'
               AND resource_id = $1"
        )
        .bind(registry_resource_id)
        .fetch_one(database.pool())
        .await?,
        resolver_address.to_owned()
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_unwrapped_authority_translates_wrapper_events_idempotently_and_skips_orphans()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
        "name_wrapper",
        "0x00000000000000000000000000000000000000dd",
        Some("name_wrapper"),
        "manifests/ens/ens_v1_wrapper_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "registry",
        "0x00000000000000000000000000000000000000bb",
        Some("registry"),
        "manifests/ens/ens_v1_registry_l1/v1.toml",
    )
    .await?;

    let orphan_block_hash = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    let block_hash = "0xabababababababababababababababababababababababababababababababab";
    let unwrap_block_hash = "0xacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacac";
    let transaction_hash = "0xtxababababababababababababababababababababababababababababababab";
    let mut orphan_block = raw_block(
        orphan_block_hash,
        Some("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
        41,
        1_700_000_041,
    );
    orphan_block.canonicality_state = CanonicalityState::Orphaned;
    upsert_raw_blocks(
        database.pool(),
        &[
            orphan_block,
            raw_block(
                block_hash,
                Some("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
                42,
                1_700_000_042,
            ),
            raw_block(unwrap_block_hash, Some(block_hash), 43, 1_700_000_043),
        ],
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let dns_name = dns_encoded_name(&["alice", "eth"]);
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: orphan_block_hash.to_owned(),
                block_number: 41,
                transaction_hash:
                    "0xtxffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                        .to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: "0x00000000000000000000000000000000000000dd".to_owned(),
                topics: vec![name_wrapped_topic0(), alice.namehash.clone()],
                data: encode_name_wrapped_log_data(
                    &dns_name,
                    "0x00000000000000000000000000000000000000ee",
                    0,
                    1_800_000_000,
                ),
                canonicality_state: CanonicalityState::Orphaned,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: "0x00000000000000000000000000000000000000dd".to_owned(),
                topics: vec![name_wrapped_topic0(), alice.namehash.clone()],
                data: encode_name_wrapped_log_data(
                    &dns_name,
                    "0x0000000000000000000000000000000000000001",
                    0,
                    1_800_000_000,
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: "0x00000000000000000000000000000000000000dd".to_owned(),
                topics: vec![fuses_set_topic0(), alice.namehash.clone()],
                data: encode_fuses_set_log_data(8),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 2,
                emitting_address: "0x00000000000000000000000000000000000000dd".to_owned(),
                topics: vec![expiry_extended_topic0(), alice.namehash.clone()],
                data: encode_expiry_extended_log_data(1_800_000_100),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 3,
                emitting_address: "0x00000000000000000000000000000000000000dd".to_owned(),
                topics: vec![
                    transfer_single_topic0(),
                    hex_string(&abi_word_address(
                        "0x00000000000000000000000000000000000000ff",
                    )),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000001",
                    )),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000002",
                    )),
                ],
                data: encode_transfer_single_log_data(&alice.namehash, 1),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 4,
                emitting_address: "0x00000000000000000000000000000000000000bb".to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(
                    "0x00000000000000000000000000000000000000cc",
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: unwrap_block_hash.to_owned(),
                block_number: 43,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 5,
                emitting_address: "0x00000000000000000000000000000000000000dd".to_owned(),
                topics: vec![name_unwrapped_topic0(), alice.namehash.clone()],
                data: encode_name_unwrapped_log_data("0x0000000000000000000000000000000000000003"),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let first = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(first.scanned_log_count, 6);
    assert_eq!(first.matched_log_count, 6);
    assert_eq!(first.total_name_surface_count, 1);
    assert_eq!(first.total_resource_count, 1);
    assert_eq!(first.total_surface_binding_count, 1);
    assert_eq!(first.total_normalized_event_count, 14);
    assert_eq!(first.by_kind.get(EVENT_KIND_EXPIRY_CHANGED), Some(&2_usize));
    assert_eq!(
        first.by_kind.get(EVENT_KIND_PERMISSION_SCOPE_CHANGED),
        Some(&2_usize)
    );
    assert_eq!(
        first.by_kind.get(EVENT_KIND_TOKEN_CONTROL_TRANSFERRED),
        Some(&2_usize)
    );
    assert_eq!(
        first.by_kind.get(EVENT_KIND_RESOLVER_CHANGED),
        Some(&1_usize)
    );

    let second = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(second.scanned_log_count, 6);
    assert_eq!(second.matched_log_count, 6);
    assert_eq!(second.total_normalized_event_count, 14);

    let wrapper_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id FROM resources WHERE provenance->>'authority_kind' = 'wrapper'",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT logical_name_id FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens:alice.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, Uuid>(
            "SELECT resource_id FROM normalized_events WHERE event_kind = 'ResolverChanged'",
        )
        .fetch_one(database.pool())
        .await?,
        wrapper_resource_id
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            "SELECT ARRAY_AGG(after_state->>'fuses' ORDER BY log_index) FROM normalized_events WHERE event_kind = 'PermissionScopeChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        vec!["0".to_owned(), "8".to_owned()]
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'to' FROM normalized_events WHERE event_kind = 'TokenControlTransferred' ORDER BY log_index DESC LIMIT 1"
        )
        .fetch_one(database.pool())
        .await?,
        "0x0000000000000000000000000000000000000002".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE raw_fact_ref->>'block_hash' = $1"
        )
        .bind(orphan_block_hash)
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        14
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_unwrapped_authority_fans_out_wrapper_transfer_batch_ids() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let wrapper_address = "0x00000000000000000000000000000000000000dd";
    let first_owner = "0x0000000000000000000000000000000000000001";
    let second_owner = "0x0000000000000000000000000000000000000002";
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
        "name_wrapper",
        wrapper_address,
        Some("name_wrapper"),
        "manifests/ens/ens_v1_wrapper_l1/v1.toml",
    )
    .await?;

    let block_hash = "0xbab1000000000000000000000000000000000000000000000000000000000001";
    let tx_hash = "0xtxbab10000000000000000000000000000000000000000000000000000000001";
    upsert_raw_blocks(
        database.pool(),
        &[raw_block(
            block_hash,
            Some("0xbab0000000000000000000000000000000000000000000000000000000000000"),
            42,
            1_700_000_042,
        )],
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let bob = observe_registrar_eth_name_with_version("bob", ENS_NORMALIZER_VERSION)?;
    let alice_dns_name = dns_encoded_name(&["alice", "eth"]);
    let bob_dns_name = dns_encoded_name(&["bob", "eth"]);
    let token_ids = vec![alice.namehash.clone(), bob.namehash.clone()];
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: wrapper_address.to_owned(),
                topics: vec![name_wrapped_topic0(), alice.namehash.clone()],
                data: encode_name_wrapped_log_data(&alice_dns_name, first_owner, 0, 1_800_000_000),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: wrapper_address.to_owned(),
                topics: vec![name_wrapped_topic0(), bob.namehash.clone()],
                data: encode_name_wrapped_log_data(&bob_dns_name, first_owner, 0, 1_800_000_000),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 2,
                emitting_address: wrapper_address.to_owned(),
                topics: vec![
                    transfer_batch_topic0_for_test(),
                    hex_string(&abi_word_address(
                        "0x00000000000000000000000000000000000000ff",
                    )),
                    hex_string(&abi_word_address(first_owner)),
                    hex_string(&abi_word_address(second_owner)),
                ],
                data: encode_transfer_batch_log_data(&token_ids, &[1, 1]),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let summary = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.scanned_log_count, 3);
    assert_eq!(summary.matched_log_count, 3);
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_TOKEN_CONTROL_TRANSFERRED),
        Some(&4_usize)
    );

    let batch_transfer_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT
         FROM normalized_events
         WHERE event_kind = 'TokenControlTransferred'
           AND block_number = 42
           AND log_index = 2
           AND after_state->>'to' = $1",
    )
    .bind(second_owner)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(batch_transfer_count, 2);

    let batch_permission_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT
         FROM normalized_events
         WHERE event_kind = 'PermissionChanged'
           AND block_number = 42
           AND log_index = 2",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(batch_permission_count, 4);

    let batch_permission_identity_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(DISTINCT event_identity)::BIGINT
         FROM normalized_events
         WHERE event_kind = 'PermissionChanged'
           AND block_number = 42
           AND log_index = 2",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(batch_permission_identity_count, 4);

    let batch_permission_resource_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(DISTINCT resource_id)::BIGINT
         FROM normalized_events
         WHERE event_kind = 'PermissionChanged'
           AND block_number = 42
           AND log_index = 2",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(batch_permission_resource_count, 2);

    let wrapper_owner_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT
         FROM resources
         WHERE provenance->>'authority_kind' = 'wrapper'
           AND provenance->>'owner' = $1",
    )
    .bind(second_owner)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(wrapper_owner_count, 2);

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_unwrapped_authority_wrap_eth2ld_reclaim_restores_then_wraps_same_tx()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let registrar_address = "0x00000000000000000000000000000000000000aa";
    let registry_address = "0x00000000000000000000000000000000000000bb";
    let wrapper_address = "0x00000000000000000000000000000000000000dd";
    let registrant = "0x0000000000000000000000000000000000000001";
    let diverged_registry_owner = "0x0000000000000000000000000000000000000002";
    let wrapped_owner = "0x0000000000000000000000000000000000000003";

    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "registry",
        registry_address,
        Some("registry"),
        "manifests/ens/ens_v1_registry_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
        "name_wrapper",
        wrapper_address,
        Some("name_wrapper"),
        "manifests/ens/ens_v1_wrapper_l1/v1.toml",
    )
    .await?;

    let registration_block_hash =
        "0xc200000000000000000000000000000000000000000000000000000000000001";
    let divergence_block_hash =
        "0xc200000000000000000000000000000000000000000000000000000000000002";
    let wrap_block_hash = "0xc200000000000000000000000000000000000000000000000000000000000003";
    let wrap_tx_hash = "0xtxc2000000000000000000000000000000000000000000000000000000000003";

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(registration_block_hash, None, 101, 1_700_000_101),
            raw_block(
                divergence_block_hash,
                Some(registration_block_hash),
                102,
                1_700_000_102,
            ),
            raw_block(
                wrap_block_hash,
                Some(divergence_block_hash),
                103,
                1_700_000_103,
            ),
        ],
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let labelhash = alice.labelhashes[0].clone();
    let dns_name = dns_encoded_name(&["alice", "eth"]);
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: registration_block_hash.to_owned(),
                block_number: 101,
                transaction_hash:
                    "0xtxc2000000000000000000000000000000000000000000000000000000000001".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    labelhash.clone(),
                    hex_string(&abi_word_address(registrant)),
                ],
                data: encode_registrar_name_registered_log_data("alice", 1_800_000_000),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: divergence_block_hash.to_owned(),
                block_number: 102,
                transaction_hash:
                    "0xtxc2000000000000000000000000000000000000000000000000000000000002".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), labelhash.clone()],
                data: abi_word_address(diverged_registry_owner).to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: wrap_block_hash.to_owned(),
                block_number: 103,
                transaction_hash: wrap_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    transfer_topic0(),
                    hex_string(&abi_word_address(registrant)),
                    hex_string(&abi_word_address(wrapper_address)),
                    labelhash.clone(),
                ],
                data: Vec::new(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: wrap_block_hash.to_owned(),
                block_number: 103,
                transaction_hash: wrap_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), labelhash],
                data: abi_word_address(wrapper_address).to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: wrap_block_hash.to_owned(),
                block_number: 103,
                transaction_hash: wrap_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 2,
                emitting_address: wrapper_address.to_owned(),
                topics: vec![name_wrapped_topic0(), alice.namehash.clone()],
                data: encode_name_wrapped_log_data(&dns_name, wrapped_owner, 0, 1_800_777_600),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let summary = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.scanned_log_count, 5);
    assert_eq!(summary.matched_log_count, 5);
    assert_eq!(summary.total_name_surface_count, 1);

    let (registrar_resource_id, registrar_lineage_id) = sqlx::query_as::<_, (Uuid, Option<Uuid>)>(
        "SELECT resource_id, token_lineage_id
         FROM resources
         WHERE provenance->>'authority_kind' = 'registrar'",
    )
    .fetch_one(database.pool())
    .await?;
    let registry_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id
         FROM resources
         WHERE provenance->>'authority_kind' = 'registry_only'",
    )
    .fetch_one(database.pool())
    .await?;
    let (wrapper_resource_id, wrapper_lineage_id) = sqlx::query_as::<_, (Uuid, Option<Uuid>)>(
        "SELECT resource_id, token_lineage_id
         FROM resources
         WHERE provenance->>'authority_kind' = 'wrapper'",
    )
    .fetch_one(database.pool())
    .await?;
    assert!(registrar_lineage_id.is_some());
    assert!(wrapper_lineage_id.is_some());
    assert_ne!(registrar_lineage_id, wrapper_lineage_id);

    assert_eq!(
        sqlx::query_scalar::<_, Uuid>(
            "SELECT resource_id
             FROM surface_bindings
             WHERE logical_name_id = 'ens:alice.eth'
               AND active_to IS NULL",
        )
        .fetch_one(database.pool())
        .await?,
        wrapper_resource_id
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM surface_bindings
             WHERE logical_name_id = 'ens:alice.eth'
               AND resource_id = $1",
        )
        .bind(registrar_resource_id)
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM surface_bindings
             WHERE logical_name_id = 'ens:alice.eth'
               AND resource_id = $1
               AND active_to IS NOT NULL",
        )
        .bind(registry_resource_id)
        .fetch_one(database.pool())
        .await?,
        1
    );

    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'to'
             FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND event_kind = 'TokenControlTransferred'
               AND block_number = 103
               AND log_index = 0
               AND resource_id = $1",
        )
        .bind(registrar_resource_id)
        .fetch_one(database.pool())
        .await?,
        wrapper_address.to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND event_kind = 'PermissionChanged'
               AND block_number = 103
               AND log_index = 0
               AND resource_id = $1
               AND after_state->>'subject' IN ($2, $3)",
        )
        .bind(registrar_resource_id)
        .bind(registrant)
        .bind(wrapper_address)
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND event_kind = 'PermissionChanged'
               AND block_number = 103
               AND log_index = 1
               AND resource_id = $1
               AND after_state->>'subject' = $2
               AND jsonb_array_length(after_state->'effective_powers') = 0",
        )
        .bind(registry_resource_id)
        .bind(diverged_registry_owner)
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, Uuid>(
            "SELECT resource_id
             FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND event_kind = 'AuthorityTransferred'
               AND block_number = 103
               AND log_index = 1
               AND after_state->>'owner' = $1",
        )
        .bind(wrapper_address)
        .fetch_one(database.pool())
        .await?,
        registrar_resource_id
    );
    let (restore_before_kind, restore_after_kind) = sqlx::query_as::<_, (String, String)>(
        "SELECT before_state->>'authority_kind', after_state->>'authority_kind'
         FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'AuthorityEpochChanged'
           AND block_number = 103
           AND before_state->>'authority_kind' = 'registry_only'
           AND after_state->>'authority_kind' = 'registrar'",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(restore_before_kind, "registry_only");
    assert_eq!(restore_after_kind, "registrar");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND event_kind = 'SurfaceBound'
               AND block_number = 103
               AND resource_id = $1
               AND after_state->>'authority_kind' = 'registrar'",
        )
        .bind(registrar_resource_id)
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND event_kind = 'SurfaceUnbound'
               AND block_number = 103
               AND resource_id = $1
               AND after_state->>'authority_kind' = 'registry_only'",
        )
        .bind(registry_resource_id)
        .fetch_one(database.pool())
        .await?,
        1
    );

    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'to'
             FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND event_kind = 'TokenControlTransferred'
               AND block_number = 103
               AND log_index = 2
               AND resource_id = $1",
        )
        .bind(wrapper_resource_id)
        .fetch_one(database.pool())
        .await?,
        wrapped_owner.to_owned()
    );
    let (wrap_before_kind, wrap_after_kind) = sqlx::query_as::<_, (String, String)>(
        "SELECT before_state->>'authority_kind', after_state->>'authority_kind'
         FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'AuthorityEpochChanged'
           AND block_number = 103
           AND before_state->>'authority_kind' = 'registrar'
           AND after_state->>'authority_kind' = 'wrapper'",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(wrap_before_kind, "registrar");
    assert_eq!(wrap_after_kind, "wrapper");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND event_kind = 'PermissionScopeChanged'
               AND block_number = 103
               AND log_index = 2
               AND resource_id = $1
               AND after_state->>'fuses' = '0'",
        )
        .bind(wrapper_resource_id)
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT
             FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND event_kind = 'SurfaceUnbound'
               AND block_number = 103
               AND resource_id = $1",
        )
        .bind(registrar_resource_id)
        .fetch_one(database.pool())
        .await?,
        1
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_unwrapped_authority_wrap_unwrap_reactivates_prior_registrar_resource_and_lineage()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let registrar_address = "0x00000000000000000000000000000000000000aa";
    let registry_address = "0x00000000000000000000000000000000000000bb";
    let wrapper_address = "0x00000000000000000000000000000000000000dd";
    let registrant = "0x0000000000000000000000000000000000000001";
    let diverged_registry_owner = "0x0000000000000000000000000000000000000002";
    let wrapper_owner = "0x0000000000000000000000000000000000000003";

    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        registrar_address,
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "registry",
        registry_address,
        Some("registry"),
        "manifests/ens/ens_v1_registry_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
        "name_wrapper",
        wrapper_address,
        Some("name_wrapper"),
        "manifests/ens/ens_v1_wrapper_l1/v1.toml",
    )
    .await?;

    let registration_block_hash =
        "0x8181818181818181818181818181818181818181818181818181818181818181";
    let wrap_block_hash = "0x8282828282828282828282828282828282828282828282828282828282828282";
    let unwrap_block_hash = "0x8383838383838383838383838383838383838383838383838383838383838383";
    let wrap_tx_hash = "0xtx82828282828282828282828282828282828282828282828282828282828282";
    let unwrap_tx_hash = "0xtx83838383838383838383838383838383838383838383838383838383838383";
    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(registration_block_hash, None, 81, 1_700_000_081),
            raw_block(
                wrap_block_hash,
                Some(registration_block_hash),
                82,
                1_700_000_082,
            ),
            raw_block(unwrap_block_hash, Some(wrap_block_hash), 83, 1_700_000_083),
        ],
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let labelhash = alice.labelhashes[0].clone();
    let dns_name = dns_encoded_name(&["alice", "eth"]);
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: registration_block_hash.to_owned(),
                block_number: 81,
                transaction_hash:
                    "0xtx81818181818181818181818181818181818181818181818181818181818181".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    labelhash.clone(),
                    hex_string(&abi_word_address(registrant)),
                ],
                data: encode_registrar_name_registered_log_data("alice", 1_800_000_000),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: registration_block_hash.to_owned(),
                block_number: 81,
                transaction_hash:
                    "0xtx81818181818181818181818181818181818181818181818181818181818181".to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), labelhash.clone()],
                data: abi_word_address(diverged_registry_owner).to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: wrap_block_hash.to_owned(),
                block_number: 82,
                transaction_hash: wrap_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    transfer_topic0(),
                    hex_string(&abi_word_address(registrant)),
                    hex_string(&abi_word_address(wrapper_address)),
                    labelhash.clone(),
                ],
                data: Vec::new(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: wrap_block_hash.to_owned(),
                block_number: 82,
                transaction_hash: wrap_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: registry_address.to_owned(),
                topics: vec![registry_transfer_topic0(), alice.namehash.clone()],
                data: abi_word_address(wrapper_address).to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: wrap_block_hash.to_owned(),
                block_number: 82,
                transaction_hash: wrap_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 2,
                emitting_address: wrapper_address.to_owned(),
                topics: vec![name_wrapped_topic0(), alice.namehash.clone()],
                data: encode_name_wrapped_log_data(&dns_name, wrapper_owner, 0, 1_800_777_600),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: unwrap_block_hash.to_owned(),
                block_number: 83,
                transaction_hash: unwrap_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![registry_transfer_topic0(), alice.namehash.clone()],
                data: abi_word_address(registrant).to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: unwrap_block_hash.to_owned(),
                block_number: 83,
                transaction_hash: unwrap_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: wrapper_address.to_owned(),
                topics: vec![name_unwrapped_topic0(), alice.namehash.clone()],
                data: encode_name_unwrapped_log_data(registrant),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: unwrap_block_hash.to_owned(),
                block_number: 83,
                transaction_hash: unwrap_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 2,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    transfer_topic0(),
                    hex_string(&abi_word_address(wrapper_address)),
                    hex_string(&abi_word_address(registrant)),
                    labelhash,
                ],
                data: Vec::new(),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let summary = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.scanned_log_count, 8);
    assert_eq!(summary.total_name_surface_count, 1);

    let (registrar_resource_id, registrar_lineage_id) = sqlx::query_as::<_, (Uuid, Option<Uuid>)>(
        "SELECT resource_id, token_lineage_id
             FROM resources
             WHERE provenance->>'authority_kind' = 'registrar'",
    )
    .fetch_one(database.pool())
    .await?;
    let (wrapper_resource_id, wrapper_lineage_id) = sqlx::query_as::<_, (Uuid, Option<Uuid>)>(
        "SELECT resource_id, token_lineage_id
         FROM resources
         WHERE provenance->>'authority_kind' = 'wrapper'",
    )
    .fetch_one(database.pool())
    .await?;
    let active_resource_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT resource_id
         FROM surface_bindings
         WHERE logical_name_id = 'ens:alice.eth'
           AND active_to IS NULL",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(active_resource_id, registrar_resource_id);
    assert_ne!(active_resource_id, wrapper_resource_id);
    assert_eq!(
        registrar_lineage_id,
        sqlx::query_scalar::<_, Option<Uuid>>(
            "SELECT token_lineage_id FROM resources WHERE resource_id = $1"
        )
        .bind(active_resource_id)
        .fetch_one(database.pool())
        .await?
    );
    assert!(registrar_lineage_id.is_some());
    assert!(wrapper_lineage_id.is_some());
    assert_ne!(registrar_lineage_id, wrapper_lineage_id);

    database.cleanup().await
}

#[tokio::test]
async fn block_hash_replay_preloads_wrapper_state_before_selected_transfer() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let wrapper_address = "0x00000000000000000000000000000000000000dd";
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
        "name_wrapper",
        wrapper_address,
        Some("name_wrapper"),
        "manifests/ens/ens_v1_wrapper_l1/v1.toml",
    )
    .await?;

    let wrap_block_hash = "0xadadadadadadadadadadadadadadadadadadadadadadadadadadadadadadadad";
    let transfer_block_hash = "0xaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeae";
    let wrap_tx_hash = "0xtxadadadadadadadadadadadadadadadadadadadadadadadadadadadadadadad";
    let transfer_tx_hash = "0xtxaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeaeae";
    let first_owner = "0x0000000000000000000000000000000000000001";
    let second_owner = "0x0000000000000000000000000000000000000002";
    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let dns_name = dns_encoded_name(&["alice", "eth"]);

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(
                wrap_block_hash,
                Some("0xacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacac"),
                42,
                1_700_000_042,
            ),
            raw_block(
                transfer_block_hash,
                Some(wrap_block_hash),
                43,
                1_700_000_043,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: wrap_block_hash.to_owned(),
                block_number: 42,
                transaction_hash: wrap_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: wrapper_address.to_owned(),
                topics: vec![name_wrapped_topic0(), alice.namehash.clone()],
                data: encode_name_wrapped_log_data(&dns_name, first_owner, 0, 1_800_000_000),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: transfer_block_hash.to_owned(),
                block_number: 43,
                transaction_hash: transfer_tx_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: wrapper_address.to_owned(),
                topics: vec![
                    transfer_single_topic0(),
                    hex_string(&abi_word_address(
                        "0x00000000000000000000000000000000000000ff",
                    )),
                    hex_string(&abi_word_address(first_owner)),
                    hex_string(&abi_word_address(second_owner)),
                ],
                data: encode_transfer_single_log_data(&alice.namehash, 1),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let seeded = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[wrap_block_hash.to_owned(), transfer_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(seeded.matched_log_count, 2);

    let replayed = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[transfer_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(replayed.matched_log_count, 1);
    assert_eq!(replayed.total_normalized_event_inserted_count, 0);

    let replayed_before_owner = sqlx::query_scalar::<_, String>(
        "SELECT before_state->>'from'
         FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'TokenControlTransferred'
           AND block_number = 43
           AND log_index = 0",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(replayed_before_owner, first_owner);

    database.cleanup().await
}

#[tokio::test]
async fn block_hash_replay_preloads_wrapper_state_at_boundary_not_resource_head() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let registry_address = "0x00000000000000000000000000000000000000bb";
    let wrapper_address = "0x00000000000000000000000000000000000000dd";
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "registry",
        registry_address,
        Some("registry"),
        "manifests/ens/ens_v1_registry_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
        "name_wrapper",
        wrapper_address,
        Some("name_wrapper"),
        "manifests/ens/ens_v1_wrapper_l1/v1.toml",
    )
    .await?;

    let wrap_block_hash = "0xba00000000000000000000000000000000000000000000000000000000000001";
    let first_transfer_block_hash =
        "0xba00000000000000000000000000000000000000000000000000000000000002";
    let resolver_block_hash = "0xba00000000000000000000000000000000000000000000000000000000000003";
    let later_transfer_block_hash =
        "0xba00000000000000000000000000000000000000000000000000000000000004";
    let first_owner = "0x0000000000000000000000000000000000000001";
    let second_owner = "0x0000000000000000000000000000000000000002";
    let later_owner = "0x0000000000000000000000000000000000000003";
    let resolver = "0x00000000000000000000000000000000000000ee";
    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let dns_name = dns_encoded_name(&["alice", "eth"]);

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(
                wrap_block_hash,
                Some("0xb900000000000000000000000000000000000000000000000000000000000000"),
                42,
                1_700_000_042,
            ),
            raw_block(
                first_transfer_block_hash,
                Some(wrap_block_hash),
                43,
                1_700_000_043,
            ),
            raw_block(
                resolver_block_hash,
                Some(first_transfer_block_hash),
                44,
                1_700_000_044,
            ),
            raw_block(
                later_transfer_block_hash,
                Some(resolver_block_hash),
                45,
                1_700_000_045,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: wrap_block_hash.to_owned(),
                block_number: 42,
                transaction_hash:
                    "0xtxba0000000000000000000000000000000000000000000000000000000001".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: wrapper_address.to_owned(),
                topics: vec![name_wrapped_topic0(), alice.namehash.clone()],
                data: encode_name_wrapped_log_data(&dns_name, first_owner, 0, 1_800_000_000),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: first_transfer_block_hash.to_owned(),
                block_number: 43,
                transaction_hash:
                    "0xtxba0000000000000000000000000000000000000000000000000000000002".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: wrapper_address.to_owned(),
                topics: vec![
                    transfer_single_topic0(),
                    hex_string(&abi_word_address(
                        "0x00000000000000000000000000000000000000ff",
                    )),
                    hex_string(&abi_word_address(first_owner)),
                    hex_string(&abi_word_address(second_owner)),
                ],
                data: encode_transfer_single_log_data(&alice.namehash, 1),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: resolver_block_hash.to_owned(),
                block_number: 44,
                transaction_hash:
                    "0xtxba0000000000000000000000000000000000000000000000000000000003".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: abi_word_address(resolver).to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: later_transfer_block_hash.to_owned(),
                block_number: 45,
                transaction_hash:
                    "0xtxba0000000000000000000000000000000000000000000000000000000004".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: wrapper_address.to_owned(),
                topics: vec![
                    transfer_single_topic0(),
                    hex_string(&abi_word_address(
                        "0x00000000000000000000000000000000000000ff",
                    )),
                    hex_string(&abi_word_address(second_owner)),
                    hex_string(&abi_word_address(later_owner)),
                ],
                data: encode_transfer_single_log_data(&alice.namehash, 1),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let seeded = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[
            wrap_block_hash.to_owned(),
            first_transfer_block_hash.to_owned(),
            resolver_block_hash.to_owned(),
            later_transfer_block_hash.to_owned(),
        ],
    )
    .await?;
    assert_eq!(seeded.matched_log_count, 4);

    let wrapper_resource_owner = sqlx::query_scalar::<_, String>(
        "SELECT provenance->>'owner'
         FROM resources
         WHERE provenance->>'authority_kind' = 'wrapper'",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(wrapper_resource_owner, later_owner);

    let replayed = EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(
        database.pool(),
        "ethereum-mainnet",
        &[resolver_block_hash.to_owned()],
    )
    .await?;
    assert_eq!(replayed.matched_log_count, 1);
    assert_eq!(replayed.total_normalized_event_inserted_count, 0);

    let resolver_subject = sqlx::query_scalar::<_, String>(
        "SELECT after_state->>'subject'
         FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth'
           AND event_kind = 'PermissionChanged'
           AND block_number = 44
           AND log_index = 0",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(resolver_subject, second_owner);

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_unwrapped_authority_emits_reverse_claim_source_observations() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let reverse_manifest_id = insert_active_contract_fixture(
        database.pool(),
        "ens_v1_reverse_l1",
        "reverse_registrar",
        "0x00000000000000000000000000000000000000ad",
        Some(CONTRACT_ROLE_REVERSE_REGISTRAR),
        "manifests/ens/ens_v1_reverse_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "registry",
        "0x00000000000000000000000000000000000000bb",
        Some("registry"),
        "manifests/ens/ens_v1_registry_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
        "resolver",
        "0x00000000000000000000000000000000000000cc",
        Some("public_resolver"),
        "manifests/ens/ens_v1_resolver_l1/v1.toml",
    )
    .await?;

    let block_hash = "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    let transaction_hash = "0xtxdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    let claimed_address = "0x0000000000000000000000000000000000001234";
    let reverse_node = reverse_node_for_address(claimed_address);
    let reverse_name = format!(
        "{}.addr.reverse",
        reverse_label_for_address(claimed_address)
    );

    upsert_raw_blocks(
        database.pool(),
        &[raw_block(
            block_hash,
            Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            42,
            1_700_000_042,
        )],
    )
    .await?;
    upsert_normalized_events(
        database.pool(),
        &[reverse_claim_event(
            reverse_manifest_id,
            block_hash,
            transaction_hash,
            0,
            claimed_address,
            &reverse_node,
            &reverse_name,
        )],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: "0x00000000000000000000000000000000000000bb".to_owned(),
                topics: vec![new_resolver_topic0(), reverse_node.clone()],
                data: encode_registry_new_resolver_log_data(
                    "0x00000000000000000000000000000000000000cc",
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 2,
                emitting_address: "0x00000000000000000000000000000000000000cc".to_owned(),
                topics: vec![name_changed_topic0(), reverse_node.clone()],
                data: encode_dynamic_string_log_data("alice.eth"),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 3,
                emitting_address: "0x00000000000000000000000000000000000000cc".to_owned(),
                topics: vec![version_changed_topic0(), reverse_node.clone()],
                data: encode_resolver_version_changed_log_data(7),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let first = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(first.scanned_log_count, 3);
    assert_eq!(first.matched_log_count, 3);
    assert_eq!(first.total_name_surface_count, 0);
    assert_eq!(first.total_resource_count, 0);
    assert_eq!(first.total_surface_binding_count, 0);
    assert_eq!(first.total_normalized_event_count, 3);
    assert_eq!(
        first.by_kind.get(EVENT_KIND_RESOLVER_CHANGED),
        Some(&1_usize)
    );
    assert_eq!(first.by_kind.get(EVENT_KIND_RECORD_CHANGED), Some(&1_usize));
    assert_eq!(
        first.by_kind.get(EVENT_KIND_RECORD_VERSION_CHANGED),
        Some(&1_usize)
    );

    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ResolverChanged' AND logical_name_id IS NULL AND resource_id IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            1
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->'primary_claim_source'->>'address' FROM normalized_events WHERE event_kind = 'ResolverChanged' AND logical_name_id IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            claimed_address.to_ascii_lowercase()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->>'raw_name' FROM normalized_events WHERE event_kind = 'RecordChanged' AND logical_name_id IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            "alice.eth".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->'primary_claim_source'->>'reverse_node' FROM normalized_events WHERE event_kind = 'RecordChanged' AND logical_name_id IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            reverse_node.to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->'primary_claim_source'->'claim_provenance'->>'contract_role' FROM normalized_events WHERE event_kind = 'RecordVersionChanged' AND logical_name_id IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            CONTRACT_ROLE_REVERSE_REGISTRAR.to_owned()
        );

    let second = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(second.scanned_log_count, 3);
    assert_eq!(second.matched_log_count, 3);
    assert_eq!(second.total_normalized_event_count, 3);
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE logical_name_id IS NULL AND event_kind IN ('ResolverChanged', 'RecordChanged', 'RecordVersionChanged')"
            )
            .fetch_one(database.pool())
            .await?,
            3
        );

    database.cleanup().await
}

#[tokio::test]
async fn reverse_name_record_preimage_releases_pending_forward_observations() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let reverse_manifest_id = insert_active_contract_fixture(
        database.pool(),
        "ens_v1_reverse_l1",
        "reverse_registrar",
        "0x00000000000000000000000000000000000000ad",
        Some(CONTRACT_ROLE_REVERSE_REGISTRAR),
        "manifests/ens/ens_v1_reverse_l1/v1.toml",
    )
    .await?;
    let registry_address = "0x00000000000000000000000000000000000000bb";
    let resolver_address = "0x00000000000000000000000000000000000000cc";
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "registry",
        registry_address,
        Some("registry"),
        "manifests/ens/ens_v1_registry_l1/v3.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
        "resolver",
        resolver_address,
        Some("public_resolver"),
        "manifests/ens/ens_v1_resolver_l1/v1.toml",
    )
    .await?;

    let block_hash = "0xefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";
    let transaction_hash = "0xtxefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";
    let owner = "0x0000000000000000000000000000000000001234";
    let reverse_node = reverse_node_for_address(owner);
    let reverse_name = format!("{}.addr.reverse", reverse_label_for_address(owner));
    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;

    upsert_raw_blocks(
        database.pool(),
        &[raw_block(
            block_hash,
            Some("0xdededededededededededededededededededededededededededededededede"),
            42,
            1_700_000_042,
        )],
    )
    .await?;
    upsert_normalized_events(
        database.pool(),
        &[reverse_claim_event(
            reverse_manifest_id,
            block_hash,
            transaction_hash,
            0,
            owner,
            &reverse_node,
            &reverse_name,
        )],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), alice.labelhashes[0].clone()],
                data: abi_word_address(owner).to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 2,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(resolver_address),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 3,
                emitting_address: resolver_address.to_owned(),
                topics: vec![addr_changed_topic0(), alice.namehash.clone()],
                data: encode_resolver_addr_changed_log_data(owner),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 4,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_resolver_topic0(), reverse_node.clone()],
                data: encode_registry_new_resolver_log_data(resolver_address),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 5,
                emitting_address: resolver_address.to_owned(),
                topics: vec![name_changed_topic0(), reverse_node.clone()],
                data: encode_dynamic_string_log_data("alice.eth"),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let summary = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.scanned_log_count, 5);
    assert_eq!(summary.matched_log_count, 5);
    assert_eq!(summary.total_name_surface_count, 1);
    assert_eq!(summary.total_resource_count, 1);
    assert_eq!(summary.total_surface_binding_count, 1);

    let surface = load_name_surface(database.pool(), "ens:alice.eth")
        .await?
        .context("reverse name preimage should admit the forward name surface")?;
    assert_eq!(surface.namehash, alice.namehash);

    let bindings =
        load_surface_bindings_by_logical_name_id(database.pool(), "ens:alice.eth").await?;
    assert_eq!(bindings.len(), 1);
    assert_eq!(
        bindings[0].binding_kind,
        SurfaceBindingKind::DeclaredRegistryPath
    );

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND event_kind = 'RecordChanged'
               AND resource_id IS NOT NULL
               AND after_state->>'record_key' = 'addr:60'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events
             WHERE logical_name_id IS NULL
               AND event_kind = 'RecordChanged'
               AND after_state->'primary_claim_source'->>'reverse_node' = $1"
        )
        .bind(&reverse_node)
        .fetch_one(database.pool())
        .await?,
        1
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_unwrapped_authority_generic_resolver_events_do_not_require_discovery_edge_range()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let reverse_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "ens",
            source_family: "ens_v1_reverse_l1",
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v1",
            rollout_status: "active",
            normalizer_version: "ensip15@ens-normalize-0.1.1",
            file_path: "manifests/ens/ens_v1_reverse_l1/v1.toml",
        },
    )
    .await?;
    let registry_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "ens",
            source_family: SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v1",
            rollout_status: "active",
            normalizer_version: "ensip15@ens-normalize-0.1.1",
            file_path: "manifests/ens/ens_v1_registry_l1/v1.toml",
        },
    )
    .await?;
    let resolver_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "ens",
            source_family: SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v1",
            rollout_status: "active",
            normalizer_version: "ensip15@ens-normalize-0.1.1",
            file_path: "manifests/ens/ens_v1_resolver_l1/v1.toml",
        },
    )
    .await?;
    let registry_contract_instance_id = Uuid::new_v4();
    let public_resolver_seed_contract_instance_id = Uuid::new_v4();
    let resolver_contract_instance_id = Uuid::new_v4();
    let registry_address = "0x00000000000000000000000000000000000000bb";
    let public_resolver_seed_address = "0x00000000000000000000000000000000000000bc";
    let resolver_address = "0x00000000000000000000000000000000000000cc";
    let unadmitted_resolver_address = "0x00000000000000000000000000000000000000dd";

    insert_contract_instance(
        database.pool(),
        registry_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        ManifestContractInstanceSeed {
            manifest_id: registry_manifest_id,
            declaration_kind: "contract",
            declaration_name: "registry",
            contract_instance_id: registry_contract_instance_id,
            declared_address: registry_address,
            role: Some("registry"),
            proxy_kind: Some("none"),
        },
    )
    .await?;
    insert_contract_instance_address(
        database.pool(),
        registry_contract_instance_id,
        "ethereum-mainnet",
        registry_address,
        registry_manifest_id,
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        public_resolver_seed_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        ManifestContractInstanceSeed {
            manifest_id: resolver_manifest_id,
            declaration_kind: "contract",
            declaration_name: "public_resolver_seed",
            contract_instance_id: public_resolver_seed_contract_instance_id,
            declared_address: public_resolver_seed_address,
            role: Some("public_resolver"),
            proxy_kind: Some("none"),
        },
    )
    .await?;
    insert_contract_instance_address(
        database.pool(),
        public_resolver_seed_contract_instance_id,
        "ethereum-mainnet",
        public_resolver_seed_address,
        resolver_manifest_id,
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        resolver_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance_address(
        database.pool(),
        resolver_contract_instance_id,
        "ethereum-mainnet",
        resolver_address,
        resolver_manifest_id,
    )
    .await?;
    insert_active_discovery_edge_with_range(
        database.pool(),
        ActiveDiscoveryEdgeSeed {
            chain_id: "ethereum-mainnet",
            edge_kind: "resolver",
            from_contract_instance_id: registry_contract_instance_id,
            to_contract_instance_id: resolver_contract_instance_id,
            source_manifest_id: registry_manifest_id,
            active_from_block_number: Some(42),
            active_to_block_number: Some(42),
        },
    )
    .await?;

    insert_active_discovery_edge_with_range(
        database.pool(),
        ActiveDiscoveryEdgeSeed {
            chain_id: "ethereum-mainnet",
            edge_kind: "resolver",
            from_contract_instance_id: registry_contract_instance_id,
            to_contract_instance_id: resolver_contract_instance_id,
            source_manifest_id: registry_manifest_id,
            active_from_block_number: Some(44),
            active_to_block_number: Some(44),
        },
    )
    .await?;
    upsert_raw_code_hashes(
        database.pool(),
        &[
            raw_code_hash_for_address(
                public_resolver_seed_address,
                "0x1111111111111111111111111111111111111111111111111111111111111111",
            ),
            raw_code_hash_for_address(
                resolver_address,
                "0x1111111111111111111111111111111111111111111111111111111111111111",
            ),
        ],
    )
    .await?;

    let block_hash = "0xedededededededededededededededededededededededededededededededed";
    let closed_block_hash = "0xefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";
    let reopened_block_hash = "0xf4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4";
    let transaction_hash = "0xtxededededededededededededededededededededededededededededededed";
    let claimed_address = "0x0000000000000000000000000000000000001234";
    let reverse_node = reverse_node_for_address(claimed_address);
    let reverse_name = format!(
        "{}.addr.reverse",
        reverse_label_for_address(claimed_address)
    );

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(
                block_hash,
                Some("0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"),
                42,
                1_700_000_042,
            ),
            raw_block(closed_block_hash, Some(block_hash), 43, 1_700_000_043),
            raw_block(
                reopened_block_hash,
                Some(closed_block_hash),
                44,
                1_700_000_044,
            ),
        ],
    )
    .await?;
    upsert_normalized_events(
        database.pool(),
        &[reverse_claim_event(
            reverse_manifest_id,
            block_hash,
            transaction_hash,
            0,
            claimed_address,
            &reverse_node,
            &reverse_name,
        )],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_resolver_topic0(), reverse_node.clone()],
                data: encode_registry_new_resolver_log_data(resolver_address),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: resolver_address.to_owned(),
                topics: vec![name_changed_topic0(), reverse_node.clone()],
                data: encode_dynamic_string_log_data("alice.eth"),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 2,
                emitting_address: resolver_address.to_owned(),
                topics: vec![version_changed_topic0(), reverse_node.clone()],
                data: encode_resolver_version_changed_log_data(7),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 3,
                emitting_address: unadmitted_resolver_address.to_owned(),
                topics: vec![name_changed_topic0(), reverse_node.clone()],
                data: encode_dynamic_string_log_data("unadmitted.eth"),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: closed_block_hash.to_owned(),
                block_number: 43,
                transaction_hash:
                    "0xtxefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: resolver_address.to_owned(),
                topics: vec![name_changed_topic0(), reverse_node.clone()],
                data: encode_dynamic_string_log_data("closed.eth"),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: reopened_block_hash.to_owned(),
                block_number: 44,
                transaction_hash: "0xtxf4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4"
                    .to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: resolver_address.to_owned(),
                topics: vec![name_changed_topic0(), reverse_node.clone()],
                data: encode_dynamic_string_log_data("reopened.eth"),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let summary = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.scanned_log_count, 6);
    assert_eq!(summary.matched_log_count, 5);
    assert_eq!(summary.total_normalized_event_count, 5);
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_RESOLVER_CHANGED),
        Some(&1_usize)
    );
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_RECORD_CHANGED),
        Some(&3_usize)
    );
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_RECORD_VERSION_CHANGED),
        Some(&1_usize)
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            "SELECT ARRAY_AGG(after_state->>'raw_name' ORDER BY block_number, log_index) FROM normalized_events WHERE event_kind = 'RecordChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        vec![
            "alice.eth".to_owned(),
            "closed.eth".to_owned(),
            "reopened.eth".to_owned()
        ]
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE raw_fact_ref->>'emitting_address' = $1"
        )
        .bind(unadmitted_resolver_address)
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE raw_fact_ref->>'block_hash' = $1"
        )
        .bind(closed_block_hash)
        .fetch_one(database.pool())
        .await?,
        1
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_unwrapped_authority_gates_discovered_ensv1_resolver_event_facts_by_profile()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let registrar_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "ens",
            source_family: SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v1",
            rollout_status: "active",
            normalizer_version: "ensip15@ens-normalize-0.1.1",
            file_path: "manifests/ens/ens_v1_registrar_l1/v1.toml",
        },
    )
    .await?;
    let registry_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "ens",
            source_family: SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v1",
            rollout_status: "active",
            normalizer_version: "ensip15@ens-normalize-0.1.1",
            file_path: "manifests/ens/ens_v1_registry_l1/v1.toml",
        },
    )
    .await?;
    let resolver_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "ens",
            source_family: SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v1",
            rollout_status: "active",
            normalizer_version: "ensip15@ens-normalize-0.1.1",
            file_path: "manifests/ens/ens_v1_resolver_l1/v1.toml",
        },
    )
    .await?;

    let registrar_contract_instance_id = Uuid::new_v4();
    let registry_contract_instance_id = Uuid::new_v4();
    let public_resolver_seed_contract_instance_id = Uuid::new_v4();
    let supported_resolver_contract_instance_id = Uuid::new_v4();
    let pending_resolver_contract_instance_id = Uuid::new_v4();
    let unsupported_resolver_contract_instance_id = Uuid::new_v4();
    let registrar_address = "0x00000000000000000000000000000000000000aa";
    let registry_address = "0x00000000000000000000000000000000000000bb";
    let public_resolver_seed_address = "0x00000000000000000000000000000000000000bc";
    let supported_resolver_address = "0x00000000000000000000000000000000000000c1";
    let pending_resolver_address = "0x00000000000000000000000000000000000000c2";
    let unsupported_resolver_address = "0x00000000000000000000000000000000000000c3";
    let unlisted_resolver_address = "0x00000000000000000000000000000000000000c4";
    let public_resolver_code_hash =
        "0x1111111111111111111111111111111111111111111111111111111111111111";

    for (contract_instance_id, address, manifest_id, role) in [
        (
            registrar_contract_instance_id,
            registrar_address,
            registrar_manifest_id,
            "registrar",
        ),
        (
            registry_contract_instance_id,
            registry_address,
            registry_manifest_id,
            "registry",
        ),
        (
            public_resolver_seed_contract_instance_id,
            public_resolver_seed_address,
            resolver_manifest_id,
            "public_resolver",
        ),
    ] {
        insert_contract_instance(
            database.pool(),
            contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;
        insert_manifest_contract_instance(
            database.pool(),
            ManifestContractInstanceSeed {
                manifest_id,
                declaration_kind: "contract",
                declaration_name: role,
                contract_instance_id,
                declared_address: address,
                role: Some(role),
                proxy_kind: Some("none"),
            },
        )
        .await?;
        insert_contract_instance_address(
            database.pool(),
            contract_instance_id,
            "ethereum-mainnet",
            address,
            manifest_id,
        )
        .await?;
    }

    for (contract_instance_id, address) in [
        (
            supported_resolver_contract_instance_id,
            supported_resolver_address,
        ),
        (
            pending_resolver_contract_instance_id,
            pending_resolver_address,
        ),
        (
            unsupported_resolver_contract_instance_id,
            unsupported_resolver_address,
        ),
    ] {
        insert_contract_instance(
            database.pool(),
            contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;
        insert_contract_instance_address(
            database.pool(),
            contract_instance_id,
            "ethereum-mainnet",
            address,
            resolver_manifest_id,
        )
        .await?;
        insert_active_discovery_edge_with_range(
            database.pool(),
            ActiveDiscoveryEdgeSeed {
                chain_id: "ethereum-mainnet",
                edge_kind: "resolver",
                from_contract_instance_id: registry_contract_instance_id,
                to_contract_instance_id: contract_instance_id,
                source_manifest_id: registry_manifest_id,
                active_from_block_number: None,
                active_to_block_number: None,
            },
        )
        .await?;
    }

    upsert_raw_code_hashes(
        database.pool(),
        &[
            raw_code_hash_for_address(public_resolver_seed_address, public_resolver_code_hash),
            raw_code_hash_for_address(supported_resolver_address, public_resolver_code_hash),
            raw_code_hash_for_address(
                unsupported_resolver_address,
                "0x2222222222222222222222222222222222222222222222222222222222222222",
            ),
        ],
    )
    .await?;

    let block_hash = "0xabababababababababababababababababababababababababababababababab";
    let transaction_hash = "0xtxababababababababababababababababababababababababababababababab";
    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    upsert_raw_blocks(
        database.pool(),
        &[raw_block(
            block_hash,
            Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            42,
            1_700_000_042,
        )],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    keccak256_hex(b"alice"),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000001",
                    )),
                ],
                data: encode_registrar_name_registered_log_data("alice", 1_700_010_000),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(supported_resolver_address),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 2,
                emitting_address: supported_resolver_address.to_owned(),
                topics: vec![name_changed_topic0(), alice.namehash.clone()],
                data: encode_dynamic_string_log_data("supported.eth"),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 3,
                emitting_address: supported_resolver_address.to_owned(),
                topics: vec![version_changed_topic0(), alice.namehash.clone()],
                data: encode_resolver_version_changed_log_data(7),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 4,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(pending_resolver_address),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 5,
                emitting_address: pending_resolver_address.to_owned(),
                topics: vec![name_changed_topic0(), alice.namehash.clone()],
                data: encode_dynamic_string_log_data("pending.eth"),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 6,
                emitting_address: pending_resolver_address.to_owned(),
                topics: vec![version_changed_topic0(), alice.namehash.clone()],
                data: encode_resolver_version_changed_log_data(8),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 7,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(unsupported_resolver_address),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 8,
                emitting_address: unsupported_resolver_address.to_owned(),
                topics: vec![name_changed_topic0(), alice.namehash.clone()],
                data: encode_dynamic_string_log_data("unsupported.eth"),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 9,
                emitting_address: unsupported_resolver_address.to_owned(),
                topics: vec![version_changed_topic0(), alice.namehash.clone()],
                data: encode_resolver_version_changed_log_data(9),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 10,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(unlisted_resolver_address),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 11,
                emitting_address: unlisted_resolver_address.to_owned(),
                topics: vec![name_changed_topic0(), alice.namehash.clone()],
                data: encode_dynamic_string_log_data("unlisted.eth"),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let summary = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.scanned_log_count, 12);
    assert_eq!(summary.matched_log_count, 7);
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_RESOLVER_CHANGED),
        Some(&4_usize)
    );
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_RECORD_CHANGED),
        Some(&1_usize)
    );
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_RECORD_VERSION_CHANGED),
        Some(&1_usize)
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            "SELECT ARRAY_AGG(after_state->>'resolver' ORDER BY log_index) FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        vec![
            supported_resolver_address.to_owned(),
            pending_resolver_address.to_owned(),
            unsupported_resolver_address.to_owned(),
            unlisted_resolver_address.to_owned(),
        ]
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            "SELECT ARRAY_AGG(after_state->>'raw_name' ORDER BY log_index) FROM normalized_events WHERE event_kind = 'RecordChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        vec!["supported.eth".to_owned()]
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE event_kind IN ('RecordChanged', 'RecordVersionChanged') AND log_index = ANY($1::BIGINT[])"
        )
        .bind(vec![5_i64, 6, 8, 9])
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE log_index = 11 AND event_kind = 'RecordChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_unwrapped_authority_emits_supported_record_change_events_idempotently()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        "0x00000000000000000000000000000000000000aa",
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "registry",
        "0x00000000000000000000000000000000000000bb",
        Some("registry"),
        "manifests/ens/ens_v1_registry_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
        "resolver",
        "0x00000000000000000000000000000000000000cc",
        Some("public_resolver"),
        "manifests/ens/ens_v1_resolver_l1/v1.toml",
    )
    .await?;

    let block_hash = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    let transaction_hash = "0xtxcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    upsert_raw_blocks(
        database.pool(),
        &[raw_block(
            block_hash,
            Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            42,
            1_700_000_042,
        )],
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    keccak256_hex(b"alice"),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000001",
                    )),
                ],
                data: encode_registrar_name_registered_log_data("alice", 1_700_010_000),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: "0x00000000000000000000000000000000000000bb".to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(
                    "0x00000000000000000000000000000000000000cc",
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 2,
                emitting_address: "0x00000000000000000000000000000000000000cc".to_owned(),
                topics: vec![
                    text_changed_topic0(),
                    alice.namehash.clone(),
                    keccak256_hex(b"com.twitter"),
                ],
                data: encode_two_dynamic_string_log_data("com.twitter", "alice-twitter"),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 3,
                emitting_address: "0x00000000000000000000000000000000000000cc".to_owned(),
                topics: vec![addr_changed_topic0(), alice.namehash.clone()],
                data: encode_resolver_addr_changed_log_data(
                    "0x00000000000000000000000000000000000000aa",
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 4,
                emitting_address: "0x00000000000000000000000000000000000000cc".to_owned(),
                topics: vec![version_changed_topic0(), alice.namehash.clone()],
                data: encode_resolver_version_changed_log_data(7),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let first = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(first.scanned_log_count, 5);
    assert_eq!(first.matched_log_count, 5);
    assert_eq!(first.total_resource_count, 1);
    assert_eq!(first.total_normalized_event_count, 10);
    assert_eq!(first.by_kind.get(EVENT_KIND_RECORD_CHANGED), Some(&2_usize));
    assert_eq!(
        first.by_kind.get(EVENT_KIND_RECORD_VERSION_CHANGED),
        Some(&1_usize)
    );

    let authority_resource_id =
        sqlx::query_scalar::<_, Uuid>("SELECT resource_id FROM resources LIMIT 1")
            .fetch_one(database.pool())
            .await?;
    let record_change_resource_ids = sqlx::query_scalar::<_, Uuid>(
            "SELECT resource_id FROM normalized_events WHERE event_kind = 'RecordChanged' ORDER BY log_index",
        )
        .fetch_all(database.pool())
        .await?;
    assert_eq!(record_change_resource_ids, vec![authority_resource_id; 2]);
    assert_eq!(
        sqlx::query_scalar::<_, Uuid>(
            "SELECT resource_id FROM normalized_events WHERE event_kind = 'RecordVersionChanged'",
        )
        .fetch_one(database.pool())
        .await?,
        authority_resource_id
    );
    assert_eq!(
            sqlx::query_scalar::<_, Vec<String>>(
                "SELECT ARRAY_AGG(after_state->>'record_key' ORDER BY log_index) FROM normalized_events WHERE event_kind = 'RecordChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            vec!["text:com.twitter".to_owned(), "addr:60".to_owned()]
        );
    assert_eq!(
            sqlx::query_scalar::<_, Vec<Option<String>>>(
                "SELECT ARRAY_AGG(after_state->>'selector_key' ORDER BY log_index) FROM normalized_events WHERE event_kind = 'RecordChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            vec![Some("com.twitter".to_owned()), Some("60".to_owned())]
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->>'record_version' FROM normalized_events WHERE event_kind = 'RecordVersionChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            "7".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE source_family = $1 AND event_kind IN ('RecordChanged', 'RecordVersionChanged')"
            )
            .bind(SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
            .fetch_one(database.pool())
            .await?,
            3
        );

    let second = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(second.scanned_log_count, 5);
    assert_eq!(second.matched_log_count, 5);
    assert_eq!(second.total_normalized_event_count, 10);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'RecordChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'RecordVersionChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_unwrapped_authority_emits_basenames_base_authority_events_idempotently()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    insert_active_contract_fixture_with_manifest(
        database.pool(),
        ActiveContractFixtureSeed {
            namespace: "basenames",
            source_family: SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
            chain: "base-mainnet",
            deployment_epoch: "basenames_v1",
            declaration_name: "registrar",
            address: "0x00000000000000000000000000000000000000aa",
            role: Some("registrar"),
            file_path: "manifests/basenames/basenames_base_registrar/v1.toml",
        },
    )
    .await?;
    insert_active_contract_fixture_with_manifest(
        database.pool(),
        ActiveContractFixtureSeed {
            namespace: "basenames",
            source_family: SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
            chain: "base-mainnet",
            deployment_epoch: "basenames_v1",
            declaration_name: "registry",
            address: "0x00000000000000000000000000000000000000bb",
            role: Some("registry"),
            file_path: "manifests/basenames/basenames_base_registry/v1.toml",
        },
    )
    .await?;
    let resolver_manifest_id = insert_active_contract_fixture_with_manifest(
        database.pool(),
        ActiveContractFixtureSeed {
            namespace: "basenames",
            source_family: SOURCE_FAMILY_BASENAMES_BASE_RESOLVER,
            chain: "base-mainnet",
            deployment_epoch: "basenames_v1",
            declaration_name: "resolver",
            address: "0x00000000000000000000000000000000000000cc",
            role: Some("resolver"),
            file_path: "manifests/basenames/basenames_base_resolver/v1.toml",
        },
    )
    .await?;
    let pending_resolver_contract_instance_id = Uuid::new_v4();
    let pending_resolver_address = "0x00000000000000000000000000000000000000dd";
    insert_contract_instance(
        database.pool(),
        pending_resolver_contract_instance_id,
        "base-mainnet",
        "contract",
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        ManifestContractInstanceSeed {
            manifest_id: resolver_manifest_id,
            declaration_kind: "contract",
            declaration_name: "pending_resolver",
            contract_instance_id: pending_resolver_contract_instance_id,
            declared_address: pending_resolver_address,
            role: Some("candidate_resolver"),
            proxy_kind: Some("none"),
        },
    )
    .await?;
    insert_contract_instance_address(
        database.pool(),
        pending_resolver_contract_instance_id,
        "base-mainnet",
        pending_resolver_address,
        resolver_manifest_id,
    )
    .await?;

    let block_hash = "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    let transaction_hash = "0xtxdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    upsert_raw_blocks(
        database.pool(),
        &[raw_block_on_chain(
            "base-mainnet",
            block_hash,
            Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            42,
            1_700_000_042,
        )],
    )
    .await?;

    let alice = observe_registrar_name_with_version(
        "alice",
        AuthorityProfile::Basenames,
        ENS_NORMALIZER_VERSION,
    )?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
                topics: vec![
                    basenames_name_registered_topic0(),
                    keccak256_hex(b"alice"),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000001",
                    )),
                ],
                data: encode_basenames_registrar_name_registered_log_data("alice", 1_700_010_000),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: "0x00000000000000000000000000000000000000bb".to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(
                    "0x00000000000000000000000000000000000000cc",
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 2,
                emitting_address: "0x00000000000000000000000000000000000000cc".to_owned(),
                topics: vec![
                    text_changed_with_value_topic0(),
                    alice.namehash.clone(),
                    keccak256_hex(b"com.twitter"),
                ],
                data: encode_two_dynamic_string_log_data("com.twitter", "alice-twitter"),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 3,
                emitting_address: "0x00000000000000000000000000000000000000cc".to_owned(),
                topics: vec![version_changed_topic0(), alice.namehash.clone()],
                data: encode_resolver_version_changed_log_data(7),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 4,
                emitting_address: pending_resolver_address.to_owned(),
                topics: vec![
                    text_changed_with_value_topic0(),
                    alice.namehash.clone(),
                    keccak256_hex(b"com.github"),
                ],
                data: encode_two_dynamic_string_log_data("com.github", "alice-github"),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 5,
                emitting_address: pending_resolver_address.to_owned(),
                topics: vec![version_changed_topic0(), alice.namehash.clone()],
                data: encode_resolver_version_changed_log_data(8),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let first = sync_ens_v1_unwrapped_authority(database.pool(), "base-mainnet").await?;
    assert_eq!(first.scanned_log_count, 6);
    assert_eq!(first.matched_log_count, 4);
    assert_eq!(first.total_name_surface_count, 1);
    assert_eq!(first.total_resource_count, 1);
    assert_eq!(first.total_surface_binding_count, 1);
    assert_eq!(first.total_normalized_event_count, 9);
    assert_eq!(
        first.by_kind,
        BTreeMap::from([
            (EVENT_KIND_AUTHORITY_EPOCH_CHANGED.to_owned(), 1_usize),
            (EVENT_KIND_EXPIRY_CHANGED.to_owned(), 1_usize),
            (EVENT_KIND_PERMISSION_CHANGED.to_owned(), 2_usize),
            (EVENT_KIND_RECORD_CHANGED.to_owned(), 1_usize),
            (EVENT_KIND_RECORD_VERSION_CHANGED.to_owned(), 1_usize),
            (EVENT_KIND_REGISTRATION_GRANTED.to_owned(), 1_usize),
            (EVENT_KIND_RESOLVER_CHANGED.to_owned(), 1_usize),
            (EVENT_KIND_SURFACE_BOUND.to_owned(), 1_usize),
        ])
    );

    let second = sync_ens_v1_unwrapped_authority(database.pool(), "base-mainnet").await?;
    assert_eq!(second.scanned_log_count, 6);
    assert_eq!(second.matched_log_count, 4);
    assert_eq!(second.total_normalized_event_count, 9);

    let logical_name_id = "basenames:alice.base.eth";
    let surface = load_name_surface(database.pool(), logical_name_id)
        .await?
        .context("Basenames name surface should persist")?;
    assert_eq!(surface.namespace, "basenames");
    assert_eq!(surface.canonical_display_name, "alice.base.eth");
    assert_eq!(surface.namehash, alice.namehash);
    assert_eq!(surface.labelhashes, alice.labelhashes);

    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            "SELECT ARRAY_AGG(DISTINCT namespace ORDER BY namespace) FROM normalized_events"
        )
        .fetch_one(database.pool())
        .await?,
        vec!["basenames".to_owned()]
    );
    assert_eq!(
            sqlx::query_scalar::<_, Vec<String>>(
                "SELECT ARRAY_AGG(event_kind ORDER BY log_index) FROM normalized_events WHERE source_family = $1"
            )
            .bind(SOURCE_FAMILY_BASENAMES_BASE_RESOLVER)
            .fetch_one(database.pool())
            .await?,
            vec![
                EVENT_KIND_RECORD_CHANGED.to_owned(),
                EVENT_KIND_RECORD_VERSION_CHANGED.to_owned(),
            ]
        );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT logical_name_id FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        logical_name_id.to_owned()
    );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->>'namehash' FROM normalized_events WHERE event_kind = 'ResolverChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            alice.namehash
        );
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE source_family = $1 AND event_kind = 'PermissionChanged'"
            )
            .bind(SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR)
            .fetch_one(database.pool())
            .await?,
            1
        );
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE source_family = $1 AND event_kind = 'PermissionChanged'"
            )
            .bind(SOURCE_FAMILY_BASENAMES_BASE_REGISTRY)
            .fetch_one(database.pool())
            .await?,
            1
        );

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_unwrapped_authority_gates_basenames_dynamic_resolver_facts_by_l2_profile()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let registrar_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "basenames",
            source_family: SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
            chain: "base-mainnet",
            deployment_epoch: "basenames_v1",
            rollout_status: "active",
            normalizer_version: "ensip15@ens-normalize-0.1.1",
            file_path: "manifests/basenames/basenames_base_registrar/v1.toml",
        },
    )
    .await?;
    let registry_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "basenames",
            source_family: SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
            chain: "base-mainnet",
            deployment_epoch: "basenames_v1",
            rollout_status: "active",
            normalizer_version: "ensip15@ens-normalize-0.1.1",
            file_path: "manifests/basenames/basenames_base_registry/v1.toml",
        },
    )
    .await?;
    let resolver_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "basenames",
            source_family: SOURCE_FAMILY_BASENAMES_BASE_RESOLVER,
            chain: "base-mainnet",
            deployment_epoch: "basenames_v1",
            rollout_status: "active",
            normalizer_version: "ensip15@ens-normalize-0.1.1",
            file_path: "manifests/basenames/basenames_base_resolver/v1.toml",
        },
    )
    .await?;
    let registrar_contract_instance_id = Uuid::new_v4();
    let registry_contract_instance_id = Uuid::new_v4();
    let seed_resolver_contract_instance_id = Uuid::new_v4();
    let supported_resolver_contract_instance_id = Uuid::new_v4();
    let pending_resolver_contract_instance_id = Uuid::new_v4();
    let unsupported_resolver_contract_instance_id = Uuid::new_v4();
    let registrar_address = "0x00000000000000000000000000000000000001aa";
    let registry_address = "0x00000000000000000000000000000000000001bb";
    let seed_resolver_address = "0x00000000000000000000000000000000000001cc";
    let supported_resolver_address = "0x00000000000000000000000000000000000001dd";
    let pending_resolver_address = "0x00000000000000000000000000000000000001ee";
    let unsupported_resolver_address = "0x00000000000000000000000000000000000001ff";
    let l2_resolver_code_hash =
        "0x1111111111111111111111111111111111111111111111111111111111111111";

    for (contract_instance_id, manifest_id, address, role) in [
        (
            registrar_contract_instance_id,
            registrar_manifest_id,
            registrar_address,
            "registrar",
        ),
        (
            registry_contract_instance_id,
            registry_manifest_id,
            registry_address,
            "registry",
        ),
        (
            seed_resolver_contract_instance_id,
            resolver_manifest_id,
            seed_resolver_address,
            "resolver",
        ),
    ] {
        insert_contract_instance(
            database.pool(),
            contract_instance_id,
            "base-mainnet",
            "contract",
        )
        .await?;
        insert_manifest_contract_instance(
            database.pool(),
            ManifestContractInstanceSeed {
                manifest_id,
                declaration_kind: "contract",
                declaration_name: role,
                contract_instance_id,
                declared_address: address,
                role: Some(role),
                proxy_kind: Some("none"),
            },
        )
        .await?;
        insert_contract_instance_address(
            database.pool(),
            contract_instance_id,
            "base-mainnet",
            address,
            manifest_id,
        )
        .await?;
    }

    for (contract_instance_id, address) in [
        (
            supported_resolver_contract_instance_id,
            supported_resolver_address,
        ),
        (
            pending_resolver_contract_instance_id,
            pending_resolver_address,
        ),
        (
            unsupported_resolver_contract_instance_id,
            unsupported_resolver_address,
        ),
    ] {
        insert_contract_instance(
            database.pool(),
            contract_instance_id,
            "base-mainnet",
            "contract",
        )
        .await?;
        insert_contract_instance_address(
            database.pool(),
            contract_instance_id,
            "base-mainnet",
            address,
            resolver_manifest_id,
        )
        .await?;
        insert_active_discovery_edge_with_range(
            database.pool(),
            ActiveDiscoveryEdgeSeed {
                chain_id: "base-mainnet",
                edge_kind: "resolver",
                from_contract_instance_id: registry_contract_instance_id,
                to_contract_instance_id: contract_instance_id,
                source_manifest_id: registry_manifest_id,
                active_from_block_number: None,
                active_to_block_number: None,
            },
        )
        .await?;
    }
    upsert_raw_code_hashes(
        database.pool(),
        &[
            raw_code_hash_for_address_on_chain(
                "base-mainnet",
                seed_resolver_address,
                l2_resolver_code_hash,
            ),
            raw_code_hash_for_address_on_chain(
                "base-mainnet",
                supported_resolver_address,
                l2_resolver_code_hash,
            ),
            raw_code_hash_for_address_on_chain(
                "base-mainnet",
                unsupported_resolver_address,
                "0x2222222222222222222222222222222222222222222222222222222222222222",
            ),
        ],
    )
    .await?;

    let block_hash = "0xfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfb";
    let transaction_hash = "0xtxfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfbfb";
    let alice = observe_registrar_name_with_version(
        "alice",
        AuthorityProfile::Basenames,
        ENS_NORMALIZER_VERSION,
    )?;
    upsert_raw_blocks(
        database.pool(),
        &[raw_block_on_chain(
            "base-mainnet",
            block_hash,
            Some("0xfafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafafa"),
            42,
            1_700_000_042,
        )],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    basenames_name_registered_topic0(),
                    keccak256_hex(b"alice"),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000001",
                    )),
                ],
                data: encode_basenames_registrar_name_registered_log_data("alice", 1_700_010_000),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(supported_resolver_address),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 2,
                emitting_address: supported_resolver_address.to_owned(),
                topics: vec![name_changed_topic0(), alice.namehash.clone()],
                data: encode_dynamic_string_log_data("supported.base.eth"),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 3,
                emitting_address: supported_resolver_address.to_owned(),
                topics: vec![version_changed_topic0(), alice.namehash.clone()],
                data: encode_resolver_version_changed_log_data(7),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 4,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(pending_resolver_address),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 5,
                emitting_address: pending_resolver_address.to_owned(),
                topics: vec![name_changed_topic0(), alice.namehash.clone()],
                data: encode_dynamic_string_log_data("pending.base.eth"),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 6,
                emitting_address: pending_resolver_address.to_owned(),
                topics: vec![version_changed_topic0(), alice.namehash.clone()],
                data: encode_resolver_version_changed_log_data(8),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 7,
                emitting_address: registry_address.to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(unsupported_resolver_address),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 8,
                emitting_address: unsupported_resolver_address.to_owned(),
                topics: vec![name_changed_topic0(), alice.namehash.clone()],
                data: encode_dynamic_string_log_data("unsupported.base.eth"),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 9,
                emitting_address: unsupported_resolver_address.to_owned(),
                topics: vec![version_changed_topic0(), alice.namehash.clone()],
                data: encode_resolver_version_changed_log_data(9),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let summary = sync_ens_v1_unwrapped_authority(database.pool(), "base-mainnet").await?;
    assert_eq!(summary.scanned_log_count, 10);
    assert_eq!(summary.matched_log_count, 6);
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_RESOLVER_CHANGED),
        Some(&3_usize)
    );
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_RECORD_CHANGED),
        Some(&1_usize)
    );
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_RECORD_VERSION_CHANGED),
        Some(&1_usize)
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            "SELECT ARRAY_AGG(after_state->>'raw_name' ORDER BY log_index) FROM normalized_events WHERE event_kind = 'RecordChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        vec!["supported.base.eth".to_owned()]
    );
    assert_eq!(
        sqlx::query_scalar::<_, Vec<String>>(
            "SELECT ARRAY_AGG(after_state->>'record_version' ORDER BY log_index) FROM normalized_events WHERE event_kind = 'RecordVersionChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        vec!["7".to_owned()]
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind IN ('RecordChanged', 'RecordVersionChanged') AND log_index = ANY($1::BIGINT[])"
        )
        .bind(vec![5_i64, 6, 8, 9])
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_unwrapped_authority_backfills_basenames_primary_claim_source_observations()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let reverse_manifest_id = insert_active_contract_fixture_with_manifest(
        database.pool(),
        ActiveContractFixtureSeed {
            namespace: "basenames",
            source_family: "basenames_base_primary",
            chain: "base-mainnet",
            deployment_epoch: "basenames_v1",
            declaration_name: "reverse_registrar",
            address: "0x00000000000000000000000000000000000000ad",
            role: Some(CONTRACT_ROLE_REVERSE_REGISTRAR),
            file_path: "manifests/basenames/basenames_base_primary/v1.toml",
        },
    )
    .await?;
    insert_active_contract_fixture_with_manifest(
        database.pool(),
        ActiveContractFixtureSeed {
            namespace: "basenames",
            source_family: SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
            chain: "base-mainnet",
            deployment_epoch: "basenames_v1",
            declaration_name: "registry",
            address: "0x00000000000000000000000000000000000000bb",
            role: Some("registry"),
            file_path: "manifests/basenames/basenames_base_registry/v1.toml",
        },
    )
    .await?;
    insert_active_contract_fixture_with_manifest(
        database.pool(),
        ActiveContractFixtureSeed {
            namespace: "basenames",
            source_family: SOURCE_FAMILY_BASENAMES_BASE_RESOLVER,
            chain: "base-mainnet",
            deployment_epoch: "basenames_v1",
            declaration_name: "resolver",
            address: "0x00000000000000000000000000000000000000cc",
            role: Some("resolver"),
            file_path: "manifests/basenames/basenames_base_resolver/v1.toml",
        },
    )
    .await?;

    let block_hash = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    let transaction_hash = "0xtxeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    let claimed_address = "0x0000000000000000000000000000000000005678";
    let reverse_node = base_reverse_node_for_address(claimed_address);
    let reverse_name = base_reverse_name_for_address(claimed_address);

    upsert_raw_blocks(
        database.pool(),
        &[raw_block_on_chain(
            "base-mainnet",
            block_hash,
            Some("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
            42,
            1_700_000_042,
        )],
    )
    .await?;
    let mut reverse_claim = basenames_reverse_claim_event(
        reverse_manifest_id,
        block_hash,
        transaction_hash,
        0,
        claimed_address,
        &reverse_node,
        &reverse_name,
    );
    reverse_claim
        .after_state
        .get_mut("claim_provenance")
        .and_then(|value| value.as_object_mut())
        .expect("test reverse claim provenance is an object")
        .remove("contract_role");
    upsert_normalized_events(database.pool(), &[reverse_claim]).await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: "0x00000000000000000000000000000000000000bb".to_owned(),
                topics: vec![new_resolver_topic0(), reverse_node.clone()],
                data: encode_registry_new_resolver_log_data(
                    "0x00000000000000000000000000000000000000cc",
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 2,
                emitting_address: "0x00000000000000000000000000000000000000cc".to_owned(),
                topics: vec![name_changed_topic0(), reverse_node.clone()],
                data: encode_dynamic_string_log_data("alice.base.eth"),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 3,
                emitting_address: "0x00000000000000000000000000000000000000cc".to_owned(),
                topics: vec![version_changed_topic0(), reverse_node.clone()],
                data: encode_resolver_version_changed_log_data(7),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let first = sync_ens_v1_unwrapped_authority(database.pool(), "base-mainnet").await?;
    assert_eq!(first.scanned_log_count, 3);
    assert_eq!(first.matched_log_count, 3);
    assert_eq!(first.total_name_surface_count, 0);
    assert_eq!(first.total_resource_count, 0);
    assert_eq!(first.total_surface_binding_count, 0);
    assert_eq!(first.total_normalized_event_count, 3);
    assert_eq!(
        first.by_kind.get(EVENT_KIND_RESOLVER_CHANGED),
        Some(&1_usize)
    );
    assert_eq!(first.by_kind.get(EVENT_KIND_RECORD_CHANGED), Some(&1_usize));
    assert_eq!(
        first.by_kind.get(EVENT_KIND_RECORD_VERSION_CHANGED),
        Some(&1_usize)
    );

    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE namespace = 'basenames' AND event_kind = 'ResolverChanged' AND logical_name_id IS NULL AND resource_id IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            1
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->'primary_claim_source'->>'address' FROM normalized_events WHERE namespace = 'basenames' AND event_kind = 'ResolverChanged' AND logical_name_id IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            claimed_address.to_ascii_lowercase()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->>'raw_name' FROM normalized_events WHERE namespace = 'basenames' AND event_kind = 'RecordChanged' AND logical_name_id IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            "alice.base.eth".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->'primary_claim_source'->>'reverse_node' FROM normalized_events WHERE namespace = 'basenames' AND event_kind = 'RecordChanged' AND logical_name_id IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            reverse_node.to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->'primary_claim_source'->'claim_provenance'->>'source_family' FROM normalized_events WHERE namespace = 'basenames' AND event_kind = 'RecordVersionChanged' AND logical_name_id IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            "basenames_base_primary".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->'primary_claim_source'->'claim_provenance'->>'contract_role' FROM normalized_events WHERE namespace = 'basenames' AND event_kind = 'RecordVersionChanged' AND logical_name_id IS NULL"
            )
            .fetch_one(database.pool())
            .await?,
            CONTRACT_ROLE_REVERSE_REGISTRAR.to_owned()
        );

    let second = sync_ens_v1_unwrapped_authority(database.pool(), "base-mainnet").await?;
    assert_eq!(second.scanned_log_count, 3);
    assert_eq!(second.matched_log_count, 3);
    assert_eq!(second.total_normalized_event_count, 3);
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE namespace = 'basenames' AND logical_name_id IS NULL AND event_kind IN ('ResolverChanged', 'RecordChanged', 'RecordVersionChanged')"
            )
            .fetch_one(database.pool())
            .await?,
            3
        );

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_unwrapped_authority_drops_resolver_record_logs_without_current_context()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        "registrar",
        "0x00000000000000000000000000000000000000aa",
        Some("registrar"),
        "manifests/ens/ens_v1_registrar_l1/v1.toml",
    )
    .await?;
    insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "registry",
        "0x00000000000000000000000000000000000000bb",
        Some("registry"),
        "manifests/ens/ens_v1_registry_l1/v1.toml",
    )
    .await?;
    let resolver_manifest_id = insert_active_contract_fixture(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
        "resolver",
        "0x00000000000000000000000000000000000000cc",
        Some("public_resolver"),
        "manifests/ens/ens_v1_resolver_l1/v1.toml",
    )
    .await?;
    let alternate_resolver_contract_instance_id = Uuid::new_v4();
    insert_contract_instance(
        database.pool(),
        alternate_resolver_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        ManifestContractInstanceSeed {
            manifest_id: resolver_manifest_id,
            declaration_kind: "contract",
            declaration_name: "resolver_alt",
            contract_instance_id: alternate_resolver_contract_instance_id,
            declared_address: "0x00000000000000000000000000000000000000dd",
            role: Some("public_resolver"),
            proxy_kind: Some("none"),
        },
    )
    .await?;
    insert_contract_instance_address(
        database.pool(),
        alternate_resolver_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000dd",
        resolver_manifest_id,
    )
    .await?;

    let block_hash = "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    let transaction_hash = "0xtxdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    upsert_raw_blocks(
        database.pool(),
        &[raw_block(
            block_hash,
            Some("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
            42,
            1_700_000_042,
        )],
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    keccak256_hex(b"alice"),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000001",
                    )),
                ],
                data: encode_registrar_name_registered_log_data("alice", 1_700_010_000),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: "0x00000000000000000000000000000000000000cc".to_owned(),
                topics: vec![
                    text_changed_topic0(),
                    alice.namehash.clone(),
                    keccak256_hex(b"com.twitter"),
                ],
                data: encode_two_dynamic_string_log_data("com.twitter", "alice-twitter"),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 2,
                emitting_address: "0x00000000000000000000000000000000000000bb".to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(
                    "0x00000000000000000000000000000000000000cc",
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 42,
                transaction_hash: transaction_hash.to_owned(),
                transaction_index: 0,
                log_index: 3,
                emitting_address: "0x00000000000000000000000000000000000000dd".to_owned(),
                topics: vec![
                    text_changed_topic0(),
                    alice.namehash.clone(),
                    keccak256_hex(b"com.github"),
                ],
                data: encode_two_dynamic_string_log_data("com.github", "alice-github"),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let summary = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.scanned_log_count, 4);
    assert_eq!(summary.matched_log_count, 4);
    assert_eq!(summary.total_normalized_event_count, 7);
    assert_eq!(summary.by_kind.get(EVENT_KIND_RECORD_CHANGED), None);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'RecordChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'RecordVersionChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_unwrapped_authority_partitions_permission_events_by_authoritative_resource_id()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let registrar_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "ens",
            source_family: SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v1",
            rollout_status: "active",
            normalizer_version: "ensip15@ens-normalize-0.1.1",
            file_path: "manifests/ens/ens_v1_registrar_l1/v1.toml",
        },
    )
    .await?;
    let registrar_contract_instance_id = Uuid::new_v4();
    insert_contract_instance(
        database.pool(),
        registrar_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        ManifestContractInstanceSeed {
            manifest_id: registrar_manifest_id,
            declaration_kind: "contract",
            declaration_name: "registrar",
            contract_instance_id: registrar_contract_instance_id,
            declared_address: "0x00000000000000000000000000000000000000aa",
            role: Some("registrar"),
            proxy_kind: Some("none"),
        },
    )
    .await?;
    insert_contract_instance_address(
        database.pool(),
        registrar_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
        registrar_manifest_id,
    )
    .await?;

    let registry_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "ens",
            source_family: SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v1",
            rollout_status: "active",
            normalizer_version: "ensip15@ens-normalize-0.1.1",
            file_path: "manifests/ens/ens_v1_registry_l1/v1.toml",
        },
    )
    .await?;
    let registry_contract_instance_id = Uuid::new_v4();
    insert_contract_instance(
        database.pool(),
        registry_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        ManifestContractInstanceSeed {
            manifest_id: registry_manifest_id,
            declaration_kind: "contract",
            declaration_name: "registry",
            contract_instance_id: registry_contract_instance_id,
            declared_address: "0x00000000000000000000000000000000000000bb",
            role: Some("registry"),
            proxy_kind: Some("none"),
        },
    )
    .await?;
    insert_contract_instance_address(
        database.pool(),
        registry_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000bb",
        registry_manifest_id,
    )
    .await?;

    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let registration_expiry = 1_700_000_100;
    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block(
                "0x1111111111111111111111111111111111111111111111111111111111111111",
                Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                41,
                1_700_000_010,
            ),
            raw_block(
                "0x2222222222222222222222222222222222222222222222222222222222222222",
                Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
                42,
                1_700_000_042,
            ),
            raw_block(
                "0x3333333333333333333333333333333333333333333333333333333333333333",
                Some("0x2222222222222222222222222222222222222222222222222222222222222222"),
                43,
                1_700_000_050,
            ),
            raw_block(
                "0x4444444444444444444444444444444444444444444444444444444444444444",
                Some("0x3333333333333333333333333333333333333333333333333333333333333333"),
                44,
                release_after_grace(OffsetDateTime::from_unix_timestamp(registration_expiry)?)?
                    .unix_timestamp(),
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x1111111111111111111111111111111111111111111111111111111111111111"
                    .to_owned(),
                block_number: 41,
                transaction_hash:
                    "0xtx11111111111111111111111111111111111111111111111111111111111111".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: "0x00000000000000000000000000000000000000bb".to_owned(),
                topics: vec![new_owner_topic0(), eth_node(), keccak256_hex(b"alice")],
                data: abi_word_address("0x0000000000000000000000000000000000000003").to_vec(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x2222222222222222222222222222222222222222222222222222222222222222"
                    .to_owned(),
                block_number: 42,
                transaction_hash:
                    "0xtx22222222222222222222222222222222222222222222222222222222222222".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
                topics: vec![
                    name_registered_topic0(),
                    keccak256_hex(b"alice"),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000001",
                    )),
                ],
                data: encode_registrar_name_registered_log_data("alice", registration_expiry),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x3333333333333333333333333333333333333333333333333333333333333333"
                    .to_owned(),
                block_number: 43,
                transaction_hash:
                    "0xtx33333333333333333333333333333333333333333333333333333333333333".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
                topics: vec![
                    transfer_topic0(),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000001",
                    )),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000002",
                    )),
                    alice.labelhashes[0].clone(),
                ],
                data: Vec::new(),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0x3333333333333333333333333333333333333333333333333333333333333333"
                    .to_owned(),
                block_number: 43,
                transaction_hash:
                    "0xtx33333333333333333333333333333333333333333333333333333333333333".to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: "0x00000000000000000000000000000000000000bb".to_owned(),
                topics: vec![new_resolver_topic0(), alice.namehash.clone()],
                data: encode_registry_new_resolver_log_data(
                    "0x00000000000000000000000000000000000000cc",
                ),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let summary = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(summary.total_resource_count, 2);
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_PERMISSION_CHANGED),
        Some(&6_usize)
    );

    let registrar_resource_id = sqlx::query_scalar::<_, Uuid>(
            "SELECT resource_id FROM resources WHERE provenance->>'authority_kind' = 'registrar' LIMIT 1",
        )
        .fetch_one(database.pool())
        .await?;
    let registry_resource_id = sqlx::query_scalar::<_, Uuid>(
            "SELECT resource_id FROM resources WHERE provenance->>'authority_kind' = 'registry_only' LIMIT 1",
        )
        .fetch_one(database.pool())
        .await?;
    assert_ne!(registrar_resource_id, registry_resource_id);
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PermissionChanged' AND resource_id = $1"
            )
            .bind(registrar_resource_id)
            .fetch_one(database.pool())
            .await?,
            3
        );
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PermissionChanged' AND resource_id = $1"
            )
            .bind(registry_resource_id)
            .fetch_one(database.pool())
            .await?,
            3
        );
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PermissionChanged' AND resource_id = $1 AND block_number = 43"
            )
            .bind(registry_resource_id)
            .fetch_one(database.pool())
            .await?,
            2
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->>'subject' FROM normalized_events WHERE event_kind = 'PermissionChanged' AND resource_id = $1 AND after_state->'scope'->>'kind' = 'resource' AND after_state->>'subject' <> '' ORDER BY block_number DESC LIMIT 1"
            )
            .bind(registry_resource_id)
            .fetch_one(database.pool())
            .await?,
            "0x0000000000000000000000000000000000000003".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT after_state->'scope'->>'resolver_address' FROM normalized_events WHERE event_kind = 'PermissionChanged' AND resource_id = $1 AND after_state->'scope'->>'kind' = 'resolver' ORDER BY block_number DESC LIMIT 1"
            )
            .bind(registry_resource_id)
            .fetch_one(database.pool())
            .await?,
            "0x00000000000000000000000000000000000000cc".to_owned()
        );

    database.cleanup().await
}
