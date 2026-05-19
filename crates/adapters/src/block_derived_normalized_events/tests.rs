use std::{
    collections::HashMap,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use bigname_storage::{
    RawBlock, RawLog, default_database_url, load_normalized_event_counts_by_kind,
    load_normalized_events_by_namespace, upsert_raw_blocks, upsert_raw_logs,
};
use serde_json::{Value, json};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::time::OffsetDateTime,
};
use uuid::Uuid;

use crate::adapter_manifest::ActiveManifestEventTopic0sBySignature;

use super::*;

const ABI_EVENT_NAME_WRAPPED: &str = "NameWrapped";
const ABI_EVENT_LABEL_REGISTERED: &str = "LabelRegistered";
const ABI_EVENT_LABEL_RESERVED: &str = "LabelReserved";
const ABI_EVENT_PARENT_UPDATED: &str = "ParentUpdated";
const ABI_EVENT_NAME_REGISTERED: &str = "NameRegistered";
const ABI_EVENT_NAME_RENEWED: &str = "NameRenewed";
const ABI_EVENT_ALIAS_CHANGED: &str = "AliasChanged";
const ABI_EVENT_NAMED_RESOURCE: &str = "NamedResource";
const ABI_EVENT_NAMED_TEXT_RESOURCE: &str = "NamedTextResource";
const ABI_EVENT_NAMED_ADDR_RESOURCE: &str = "NamedAddrResource";

// ENSv1's `NameWrapped` event declaration starts at this pinned source line.
// (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f)
const UPSTREAM_NAME_WRAPPED_SIGNATURE: &str = "NameWrapped(bytes32,bytes,address,uint32,uint64)";
const UPSTREAM_NAME_WRAPPED_TOPIC0: &str =
    "0x8ce7013e8abebc55c3890a68f5a27c67c3f7efa64e584de5fb22363c606fd340";
const OLD_SWAPPED_NAME_WRAPPED_SIGNATURE: &str = "NameWrapped(bytes,bytes32,address,uint32,uint64)";
const OLD_SWAPPED_NAME_WRAPPED_TOPIC0: &str =
    "0xaeee18e42fd564b93988f0f5a001eb2dea6bde99cb3caa60a682c28105483c67";

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

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
            .context("failed to parse database URL for block-derived normalized-event tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_adapters_block_derived_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for block-derived normalized-event tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect test pool for block-derived normalized-event tests")?;

        bigname_storage::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for block-derived normalized-event tests")?;

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

async fn insert_manifest_version(pool: &PgPool, seed: ManifestVersionSeed<'_>) -> Result<i64> {
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
    .bind(manifest_payload(&seed))
    .fetch_one(pool)
    .await
    .context("failed to insert manifest version")
}

fn manifest_payload(seed: &ManifestVersionSeed<'_>) -> Value {
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
            "events": manifest_abi_events(seed.source_family),
        },
    })
}

fn manifest_abi_events(source_family: &str) -> Vec<Value> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1 => vec![
            json!({
                "name": ABI_EVENT_NAME_REGISTERED,
                "fragment": "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 cost, uint256 expires)",
            }),
            json!({
                "name": ABI_EVENT_NAME_RENEWED,
                "fragment": "event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires)",
            }),
        ],
        SOURCE_FAMILY_ENS_V1_WRAPPER_L1 => vec![json!({
            "name": ABI_EVENT_NAME_WRAPPED,
            "fragment": "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
        })],
        SOURCE_FAMILY_ENS_V2_ROOT_L1 | SOURCE_FAMILY_ENS_V2_REGISTRY_L1 => vec![
            json!({
                "name": ABI_EVENT_LABEL_REGISTERED,
                "fragment": "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
            }),
            json!({
                "name": ABI_EVENT_LABEL_RESERVED,
                "fragment": "event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender)",
            }),
            json!({
                "name": ABI_EVENT_PARENT_UPDATED,
                "fragment": "event ParentUpdated(address parent, string label, address indexed sender)",
            }),
        ],
        SOURCE_FAMILY_ENS_V2_REGISTRAR_L1 => vec![
            json!({
                "name": ABI_EVENT_NAME_REGISTERED,
                "fragment": "event NameRegistered(uint256 indexed tokenId, string label, address owner, address subregistry, address resolver, uint64 duration, address paymentToken, bytes32 referrer, uint256 base, uint256 premium)",
            }),
            json!({
                "name": ABI_EVENT_NAME_RENEWED,
                "fragment": "event NameRenewed(uint256 indexed tokenId, string label, uint64 duration, uint64 newExpiry, address paymentToken, bytes32 referrer, uint256 base)",
            }),
        ],
        SOURCE_FAMILY_ENS_V2_RESOLVER_L1 => vec![
            json!({
                "name": ABI_EVENT_ALIAS_CHANGED,
                "fragment": "event AliasChanged(bytes indexed indexedFromName, bytes indexed indexedToName, bytes fromName, bytes toName)",
            }),
            json!({
                "name": ABI_EVENT_NAMED_RESOURCE,
                "fragment": "event NamedResource(uint256 indexed resource, bytes name)",
            }),
            json!({
                "name": ABI_EVENT_NAMED_TEXT_RESOURCE,
                "fragment": "event NamedTextResource(uint256 indexed resource, bytes name, bytes32 indexed keyHash, string key)",
            }),
            json!({
                "name": ABI_EVENT_NAMED_ADDR_RESOURCE,
                "fragment": "event NamedAddrResource(uint256 indexed resource, bytes name, uint256 indexed coinType)",
            }),
        ],
        _ => Vec::new(),
    }
}

struct ManifestContractInstanceSeed<'a> {
    manifest_id: i64,
    declaration_kind: &'a str,
    declaration_name: &'a str,
    contract_instance_id: Uuid,
    declared_address: &'a str,
    role: Option<&'a str>,
    proxy_kind: Option<&'a str>,
    implementation_contract_instance_id: Option<Uuid>,
    declared_implementation_address: Option<&'a str>,
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
        VALUES ($1, $2, $3, $4, $5, NULL, NULL, $6, $7, $8, $9)
        "#,
    )
    .bind(seed.manifest_id)
    .bind(seed.declaration_kind)
    .bind(seed.declaration_name)
    .bind(seed.contract_instance_id)
    .bind(seed.declared_address)
    .bind(seed.role)
    .bind(seed.proxy_kind)
    .bind(seed.implementation_contract_instance_id)
    .bind(seed.declared_implementation_address)
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

async fn deactivate_active_contract_instance_addresses(
    pool: &PgPool,
    contract_instance_id: Uuid,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE contract_instance_addresses
        SET deactivated_at = now()
        WHERE contract_instance_id = $1
          AND deactivated_at IS NULL
        "#,
    )
    .bind(contract_instance_id)
    .execute(pool)
    .await
    .context("failed to deactivate contract-instance address rows")?;
    Ok(())
}

async fn insert_discovery_edge(
    pool: &PgPool,
    chain_id: &str,
    edge_kind: &str,
    from_contract_instance_id: Uuid,
    to_contract_instance_id: Uuid,
    source_manifest_id: i64,
) -> Result<()> {
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
            provenance
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
        "#,
    )
    .bind(chain_id)
    .bind(edge_kind)
    .bind(from_contract_instance_id)
    .bind(to_contract_instance_id)
    .bind(format!("test:{edge_kind}"))
    .bind(source_manifest_id)
    .bind("automatic")
    .bind("{}")
    .execute(pool)
    .await
    .context("failed to insert discovery edge")?;
    Ok(())
}

async fn insert_raw_name_wrapped_log(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
    address: &str,
    canonicality_state: CanonicalityState,
) -> Result<()> {
    upsert_raw_blocks(
        pool,
        &[RawBlock {
            chain_id: chain_id.to_owned(),
            block_hash: block_hash.to_owned(),
            parent_hash: None,
            block_number,
            block_timestamp: OffsetDateTime::UNIX_EPOCH,
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state,
        }],
    )
    .await?;

    let dns_name = dns_encoded_name(&["wrapped", "eth"]);
    upsert_raw_logs(
        pool,
        &[RawLog {
            chain_id: chain_id.to_owned(),
            block_hash: block_hash.to_owned(),
            block_number,
            transaction_hash: format!("0xtx{block_number:02x}"),
            transaction_index: 0,
            log_index: 0,
            emitting_address: address.to_owned(),
            topics: vec![
                UPSTREAM_NAME_WRAPPED_TOPIC0.to_owned(),
                namehash_hex_bytes(&dns_name),
            ],
            data: encode_name_wrapped_log_data(&dns_name),
            canonicality_state,
        }],
    )
    .await?;

    Ok(())
}

fn dns_encoded_name(labels: &[&str]) -> Vec<u8> {
    let mut encoded = Vec::new();
    for label in labels {
        encoded.push(u8::try_from(label.len()).expect("test label length must fit in u8"));
        encoded.extend_from_slice(label.as_bytes());
    }
    encoded.push(0);
    encoded
}

fn namehash_hex_bytes(dns_name: &[u8]) -> String {
    let observation =
        observe_dns_encoded_name(dns_name).expect("test dns-encoded name must decode");
    observation.namehash
}

fn encode_name_wrapped_log_data(dns_name: &[u8]) -> Vec<u8> {
    let mut output = Vec::new();

    output.extend_from_slice(&abi_word_u64(128));
    output.extend_from_slice(&abi_word_address(
        "0x0000000000000000000000000000000000000001",
    ));
    output.extend_from_slice(&abi_word_u64(0));
    output.extend_from_slice(&abi_word_u64(0));
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(dns_name.len()).expect("test dns-encoded name length must fit in u64"),
    ));
    output.extend_from_slice(dns_name);

    let padded_length = dns_name.len().div_ceil(32) * 32;
    output.resize(32 * 5 + padded_length, 0);
    output
}

#[derive(Clone, Copy, Debug)]
enum RegistrarExplicitLabelEvent {
    NameRegistered,
    NameRenewed,
}

impl RegistrarExplicitLabelEvent {
    fn topic0(self) -> String {
        match self {
            Self::NameRegistered => {
                keccak_signature_hex("NameRegistered(string,bytes32,address,uint256,uint256)")
            }
            Self::NameRenewed => {
                keccak_signature_hex("NameRenewed(string,bytes32,uint256,uint256)")
            }
        }
    }

    fn topics(self, label: &str) -> Vec<String> {
        let mut topics = vec![self.topic0(), keccak256_hex(label.as_bytes())];
        if matches!(self, Self::NameRegistered) {
            topics.push(hex_string(&abi_word_address(
                "0x0000000000000000000000000000000000000001",
            )));
        }
        topics
    }
}

struct RegistrarLabelRawLogSeed<'a> {
    chain_id: &'a str,
    block_hash: &'a str,
    block_number: i64,
    address: &'a str,
    label: &'a str,
    source_event: RegistrarExplicitLabelEvent,
    canonicality_state: CanonicalityState,
}

async fn insert_raw_registrar_label_log(
    pool: &PgPool,
    seed: RegistrarLabelRawLogSeed<'_>,
) -> Result<()> {
    insert_raw_registrar_label_log_at_index(pool, seed, 0).await
}

async fn insert_raw_registrar_label_log_at_index(
    pool: &PgPool,
    seed: RegistrarLabelRawLogSeed<'_>,
    log_index: i64,
) -> Result<()> {
    upsert_raw_blocks(
        pool,
        &[RawBlock {
            chain_id: seed.chain_id.to_owned(),
            block_hash: seed.block_hash.to_owned(),
            parent_hash: None,
            block_number: seed.block_number,
            block_timestamp: OffsetDateTime::UNIX_EPOCH,
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state: seed.canonicality_state,
        }],
    )
    .await?;

    upsert_raw_logs(
        pool,
        &[RawLog {
            chain_id: seed.chain_id.to_owned(),
            block_hash: seed.block_hash.to_owned(),
            block_number: seed.block_number,
            transaction_hash: format!("0xtx{:02x}", seed.block_number),
            transaction_index: 0,
            log_index,
            emitting_address: seed.address.to_owned(),
            topics: seed.source_event.topics(seed.label),
            data: encode_registrar_label_log_data(seed.label),
            canonicality_state: seed.canonicality_state,
        }],
    )
    .await?;

    Ok(())
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct UnselectedPayloadRowCounts {
    transactions: i64,
    receipts: i64,
    payload_cache_metadata: i64,
}

async fn load_unselected_payload_row_counts(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<UnselectedPayloadRowCounts> {
    Ok(UnselectedPayloadRowCounts {
        transactions: count_block_rows(pool, "raw_transactions", chain_id, block_hash).await?,
        receipts: count_block_rows(pool, "raw_receipts", chain_id, block_hash).await?,
        payload_cache_metadata: count_block_rows(
            pool,
            "raw_payload_cache_metadata",
            chain_id,
            block_hash,
        )
        .await?,
    })
}

async fn count_block_rows(
    pool: &PgPool,
    table_name: &'static str,
    chain_id: &str,
    block_hash: &str,
) -> Result<i64> {
    let qualified_table_name = format!("public.{table_name}");
    let table_exists = sqlx::query_scalar::<_, Option<String>>("SELECT to_regclass($1)::TEXT")
        .bind(&qualified_table_name)
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to check whether {table_name} exists"))?
        .is_some();
    if !table_exists {
        return Ok(0);
    }

    let query = format!(
        "SELECT COUNT(*)::BIGINT FROM {table_name} WHERE chain_id = $1 AND block_hash = $2"
    );
    sqlx::query_scalar::<_, i64>(&query)
        .bind(chain_id)
        .bind(block_hash)
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to count {table_name} rows for block {block_hash}"))
}

fn encode_registrar_label_log_data(label: &str) -> Vec<u8> {
    let label_bytes = label.as_bytes();
    let mut output = Vec::new();

    output.extend_from_slice(&abi_word_u64(96));
    output.extend_from_slice(&abi_word_u64(1));
    output.extend_from_slice(&abi_word_u64(2));
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(label_bytes.len()).expect("test label length must fit in u64"),
    ));
    output.extend_from_slice(label_bytes);

    let padded_length = label_bytes.len().div_ceil(32) * 32;
    output.resize(32 * 4 + padded_length, 0);
    output
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

#[test]
fn name_wrapped_topic0_matches_upstream_shape_and_not_old_swapped_shape() {
    assert_eq!(
        keccak_signature_hex(UPSTREAM_NAME_WRAPPED_SIGNATURE),
        UPSTREAM_NAME_WRAPPED_TOPIC0
    );
    assert!(
        test_preimage_observed_event_topics()
            .query_topic0s()
            .contains(&UPSTREAM_NAME_WRAPPED_TOPIC0.to_owned())
    );

    assert_eq!(
        keccak_signature_hex(OLD_SWAPPED_NAME_WRAPPED_SIGNATURE),
        OLD_SWAPPED_NAME_WRAPPED_TOPIC0
    );
    assert!(
        !test_preimage_observed_event_topics()
            .query_topic0s()
            .contains(&OLD_SWAPPED_NAME_WRAPPED_TOPIC0.to_owned())
    );
}

#[test]
fn name_wrapped_upstream_topic_emits_preimage_and_old_swapped_topic_is_ignored() -> Result<()> {
    let dns_name = dns_encoded_name(&["wrapped", "eth"]);
    let upstream_log = watched_log(
        "ens_v1_wrapper_l1",
        1,
        vec![
            UPSTREAM_NAME_WRAPPED_TOPIC0.to_owned(),
            namehash_hex_bytes(&dns_name),
        ],
        encode_name_wrapped_log_data(&dns_name),
    );
    let upstream_events = build_test_preimage_observed_events(&upstream_log)?;
    assert_eq!(upstream_events.len(), 1);
    assert_eq!(
        upstream_events[0].after_state["source_event"],
        SOURCE_EVENT_NAME_WRAPPED
    );
    assert_eq!(
        upstream_events[0].after_state["decoded_name"],
        "wrapped.eth"
    );

    let old_swapped_log = watched_log(
        "ens_v1_wrapper_l1",
        2,
        vec![
            OLD_SWAPPED_NAME_WRAPPED_TOPIC0.to_owned(),
            namehash_hex_bytes(&dns_name),
        ],
        encode_name_wrapped_log_data(&dns_name),
    );
    assert!(build_test_preimage_observed_events(&old_swapped_log)?.is_empty());

    Ok(())
}

#[test]
fn ens_v2_registry_and_registrar_name_bearing_logs_emit_preimage_observations() -> Result<()> {
    let registry_log = watched_log(
        SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
        1,
        vec![
            keccak_signature_hex("LabelRegistered(uint256,bytes32,string,address,uint64,address)"),
            hex_string(&abi_word_u64(1)),
            keccak256_hex(b"alice"),
            hex_string(&abi_word_address(
                "0x00000000000000000000000000000000000000aa",
            )),
        ],
        encode_ens_v2_label_registered_data(
            "alice",
            "0x00000000000000000000000000000000000000bb",
            2_000_000_000,
        ),
    );
    let registry_events = build_test_preimage_observed_events(&registry_log)?;
    assert_eq!(registry_events.len(), 1);
    assert_eq!(
        registry_events[0].after_state["source_event"],
        SOURCE_EVENT_LABEL_REGISTERED
    );
    assert_eq!(registry_events[0].after_state["decoded_name"], "alice");
    assert_eq!(
        registry_events[0].after_state["labelhashes"][0],
        keccak256_hex(b"alice")
    );

    let parent_log = watched_log(
        SOURCE_FAMILY_ENS_V2_ROOT_L1,
        2,
        vec![
            keccak_signature_hex("ParentUpdated(address,string,address)"),
            hex_string(&abi_word_address(
                "0x00000000000000000000000000000000000000cc",
            )),
            hex_string(&abi_word_address(
                "0x00000000000000000000000000000000000000dd",
            )),
        ],
        encode_single_dynamic_string("eth"),
    );
    let parent_events = build_test_preimage_observed_events(&parent_log)?;
    assert_eq!(parent_events.len(), 1);
    assert_eq!(
        parent_events[0].after_state["source_event"],
        SOURCE_EVENT_PARENT_UPDATED
    );
    assert_eq!(parent_events[0].after_state["decoded_name"], "eth");

    let registrar_log = watched_log(
        SOURCE_FAMILY_ENS_V2_REGISTRAR_L1,
        3,
        vec![
            keccak_signature_hex(
                "NameRenewed(uint256,string,uint64,uint64,address,bytes32,uint256)",
            ),
            hex_string(&abi_word_u64(1)),
        ],
        encode_ens_v2_registrar_name_renewed_data("renewed"),
    );
    let registrar_events = build_test_preimage_observed_events(&registrar_log)?;
    assert_eq!(registrar_events.len(), 1);
    assert_eq!(
        registrar_events[0].after_state["source_event"],
        SOURCE_EVENT_NAME_RENEWED
    );
    assert_eq!(
        registrar_events[0].after_state["decoded_name"],
        "renewed.eth"
    );

    Ok(())
}

#[test]
fn oversized_label_logs_do_not_abort_preimage_observation() -> Result<()> {
    let oversized_label = "a".repeat(usize::from(u8::MAX) + 1);
    let registrar_log = watched_log(
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        4,
        RegistrarExplicitLabelEvent::NameRegistered.topics(&oversized_label),
        encode_registrar_label_log_data(&oversized_label),
    );
    assert!(build_test_preimage_observed_events(&registrar_log)?.is_empty());

    let registry_log = watched_log(
        SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
        5,
        vec![
            keccak_signature_hex("LabelRegistered(uint256,bytes32,string,address,uint64,address)"),
            hex_string(&abi_word_u64(1)),
            keccak256_hex(oversized_label.as_bytes()),
            hex_string(&abi_word_address(
                "0x00000000000000000000000000000000000000aa",
            )),
        ],
        encode_ens_v2_label_registered_data(
            &oversized_label,
            "0x00000000000000000000000000000000000000bb",
            2_000_000_000,
        ),
    );
    assert!(build_test_preimage_observed_events(&registry_log)?.is_empty());

    Ok(())
}

#[test]
fn malformed_label_payloads_do_not_abort_preimage_observation() -> Result<()> {
    let malformed_dynamic_string = abi_word_u64(96).to_vec();
    let registrar_log = watched_log(
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
        6,
        RegistrarExplicitLabelEvent::NameRegistered.topics("malformed"),
        malformed_dynamic_string.clone(),
    );
    assert!(build_test_preimage_observed_events(&registrar_log)?.is_empty());

    let registry_log = watched_log(
        SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
        7,
        vec![
            keccak_signature_hex("LabelRegistered(uint256,bytes32,string,address,uint64,address)"),
            hex_string(&abi_word_u64(1)),
            keccak256_hex(b"malformed"),
            hex_string(&abi_word_address(
                "0x00000000000000000000000000000000000000aa",
            )),
        ],
        malformed_dynamic_string,
    );
    assert!(build_test_preimage_observed_events(&registry_log)?.is_empty());

    Ok(())
}

#[test]
fn ens_v2_resolver_name_bearing_logs_emit_preimage_observations() -> Result<()> {
    let alice_dns_name = dns_encoded_name(&["alice", "eth"]);
    let bob_dns_name = dns_encoded_name(&["bob", "eth"]);
    let alias_log = watched_log(
        SOURCE_FAMILY_ENS_V2_RESOLVER_L1,
        4,
        vec![
            keccak_signature_hex("AliasChanged(bytes,bytes,bytes,bytes)"),
            keccak256_hex(&alice_dns_name),
            keccak256_hex(&bob_dns_name),
        ],
        encode_two_dynamic_bytes(&alice_dns_name, &bob_dns_name),
    );
    let alias_events = build_test_preimage_observed_events(&alias_log)?;
    let resolver_alias_events = resolver_preimage_events_for_watched_log(&alias_log)?;
    assert_eq!(resolver_alias_events, alias_events);
    assert_eq!(alias_events.len(), 2);
    assert_eq!(
        alias_events[0].after_state["source_event"],
        SOURCE_EVENT_ALIAS_CHANGED
    );
    assert_eq!(alias_events[0].after_state["observation_slot"], "from_name");
    assert_eq!(alias_events[0].after_state["decoded_name"], "alice.eth");
    assert_eq!(alias_events[1].after_state["observation_slot"], "to_name");
    assert_eq!(alias_events[1].after_state["decoded_name"], "bob.eth");
    assert_ne!(
        alias_events[0].event_identity,
        alias_events[1].event_identity
    );

    let named_cases = [
        (
            SOURCE_EVENT_NAMED_RESOURCE,
            encode_single_dynamic_bytes(&alice_dns_name),
            vec![
                keccak_signature_hex("NamedResource(uint256,bytes)"),
                hex_string(&abi_word_u64(42)),
            ],
        ),
        (
            SOURCE_EVENT_NAMED_TEXT_RESOURCE,
            encode_dynamic_bytes_and_string(&alice_dns_name, "url"),
            vec![
                keccak_signature_hex("NamedTextResource(uint256,bytes,bytes32,string)"),
                hex_string(&abi_word_u64(43)),
                keccak256_hex(b"url"),
            ],
        ),
        (
            SOURCE_EVENT_NAMED_ADDR_RESOURCE,
            encode_single_dynamic_bytes(&alice_dns_name),
            vec![
                keccak_signature_hex("NamedAddrResource(uint256,bytes,uint256)"),
                hex_string(&abi_word_u64(44)),
                hex_string(&abi_word_u64(60)),
            ],
        ),
    ];
    for (index, (source_event, data, topics)) in named_cases.into_iter().enumerate() {
        let named_log = watched_log(
            SOURCE_FAMILY_ENS_V2_RESOLVER_L1,
            10 + i64::try_from(index)?,
            topics,
            data,
        );
        let events = build_test_preimage_observed_events(&named_log)?;
        let resolver_events = resolver_preimage_events_for_watched_log(&named_log)?;
        assert_eq!(resolver_events, events);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].after_state["source_event"], source_event);
        assert_eq!(events[0].after_state["decoded_name"], "alice.eth");
    }

    Ok(())
}

fn build_test_preimage_observed_events(raw_log: &WatchedRawLogRow) -> Result<Vec<NormalizedEvent>> {
    let event_topics = test_preimage_observed_event_topics();
    build_preimage_observed_events(raw_log, &event_topics)
}

fn test_preimage_observed_event_topics() -> event_topics::PreimageObservedEventTopics {
    event_topics::PreimageObservedEventTopics::from_manifest_topic0s(HashMap::from([
        (
            test_source_manifest_id(SOURCE_FAMILY_ENS_V1_REGISTRAR_L1),
            ActiveManifestEventTopic0sBySignature::new(HashMap::from([
                (
                    ENS_V1_NAME_REGISTERED_SIGNATURE.to_owned(),
                    keccak_signature_hex("NameRegistered(string,bytes32,address,uint256,uint256)"),
                ),
                (
                    ENS_V1_NAME_RENEWED_SIGNATURE.to_owned(),
                    keccak_signature_hex("NameRenewed(string,bytes32,uint256,uint256)"),
                ),
            ])),
        ),
        (
            test_source_manifest_id(SOURCE_FAMILY_ENS_V1_WRAPPER_L1),
            ActiveManifestEventTopic0sBySignature::new(HashMap::from([(
                NAME_WRAPPED_SIGNATURE.to_owned(),
                keccak_signature_hex(UPSTREAM_NAME_WRAPPED_SIGNATURE),
            )])),
        ),
        (
            test_source_manifest_id(SOURCE_FAMILY_ENS_V2_REGISTRY_L1),
            ActiveManifestEventTopic0sBySignature::new(HashMap::from([
                (
                    LABEL_REGISTERED_SIGNATURE.to_owned(),
                    keccak_signature_hex(
                        "LabelRegistered(uint256,bytes32,string,address,uint64,address)",
                    ),
                ),
                (
                    LABEL_RESERVED_SIGNATURE.to_owned(),
                    keccak_signature_hex("LabelReserved(uint256,bytes32,string,uint64,address)"),
                ),
                (
                    PARENT_UPDATED_SIGNATURE.to_owned(),
                    keccak_signature_hex("ParentUpdated(address,string,address)"),
                ),
            ])),
        ),
        (
            test_source_manifest_id(SOURCE_FAMILY_ENS_V2_REGISTRAR_L1),
            ActiveManifestEventTopic0sBySignature::new(HashMap::from([
                (
                    ENS_V2_NAME_REGISTERED_SIGNATURE.to_owned(),
                    keccak_signature_hex(
                        "NameRegistered(uint256,string,address,address,address,uint64,address,bytes32,uint256,uint256)",
                    ),
                ),
                (
                    ENS_V2_NAME_RENEWED_SIGNATURE.to_owned(),
                    keccak_signature_hex(
                        "NameRenewed(uint256,string,uint64,uint64,address,bytes32,uint256)",
                    ),
                ),
            ])),
        ),
        (
            test_source_manifest_id(SOURCE_FAMILY_ENS_V2_RESOLVER_L1),
            ActiveManifestEventTopic0sBySignature::new(HashMap::from([
                (
                    ALIAS_CHANGED_SIGNATURE.to_owned(),
                    keccak_signature_hex("AliasChanged(bytes,bytes,bytes,bytes)"),
                ),
                (
                    NAMED_RESOURCE_SIGNATURE.to_owned(),
                    keccak_signature_hex("NamedResource(uint256,bytes)"),
                ),
                (
                    NAMED_TEXT_RESOURCE_SIGNATURE.to_owned(),
                    keccak_signature_hex("NamedTextResource(uint256,bytes,bytes32,string)"),
                ),
                (
                    NAMED_ADDR_RESOURCE_SIGNATURE.to_owned(),
                    keccak_signature_hex("NamedAddrResource(uint256,bytes,uint256)"),
                ),
            ])),
        ),
    ]))
}

fn test_source_manifest_id(source_family: &str) -> i64 {
    match source_family {
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1 => 1,
        SOURCE_FAMILY_ENS_V1_WRAPPER_L1 => 2,
        SOURCE_FAMILY_ENS_V2_ROOT_L1 | SOURCE_FAMILY_ENS_V2_REGISTRY_L1 => 3,
        SOURCE_FAMILY_ENS_V2_REGISTRAR_L1 => 4,
        SOURCE_FAMILY_ENS_V2_RESOLVER_L1 => 5,
        _ => 1,
    }
}

fn watched_log(
    source_family: &str,
    log_index: i64,
    topics: Vec<String>,
    data: Vec<u8>,
) -> WatchedRawLogRow {
    WatchedRawLogRow {
        chain_id: "ethereum-sepolia".to_owned(),
        block_hash: format!("0xblock{log_index}"),
        block_number: 100 + log_index,
        transaction_hash: format!("0xtx{log_index}"),
        transaction_index: 0,
        log_index,
        emitting_address: "0x00000000000000000000000000000000000000ee".to_owned(),
        topics,
        data,
        canonicality_state: CanonicalityState::Finalized,
        source_manifest_id: test_source_manifest_id(source_family),
        namespace: "ens".to_owned(),
        source_family: source_family.to_owned(),
        manifest_version: 1,
    }
}

fn resolver_preimage_events_for_watched_log(
    raw_log: &WatchedRawLogRow,
) -> Result<Vec<NormalizedEvent>> {
    crate::ens_v2_resolver::testsupport::build_preimage_observed_events(
        crate::ens_v2_resolver::testsupport::ResolverPreimageRawLog {
            chain_id: raw_log.chain_id.clone(),
            block_hash: raw_log.block_hash.clone(),
            block_number: raw_log.block_number,
            transaction_hash: raw_log.transaction_hash.clone(),
            transaction_index: raw_log.transaction_index,
            log_index: raw_log.log_index,
            emitting_address: raw_log.emitting_address.clone(),
            topics: raw_log.topics.clone(),
            data: raw_log.data.clone(),
            canonicality_state: raw_log.canonicality_state,
            source_manifest_id: raw_log.source_manifest_id,
            namespace: raw_log.namespace.clone(),
            source_family: raw_log.source_family.clone(),
            manifest_version: raw_log.manifest_version,
        },
    )
}

fn encode_ens_v2_label_registered_data(label: &str, owner: &str, expiry_unix: u64) -> Vec<u8> {
    encode_dynamic_string_with_prefix(label, &[abi_word_address(owner), abi_word_u64(expiry_unix)])
}

fn encode_ens_v2_registrar_name_renewed_data(label: &str) -> Vec<u8> {
    encode_dynamic_string_with_prefix(
        label,
        &[
            abi_word_u64(31_536_000),
            abi_word_u64(2_000_000_000),
            abi_word_address("0x0000000000000000000000000000000000000000"),
            [0u8; 32],
            abi_word_u64(1),
        ],
    )
}

fn encode_single_dynamic_string(value: &str) -> Vec<u8> {
    encode_dynamic_string_with_prefix(value, &[])
}

fn encode_dynamic_string_with_prefix(value: &str, fixed_words: &[[u8; 32]]) -> Vec<u8> {
    let value_bytes = value.as_bytes();
    let dynamic_offset = 32 * (fixed_words.len() + 1);
    let mut output = Vec::new();
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(dynamic_offset).expect("test ABI offset must fit in u64"),
    ));
    for word in fixed_words {
        output.extend_from_slice(word);
    }
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(value_bytes.len()).expect("test string length must fit in u64"),
    ));
    output.extend_from_slice(value_bytes);
    let padded_length = value_bytes.len().div_ceil(32) * 32;
    output.resize(dynamic_offset + 32 + padded_length, 0);
    output
}

fn encode_single_dynamic_bytes(value: &[u8]) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(&abi_word_u64(32));
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(value.len()).expect("test bytes length must fit in u64"),
    ));
    output.extend_from_slice(value);
    let padded_length = value.len().div_ceil(32) * 32;
    output.resize(64 + padded_length, 0);
    output
}

fn encode_two_dynamic_bytes(left: &[u8], right: &[u8]) -> Vec<u8> {
    let left_padded_length = left.len().div_ceil(32) * 32;
    let right_offset = 64 + 32 + left_padded_length;
    let mut output = Vec::new();
    output.extend_from_slice(&abi_word_u64(64));
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(right_offset).expect("test ABI offset must fit in u64"),
    ));
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(left.len()).expect("left bytes length must fit in u64"),
    ));
    output.extend_from_slice(left);
    output.resize(64 + 32 + left_padded_length, 0);
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(right.len()).expect("right bytes length must fit in u64"),
    ));
    output.extend_from_slice(right);
    let right_padded_length = right.len().div_ceil(32) * 32;
    output.resize(right_offset + 32 + right_padded_length, 0);
    output
}

fn encode_dynamic_bytes_and_string(bytes: &[u8], value: &str) -> Vec<u8> {
    let bytes_padded_length = bytes.len().div_ceil(32) * 32;
    let string_offset = 64 + 32 + bytes_padded_length;
    let value_bytes = value.as_bytes();
    let mut output = Vec::new();
    output.extend_from_slice(&abi_word_u64(64));
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(string_offset).expect("test ABI offset must fit in u64"),
    ));
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(bytes.len()).expect("test bytes length must fit in u64"),
    ));
    output.extend_from_slice(bytes);
    output.resize(64 + 32 + bytes_padded_length, 0);
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(value_bytes.len()).expect("test string length must fit in u64"),
    ));
    output.extend_from_slice(value_bytes);
    let string_padded_length = value_bytes.len().div_ceil(32) * 32;
    output.resize(string_offset + 32 + string_padded_length, 0);
    output
}

#[tokio::test]
async fn sync_block_derived_normalized_events_is_idempotent() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let active_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "ens",
            source_family: SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v1",
            rollout_status: "active",
            normalizer_version: "uts46-v1",
            file_path: "manifests/ens/ens_v1_wrapper_l1/1.toml",
        },
    )
    .await?;
    let inactive_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "ens",
            source_family: SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v1_shadow",
            rollout_status: "draft",
            normalizer_version: "uts46-v1",
            file_path: "manifests/ens/ens_v1_wrapper_l1/2.toml",
        },
    )
    .await?;

    let active_contract_instance_id = Uuid::new_v4();
    let inactive_contract_instance_id = Uuid::new_v4();
    insert_contract_instance(
        database.pool(),
        active_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        inactive_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;

    insert_manifest_contract_instance(
        database.pool(),
        ManifestContractInstanceSeed {
            manifest_id: active_manifest_id,
            declaration_kind: "contract",
            declaration_name: "wrapper",
            contract_instance_id: active_contract_instance_id,
            declared_address: "0x00000000000000000000000000000000000000aa",
            role: Some("wrapper"),
            proxy_kind: Some("none"),
            implementation_contract_instance_id: None,
            declared_implementation_address: None,
        },
    )
    .await?;
    insert_contract_instance_address(
        database.pool(),
        active_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
        active_manifest_id,
    )
    .await?;

    insert_manifest_contract_instance(
        database.pool(),
        ManifestContractInstanceSeed {
            manifest_id: inactive_manifest_id,
            declaration_kind: "contract",
            declaration_name: "wrapper",
            contract_instance_id: inactive_contract_instance_id,
            declared_address: "0x00000000000000000000000000000000000000bb",
            role: Some("wrapper"),
            proxy_kind: Some("none"),
            implementation_contract_instance_id: None,
            declared_implementation_address: None,
        },
    )
    .await?;
    insert_contract_instance_address(
        database.pool(),
        inactive_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000bb",
        inactive_manifest_id,
    )
    .await?;

    insert_raw_name_wrapped_log(
        database.pool(),
        "ethereum-mainnet",
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        42,
        "0x00000000000000000000000000000000000000aa",
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        "ethereum-mainnet",
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        41,
        "0x00000000000000000000000000000000000000bb",
        CanonicalityState::Canonical,
    )
    .await?;

    let first = sync_block_derived_normalized_events(
        database.pool(),
        "ethereum-mainnet",
        &[
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
        ],
        None,
    )
    .await?;
    assert_eq!(first.scanned_log_count, 2);
    assert_eq!(first.matched_log_count, 1);
    assert_eq!(first.total_synced_count, 1);
    assert_eq!(first.total_inserted_count, 1);
    assert_eq!(
        first.by_kind,
        BTreeMap::from([(
            EVENT_KIND_PREIMAGE_OBSERVED.to_owned(),
            BlockDerivedNormalizedEventKindSyncSummary {
                synced_count: 1,
                inserted_count: 1,
            }
        )])
    );

    let events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_kind, EVENT_KIND_PREIMAGE_OBSERVED);
    assert_eq!(
        events[0].derivation_kind,
        DERIVATION_KIND_RAW_LOG_PREIMAGE_OBSERVATION
    );
    assert_eq!(events[0].canonicality_state, CanonicalityState::Canonical);
    assert_eq!(events[0].source_manifest_id, Some(active_manifest_id));
    assert_eq!(events[0].after_state["decoded_name"], "wrapped.eth");

    let second = sync_block_derived_normalized_events(
        database.pool(),
        "ethereum-mainnet",
        &["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
        None,
    )
    .await?;
    assert_eq!(second.scanned_log_count, 1);
    assert_eq!(second.matched_log_count, 1);
    assert_eq!(second.total_synced_count, 1);
    assert_eq!(second.total_inserted_count, 0);

    let counts = load_normalized_event_counts_by_kind(database.pool(), "ens").await?;
    assert_eq!(
        counts,
        BTreeMap::from([(EVENT_KIND_PREIMAGE_OBSERVED.to_owned(), 1_usize)])
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_block_derived_normalized_events_replays_scoped_selected_logs_without_payload_rows()
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
            normalizer_version: "uts46-v1",
            file_path: "manifests/ens/ens_v1_registrar_l1/v1.toml",
        },
    )
    .await?;

    let selected_contract_instance_id = Uuid::new_v4();
    let unselected_contract_instance_id = Uuid::new_v4();
    insert_contract_instance(
        database.pool(),
        selected_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        unselected_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;

    insert_manifest_contract_instance(
        database.pool(),
        ManifestContractInstanceSeed {
            manifest_id,
            declaration_kind: "contract",
            declaration_name: "selected_registrar",
            contract_instance_id: selected_contract_instance_id,
            declared_address: "0x00000000000000000000000000000000000000aa",
            role: Some("registrar"),
            proxy_kind: Some("none"),
            implementation_contract_instance_id: None,
            declared_implementation_address: None,
        },
    )
    .await?;
    insert_contract_instance_address(
        database.pool(),
        selected_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
        manifest_id,
    )
    .await?;

    insert_manifest_contract_instance(
        database.pool(),
        ManifestContractInstanceSeed {
            manifest_id,
            declaration_kind: "contract",
            declaration_name: "unselected_registrar",
            contract_instance_id: unselected_contract_instance_id,
            declared_address: "0x00000000000000000000000000000000000000bb",
            role: Some("registrar"),
            proxy_kind: Some("none"),
            implementation_contract_instance_id: None,
            declared_implementation_address: None,
        },
    )
    .await?;
    insert_contract_instance_address(
        database.pool(),
        unselected_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000bb",
        manifest_id,
    )
    .await?;

    let block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    insert_raw_registrar_label_log_at_index(
        database.pool(),
        RegistrarLabelRawLogSeed {
            chain_id: "ethereum-mainnet",
            block_hash,
            block_number: 42,
            address: "0x00000000000000000000000000000000000000aa",
            label: "selected",
            source_event: RegistrarExplicitLabelEvent::NameRegistered,
            canonicality_state: CanonicalityState::Canonical,
        },
        0,
    )
    .await?;
    insert_raw_registrar_label_log_at_index(
        database.pool(),
        RegistrarLabelRawLogSeed {
            chain_id: "ethereum-mainnet",
            block_hash,
            block_number: 42,
            address: "0x00000000000000000000000000000000000000bb",
            label: "unselected",
            source_event: RegistrarExplicitLabelEvent::NameRegistered,
            canonicality_state: CanonicalityState::Canonical,
        },
        1,
    )
    .await?;

    let counts =
        load_unselected_payload_row_counts(database.pool(), "ethereum-mainnet", block_hash).await?;
    assert_eq!(counts, UnselectedPayloadRowCounts::default());

    let summary = sync_block_derived_normalized_events(
        database.pool(),
        "ethereum-mainnet",
        &[block_hash.to_owned()],
        Some(&[(
            SOURCE_FAMILY_ENS_V1_REGISTRAR_L1.to_owned(),
            "0x00000000000000000000000000000000000000aa".to_owned(),
            42,
            42,
        )]),
    )
    .await?;
    assert_eq!(summary.scanned_log_count, 2);
    assert_eq!(summary.matched_log_count, 1);
    assert_eq!(summary.total_synced_count, 1);
    assert_eq!(summary.total_inserted_count, 1);

    let events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_kind, EVENT_KIND_PREIMAGE_OBSERVED);
    assert_eq!(events[0].source_family, SOURCE_FAMILY_ENS_V1_REGISTRAR_L1);
    assert_eq!(events[0].after_state["decoded_name"], "selected.eth");
    assert_eq!(
        events[0].raw_fact_ref["emitting_address"],
        "0x00000000000000000000000000000000000000aa"
    );

    let counts =
        load_unselected_payload_row_counts(database.pool(), "ethereum-mainnet", block_hash).await?;
    assert_eq!(counts, UnselectedPayloadRowCounts::default());

    database.cleanup().await
}

#[tokio::test]
async fn sync_block_derived_normalized_events_uses_active_manifest_after_reactivation_gap()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let previous_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "ens",
            source_family: SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v0",
            rollout_status: "deprecated",
            normalizer_version: "uts46-v1",
            file_path: "manifests/ens/ens_v1_wrapper_l1/0.toml",
        },
    )
    .await?;
    let active_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 2,
            namespace: "ens",
            source_family: SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v1",
            rollout_status: "active",
            normalizer_version: "uts46-v1",
            file_path: "manifests/ens/ens_v1_wrapper_l1/1.toml",
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
            manifest_id: active_manifest_id,
            declaration_kind: "contract",
            declaration_name: "wrapper",
            contract_instance_id,
            declared_address: "0x00000000000000000000000000000000000000aa",
            role: Some("wrapper"),
            proxy_kind: Some("none"),
            implementation_contract_instance_id: None,
            declared_implementation_address: None,
        },
    )
    .await?;
    insert_contract_instance_address(
        database.pool(),
        contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
        previous_manifest_id,
    )
    .await?;
    deactivate_active_contract_instance_addresses(database.pool(), contract_instance_id).await?;
    insert_contract_instance_address(
        database.pool(),
        contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
        active_manifest_id,
    )
    .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        "ethereum-mainnet",
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        42,
        "0x00000000000000000000000000000000000000aa",
        CanonicalityState::Canonical,
    )
    .await?;

    let first = sync_block_derived_normalized_events(
        database.pool(),
        "ethereum-mainnet",
        &["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
        None,
    )
    .await?;
    assert_eq!(first.scanned_log_count, 1);
    assert_eq!(first.matched_log_count, 1);
    assert_eq!(first.total_synced_count, 1);
    assert_eq!(first.total_inserted_count, 1);

    let events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].source_manifest_id, Some(active_manifest_id));
    assert_eq!(events[0].manifest_version, 2);
    assert_eq!(
        events[0].raw_fact_ref["emitting_address"],
        "0x00000000000000000000000000000000000000aa"
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_block_derived_normalized_events_watches_proxy_implementations_but_not_migrations()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "ens",
            source_family: SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v1",
            rollout_status: "active",
            normalizer_version: "uts46-v1",
            file_path: "manifests/ens/ens_v1_wrapper_l1/1.toml",
        },
    )
    .await?;
    let proxy_contract_instance_id = Uuid::new_v4();
    let implementation_contract_instance_id = Uuid::new_v4();
    let successor_contract_instance_id = Uuid::new_v4();
    insert_contract_instance(
        database.pool(),
        proxy_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        implementation_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        successor_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;

    insert_manifest_contract_instance(
        database.pool(),
        ManifestContractInstanceSeed {
            manifest_id,
            declaration_kind: "contract",
            declaration_name: "wrapper",
            contract_instance_id: proxy_contract_instance_id,
            declared_address: "0x00000000000000000000000000000000000000aa",
            role: Some("name_wrapper"),
            proxy_kind: Some("erc1967"),
            implementation_contract_instance_id: Some(implementation_contract_instance_id),
            declared_implementation_address: Some("0x00000000000000000000000000000000000000dd"),
        },
    )
    .await?;
    insert_contract_instance_address(
        database.pool(),
        proxy_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
        manifest_id,
    )
    .await?;
    insert_contract_instance_address(
        database.pool(),
        implementation_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000dd",
        manifest_id,
    )
    .await?;
    insert_contract_instance_address(
        database.pool(),
        successor_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000ee",
        manifest_id,
    )
    .await?;
    insert_discovery_edge(
        database.pool(),
        "ethereum-mainnet",
        "proxy_implementation",
        proxy_contract_instance_id,
        implementation_contract_instance_id,
        manifest_id,
    )
    .await?;
    insert_discovery_edge(
        database.pool(),
        "ethereum-mainnet",
        "migration",
        proxy_contract_instance_id,
        successor_contract_instance_id,
        manifest_id,
    )
    .await?;

    insert_raw_name_wrapped_log(
        database.pool(),
        "ethereum-mainnet",
        "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
        43,
        "0x00000000000000000000000000000000000000dd",
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        "ethereum-mainnet",
        "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        44,
        "0x00000000000000000000000000000000000000ee",
        CanonicalityState::Canonical,
    )
    .await?;

    let summary = sync_block_derived_normalized_events(
        database.pool(),
        "ethereum-mainnet",
        &[
            "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_owned(),
            "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_owned(),
        ],
        None,
    )
    .await?;
    assert_eq!(summary.scanned_log_count, 2);
    assert_eq!(summary.matched_log_count, 1);
    assert_eq!(summary.total_synced_count, 1);
    assert_eq!(summary.total_inserted_count, 1);

    let events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].source_manifest_id, Some(manifest_id));
    assert_eq!(
        events[0].raw_fact_ref["emitting_address"],
        "0x00000000000000000000000000000000000000dd"
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_block_derived_normalized_events_skips_inactive_manifests() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "ens",
            source_family: SOURCE_FAMILY_ENS_V1_WRAPPER_L1,
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v1",
            rollout_status: "deprecated",
            normalizer_version: "uts46-v1",
            file_path: "manifests/ens/ens_v1_wrapper_l1/1.toml",
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
            declaration_name: "wrapper",
            contract_instance_id,
            declared_address: "0x00000000000000000000000000000000000000aa",
            role: Some("wrapper"),
            proxy_kind: Some("none"),
            implementation_contract_instance_id: None,
            declared_implementation_address: None,
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
    insert_raw_name_wrapped_log(
        database.pool(),
        "ethereum-mainnet",
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        42,
        "0x00000000000000000000000000000000000000aa",
        CanonicalityState::Canonical,
    )
    .await?;

    let summary = sync_block_derived_normalized_events(
        database.pool(),
        "ethereum-mainnet",
        &["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
        None,
    )
    .await?;
    assert_eq!(summary.scanned_log_count, 1);
    assert_eq!(summary.matched_log_count, 0);
    assert_eq!(summary.total_synced_count, 0);
    assert_eq!(summary.total_inserted_count, 0);
    assert!(
        load_normalized_events_by_namespace(database.pool(), "ens")
            .await?
            .is_empty()
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_block_derived_normalized_events_emits_registrar_observations_for_label_logs()
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
            normalizer_version: "uts46-v1",
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
            implementation_contract_instance_id: None,
            declared_implementation_address: None,
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

    insert_raw_registrar_label_log(
        database.pool(),
        RegistrarLabelRawLogSeed {
            chain_id: "ethereum-mainnet",
            block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            block_number: 42,
            address: "0x00000000000000000000000000000000000000aa",
            label: "registered",
            source_event: RegistrarExplicitLabelEvent::NameRegistered,
            canonicality_state: CanonicalityState::Canonical,
        },
    )
    .await?;
    insert_raw_registrar_label_log(
        database.pool(),
        RegistrarLabelRawLogSeed {
            chain_id: "ethereum-mainnet",
            block_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            block_number: 43,
            address: "0x00000000000000000000000000000000000000aa",
            label: "renewed",
            source_event: RegistrarExplicitLabelEvent::NameRenewed,
            canonicality_state: CanonicalityState::Canonical,
        },
    )
    .await?;

    let summary = sync_block_derived_normalized_events(
        database.pool(),
        "ethereum-mainnet",
        &[
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
        ],
        None,
    )
    .await?;
    assert_eq!(summary.scanned_log_count, 2);
    assert_eq!(summary.matched_log_count, 2);
    assert_eq!(summary.total_synced_count, 2);
    assert_eq!(summary.total_inserted_count, 2);
    assert_eq!(
        summary.by_kind,
        BTreeMap::from([(
            EVENT_KIND_PREIMAGE_OBSERVED.to_owned(),
            BlockDerivedNormalizedEventKindSyncSummary {
                synced_count: 2,
                inserted_count: 2,
            }
        )])
    );

    let events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].source_family, SOURCE_FAMILY_ENS_V1_REGISTRAR_L1);
    assert_eq!(events[0].source_manifest_id, Some(manifest_id));
    assert_eq!(events[0].canonicality_state, CanonicalityState::Canonical);
    assert_eq!(
        events[0].after_state["source_event"],
        SOURCE_EVENT_NAME_REGISTERED
    );
    assert_eq!(events[0].after_state["decoded_name"], "registered.eth");
    assert_eq!(
        events[1].after_state["source_event"],
        SOURCE_EVENT_NAME_RENEWED
    );
    assert_eq!(events[1].after_state["decoded_name"], "renewed.eth");

    database.cleanup().await
}

#[tokio::test]
async fn sync_block_derived_normalized_events_is_idempotent_for_registrar_label_logs() -> Result<()>
{
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
            normalizer_version: "uts46-v1",
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
            implementation_contract_instance_id: None,
            declared_implementation_address: None,
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
    insert_raw_registrar_label_log(
        database.pool(),
        RegistrarLabelRawLogSeed {
            chain_id: "ethereum-mainnet",
            block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            block_number: 42,
            address: "0x00000000000000000000000000000000000000aa",
            label: "repeat",
            source_event: RegistrarExplicitLabelEvent::NameRegistered,
            canonicality_state: CanonicalityState::Canonical,
        },
    )
    .await?;

    let first = sync_block_derived_normalized_events(
        database.pool(),
        "ethereum-mainnet",
        &["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
        None,
    )
    .await?;
    assert_eq!(first.scanned_log_count, 1);
    assert_eq!(first.matched_log_count, 1);
    assert_eq!(first.total_synced_count, 1);
    assert_eq!(first.total_inserted_count, 1);

    let second = sync_block_derived_normalized_events(
        database.pool(),
        "ethereum-mainnet",
        &["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
        None,
    )
    .await?;
    assert_eq!(second.scanned_log_count, 1);
    assert_eq!(second.matched_log_count, 1);
    assert_eq!(second.total_synced_count, 1);
    assert_eq!(second.total_inserted_count, 0);

    let counts = load_normalized_event_counts_by_kind(database.pool(), "ens").await?;
    assert_eq!(
        counts,
        BTreeMap::from([(EVENT_KIND_PREIMAGE_OBSERVED.to_owned(), 1_usize)])
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_block_derived_normalized_events_skips_orphaned_registrar_logs() -> Result<()> {
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
            normalizer_version: "uts46-v1",
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
            implementation_contract_instance_id: None,
            declared_implementation_address: None,
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

    insert_raw_registrar_label_log(
        database.pool(),
        RegistrarLabelRawLogSeed {
            chain_id: "ethereum-mainnet",
            block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            block_number: 42,
            address: "0x00000000000000000000000000000000000000aa",
            label: "canonical",
            source_event: RegistrarExplicitLabelEvent::NameRegistered,
            canonicality_state: CanonicalityState::Canonical,
        },
    )
    .await?;
    insert_raw_registrar_label_log(
        database.pool(),
        RegistrarLabelRawLogSeed {
            chain_id: "ethereum-mainnet",
            block_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            block_number: 43,
            address: "0x00000000000000000000000000000000000000aa",
            label: "orphaned",
            source_event: RegistrarExplicitLabelEvent::NameRenewed,
            canonicality_state: CanonicalityState::Orphaned,
        },
    )
    .await?;

    let summary = sync_block_derived_normalized_events(
        database.pool(),
        "ethereum-mainnet",
        &[
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
        ],
        None,
    )
    .await?;
    assert_eq!(summary.scanned_log_count, 1);
    assert_eq!(summary.matched_log_count, 1);
    assert_eq!(summary.total_synced_count, 1);
    assert_eq!(summary.total_inserted_count, 1);

    let events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].block_number, Some(42));
    assert_eq!(events[0].after_state["decoded_name"], "canonical.eth");

    database.cleanup().await
}

#[tokio::test]
async fn sync_block_derived_normalized_events_skips_inactive_and_non_registrar_label_logs()
-> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let inactive_registrar_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "ens",
            source_family: SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v1",
            rollout_status: "deprecated",
            normalizer_version: "uts46-v1",
            file_path: "manifests/ens/ens_v1_registrar_l1/v1.toml",
        },
    )
    .await?;
    let non_registrar_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "ens",
            source_family: "ens_test_wrapper",
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v1",
            rollout_status: "active",
            normalizer_version: "uts46-v1",
            file_path: "manifests/ens/ens_test_wrapper/v1.toml",
        },
    )
    .await?;
    let inactive_contract_instance_id = Uuid::new_v4();
    let non_registrar_contract_instance_id = Uuid::new_v4();
    insert_contract_instance(
        database.pool(),
        inactive_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        non_registrar_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        ManifestContractInstanceSeed {
            manifest_id: inactive_registrar_manifest_id,
            declaration_kind: "contract",
            declaration_name: "registrar",
            contract_instance_id: inactive_contract_instance_id,
            declared_address: "0x00000000000000000000000000000000000000aa",
            role: Some("registrar"),
            proxy_kind: Some("none"),
            implementation_contract_instance_id: None,
            declared_implementation_address: None,
        },
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        ManifestContractInstanceSeed {
            manifest_id: non_registrar_manifest_id,
            declaration_kind: "contract",
            declaration_name: "wrapper",
            contract_instance_id: non_registrar_contract_instance_id,
            declared_address: "0x00000000000000000000000000000000000000bb",
            role: Some("wrapper"),
            proxy_kind: Some("none"),
            implementation_contract_instance_id: None,
            declared_implementation_address: None,
        },
    )
    .await?;
    insert_contract_instance_address(
        database.pool(),
        inactive_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
        inactive_registrar_manifest_id,
    )
    .await?;
    insert_contract_instance_address(
        database.pool(),
        non_registrar_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000bb",
        non_registrar_manifest_id,
    )
    .await?;
    insert_raw_registrar_label_log(
        database.pool(),
        RegistrarLabelRawLogSeed {
            chain_id: "ethereum-mainnet",
            block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            block_number: 42,
            address: "0x00000000000000000000000000000000000000aa",
            label: "inactive",
            source_event: RegistrarExplicitLabelEvent::NameRegistered,
            canonicality_state: CanonicalityState::Canonical,
        },
    )
    .await?;
    insert_raw_registrar_label_log(
        database.pool(),
        RegistrarLabelRawLogSeed {
            chain_id: "ethereum-mainnet",
            block_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            block_number: 43,
            address: "0x00000000000000000000000000000000000000bb",
            label: "nonsource",
            source_event: RegistrarExplicitLabelEvent::NameRenewed,
            canonicality_state: CanonicalityState::Canonical,
        },
    )
    .await?;

    let summary = sync_block_derived_normalized_events(
        database.pool(),
        "ethereum-mainnet",
        &[
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
        ],
        None,
    )
    .await?;
    assert_eq!(summary.scanned_log_count, 2);
    assert_eq!(summary.matched_log_count, 0);
    assert_eq!(summary.total_synced_count, 0);
    assert_eq!(summary.total_inserted_count, 0);
    assert!(
        load_normalized_events_by_namespace(database.pool(), "ens")
            .await?
            .is_empty()
    );

    database.cleanup().await
}
