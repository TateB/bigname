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
        let database_name = format!(
            "bigname_adapters_ens_v1_unwrapped_authority_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

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
    .bind("{}")
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
            normalizer_version: "uts46-v1",
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

fn encode_registrar_name_registered_log_data(label: &str, expiry_unix: i64) -> Vec<u8> {
    let label_bytes = label.as_bytes();
    let mut output = Vec::new();

    output.extend_from_slice(&abi_word_u64(96));
    output.extend_from_slice(&abi_word_u64(1));
    output.extend_from_slice(&abi_word_u64(expiry_unix as u64));
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(label_bytes.len()).expect("test label length must fit in u64"),
    ));
    output.extend_from_slice(label_bytes);

    let padded_length = label_bytes.len().div_ceil(32) * 32;
    output.resize(32 * 4 + padded_length, 0);
    output
}

fn encode_registry_new_resolver_log_data(resolver: &str) -> Vec<u8> {
    abi_word_address(resolver).to_vec()
}

fn encode_dynamic_string_log_data(value: &str) -> Vec<u8> {
    let value_bytes = value.as_bytes();
    let mut output = Vec::new();
    output.extend_from_slice(&abi_word_u64(32));
    output.extend_from_slice(&abi_word_u64(
        u64::try_from(value_bytes.len()).expect("test string length must fit in u64"),
    ));
    output.extend_from_slice(value_bytes);
    let padded_length = value_bytes.len().div_ceil(32) * 32;
    output.resize(64 + padded_length, 0);
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
            "source_event": "ReverseClaimed",
            "address": claimed_address,
            "coin_type": ENS_NATIVE_COIN_TYPE,
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
    }
}

#[test]
fn build_authority_observation_decodes_resolver_record_logs() -> Result<()> {
    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let resolver_address = "0x00000000000000000000000000000000000000cc";

    let text_observation = build_authority_observation(&resolver_raw_log(
        resolver_address,
        vec![
            text_changed_topic0(),
            alice.namehash.clone(),
            keccak256_hex(b"com.twitter"),
        ],
        encode_dynamic_string_log_data("com.twitter"),
        0,
    ))?
    .context("TextChanged observation should decode")?;
    assert_eq!(
        text_observation,
        AuthorityObservation::RecordChanged(RecordChangeObservation {
            namehash: alice.namehash.clone(),
            resolver: resolver_address.to_owned(),
            selector: RecordSelector {
                record_key: "text".to_owned(),
                record_family: "text".to_owned(),
                selector_key: None,
            },
            raw_name: None,
            reference: resolver_raw_log(resolver_address, Vec::new(), Vec::new(), 0).reference(),
        })
    );

    let name_observation = build_authority_observation(&resolver_raw_log(
        resolver_address,
        vec![name_changed_topic0(), alice.namehash.clone()],
        encode_dynamic_string_log_data("alice.eth"),
        1,
    ))?
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
            raw_name: Some("alice.eth".to_owned()),
            reference: resolver_raw_log(resolver_address, Vec::new(), Vec::new(), 1).reference(),
        })
    );

    let addr_observation = build_authority_observation(&resolver_raw_log(
        resolver_address,
        vec![addr_changed_topic0(), alice.namehash.clone()],
        encode_resolver_addr_changed_log_data("0x00000000000000000000000000000000000000aa"),
        2,
    ))?
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
            raw_name: None,
            reference: resolver_raw_log(resolver_address, Vec::new(), Vec::new(), 2).reference(),
        })
    );

    let multicoin_addr_observation = build_authority_observation(&resolver_raw_log(
        resolver_address,
        vec![address_changed_topic0(), alice.namehash.clone()],
        encode_resolver_address_changed_log_data(61, &[0xde, 0xad, 0xbe, 0xef]),
        3,
    ))?
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
            raw_name: None,
            reference: resolver_raw_log(resolver_address, Vec::new(), Vec::new(), 3).reference(),
        })
    );

    let record_version_observation = build_authority_observation(&resolver_raw_log(
        resolver_address,
        vec![version_changed_topic0(), alice.namehash.clone()],
        encode_resolver_version_changed_log_data(7),
        4,
    ))?
    .context("VersionChanged observation should decode")?;
    assert_eq!(
        record_version_observation,
        AuthorityObservation::RecordVersionChanged(RecordVersionObservation {
            namehash: alice.namehash,
            resolver: resolver_address.to_owned(),
            record_version: 7,
            reference: resolver_raw_log(resolver_address, Vec::new(), Vec::new(), 4).reference(),
        })
    );

    Ok(())
}

#[test]
fn build_authority_observation_decodes_wrapper_logs() -> Result<()> {
    let alice = observe_registrar_eth_name_with_version("alice", ENS_NORMALIZER_VERSION)?;
    let dns_name = dns_encoded_name(&["alice", "eth"]);
    let owner = "0x0000000000000000000000000000000000000001";

    let wrapped_observation = build_authority_observation(&wrapper_raw_log(
        vec![name_wrapped_topic0(), alice.namehash.clone()],
        encode_name_wrapped_log_data(&dns_name, owner, 8, 1_800_000_000),
        0,
    ))?
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

    let unwrapped_observation = build_authority_observation(&wrapper_raw_log(
        vec![name_unwrapped_topic0(), alice.namehash.clone()],
        encode_name_unwrapped_log_data("0x0000000000000000000000000000000000000002"),
        1,
    ))?
    .context("NameUnwrapped observation should decode")?;
    assert_eq!(
        unwrapped_observation,
        AuthorityObservation::WrapperNameUnwrapped(WrapperNameUnwrappedObservation {
            namehash: alice.namehash.clone(),
            owner: "0x0000000000000000000000000000000000000002".to_owned(),
            reference: wrapper_raw_log(Vec::new(), Vec::new(), 1).reference(),
        })
    );

    let fuses_observation = build_authority_observation(&wrapper_raw_log(
        vec![fuses_set_topic0(), alice.namehash.clone()],
        encode_fuses_set_log_data(10),
        2,
    ))?
    .context("FusesSet observation should decode")?;
    assert_eq!(
        fuses_observation,
        AuthorityObservation::WrapperFusesSet(WrapperFusesObservation {
            namehash: alice.namehash.clone(),
            fuses: 10,
            reference: wrapper_raw_log(Vec::new(), Vec::new(), 2).reference(),
        })
    );

    let expiry_observation = build_authority_observation(&wrapper_raw_log(
        vec![expiry_extended_topic0(), alice.namehash.clone()],
        encode_expiry_extended_log_data(1_800_000_100),
        3,
    ))?
    .context("ExpiryExtended observation should decode")?;
    assert_eq!(
        expiry_observation,
        AuthorityObservation::WrapperExpiryExtended(WrapperExpiryObservation {
            namehash: alice.namehash.clone(),
            expiry: OffsetDateTime::from_unix_timestamp(1_800_000_100)?,
            reference: wrapper_raw_log(Vec::new(), Vec::new(), 3).reference(),
        })
    );

    let transfer_observation = build_authority_observation(&wrapper_raw_log(
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
    ))?
    .context("TransferSingle observation should decode")?;
    assert_eq!(
        transfer_observation,
        AuthorityObservation::WrapperTokenTransferred(WrapperTokenTransferObservation {
            namehash: alice.namehash,
            from_address: "0x0000000000000000000000000000000000000001".to_owned(),
            to_address: "0x0000000000000000000000000000000000000002".to_owned(),
            value: 1,
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

    let first = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
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
            normalizer_version: "uts46-v1",
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
            normalizer_version: "uts46-v1",
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
        1
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
async fn sync_ens_v1_unwrapped_authority_new_resolver_discovery_edge_respects_effective_range()
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
            normalizer_version: "uts46-v1",
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
            normalizer_version: "uts46-v1",
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
            normalizer_version: "uts46-v1",
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
    assert_eq!(summary.scanned_log_count, 4);
    assert_eq!(summary.matched_log_count, 4);
    assert_eq!(summary.total_normalized_event_count, 4);
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_RESOLVER_CHANGED),
        Some(&1_usize)
    );
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_RECORD_CHANGED),
        Some(&2_usize)
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
        vec!["alice.eth".to_owned(), "reopened.eth".to_owned()]
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
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v1_unwrapped_authority_gates_discovered_ensv1_resolver_local_facts_by_profile()
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
            normalizer_version: "uts46-v1",
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
            normalizer_version: "uts46-v1",
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
            normalizer_version: "uts46-v1",
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
        ],
    )
    .await?;

    let summary = sync_ens_v1_unwrapped_authority(database.pool(), "ethereum-mainnet").await?;
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
            "SELECT ARRAY_AGG(after_state->>'resolver' ORDER BY log_index) FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        vec![
            supported_resolver_address.to_owned(),
            pending_resolver_address.to_owned(),
            unsupported_resolver_address.to_owned(),
        ]
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'raw_name' FROM normalized_events WHERE event_kind = 'RecordChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "supported.eth".to_owned()
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
                data: encode_dynamic_string_log_data("com.twitter"),
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
            vec!["text".to_owned(), "addr:60".to_owned()]
        );
    assert_eq!(
            sqlx::query_scalar::<_, Vec<Option<String>>>(
                "SELECT ARRAY_AGG(after_state->>'selector_key' ORDER BY log_index) FROM normalized_events WHERE event_kind = 'RecordChanged'"
            )
            .fetch_one(database.pool())
            .await?,
            vec![None, Some("60".to_owned())]
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
                    text_changed_topic0(),
                    alice.namehash.clone(),
                    keccak256_hex(b"com.twitter"),
                ],
                data: encode_dynamic_string_log_data("com.twitter"),
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
                    text_changed_topic0(),
                    alice.namehash.clone(),
                    keccak256_hex(b"com.github"),
                ],
                data: encode_dynamic_string_log_data("com.github"),
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
            normalizer_version: "uts46-v1",
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
            normalizer_version: "uts46-v1",
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
            normalizer_version: "uts46-v1",
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
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'raw_name' FROM normalized_events WHERE event_kind = 'RecordChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "supported.base.eth".to_owned()
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
    let reverse_node = reverse_node_for_address(claimed_address);
    let reverse_name = format!(
        "{}.addr.reverse",
        reverse_label_for_address(claimed_address)
    );

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
    upsert_normalized_events(
        database.pool(),
        &[basenames_reverse_claim_event(
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
                data: encode_dynamic_string_log_data("com.twitter"),
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
                data: encode_dynamic_string_log_data("com.github"),
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
            normalizer_version: "uts46-v1",
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
            normalizer_version: "uts46-v1",
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
            4
        );
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PermissionChanged' AND resource_id = $1"
            )
            .bind(registry_resource_id)
            .fetch_one(database.pool())
            .await?,
            2
        );
    assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PermissionChanged' AND resource_id = $1 AND block_number = 44"
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
