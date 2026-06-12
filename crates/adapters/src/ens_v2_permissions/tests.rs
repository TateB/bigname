use std::{
    collections::{BTreeSet, HashMap},
    path::PathBuf,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use bigname_manifests::{
    load_active_manifest_abi_events_by_chain_and_source_families, load_repository, sync_repository,
};
use bigname_storage::{
    CanonicalityState, RawBlock, RawLog, default_database_url, load_normalized_events_by_namespace,
    upsert_raw_blocks, upsert_raw_logs,
};
use serde_json::json;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::{Uuid, time::OffsetDateTime},
};

use crate::adapter_manifest::ActiveManifestEventTopic0sBySignature;
use crate::ens_v2_common::normalize_address;
use crate::evm_abi::keccak_signature_hex;

use super::constants::*;
use super::decode::build_permissions_observation;
use super::hints::{fallback_resource_hint, resolver_resource_hint};
use super::normalized::permission_changed_event;
use super::types::{PermissionsObservation, PermissionsRawLogRow};
use super::util::hex_string;

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
            .context("failed to parse database URL for ENSv2 permissions tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bn_ad_ensv2_perm_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for ENSv2 permissions tests")?;
        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect test pool for ENSv2 permissions tests")?;
        bigname_storage::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for ENSv2 permissions tests")?;

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

#[test]
fn decodes_named_text_resource_observation() -> Result<()> {
    let resource = topic_word(0x42);
    let key_hash = topic_word(0xab);
    let name = dns_name("Alice", "ETH");
    let key = b"avatar";
    let raw_log = raw_log(
        vec![
            keccak_signature_hex("NamedTextResource(uint256,bytes,bytes32,string)"),
            resource.clone(),
            key_hash.clone(),
        ],
        encode_two_dynamic(&name, key),
    );

    let event_topics = test_permissions_event_topics();
    let observation = build_permissions_observation(&raw_log, &event_topics)?;

    match observation {
        Some(PermissionsObservation::NamedTextResource {
            resource: decoded_resource,
            name: decoded_name,
            key_hash: decoded_key_hash,
            key: decoded_key,
        }) => {
            assert_eq!(decoded_resource, resource);
            assert_eq!(decoded_name, name);
            assert_eq!(decoded_key_hash, key_hash);
            assert_eq!(decoded_key, "avatar");
        }
        _ => panic!("expected NamedTextResource observation"),
    }
    Ok(())
}

#[test]
fn builds_resource_hints_for_observed_and_fallback_resources() -> Result<()> {
    let raw_log = raw_log(Vec::new(), Vec::new());
    let resource = topic_word(0x33);
    let name = dns_name("Alice", "ETH");

    let hint = resolver_resource_hint(
        &raw_log,
        resource.clone(),
        name.clone(),
        "addr",
        Some("60".to_owned()),
        None,
    )?;

    assert_eq!(hint.upstream_resource, resource);
    assert_eq!(hint.logical_name_id.as_deref(), Some("ens:alice.eth"));
    assert_eq!(hint.normalized_name.as_deref(), Some("alice.eth"));
    assert_eq!(hint.dns_encoded_name.as_deref(), Some(name.as_slice()));
    assert_eq!(hint.selector_kind, "addr");
    assert_eq!(hint.selector_key.as_deref(), Some("60"));

    let invalid_name = dns_name("Ni\u{200d}ck", "eth");
    let invalid_hint = resolver_resource_hint(
        &raw_log,
        resource.clone(),
        invalid_name.clone(),
        "addr",
        Some("60".to_owned()),
        None,
    )?;
    assert_eq!(invalid_hint.upstream_resource, resource);
    assert_eq!(invalid_hint.logical_name_id, None);
    assert_eq!(invalid_hint.normalized_name, None);
    assert_eq!(
        invalid_hint.dns_encoded_name.as_deref(),
        Some(invalid_name.as_slice())
    );

    let root = "0x0000000000000000000000000000000000000000000000000000000000000000";
    let fallback = fallback_resource_hint(&raw_log, root.to_owned(), true);
    assert_eq!(fallback.logical_name_id, None);
    assert_eq!(fallback.selector_kind, "root");

    Ok(())
}

#[test]
fn builds_permission_changed_event_payload() -> Result<()> {
    let account = "0x1111111111111111111111111111111111111111";
    let resource = topic_word(0x33);
    let name = dns_name("alice", "eth");
    let raw_log = raw_log(
        vec![
            keccak_signature_hex("EACRolesChanged(uint256,address,uint256,uint256)"),
            resource.clone(),
            address_topic(account),
        ],
        [hex_word(0), bitmap_with_last_byte(0x11)]
            .into_iter()
            .flatten()
            .collect(),
    );
    let hint =
        resolver_resource_hint(&raw_log, resource.clone(), name.clone(), "name", None, None)?;
    let resource_id = Uuid::parse_str("11111111-1111-5111-8111-111111111111")?;
    let event = permission_changed_event(
        &raw_log,
        &hint,
        resource_id,
        account.to_owned(),
        topic_word(0),
        bitmap_hex_with_last_byte(0x11),
    )?;

    assert_eq!(
        event.event_identity,
        format!("ens_v2_permissions:42:0xblock:0xtx:9:{EVENT_KIND_PERMISSION_CHANGED}:{resource}")
    );
    assert_eq!(event.namespace, "ens");
    assert_eq!(event.logical_name_id.as_deref(), Some("ens:alice.eth"));
    assert_eq!(event.resource_id, Some(resource_id));
    assert_eq!(event.event_kind, EVENT_KIND_PERMISSION_CHANGED);
    assert_eq!(event.source_family, "ens_v2_resolver_l1");
    assert_eq!(event.manifest_version, 7);
    assert_eq!(event.source_manifest_id, Some(42));
    assert_eq!(event.chain_id.as_deref(), Some("sepolia-dev"));
    assert_eq!(event.block_number, Some(123));
    assert_eq!(event.block_hash.as_deref(), Some("0xblock"));
    assert_eq!(event.transaction_hash.as_deref(), Some("0xtx"));
    assert_eq!(event.log_index, Some(9));
    assert_eq!(event.derivation_kind, "ens_v2_permissions");
    assert_eq!(event.canonicality_state, CanonicalityState::Canonical);
    assert_eq!(
        event.raw_fact_ref,
        json!({
            "kind": "raw_log",
            "chain_id": "sepolia-dev",
            "block_hash": "0xblock",
            "block_number": 123,
            "transaction_hash": "0xtx",
            "transaction_index": 4,
            "log_index": 9,
            "emitting_address": "0x2222222222222222222222222222222222222222",
        })
    );
    assert_eq!(
        event.before_state,
        json!({
            "subject": account,
            "role_bitmap": topic_word(0),
            "effective_powers": [],
        })
    );
    assert_eq!(event.after_state["subject"], json!(account));
    assert_eq!(
        event.after_state["scope"],
        json!({
            "kind": "resolver",
            "chain_id": "sepolia-dev",
            "resolver_address": "0x2222222222222222222222222222222222222222",
        })
    );
    assert_eq!(
        event.after_state["effective_powers"],
        json!(["set_addr", "set_text"])
    );
    assert_eq!(
        event.after_state["grant_source"]["source_event"],
        "EACRolesChanged"
    );
    assert_eq!(
        event.after_state["grant_source"]["upstream_resource"],
        resource
    );
    assert_eq!(event.after_state["grant_source"]["root_resource"], false);
    assert_eq!(
        event.after_state["grant_source"]["changed_powers"],
        json!(["set_addr", "set_text"])
    );
    assert_eq!(event.after_state["revocation_source"], json!(null));
    assert_eq!(event.after_state["inheritance_path"], json!([]));
    assert_eq!(
        event.after_state["selector"],
        json!({
            "kind": "name",
            "key": null,
            "hash": null,
            "normalized_name": "alice.eth",
            "dns_encoded_name": format!("0x{}", hex_string(&name)),
        })
    );

    Ok(())
}

#[test]
fn registry_root_resource_builds_root_permission_changed_event_payload() -> Result<()> {
    let account = "0x1111111111111111111111111111111111111111";
    let root_resource = topic_word(0);
    let raw_log = raw_log_with_source_family(
        "ens_v2_registry_l1",
        vec![
            keccak_signature_hex("EACRolesChanged(uint256,address,uint256,uint256)"),
            root_resource.clone(),
            address_topic(account),
        ],
        [hex_word(0), bitmap_with_last_byte(0x11)]
            .into_iter()
            .flatten()
            .collect(),
    );
    let hint = fallback_resource_hint(&raw_log, root_resource.clone(), true);
    let resource_id = Uuid::parse_str("11111111-1111-5111-8111-111111111111")?;
    let event = permission_changed_event(
        &raw_log,
        &hint,
        resource_id,
        account.to_owned(),
        topic_word(0),
        bitmap_hex_with_last_byte(0x11),
    )?;

    assert_eq!(event.event_kind, "RootPermissionChanged");
    assert_eq!(
        event.event_identity,
        format!("ens_v2_permissions:42:0xblock:0xtx:9:RootPermissionChanged:{root_resource}")
    );
    assert_eq!(
        event.after_state["scope"],
        json!({
            "kind": "registry_root",
            "chain_id": "sepolia-dev",
            "registry_address": "0x2222222222222222222222222222222222222222",
        })
    );
    assert_eq!(
        event.after_state["grant_source"]["registry_contract_instance_id"],
        raw_log.emitting_contract_instance_id.to_string()
    );
    assert_eq!(event.after_state["grant_source"]["root_resource"], true);
    assert_eq!(
        event.after_state["inheritance_path"][0]["kind"],
        "registry_root_fallback"
    );

    Ok(())
}

#[tokio::test]
async fn sync_ens_v2_permissions_consumes_registry_root_eac_roles_changed() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let root_address = "0x3a3e15a5d27ff6f05c844313312f2e72096d3ed3";
    let registry_address = "0x796fff2e907449be8d5921bcc215b1b76d89d080";
    let account = "0x1111111111111111111111111111111111111111";

    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &repository).await?;
    assert_registry_and_root_eac_roles_changed_are_admitted(database.pool(), chain).await?;

    upsert_raw_blocks(
        database.pool(),
        &[permissions_test_raw_block(chain, block_hash, 10_462_900)],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: chain.to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 10_462_900,
                transaction_hash: "0xtx420".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: normalize_address(root_address),
                topics: vec![
                    keccak_signature_hex(ABI_EVENT_EAC_ROLES_CHANGED_SIGNATURE),
                    topic_word(0),
                    address_topic(account),
                ],
                data: [hex_word(0), bitmap_with_last_byte(0x11)]
                    .into_iter()
                    .flatten()
                    .collect(),
                canonicality_state: CanonicalityState::Finalized,
            },
            RawLog {
                chain_id: chain.to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 10_462_900,
                transaction_hash: "0xtx420".to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: normalize_address(registry_address),
                topics: vec![
                    keccak_signature_hex(ABI_EVENT_EAC_ROLES_CHANGED_SIGNATURE),
                    topic_word(0),
                    address_topic(account),
                ],
                data: [hex_word(0), bitmap_with_last_byte(0x11)]
                    .into_iter()
                    .flatten()
                    .collect(),
                canonicality_state: CanonicalityState::Finalized,
            },
        ],
    )
    .await?;

    let summary = super::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
        database.pool(),
        chain,
        &[block_hash.to_owned()],
    )
    .await?;

    assert_eq!(summary.scanned_log_count, 2);
    assert_eq!(summary.matched_log_count, 2);
    assert_eq!(summary.total_synced_count, 2);
    assert_eq!(
        summary
            .by_kind
            .get(EVENT_KIND_ROOT_PERMISSION_CHANGED)
            .map(|entry| entry.synced_count),
        Some(2)
    );

    let events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
    assert_eq!(events.len(), 2);
    assert!(
        events
            .iter()
            .all(|event| event.event_kind == EVENT_KIND_ROOT_PERMISSION_CHANGED)
    );
    assert_eq!(
        events
            .iter()
            .map(|event| event.source_family.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            SOURCE_FAMILY_ENS_V2_ROOT_L1,
            SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
        ])
    );
    assert!(
        events
            .iter()
            .all(|event| event.after_state["scope"]["kind"] == "registry_root")
    );
    assert!(events.iter().all(|event| {
        event.after_state["registry_contract_instance_id"]
            .as_str()
            .is_some()
    }));

    database.cleanup().await
}

async fn assert_registry_and_root_eac_roles_changed_are_admitted(
    pool: &PgPool,
    chain: &str,
) -> Result<()> {
    let source_families = vec![
        SOURCE_FAMILY_ENS_V2_ROOT_L1.to_owned(),
        SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
    ];
    let admitted =
        load_active_manifest_abi_events_by_chain_and_source_families(pool, chain, &source_families)
            .await?
            .into_iter()
            .filter(|event| event.canonical_signature == ABI_EVENT_EAC_ROLES_CHANGED_SIGNATURE)
            .filter(|event| event.topic0.is_some())
            .map(|event| event.source_family)
            .collect::<BTreeSet<_>>();

    assert_eq!(
        admitted,
        BTreeSet::from([
            SOURCE_FAMILY_ENS_V2_ROOT_L1.to_owned(),
            SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
        ])
    );
    Ok(())
}

fn checked_in_manifest_root(profile_root: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(profile_root)
}

fn permissions_test_raw_block(chain: &str, block_hash: &str, block_number: i64) -> RawBlock {
    RawBlock {
        chain_id: chain.to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: None,
        block_number,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_717_172_700 + block_number)
            .expect("test timestamp should fit"),
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn raw_log(topics: Vec<String>, data: Vec<u8>) -> PermissionsRawLogRow {
    raw_log_with_source_family("ens_v2_resolver_l1", topics, data)
}

fn raw_log_with_source_family(
    source_family: &str,
    topics: Vec<String>,
    data: Vec<u8>,
) -> PermissionsRawLogRow {
    PermissionsRawLogRow {
        chain_id: "sepolia-dev".to_owned(),
        block_hash: "0xblock".to_owned(),
        block_number: 123,
        transaction_hash: "0xtx".to_owned(),
        transaction_index: 4,
        log_index: 9,
        emitting_address: "0x2222222222222222222222222222222222222222".to_owned(),
        emitting_contract_instance_id: Uuid::parse_str("22222222-2222-5222-8222-222222222222")
            .expect("test uuid should parse"),
        topics,
        data,
        canonicality_state: CanonicalityState::Canonical,
        source_manifest_id: 42,
        namespace: "ens".to_owned(),
        source_family: source_family.to_owned(),
        manifest_version: 7,
    }
}

fn dns_name(first: &str, second: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.push(first.len() as u8);
    bytes.extend_from_slice(first.as_bytes());
    bytes.push(second.len() as u8);
    bytes.extend_from_slice(second.as_bytes());
    bytes.push(0);
    bytes
}

fn encode_two_dynamic(first: &[u8], second: &[u8]) -> Vec<u8> {
    let first_tail = dynamic_tail(first);
    let second_tail = dynamic_tail(second);
    let second_offset = 64 + first_tail.len();
    let mut data = Vec::new();
    data.extend_from_slice(&hex_word(64));
    data.extend_from_slice(&hex_word(second_offset as u64));
    data.extend_from_slice(&first_tail);
    data.extend_from_slice(&second_tail);
    data
}

fn dynamic_tail(bytes: &[u8]) -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(&hex_word(bytes.len() as u64));
    data.extend_from_slice(bytes);
    let padding = (32 - (bytes.len() % 32)) % 32;
    data.extend(std::iter::repeat_n(0, padding));
    data
}

fn address_topic(address: &str) -> String {
    format!("0x{:0>64}", address.trim_start_matches("0x"))
}

fn topic_word(value: u64) -> String {
    format!("0x{value:064x}")
}

fn hex_word(value: u64) -> Vec<u8> {
    let mut bytes = [0u8; 32];
    bytes[24..].copy_from_slice(&value.to_be_bytes());
    bytes.to_vec()
}

fn bitmap_with_last_byte(value: u8) -> Vec<u8> {
    let mut bytes = [0u8; 32];
    bytes[31] = value;
    bytes.to_vec()
}

fn bitmap_hex_with_last_byte(value: u8) -> String {
    format!("0x{}", hex_string(bitmap_with_last_byte(value)))
}

fn test_permissions_event_topics() -> ActiveManifestEventTopic0sBySignature {
    ActiveManifestEventTopic0sBySignature::new(HashMap::from([
        (
            ABI_EVENT_NAMED_RESOURCE_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_NAMED_RESOURCE_SIGNATURE),
        ),
        (
            ABI_EVENT_NAMED_TEXT_RESOURCE_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_NAMED_TEXT_RESOURCE_SIGNATURE),
        ),
        (
            ABI_EVENT_NAMED_ADDR_RESOURCE_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_NAMED_ADDR_RESOURCE_SIGNATURE),
        ),
        (
            ABI_EVENT_EAC_ROLES_CHANGED_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_EAC_ROLES_CHANGED_SIGNATURE),
        ),
    ]))
}
