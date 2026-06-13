use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use bigname_storage::{
    CanonicalityState, NormalizedEvent, PrimaryNameClaimStatus,
    VERIFIED_PRIMARY_NAME_INVALIDATION_KEY, VERIFIED_PRIMARY_NAME_LOOKUP_KEY, default_database_url,
};
use serde_json::{Map, Value, json};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

pub(super) const ENS_NAMESPACE: &str = "ens";
pub(super) const BASENAMES_NAMESPACE: &str = "basenames";
pub(super) const BASE_COIN_TYPE: &str = "2147492101";
const EVENT_KIND_REVERSE_CHANGED: &str = "ReverseChanged";

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

pub(super) struct TestDatabase {
    admin_pool: PgPool,
    pool: PgPool,
    database_name: String,
}

impl TestDatabase {
    pub(super) async fn new() -> Result<Self> {
        Self::new_with_max_connections(5).await
    }

    pub(super) async fn new_with_max_connections(max_connections: u32) -> Result<Self> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for worker primary_names_current tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!("bn_wpn_{}_{}_{}", std::process::id(), unique, sequence);

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for worker primary_names_current tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect worker primary_names_current test pool")?;

        bigname_storage::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for worker primary_names_current tests")?;

        Ok(Self {
            admin_pool,
            pool,
            database_name,
        })
    }

    pub(super) fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub(super) async fn cleanup(self) -> Result<()> {
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

pub(super) fn reverse_changed_event(
    event_identity: &str,
    address: &str,
    coin_type: &str,
    block_number: i64,
    log_index: i64,
    canonicality_state: CanonicalityState,
) -> NormalizedEvent {
    let normalized_address = address.to_ascii_lowercase();
    let reverse_label = normalized_address.trim_start_matches("0x").to_owned();

    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: ENS_NAMESPACE.to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_REVERSE_CHANGED.to_owned(),
        source_family: "ens_v1_reverse_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("ethereum-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xblock{block_number:064x}")),
        transaction_hash: Some(format!("0xtx{block_number:064x}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "ethereum-mainnet",
            "block_number": block_number,
            "log_index": log_index,
        }),
        derivation_kind: "ens_v1_reverse_claim".to_owned(),
        canonicality_state,
        before_state: json!({}),
        after_state: json!({
            "source_event": "ReverseClaimed",
            "address": normalized_address,
            "coin_type": coin_type,
            "namespace": ENS_NAMESPACE,
            "reverse_namespace": ENS_NAMESPACE,
            "reverse_label": reverse_label,
            "reverse_name": format!("{reverse_label}.addr.reverse"),
            "reverse_node": format!("0x{block_number:064x}"),
            "claim_provenance": {
                "source_family": "ens_v1_reverse_l1",
                "contract_role": "reverse_registrar",
                "contract_instance_id": format!("00000000-0000-0000-0000-{block_number:012x}"),
                "emitting_address": "0x00000000000000000000000000000000000000ad",
            },
        }),
    }
}

pub(super) fn reverse_linked_name_event(
    event_identity: &str,
    address: &str,
    coin_type: &str,
    raw_name: Option<&str>,
    block_number: i64,
    log_index: i64,
    canonicality_state: CanonicalityState,
) -> NormalizedEvent {
    let normalized_address = address.to_ascii_lowercase();
    let reverse_label = normalized_address.trim_start_matches("0x").to_owned();
    let mut after_state = serde_json::Map::from_iter([
        ("record_key".to_owned(), json!("name")),
        ("record_family".to_owned(), json!("name")),
        ("selector_key".to_owned(), Value::Null),
        (
            "primary_claim_source".to_owned(),
            json!({
                "address": normalized_address,
                "namespace": ENS_NAMESPACE,
                "coin_type": coin_type,
                "reverse_name": format!("{reverse_label}.addr.reverse"),
                "reverse_node": format!("0x{block_number:064x}"),
                "claim_provenance": {
                    "source_family": "ens_v1_reverse_l1",
                    "contract_role": "reverse_registrar",
                    "contract_instance_id": format!("00000000-0000-0000-0000-{block_number:012x}"),
                    "emitting_address": "0x00000000000000000000000000000000000000ad",
                },
            }),
        ),
    ]);
    if let Some(raw_name) = raw_name {
        after_state.insert("raw_name".to_owned(), json!(raw_name));
    }

    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: ENS_NAMESPACE.to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: "RecordChanged".to_owned(),
        source_family: "ens_v1_unwrapped_authority".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("ethereum-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xclaimblock{block_number:064x}")),
        transaction_hash: Some(format!("0xclaimtx{block_number:064x}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "ethereum-mainnet",
            "block_number": block_number,
            "log_index": log_index,
        }),
        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
        canonicality_state,
        before_state: json!({}),
        after_state: Value::Object(after_state),
    }
}

pub(super) fn basenames_reverse_changed_event(
    event_identity: &str,
    address: &str,
    coin_type: &str,
    block_number: i64,
    log_index: i64,
    canonicality_state: CanonicalityState,
) -> NormalizedEvent {
    let normalized_address = address.to_ascii_lowercase();
    let reverse_label = normalized_address.trim_start_matches("0x").to_owned();

    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: BASENAMES_NAMESPACE.to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_REVERSE_CHANGED.to_owned(),
        source_family: "basenames_base_primary".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("base-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xbaseblock{block_number:064x}")),
        transaction_hash: Some(format!("0xbasetx{block_number:064x}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "base-mainnet",
            "block_number": block_number,
            "log_index": log_index,
        }),
        derivation_kind: "ens_v1_reverse_claim".to_owned(),
        canonicality_state,
        before_state: json!({}),
        after_state: json!({
            "source_event": "NameForAddrChanged",
            "address": normalized_address,
            "coin_type": coin_type,
            "namespace": BASENAMES_NAMESPACE,
            "reverse_namespace": BASENAMES_NAMESPACE,
            "reverse_label": reverse_label,
            "reverse_name": format!("{reverse_label}.80002105.reverse"),
            "reverse_node": format!("0x{block_number:064x}"),
            "claim_provenance": {
                "source_family": "basenames_base_primary",
                "contract_role": "reverse_registrar",
                "contract_instance_id": format!("00000000-0000-0000-0000-{block_number:012x}"),
                "emitting_address": "0x00000000000000000000000000000000000000ad",
            },
        }),
    }
}

pub(super) fn basenames_reverse_linked_name_event(
    event_identity: &str,
    address: &str,
    coin_type: &str,
    raw_name: Option<&str>,
    block_number: i64,
    log_index: i64,
    canonicality_state: CanonicalityState,
) -> NormalizedEvent {
    let normalized_address = address.to_ascii_lowercase();
    let reverse_label = normalized_address.trim_start_matches("0x").to_owned();
    let mut after_state = serde_json::Map::from_iter([
        ("record_key".to_owned(), json!("name")),
        ("record_family".to_owned(), json!("name")),
        ("selector_key".to_owned(), Value::Null),
        (
            "primary_claim_source".to_owned(),
            json!({
                "address": normalized_address,
                "namespace": BASENAMES_NAMESPACE,
                "coin_type": coin_type,
                "reverse_name": format!("{reverse_label}.80002105.reverse"),
                "reverse_node": format!("0x{block_number:064x}"),
                "claim_provenance": {
                    "source_family": "basenames_base_primary",
                    "contract_role": "reverse_registrar",
                    "contract_instance_id": format!("00000000-0000-0000-0000-{block_number:012x}"),
                    "emitting_address": "0x00000000000000000000000000000000000000ad",
                },
            }),
        ),
    ]);
    if let Some(raw_name) = raw_name {
        after_state.insert("raw_name".to_owned(), json!(raw_name));
    }

    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: BASENAMES_NAMESPACE.to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: "RecordChanged".to_owned(),
        source_family: "basenames_base_resolver".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("base-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xbaseclaimblock{block_number:064x}")),
        transaction_hash: Some(format!("0xbaseclaimtx{block_number:064x}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "base-mainnet",
            "block_number": block_number,
            "log_index": log_index,
        }),
        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
        canonicality_state,
        before_state: json!({}),
        after_state: Value::Object(after_state),
    }
}

pub(super) fn expected_claim_provenance(
    address: &str,
    coin_type: &str,
    reverse_block_number: i64,
    claim_status: PrimaryNameClaimStatus,
    primary_claim_block_number: Option<i64>,
) -> Value {
    expected_claim_provenance_for_namespace(
        address,
        ENS_NAMESPACE,
        "ens_v1_reverse_l1",
        coin_type,
        reverse_block_number,
        claim_status,
        primary_claim_block_number,
    )
}

pub(super) fn basenames_expected_claim_provenance(
    address: &str,
    coin_type: &str,
    reverse_block_number: i64,
    claim_status: PrimaryNameClaimStatus,
    primary_claim_block_number: Option<i64>,
) -> Value {
    expected_claim_provenance_for_namespace(
        address,
        BASENAMES_NAMESPACE,
        "basenames_base_primary",
        coin_type,
        reverse_block_number,
        claim_status,
        primary_claim_block_number,
    )
}

fn expected_claim_provenance_for_namespace(
    address: &str,
    namespace: &str,
    source_family: &str,
    coin_type: &str,
    reverse_block_number: i64,
    claim_status: PrimaryNameClaimStatus,
    primary_claim_block_number: Option<i64>,
) -> Value {
    let normalized_address = address.to_ascii_lowercase();
    let reverse_label = normalized_address.trim_start_matches("0x").to_owned();
    let mut claim_provenance = Map::from_iter([
        ("source_family".to_owned(), json!(source_family)),
        ("contract_role".to_owned(), json!("reverse_registrar")),
        (
            "contract_instance_id".to_owned(),
            json!(format!(
                "00000000-0000-0000-0000-{reverse_block_number:012x}"
            )),
        ),
        (
            "emitting_address".to_owned(),
            json!("0x00000000000000000000000000000000000000ad"),
        ),
        (
            VERIFIED_PRIMARY_NAME_LOOKUP_KEY.to_owned(),
            json!({
                "address": normalized_address.clone(),
                "namespace": namespace,
                "coin_type": coin_type,
            }),
        ),
    ]);
    let mut invalidation =
        Map::from_iter([("claim_status".to_owned(), json!(claim_status.as_str()))]);
    if let Some(primary_claim_block_number) = primary_claim_block_number {
        let reverse_name = if namespace == BASENAMES_NAMESPACE {
            format!("{reverse_label}.80002105.reverse")
        } else {
            format!("{reverse_label}.addr.reverse")
        };
        invalidation.insert(
            "primary_claim_source".to_owned(),
            json!({
                "address": normalized_address.clone(),
                "namespace": namespace,
                "coin_type": coin_type,
                "reverse_name": reverse_name,
                "reverse_node": format!("0x{primary_claim_block_number:064x}"),
                "claim_provenance": {
                    "source_family": source_family,
                    "contract_role": "reverse_registrar",
                    "contract_instance_id": format!(
                        "00000000-0000-0000-0000-{primary_claim_block_number:012x}"
                    ),
                    "emitting_address": "0x00000000000000000000000000000000000000ad",
                },
            }),
        );
    }
    claim_provenance.insert(
        VERIFIED_PRIMARY_NAME_INVALIDATION_KEY.to_owned(),
        Value::Object(invalidation),
    );

    Value::Object(claim_provenance)
}
