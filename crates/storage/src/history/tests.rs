use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use serde_json::json;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

use super::*;
use crate::{
    AddressNameCurrentRow, AddressNameRelation, NameSurface, NormalizedEvent, RawBlock, Resource,
    SurfaceBinding, SurfaceBindingKind, TokenLineage, default_database_url,
    upsert_address_names_current_rows, upsert_name_surfaces, upsert_normalized_events,
    upsert_raw_blocks, upsert_resources, upsert_surface_bindings, upsert_token_lineages,
};

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
            .context("failed to parse database URL for history tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_storage_history_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for history tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect history test pool")?;

        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for history tests")?;

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

fn timestamp(unix_timestamp: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(unix_timestamp).expect("valid unix timestamp")
}

fn raw_block(
    chain_id: &str,
    block_hash: &str,
    parent_hash: Option<&str>,
    block_number: i64,
    block_timestamp: i64,
) -> RawBlock {
    RawBlock {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: parent_hash.map(str::to_owned),
        block_number,
        block_timestamp: timestamp(block_timestamp),
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn resource(resource_id: Uuid) -> Resource {
    Resource {
        resource_id,
        token_lineage_id: None,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xresource".to_owned(),
        block_number: 99,
        provenance: json!({"seed": "resource"}),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn tokenized_resource(resource_id: Uuid, token_lineage_id: Uuid) -> Resource {
    Resource {
        token_lineage_id: Some(token_lineage_id),
        ..resource(resource_id)
    }
}

fn token_lineage(token_lineage_id: Uuid) -> TokenLineage {
    TokenLineage {
        token_lineage_id,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: format!("0xlineage{}", token_lineage_id.simple()),
        block_number: 97,
        provenance: json!({"seed": "token_lineage"}),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn name_surface(logical_name_id: &str) -> NameSurface {
    let normalized_name = logical_name_id
        .split_once(':')
        .map(|(_, normalized_name)| normalized_name)
        .expect("logical_name_id must include namespace");

    NameSurface {
        logical_name_id: logical_name_id.to_owned(),
        namespace: "ens".to_owned(),
        input_name: normalized_name.to_owned(),
        canonical_display_name: "Alice.eth".to_owned(),
        normalized_name: normalized_name.to_owned(),
        dns_encoded_name: vec![5, b'a', b'l', b'i', b'c', b'e'],
        namehash: format!("namehash:{normalized_name}"),
        labelhashes: vec!["labelhash:alice".to_owned()],
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xsurface".to_owned(),
        block_number: 98,
        provenance: json!({"seed": "surface"}),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn surface_binding(
    surface_binding_id: Uuid,
    logical_name_id: &str,
    resource_id: Uuid,
    active_from: OffsetDateTime,
) -> SurfaceBinding {
    SurfaceBinding {
        surface_binding_id,
        logical_name_id: logical_name_id.to_owned(),
        resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from,
        active_to: None,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xbinding".to_owned(),
        block_number: 100,
        provenance: json!({"seed": "binding"}),
        canonicality_state: CanonicalityState::Canonical,
    }
}

#[allow(clippy::too_many_arguments)]
fn history_event(
    event_identity: &str,
    logical_name_id: Option<&str>,
    resource_id: Option<Uuid>,
    chain_id: Option<&str>,
    block_number: Option<i64>,
    block_hash: Option<&str>,
    transaction_hash: Option<&str>,
    log_index: Option<i64>,
    canonicality_state: CanonicalityState,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: logical_name_id.map(str::to_owned),
        resource_id,
        event_kind: "HistoryEvent".to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 7,
        source_manifest_id: None,
        chain_id: chain_id.map(str::to_owned),
        block_number,
        block_hash: block_hash.map(str::to_owned),
        transaction_hash: transaction_hash.map(str::to_owned),
        log_index,
        raw_fact_ref: json!({
            "kind": "raw_log",
            "transaction_index": transaction_hash.map(|_| 3),
            "event_identity": event_identity,
        }),
        derivation_kind: "history_test".to_owned(),
        canonicality_state,
        before_state: json!({
            "provenance": {
                "before": event_identity,
            }
        }),
        after_state: json!({
            "provenance": {
                "after": event_identity,
            },
            "coverage": {
                "status": "full",
                "event_identity": event_identity,
            }
        }),
    }
}

fn authority_match_event(
    event_identity: &str,
    logical_name_id: &str,
    resource_id: Uuid,
    event_kind: &str,
    block_number: i64,
    block_hash: &str,
    after_state: Value,
) -> NormalizedEvent {
    NormalizedEvent {
        event_kind: event_kind.to_owned(),
        source_family: "ens_v1_registrar_l1".to_owned(),
        derivation_kind: ENS_V1_AUTHORITY_DERIVATION_KIND.to_owned(),
        after_state,
        before_state: json!({}),
        ..history_event(
            event_identity,
            Some(logical_name_id),
            Some(resource_id),
            Some("ethereum-mainnet"),
            Some(block_number),
            Some(block_hash),
            Some(&format!("0xtx{block_number}")),
            Some(0),
            CanonicalityState::Canonical,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn ensv2_registry_event(
    event_identity: &str,
    logical_name_id: &str,
    resource_id: Option<Uuid>,
    event_kind: &str,
    block_number: i64,
    block_hash: &str,
    after_state: Value,
    canonicality_state: CanonicalityState,
) -> NormalizedEvent {
    NormalizedEvent {
        event_kind: event_kind.to_owned(),
        source_family: "ens_v2_registry_l1".to_owned(),
        derivation_kind: ENS_V2_REGISTRY_DERIVATION_KIND.to_owned(),
        after_state,
        before_state: json!({}),
        ..history_event(
            event_identity,
            Some(logical_name_id),
            resource_id,
            Some("ethereum-sepolia"),
            Some(block_number),
            Some(block_hash),
            Some(&format!("0xensv2tx{block_number}")),
            Some(0),
            canonicality_state,
        )
    }
}

fn address_name_current_row(
    address: &str,
    logical_name_id: &str,
    relation: AddressNameRelation,
    surface_binding_id: Uuid,
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    block_number: i64,
) -> AddressNameCurrentRow {
    let normalized_name = logical_name_id
        .split_once(':')
        .map(|(_, normalized_name)| normalized_name)
        .expect("logical_name_id must include namespace");
    let namespace = logical_name_id
        .split_once(':')
        .map(|(namespace, _)| namespace)
        .expect("logical_name_id must include namespace");
    let (chain_slot, chain_id, source_family) = match namespace {
        "basenames" => ("base", "base-mainnet", "basenames_base_registry"),
        _ => ("ethereum", "ethereum-mainnet", "ens_v1_registrar_l1"),
    };

    AddressNameCurrentRow {
        address: address.to_owned(),
        logical_name_id: logical_name_id.to_owned(),
        relation,
        namespace: namespace.to_owned(),
        canonical_display_name: normalized_name.to_owned(),
        normalized_name: normalized_name.to_owned(),
        namehash: format!("namehash:{normalized_name}"),
        surface_binding_id,
        resource_id,
        token_lineage_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        provenance: json!({
            "normalized_event_ids": [block_number],
            "raw_fact_refs": [{
                "kind": "raw_log",
                "block_number": block_number,
            }],
            "manifest_versions": [{
                "manifest_version": 3,
                "source_family": source_family,
                "source_manifest_id": null,
            }],
            "execution_trace_id": null,
            "derivation_kind": "address_names_current_rebuild",
        }),
        coverage: json!({
            "status": "full",
            "exhaustiveness": "authoritative",
            "source_classes_considered": ["ensv1_registry_path"],
            "enumeration_basis": "surface_current_relations",
            "unsupported_reason": null,
        }),
        chain_positions: json!({
            chain_slot: {
                "chain_id": chain_id,
                "block_number": block_number,
                "block_hash": format!("0xaddr{block_number:02x}"),
                "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
            }
        }),
        canonicality_summary: json!({
            "status": "canonical",
            "chains": {
                chain_id: "canonical",
            }
        }),
        manifest_version: 3,
        last_recomputed_at: timestamp(1_717_173_000 + block_number),
    }
}

#[tokio::test]
async fn canonical_only_history_excludes_observed_and_orphaned_rows() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0xa001);

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0x100", None, 100, 1_700_000_100),
            raw_block(
                "ethereum-mainnet",
                "0x101",
                Some("0x100"),
                101,
                1_700_000_101,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x102",
                Some("0x101"),
                102,
                1_700_000_102,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x103",
                Some("0x102"),
                103,
                1_700_000_103,
            ),
        ],
    )
    .await?;

    upsert_normalized_events(
        database.pool(),
        &[
            history_event(
                "history:canonical",
                Some("ens:alice.eth"),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(100),
                Some("0x100"),
                Some("0xtx100"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "history:safe",
                Some("ens:alice.eth"),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(101),
                Some("0x101"),
                Some("0xtx101"),
                Some(0),
                CanonicalityState::Safe,
            ),
            history_event(
                "history:finalized",
                Some("ens:alice.eth"),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(102),
                Some("0x102"),
                Some("0xtx102"),
                Some(0),
                CanonicalityState::Finalized,
            ),
            history_event(
                "history:observed",
                Some("ens:alice.eth"),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(103),
                Some("0x103"),
                Some("0xtx103"),
                Some(0),
                CanonicalityState::Observed,
            ),
            history_event(
                "history:orphaned",
                Some("ens:alice.eth"),
                Some(resource_id),
                None,
                None,
                None,
                None,
                None,
                CanonicalityState::Orphaned,
            ),
        ],
    )
    .await?;

    let canonical_only = load_name_history(
        database.pool(),
        "ens:alice.eth",
        &[resource_id],
        HistoryScope::Both,
        true,
    )
    .await?;

    assert_eq!(
        canonical_only
            .iter()
            .map(|row| row.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec!["history:finalized", "history:safe", "history:canonical"]
    );

    let all_rows = load_name_history(
        database.pool(),
        "ens:alice.eth",
        &[resource_id],
        HistoryScope::Both,
        false,
    )
    .await?;
    assert_eq!(all_rows.len(), 5);

    database.cleanup().await
}

#[tokio::test]
async fn name_history_scope_uses_logical_name_and_resource_filters() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0xa100);
    let other_resource_id = Uuid::from_u128(0xa101);

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0x200", None, 200, 1_700_000_200),
            raw_block(
                "ethereum-mainnet",
                "0x201",
                Some("0x200"),
                201,
                1_700_000_201,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x202",
                Some("0x201"),
                202,
                1_700_000_202,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x203",
                Some("0x202"),
                203,
                1_700_000_203,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x204",
                Some("0x203"),
                204,
                1_700_000_204,
            ),
        ],
    )
    .await?;

    upsert_normalized_events(
        database.pool(),
        &[
            history_event(
                "surface-only",
                Some("ens:alice.eth"),
                None,
                Some("ethereum-mainnet"),
                Some(200),
                Some("0x200"),
                Some("0xtx200"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "resource-only",
                None,
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(201),
                Some("0x201"),
                Some("0xtx201"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "both-anchors",
                Some("ens:alice.eth"),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(202),
                Some("0x202"),
                Some("0xtx202"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "same-resource-other-name",
                Some("ens:other.eth"),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(203),
                Some("0x203"),
                Some("0xtx203"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "same-name-other-resource",
                Some("ens:alice.eth"),
                Some(other_resource_id),
                Some("ethereum-mainnet"),
                Some(204),
                Some("0x204"),
                Some("0xtx204"),
                Some(0),
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let surface_rows = load_name_history(
        database.pool(),
        "ens:alice.eth",
        &[resource_id],
        HistoryScope::Surface,
        true,
    )
    .await?;
    assert_eq!(
        surface_rows
            .iter()
            .map(|row| row.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec!["same-name-other-resource", "both-anchors", "surface-only"]
    );

    let resource_rows = load_name_history(
        database.pool(),
        "ens:alice.eth",
        &[resource_id],
        HistoryScope::Resource,
        true,
    )
    .await?;
    assert_eq!(
        resource_rows
            .iter()
            .map(|row| row.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec!["same-resource-other-name", "both-anchors", "resource-only"]
    );

    let both_rows = load_name_history(
        database.pool(),
        "ens:alice.eth",
        &[resource_id],
        HistoryScope::Both,
        true,
    )
    .await?;
    assert_eq!(
        both_rows
            .iter()
            .map(|row| row.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec![
            "same-name-other-resource",
            "same-resource-other-name",
            "both-anchors",
            "resource-only",
            "surface-only",
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn event_history_filter_composes_projection_anchors_and_event_filters() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0xa105);
    let surface_binding_id = Uuid::from_u128(0xb105);

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0x250", None, 250, 1_700_000_250),
            raw_block(
                "ethereum-mainnet",
                "0x251",
                Some("0x250"),
                251,
                1_700_000_251,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x252",
                Some("0x251"),
                252,
                1_700_000_252,
            ),
        ],
    )
    .await?;
    upsert_name_surfaces(database.pool(), &[name_surface(logical_name_id)]).await?;
    upsert_resources(database.pool(), &[resource(resource_id)]).await?;
    upsert_surface_bindings(
        database.pool(),
        &[surface_binding(
            surface_binding_id,
            logical_name_id,
            resource_id,
            timestamp(1_700_000_240),
        )],
    )
    .await?;
    upsert_address_names_current_rows(
        database.pool(),
        &[address_name_current_row(
            address,
            logical_name_id,
            AddressNameRelation::Registrant,
            surface_binding_id,
            resource_id,
            None,
            250,
        )],
    )
    .await?;

    upsert_normalized_events(
        database.pool(),
        &[
            NormalizedEvent {
                event_kind: "RecordChanged".to_owned(),
                after_state: json!({"key": "avatar"}),
                ..history_event(
                    "events:surface-record",
                    Some(logical_name_id),
                    None,
                    Some("ethereum-mainnet"),
                    Some(250),
                    Some("0x250"),
                    Some("0xtx250"),
                    Some(0),
                    CanonicalityState::Canonical,
                )
            },
            NormalizedEvent {
                event_kind: "RegistrationGranted".to_owned(),
                after_state: json!({"registrant": address.to_ascii_uppercase()}),
                ..history_event(
                    "events:resource-registration",
                    None,
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(251),
                    Some("0x251"),
                    Some("0xtx251"),
                    Some(0),
                    CanonicalityState::Canonical,
                )
            },
            NormalizedEvent {
                event_kind: "RegistrationGranted".to_owned(),
                after_state: json!({"registrant": address}),
                ..history_event(
                    "events:observed-registration",
                    None,
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(252),
                    Some("0x252"),
                    Some("0xtx252"),
                    Some(0),
                    CanonicalityState::Observed,
                )
            },
        ],
    )
    .await?;

    let rows = load_event_history(
        database.pool(),
        EventHistoryFilter {
            namespace: Some("ens".to_owned()),
            logical_name_id: Some(logical_name_id.to_owned()),
            address: Some(EventHistoryAddressFilter {
                address: address.to_owned(),
                relation: Some(AddressNameRelation::Registrant),
            }),
            event_kinds: vec!["RegistrationGranted".to_owned()],
            from_block: Some(251),
            to_block: Some(251),
            ..EventHistoryFilter::default()
        },
        true,
    )
    .await?;

    assert_eq!(
        rows.iter()
            .map(|row| row.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec!["events:resource-registration"]
    );

    database.cleanup().await
}

#[tokio::test]
async fn name_history_resource_scope_preserves_rebound_resource_ids() -> Result<()> {
    let database = TestDatabase::new().await?;
    let logical_name_id = "ens:alice.eth";
    let old_resource_id = Uuid::from_u128(0xa110);
    let current_resource_id = Uuid::from_u128(0xa111);

    upsert_name_surfaces(database.pool(), &[name_surface(logical_name_id)]).await?;
    upsert_resources(
        database.pool(),
        &[resource(old_resource_id), resource(current_resource_id)],
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &[
            SurfaceBinding {
                active_to: Some(timestamp(1_700_000_250)),
                ..surface_binding(
                    Uuid::from_u128(0xb110),
                    logical_name_id,
                    old_resource_id,
                    timestamp(1_700_000_200),
                )
            },
            surface_binding(
                Uuid::from_u128(0xb111),
                logical_name_id,
                current_resource_id,
                timestamp(1_700_000_251),
            ),
        ],
    )
    .await?;

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0x210", None, 210, 1_700_000_210),
            raw_block(
                "ethereum-mainnet",
                "0x211",
                Some("0x210"),
                211,
                1_700_000_211,
            ),
        ],
    )
    .await?;

    upsert_normalized_events(
        database.pool(),
        &[
            history_event(
                "resource-old",
                None,
                Some(old_resource_id),
                Some("ethereum-mainnet"),
                Some(210),
                Some("0x210"),
                Some("0xtx210"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "resource-current",
                None,
                Some(current_resource_id),
                Some("ethereum-mainnet"),
                Some(211),
                Some("0x211"),
                Some("0xtx211"),
                Some(0),
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let rows = load_name_history(
        database.pool(),
        logical_name_id,
        &[old_resource_id, current_resource_id],
        HistoryScope::Resource,
        true,
    )
    .await?;

    assert_eq!(
        rows.iter()
            .map(|row| row.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec!["resource-current", "resource-old"]
    );

    database.cleanup().await
}

#[tokio::test]
async fn resource_history_scope_uses_resource_and_logical_name_filters() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0xa200);
    let other_resource_id = Uuid::from_u128(0xa201);

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0x300", None, 300, 1_700_000_300),
            raw_block(
                "ethereum-mainnet",
                "0x301",
                Some("0x300"),
                301,
                1_700_000_301,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x302",
                Some("0x301"),
                302,
                1_700_000_302,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x303",
                Some("0x302"),
                303,
                1_700_000_303,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x304",
                Some("0x303"),
                304,
                1_700_000_304,
            ),
        ],
    )
    .await?;

    upsert_normalized_events(
        database.pool(),
        &[
            history_event(
                "surface-only",
                Some("ens:alice.eth"),
                None,
                Some("ethereum-mainnet"),
                Some(300),
                Some("0x300"),
                Some("0xtx300"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "resource-only",
                None,
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(301),
                Some("0x301"),
                Some("0xtx301"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "both-anchors",
                Some("ens:alice.eth"),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(302),
                Some("0x302"),
                Some("0xtx302"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "same-resource-other-name",
                Some("ens:other.eth"),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(303),
                Some("0x303"),
                Some("0xtx303"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "same-name-other-resource",
                Some("ens:alice.eth"),
                Some(other_resource_id),
                Some("ethereum-mainnet"),
                Some(304),
                Some("0x304"),
                Some("0xtx304"),
                Some(0),
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let surface_rows = load_resource_history(
        database.pool(),
        resource_id,
        &["ens:alice.eth".to_owned()],
        HistoryScope::Surface,
        true,
    )
    .await?;
    assert_eq!(
        surface_rows
            .iter()
            .map(|row| row.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec!["same-name-other-resource", "both-anchors", "surface-only"]
    );

    let resource_rows = load_resource_history(
        database.pool(),
        resource_id,
        &["ens:alice.eth".to_owned()],
        HistoryScope::Resource,
        true,
    )
    .await?;
    assert_eq!(
        resource_rows
            .iter()
            .map(|row| row.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec!["same-resource-other-name", "both-anchors", "resource-only"]
    );

    let both_rows = load_resource_history(
        database.pool(),
        resource_id,
        &["ens:alice.eth".to_owned()],
        HistoryScope::Both,
        true,
    )
    .await?;
    assert_eq!(
        both_rows
            .iter()
            .map(|row| row.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec![
            "same-name-other-resource",
            "same-resource-other-name",
            "both-anchors",
            "resource-only",
            "surface-only",
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn resource_history_surface_scope_preserves_multiple_bound_surfaces() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0xa220);
    let primary_logical_name_id = "ens:alice.eth";
    let alias_logical_name_id = "ens:alice-base.eth";

    upsert_name_surfaces(
        database.pool(),
        &[
            name_surface(primary_logical_name_id),
            name_surface(alias_logical_name_id),
        ],
    )
    .await?;
    upsert_resources(database.pool(), &[resource(resource_id)]).await?;
    upsert_surface_bindings(
        database.pool(),
        &[
            surface_binding(
                Uuid::from_u128(0xb220),
                primary_logical_name_id,
                resource_id,
                timestamp(1_700_000_300),
            ),
            surface_binding(
                Uuid::from_u128(0xb221),
                alias_logical_name_id,
                resource_id,
                timestamp(1_700_000_301),
            ),
        ],
    )
    .await?;

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0x320", None, 320, 1_700_000_320),
            raw_block(
                "ethereum-mainnet",
                "0x321",
                Some("0x320"),
                321,
                1_700_000_321,
            ),
        ],
    )
    .await?;

    upsert_normalized_events(
        database.pool(),
        &[
            history_event(
                "surface-primary",
                Some(primary_logical_name_id),
                None,
                Some("ethereum-mainnet"),
                Some(320),
                Some("0x320"),
                Some("0xtx320"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "surface-alias",
                Some(alias_logical_name_id),
                None,
                Some("ethereum-mainnet"),
                Some(321),
                Some("0x321"),
                Some("0xtx321"),
                Some(0),
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let rows = load_resource_history(
        database.pool(),
        resource_id,
        &[
            primary_logical_name_id.to_owned(),
            alias_logical_name_id.to_owned(),
        ],
        HistoryScope::Surface,
        true,
    )
    .await?;

    assert_eq!(
        rows.iter()
            .map(|row| row.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec!["surface-alias", "surface-primary"]
    );

    database.cleanup().await
}

#[tokio::test]
async fn address_history_uses_current_and_historical_address_matches() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let current_logical_name_id = "ens:current.eth";
    let historical_logical_name_id = "ens:historical.eth";
    let current_resource_id = Uuid::from_u128(0xa230);
    let current_token_lineage_id = Uuid::from_u128(0xa231);
    let current_surface_binding_id = Uuid::from_u128(0xb230);
    let historical_resource_id = Uuid::from_u128(0xa232);
    let historical_token_lineage_id = Uuid::from_u128(0xa233);

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0x430", None, 430, 1_700_000_430),
            raw_block(
                "ethereum-mainnet",
                "0x431",
                Some("0x430"),
                431,
                1_700_000_431,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x432",
                Some("0x431"),
                432,
                1_700_000_432,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x433",
                Some("0x432"),
                433,
                1_700_000_433,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x434",
                Some("0x433"),
                434,
                1_700_000_434,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x435",
                Some("0x434"),
                435,
                1_700_000_435,
            ),
        ],
    )
    .await?;

    upsert_token_lineages(
        database.pool(),
        &[
            token_lineage(current_token_lineage_id),
            token_lineage(historical_token_lineage_id),
        ],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[
            tokenized_resource(current_resource_id, current_token_lineage_id),
            tokenized_resource(historical_resource_id, historical_token_lineage_id),
        ],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[
            name_surface(current_logical_name_id),
            name_surface(historical_logical_name_id),
        ],
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &[surface_binding(
            current_surface_binding_id,
            current_logical_name_id,
            current_resource_id,
            timestamp(1_700_000_430),
        )],
    )
    .await?;
    upsert_address_names_current_rows(
        database.pool(),
        &[address_name_current_row(
            address,
            current_logical_name_id,
            AddressNameRelation::Registrant,
            current_surface_binding_id,
            current_resource_id,
            Some(current_token_lineage_id),
            430,
        )],
    )
    .await?;

    upsert_normalized_events(
        database.pool(),
        &[
            history_event(
                "current-surface",
                Some(current_logical_name_id),
                None,
                Some("ethereum-mainnet"),
                Some(434),
                Some("0x434"),
                Some("0xtx434"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "current-resource",
                None,
                Some(current_resource_id),
                Some("ethereum-mainnet"),
                Some(435),
                Some("0x435"),
                Some("0xtx435"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "historical-surface",
                Some(historical_logical_name_id),
                None,
                Some("ethereum-mainnet"),
                Some(433),
                Some("0x433"),
                Some("0xtx433"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "historical-resource",
                None,
                Some(historical_resource_id),
                Some("ethereum-mainnet"),
                Some(432),
                Some("0x432"),
                Some("0xtx432"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            authority_match_event(
                "historical-match",
                historical_logical_name_id,
                historical_resource_id,
                "RegistrationGranted",
                431,
                "0x431",
                json!({
                    "registrant": "0x0000000000000000000000000000000000000ABC",
                }),
            ),
        ],
    )
    .await?;

    let surface_rows = load_address_history(
        database.pool(),
        address,
        Some("ens"),
        Some(AddressNameRelation::Registrant),
        HistoryScope::Surface,
        true,
    )
    .await?;
    assert_eq!(
        surface_rows
            .iter()
            .map(|row| row.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec!["current-surface", "historical-surface", "historical-match"]
    );

    let resource_rows = load_address_history(
        database.pool(),
        address,
        Some("ens"),
        Some(AddressNameRelation::Registrant),
        HistoryScope::Resource,
        true,
    )
    .await?;
    assert_eq!(
        resource_rows
            .iter()
            .map(|row| row.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec![
            "current-resource",
            "historical-resource",
            "historical-match"
        ]
    );

    let both_rows = load_address_history(
        database.pool(),
        address,
        Some("ens"),
        Some(AddressNameRelation::Registrant),
        HistoryScope::Both,
        true,
    )
    .await?;
    assert_eq!(
        both_rows
            .iter()
            .map(|row| row.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec![
            "current-resource",
            "current-surface",
            "historical-surface",
            "historical-resource",
            "historical-match",
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn address_history_basenames_matches_do_not_require_token_lineage_ids() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000b0b";
    let current_logical_name_id = "basenames:current.base.eth";
    let historical_logical_name_id = "basenames:historical.base.eth";
    let current_resource_id = Uuid::from_u128(0xa234);
    let current_surface_binding_id = Uuid::from_u128(0xb234);
    let historical_resource_id = Uuid::from_u128(0xa235);

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block("base-mainnet", "0xb430", None, 430, 1_700_000_430),
            raw_block("base-mainnet", "0xb431", Some("0xb430"), 431, 1_700_000_431),
            raw_block("base-mainnet", "0xb432", Some("0xb431"), 432, 1_700_000_432),
            raw_block("base-mainnet", "0xb433", Some("0xb432"), 433, 1_700_000_433),
            raw_block("base-mainnet", "0xb434", Some("0xb433"), 434, 1_700_000_434),
        ],
    )
    .await?;

    upsert_resources(
        database.pool(),
        &[
            Resource {
                chain_id: "base-mainnet".to_owned(),
                block_hash: "0xb430".to_owned(),
                block_number: 430,
                ..resource(current_resource_id)
            },
            Resource {
                chain_id: "base-mainnet".to_owned(),
                block_hash: "0xb431".to_owned(),
                block_number: 431,
                ..resource(historical_resource_id)
            },
        ],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[
            NameSurface {
                namespace: "basenames".to_owned(),
                chain_id: "base-mainnet".to_owned(),
                canonical_display_name: "current.base.eth".to_owned(),
                input_name: "current.base.eth".to_owned(),
                normalized_name: "current.base.eth".to_owned(),
                namehash: "namehash:current.base.eth".to_owned(),
                labelhashes: vec!["labelhash:current.base.eth".to_owned()],
                ..name_surface(current_logical_name_id)
            },
            NameSurface {
                namespace: "basenames".to_owned(),
                chain_id: "base-mainnet".to_owned(),
                canonical_display_name: "historical.base.eth".to_owned(),
                input_name: "historical.base.eth".to_owned(),
                normalized_name: "historical.base.eth".to_owned(),
                namehash: "namehash:historical.base.eth".to_owned(),
                labelhashes: vec!["labelhash:historical.base.eth".to_owned()],
                ..name_surface(historical_logical_name_id)
            },
        ],
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &[SurfaceBinding {
            chain_id: "base-mainnet".to_owned(),
            block_hash: "0xb430".to_owned(),
            block_number: 430,
            ..surface_binding(
                current_surface_binding_id,
                current_logical_name_id,
                current_resource_id,
                timestamp(1_700_000_430),
            )
        }],
    )
    .await?;
    upsert_address_names_current_rows(
        database.pool(),
        &[address_name_current_row(
            address,
            current_logical_name_id,
            AddressNameRelation::Registrant,
            current_surface_binding_id,
            current_resource_id,
            None,
            430,
        )],
    )
    .await?;

    upsert_normalized_events(
        database.pool(),
        &[
            NormalizedEvent {
                namespace: "basenames".to_owned(),
                source_family: "basenames_base_registry".to_owned(),
                ..history_event(
                    "current-surface",
                    Some(current_logical_name_id),
                    None,
                    Some("base-mainnet"),
                    Some(434),
                    Some("0xb434"),
                    Some("0xtx434"),
                    Some(0),
                    CanonicalityState::Canonical,
                )
            },
            NormalizedEvent {
                namespace: "basenames".to_owned(),
                source_family: "basenames_base_registry".to_owned(),
                ..history_event(
                    "current-resource",
                    None,
                    Some(current_resource_id),
                    Some("base-mainnet"),
                    Some(433),
                    Some("0xb433"),
                    Some("0xtx433"),
                    Some(0),
                    CanonicalityState::Canonical,
                )
            },
            NormalizedEvent {
                namespace: "basenames".to_owned(),
                source_family: "basenames_base_registry".to_owned(),
                ..history_event(
                    "historical-surface",
                    Some(historical_logical_name_id),
                    None,
                    Some("base-mainnet"),
                    Some(432),
                    Some("0xb432"),
                    Some("0xtx432"),
                    Some(0),
                    CanonicalityState::Canonical,
                )
            },
            NormalizedEvent {
                namespace: "basenames".to_owned(),
                source_family: "basenames_base_registry".to_owned(),
                ..history_event(
                    "historical-resource",
                    None,
                    Some(historical_resource_id),
                    Some("base-mainnet"),
                    Some(431),
                    Some("0xb431"),
                    Some("0xtx431"),
                    Some(0),
                    CanonicalityState::Canonical,
                )
            },
            NormalizedEvent {
                namespace: "basenames".to_owned(),
                source_family: "basenames_base_registry".to_owned(),
                chain_id: Some("base-mainnet".to_owned()),
                ..authority_match_event(
                    "historical-match",
                    historical_logical_name_id,
                    historical_resource_id,
                    "RegistrationGranted",
                    430,
                    "0xb430",
                    json!({
                        "registrant": "0x0000000000000000000000000000000000000B0B",
                    }),
                )
            },
        ],
    )
    .await?;

    let rows = load_address_history(
        database.pool(),
        address,
        Some("basenames"),
        Some(AddressNameRelation::Registrant),
        HistoryScope::Both,
        true,
    )
    .await?;
    assert_eq!(
        rows.iter()
            .map(|row| row.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec![
            "current-surface",
            "current-resource",
            "historical-surface",
            "historical-resource",
            "historical-match",
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn address_history_ens_tokenized_matches_require_token_lineage_ids() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000bad";
    let current_logical_name_id = "ens:current-token.eth";
    let registration_logical_name_id = "ens:null-registration.eth";
    let transfer_logical_name_id = "ens:null-transfer.eth";
    let current_resource_id = Uuid::from_u128(0xa236);
    let current_token_lineage_id = Uuid::from_u128(0xa237);
    let current_surface_binding_id = Uuid::from_u128(0xb236);
    let registration_resource_id = Uuid::from_u128(0xa238);
    let transfer_resource_id = Uuid::from_u128(0xa239);

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0x439", None, 439, 1_700_000_439),
            raw_block(
                "ethereum-mainnet",
                "0x440",
                Some("0x439"),
                440,
                1_700_000_440,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x441",
                Some("0x440"),
                441,
                1_700_000_441,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x442",
                Some("0x441"),
                442,
                1_700_000_442,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x443",
                Some("0x442"),
                443,
                1_700_000_443,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x444",
                Some("0x443"),
                444,
                1_700_000_444,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x445",
                Some("0x444"),
                445,
                1_700_000_445,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x446",
                Some("0x445"),
                446,
                1_700_000_446,
            ),
        ],
    )
    .await?;

    upsert_token_lineages(database.pool(), &[token_lineage(current_token_lineage_id)]).await?;
    upsert_resources(
        database.pool(),
        &[
            tokenized_resource(current_resource_id, current_token_lineage_id),
            resource(registration_resource_id),
            resource(transfer_resource_id),
        ],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[
            name_surface(current_logical_name_id),
            name_surface(registration_logical_name_id),
            name_surface(transfer_logical_name_id),
        ],
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &[surface_binding(
            current_surface_binding_id,
            current_logical_name_id,
            current_resource_id,
            timestamp(1_700_000_439),
        )],
    )
    .await?;
    upsert_address_names_current_rows(
        database.pool(),
        &[address_name_current_row(
            address,
            current_logical_name_id,
            AddressNameRelation::Registrant,
            current_surface_binding_id,
            current_resource_id,
            Some(current_token_lineage_id),
            439,
        )],
    )
    .await?;

    upsert_normalized_events(
        database.pool(),
        &[
            history_event(
                "current-resource",
                None,
                Some(current_resource_id),
                Some("ethereum-mainnet"),
                Some(446),
                Some("0x446"),
                Some("0xtx446"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "current-surface",
                Some(current_logical_name_id),
                None,
                Some("ethereum-mainnet"),
                Some(445),
                Some("0x445"),
                Some("0xtx445"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "registration-null-token-surface",
                Some(registration_logical_name_id),
                None,
                Some("ethereum-mainnet"),
                Some(444),
                Some("0x444"),
                Some("0xtx444"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "registration-null-token-resource",
                None,
                Some(registration_resource_id),
                Some("ethereum-mainnet"),
                Some(443),
                Some("0x443"),
                Some("0xtx443"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "transfer-null-token-surface",
                Some(transfer_logical_name_id),
                None,
                Some("ethereum-mainnet"),
                Some(442),
                Some("0x442"),
                Some("0xtx442"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "transfer-null-token-resource",
                None,
                Some(transfer_resource_id),
                Some("ethereum-mainnet"),
                Some(441),
                Some("0x441"),
                Some("0xtx441"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            authority_match_event(
                "registration-null-token-match",
                registration_logical_name_id,
                registration_resource_id,
                "RegistrationGranted",
                440,
                "0x440",
                json!({
                    "registrant": "0x0000000000000000000000000000000000000BAD",
                }),
            ),
            authority_match_event(
                "transfer-null-token-match",
                transfer_logical_name_id,
                transfer_resource_id,
                "TokenControlTransferred",
                439,
                "0x439",
                json!({
                    "to": "0x0000000000000000000000000000000000000BAD",
                }),
            ),
        ],
    )
    .await?;

    let rows = load_address_history(
        database.pool(),
        address,
        Some("ens"),
        Some(AddressNameRelation::Registrant),
        HistoryScope::Both,
        true,
    )
    .await?;
    assert_eq!(
        rows.iter()
            .map(|row| row.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec!["current-resource", "current-surface"]
    );

    database.cleanup().await
}

#[tokio::test]
async fn address_history_ensv2_uses_current_and_historical_registry_matches() -> Result<()> {
    let database = TestDatabase::new().await?;
    let holder = "0x0000000000000000000000000000000000000b0b";
    let controller = "0x0000000000000000000000000000000000000c0c";
    let current_logical_name_id = "ens:current-v2.eth";
    let historical_logical_name_id = "ens:historical-v2.eth";
    let pending_logical_name_id = "ens:pending-v2.eth";
    let observed_logical_name_id = "ens:observed-v2.eth";
    let current_resource_id = Uuid::from_u128(0xa24a);
    let current_token_lineage_id = Uuid::from_u128(0xa24b);
    let current_surface_binding_id = Uuid::from_u128(0xb24a);
    let historical_resource_id = Uuid::from_u128(0xa24c);
    let historical_token_lineage_id = Uuid::from_u128(0xa24d);
    let observed_resource_id = Uuid::from_u128(0xa24e);
    let observed_token_lineage_id = Uuid::from_u128(0xa24f);

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-sepolia", "0xev2430", None, 430, 1_700_000_430),
            raw_block("ethereum-sepolia", "0xev2431", None, 431, 1_700_000_431),
            raw_block("ethereum-sepolia", "0xev2432", None, 432, 1_700_000_432),
            raw_block("ethereum-sepolia", "0xev2433", None, 433, 1_700_000_433),
            raw_block("ethereum-sepolia", "0xev2434", None, 434, 1_700_000_434),
            raw_block("ethereum-sepolia", "0xev2435", None, 435, 1_700_000_435),
            raw_block("ethereum-sepolia", "0xev2436", None, 436, 1_700_000_436),
            raw_block("ethereum-sepolia", "0xev2437", None, 437, 1_700_000_437),
            raw_block("ethereum-sepolia", "0xev2439", None, 439, 1_700_000_439),
            raw_block("ethereum-sepolia", "0xev2440", None, 440, 1_700_000_440),
            raw_block("ethereum-sepolia", "0xev2441", None, 441, 1_700_000_441),
        ],
    )
    .await?;
    upsert_token_lineages(
        database.pool(),
        &[
            token_lineage(current_token_lineage_id),
            token_lineage(historical_token_lineage_id),
            token_lineage(observed_token_lineage_id),
        ],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[
            tokenized_resource(current_resource_id, current_token_lineage_id),
            tokenized_resource(historical_resource_id, historical_token_lineage_id),
            tokenized_resource(observed_resource_id, observed_token_lineage_id),
        ],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[
            name_surface(current_logical_name_id),
            name_surface(historical_logical_name_id),
            name_surface(pending_logical_name_id),
            name_surface(observed_logical_name_id),
        ],
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &[surface_binding(
            current_surface_binding_id,
            current_logical_name_id,
            current_resource_id,
            timestamp(1_700_000_430),
        )],
    )
    .await?;
    upsert_address_names_current_rows(
        database.pool(),
        &[
            address_name_current_row(
                holder,
                current_logical_name_id,
                AddressNameRelation::Registrant,
                current_surface_binding_id,
                current_resource_id,
                Some(current_token_lineage_id),
                430,
            ),
            address_name_current_row(
                controller,
                current_logical_name_id,
                AddressNameRelation::EffectiveController,
                current_surface_binding_id,
                current_resource_id,
                Some(current_token_lineage_id),
                431,
            ),
        ],
    )
    .await?;

    upsert_normalized_events(
        database.pool(),
        &[
            history_event(
                "current-resource",
                None,
                Some(current_resource_id),
                Some("ethereum-sepolia"),
                Some(437),
                Some("0xev2437"),
                Some("0xev2tx437"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "current-surface",
                Some(current_logical_name_id),
                None,
                Some("ethereum-sepolia"),
                Some(436),
                Some("0xev2436"),
                Some("0xev2tx436"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "historical-surface",
                Some(historical_logical_name_id),
                None,
                Some("ethereum-sepolia"),
                Some(435),
                Some("0xev2435"),
                Some("0xev2tx435"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "historical-resource",
                None,
                Some(historical_resource_id),
                Some("ethereum-sepolia"),
                Some(434),
                Some("0xev2434"),
                Some("0xev2tx434"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            ensv2_registry_event(
                "historical-v2-grant",
                historical_logical_name_id,
                Some(historical_resource_id),
                "RegistrationGranted",
                433,
                "0xev2433",
                json!({
                    "registrant": "0x0000000000000000000000000000000000000B0B",
                }),
                CanonicalityState::Canonical,
            ),
            ensv2_registry_event(
                "historical-v2-authority",
                historical_logical_name_id,
                Some(historical_resource_id),
                "AuthorityTransferred",
                432,
                "0xev2432",
                json!({
                    "owner": "0x0000000000000000000000000000000000000C0C",
                }),
                CanonicalityState::Canonical,
            ),
            ensv2_registry_event(
                "pending-v2-grant",
                pending_logical_name_id,
                None,
                "RegistrationGranted",
                431,
                "0xev2431",
                json!({
                    "registrant": "0x0000000000000000000000000000000000000B0B",
                    "resource_pending": true,
                }),
                CanonicalityState::Canonical,
            ),
            history_event(
                "pending-surface",
                Some(pending_logical_name_id),
                None,
                Some("ethereum-sepolia"),
                Some(430),
                Some("0xev2430"),
                Some("0xev2tx430"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "observed-anchor-leak-surface",
                Some(observed_logical_name_id),
                None,
                Some("ethereum-sepolia"),
                Some(441),
                Some("0xev2441"),
                Some("0xev2tx441"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "observed-anchor-leak-resource",
                None,
                Some(observed_resource_id),
                Some("ethereum-sepolia"),
                Some(440),
                Some("0xev2440"),
                Some("0xev2tx440"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            ensv2_registry_event(
                "observed-v2-grant",
                observed_logical_name_id,
                Some(observed_resource_id),
                "RegistrationGranted",
                439,
                "0xev2439",
                json!({
                    "registrant": "0x0000000000000000000000000000000000000B0B",
                }),
                CanonicalityState::Observed,
            ),
        ],
    )
    .await?;

    let holder_rows = load_address_history(
        database.pool(),
        holder,
        Some("ens"),
        Some(AddressNameRelation::Registrant),
        HistoryScope::Both,
        true,
    )
    .await?;
    assert_eq!(
        holder_rows
            .iter()
            .map(|row| row.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec![
            "current-resource",
            "current-surface",
            "historical-surface",
            "historical-resource",
            "historical-v2-grant",
            "historical-v2-authority",
            "pending-v2-grant",
            "pending-surface",
        ]
    );

    let controller_rows = load_address_history(
        database.pool(),
        controller,
        Some("ens"),
        Some(AddressNameRelation::EffectiveController),
        HistoryScope::Both,
        true,
    )
    .await?;
    assert_eq!(
        controller_rows
            .iter()
            .map(|row| row.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec![
            "current-resource",
            "current-surface",
            "historical-surface",
            "historical-resource",
            "historical-v2-grant",
            "historical-v2-authority",
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn address_history_effective_controller_includes_registry_owner_matches() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000def";
    let logical_name_id = "ens:controller.eth";
    let resource_id = Uuid::from_u128(0xa240);

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0x440", None, 440, 1_700_000_440),
            raw_block(
                "ethereum-mainnet",
                "0x441",
                Some("0x440"),
                441,
                1_700_000_441,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x442",
                Some("0x441"),
                442,
                1_700_000_442,
            ),
        ],
    )
    .await?;
    upsert_resources(database.pool(), &[resource(resource_id)]).await?;

    upsert_normalized_events(
        database.pool(),
        &[
            authority_match_event(
                "controller-match",
                logical_name_id,
                resource_id,
                "AuthorityTransferred",
                440,
                "0x440",
                json!({
                    "owner": "0x0000000000000000000000000000000000000DEF",
                }),
            ),
            history_event(
                "controller-surface",
                Some(logical_name_id),
                None,
                Some("ethereum-mainnet"),
                Some(441),
                Some("0x441"),
                Some("0xtx441"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "controller-resource",
                None,
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(442),
                Some("0x442"),
                Some("0xtx442"),
                Some(0),
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let registrant_rows = load_address_history(
        database.pool(),
        address,
        Some("ens"),
        Some(AddressNameRelation::Registrant),
        HistoryScope::Both,
        true,
    )
    .await?;
    assert!(registrant_rows.is_empty());

    let controller_rows = load_address_history(
        database.pool(),
        address,
        Some("ens"),
        Some(AddressNameRelation::EffectiveController),
        HistoryScope::Both,
        true,
    )
    .await?;
    assert_eq!(
        controller_rows
            .iter()
            .map(|row| row.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec![
            "controller-resource",
            "controller-surface",
            "controller-match"
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn history_reads_use_deterministic_chain_position_desc_ordering() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0xa300);

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block("base-mainnet", "0xb101", None, 101, 1_700_000_401),
            raw_block("ethereum-mainnet", "0xe100", None, 100, 1_700_000_400),
            raw_block("base-mainnet", "0xb100", Some("0xb101"), 100, 1_700_000_399),
        ],
    )
    .await?;

    upsert_normalized_events(
        database.pool(),
        &[
            history_event(
                "no-chain-position",
                Some("ens:alice.eth"),
                Some(resource_id),
                None,
                None,
                None,
                None,
                None,
                CanonicalityState::Canonical,
            ),
            history_event(
                "ethereum-lower-log",
                Some("ens:alice.eth"),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(100),
                Some("0xe100"),
                Some("0xtx100"),
                Some(1),
                CanonicalityState::Canonical,
            ),
            history_event(
                "ethereum-higher-log",
                Some("ens:alice.eth"),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(100),
                Some("0xe100"),
                Some("0xtx100"),
                Some(7),
                CanonicalityState::Canonical,
            ),
            history_event(
                "base-same-height",
                Some("ens:alice.eth"),
                Some(resource_id),
                Some("base-mainnet"),
                Some(100),
                Some("0xb100"),
                Some("0xtx090"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "base-higher-height",
                Some("ens:alice.eth"),
                Some(resource_id),
                Some("base-mainnet"),
                Some(101),
                Some("0xb101"),
                Some("0xtx101"),
                Some(0),
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let rows = load_name_history(
        database.pool(),
        "ens:alice.eth",
        &[resource_id],
        HistoryScope::Both,
        true,
    )
    .await?;

    assert_eq!(
        rows.iter()
            .map(|row| row.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec![
            "base-higher-height",
            "base-same-height",
            "ethereum-higher-log",
            "ethereum-lower-log",
            "no-chain-position",
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn history_page_uses_storage_limit_and_keyset_cursor() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0xa305);

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0x510", None, 510, 1_700_000_510),
            raw_block(
                "ethereum-mainnet",
                "0x511",
                Some("0x510"),
                511,
                1_700_000_511,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x512",
                Some("0x511"),
                512,
                1_700_000_512,
            ),
        ],
    )
    .await?;

    upsert_normalized_events(
        database.pool(),
        &[
            history_event(
                "page:first",
                Some("ens:alice.eth"),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(512),
                Some("0x512"),
                Some("0xtx512"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "page:second",
                Some("ens:alice.eth"),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(511),
                Some("0x511"),
                Some("0xtx511"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "page:third",
                Some("ens:alice.eth"),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(510),
                Some("0x510"),
                Some("0xtx510"),
                Some(0),
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let first_page = load_name_history_page(
        database.pool(),
        "ens:alice.eth",
        &[resource_id],
        HistoryScope::Both,
        true,
        None,
        1,
        HistorySummaryMode::None,
    )
    .await?;
    assert_eq!(
        first_page
            .rows
            .iter()
            .map(|row| row.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec!["page:first"]
    );
    let next_cursor = first_page
        .next_cursor
        .as_ref()
        .expect("first history page must return a cursor");
    assert_eq!(next_cursor.event_identity, "page:first");

    let second_page = load_name_history_page(
        database.pool(),
        "ens:alice.eth",
        &[resource_id],
        HistoryScope::Both,
        true,
        Some(next_cursor),
        1,
        HistorySummaryMode::None,
    )
    .await?;
    assert_eq!(
        second_page
            .rows
            .iter()
            .map(|row| row.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec!["page:second"]
    );

    database.cleanup().await
}

#[tokio::test]
async fn history_page_size_one_walk_matches_unpaged_order_across_sort_ties() -> Result<()> {
    let database = TestDatabase::new().await?;
    let logical_name_id = "ens:ties.eth";
    let resource_id = Uuid::from_u128(0xa307);

    upsert_normalized_events(
        database.pool(),
        &[
            history_event(
                "tie:block-newer",
                Some(logical_name_id),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(906),
                Some("0x906"),
                Some("0xtx906"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "tie:chain-base",
                Some(logical_name_id),
                Some(resource_id),
                Some("base-mainnet"),
                Some(905),
                Some("0x905base"),
                Some("0xtx905base"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "tie:chain-ethereum",
                Some(logical_name_id),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(905),
                Some("0x905ethereum"),
                Some("0xtx905ethereum"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "tie:chain-null",
                Some(logical_name_id),
                Some(resource_id),
                None,
                Some(905),
                Some("0x905null"),
                Some("0xtx905null"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "tie:block-hash-high",
                Some(logical_name_id),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(904),
                Some("0x904ff"),
                Some("0xtx904"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "tie:block-hash-low",
                Some(logical_name_id),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(904),
                Some("0x904aa"),
                Some("0xtx904"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "tie:tx-high",
                Some(logical_name_id),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(903),
                Some("0x903"),
                Some("0xtxff"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "tie:tx-low",
                Some(logical_name_id),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(903),
                Some("0x903"),
                Some("0xtxaa"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "tie:tx-null",
                Some(logical_name_id),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(903),
                Some("0x903"),
                None,
                None,
                CanonicalityState::Canonical,
            ),
            history_event(
                "tie:log-one",
                Some(logical_name_id),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(902),
                Some("0x902"),
                Some("0xtx902"),
                Some(1),
                CanonicalityState::Canonical,
            ),
            history_event(
                "tie:log-zero",
                Some(logical_name_id),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(902),
                Some("0x902"),
                Some("0xtx902"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "tie:log-null",
                Some(logical_name_id),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(902),
                Some("0x902"),
                Some("0xtx902"),
                None,
                CanonicalityState::Canonical,
            ),
            history_event(
                "tie:same:z",
                Some(logical_name_id),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(901),
                Some("0x901"),
                Some("0xtx901"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "tie:same:y",
                Some(logical_name_id),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(901),
                Some("0x901"),
                Some("0xtx901"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "tie:block-null",
                Some(logical_name_id),
                Some(resource_id),
                None,
                None,
                None,
                None,
                None,
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    let unpaged = load_name_history(
        database.pool(),
        logical_name_id,
        &[resource_id],
        HistoryScope::Both,
        true,
    )
    .await?;
    let expected = unpaged
        .iter()
        .map(|row| row.event_identity.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        expected,
        vec![
            "tie:block-newer",
            "tie:chain-base",
            "tie:chain-ethereum",
            "tie:chain-null",
            "tie:block-hash-high",
            "tie:block-hash-low",
            "tie:tx-high",
            "tie:tx-low",
            "tie:tx-null",
            "tie:log-one",
            "tie:log-zero",
            "tie:log-null",
            "tie:same:z",
            "tie:same:y",
            "tie:block-null",
        ]
    );

    let mut walked = Vec::new();
    let mut cursor = None;
    loop {
        let page = load_name_history_page(
            database.pool(),
            logical_name_id,
            &[resource_id],
            HistoryScope::Both,
            true,
            cursor.as_ref(),
            1,
            HistorySummaryMode::None,
        )
        .await?;
        assert_eq!(page.rows.len(), 1, "page should return one walked row");
        walked.push(page.rows[0].event_identity.clone());

        let walked_unique = walked
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        assert_eq!(walked_unique, walked.len(), "page walk duplicated a row");

        cursor = match page.next_cursor {
            Some(next_cursor) => Some(next_cursor),
            None => break,
        };
    }

    assert_eq!(walked, expected);

    database.cleanup().await
}

#[tokio::test]
async fn history_page_rejects_cursor_when_selector_is_empty() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_raw_blocks(
        database.pool(),
        &[raw_block(
            "ethereum-mainnet",
            "0x520",
            None,
            520,
            1_700_000_520,
        )],
    )
    .await?;

    upsert_normalized_events(
        database.pool(),
        &[history_event(
            "page:cursor",
            Some("ens:alice.eth"),
            Some(Uuid::from_u128(0xa306)),
            Some("ethereum-mainnet"),
            Some(520),
            Some("0x520"),
            Some("0xtx520"),
            Some(0),
            CanonicalityState::Canonical,
        )],
    )
    .await?;

    let cursor_page = load_name_history_page(
        database.pool(),
        "ens:alice.eth",
        &[Uuid::from_u128(0xa306)],
        HistoryScope::Both,
        true,
        None,
        1,
        HistorySummaryMode::None,
    )
    .await?;
    let cursor_row = cursor_page
        .rows
        .first()
        .expect("history page must include the cursor row");
    let cursor = HistoryCursor {
        normalized_event_id: cursor_row.normalized_event_id,
        event_identity: cursor_row.event_identity.clone(),
    };

    let error = load_name_history_page(
        database.pool(),
        "ens:missing.eth",
        &[],
        HistoryScope::Resource,
        true,
        Some(&cursor),
        1,
        HistorySummaryMode::None,
    )
    .await
    .expect_err("empty selector with stale cursor must reject the cursor");
    assert!(
        error.downcast_ref::<InvalidHistoryCursor>().is_some(),
        "expected invalid history cursor error, got {error:?}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn name_history_head_matches_first_row_for_surface_and_resource_scopes() -> Result<()> {
    let database = TestDatabase::new().await?;
    let logical_name_id = "ens:alice.eth";
    let resource_id = Uuid::from_u128(0xa301);
    let other_resource_id = Uuid::from_u128(0xa302);

    upsert_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0x500", None, 500, 1_700_000_500),
            raw_block(
                "ethereum-mainnet",
                "0x501",
                Some("0x500"),
                501,
                1_700_000_501,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x502",
                Some("0x501"),
                502,
                1_700_000_502,
            ),
            raw_block(
                "ethereum-mainnet",
                "0x503",
                Some("0x502"),
                503,
                1_700_000_503,
            ),
        ],
    )
    .await?;

    upsert_normalized_events(
        database.pool(),
        &[
            history_event(
                "surface-earlier",
                Some(logical_name_id),
                None,
                Some("ethereum-mainnet"),
                Some(500),
                Some("0x500"),
                Some("0xtx500"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "resource-earlier",
                None,
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(501),
                Some("0x501"),
                Some("0xtx501"),
                Some(0),
                CanonicalityState::Canonical,
            ),
            history_event(
                "surface-latest",
                Some(logical_name_id),
                Some(other_resource_id),
                Some("ethereum-mainnet"),
                Some(503),
                Some("0x503"),
                Some("0xtx503"),
                Some(0),
                CanonicalityState::Finalized,
            ),
            history_event(
                "resource-latest",
                Some("ens:other.eth"),
                Some(resource_id),
                Some("ethereum-mainnet"),
                Some(502),
                Some("0x502"),
                Some("0xtx502"),
                Some(0),
                CanonicalityState::Safe,
            ),
        ],
    )
    .await?;

    let surface_rows = load_name_history(
        database.pool(),
        logical_name_id,
        &[resource_id],
        HistoryScope::Surface,
        true,
    )
    .await?;
    let surface_head = load_name_history_head(
        database.pool(),
        logical_name_id,
        &[resource_id],
        HistoryScope::Surface,
        true,
    )
    .await?;
    assert_eq!(surface_head, surface_rows.first().cloned());

    let resource_rows = load_name_history(
        database.pool(),
        logical_name_id,
        &[resource_id],
        HistoryScope::Resource,
        true,
    )
    .await?;
    let resource_head = load_name_history_head(
        database.pool(),
        logical_name_id,
        &[resource_id],
        HistoryScope::Resource,
        true,
    )
    .await?;
    assert_eq!(resource_head, resource_rows.first().cloned());

    database.cleanup().await
}

#[tokio::test]
async fn history_rows_expose_object_provenance_and_coverage_payloads() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0xa400);

    upsert_normalized_events(
        database.pool(),
        &[
            history_event(
                "with-payload",
                Some("ens:alice.eth"),
                Some(resource_id),
                None,
                None,
                None,
                None,
                None,
                CanonicalityState::Canonical,
            ),
            NormalizedEvent {
                after_state: json!({
                    "coverage": "invalid-scalar"
                }),
                before_state: json!({
                    "provenance": {
                        "fallback": true,
                    }
                }),
                ..history_event(
                    "payload-defaults",
                    Some("ens:alice.eth"),
                    Some(resource_id),
                    None,
                    None,
                    None,
                    None,
                    None,
                    CanonicalityState::Canonical,
                )
            },
        ],
    )
    .await?;

    let rows = load_name_history(
        database.pool(),
        "ens:alice.eth",
        &[resource_id],
        HistoryScope::Both,
        true,
    )
    .await?;

    let with_payload = rows
        .iter()
        .find(|row| row.event_identity == "with-payload")
        .context("missing with-payload row")?;
    assert_eq!(with_payload.provenance, json!({"after": "with-payload"}));
    assert_eq!(
        with_payload.coverage,
        json!({
            "status": "full",
            "event_identity": "with-payload",
        })
    );

    let defaults = rows
        .iter()
        .find(|row| row.event_identity == "payload-defaults")
        .context("missing payload-defaults row")?;
    assert_eq!(defaults.provenance, json!({"fallback": true}));
    assert_eq!(defaults.coverage, json!({}));

    database.cleanup().await
}
