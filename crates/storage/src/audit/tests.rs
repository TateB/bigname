use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use serde_json::json;
use sqlx::types::time::OffsetDateTime;
use sqlx::{
    PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

use super::*;
use crate::{
    NormalizedEvent, RawBlock, RawCallSnapshot, RawCodeHash, RawLog, RawPayloadCacheMetadataUpsert,
    RawReceipt, RawTransaction, default_database_url, list_canonical_raw_log_replay_inputs,
    list_canonical_raw_log_replay_inputs_for_block_hashes, upsert_chain_lineage_blocks,
    upsert_normalized_events, upsert_raw_blocks, upsert_raw_call_snapshots, upsert_raw_code_hashes,
    upsert_raw_logs, upsert_raw_payload_cache_metadata, upsert_raw_receipts,
    upsert_raw_transactions,
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

struct TestDatabase {
    admin_pool: PgPool,
    pool: PgPool,
    database_name: String,
}

struct ManifestAlertFixture {
    manifest_id: i64,
    root_id: Uuid,
    proxy_id: Uuid,
    expected_impl_id: Uuid,
    observed_impl_id: Uuid,
    discovery_edge_id: i64,
}

impl TestDatabase {
    async fn new() -> Result<Self> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for audit tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_storage_audit_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for audit tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect audit test pool")?;

        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for audit tests")?;

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

fn timestamp(block_number: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000 + block_number)
        .expect("test timestamp must be valid")
}

fn lineage_block(
    block_hash: &str,
    parent_hash: Option<&str>,
    block_number: i64,
    state: CanonicalityState,
) -> ChainLineageBlock {
    ChainLineageBlock {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: parent_hash.map(str::to_owned),
        block_number,
        block_timestamp: timestamp(block_number),
        logs_bloom: Some(vec![block_number as u8]),
        transactions_root: Some(format!("0xtxroot{block_number:02x}")),
        receipts_root: Some(format!("0xrcroot{block_number:02x}")),
        state_root: Some(format!("0xstroot{block_number:02x}")),
        canonicality_state: state,
    }
}

fn raw_block(block_hash: &str, parent_hash: Option<&str>, block_number: i64) -> RawBlock {
    RawBlock {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: parent_hash.map(str::to_owned),
        block_number,
        block_timestamp: timestamp(block_number),
        logs_bloom: Some(vec![block_number as u8]),
        transactions_root: Some(format!("0xtxroot{block_number:02x}")),
        receipts_root: Some(format!("0xrcroot{block_number:02x}")),
        state_root: Some(format!("0xstroot{block_number:02x}")),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn raw_transaction(block_hash: &str, block_number: i64) -> RawTransaction {
    RawTransaction {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xtx{block_number:02x}"),
        transaction_index: 0,
        from_address: "0x0000000000000000000000000000000000000001".to_owned(),
        to_address: Some("0x0000000000000000000000000000000000000002".to_owned()),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn raw_receipt(block_hash: &str, block_number: i64) -> RawReceipt {
    RawReceipt {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xtx{block_number:02x}"),
        transaction_index: 0,
        contract_address: None,
        status: Some(true),
        gas_used: Some(21_000),
        cumulative_gas_used: Some(21_000),
        logs_bloom: Some(vec![0xaa]),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn raw_log(block_hash: &str, block_number: i64, log_index: i64) -> RawLog {
    raw_log_with_state(
        block_hash,
        block_number,
        log_index,
        CanonicalityState::Canonical,
    )
}

fn raw_log_with_state(
    block_hash: &str,
    block_number: i64,
    log_index: i64,
    canonicality_state: CanonicalityState,
) -> RawLog {
    RawLog {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xtx{block_number:02x}"),
        transaction_index: 0,
        log_index,
        emitting_address: "0x0000000000000000000000000000000000000003".to_owned(),
        topics: vec!["0xtopic0".to_owned()],
        data: vec![0xde, 0xad],
        canonicality_state,
    }
}

fn raw_code_hash(block_hash: &str, block_number: i64) -> RawCodeHash {
    raw_code_hash_for_address(
        block_hash,
        block_number,
        "0x0000000000000000000000000000000000000003",
        &format!("0xcode{block_number:02x}"),
        CanonicalityState::Canonical,
    )
}

fn raw_code_hash_for_address(
    block_hash: &str,
    block_number: i64,
    contract_address: &str,
    code_hash: &str,
    canonicality_state: CanonicalityState,
) -> RawCodeHash {
    RawCodeHash {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        contract_address: contract_address.to_owned(),
        code_hash: code_hash.to_owned(),
        code_byte_length: 123,
        canonicality_state,
    }
}

async fn insert_live_manifest_audit_fixture(pool: &PgPool) -> Result<()> {
    let root_id = Uuid::from_u128(0x0e7ec7ace00000000000000000001001);
    let proxy_id = Uuid::from_u128(0x0e7ec7ace00000000000000000001002);
    let expected_impl_id = Uuid::from_u128(0x0e7ec7ace00000000000000000001003);
    let observed_impl_id = Uuid::from_u128(0x0e7ec7ace00000000000000000001004);

    for (contract_instance_id, address) in [
        (root_id, "0x0000000000000000000000000000000000000001"),
        (proxy_id, "0x00000000000000000000000000000000000000aa"),
        (
            expected_impl_id,
            "0x00000000000000000000000000000000000000dd",
        ),
        (
            observed_impl_id,
            "0x00000000000000000000000000000000000000ee",
        ),
    ] {
        sqlx::query(
            r#"
            INSERT INTO contract_instances (
                contract_instance_id,
                chain_id,
                contract_kind,
                provenance
            )
            VALUES ($1, 'eth-mainnet', 'contract', '{}'::JSONB)
            "#,
        )
        .bind(contract_instance_id)
        .execute(pool)
        .await
        .context("failed to insert live audit contract instance")?;

        sqlx::query(
            r#"
            INSERT INTO contract_instance_addresses (
                contract_instance_id,
                chain_id,
                address,
                active_from_block_number,
                provenance
            )
            VALUES ($1, 'eth-mainnet', $2, 10, '{}'::JSONB)
            "#,
        )
        .bind(contract_instance_id)
        .bind(address)
        .execute(pool)
        .await
        .context("failed to insert live audit contract address")?;
    }

    let manifest_id = sqlx::query_scalar::<_, i64>(
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
        VALUES (
            3,
            'ens',
            'ens_v2_registry_l1',
            'eth-mainnet',
            'ens_v2',
            'active',
            'ensip15@ens-normalize-0.1.1',
            'manifests/ens/ens_v2_registry_l1/v1.toml',
            '{"rollout_status":"active"}'::JSONB
        )
        RETURNING manifest_id
        "#,
    )
    .fetch_one(pool)
    .await
    .context("failed to insert live audit manifest")?;

    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances (
            manifest_id,
            declaration_kind,
            declaration_name,
            contract_instance_id,
            declared_address,
            code_hash,
            abi_ref
        )
        VALUES ($1, 'root', 'RootRegistry', $2, '0x0000000000000000000000000000000000000001', '0xroot-expected', 'abis/root.json')
        "#,
    )
    .bind(manifest_id)
    .bind(root_id)
    .execute(pool)
    .await
    .context("failed to insert live audit root declaration")?;

    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances (
            manifest_id,
            declaration_kind,
            declaration_name,
            contract_instance_id,
            declared_address,
            code_hash,
            role,
            proxy_kind,
            implementation_contract_instance_id,
            declared_implementation_address
        )
        VALUES (
            $1,
            'contract',
            'registry',
            $2,
            '0x00000000000000000000000000000000000000aa',
            '0xproxy-current',
            'registry',
            'erc1967',
            $3,
            '0x00000000000000000000000000000000000000dd'
        )
        "#,
    )
    .bind(manifest_id)
    .bind(proxy_id)
    .bind(expected_impl_id)
    .execute(pool)
    .await
    .context("failed to insert live audit proxy declaration")?;

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
            provenance
        )
        VALUES (
            'eth-mainnet',
            'proxy_implementation',
            $1,
            $2,
            'manifest_declared_proxy',
            $3,
            'observed',
            20,
            '{"slot":"eip1967.proxy.implementation"}'::JSONB
        )
        "#,
    )
    .bind(proxy_id)
    .bind(observed_impl_id)
    .bind(manifest_id)
    .execute(pool)
    .await
    .context("failed to insert live audit proxy edge")?;

    upsert_raw_code_hashes(
        pool,
        &[
            raw_code_hash_for_address(
                "0xroot100",
                100,
                "0x0000000000000000000000000000000000000001",
                "0xroot-old",
                CanonicalityState::Canonical,
            ),
            raw_code_hash_for_address(
                "0xroot101",
                101,
                "0x0000000000000000000000000000000000000001",
                "0xroot-observed",
                CanonicalityState::Finalized,
            ),
            raw_code_hash_for_address(
                "0xroot102",
                102,
                "0x0000000000000000000000000000000000000001",
                "0xroot-orphaned",
                CanonicalityState::Orphaned,
            ),
            raw_code_hash_for_address(
                "0xproxy100",
                100,
                "0x00000000000000000000000000000000000000aa",
                "0xproxy-current",
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;

    Ok(())
}

async fn insert_manifest_alert_persistence_fixture(pool: &PgPool) -> Result<ManifestAlertFixture> {
    let root_id = Uuid::from_u128(0x0e7ec7ace00000000000000000002001);
    let proxy_id = Uuid::from_u128(0x0e7ec7ace00000000000000000002002);
    let expected_impl_id = Uuid::from_u128(0x0e7ec7ace00000000000000000002003);
    let observed_impl_id = Uuid::from_u128(0x0e7ec7ace00000000000000000002004);

    for contract_instance_id in [root_id, proxy_id, expected_impl_id, observed_impl_id] {
        sqlx::query(
            r#"
            INSERT INTO contract_instances (
                contract_instance_id,
                chain_id,
                contract_kind,
                provenance
            )
            VALUES ($1, 'eth-mainnet', 'contract', '{}'::JSONB)
            "#,
        )
        .bind(contract_instance_id)
        .execute(pool)
        .await
        .context("failed to insert manifest alert contract instance")?;
    }

    let manifest_id = sqlx::query_scalar::<_, i64>(
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
        VALUES (
            7,
            'ens',
            'ens_v1_registry_l1',
            'eth-mainnet',
            'ens_v1',
            'active',
            'ensip15@ens-normalize-0.1.1',
            'manifests/ens/ens_v1_registry_l1/v7.toml',
            '{"rollout_status":"active"}'::JSONB
        )
        RETURNING manifest_id
        "#,
    )
    .fetch_one(pool)
    .await
    .context("failed to insert manifest alert manifest")?;

    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances (
            manifest_id,
            declaration_kind,
            declaration_name,
            contract_instance_id,
            declared_address,
            code_hash,
            abi_ref
        )
        VALUES ($1, 'root', 'Registry', $2, '0x0000000000000000000000000000000000000001', '0xexpected', 'abis/registry.json')
        "#,
    )
    .bind(manifest_id)
    .bind(root_id)
    .execute(pool)
    .await
    .context("failed to insert manifest alert root declaration")?;

    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances (
            manifest_id,
            declaration_kind,
            declaration_name,
            contract_instance_id,
            declared_address,
            role,
            proxy_kind,
            implementation_contract_instance_id,
            declared_implementation_address
        )
        VALUES (
            $1,
            'contract',
            'name_wrapper',
            $2,
            '0x00000000000000000000000000000000000000aa',
            'name_wrapper',
            'erc1967',
            $3,
            '0x00000000000000000000000000000000000000dd'
        )
        "#,
    )
    .bind(manifest_id)
    .bind(proxy_id)
    .bind(expected_impl_id)
    .execute(pool)
    .await
    .context("failed to insert manifest alert proxy declaration")?;

    let discovery_edge_id = sqlx::query_scalar::<_, i64>(
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
            provenance
        )
        VALUES (
            'eth-mainnet',
            'proxy_implementation',
            $1,
            $2,
            'manifest_declared_proxy',
            $3,
            'observed',
            120,
            '{"slot":"eip1967.proxy.implementation"}'::JSONB
        )
        RETURNING discovery_edge_id
        "#,
    )
    .bind(proxy_id)
    .bind(observed_impl_id)
    .bind(manifest_id)
    .fetch_one(pool)
    .await
    .context("failed to insert manifest alert discovery edge")?;

    Ok(ManifestAlertFixture {
        manifest_id,
        root_id,
        proxy_id,
        expected_impl_id,
        observed_impl_id,
        discovery_edge_id,
    })
}

fn raw_call_snapshot(block_hash: &str, block_number: i64) -> RawCallSnapshot {
    RawCallSnapshot {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        request_hash: format!("0xrequest{block_number:02x}"),
        request_payload: json!({
            "to": "0x0000000000000000000000000000000000000003",
            "data": "0x"
        }),
        response_hash: format!("0xresponse{block_number:02x}"),
        response_payload: json!({ "result": "0x01" }),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn raw_payload_cache_metadata(
    block_hash: &str,
    block_number: i64,
    retained_digest: &str,
) -> RawPayloadCacheMetadataUpsert {
    RawPayloadCacheMetadataUpsert {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        payload_kind: "full_block".to_owned(),
        digest_algorithm: Some("sha256".to_owned()),
        retained_digest: Some(retained_digest.to_owned()),
        block_number: Some(block_number),
        payload_size_bytes: 2048,
        content_type: Some("application/json".to_owned()),
        content_encoding: Some("identity".to_owned()),
        cache_metadata: json!({
            "source": "audit-test",
            "scope": "block"
        }),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn normalized_event(block_hash: &str, block_number: i64, index: i64) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!("eth-mainnet:{block_hash}:event:{index}"),
        namespace: "ens".to_owned(),
        logical_name_id: Some("ens:alice.eth".to_owned()),
        resource_id: None,
        event_kind: "NameRegistered".to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("eth-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(block_hash.to_owned()),
        transaction_hash: Some(format!("0xtx{block_number:02x}")),
        log_index: Some(index),
        raw_fact_ref: json!({ "raw_log": index }),
        derivation_kind: "log".to_owned(),
        canonicality_state: CanonicalityState::Canonical,
        before_state: json!({}),
        after_state: json!({ "name": "alice.eth" }),
    }
}

fn manifest_code_hash_drift_alert_observation(
    fixture: &ManifestAlertFixture,
    observation_identity: &str,
    address: &str,
) -> ManifestDriftAlertObservationCreate {
    ManifestDriftAlertObservationCreate {
        observation_identity: observation_identity.to_owned(),
        alert_kind: ManifestDriftAlertKind::CodeHashDrift,
        lifecycle_status: ManifestDriftAlertLifecycleStatus::Active,
        namespace: "ens".to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 7,
        source_manifest_id: Some(fixture.manifest_id),
        chain_id: "eth-mainnet".to_owned(),
        contract_instance_id: fixture.root_id,
        proxy_contract_instance_id: None,
        expected_implementation_contract_instance_id: None,
        observed_implementation_contract_instance_id: None,
        discovery_edge_id: None,
        expected_code_hash: Some("0xexpected".to_owned()),
        observed_code_hash: Some("0xobserved".to_owned()),
        observed_code_byte_length: Some(512),
        observed_block_number: Some(123),
        observed_block_hash: Some("0xalertblock".to_owned()),
        observed_canonicality_state: Some(CanonicalityState::Canonical),
        raw_fact_ref: json!({
            "manifest_id": fixture.manifest_id,
            "contract_instance_id": fixture.root_id.to_string(),
            "address": address,
            "raw_code_hash_id": 88,
            "observed_block_number": 123,
            "observed_block_hash": "0xalertblock"
        }),
        expected_material: json!({
            "expected_code_hash": "0xexpected"
        }),
        observed_material: json!({
            "observed_code_hash": "0xobserved",
            "observed_code_byte_length": 512
        }),
        watch_plan_metadata: json!({
            "watched_source": "manifest_contract",
            "source_manifest_id": fixture.manifest_id
        }),
        alert_metadata: json!({
            "declaration_kind": "contract",
            "declaration_name": "registry",
            "address": address,
        }),
        remediation_status: None,
        remediation_metadata: None,
        first_observed_at: timestamp(123),
        last_observed_at: timestamp(124),
        remediated_at: None,
    }
}

fn manifest_proxy_implementation_alert_observation(
    fixture: &ManifestAlertFixture,
    observation_identity: &str,
) -> ManifestDriftAlertObservationCreate {
    ManifestDriftAlertObservationCreate {
        observation_identity: observation_identity.to_owned(),
        alert_kind: ManifestDriftAlertKind::ProxyImplementation,
        lifecycle_status: ManifestDriftAlertLifecycleStatus::Remediated,
        namespace: "ens".to_owned(),
        source_family: "ens_v1_wrapper_l1".to_owned(),
        manifest_version: 9,
        source_manifest_id: Some(fixture.manifest_id),
        chain_id: "eth-mainnet".to_owned(),
        contract_instance_id: fixture.proxy_id,
        proxy_contract_instance_id: Some(fixture.proxy_id),
        expected_implementation_contract_instance_id: Some(fixture.expected_impl_id),
        observed_implementation_contract_instance_id: Some(fixture.observed_impl_id),
        discovery_edge_id: Some(fixture.discovery_edge_id),
        expected_code_hash: None,
        observed_code_hash: None,
        observed_code_byte_length: None,
        observed_block_number: None,
        observed_block_hash: None,
        observed_canonicality_state: None,
        raw_fact_ref: json!({
            "manifest_id": fixture.manifest_id,
            "discovery_edge_id": fixture.discovery_edge_id,
            "proxy_contract_instance_id": fixture.proxy_id.to_string(),
            "expected_implementation_contract_instance_id": fixture.expected_impl_id.to_string(),
            "observed_implementation_contract_instance_id": fixture.observed_impl_id.to_string()
        }),
        expected_material: json!({
            "expected_implementation_address": "0xexpectedimpl"
        }),
        observed_material: json!({
            "implementation_address": "0ximpl"
        }),
        watch_plan_metadata: json!({
            "active_from_block_number": 120,
            "active_to_block_number": null
        }),
        alert_metadata: json!({
            "proxy_address": "0xproxy",
            "declaration_name": "name_wrapper",
            "role": "name_wrapper",
            "proxy_kind": "eip1967",
            "admission": "observed",
            "provenance": {
                "slot": "eip1967.proxy.implementation"
            }
        }),
        remediation_status: Some("manifest_updated".to_owned()),
        remediation_metadata: Some(json!({
            "ticket": "OPS-42",
            "resolved_by": "manifest-sync"
        })),
        first_observed_at: timestamp(125),
        last_observed_at: timestamp(126),
        remediated_at: Some(timestamp(127)),
    }
}

#[tokio::test]
async fn audit_reports_lineage_status_and_block_scoped_counts() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_chain_lineage_blocks(
        database.pool(),
        &[lineage_block(
            "0xaaa",
            Some("0x999"),
            100,
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    upsert_raw_blocks(database.pool(), &[raw_block("0xaaa", Some("0x999"), 100)]).await?;
    upsert_raw_transactions(database.pool(), &[raw_transaction("0xaaa", 100)]).await?;
    upsert_raw_receipts(database.pool(), &[raw_receipt("0xaaa", 100)]).await?;
    upsert_raw_logs(
        database.pool(),
        &[raw_log("0xaaa", 100, 0), raw_log("0xaaa", 100, 1)],
    )
    .await?;
    upsert_raw_code_hashes(database.pool(), &[raw_code_hash("0xaaa", 100)]).await?;
    upsert_raw_call_snapshots(database.pool(), &[raw_call_snapshot("0xaaa", 100)]).await?;
    upsert_normalized_events(
        database.pool(),
        &[
            normalized_event("0xaaa", 100, 0),
            normalized_event("0xaaa", 100, 1),
        ],
    )
    .await?;

    let inspection = inspect_block_canonicality(database.pool(), "eth-mainnet", "0xaaa").await?;

    assert_eq!(inspection.status, CanonicalityInspectionStatus::Canonical);
    assert_eq!(inspection.lineage_state, Some(CanonicalityState::Canonical));
    assert_eq!(inspection.parent_hash.as_deref(), Some("0x999"));
    assert_eq!(inspection.block_number, Some(100));
    assert_eq!(inspection.raw_fact_counts.raw_block_count, 1);
    assert_eq!(inspection.raw_fact_counts.raw_code_hash_count, 1);
    assert_eq!(inspection.raw_fact_counts.raw_transaction_count, 1);
    assert_eq!(inspection.raw_fact_counts.raw_receipt_count, 1);
    assert_eq!(inspection.raw_fact_counts.raw_log_count, 2);
    assert_eq!(inspection.raw_fact_counts.raw_call_snapshot_count, 1);
    assert_eq!(inspection.raw_fact_counts.total(), 7);
    assert_eq!(inspection.normalized_event_count, 2);

    database.cleanup().await
}

#[tokio::test]
async fn audit_lists_payload_cache_metadata_without_payload_bytes() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_raw_payload_cache_metadata(
        database.pool(),
        &[raw_payload_cache_metadata("0xaaa", 100, "0xdigest")],
    )
    .await?;

    let metadata =
        list_raw_payload_cache_audit_metadata(database.pool(), "eth-mainnet", "0xaaa").await?;

    assert_eq!(metadata.len(), 1);
    assert_eq!(metadata[0].payload_kind, "full_block");
    assert_eq!(metadata[0].digest_algorithm.as_deref(), Some("sha256"));
    assert_eq!(metadata[0].retained_digest.as_deref(), Some("0xdigest"));
    assert_eq!(metadata[0].block_number, Some(100));
    assert_eq!(metadata[0].payload_size_bytes, 2048);
    assert_eq!(
        metadata[0].content_type.as_deref(),
        Some("application/json")
    );
    assert_eq!(metadata[0].content_encoding.as_deref(), Some("identity"));
    assert_eq!(metadata[0].cache_metadata["source"], "audit-test");
    assert_eq!(metadata[0].canonicality_state, CanonicalityState::Canonical);

    database.cleanup().await
}

#[tokio::test]
async fn audit_reports_missing_block_status_without_counts() -> Result<()> {
    let database = TestDatabase::new().await?;

    let inspection =
        inspect_block_canonicality(database.pool(), "eth-mainnet", "0xmissing").await?;

    assert_eq!(inspection.status, CanonicalityInspectionStatus::Missing);
    assert_eq!(inspection.lineage_state, None);
    assert_eq!(inspection.parent_hash, None);
    assert_eq!(inspection.block_number, None);
    assert_eq!(inspection.raw_fact_counts, RawFactAuditCounts::default());
    assert_eq!(inspection.normalized_event_count, 0);

    database.cleanup().await
}

#[tokio::test]
async fn audit_range_reports_stored_lineage_order_and_orphaned_status() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_chain_lineage_blocks(
        database.pool(),
        &[
            lineage_block("0x010", None, 10, CanonicalityState::Canonical),
            lineage_block("0x011", Some("0x010"), 11, CanonicalityState::Orphaned),
            lineage_block("0x012", Some("0x011"), 12, CanonicalityState::Safe),
        ],
    )
    .await?;

    let inspections = inspect_canonicality_range(database.pool(), "eth-mainnet", 10, 12).await?;

    assert_eq!(
        inspections
            .iter()
            .map(|inspection| (
                inspection.block_hash.as_str(),
                inspection.status,
                inspection.block_number
            ))
            .collect::<Vec<_>>(),
        vec![
            ("0x010", CanonicalityInspectionStatus::Canonical, Some(10)),
            ("0x011", CanonicalityInspectionStatus::Orphaned, Some(11)),
            ("0x012", CanonicalityInspectionStatus::Safe, Some(12)),
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn stored_lineage_range_lists_only_stored_rows_in_stable_order() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_chain_lineage_blocks(
        database.pool(),
        &[
            lineage_block("0x012b", Some("0x010"), 12, CanonicalityState::Safe),
            lineage_block("0x010", None, 10, CanonicalityState::Canonical),
            lineage_block("0x012a", Some("0x010"), 12, CanonicalityState::Orphaned),
        ],
    )
    .await?;

    let rows = list_stored_lineage_range(database.pool(), "eth-mainnet", 10, 12).await?;

    assert_eq!(
        rows.iter()
            .map(|row| (
                row.block_number,
                row.block_hash.as_str(),
                row.parent_hash.as_deref(),
                row.canonicality_state
            ))
            .collect::<Vec<_>>(),
        vec![
            (10, "0x010", None, CanonicalityState::Canonical),
            (12, "0x012a", Some("0x010"), CanonicalityState::Orphaned),
            (12, "0x012b", Some("0x010"), CanonicalityState::Safe),
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn manifest_drift_audit_lists_stored_alert_observations() -> Result<()> {
    let database = TestDatabase::new().await?;
    let fixture = insert_manifest_alert_persistence_fixture(database.pool()).await?;

    upsert_manifest_drift_alert_observation(
        database.pool(),
        &manifest_proxy_implementation_alert_observation(&fixture, "manifest_alert:proxy"),
    )
    .await?;
    upsert_manifest_drift_alert_observation(
        database.pool(),
        &manifest_code_hash_drift_alert_observation(
            &fixture,
            "manifest_alert:code:z",
            "0xregistry-z",
        ),
    )
    .await?;
    upsert_manifest_drift_alert_observation(
        database.pool(),
        &manifest_code_hash_drift_alert_observation(
            &fixture,
            "manifest_alert:code:a",
            "0xregistry-a",
        ),
    )
    .await?;

    let inspection = list_manifest_drift_alert_observations(database.pool()).await?;

    assert_eq!(inspection.total_alert_count(), 3);
    assert_eq!(
        inspection
            .code_hash_drift_alerts
            .iter()
            .map(|alert| alert.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec!["manifest_alert:code:a", "manifest_alert:code:z"]
    );
    for alert in &inspection.code_hash_drift_alerts {
        assert_eq!(alert.alert_kind, ManifestDriftAlertKind::CodeHashDrift);
        assert_eq!(alert.alert_kind.event_kind(), "ManifestCodeHashDriftAlert");
        assert_eq!(alert.alert_kind.observation_kind(), "manifest_drift");
        assert_eq!(alert.alert_kind.alert_type(), "manifest_code_hash_drift");
        assert_eq!(alert.source_family, "ens_v1_registry_l1");
        assert_eq!(alert.manifest_version, 7);
        assert_eq!(alert.chain_id.as_deref(), Some("eth-mainnet"));
        assert_eq!(alert.block_number, Some(123));
        assert_eq!(alert.block_hash.as_deref(), Some("0xalertblock"));
        assert_eq!(alert.canonicality_state, CanonicalityState::Canonical);
        assert_eq!(alert.source_manifest_id, Some(fixture.manifest_id));
        assert_eq!(
            alert.alert_state["expected_code_hash"].as_str(),
            Some("0xexpected")
        );
        assert_eq!(
            alert.alert_state["observed_code_hash"].as_str(),
            Some("0xobserved")
        );
    }
    assert_eq!(
        inspection.code_hash_drift_alerts[0].alert_state["address"].as_str(),
        Some("0xregistry-a")
    );
    assert_eq!(
        inspection.code_hash_drift_alerts[1].alert_state["address"].as_str(),
        Some("0xregistry-z")
    );
    assert_eq!(inspection.proxy_implementation_alerts.len(), 1);
    let proxy_alert = &inspection.proxy_implementation_alerts[0];
    assert_eq!(
        proxy_alert.alert_kind,
        ManifestDriftAlertKind::ProxyImplementation
    );
    assert_eq!(
        proxy_alert.alert_kind.observation_kind(),
        "proxy_implementation_drift"
    );
    assert_eq!(proxy_alert.event_identity, "manifest_alert:proxy");
    assert_eq!(proxy_alert.source_family, "ens_v1_wrapper_l1");
    assert_eq!(proxy_alert.manifest_version, 9);
    assert_eq!(proxy_alert.source_manifest_id, Some(fixture.manifest_id));
    assert_eq!(proxy_alert.canonicality_state, CanonicalityState::Observed);
    assert_eq!(proxy_alert.alert_state["proxy_address"], "0xproxy");
    assert_eq!(proxy_alert.alert_state["implementation_address"], "0ximpl");
    let proxy_id = fixture.proxy_id.to_string();
    let observed_impl_id = fixture.observed_impl_id.to_string();
    assert_eq!(
        proxy_alert.alert_state["proxy_contract_instance_id"].as_str(),
        Some(proxy_id.as_str())
    );
    assert_eq!(
        proxy_alert.alert_state["implementation_contract_instance_id"].as_str(),
        Some(observed_impl_id.as_str())
    );
    assert_eq!(
        proxy_alert.raw_fact_ref["discovery_edge_id"],
        fixture.discovery_edge_id
    );
    assert_eq!(
        proxy_alert.alert_state["remediation_metadata"]["ticket"],
        "OPS-42"
    );

    database.cleanup().await
}

#[tokio::test]
async fn manifest_drift_alert_persistence_is_idempotent_and_operational_only() -> Result<()> {
    let database = TestDatabase::new().await?;
    let fixture = insert_manifest_alert_persistence_fixture(database.pool()).await?;
    let code_alert = manifest_code_hash_drift_alert_observation(
        &fixture,
        "manifest_alert:readonly:code",
        "0xregistry",
    );
    let proxy_alert =
        manifest_proxy_implementation_alert_observation(&fixture, "manifest_alert:readonly:proxy");

    let before_events = load_normalized_event_total(database.pool()).await?;
    let before_manifests = load_manifest_version_total(database.pool()).await?;
    let before_manifest_contracts = load_manifest_contract_instance_total(database.pool()).await?;
    let before_discovery_edges = load_discovery_edge_total(database.pool()).await?;
    let before_contract_instances = load_contract_instance_total(database.pool()).await?;

    let inserted_code =
        upsert_manifest_drift_alert_observation(database.pool(), &code_alert).await?;
    let repeated_code =
        upsert_manifest_drift_alert_observation(database.pool(), &code_alert).await?;
    let inserted_proxy =
        upsert_manifest_drift_alert_observation(database.pool(), &proxy_alert).await?;
    let repeated_proxy =
        upsert_manifest_drift_alert_observation(database.pool(), &proxy_alert).await?;

    assert_eq!(repeated_code, inserted_code);
    assert_eq!(repeated_proxy, inserted_proxy);
    let proxy_id = fixture.proxy_id.to_string();
    let observed_impl_id = fixture.observed_impl_id.to_string();
    assert_eq!(
        inserted_proxy.alert_state["proxy_contract_instance_id"].as_str(),
        Some(proxy_id.as_str())
    );
    assert_eq!(
        inserted_proxy.alert_state["implementation_contract_instance_id"].as_str(),
        Some(observed_impl_id.as_str())
    );

    let inspection = list_manifest_drift_alert_observations(database.pool()).await?;
    assert_eq!(inspection.total_alert_count(), 2);
    assert_eq!(
        load_manifest_alert_observation_total(database.pool()).await?,
        2
    );
    assert_eq!(
        load_normalized_event_total(database.pool()).await?,
        before_events
    );
    assert_eq!(
        load_manifest_version_total(database.pool()).await?,
        before_manifests
    );
    assert_eq!(
        load_manifest_contract_instance_total(database.pool()).await?,
        before_manifest_contracts
    );
    assert_eq!(
        load_discovery_edge_total(database.pool()).await?,
        before_discovery_edges
    );
    assert_eq!(
        load_contract_instance_total(database.pool()).await?,
        before_contract_instances
    );

    database.cleanup().await
}

#[tokio::test]
async fn manifest_drift_audit_computes_live_candidates_without_persistence() -> Result<()> {
    let database = TestDatabase::new().await?;
    insert_live_manifest_audit_fixture(database.pool()).await?;

    let before_events = load_normalized_event_total(database.pool()).await?;
    let before_alerts = load_manifest_alert_observation_total(database.pool()).await?;
    let audit =
        ManifestDriftAlertInspection::compute_live_manifest_drift_audit(database.pool()).await?;
    let after_events = load_normalized_event_total(database.pool()).await?;
    let after_alerts = load_manifest_alert_observation_total(database.pool()).await?;

    assert_eq!(audit["command"], "manifest-drift audit");
    assert_eq!(audit["read_only"], true);
    assert_eq!(audit["persistence"]["writes_normalized_events"], false);
    assert_eq!(audit["persistence"]["writes_alert_table"], false);
    assert_eq!(audit["counts"]["manifest_code_hash_drift"], 1);
    assert_eq!(audit["counts"]["manifest_proxy_implementation"], 1);
    assert_eq!(audit["counts"]["total"], 2);
    assert_eq!(
        ManifestDriftAlertInspection::audit_total_alert_count(&audit)?,
        2
    );

    let code_alert = &audit["manifest_code_hash_drift_alerts"][0];
    assert_eq!(code_alert["alert_type"], "manifest_code_hash_drift");
    assert_eq!(code_alert["event_kind"], "ManifestCodeHashDriftAlert");
    assert_eq!(code_alert["namespace"], "ens");
    assert_eq!(code_alert["source_family"], "ens_v2_registry_l1");
    assert_eq!(code_alert["manifest_version"], 3);
    assert_eq!(code_alert["chain"], "eth-mainnet");
    assert_eq!(code_alert["lifecycle"]["persisted"], false);
    assert_eq!(code_alert["declaration"]["kind"], "root");
    assert_eq!(code_alert["declaration"]["name"], "RootRegistry");
    assert_eq!(code_alert["code_hash"]["expected"], "0xroot-expected");
    assert_eq!(code_alert["code_hash"]["observed"], "0xroot-observed");
    assert_eq!(code_alert["observed_block"]["number"], 101);
    assert_eq!(code_alert["observed_block"]["hash"], "0xroot101");
    assert_eq!(
        code_alert["observed_block"]["canonicality_state"],
        "finalized"
    );
    assert_eq!(code_alert["watched_target"]["source"], "manifest_root");

    let proxy_alert = &audit["proxy_implementation_alerts"][0];
    assert_eq!(
        proxy_alert["alert_type"],
        "manifest_proxy_implementation_edge"
    );
    assert_eq!(
        proxy_alert["event_kind"],
        "ManifestProxyImplementationAlert"
    );
    assert_eq!(proxy_alert["candidate_reason"], "implementation_mismatch");
    assert_eq!(proxy_alert["declaration"]["name"], "registry");
    assert_eq!(proxy_alert["declaration"]["role"], "registry");
    assert_eq!(proxy_alert["declaration"]["proxy_kind"], "erc1967");
    assert_eq!(
        proxy_alert["expected_implementation"]["address"],
        "0x00000000000000000000000000000000000000dd"
    );
    assert_eq!(
        proxy_alert["observed_implementation"]["address"],
        "0x00000000000000000000000000000000000000ee"
    );
    assert_eq!(proxy_alert["implementation_edge"]["admission"], "observed");
    assert_eq!(
        proxy_alert["implementation_edge"]["provenance"]["slot"],
        "eip1967.proxy.implementation"
    );

    assert_eq!(after_events, before_events);
    assert_eq!(after_alerts, before_alerts);

    database.cleanup().await
}

#[tokio::test]
async fn manifest_drift_audit_reports_clean_live_audit_without_persistence() -> Result<()> {
    let database = TestDatabase::new().await?;

    let before_events = load_normalized_event_total(database.pool()).await?;
    let before_alerts = load_manifest_alert_observation_total(database.pool()).await?;
    let audit =
        ManifestDriftAlertInspection::compute_live_manifest_drift_audit(database.pool()).await?;
    let after_events = load_normalized_event_total(database.pool()).await?;
    let after_alerts = load_manifest_alert_observation_total(database.pool()).await?;

    assert_eq!(audit["command"], "manifest-drift audit");
    assert_eq!(audit["read_only"], true);
    assert_eq!(audit["persistence"]["writes_normalized_events"], false);
    assert_eq!(audit["persistence"]["writes_alert_table"], false);
    assert_eq!(audit["counts"]["manifest_code_hash_drift"], 0);
    assert_eq!(audit["counts"]["manifest_proxy_implementation"], 0);
    assert_eq!(audit["counts"]["total"], 0);
    assert_eq!(
        ManifestDriftAlertInspection::audit_total_alert_count(&audit)?,
        0
    );
    assert_eq!(audit["manifest_code_hash_drift_alerts"], json!([]));
    assert_eq!(audit["proxy_implementation_alerts"], json!([]));
    assert_eq!(after_events, before_events);
    assert_eq!(after_alerts, before_alerts);

    database.cleanup().await
}

#[tokio::test]
async fn raw_log_replay_inputs_include_only_canonical_persisted_facts() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_chain_lineage_blocks(
        database.pool(),
        &[
            lineage_block("0x100", None, 100, CanonicalityState::Canonical),
            lineage_block("0x101", Some("0x100"), 101, CanonicalityState::Safe),
            lineage_block("0x102", Some("0x101"), 102, CanonicalityState::Finalized),
            lineage_block("0x103", Some("0x102"), 103, CanonicalityState::Observed),
            lineage_block("0x104", Some("0x103"), 104, CanonicalityState::Orphaned),
        ],
    )
    .await?;

    upsert_raw_logs(
        database.pool(),
        &[
            raw_log("0x100", 100, 0),
            raw_log("0x101", 101, 0),
            raw_log("0x102", 102, 0),
            raw_log("0x103", 103, 0),
            raw_log("0x104", 104, 0),
            raw_log_with_state("0x102", 102, 9, CanonicalityState::Orphaned),
        ],
    )
    .await?;

    let range_inputs =
        list_canonical_raw_log_replay_inputs(database.pool(), "eth-mainnet", 100, 104).await?;

    assert_eq!(
        range_inputs
            .iter()
            .map(|input| (
                input.block_hash.as_str(),
                input.lineage_canonicality_state,
                input.log_index,
                input.raw_canonicality_state
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                "0x100",
                CanonicalityState::Canonical,
                0,
                CanonicalityState::Canonical
            ),
            (
                "0x101",
                CanonicalityState::Safe,
                0,
                CanonicalityState::Canonical
            ),
            (
                "0x102",
                CanonicalityState::Finalized,
                0,
                CanonicalityState::Canonical
            ),
        ]
    );

    let hash_inputs = list_canonical_raw_log_replay_inputs_for_block_hashes(
        database.pool(),
        "eth-mainnet",
        &["0x102".to_owned(), "0x100".to_owned(), "0x103".to_owned()],
    )
    .await?;

    assert_eq!(
        hash_inputs
            .iter()
            .map(|input| (
                input.block_number,
                input.block_hash.as_str(),
                input.log_index
            ))
            .collect::<Vec<_>>(),
        vec![(100, "0x100", 0), (102, "0x102", 0)]
    );

    database.cleanup().await
}

async fn load_normalized_event_total(pool: &PgPool) -> Result<i64> {
    let row = sqlx::query(
        r#"
        SELECT COUNT(*)::BIGINT AS event_count
        FROM normalized_events
        "#,
    )
    .fetch_one(pool)
    .await
    .context("failed to load normalized-event total")?;

    row.try_get("event_count")
        .context("missing normalized-event total")
}

async fn load_manifest_alert_observation_total(pool: &PgPool) -> Result<i64> {
    load_table_count(
        pool,
        "manifest_alert_observations",
        "manifest_alert_observation_count",
    )
    .await
}

async fn load_manifest_version_total(pool: &PgPool) -> Result<i64> {
    load_table_count(pool, "manifest_versions", "manifest_version_count").await
}

async fn load_manifest_contract_instance_total(pool: &PgPool) -> Result<i64> {
    load_table_count(
        pool,
        "manifest_contract_instances",
        "manifest_contract_instance_count",
    )
    .await
}

async fn load_discovery_edge_total(pool: &PgPool) -> Result<i64> {
    load_table_count(pool, "discovery_edges", "discovery_edge_count").await
}

async fn load_contract_instance_total(pool: &PgPool) -> Result<i64> {
    load_table_count(pool, "contract_instances", "contract_instance_count").await
}

async fn load_table_count(pool: &PgPool, table_name: &str, column_name: &str) -> Result<i64> {
    let row = sqlx::query(&format!(
        "SELECT COUNT(*)::BIGINT AS {column_name} FROM {table_name}"
    ))
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to load {table_name} count"))?;

    row.try_get(column_name)
        .with_context(|| format!("missing {table_name} count"))
}
