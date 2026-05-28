use std::{
    collections::BTreeMap,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use bigname_storage::{
    CanonicalityState, default_database_url, load_normalized_event_counts_by_kind,
    load_normalized_events_by_namespace,
};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

use super::{
    ManifestNormalizedEventKindSyncSummary,
    constants::{
        DERIVATION_KIND_MANIFEST_SYNC, EVENT_KIND_CAPABILITY_CHANGED,
        EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED, EVENT_KIND_SOURCE_MANIFEST_UPDATED,
    },
    sync_manifest_normalized_events,
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
            .context("failed to parse database URL for manifest sync tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_adapters_manifest_sync_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for manifest sync tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect test pool for manifest sync tests")?;

        bigname_storage::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for manifest sync tests")?;

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

async fn insert_capability_flag(
    pool: &PgPool,
    manifest_id: i64,
    capability_name: &str,
    status: &str,
    notes: Option<&str>,
) -> Result<()> {
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
    .bind(capability_name)
    .bind(status)
    .bind(notes)
    .execute(pool)
    .await
    .context("failed to insert capability flag")?;
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

struct ContractSeed<'a> {
    manifest_id: i64,
    contract_instance_id: Uuid,
    declaration_name: &'a str,
    role: &'a str,
    address: &'a str,
    proxy_kind: &'a str,
    implementation_contract_instance_id: Option<Uuid>,
    implementation: Option<&'a str>,
}

async fn insert_contract(pool: &PgPool, seed: ContractSeed<'_>) -> Result<()> {
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
        VALUES ($1, 'contract', $2, $3, $4, NULL, NULL, $5, $6, $7, $8)
        "#,
    )
    .bind(seed.manifest_id)
    .bind(seed.declaration_name)
    .bind(seed.contract_instance_id)
    .bind(seed.address)
    .bind(seed.role)
    .bind(seed.proxy_kind)
    .bind(seed.implementation_contract_instance_id)
    .bind(seed.implementation)
    .execute(pool)
    .await
    .context("failed to insert manifest contract instance")?;
    Ok(())
}

#[tokio::test]
async fn sync_manifest_normalized_events_is_idempotent() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let active_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "ens",
            source_family: "ens_v2_registry_l1",
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v2",
            rollout_status: "active",
            normalizer_version: "ensip15@ens-normalize-0.1.1",
            file_path: "manifests/ens/ens_v2_registry_l1/1.toml",
        },
    )
    .await?;
    let inactive_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 2,
            namespace: "ens",
            source_family: "ens_v2_registry_l1",
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v2_shadow",
            rollout_status: "draft",
            normalizer_version: "ensip15@ens-normalize-0.1.1",
            file_path: "manifests/ens/ens_v2_registry_l1/2.toml",
        },
    )
    .await?;

    insert_capability_flag(
        database.pool(),
        active_manifest_id,
        "declared_children",
        "supported",
        Some("live"),
    )
    .await?;
    insert_capability_flag(
        database.pool(),
        active_manifest_id,
        "verified_resolution",
        "shadow",
        None,
    )
    .await?;
    insert_capability_flag(
        database.pool(),
        inactive_manifest_id,
        "declared_children",
        "unsupported",
        Some("ignored"),
    )
    .await?;

    let active_contract_instance_id = Uuid::new_v4();
    let active_implementation_contract_instance_id = Uuid::new_v4();
    let inactive_contract_instance_id = Uuid::new_v4();
    let inactive_implementation_contract_instance_id = Uuid::new_v4();
    insert_contract_instance(
        database.pool(),
        active_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        active_implementation_contract_instance_id,
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
    insert_contract_instance(
        database.pool(),
        inactive_implementation_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;

    insert_contract(
        database.pool(),
        ContractSeed {
            manifest_id: active_manifest_id,
            contract_instance_id: active_contract_instance_id,
            declaration_name: "registry",
            role: "registry",
            address: "0x00000000000000000000000000000000000000aa",
            proxy_kind: "erc1967",
            implementation_contract_instance_id: Some(active_implementation_contract_instance_id),
            implementation: Some("0x00000000000000000000000000000000000000dd"),
        },
    )
    .await?;
    insert_contract(
        database.pool(),
        ContractSeed {
            manifest_id: inactive_manifest_id,
            contract_instance_id: inactive_contract_instance_id,
            declaration_name: "registry",
            role: "registry",
            address: "0x00000000000000000000000000000000000000bb",
            proxy_kind: "erc1967",
            implementation_contract_instance_id: Some(inactive_implementation_contract_instance_id),
            implementation: Some("0x00000000000000000000000000000000000000ee"),
        },
    )
    .await?;

    let first_summary = sync_manifest_normalized_events(database.pool()).await?;
    assert_eq!(first_summary.total_synced_count, 4);
    assert_eq!(first_summary.total_inserted_count, 4);
    assert_eq!(
        first_summary.by_kind,
        BTreeMap::from([
            (
                EVENT_KIND_CAPABILITY_CHANGED.to_owned(),
                ManifestNormalizedEventKindSyncSummary {
                    synced_count: 2,
                    inserted_count: 2,
                },
            ),
            (
                EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED.to_owned(),
                ManifestNormalizedEventKindSyncSummary {
                    synced_count: 1,
                    inserted_count: 1,
                },
            ),
            (
                EVENT_KIND_SOURCE_MANIFEST_UPDATED.to_owned(),
                ManifestNormalizedEventKindSyncSummary {
                    synced_count: 1,
                    inserted_count: 1,
                },
            ),
        ])
    );

    let loaded = load_normalized_events_by_namespace(database.pool(), "ens").await?;
    assert_eq!(loaded.len(), 4);
    assert!(loaded.iter().all(|event| {
        event.canonicality_state == CanonicalityState::Finalized
            && event.derivation_kind == DERIVATION_KIND_MANIFEST_SYNC
            && event.source_manifest_id == Some(active_manifest_id)
    }));
    assert_eq!(
        loaded
            .iter()
            .map(|event| event.event_kind.as_str())
            .collect::<Vec<_>>(),
        vec![
            EVENT_KIND_SOURCE_MANIFEST_UPDATED,
            EVENT_KIND_CAPABILITY_CHANGED,
            EVENT_KIND_CAPABILITY_CHANGED,
            EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED,
        ]
    );

    let counts = load_normalized_event_counts_by_kind(database.pool(), "ens").await?;
    assert_eq!(
        counts,
        BTreeMap::from([
            (EVENT_KIND_CAPABILITY_CHANGED.to_owned(), 2_usize),
            (EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED.to_owned(), 1_usize),
            (EVENT_KIND_SOURCE_MANIFEST_UPDATED.to_owned(), 1_usize),
        ])
    );

    let second_summary = sync_manifest_normalized_events(database.pool()).await?;
    assert_eq!(second_summary.total_synced_count, 4);
    assert_eq!(second_summary.total_inserted_count, 0);
    assert_eq!(
        second_summary.by_kind,
        BTreeMap::from([
            (
                EVENT_KIND_CAPABILITY_CHANGED.to_owned(),
                ManifestNormalizedEventKindSyncSummary {
                    synced_count: 2,
                    inserted_count: 0,
                },
            ),
            (
                EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED.to_owned(),
                ManifestNormalizedEventKindSyncSummary {
                    synced_count: 1,
                    inserted_count: 0,
                },
            ),
            (
                EVENT_KIND_SOURCE_MANIFEST_UPDATED.to_owned(),
                ManifestNormalizedEventKindSyncSummary {
                    synced_count: 1,
                    inserted_count: 0,
                },
            ),
        ])
    );

    let loaded_after_rerun = load_normalized_events_by_namespace(database.pool(), "ens").await?;
    assert_eq!(loaded_after_rerun, loaded);

    database.cleanup().await
}

#[tokio::test]
async fn sync_manifest_normalized_events_skips_inactive_manifests() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;

    let active_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 1,
            namespace: "ens",
            source_family: "ens_v2_registry_l1",
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v2",
            rollout_status: "active",
            normalizer_version: "ensip15@ens-normalize-0.1.1",
            file_path: "manifests/ens/ens_v2_registry_l1/1.toml",
        },
    )
    .await?;
    let inactive_manifest_id = insert_manifest_version(
        database.pool(),
        ManifestVersionSeed {
            manifest_version: 2,
            namespace: "ens",
            source_family: "ens_v2_registry_l1",
            chain: "ethereum-mainnet",
            deployment_epoch: "ens_v2_shadow",
            rollout_status: "deprecated",
            normalizer_version: "ensip15@ens-normalize-0.1.1",
            file_path: "manifests/ens/ens_v2_registry_l1/2.toml",
        },
    )
    .await?;

    insert_capability_flag(
        database.pool(),
        active_manifest_id,
        "declared_children",
        "supported",
        None,
    )
    .await?;
    insert_capability_flag(
        database.pool(),
        inactive_manifest_id,
        "declared_children",
        "unsupported",
        None,
    )
    .await?;

    let active_contract_instance_id = Uuid::new_v4();
    let active_implementation_contract_instance_id = Uuid::new_v4();
    let inactive_contract_instance_id = Uuid::new_v4();
    let inactive_implementation_contract_instance_id = Uuid::new_v4();
    insert_contract_instance(
        database.pool(),
        active_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        active_implementation_contract_instance_id,
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
    insert_contract_instance(
        database.pool(),
        inactive_implementation_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;

    insert_contract(
        database.pool(),
        ContractSeed {
            manifest_id: active_manifest_id,
            contract_instance_id: active_contract_instance_id,
            declaration_name: "registry",
            role: "registry",
            address: "0x00000000000000000000000000000000000000aa",
            proxy_kind: "erc1967",
            implementation_contract_instance_id: Some(active_implementation_contract_instance_id),
            implementation: Some("0x00000000000000000000000000000000000000dd"),
        },
    )
    .await?;
    insert_contract(
        database.pool(),
        ContractSeed {
            manifest_id: inactive_manifest_id,
            contract_instance_id: inactive_contract_instance_id,
            declaration_name: "registry",
            role: "registry",
            address: "0x00000000000000000000000000000000000000bb",
            proxy_kind: "erc1967",
            implementation_contract_instance_id: Some(inactive_implementation_contract_instance_id),
            implementation: Some("0x00000000000000000000000000000000000000ee"),
        },
    )
    .await?;

    let summary = sync_manifest_normalized_events(database.pool()).await?;
    assert_eq!(summary.total_synced_count, 3);
    assert_eq!(summary.total_inserted_count, 3);
    assert_eq!(
        load_normalized_events_by_namespace(database.pool(), "ens")
            .await?
            .len(),
        3
    );
    assert_eq!(
        load_normalized_event_counts_by_kind(database.pool(), "ens").await?,
        BTreeMap::from([
            (EVENT_KIND_CAPABILITY_CHANGED.to_owned(), 1_usize),
            (EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED.to_owned(), 1_usize),
            (EVENT_KIND_SOURCE_MANIFEST_UPDATED.to_owned(), 1_usize),
        ])
    );

    database.cleanup().await
}
