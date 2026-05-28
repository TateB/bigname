use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result};
use bigname_storage::{
    NameSurface, NormalizedEvent, RawBlock, RawCodeHash, RawLog, Resource, SurfaceBinding,
    default_database_url, load_resolver_current, upsert_name_surfaces, upsert_normalized_events,
    upsert_raw_blocks, upsert_raw_code_hashes, upsert_raw_logs, upsert_resolver_current_rows,
    upsert_resources, upsert_surface_bindings,
};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use super::*;
use crate::permissions::rebuild_permissions_current;

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
            .context("failed to parse database URL for worker resolver_current tests")?;
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bg_wr_{}_{}_{}",
            std::process::id(),
            sequence,
            &Uuid::new_v4().simple().to_string()[..8]
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for worker resolver_current tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect worker resolver_current test pool")?;

        bigname_storage::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for worker resolver_current tests")?;

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

#[tokio::test]
async fn resolver_current_keyed_rebuild_projects_bindings_permissions_and_unsupported_aliases()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x8100);
    let surface_binding_id = Uuid::from_u128(0x8200);
    let alias_resource_id = Uuid::from_u128(0x8101);
    let alias_surface_binding_id = Uuid::from_u128(0x8201);
    let resolver_address = "0x0000000000000000000000000000000000000aaa";

    seed_identity(
        database.pool(),
        "ens:alpha.eth",
        resource_id,
        surface_binding_id,
        "alpha.eth",
        SurfaceBindingKind::DeclaredRegistryPath,
    )
    .await?;
    seed_identity(
        database.pool(),
        "ens:beta.eth",
        alias_resource_id,
        alias_surface_binding_id,
        "beta.eth",
        SurfaceBindingKind::ResolverAliasPath,
    )
    .await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xres0100", 100, 1_776_200_100),
            raw_block("ethereum-mainnet", "0xres0101", 101, 1_776_200_101),
        ],
    )
    .await?;
    seed_resolver_events(
        database.pool(),
        &[
            resolver_event(
                "resolver-alpha",
                "ens:alpha.eth",
                resource_id,
                resolver_address,
                100,
                0,
            ),
            resolver_event(
                "resolver-beta",
                "ens:beta.eth",
                alias_resource_id,
                resolver_address,
                100,
                1,
            ),
        ],
    )
    .await?;
    seed_permission_events(
        database.pool(),
        &[
            resolver_permission_event(
                "permission-alpha-1",
                Some("ens:alpha.eth"),
                resource_id,
                "0x0000000000000000000000000000000000000abc",
                "ethereum-mainnet",
                "0x0000000000000000000000000000000000000aaa",
                json!(["set_resolver"]),
                101,
                0,
            ),
            resolver_permission_event(
                "permission-alpha-2",
                Some("ens:alpha.eth"),
                resource_id,
                "0x0000000000000000000000000000000000000abc",
                "ethereum-mainnet",
                "0x0000000000000000000000000000000000000aaa",
                json!(["set_resolver", "set_records"]),
                101,
                1,
            ),
            resolver_permission_event(
                "permission-only",
                None,
                database_resource_id(1),
                "0x0000000000000000000000000000000000000def",
                "ethereum-mainnet",
                "0x0000000000000000000000000000000000000aaa",
                json!(["set_resolver"]),
                101,
                2,
            ),
        ],
    )
    .await?;
    rebuild_permissions_current(database.pool(), None).await?;

    let summary = rebuild_resolver_current(
        database.pool(),
        Some("ethereum-mainnet"),
        Some(resolver_address),
    )
    .await?;
    assert_eq!(summary.requested_resolver_count, 1);
    assert_eq!(summary.upserted_row_count, 1);
    assert_eq!(summary.deleted_row_count, 0);

    let row = load_resolver_current(database.pool(), "ethereum-mainnet", resolver_address)
        .await?
        .context("resolver_current row should exist")?;

    assert_eq!(row.declared_summary["bindings"]["count"], json!(2));
    assert_eq!(
        row.declared_summary["bindings"]["items"][0]["logical_name_id"],
        json!("ens:alpha.eth")
    );
    assert_eq!(
        row.declared_summary["bindings"]["items"][1]["logical_name_id"],
        json!("ens:beta.eth")
    );
    assert_eq!(
        row.declared_summary["aliases"]["status"],
        json!("supported")
    );
    assert_eq!(row.declared_summary["aliases"]["count"], json!(1));
    assert_eq!(
        row.declared_summary["aliases"]["items"][0]["logical_name_id"],
        json!("ens:beta.eth")
    );
    assert_eq!(
        row.declared_summary["aliases"]["items"][0]["binding_kind"],
        json!("resolver_alias_path")
    );
    assert_eq!(
        row.declared_summary["aliases"]["items"][0],
        row.declared_summary["bindings"]["items"][1]
    );
    assert_eq!(row.declared_summary["permissions"]["count"], json!(2));
    assert_eq!(row.declared_summary["role_holders"]["count"], json!(2));
    assert_eq!(
        row.declared_summary["event_summary"]["by_kind"][EVENT_KIND_RESOLVER_CHANGED],
        json!(2)
    );
    assert_eq!(
        row.declared_summary["event_summary"]["by_kind"][EVENT_KIND_PERMISSION_CHANGED],
        json!(3)
    );
    assert_eq!(
        row.provenance["normalized_event_ids"],
        json!([1, 2, 3, 4, 5])
    );
    assert_eq!(
        row.coverage["enumeration_basis"],
        json!(RESOLVER_CURRENT_ENUMERATION_BASIS)
    );
    assert_eq!(
        row.chain_positions["ethereum-mainnet"]["block_number"],
        json!(101)
    );
    assert_eq!(row.canonicality_summary["status"], json!("finalized"));

    database.cleanup().await
}

#[tokio::test]
async fn resolver_current_ignores_suppressed_old_registry_raw_facts_after_migration() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x8120);
    let surface_binding_id = Uuid::from_u128(0x8220);
    let registry_manifest_id = insert_manifest_version(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "manifests/ens/ens_v1_registry_l1/v3.toml",
    )
    .await?;
    let resolver_manifest_id = insert_manifest_version(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
        "manifests/ens/ens_v1_resolver_l1/v1.toml",
    )
    .await?;
    let resolver_contract_instance_id = Uuid::from_u128(0x8121);
    let current_resolver = "0x0000000000000000000000000000000000008121";
    let suppressed_resolver = "0x0000000000000000000000000000000000008122";

    insert_contract_instance(
        database.pool(),
        resolver_contract_instance_id,
        current_resolver,
        resolver_manifest_id,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        resolver_manifest_id,
        "public_resolver",
        resolver_contract_instance_id,
        current_resolver,
    )
    .await?;
    seed_identity(
        database.pool(),
        "ens:migrated.eth",
        resource_id,
        surface_binding_id,
        "migrated.eth",
        SurfaceBindingKind::DeclaredRegistryPath,
    )
    .await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xres0150", 150, 1_776_302_150),
            raw_block(
                "ethereum-mainnet",
                "0xensold-resolver-suppressed",
                550,
                1_776_302_550,
            ),
        ],
    )
    .await?;
    seed_raw_logs(
        database.pool(),
        &[old_registry_raw_log(
            "suppressed-resolver-overview",
            "0xensold-resolver-suppressed",
            550,
            11,
            suppressed_resolver,
        )],
    )
    .await?;
    seed_resolver_events(
        database.pool(),
        &[resolver_event_with_manifest(
            "surviving-current-resolver",
            "ens:migrated.eth",
            resource_id,
            current_resolver,
            SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
            registry_manifest_id,
            150,
            0,
        )],
    )
    .await?;

    let summary = rebuild_resolver_current(database.pool(), None, None).await?;
    assert_eq!(summary.requested_resolver_count, 1);
    assert_eq!(summary.upserted_row_count, 1);

    let current_row = load_resolver_current(database.pool(), "ethereum-mainnet", current_resolver)
        .await?
        .context("current resolver_current row should exist")?;
    assert_eq!(
        current_row.declared_summary["bindings"]["status"],
        json!("unsupported")
    );
    assert_eq!(
        current_row.declared_summary["bindings"]["unsupported_reason"],
        json!(RESOLVER_BINDING_ENUMERATION_NOT_PROJECTED_REASON)
    );
    assert_eq!(current_row.coverage["status"], json!("partial"));
    assert_eq!(
        current_row.coverage["unsupported_reason"],
        json!(RESOLVER_BINDING_ENUMERATION_NOT_PROJECTED_REASON)
    );

    let suppressed_summary = rebuild_resolver_current(
        database.pool(),
        Some("ethereum-mainnet"),
        Some(suppressed_resolver),
    )
    .await?;
    assert_eq!(suppressed_summary.upserted_row_count, 0);
    assert!(
        load_resolver_current(database.pool(), "ethereum-mainnet", suppressed_resolver)
            .await?
            .is_none()
    );

    let projection_json = serde_json::to_string(&json!({
        "declared_summary": current_row.declared_summary,
        "provenance": current_row.provenance,
        "coverage": current_row.coverage,
        "chain_positions": current_row.chain_positions,
        "canonicality_summary": current_row.canonicality_summary,
    }))?;
    assert!(!projection_json.contains("0xensold-resolver-suppressed"));
    assert!(!projection_json.contains(suppressed_resolver));
    assert!(!projection_json.contains("suppressed-resolver-overview"));

    database.cleanup().await
}

#[tokio::test]
async fn resolver_current_skips_known_ensv1_public_resolver_binding_enumeration() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x8130);
    let surface_binding_id = Uuid::from_u128(0x8230);
    let alias_resource_id = Uuid::from_u128(0x8131);
    let alias_surface_binding_id = Uuid::from_u128(0x8231);
    let resolver_contract_instance_id = Uuid::from_u128(0x8132);
    let resolver_address = "0x0000000000000000000000000000000000008132";

    let registry_manifest_id = insert_manifest_version(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
        "manifests/ens/ens_v1_registry_l1/v3.toml",
    )
    .await?;
    let resolver_manifest_id = insert_manifest_version(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
        "manifests/ens/ens_v1_resolver_l1/v1.toml",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        resolver_contract_instance_id,
        resolver_address,
        resolver_manifest_id,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        resolver_manifest_id,
        "public_resolver",
        resolver_contract_instance_id,
        resolver_address,
    )
    .await?;

    seed_identity(
        database.pool(),
        "ens:shared-alpha.eth",
        resource_id,
        surface_binding_id,
        "shared-alpha.eth",
        SurfaceBindingKind::DeclaredRegistryPath,
    )
    .await?;
    seed_identity(
        database.pool(),
        "ens:shared-alias.eth",
        alias_resource_id,
        alias_surface_binding_id,
        "shared-alias.eth",
        SurfaceBindingKind::ResolverAliasPath,
    )
    .await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xres0160", 160, 1_776_302_160),
            raw_block("ethereum-mainnet", "0xres0161", 161, 1_776_302_161),
        ],
    )
    .await?;
    seed_resolver_events(
        database.pool(),
        &[
            resolver_event_with_manifest(
                "shared-current-resolver-alpha",
                "ens:shared-alpha.eth",
                resource_id,
                resolver_address,
                SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
                registry_manifest_id,
                160,
                0,
            ),
            resolver_event_with_manifest(
                "shared-current-resolver-alias",
                "ens:shared-alias.eth",
                alias_resource_id,
                resolver_address,
                SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
                registry_manifest_id,
                160,
                1,
            ),
        ],
    )
    .await?;
    seed_permission_events(
        database.pool(),
        &[resolver_permission_event(
            "public-resolver-permission",
            Some("ens:shared-alpha.eth"),
            resource_id,
            "0x0000000000000000000000000000000000000abc",
            "ethereum-mainnet",
            resolver_address,
            json!(["set_resolver"]),
            161,
            0,
        )],
    )
    .await?;
    rebuild_permissions_current(database.pool(), None).await?;

    let summary = rebuild_resolver_current(
        database.pool(),
        Some("ethereum-mainnet"),
        Some(resolver_address),
    )
    .await?;
    assert_eq!(summary.requested_resolver_count, 1);
    assert_eq!(summary.upserted_row_count, 1);

    let row = load_resolver_current(database.pool(), "ethereum-mainnet", resolver_address)
        .await?
        .context("known public resolver_current row should exist")?;
    for section in [
        "bindings",
        "aliases",
        "permissions",
        "role_holders",
        "event_summary",
    ] {
        assert_eq!(
            row.declared_summary[section]["status"],
            json!("unsupported")
        );
        assert_eq!(
            row.declared_summary[section]["unsupported_reason"],
            json!(RESOLVER_BINDING_ENUMERATION_NOT_PROJECTED_REASON)
        );
    }
    assert_eq!(row.coverage["status"], json!("partial"));
    assert_eq!(row.coverage["exhaustiveness"], json!("non_enumerable"));
    assert_eq!(
        row.coverage["unsupported_reason"],
        json!(RESOLVER_BINDING_ENUMERATION_NOT_PROJECTED_REASON)
    );
    assert_eq!(row.provenance["normalized_event_ids"], json!([]));

    database.cleanup().await
}

#[tokio::test]
async fn resolver_current_projects_latest_ensv2_alias_tombstone() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resolver_address = "0x0000000000000000000000000000000000000aaa";
    let target_resource_id = Uuid::from_u128(0x8102);

    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xres0300", 300, 1_776_200_300),
            raw_block("ethereum-mainnet", "0xres0301", 301, 1_776_200_301),
        ],
    )
    .await?;
    upsert_resources(database.pool(), &[resource(target_resource_id)]).await?;
    upsert_normalized_events(
        database.pool(),
        &[
            alias_event(
                "alias-active",
                Some("ens:from.eth"),
                Some(target_resource_id),
                resolver_address,
                "0x0466726f6d0365746800",
                "0x02746f0365746800",
                Some("from.eth"),
                Some("to.eth"),
                "active",
                300,
                0,
            ),
            alias_event(
                "alias-removed",
                Some("ens:from.eth"),
                None,
                resolver_address,
                "0x0466726f6d0365746800",
                "0x",
                Some("from.eth"),
                None,
                "removed",
                301,
                0,
            ),
        ],
    )
    .await?;

    let summary = rebuild_resolver_current(
        database.pool(),
        Some("ethereum-mainnet"),
        Some(resolver_address),
    )
    .await?;
    assert_eq!(summary.upserted_row_count, 1);

    let row = load_resolver_current(database.pool(), "ethereum-mainnet", resolver_address)
        .await?
        .context("resolver_current row should exist")?;
    assert_eq!(row.declared_summary["aliases"]["count"], json!(1));
    assert_eq!(
        row.declared_summary["aliases"]["items"][0]["alias_state"],
        json!("removed")
    );
    assert_eq!(
        row.declared_summary["aliases"]["items"][0]["active"],
        json!(false)
    );
    assert_eq!(
        row.declared_summary["aliases"]["items"][0]["to_dns_encoded_name"],
        json!("0x")
    );
    assert_eq!(row.provenance["normalized_event_ids"], json!([2]));

    database.cleanup().await
}

#[tokio::test]
async fn resolver_current_full_rebuild_clears_stale_rows_and_rebuilds_all_targets() -> Result<()> {
    let database = TestDatabase::new().await?;
    let binding_resource_id = Uuid::from_u128(0x8300);
    let binding_surface_binding_id = Uuid::from_u128(0x8301);
    let permission_only_resource_id = Uuid::from_u128(0x8302);

    seed_identity(
        database.pool(),
        "ens:beta.eth",
        binding_resource_id,
        binding_surface_binding_id,
        "beta.eth",
        SurfaceBindingKind::DeclaredRegistryPath,
    )
    .await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xres0200", 200, 1_776_200_200),
            raw_block("ethereum-mainnet", "0xres0210", 210, 1_776_200_210),
        ],
    )
    .await?;
    seed_resolver_events(
        database.pool(),
        &[resolver_event(
            "resolver-beta",
            "ens:beta.eth",
            binding_resource_id,
            "0x0000000000000000000000000000000000000bbb",
            200,
            0,
        )],
    )
    .await?;
    seed_permission_events(
        database.pool(),
        &[resolver_permission_event(
            "permission-only-target",
            None,
            permission_only_resource_id,
            "0x0000000000000000000000000000000000000abc",
            "ethereum-mainnet",
            "0x0000000000000000000000000000000000000ccc",
            json!(["set_resolver"]),
            210,
            0,
        )],
    )
    .await?;
    rebuild_permissions_current(database.pool(), None).await?;
    upsert_resolver_current_rows(
        database.pool(),
        &[ResolverCurrentRow {
            chain_id: "ethereum-mainnet".to_owned(),
            resolver_address: "0x0000000000000000000000000000000000000bad".to_owned(),
            declared_summary: json!({"stale": true}),
            provenance: json!({"derivation_kind": RESOLVER_CURRENT_DERIVATION_KIND}),
            coverage: json!({"enumeration_basis": RESOLVER_CURRENT_ENUMERATION_BASIS}),
            chain_positions: json!({}),
            canonicality_summary: json!({"status": "finalized", "chains": {}}),
            manifest_version: 1,
            last_recomputed_at: timestamp(1_776_200_001),
        }],
    )
    .await?;

    let summary = rebuild_resolver_current(database.pool(), None, None).await?;
    assert_eq!(summary.requested_resolver_count, 2);
    assert_eq!(summary.upserted_row_count, 2);
    assert_eq!(summary.deleted_row_count, 1);

    let binding_row = load_resolver_current(
        database.pool(),
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000bbb",
    )
    .await?;
    let permission_row = load_resolver_current(
        database.pool(),
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000ccc",
    )
    .await?;
    let stale_row = load_resolver_current(
        database.pool(),
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000bad",
    )
    .await?;

    assert!(binding_row.is_some());
    assert!(permission_row.is_some());
    assert!(stale_row.is_none());
    let permission_row = permission_row.context("permission-only resolver row should exist")?;
    assert_eq!(
        permission_row.declared_summary["bindings"]["unsupported_reason"],
        json!(RESOLVER_BINDING_ENUMERATION_NOT_PROJECTED_REASON)
    );
    assert_eq!(
        permission_row.declared_summary["permissions"]["unsupported_reason"],
        json!(RESOLVER_BINDING_ENUMERATION_NOT_PROJECTED_REASON)
    );

    database.cleanup().await
}

#[tokio::test]
async fn resolver_current_keeps_pending_ensv1_dynamic_resolver_sections_unsupported() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x8400);
    let surface_binding_id = Uuid::from_u128(0x8401);
    let registry_contract_instance_id = Uuid::from_u128(0x8402);
    let public_resolver_contract_instance_id = Uuid::from_u128(0x8403);
    let registry_address = "0x0000000000000000000000000000000000008402";
    let public_resolver_address = "0x0000000000000000000000000000000000008403";
    let pending_resolver_address = "0x0000000000000000000000000000000000008404";

    let registry_manifest_id = insert_manifest_version(
        database.pool(),
        "ens_v1_registry_l1",
        "manifests/ens/ens_v1_registry_l1/v2.toml",
    )
    .await?;
    let resolver_manifest_id = insert_manifest_version(
        database.pool(),
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
        "manifests/ens/ens_v1_resolver_l1/v1.toml",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        registry_contract_instance_id,
        registry_address,
        registry_manifest_id,
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        public_resolver_contract_instance_id,
        public_resolver_address,
        resolver_manifest_id,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        resolver_manifest_id,
        "public_resolver",
        public_resolver_contract_instance_id,
        public_resolver_address,
    )
    .await?;

    seed_identity(
        database.pool(),
        "ens:pending.eth",
        resource_id,
        surface_binding_id,
        "pending.eth",
        SurfaceBindingKind::DeclaredRegistryPath,
    )
    .await?;
    seed_raw_blocks(
        database.pool(),
        &[raw_block(
            "ethereum-mainnet",
            "0xres0400",
            400,
            1_776_200_400,
        )],
    )
    .await?;
    seed_resolver_events(
        database.pool(),
        &[resolver_event_with_manifest(
            "pending-resolver",
            "ens:pending.eth",
            resource_id,
            pending_resolver_address,
            "ens_v1_registry_l1",
            registry_manifest_id,
            400,
            0,
        )],
    )
    .await?;

    let summary = rebuild_resolver_current(
        database.pool(),
        Some("ethereum-mainnet"),
        Some(pending_resolver_address),
    )
    .await?;
    assert_eq!(summary.requested_resolver_count, 1);
    assert_eq!(summary.upserted_row_count, 1);

    let row = load_resolver_current(
        database.pool(),
        "ethereum-mainnet",
        pending_resolver_address,
    )
    .await?
    .context("pending resolver_current row should exist")?;
    for section in [
        "bindings",
        "aliases",
        "permissions",
        "role_holders",
        "event_summary",
    ] {
        assert_eq!(
            row.declared_summary[section]["status"],
            json!("unsupported")
        );
        assert_eq!(
            row.declared_summary[section]["unsupported_reason"],
            json!(RESOLVER_FAMILY_PENDING_REASON)
        );
    }
    assert_eq!(
        row.coverage["unsupported_reason"],
        json!(RESOLVER_FAMILY_PENDING_REASON)
    );
    assert_eq!(row.provenance["normalized_event_ids"], json!([1]));

    database.cleanup().await
}

#[tokio::test]
async fn resolver_current_keeps_unadmitted_basenames_dynamic_resolver_sections_unsupported()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x8500);
    let surface_binding_id = Uuid::from_u128(0x8501);
    let resolver_address = "0x0000000000000000000000000000000000008502";

    seed_basenames_identity(
        database.pool(),
        "basenames:pending.base.eth",
        resource_id,
        surface_binding_id,
        "pending.base.eth",
        SurfaceBindingKind::DeclaredRegistryPath,
    )
    .await?;
    seed_raw_blocks(
        database.pool(),
        &[raw_block(
            "base-mainnet",
            "0xbase-res0400",
            400,
            1_776_200_400,
        )],
    )
    .await?;
    seed_resolver_events(
        database.pool(),
        &[basenames_resolver_event(
            "base-pending-resolver",
            "basenames:pending.base.eth",
            resource_id,
            resolver_address,
            400,
            0,
        )],
    )
    .await?;

    let summary = rebuild_resolver_current(
        database.pool(),
        Some("base-mainnet"),
        Some(resolver_address),
    )
    .await?;
    assert_eq!(summary.requested_resolver_count, 1);
    assert_eq!(summary.upserted_row_count, 1);

    let row = load_resolver_current(database.pool(), "base-mainnet", resolver_address)
        .await?
        .context("unadmitted Basenames resolver_current row should exist")?;
    for section in [
        "bindings",
        "aliases",
        "permissions",
        "role_holders",
        "event_summary",
    ] {
        assert_eq!(
            row.declared_summary[section]["status"],
            json!("unsupported")
        );
        assert_eq!(
            row.declared_summary[section]["unsupported_reason"],
            json!(RESOLVER_FAMILY_PENDING_REASON)
        );
    }
    assert_eq!(
        row.coverage["unsupported_reason"],
        json!(RESOLVER_FAMILY_PENDING_REASON)
    );
    assert_eq!(row.provenance["normalized_event_ids"], json!([1]));

    database.cleanup().await
}

#[tokio::test]
async fn resolver_current_basenames_dynamic_resolver_gates_supported_pending_and_unsupported_targets()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let supported_resource_id = Uuid::from_u128(0x8600);
    let pending_resource_id = Uuid::from_u128(0x8601);
    let unsupported_resource_id = Uuid::from_u128(0x8602);
    let supported_surface_binding_id = Uuid::from_u128(0x8610);
    let pending_surface_binding_id = Uuid::from_u128(0x8611);
    let unsupported_surface_binding_id = Uuid::from_u128(0x8612);
    let seed_resolver_contract_instance_id = Uuid::from_u128(0x8620);
    let supported_resolver_contract_instance_id = Uuid::from_u128(0x8621);
    let pending_resolver_contract_instance_id = Uuid::from_u128(0x8622);
    let unsupported_resolver_contract_instance_id = Uuid::from_u128(0x8623);
    let seed_resolver_address = "0x0000000000000000000000000000000000008620";
    let supported_resolver_address = "0x0000000000000000000000000000000000008621";
    let pending_resolver_address = "0x0000000000000000000000000000000000008622";
    let unsupported_resolver_address = "0x0000000000000000000000000000000000008623";

    insert_basenames_dynamic_resolver_profile_fixture(
        database.pool(),
        seed_resolver_contract_instance_id,
        seed_resolver_address,
        &[
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
        ],
        &[
            (supported_resolver_address, Some(BASENAMES_L2_CODE_HASH)),
            (pending_resolver_address, None),
            (unsupported_resolver_address, Some(UNSUPPORTED_CODE_HASH)),
        ],
    )
    .await?;
    seed_basenames_identity(
        database.pool(),
        "basenames:supported.base.eth",
        supported_resource_id,
        supported_surface_binding_id,
        "supported.base.eth",
        SurfaceBindingKind::DeclaredRegistryPath,
    )
    .await?;
    seed_basenames_identity(
        database.pool(),
        "basenames:pending.base.eth",
        pending_resource_id,
        pending_surface_binding_id,
        "pending.base.eth",
        SurfaceBindingKind::DeclaredRegistryPath,
    )
    .await?;
    seed_basenames_identity(
        database.pool(),
        "basenames:unsupported.base.eth",
        unsupported_resource_id,
        unsupported_surface_binding_id,
        "unsupported.base.eth",
        SurfaceBindingKind::DeclaredRegistryPath,
    )
    .await?;
    seed_raw_blocks(
        database.pool(),
        &[raw_block(
            "base-mainnet",
            "0xbase-res0500",
            500,
            1_776_200_500,
        )],
    )
    .await?;
    seed_resolver_events(
        database.pool(),
        &[
            basenames_resolver_event(
                "base-supported-resolver",
                "basenames:supported.base.eth",
                supported_resource_id,
                supported_resolver_address,
                500,
                0,
            ),
            basenames_resolver_event(
                "base-pending-resolver",
                "basenames:pending.base.eth",
                pending_resource_id,
                pending_resolver_address,
                500,
                1,
            ),
            basenames_resolver_event(
                "base-unsupported-resolver",
                "basenames:unsupported.base.eth",
                unsupported_resource_id,
                unsupported_resolver_address,
                500,
                2,
            ),
        ],
    )
    .await?;

    for resolver_address in [
        supported_resolver_address,
        pending_resolver_address,
        unsupported_resolver_address,
    ] {
        let summary = rebuild_resolver_current(
            database.pool(),
            Some("base-mainnet"),
            Some(resolver_address),
        )
        .await?;
        assert_eq!(summary.requested_resolver_count, 1);
        assert_eq!(summary.upserted_row_count, 1);
    }

    let supported_row =
        load_resolver_current(database.pool(), "base-mainnet", supported_resolver_address)
            .await?
            .context("supported Basenames resolver_current row should exist")?;
    assert_eq!(
        supported_row.declared_summary["bindings"]["status"],
        json!("supported")
    );
    assert_eq!(
        supported_row.declared_summary["bindings"]["count"],
        json!(1)
    );
    assert_eq!(
        supported_row.declared_summary["bindings"]["items"][0]["logical_name_id"],
        json!("basenames:supported.base.eth")
    );
    assert_eq!(supported_row.coverage["unsupported_reason"], Value::Null);

    for (resolver_address, logical_name_id) in [
        (pending_resolver_address, "basenames:pending.base.eth"),
        (
            unsupported_resolver_address,
            "basenames:unsupported.base.eth",
        ),
    ] {
        let row = load_resolver_current(database.pool(), "base-mainnet", resolver_address)
            .await?
            .with_context(|| format!("{logical_name_id} resolver_current row should exist"))?;
        for section in [
            "bindings",
            "aliases",
            "permissions",
            "role_holders",
            "event_summary",
        ] {
            assert_eq!(
                row.declared_summary[section]["status"],
                json!("unsupported")
            );
            assert_eq!(
                row.declared_summary[section]["unsupported_reason"],
                json!(RESOLVER_FAMILY_PENDING_REASON)
            );
        }
        assert_eq!(
            row.coverage["unsupported_reason"],
            json!(RESOLVER_FAMILY_PENDING_REASON)
        );
        assert_eq!(
            row.provenance["normalized_event_ids"]
                .as_array()
                .map(Vec::len),
            Some(1)
        );
    }

    database.cleanup().await
}

async fn seed_identity(
    pool: &PgPool,
    logical_name_id: &str,
    resource_id: Uuid,
    surface_binding_id: Uuid,
    display_name: &str,
    binding_kind: SurfaceBindingKind,
) -> Result<()> {
    upsert_name_surfaces(pool, &[name_surface(logical_name_id, display_name)]).await?;
    upsert_resources(pool, &[resource(resource_id)]).await?;
    upsert_surface_bindings(
        pool,
        &[surface_binding(
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
        )],
    )
    .await?;
    Ok(())
}

async fn seed_basenames_identity(
    pool: &PgPool,
    logical_name_id: &str,
    resource_id: Uuid,
    surface_binding_id: Uuid,
    display_name: &str,
    binding_kind: SurfaceBindingKind,
) -> Result<()> {
    upsert_name_surfaces(
        pool,
        &[basenames_name_surface(logical_name_id, display_name)],
    )
    .await?;
    upsert_resources(pool, &[basenames_resource(resource_id)]).await?;
    upsert_surface_bindings(
        pool,
        &[basenames_surface_binding(
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
        )],
    )
    .await?;
    Ok(())
}

async fn seed_raw_blocks(pool: &PgPool, blocks: &[RawBlock]) -> Result<()> {
    upsert_raw_blocks(pool, blocks).await?;
    Ok(())
}

async fn seed_raw_logs(pool: &PgPool, logs: &[RawLog]) -> Result<()> {
    upsert_raw_logs(pool, logs).await?;
    Ok(())
}

async fn seed_resolver_events(pool: &PgPool, events: &[NormalizedEvent]) -> Result<()> {
    upsert_normalized_events(pool, events).await?;
    Ok(())
}

async fn seed_permission_events(pool: &PgPool, events: &[NormalizedEvent]) -> Result<()> {
    let mut resource_ids = events
        .iter()
        .filter_map(|event| event.resource_id)
        .collect::<Vec<_>>();
    resource_ids.sort();
    resource_ids.dedup();
    let resources = resource_ids.into_iter().map(resource).collect::<Vec<_>>();
    upsert_resources(pool, &resources).await?;
    upsert_normalized_events(pool, events).await?;
    Ok(())
}

const BASENAMES_L2_CODE_HASH: &str =
    "0x1111111111111111111111111111111111111111111111111111111111111111";
const UNSUPPORTED_CODE_HASH: &str =
    "0x2222222222222222222222222222222222222222222222222222222222222222";

async fn insert_basenames_dynamic_resolver_profile_fixture(
    pool: &PgPool,
    seed_contract_instance_id: Uuid,
    seed_address: &str,
    dynamic_resolvers: &[(Uuid, &str)],
    code_hashes: &[(&str, Option<&str>)],
) -> Result<()> {
    let resolver_manifest_id = insert_basenames_manifest_version(
        pool,
        SOURCE_FAMILY_BASENAMES_BASE_RESOLVER,
        "manifests/basenames/basenames_base_resolver/v1.toml",
    )
    .await?;
    let registry_manifest_id = insert_basenames_manifest_version(
        pool,
        SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
        "manifests/basenames/basenames_base_registry/v1.toml",
    )
    .await?;
    insert_basenames_contract_instance(
        pool,
        seed_contract_instance_id,
        seed_address,
        resolver_manifest_id,
        "contract",
    )
    .await?;
    insert_basenames_manifest_contract_instance(
        pool,
        resolver_manifest_id,
        "resolver",
        seed_contract_instance_id,
        seed_address,
    )
    .await?;

    let registry_contract_instance_id = Uuid::from_u128(0x86ff);
    insert_basenames_contract_instance(
        pool,
        registry_contract_instance_id,
        "0x00000000000000000000000000000000000086ff",
        registry_manifest_id,
        "root",
    )
    .await?;

    for (contract_instance_id, address) in dynamic_resolvers {
        insert_basenames_contract_instance(
            pool,
            *contract_instance_id,
            address,
            resolver_manifest_id,
            "contract",
        )
        .await?;
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
            VALUES (
                'base-mainnet',
                'resolver',
                $1,
                $2,
                $3,
                $4,
                'test',
                '{}'::jsonb
            )
            "#,
        )
        .bind(registry_contract_instance_id)
        .bind(contract_instance_id)
        .bind(format!("test:basenames-dynamic-resolver:{address}"))
        .bind(registry_manifest_id)
        .execute(pool)
        .await
        .context("failed to insert Basenames dynamic resolver discovery_edge")?;
    }

    let mut raw_code_hashes = vec![basenames_raw_code_hash(
        seed_address,
        BASENAMES_L2_CODE_HASH,
    )];
    raw_code_hashes.extend(code_hashes.iter().filter_map(|(address, code_hash)| {
        code_hash.map(|code_hash| basenames_raw_code_hash(address, code_hash))
    }));
    upsert_raw_code_hashes(pool, &raw_code_hashes).await?;

    Ok(())
}

async fn insert_basenames_manifest_version(
    pool: &PgPool,
    source_family: &str,
    file_path: &str,
) -> Result<i64> {
    sqlx::query(
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
        VALUES (1, 'basenames', $1, 'base-mainnet', 'basenames_v1', 'active', 'ensip15@ens-normalize-0.1.1', $2, '{}'::jsonb)
        RETURNING manifest_id
        "#,
    )
    .bind(source_family)
    .bind(file_path)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to insert manifest_version for {source_family}"))?
    .try_get::<i64, _>("manifest_id")
    .context("failed to read Basenames manifest_id")
}

async fn insert_basenames_contract_instance(
    pool: &PgPool,
    contract_instance_id: Uuid,
    address: &str,
    source_manifest_id: i64,
    contract_kind: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind, provenance)
        VALUES ($1, 'base-mainnet', $2, '{}'::jsonb)
        "#,
    )
    .bind(contract_instance_id)
    .bind(contract_kind)
    .execute(pool)
    .await
    .context("failed to insert Basenames contract_instance")?;
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address,
            source_manifest_id,
            provenance
        )
        VALUES ($1, 'base-mainnet', lower($2), $3, '{}'::jsonb)
        "#,
    )
    .bind(contract_instance_id)
    .bind(address)
    .bind(source_manifest_id)
    .execute(pool)
    .await
    .context("failed to insert Basenames contract_instance_address")?;
    Ok(())
}

async fn insert_basenames_manifest_contract_instance(
    pool: &PgPool,
    manifest_id: i64,
    role: &str,
    contract_instance_id: Uuid,
    address: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances (
            manifest_id,
            declaration_kind,
            declaration_name,
            contract_instance_id,
            declared_address,
            role,
            proxy_kind
        )
        VALUES ($1, 'contract', $2, $3, lower($4), $2, 'none')
        "#,
    )
    .bind(manifest_id)
    .bind(role)
    .bind(contract_instance_id)
    .bind(address)
    .execute(pool)
    .await
    .context("failed to insert Basenames manifest_contract_instance")?;
    Ok(())
}

async fn insert_manifest_version(
    pool: &PgPool,
    source_family: &str,
    file_path: &str,
) -> Result<i64> {
    sqlx::query(
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
        VALUES (1, 'ens', $1, 'ethereum-mainnet', 'ens_v1', 'active', 'ensip15@ens-normalize-0.1.1', $2, '{}'::jsonb)
        RETURNING manifest_id
        "#,
    )
    .bind(source_family)
    .bind(file_path)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to insert manifest_version for {source_family}"))?
    .try_get::<i64, _>("manifest_id")
    .context("failed to read manifest_id")
}

async fn insert_contract_instance(
    pool: &PgPool,
    contract_instance_id: Uuid,
    address: &str,
    source_manifest_id: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind, provenance)
        VALUES ($1, 'ethereum-mainnet', 'contract', '{}'::jsonb)
        "#,
    )
    .bind(contract_instance_id)
    .execute(pool)
    .await
    .context("failed to insert contract_instance")?;

    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address,
            source_manifest_id,
            provenance
        )
        VALUES ($1, 'ethereum-mainnet', lower($2), $3, '{}'::jsonb)
        "#,
    )
    .bind(contract_instance_id)
    .bind(address)
    .bind(source_manifest_id)
    .execute(pool)
    .await
    .context("failed to insert contract_instance_address")?;

    Ok(())
}

async fn insert_manifest_contract_instance(
    pool: &PgPool,
    manifest_id: i64,
    role: &str,
    contract_instance_id: Uuid,
    address: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances (
            manifest_id,
            declaration_kind,
            declaration_name,
            contract_instance_id,
            declared_address,
            role,
            proxy_kind
        )
        VALUES ($1, 'contract', $2, $3, lower($4), $2, 'none')
        "#,
    )
    .bind(manifest_id)
    .bind(role)
    .bind(contract_instance_id)
    .bind(address)
    .execute(pool)
    .await
    .context("failed to insert manifest_contract_instance")?;
    Ok(())
}

fn name_surface(logical_name_id: &str, display_name: &str) -> NameSurface {
    NameSurface {
        logical_name_id: logical_name_id.to_owned(),
        namespace: "ens".to_owned(),
        input_name: display_name.to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: display_name.to_owned(),
        dns_encoded_name: display_name.as_bytes().to_vec(),
        namehash: format!("namehash:{display_name}"),
        labelhashes: vec![format!("labelhash:{display_name}")],
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xsurface".to_owned(),
        block_number: 1,
        provenance: json!({"source": "worker_resolver_current_test"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn basenames_name_surface(logical_name_id: &str, display_name: &str) -> NameSurface {
    NameSurface {
        logical_name_id: logical_name_id.to_owned(),
        namespace: BASENAMES_NAMESPACE.to_owned(),
        input_name: display_name.to_owned(),
        canonical_display_name: display_name.to_owned(),
        normalized_name: display_name.to_owned(),
        dns_encoded_name: display_name.as_bytes().to_vec(),
        namehash: format!("namehash:{display_name}"),
        labelhashes: vec![format!("labelhash:{display_name}")],
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: "base-mainnet".to_owned(),
        block_hash: "0xbase-surface".to_owned(),
        block_number: 1,
        provenance: json!({"source": "worker_resolver_current_test"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn resource(resource_id: Uuid) -> Resource {
    Resource {
        resource_id,
        token_lineage_id: None,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: format!("0xresource{}", &resource_id.simple().to_string()[..8]),
        block_number: 10,
        provenance: json!({"source": "worker_resolver_current_test"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn basenames_resource(resource_id: Uuid) -> Resource {
    Resource {
        resource_id,
        token_lineage_id: None,
        chain_id: "base-mainnet".to_owned(),
        block_hash: format!("0xbase-resource{}", &resource_id.simple().to_string()[..8]),
        block_number: 10,
        provenance: json!({"source": "worker_resolver_current_test"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn surface_binding(
    surface_binding_id: Uuid,
    logical_name_id: &str,
    resource_id: Uuid,
    binding_kind: SurfaceBindingKind,
) -> SurfaceBinding {
    SurfaceBinding {
        surface_binding_id,
        logical_name_id: logical_name_id.to_owned(),
        resource_id,
        binding_kind,
        active_from: timestamp(1_776_200_000),
        active_to: None,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xbind".to_owned(),
        block_number: 11,
        provenance: json!({"source": "worker_resolver_current_test"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn basenames_surface_binding(
    surface_binding_id: Uuid,
    logical_name_id: &str,
    resource_id: Uuid,
    binding_kind: SurfaceBindingKind,
) -> SurfaceBinding {
    SurfaceBinding {
        surface_binding_id,
        logical_name_id: logical_name_id.to_owned(),
        resource_id,
        binding_kind,
        active_from: timestamp(1_776_200_000),
        active_to: None,
        chain_id: "base-mainnet".to_owned(),
        block_hash: "0xbase-bind".to_owned(),
        block_number: 11,
        provenance: json!({"source": "worker_resolver_current_test"}),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn raw_block(chain_id: &str, block_hash: &str, block_number: i64, unix_timestamp: i64) -> RawBlock {
    RawBlock {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: None,
        block_number,
        block_timestamp: timestamp(unix_timestamp),
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn old_registry_raw_log(
    event_identity: &str,
    block_hash: &str,
    block_number: i64,
    log_index: i64,
    resolver_address: &str,
) -> RawLog {
    RawLog {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xtxensoldresolver{block_number:04x}"),
        transaction_index: 0,
        log_index,
        emitting_address: "0x0000000000000000000000000000000000000f01".to_owned(),
        topics: vec![
            "ENSRegistryOld".to_owned(),
            event_identity.to_owned(),
            resolver_address.to_owned(),
        ],
        data: format!("suppressed-old-registry:{event_identity}").into_bytes(),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn basenames_raw_code_hash(address: &str, code_hash: &str) -> RawCodeHash {
    RawCodeHash {
        chain_id: "base-mainnet".to_owned(),
        block_hash: "0xbase-code-hash".to_owned(),
        block_number: 41,
        contract_address: address.to_owned(),
        code_hash: code_hash.to_owned(),
        code_byte_length: 5,
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn resolver_event(
    event_identity: &str,
    logical_name_id: &str,
    resource_id: Uuid,
    resolver_address: &str,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: Some(logical_name_id.to_owned()),
        resource_id: Some(resource_id),
        event_kind: EVENT_KIND_RESOLVER_CHANGED.to_owned(),
        source_family: "ens_v1_unwrapped_authority".to_owned(),
        manifest_version: 4,
        source_manifest_id: None,
        chain_id: Some("ethereum-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xres{block_number:04}")),
        transaction_hash: Some(format!("0xtx{block_number:04x}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "ethereum-mainnet",
            "block_number": block_number,
            "log_index": log_index
        }),
        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "resolver": resolver_address,
            "namehash": format!("namehash:{logical_name_id}"),
        }),
    }
}

#[allow(clippy::too_many_arguments)]
fn resolver_event_with_manifest(
    event_identity: &str,
    logical_name_id: &str,
    resource_id: Uuid,
    resolver_address: &str,
    source_family: &str,
    source_manifest_id: i64,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    let mut event = resolver_event(
        event_identity,
        logical_name_id,
        resource_id,
        resolver_address,
        block_number,
        log_index,
    );
    event.source_family = source_family.to_owned();
    event.source_manifest_id = Some(source_manifest_id);
    event
}

fn basenames_resolver_event(
    event_identity: &str,
    logical_name_id: &str,
    resource_id: Uuid,
    resolver_address: &str,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: BASENAMES_NAMESPACE.to_owned(),
        logical_name_id: Some(logical_name_id.to_owned()),
        resource_id: Some(resource_id),
        event_kind: EVENT_KIND_RESOLVER_CHANGED.to_owned(),
        source_family: SOURCE_FAMILY_BASENAMES_BASE_REGISTRY.to_owned(),
        manifest_version: 4,
        source_manifest_id: None,
        chain_id: Some("base-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xbase-res{block_number:04}")),
        transaction_hash: Some(format!("0xbase-tx{block_number:04x}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "base-mainnet",
            "block_number": block_number,
            "log_index": log_index
        }),
        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "resolver": resolver_address,
            "namehash": format!("namehash:{logical_name_id}"),
        }),
    }
}

#[allow(clippy::too_many_arguments)]
fn resolver_permission_event(
    event_identity: &str,
    logical_name_id: Option<&str>,
    resource_id: Uuid,
    subject: &str,
    chain_id: &str,
    resolver_address: &str,
    effective_powers: Value,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: logical_name_id.map(str::to_owned),
        resource_id: Some(resource_id),
        event_kind: EVENT_KIND_PERMISSION_CHANGED.to_owned(),
        source_family: "ens_v1_unwrapped_authority".to_owned(),
        manifest_version: 9,
        source_manifest_id: None,
        chain_id: Some(chain_id.to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xres{block_number:04}")),
        transaction_hash: Some(format!("0xperm{block_number:04x}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": chain_id,
            "block_number": block_number,
            "log_index": log_index,
        }),
        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "subject": subject,
            "scope": {
                "kind": "resolver",
                "chain_id": chain_id,
                "resolver_address": resolver_address,
            },
            "effective_powers": effective_powers,
            "grant_source": {
                "kind": "normalized_event",
                "event_identity": event_identity,
            },
            "revocation_source": Value::Null,
            "inheritance_path": [],
            "transfer_behavior": {},
        }),
    }
}

#[allow(clippy::too_many_arguments)]
fn alias_event(
    event_identity: &str,
    logical_name_id: Option<&str>,
    resource_id: Option<Uuid>,
    resolver_address: &str,
    from_dns_encoded_name: &str,
    to_dns_encoded_name: &str,
    from_name: Option<&str>,
    to_name: Option<&str>,
    alias_state: &str,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: logical_name_id.map(str::to_owned),
        resource_id,
        event_kind: EVENT_KIND_ALIAS_CHANGED.to_owned(),
        source_family: "ens_v2_resolver_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("ethereum-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xres{block_number:04}")),
        transaction_hash: Some(format!("0xalias{block_number:04x}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "ethereum-mainnet",
            "block_number": block_number,
            "log_index": log_index,
        }),
        derivation_kind: "ens_v2_resolver".to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "source_event": "AliasChanged",
            "resolver": resolver_address,
            "from_dns_encoded_name": from_dns_encoded_name,
            "to_dns_encoded_name": to_dns_encoded_name,
            "alias_state": alias_state,
            "active": alias_state == "active",
            "from_name": from_name,
            "to_name": to_name,
            "to_logical_name_id": to_name.map(|name| format!("ens:{name}")),
            "to_resource_id": resource_id.map(|value| value.to_string()),
        }),
    }
}

fn database_resource_id(offset: u128) -> Uuid {
    Uuid::from_u128(0x8f00 + offset)
}

fn timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
}
