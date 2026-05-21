use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde_json::json;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

use super::{
    IdentityOrphanCounts, NameSurface, Resource, SurfaceBinding, SurfaceBindingKind, TokenLineage,
    load_name_surface, load_name_surface_including_noncanonical, load_resource,
    load_resource_including_noncanonical, load_surface_binding,
    load_surface_binding_including_noncanonical, load_surface_bindings_by_logical_name_id,
    load_surface_bindings_by_logical_name_id_including_noncanonical,
    load_surface_bindings_by_resource_id,
    load_surface_bindings_by_resource_id_including_noncanonical, load_token_lineage,
    load_token_lineage_including_noncanonical, mark_identity_rows_range_orphaned,
    mark_surface_binding_range_orphaned, upsert_name_surfaces,
    upsert_name_surfaces_without_snapshots, upsert_resources, upsert_resources_without_snapshots,
    upsert_surface_bindings, upsert_surface_bindings_without_snapshots, upsert_token_lineages,
    upsert_token_lineages_without_snapshots,
};
use crate::{
    CanonicalityState, ChainLineageBlock, default_database_url, upsert_chain_lineage_blocks,
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
            .context("failed to parse database URL for storage identity integration tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_storage_identity_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for storage identity integration tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect storage identity integration test pool")?;

        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for storage identity integration tests")?;

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

fn timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
}

fn lineage_block(
    chain_id: &str,
    block_hash: &str,
    parent_hash: Option<&str>,
    block_number: i64,
    block_timestamp: OffsetDateTime,
    canonicality_state: CanonicalityState,
) -> ChainLineageBlock {
    ChainLineageBlock {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: parent_hash.map(str::to_owned),
        block_number,
        block_timestamp,
        logs_bloom: Some(vec![block_number as u8]),
        transactions_root: Some(format!("0xtx{:02x}", block_number)),
        receipts_root: Some(format!("0xrc{:02x}", block_number)),
        state_root: Some(format!("0xst{:02x}", block_number)),
        canonicality_state,
    }
}

fn anchor(label: &str, block_number: i64) -> (String, String, i64) {
    (
        format!("chain:{label}"),
        format!("0x{label}_{block_number:08x}"),
        block_number,
    )
}

fn token_lineage(
    token_lineage_id: Uuid,
    namespace: &str,
    chain_label: &str,
    block_number: i64,
    canonicality_state: CanonicalityState,
) -> TokenLineage {
    let (chain_id, block_hash, block_number) = anchor(chain_label, block_number);
    TokenLineage {
        token_lineage_id,
        chain_id,
        block_hash,
        block_number,
        provenance: json!({"source": namespace, "anchor": "token_lineage"}),
        canonicality_state,
    }
}

fn resource(
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    namespace: &str,
    chain_label: &str,
    block_number: i64,
    canonicality_state: CanonicalityState,
) -> Resource {
    let (chain_id, block_hash, block_number) = anchor(chain_label, block_number);
    Resource {
        resource_id,
        token_lineage_id,
        chain_id,
        block_hash,
        block_number,
        provenance: json!({"source": namespace, "anchor": "resource"}),
        canonicality_state,
    }
}

fn name_surface(
    logical_name_id: &str,
    input_name: &str,
    normalized_name: &str,
    chain_label: &str,
    block_number: i64,
    canonicality_state: CanonicalityState,
) -> NameSurface {
    let (chain_id, block_hash, block_number) = anchor(chain_label, block_number);
    NameSurface {
        logical_name_id: logical_name_id.to_owned(),
        namespace: "ens".to_owned(),
        input_name: input_name.to_owned(),
        canonical_display_name: input_name.to_owned(),
        normalized_name: normalized_name.to_owned(),
        dns_encoded_name: vec![4, b't', b'e', b's', b't', 3, b'e', b't', b'h', 0],
        namehash: format!("namehash:{normalized_name}"),
        labelhashes: vec![format!("labelhash:{normalized_name}")],
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id,
        block_hash,
        block_number,
        provenance: json!({"source": "registry_sync", "surface": logical_name_id}),
        canonicality_state,
    }
}

struct BindingSeed<'a> {
    surface_binding_id: Uuid,
    logical_name_id: &'a str,
    resource_id: Uuid,
    binding_kind: SurfaceBindingKind,
    active_from: OffsetDateTime,
    active_to: Option<OffsetDateTime>,
    source: &'a str,
    chain_label: &'a str,
    block_number: i64,
    canonicality_state: CanonicalityState,
}

fn binding(seed: BindingSeed<'_>) -> SurfaceBinding {
    let (chain_id, block_hash, block_number) = anchor(seed.chain_label, seed.block_number);
    SurfaceBinding {
        surface_binding_id: seed.surface_binding_id,
        logical_name_id: seed.logical_name_id.to_owned(),
        resource_id: seed.resource_id,
        binding_kind: seed.binding_kind,
        active_from: seed.active_from,
        active_to: seed.active_to,
        chain_id,
        block_hash,
        block_number,
        provenance: json!({"source": seed.source}),
        canonicality_state: seed.canonicality_state,
    }
}

#[tokio::test]
async fn persists_canonical_surface_round_trip_with_resource_and_token_lineage() -> Result<()> {
    let database = TestDatabase::new().await?;
    let token_lineage_id = Uuid::from_u128(0x1000);
    let resource_id = Uuid::from_u128(0x2000);
    let surface_binding_id = Uuid::from_u128(0x3000);

    let expected_token_lineage = token_lineage(
        token_lineage_id,
        "ens",
        "token_round_trip",
        101,
        CanonicalityState::Finalized,
    );
    let expected_resource = resource(
        resource_id,
        Some(token_lineage_id),
        "ens",
        "resource_round_trip",
        102,
        CanonicalityState::Canonical,
    );
    let expected_surface = name_surface(
        "ens:test.eth",
        "test.eth",
        "test.eth",
        "surface_round_trip",
        103,
        CanonicalityState::Finalized,
    );
    let expected_binding = binding(BindingSeed {
        surface_binding_id,
        logical_name_id: "ens:test.eth",
        resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: timestamp(1_717_171_700),
        active_to: None,
        source: "declared_registry_path",
        chain_label: "binding_round_trip",
        block_number: 104,
        canonicality_state: CanonicalityState::Safe,
    });

    assert_eq!(
        upsert_token_lineages(
            database.pool(),
            std::slice::from_ref(&expected_token_lineage)
        )
        .await?,
        vec![expected_token_lineage.clone()]
    );
    assert_eq!(
        upsert_resources(database.pool(), std::slice::from_ref(&expected_resource)).await?,
        vec![expected_resource.clone()]
    );
    assert_eq!(
        upsert_name_surfaces(database.pool(), std::slice::from_ref(&expected_surface)).await?,
        vec![expected_surface.clone()]
    );
    assert_eq!(
        upsert_surface_bindings(database.pool(), std::slice::from_ref(&expected_binding)).await?,
        vec![expected_binding.clone()]
    );

    assert_eq!(
        load_token_lineage(database.pool(), token_lineage_id).await?,
        Some(expected_token_lineage)
    );
    assert_eq!(
        load_resource(database.pool(), resource_id).await?,
        Some(expected_resource)
    );
    assert_eq!(
        load_name_surface(database.pool(), "ens:test.eth").await?,
        Some(expected_surface)
    );
    assert_eq!(
        load_surface_binding(database.pool(), surface_binding_id).await?,
        Some(expected_binding.clone())
    );
    assert_eq!(
        load_surface_bindings_by_logical_name_id(database.pool(), "ens:test.eth").await?,
        vec![expected_binding.clone()]
    );
    assert_eq!(
        load_surface_bindings_by_resource_id(database.pool(), resource_id).await?,
        vec![expected_binding]
    );

    database.cleanup().await
}

#[tokio::test]
async fn upsert_surface_binding_tightens_replayed_active_to() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x2010);
    let surface_binding_id = Uuid::from_u128(0x3010);
    let earlier_close = timestamp(1_717_171_900);
    let later_close = timestamp(1_717_172_100);

    upsert_resources(
        database.pool(),
        &[resource(
            resource_id,
            None,
            "ens",
            "resource_tighten",
            112,
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[name_surface(
            "ens:tighten.eth",
            "tighten.eth",
            "tighten.eth",
            "surface_tighten",
            113,
            CanonicalityState::Finalized,
        )],
    )
    .await?;

    let initial_binding = binding(BindingSeed {
        surface_binding_id,
        logical_name_id: "ens:tighten.eth",
        resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: timestamp(1_717_171_700),
        active_to: Some(later_close),
        source: "declared_registry_path",
        chain_label: "binding_tighten",
        block_number: 114,
        canonicality_state: CanonicalityState::Canonical,
    });
    upsert_surface_bindings(database.pool(), std::slice::from_ref(&initial_binding)).await?;

    let tightened_binding = SurfaceBinding {
        active_to: Some(earlier_close),
        ..initial_binding
    };
    assert_eq!(
        upsert_surface_bindings(database.pool(), std::slice::from_ref(&tightened_binding)).await?,
        vec![tightened_binding.clone()]
    );
    assert_eq!(
        load_surface_binding(database.pool(), surface_binding_id).await?,
        Some(tightened_binding)
    );

    database.cleanup().await
}

async fn load_surface_binding_observed_at(
    pool: &PgPool,
    surface_binding_id: Uuid,
) -> Result<OffsetDateTime> {
    sqlx::query_scalar::<_, OffsetDateTime>(
        "SELECT observed_at FROM surface_bindings WHERE surface_binding_id = $1",
    )
    .bind(surface_binding_id)
    .fetch_one(pool)
    .await
    .context("failed to load surface binding observed_at")
}

async fn load_resource_observed_at(pool: &PgPool, resource_id: Uuid) -> Result<OffsetDateTime> {
    sqlx::query_scalar::<_, OffsetDateTime>(
        "SELECT observed_at FROM resources WHERE resource_id = $1",
    )
    .bind(resource_id)
    .fetch_one(pool)
    .await
    .context("failed to load resource observed_at")
}

async fn load_token_lineage_observed_at(
    pool: &PgPool,
    token_lineage_id: Uuid,
) -> Result<OffsetDateTime> {
    sqlx::query_scalar::<_, OffsetDateTime>(
        "SELECT observed_at FROM token_lineages WHERE token_lineage_id = $1",
    )
    .bind(token_lineage_id)
    .fetch_one(pool)
    .await
    .context("failed to load token lineage observed_at")
}

async fn load_name_surface_observed_at(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<OffsetDateTime> {
    sqlx::query_scalar::<_, OffsetDateTime>(
        "SELECT observed_at FROM name_surfaces WHERE logical_name_id = $1",
    )
    .bind(logical_name_id)
    .fetch_one(pool)
    .await
    .context("failed to load name surface observed_at")
}

#[tokio::test]
async fn no_snapshot_token_lineage_upsert_accepts_compatible_existing_lineage() -> Result<()> {
    let database = TestDatabase::new().await?;
    let token_lineage_id = Uuid::from_u128(0xf100);
    let anchored_observed_at = timestamp(946_684_800);
    let initial_lineage = token_lineage(
        token_lineage_id,
        "ens",
        "token_no_snapshot_initial",
        111,
        CanonicalityState::Canonical,
    );
    upsert_token_lineages_without_snapshots(
        database.pool(),
        std::slice::from_ref(&initial_lineage),
    )
    .await?;

    sqlx::query("UPDATE token_lineages SET observed_at = $1 WHERE token_lineage_id = $2")
        .bind(anchored_observed_at)
        .bind(token_lineage_id)
        .execute(database.pool())
        .await
        .context("failed to anchor token lineage observed_at")?;

    let mut compatible_lineage = token_lineage(
        token_lineage_id,
        "ens",
        "token_no_snapshot_later",
        112,
        CanonicalityState::Finalized,
    );
    compatible_lineage.provenance =
        json!({"source": "ens", "anchor": "token_lineage", "later_observation": true});
    upsert_token_lineages_without_snapshots(
        database.pool(),
        std::slice::from_ref(&compatible_lineage),
    )
    .await?;

    let mut expected_lineage = initial_lineage;
    expected_lineage.canonicality_state = CanonicalityState::Finalized;
    assert_eq!(
        load_token_lineage(database.pool(), token_lineage_id).await?,
        Some(expected_lineage)
    );
    assert_eq!(
        load_token_lineage_observed_at(database.pool(), token_lineage_id).await?,
        anchored_observed_at
    );

    database.cleanup().await
}

#[tokio::test]
async fn no_snapshot_resource_upsert_accepts_compatible_existing_resource() -> Result<()> {
    let database = TestDatabase::new().await?;
    let token_lineage_id = Uuid::from_u128(0xf101);
    let resource_id = Uuid::from_u128(0xf102);
    let anchored_observed_at = timestamp(946_684_800);

    upsert_token_lineages(
        database.pool(),
        &[token_lineage(
            token_lineage_id,
            "ens",
            "token_resource_no_snapshot",
            111,
            CanonicalityState::Canonical,
        )],
    )
    .await?;

    let initial_resource = resource(
        resource_id,
        Some(token_lineage_id),
        "ens",
        "resource_no_snapshot_initial",
        112,
        CanonicalityState::Canonical,
    );
    upsert_resources_without_snapshots(database.pool(), std::slice::from_ref(&initial_resource))
        .await?;

    sqlx::query("UPDATE resources SET observed_at = $1 WHERE resource_id = $2")
        .bind(anchored_observed_at)
        .bind(resource_id)
        .execute(database.pool())
        .await
        .context("failed to anchor resource observed_at")?;

    let mut compatible_resource = resource(
        resource_id,
        Some(token_lineage_id),
        "ens",
        "resource_no_snapshot_later",
        113,
        CanonicalityState::Finalized,
    );
    compatible_resource.provenance =
        json!({"source": "ens", "anchor": "resource", "later_observation": true});
    upsert_resources_without_snapshots(database.pool(), std::slice::from_ref(&compatible_resource))
        .await?;

    let mut expected_resource = initial_resource;
    expected_resource.canonicality_state = CanonicalityState::Finalized;
    assert_eq!(
        load_resource(database.pool(), resource_id).await?,
        Some(expected_resource)
    );
    assert_eq!(
        load_resource_observed_at(database.pool(), resource_id).await?,
        anchored_observed_at
    );

    database.cleanup().await
}

#[tokio::test]
async fn no_snapshot_name_surface_upsert_accepts_compatible_existing_surface() -> Result<()> {
    let database = TestDatabase::new().await?;
    let logical_name_id = "ens:no-snapshot-surface.eth";
    let anchored_observed_at = timestamp(946_684_800);
    let initial_surface = name_surface(
        logical_name_id,
        "no-snapshot-surface.eth",
        "no-snapshot-surface.eth",
        "surface_no_snapshot_initial",
        113,
        CanonicalityState::Canonical,
    );
    upsert_name_surfaces_without_snapshots(database.pool(), std::slice::from_ref(&initial_surface))
        .await?;

    sqlx::query("UPDATE name_surfaces SET observed_at = $1 WHERE logical_name_id = $2")
        .bind(anchored_observed_at)
        .bind(logical_name_id)
        .execute(database.pool())
        .await
        .context("failed to anchor name surface observed_at")?;

    let mut compatible_surface = name_surface(
        logical_name_id,
        "no-snapshot-surface.eth",
        "no-snapshot-surface.eth",
        "surface_no_snapshot_later",
        114,
        CanonicalityState::Finalized,
    );
    compatible_surface.input_name = "No-Snapshot-Surface.eth".to_owned();
    compatible_surface.canonical_display_name = "no-snapshot-surface.eth".to_owned();
    compatible_surface.normalizer_version = "ensip15@ens-normalize-0.1.1".to_owned();
    compatible_surface.normalization_warnings = json!(["display_metadata_changed"]);
    upsert_name_surfaces_without_snapshots(
        database.pool(),
        std::slice::from_ref(&compatible_surface),
    )
    .await?;

    let mut expected_surface = initial_surface;
    expected_surface.canonicality_state = CanonicalityState::Finalized;
    assert_eq!(
        load_name_surface(database.pool(), logical_name_id).await?,
        Some(expected_surface)
    );
    assert_eq!(
        load_name_surface_observed_at(database.pool(), logical_name_id).await?,
        anchored_observed_at
    );

    database.cleanup().await
}

#[tokio::test]
async fn no_snapshot_surface_binding_upsert_skips_idempotent_rewrite() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x2011);
    let surface_binding_id = Uuid::from_u128(0x3011);
    let anchored_observed_at = timestamp(946_684_800);
    let earlier_close = timestamp(1_717_171_900);
    let later_close = timestamp(1_717_172_100);

    upsert_resources(
        database.pool(),
        &[resource(
            resource_id,
            None,
            "ens",
            "resource_no_snapshot_idempotent",
            112,
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[name_surface(
            "ens:no-snapshot-idempotent.eth",
            "no-snapshot-idempotent.eth",
            "no-snapshot-idempotent.eth",
            "surface_no_snapshot_idempotent",
            113,
            CanonicalityState::Finalized,
        )],
    )
    .await?;

    let initial_binding = binding(BindingSeed {
        surface_binding_id,
        logical_name_id: "ens:no-snapshot-idempotent.eth",
        resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: timestamp(1_717_171_700),
        active_to: Some(later_close),
        source: "declared_registry_path",
        chain_label: "binding_no_snapshot_idempotent",
        block_number: 114,
        canonicality_state: CanonicalityState::Canonical,
    });
    upsert_surface_bindings_without_snapshots(
        database.pool(),
        std::slice::from_ref(&initial_binding),
    )
    .await?;

    sqlx::query("UPDATE surface_bindings SET observed_at = $1 WHERE surface_binding_id = $2")
        .bind(anchored_observed_at)
        .bind(surface_binding_id)
        .execute(database.pool())
        .await
        .context("failed to anchor surface binding observed_at")?;

    upsert_surface_bindings_without_snapshots(
        database.pool(),
        std::slice::from_ref(&initial_binding),
    )
    .await?;
    assert_eq!(
        load_surface_binding_observed_at(database.pool(), surface_binding_id).await?,
        anchored_observed_at
    );

    let tightened_binding = SurfaceBinding {
        active_to: Some(earlier_close),
        ..initial_binding
    };
    upsert_surface_bindings_without_snapshots(
        database.pool(),
        std::slice::from_ref(&tightened_binding),
    )
    .await?;
    assert_eq!(
        load_surface_binding(database.pool(), surface_binding_id).await?,
        Some(tightened_binding)
    );
    assert!(
        load_surface_binding_observed_at(database.pool(), surface_binding_id).await?
            > anchored_observed_at
    );

    database.cleanup().await
}

#[tokio::test]
async fn no_snapshot_surface_binding_upsert_closes_existing_before_opening_replacement()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let old_resource_id = Uuid::from_u128(0x2012);
    let new_resource_id = Uuid::from_u128(0x2013);
    let old_binding_id = Uuid::from_u128(0x3012);
    let new_binding_id = Uuid::from_u128(0x3013);
    let first_start = timestamp(1_717_171_700);
    let rebind_at = timestamp(1_717_171_900);

    upsert_resources(
        database.pool(),
        &[
            resource(
                old_resource_id,
                None,
                "ens",
                "resource_no_snapshot_old",
                116,
                CanonicalityState::Finalized,
            ),
            resource(
                new_resource_id,
                None,
                "ens",
                "resource_no_snapshot_new",
                117,
                CanonicalityState::Finalized,
            ),
        ],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[name_surface(
            "ens:no-snapshot-rebind.eth",
            "no-snapshot-rebind.eth",
            "no-snapshot-rebind.eth",
            "surface_no_snapshot_rebind",
            118,
            CanonicalityState::Finalized,
        )],
    )
    .await?;

    let initial_binding = binding(BindingSeed {
        surface_binding_id: old_binding_id,
        logical_name_id: "ens:no-snapshot-rebind.eth",
        resource_id: old_resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: first_start,
        active_to: None,
        source: "no_snapshot_old",
        chain_label: "binding_no_snapshot_old",
        block_number: 119,
        canonicality_state: CanonicalityState::Finalized,
    });
    upsert_surface_bindings_without_snapshots(
        database.pool(),
        std::slice::from_ref(&initial_binding),
    )
    .await?;

    let closed_binding = SurfaceBinding {
        active_to: Some(rebind_at),
        ..initial_binding
    };
    let replacement_binding = binding(BindingSeed {
        surface_binding_id: new_binding_id,
        logical_name_id: "ens:no-snapshot-rebind.eth",
        resource_id: new_resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: rebind_at,
        active_to: None,
        source: "no_snapshot_new",
        chain_label: "binding_no_snapshot_new",
        block_number: 120,
        canonicality_state: CanonicalityState::Finalized,
    });

    upsert_surface_bindings_without_snapshots(
        database.pool(),
        &[closed_binding.clone(), replacement_binding.clone()],
    )
    .await?;

    let bindings =
        load_surface_bindings_by_logical_name_id(database.pool(), "ens:no-snapshot-rebind.eth")
            .await?;
    assert_eq!(
        bindings,
        vec![closed_binding.clone(), replacement_binding.clone()]
    );
    assert_eq!(
        load_surface_binding(database.pool(), old_binding_id).await?,
        Some(closed_binding)
    );
    assert_eq!(
        load_surface_binding(database.pool(), new_binding_id).await?,
        Some(replacement_binding)
    );

    database.cleanup().await
}

#[tokio::test]
async fn closes_open_binding_interval_on_rebind_and_preserves_history_continuity() -> Result<()> {
    let database = TestDatabase::new().await?;
    let old_token_lineage_id = Uuid::from_u128(0x4000);
    let new_token_lineage_id = Uuid::from_u128(0x5000);
    let old_resource_id = Uuid::from_u128(0x6000);
    let new_resource_id = Uuid::from_u128(0x7000);
    let first_binding_id = Uuid::from_u128(0x8000);
    let second_binding_id = Uuid::from_u128(0x9000);
    let first_start = timestamp(1_717_171_710);
    let rebind_at = timestamp(1_717_171_900);

    upsert_token_lineages(
        database.pool(),
        &[
            token_lineage(
                old_token_lineage_id,
                "ens-old",
                "token_old",
                201,
                CanonicalityState::Finalized,
            ),
            token_lineage(
                new_token_lineage_id,
                "ens-new",
                "token_new",
                202,
                CanonicalityState::Finalized,
            ),
        ],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[
            resource(
                old_resource_id,
                Some(old_token_lineage_id),
                "ens-old",
                "resource_old",
                203,
                CanonicalityState::Canonical,
            ),
            resource(
                new_resource_id,
                Some(new_token_lineage_id),
                "ens-new",
                "resource_new",
                204,
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[name_surface(
            "ens:rebind.eth",
            "rebind.eth",
            "rebind.eth",
            "surface_rebind",
            205,
            CanonicalityState::Finalized,
        )],
    )
    .await?;

    let initial_binding = binding(BindingSeed {
        surface_binding_id: first_binding_id,
        logical_name_id: "ens:rebind.eth",
        resource_id: old_resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: first_start,
        active_to: None,
        source: "initial_bind",
        chain_label: "binding_initial",
        block_number: 206,
        canonicality_state: CanonicalityState::Finalized,
    });
    upsert_surface_bindings(database.pool(), &[initial_binding]).await?;

    let closed_binding = binding(BindingSeed {
        surface_binding_id: first_binding_id,
        logical_name_id: "ens:rebind.eth",
        resource_id: old_resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: first_start,
        active_to: Some(rebind_at),
        source: "initial_bind",
        chain_label: "binding_initial",
        block_number: 206,
        canonicality_state: CanonicalityState::Finalized,
    });
    let rebound_binding = binding(BindingSeed {
        surface_binding_id: second_binding_id,
        logical_name_id: "ens:rebind.eth",
        resource_id: new_resource_id,
        binding_kind: SurfaceBindingKind::MigrationRebind,
        active_from: rebind_at,
        active_to: None,
        source: "migration_rebind",
        chain_label: "binding_rebind",
        block_number: 207,
        canonicality_state: CanonicalityState::Safe,
    });
    upsert_surface_bindings(
        database.pool(),
        &[closed_binding.clone(), rebound_binding.clone()],
    )
    .await?;

    let bindings =
        load_surface_bindings_by_logical_name_id(database.pool(), "ens:rebind.eth").await?;
    assert_eq!(
        bindings,
        vec![closed_binding.clone(), rebound_binding.clone()]
    );
    assert_eq!(bindings[0].active_to, Some(bindings[1].active_from));
    assert_ne!(bindings[0].resource_id, bindings[1].resource_id);
    assert_eq!(
        load_surface_binding(database.pool(), first_binding_id).await?,
        Some(closed_binding)
    );
    assert_eq!(
        load_surface_binding(database.pool(), second_binding_id).await?,
        Some(rebound_binding)
    );

    database.cleanup().await
}

#[tokio::test]
async fn loads_shared_resource_bindings_for_multiple_surfaces() -> Result<()> {
    let database = TestDatabase::new().await?;
    let token_lineage_id = Uuid::from_u128(0xa000);
    let shared_resource_id = Uuid::from_u128(0xb000);
    let first_binding = binding(BindingSeed {
        surface_binding_id: Uuid::from_u128(0xc000),
        logical_name_id: "ens:alpha.eth",
        resource_id: shared_resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: timestamp(1_717_171_720),
        active_to: None,
        source: "alpha_declared",
        chain_label: "binding_alpha",
        block_number: 305,
        canonicality_state: CanonicalityState::Finalized,
    });
    let second_binding = binding(BindingSeed {
        surface_binding_id: Uuid::from_u128(0xd000),
        logical_name_id: "ens:beta.eth",
        resource_id: shared_resource_id,
        binding_kind: SurfaceBindingKind::LinkedSubregistryPath,
        active_from: timestamp(1_717_171_730),
        active_to: None,
        source: "beta_linked",
        chain_label: "binding_beta",
        block_number: 306,
        canonicality_state: CanonicalityState::Safe,
    });

    upsert_token_lineages(
        database.pool(),
        &[token_lineage(
            token_lineage_id,
            "ens",
            "token_shared",
            301,
            CanonicalityState::Finalized,
        )],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[resource(
            shared_resource_id,
            Some(token_lineage_id),
            "ens",
            "resource_shared",
            302,
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[
            name_surface(
                "ens:alpha.eth",
                "alpha.eth",
                "alpha.eth",
                "surface_alpha",
                303,
                CanonicalityState::Finalized,
            ),
            name_surface(
                "ens:beta.eth",
                "beta.eth",
                "beta.eth",
                "surface_beta",
                304,
                CanonicalityState::Finalized,
            ),
        ],
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &[first_binding.clone(), second_binding.clone()],
    )
    .await?;

    assert_eq!(
        load_surface_bindings_by_logical_name_id(database.pool(), "ens:alpha.eth").await?,
        vec![first_binding.clone()]
    );
    assert_eq!(
        load_surface_bindings_by_logical_name_id(database.pool(), "ens:beta.eth").await?,
        vec![second_binding.clone()]
    );
    assert_eq!(
        load_surface_bindings_by_resource_id(database.pool(), shared_resource_id).await?,
        vec![first_binding, second_binding]
    );

    database.cleanup().await
}

#[tokio::test]
async fn rejects_placeholder_anchor_defaults_in_identity_rows() -> Result<()> {
    let database = TestDatabase::new().await?;
    let error = upsert_token_lineages(
        database.pool(),
        &[TokenLineage {
            token_lineage_id: Uuid::from_u128(0xe000),
            chain_id: "unknown".to_owned(),
            block_hash: "unknown".to_owned(),
            block_number: 0,
            provenance: json!({"source": "bad_anchor"}),
            canonicality_state: CanonicalityState::Observed,
        }],
    )
    .await
    .expect_err("placeholder migration defaults must be rejected");

    assert!(
        error
            .to_string()
            .contains("must provide a real chain_id anchor"),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn rejects_overlapping_or_duplicate_current_bindings_for_one_logical_name_id() -> Result<()> {
    let database = TestDatabase::new().await?;
    let first_resource_id = Uuid::from_u128(0xe100);
    let second_resource_id = Uuid::from_u128(0xe101);

    upsert_token_lineages(
        database.pool(),
        &[
            token_lineage(
                Uuid::from_u128(0xe102),
                "ens",
                "token_overlap_1",
                401,
                CanonicalityState::Finalized,
            ),
            token_lineage(
                Uuid::from_u128(0xe103),
                "ens",
                "token_overlap_2",
                402,
                CanonicalityState::Finalized,
            ),
        ],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[
            resource(
                first_resource_id,
                Some(Uuid::from_u128(0xe102)),
                "ens",
                "resource_overlap_1",
                403,
                CanonicalityState::Canonical,
            ),
            resource(
                second_resource_id,
                Some(Uuid::from_u128(0xe103)),
                "ens",
                "resource_overlap_2",
                404,
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[name_surface(
            "ens:overlap.eth",
            "overlap.eth",
            "overlap.eth",
            "surface_overlap",
            405,
            CanonicalityState::Finalized,
        )],
    )
    .await?;

    upsert_surface_bindings(
        database.pool(),
        &[binding(BindingSeed {
            surface_binding_id: Uuid::from_u128(0xe104),
            logical_name_id: "ens:overlap.eth",
            resource_id: first_resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from: timestamp(1_717_172_000),
            active_to: None,
            source: "current_1",
            chain_label: "binding_overlap_1",
            block_number: 406,
            canonicality_state: CanonicalityState::Finalized,
        })],
    )
    .await?;

    let error = upsert_surface_bindings(
        database.pool(),
        &[binding(BindingSeed {
            surface_binding_id: Uuid::from_u128(0xe105),
            logical_name_id: "ens:overlap.eth",
            resource_id: second_resource_id,
            binding_kind: SurfaceBindingKind::MigrationRebind,
            active_from: timestamp(1_717_172_100),
            active_to: None,
            source: "current_2",
            chain_label: "binding_overlap_2",
            block_number: 407,
            canonicality_state: CanonicalityState::Finalized,
        })],
    )
    .await
    .expect_err("overlapping current bindings must be rejected");

    let error_chain = format!("{error:#}");
    assert!(
        error_chain.contains("surface_bindings_no_overlap"),
        "unexpected error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn orphaned_binding_can_coexist_with_overlapping_replacement_after_repair() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain_id = "chain:binding_reorg";
    let parent_hash = "0xparent_binding_reorg";
    let losing_hash = "0xlosing_binding_reorg";
    let replacement_hash = "0xreplacement_binding_reorg";
    let active_from = timestamp(1_717_172_400);
    let old_binding_id = Uuid::from_u128(0xe110);
    let replacement_binding_id = Uuid::from_u128(0xe111);
    let old_resource_id = Uuid::from_u128(0xe112);
    let replacement_resource_id = Uuid::from_u128(0xe113);
    let old_token_lineage_id = Uuid::from_u128(0xe114);
    let replacement_token_lineage_id = Uuid::from_u128(0xe115);

    upsert_chain_lineage_blocks(
        database.pool(),
        &[
            lineage_block(
                chain_id,
                parent_hash,
                Some("0xgenesis_binding_reorg"),
                9,
                timestamp(1_717_172_390),
                CanonicalityState::Finalized,
            ),
            lineage_block(
                chain_id,
                losing_hash,
                Some(parent_hash),
                10,
                timestamp(1_717_172_395),
                CanonicalityState::Finalized,
            ),
            lineage_block(
                chain_id,
                replacement_hash,
                Some(parent_hash),
                10,
                timestamp(1_717_172_396),
                CanonicalityState::Finalized,
            ),
        ],
    )
    .await?;

    upsert_token_lineages(
        database.pool(),
        &[
            TokenLineage {
                token_lineage_id: old_token_lineage_id,
                chain_id: chain_id.to_owned(),
                block_hash: losing_hash.to_owned(),
                block_number: 10,
                provenance: json!({"source": "losing_branch"}),
                canonicality_state: CanonicalityState::Finalized,
            },
            TokenLineage {
                token_lineage_id: replacement_token_lineage_id,
                chain_id: chain_id.to_owned(),
                block_hash: replacement_hash.to_owned(),
                block_number: 10,
                provenance: json!({"source": "replacement_branch"}),
                canonicality_state: CanonicalityState::Finalized,
            },
        ],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[
            Resource {
                resource_id: old_resource_id,
                token_lineage_id: Some(old_token_lineage_id),
                chain_id: chain_id.to_owned(),
                block_hash: losing_hash.to_owned(),
                block_number: 10,
                provenance: json!({"source": "losing_branch"}),
                canonicality_state: CanonicalityState::Finalized,
            },
            Resource {
                resource_id: replacement_resource_id,
                token_lineage_id: Some(replacement_token_lineage_id),
                chain_id: chain_id.to_owned(),
                block_hash: replacement_hash.to_owned(),
                block_number: 10,
                provenance: json!({"source": "replacement_branch"}),
                canonicality_state: CanonicalityState::Finalized,
            },
        ],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[NameSurface {
            logical_name_id: "ens:repair.eth".to_owned(),
            namespace: "ens".to_owned(),
            input_name: "repair.eth".to_owned(),
            canonical_display_name: "repair.eth".to_owned(),
            normalized_name: "repair.eth".to_owned(),
            dns_encoded_name: vec![
                6, b'r', b'e', b'p', b'a', b'i', b'r', 3, b'e', b't', b'h', 0,
            ],
            namehash: "namehash:repair.eth".to_owned(),
            labelhashes: vec!["labelhash:repair.eth".to_owned()],
            normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: chain_id.to_owned(),
            block_hash: parent_hash.to_owned(),
            block_number: 9,
            provenance: json!({"source": "surface_branch"}),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;

    let old_binding = SurfaceBinding {
        surface_binding_id: old_binding_id,
        logical_name_id: "ens:repair.eth".to_owned(),
        resource_id: old_resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from,
        active_to: None,
        chain_id: chain_id.to_owned(),
        block_hash: losing_hash.to_owned(),
        block_number: 10,
        provenance: json!({"source": "losing_binding"}),
        canonicality_state: CanonicalityState::Finalized,
    };
    upsert_surface_bindings(database.pool(), std::slice::from_ref(&old_binding)).await?;

    let orphaned_count = mark_surface_binding_range_orphaned(
        database.pool(),
        chain_id,
        losing_hash,
        Some(parent_hash),
    )
    .await?;
    assert_eq!(orphaned_count, 1);
    assert_eq!(
        load_token_lineage(database.pool(), old_token_lineage_id).await?,
        Some(TokenLineage {
            token_lineage_id: old_token_lineage_id,
            chain_id: chain_id.to_owned(),
            block_hash: losing_hash.to_owned(),
            block_number: 10,
            provenance: json!({"source": "losing_branch"}),
            canonicality_state: CanonicalityState::Finalized,
        })
    );
    assert_eq!(
        load_resource(database.pool(), old_resource_id).await?,
        Some(Resource {
            resource_id: old_resource_id,
            token_lineage_id: Some(old_token_lineage_id),
            chain_id: chain_id.to_owned(),
            block_hash: losing_hash.to_owned(),
            block_number: 10,
            provenance: json!({"source": "losing_branch"}),
            canonicality_state: CanonicalityState::Finalized,
        })
    );
    assert_eq!(
        load_name_surface(database.pool(), "ens:repair.eth").await?,
        Some(NameSurface {
            logical_name_id: "ens:repair.eth".to_owned(),
            namespace: "ens".to_owned(),
            input_name: "repair.eth".to_owned(),
            canonical_display_name: "repair.eth".to_owned(),
            normalized_name: "repair.eth".to_owned(),
            dns_encoded_name: vec![
                6, b'r', b'e', b'p', b'a', b'i', b'r', 3, b'e', b't', b'h', 0,
            ],
            namehash: "namehash:repair.eth".to_owned(),
            labelhashes: vec!["labelhash:repair.eth".to_owned()],
            normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: chain_id.to_owned(),
            block_hash: parent_hash.to_owned(),
            block_number: 9,
            provenance: json!({"source": "surface_branch"}),
            canonicality_state: CanonicalityState::Finalized,
        })
    );

    let replacement_binding = SurfaceBinding {
        surface_binding_id: replacement_binding_id,
        logical_name_id: "ens:repair.eth".to_owned(),
        resource_id: replacement_resource_id,
        binding_kind: SurfaceBindingKind::MigrationRebind,
        active_from,
        active_to: None,
        chain_id: chain_id.to_owned(),
        block_hash: replacement_hash.to_owned(),
        block_number: 10,
        provenance: json!({"source": "replacement_binding"}),
        canonicality_state: CanonicalityState::Finalized,
    };
    upsert_surface_bindings(database.pool(), std::slice::from_ref(&replacement_binding)).await?;

    let orphaned_binding =
        load_surface_binding_including_noncanonical(database.pool(), old_binding_id)
            .await?
            .expect("orphaned binding should remain accessible via history path");
    assert_eq!(
        orphaned_binding.canonicality_state,
        CanonicalityState::Orphaned
    );
    assert_eq!(
        load_surface_binding(database.pool(), old_binding_id).await?,
        None
    );
    assert_eq!(
        load_surface_bindings_by_logical_name_id(database.pool(), "ens:repair.eth").await?,
        vec![replacement_binding.clone()]
    );
    assert_eq!(
        load_surface_bindings_by_logical_name_id_including_noncanonical(
            database.pool(),
            "ens:repair.eth",
        )
        .await?,
        vec![orphaned_binding, replacement_binding.clone()]
    );
    assert_eq!(
        load_surface_binding(database.pool(), replacement_binding_id).await?,
        Some(replacement_binding)
    );

    database.cleanup().await
}

#[tokio::test]
async fn orphaned_stable_identity_rows_can_be_reobserved_with_same_ids_on_winning_branch()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let chain_id = "chain:stable_identity_reorg";
    let parent_hash = "0xparent_stable_identity_reorg";
    let losing_hash = "0xlosing_stable_identity_reorg";
    let winning_hash = "0xwinning_stable_identity_reorg";
    let token_lineage_id = Uuid::from_u128(0xe120);
    let resource_id = Uuid::from_u128(0xe121);

    upsert_chain_lineage_blocks(
        database.pool(),
        &[
            lineage_block(
                chain_id,
                parent_hash,
                Some("0xgenesis_stable_identity_reorg"),
                20,
                timestamp(1_717_172_500),
                CanonicalityState::Finalized,
            ),
            lineage_block(
                chain_id,
                losing_hash,
                Some(parent_hash),
                21,
                timestamp(1_717_172_510),
                CanonicalityState::Finalized,
            ),
            lineage_block(
                chain_id,
                winning_hash,
                Some(parent_hash),
                21,
                timestamp(1_717_172_511),
                CanonicalityState::Finalized,
            ),
        ],
    )
    .await?;

    upsert_token_lineages(
        database.pool(),
        &[TokenLineage {
            token_lineage_id,
            chain_id: chain_id.to_owned(),
            block_hash: losing_hash.to_owned(),
            block_number: 21,
            provenance: json!({"source": "losing_token"}),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[Resource {
            resource_id,
            token_lineage_id: Some(token_lineage_id),
            chain_id: chain_id.to_owned(),
            block_hash: losing_hash.to_owned(),
            block_number: 21,
            provenance: json!({"source": "losing_resource"}),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[NameSurface {
            logical_name_id: "ens:stable.eth".to_owned(),
            namespace: "ens".to_owned(),
            input_name: "stable.eth".to_owned(),
            canonical_display_name: "stable.eth".to_owned(),
            normalized_name: "stable.eth".to_owned(),
            dns_encoded_name: vec![
                6, b's', b't', b'a', b'b', b'l', b'e', 3, b'e', b't', b'h', 0,
            ],
            namehash: "namehash:stable.eth".to_owned(),
            labelhashes: vec!["labelhash:stable.eth".to_owned()],
            normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: chain_id.to_owned(),
            block_hash: losing_hash.to_owned(),
            block_number: 21,
            provenance: json!({"source": "losing_surface"}),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;

    let orphan_counts = mark_identity_rows_range_orphaned(
        database.pool(),
        chain_id,
        losing_hash,
        Some(parent_hash),
    )
    .await?;
    assert_eq!(
        orphan_counts,
        IdentityOrphanCounts {
            token_lineage_count: 1,
            resource_count: 1,
            name_surface_count: 1,
            surface_binding_count: 0,
        }
    );

    let winning_token_lineage = TokenLineage {
        token_lineage_id,
        chain_id: chain_id.to_owned(),
        block_hash: winning_hash.to_owned(),
        block_number: 21,
        provenance: json!({"source": "winning_token"}),
        canonicality_state: CanonicalityState::Finalized,
    };
    let winning_resource = Resource {
        resource_id,
        token_lineage_id: Some(token_lineage_id),
        chain_id: chain_id.to_owned(),
        block_hash: winning_hash.to_owned(),
        block_number: 21,
        provenance: json!({"source": "winning_resource"}),
        canonicality_state: CanonicalityState::Finalized,
    };
    let winning_surface = NameSurface {
        logical_name_id: "ens:stable.eth".to_owned(),
        namespace: "ens".to_owned(),
        input_name: "stable.eth".to_owned(),
        canonical_display_name: "stable.eth".to_owned(),
        normalized_name: "stable.eth".to_owned(),
        dns_encoded_name: vec![
            6, b's', b't', b'a', b'b', b'l', b'e', 3, b'e', b't', b'h', 0,
        ],
        namehash: "namehash:stable.eth".to_owned(),
        labelhashes: vec!["labelhash:stable.eth".to_owned()],
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: chain_id.to_owned(),
        block_hash: winning_hash.to_owned(),
        block_number: 21,
        provenance: json!({"source": "winning_surface"}),
        canonicality_state: CanonicalityState::Finalized,
    };

    upsert_token_lineages(
        database.pool(),
        std::slice::from_ref(&winning_token_lineage),
    )
    .await?;
    upsert_resources(database.pool(), std::slice::from_ref(&winning_resource)).await?;
    upsert_name_surfaces(database.pool(), std::slice::from_ref(&winning_surface)).await?;

    assert_eq!(
        load_token_lineage(database.pool(), token_lineage_id).await?,
        Some(winning_token_lineage.clone())
    );
    assert_eq!(
        load_resource(database.pool(), resource_id).await?,
        Some(winning_resource.clone())
    );
    assert_eq!(
        load_name_surface(database.pool(), "ens:stable.eth").await?,
        Some(winning_surface.clone())
    );
    assert_eq!(
        load_token_lineage_including_noncanonical(database.pool(), token_lineage_id).await?,
        Some(winning_token_lineage)
    );
    assert_eq!(
        load_resource_including_noncanonical(database.pool(), resource_id).await?,
        Some(winning_resource)
    );
    assert_eq!(
        load_name_surface_including_noncanonical(database.pool(), "ens:stable.eth").await?,
        Some(winning_surface)
    );

    database.cleanup().await
}

#[tokio::test]
async fn canonical_only_default_reads_exclude_observed_and_orphaned() -> Result<()> {
    let database = TestDatabase::new().await?;
    let token_lineage_id = Uuid::from_u128(0xe200);
    let resource_id = Uuid::from_u128(0xe201);
    let surface_binding_id = Uuid::from_u128(0xe202);

    let observed_token_lineage = token_lineage(
        token_lineage_id,
        "ens",
        "token_observed",
        501,
        CanonicalityState::Observed,
    );
    let observed_resource = resource(
        resource_id,
        Some(token_lineage_id),
        "ens",
        "resource_observed",
        502,
        CanonicalityState::Observed,
    );
    let orphaned_surface = name_surface(
        "ens:hidden.eth",
        "hidden.eth",
        "hidden.eth",
        "surface_orphaned",
        503,
        CanonicalityState::Orphaned,
    );
    let observed_binding = binding(BindingSeed {
        surface_binding_id,
        logical_name_id: "ens:hidden.eth",
        resource_id,
        binding_kind: SurfaceBindingKind::ObservedOnly,
        active_from: timestamp(1_717_172_200),
        active_to: None,
        source: "observed_only",
        chain_label: "binding_observed",
        block_number: 504,
        canonicality_state: CanonicalityState::Observed,
    });

    upsert_token_lineages(
        database.pool(),
        std::slice::from_ref(&observed_token_lineage),
    )
    .await?;
    upsert_resources(database.pool(), std::slice::from_ref(&observed_resource)).await?;
    upsert_name_surfaces(database.pool(), std::slice::from_ref(&orphaned_surface)).await?;
    upsert_surface_bindings(database.pool(), std::slice::from_ref(&observed_binding)).await?;

    assert_eq!(
        load_token_lineage(database.pool(), token_lineage_id).await?,
        None
    );
    assert_eq!(load_resource(database.pool(), resource_id).await?, None);
    assert_eq!(
        load_name_surface(database.pool(), "ens:hidden.eth").await?,
        None
    );
    assert_eq!(
        load_surface_binding(database.pool(), surface_binding_id).await?,
        None
    );
    assert!(
        load_surface_bindings_by_logical_name_id(database.pool(), "ens:hidden.eth")
            .await?
            .is_empty()
    );
    assert!(
        load_surface_bindings_by_resource_id(database.pool(), resource_id)
            .await?
            .is_empty()
    );

    database.cleanup().await
}

#[tokio::test]
async fn explicit_noncanonical_opt_in_reads_include_observed_and_orphaned_history() -> Result<()> {
    let database = TestDatabase::new().await?;
    let token_lineage_id = Uuid::from_u128(0xe300);
    let resource_id = Uuid::from_u128(0xe301);
    let surface_binding_id = Uuid::from_u128(0xe302);

    let observed_token_lineage = token_lineage(
        token_lineage_id,
        "ens",
        "token_history",
        601,
        CanonicalityState::Observed,
    );
    let orphaned_resource = resource(
        resource_id,
        Some(token_lineage_id),
        "ens",
        "resource_history",
        602,
        CanonicalityState::Orphaned,
    );
    let observed_surface = name_surface(
        "ens:history.eth",
        "history.eth",
        "history.eth",
        "surface_history",
        603,
        CanonicalityState::Observed,
    );
    let orphaned_binding = binding(BindingSeed {
        surface_binding_id,
        logical_name_id: "ens:history.eth",
        resource_id,
        binding_kind: SurfaceBindingKind::ObservedOnly,
        active_from: timestamp(1_717_172_300),
        active_to: None,
        source: "observed_history",
        chain_label: "binding_history",
        block_number: 604,
        canonicality_state: CanonicalityState::Orphaned,
    });

    upsert_token_lineages(
        database.pool(),
        std::slice::from_ref(&observed_token_lineage),
    )
    .await?;
    upsert_resources(database.pool(), std::slice::from_ref(&orphaned_resource)).await?;
    upsert_name_surfaces(database.pool(), std::slice::from_ref(&observed_surface)).await?;
    upsert_surface_bindings(database.pool(), std::slice::from_ref(&orphaned_binding)).await?;

    assert_eq!(
        load_token_lineage_including_noncanonical(database.pool(), token_lineage_id).await?,
        Some(observed_token_lineage)
    );
    assert_eq!(
        load_resource_including_noncanonical(database.pool(), resource_id).await?,
        Some(orphaned_resource)
    );
    assert_eq!(
        load_name_surface_including_noncanonical(database.pool(), "ens:history.eth").await?,
        Some(observed_surface)
    );
    assert_eq!(
        load_surface_binding_including_noncanonical(database.pool(), surface_binding_id).await?,
        Some(orphaned_binding.clone())
    );
    assert_eq!(
        load_surface_bindings_by_logical_name_id_including_noncanonical(
            database.pool(),
            "ens:history.eth",
        )
        .await?,
        vec![orphaned_binding.clone()]
    );
    assert_eq!(
        load_surface_bindings_by_resource_id_including_noncanonical(database.pool(), resource_id,)
            .await?,
        vec![orphaned_binding]
    );

    database.cleanup().await
}
