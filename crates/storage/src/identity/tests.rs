use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde_json::json;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::types::time::OffsetDateTime;
use tokio::time::{sleep, timeout};
use uuid::Uuid;

use super::{
    IdentityOrphanCounts, NameSurface, Resource, SurfaceBinding, SurfaceBindingKind, TokenLineage,
    load_name_surface, load_name_surface_including_noncanonical,
    load_name_surfaces_by_logical_name_ids, load_resource, load_resource_including_noncanonical,
    load_surface_binding, load_surface_binding_including_noncanonical,
    load_surface_bindings_by_logical_name_id,
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

async fn identity_count(pool: &PgPool, address: &str, roles: &str) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT total_count
        FROM address_names_current_identity_counts
        WHERE address = $1 AND roles = $2
        "#,
    )
    .bind(address)
    .bind(roles)
    .fetch_optional(pool)
    .await
    .map(|count| count.unwrap_or_default())
    .context("failed to load address_names_current identity count")
}

async fn identity_count_updated_at(
    pool: &PgPool,
    address: &str,
    roles: &str,
) -> Result<OffsetDateTime> {
    sqlx::query_scalar::<_, OffsetDateTime>(
        r#"
        SELECT updated_at
        FROM address_names_current_identity_counts
        WHERE address = $1 AND roles = $2
        "#,
    )
    .bind(address)
    .bind(roles)
    .fetch_one(pool)
    .await
    .context("failed to load address_names_current identity count updated_at")
}

async fn identity_feed_count(pool: &PgPool, address: &str, roles: &str) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM address_names_current_identity_feed
        WHERE address = $1 AND roles = $2
        "#,
    )
    .bind(address)
    .bind(roles)
    .fetch_one(pool)
    .await
    .context("failed to count address_names_current identity feed rows")
}

async fn identity_feed_recomputed_at(
    pool: &PgPool,
    address: &str,
    roles: &str,
) -> Result<OffsetDateTime> {
    sqlx::query_scalar::<_, OffsetDateTime>(
        r#"
        SELECT last_recomputed_at
        FROM address_names_current_identity_feed
        WHERE address = $1 AND roles = $2 AND coin_type = ''
        "#,
    )
    .bind(address)
    .bind(roles)
    .fetch_one(pool)
    .await
    .context("failed to load address_names_current identity feed last_recomputed_at")
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
async fn name_surface_batch_load_matches_per_row_lookup_for_duplicates_and_missing_ids()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let alpha = name_surface(
        "ens:alpha.eth",
        "alpha.eth",
        "alpha.eth",
        "surface_batch_alpha",
        105,
        CanonicalityState::Finalized,
    );
    let beta = name_surface(
        "ens:beta.eth",
        "beta.eth",
        "beta.eth",
        "surface_batch_beta",
        106,
        CanonicalityState::Safe,
    );
    let observed = name_surface(
        "ens:observed.eth",
        "observed.eth",
        "observed.eth",
        "surface_batch_observed",
        107,
        CanonicalityState::Observed,
    );

    upsert_name_surfaces(
        database.pool(),
        &[alpha.clone(), beta.clone(), observed.clone()],
    )
    .await?;

    let requested_ids = vec![
        "ens:beta.eth".to_owned(),
        "ens:missing.eth".to_owned(),
        "ens:alpha.eth".to_owned(),
        "ens:beta.eth".to_owned(),
        "ens:observed.eth".to_owned(),
    ];
    let expected = futures_util::future::try_join_all(
        requested_ids
            .iter()
            .map(|logical_name_id| load_name_surface(database.pool(), logical_name_id)),
    )
    .await?;

    let loaded = load_name_surfaces_by_logical_name_ids(database.pool(), &requested_ids).await?;
    let actual = requested_ids
        .iter()
        .map(|logical_name_id| loaded.get(logical_name_id).cloned())
        .collect::<Vec<_>>();

    assert_eq!(actual, expected);
    assert_eq!(actual[0], Some(beta.clone()));
    assert_eq!(actual[1], None);
    assert_eq!(actual[2], Some(alpha));
    assert_eq!(actual[3], Some(beta));
    assert_eq!(actual[4], None);

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

#[tokio::test]
async fn row_path_resource_upsert_preserves_concurrent_orphan_update() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x2014);
    let initial_resource = resource(
        resource_id,
        None,
        "ens",
        "resource_row_path_orphan",
        121,
        CanonicalityState::Canonical,
    );
    upsert_resources(database.pool(), std::slice::from_ref(&initial_resource)).await?;

    let mut replayed_resource = initial_resource.clone();
    replayed_resource.canonicality_state = CanonicalityState::Finalized;
    let hook =
        super::write_rows::test_hooks::RowPathReloadHook::new("resources", resource_id.to_string());
    super::write_rows::test_hooks::install_row_path_reload_hook(hook.clone());

    let upsert_pool = database.pool().clone();
    let upsert_task = tokio::spawn(async move {
        upsert_resources(&upsert_pool, std::slice::from_ref(&replayed_resource)).await
    });

    timeout(std::time::Duration::from_secs(5), hook.wait_until_reached())
        .await
        .context("timed out waiting for resource row-path reload hook")?;

    let orphan_pool = database.pool().clone();
    let orphan_task = tokio::spawn(async move {
        sqlx::query(
            r#"
            UPDATE resources
            SET canonicality_state = 'orphaned'::canonicality_state
            WHERE resource_id = $1
            "#,
        )
        .bind(resource_id)
        .execute(&orphan_pool)
        .await
        .context("failed to orphan resource concurrently")
        .map(|_| ())
    });

    let _ = timeout(std::time::Duration::from_millis(250), async {
        while !orphan_task.is_finished() {
            sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;

    hook.release();
    upsert_task
        .await
        .context("resource row-path upsert task panicked")??;
    orphan_task
        .await
        .context("concurrent resource orphan task panicked")??;

    let final_resource = load_resource_including_noncanonical(database.pool(), resource_id)
        .await?
        .context("resource should still exist after orphaning")?;
    assert_eq!(
        final_resource.canonicality_state,
        CanonicalityState::Orphaned
    );

    database.cleanup().await
}

#[tokio::test]
async fn row_path_resource_batch_retries_deadlock() -> Result<()> {
    let database = TestDatabase::new().await?;
    let first_resource_id = Uuid::from_u128(0x2016);
    let second_resource_id = Uuid::from_u128(0x2017);
    let first_resource = resource(
        first_resource_id,
        None,
        "ens",
        "resource_deadlock_first",
        130,
        CanonicalityState::Canonical,
    );
    let second_resource = resource(
        second_resource_id,
        None,
        "ens",
        "resource_deadlock_second",
        131,
        CanonicalityState::Canonical,
    );
    upsert_resources(
        database.pool(),
        &[first_resource.clone(), second_resource.clone()],
    )
    .await?;

    let mut first_replay = first_resource.clone();
    first_replay.canonicality_state = CanonicalityState::Finalized;
    let mut second_replay = second_resource.clone();
    second_replay.canonicality_state = CanonicalityState::Finalized;

    let first_hook = super::write_rows::test_hooks::RowPathReloadHook::new(
        "resources",
        first_resource_id.to_string(),
    );
    let second_hook = super::write_rows::test_hooks::RowPathReloadHook::new(
        "resources",
        second_resource_id.to_string(),
    );
    super::write_rows::test_hooks::install_row_path_reload_hook(first_hook.clone());
    super::write_rows::test_hooks::install_row_path_reload_hook(second_hook.clone());

    let first_pool = database.pool().clone();
    let first_task = tokio::spawn({
        let first_replay = first_replay.clone();
        let second_replay = second_replay.clone();
        async move { upsert_resources(&first_pool, &[first_replay, second_replay]).await }
    });

    let second_pool = database.pool().clone();
    let second_task = tokio::spawn({
        let first_replay = first_replay.clone();
        let second_replay = second_replay.clone();
        async move { upsert_resources(&second_pool, &[second_replay, first_replay]).await }
    });

    timeout(
        std::time::Duration::from_secs(5),
        first_hook.wait_until_reached(),
    )
    .await
    .context("timed out waiting for first resource row-path reload hook")?;
    timeout(
        std::time::Duration::from_secs(5),
        second_hook.wait_until_reached(),
    )
    .await
    .context("timed out waiting for second resource row-path reload hook")?;

    first_hook.release();
    second_hook.release();

    first_task
        .await
        .context("first resource deadlock retry task panicked")??;
    second_task
        .await
        .context("second resource deadlock retry task panicked")??;

    assert_eq!(
        load_resource(database.pool(), first_resource_id)
            .await?
            .expect("first resource must remain readable")
            .canonicality_state,
        CanonicalityState::Finalized
    );
    assert_eq!(
        load_resource(database.pool(), second_resource_id)
            .await?
            .expect("second resource must remain readable")
            .canonicality_state,
        CanonicalityState::Finalized
    );

    database.cleanup().await
}

#[tokio::test]
async fn row_path_surface_binding_upsert_preserves_concurrent_tighten() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x2015);
    let surface_binding_id = Uuid::from_u128(0x3015);
    let earlier_close = timestamp(1_717_171_900);
    let later_close = timestamp(1_717_172_100);

    upsert_resources(
        database.pool(),
        &[resource(
            resource_id,
            None,
            "ens",
            "resource_row_path_tighten",
            122,
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[name_surface(
            "ens:row-path-tighten.eth",
            "row-path-tighten.eth",
            "row-path-tighten.eth",
            "surface_row_path_tighten",
            123,
            CanonicalityState::Finalized,
        )],
    )
    .await?;

    let open_binding = binding(BindingSeed {
        surface_binding_id,
        logical_name_id: "ens:row-path-tighten.eth",
        resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: timestamp(1_717_171_700),
        active_to: None,
        source: "declared_registry_path",
        chain_label: "binding_row_path_tighten",
        block_number: 124,
        canonicality_state: CanonicalityState::Canonical,
    });
    upsert_surface_bindings(database.pool(), std::slice::from_ref(&open_binding)).await?;

    let later_close_binding = SurfaceBinding {
        active_to: Some(later_close),
        ..open_binding
    };
    let hook = super::write_rows::test_hooks::RowPathReloadHook::new(
        "surface_bindings",
        surface_binding_id.to_string(),
    );
    super::write_rows::test_hooks::install_row_path_reload_hook(hook.clone());

    let upsert_pool = database.pool().clone();
    let upsert_task = tokio::spawn(async move {
        upsert_surface_bindings(&upsert_pool, std::slice::from_ref(&later_close_binding)).await
    });

    timeout(std::time::Duration::from_secs(5), hook.wait_until_reached())
        .await
        .context("timed out waiting for surface-binding row-path reload hook")?;

    let tighten_pool = database.pool().clone();
    let tighten_task = tokio::spawn(async move {
        sqlx::query(
            r#"
            UPDATE surface_bindings
            SET active_to = $2
            WHERE surface_binding_id = $1
            "#,
        )
        .bind(surface_binding_id)
        .bind(earlier_close)
        .execute(&tighten_pool)
        .await
        .context("failed to tighten surface binding concurrently")
        .map(|_| ())
    });

    let _ = timeout(std::time::Duration::from_millis(250), async {
        while !tighten_task.is_finished() {
            sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;

    hook.release();
    upsert_task
        .await
        .context("surface-binding row-path upsert task panicked")??;
    tighten_task
        .await
        .context("concurrent surface-binding tighten task panicked")??;

    let final_binding = load_surface_binding(database.pool(), surface_binding_id)
        .await?
        .context("surface binding should remain readable")?;
    assert_eq!(final_binding.active_to, Some(earlier_close));

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
async fn no_snapshot_token_lineage_upsert_skips_idempotent_rewrite() -> Result<()> {
    let database = TestDatabase::new().await?;
    let token_lineage_id = Uuid::from_u128(0xf103);
    let anchored_observed_at = timestamp(946_684_800);
    let lineage = token_lineage(
        token_lineage_id,
        "ens",
        "token_no_snapshot_idempotent",
        111,
        CanonicalityState::Canonical,
    );
    upsert_token_lineages_without_snapshots(database.pool(), std::slice::from_ref(&lineage))
        .await?;

    sqlx::query("UPDATE token_lineages SET observed_at = $1 WHERE token_lineage_id = $2")
        .bind(anchored_observed_at)
        .bind(token_lineage_id)
        .execute(database.pool())
        .await
        .context("failed to anchor token lineage observed_at")?;

    upsert_token_lineages_without_snapshots(database.pool(), std::slice::from_ref(&lineage))
        .await?;
    assert_eq!(
        load_token_lineage(database.pool(), token_lineage_id).await?,
        Some(lineage)
    );
    assert_eq!(
        load_token_lineage_observed_at(database.pool(), token_lineage_id).await?,
        anchored_observed_at
    );

    database.cleanup().await
}

#[tokio::test]
async fn no_snapshot_resource_upsert_skips_idempotent_rewrite() -> Result<()> {
    let database = TestDatabase::new().await?;
    let token_lineage_id = Uuid::from_u128(0xf104);
    let resource_id = Uuid::from_u128(0xf105);
    let anchored_observed_at = timestamp(946_684_800);

    upsert_token_lineages(
        database.pool(),
        &[token_lineage(
            token_lineage_id,
            "ens",
            "token_resource_no_snapshot_idempotent",
            111,
            CanonicalityState::Canonical,
        )],
    )
    .await?;

    let resource = resource(
        resource_id,
        Some(token_lineage_id),
        "ens",
        "resource_no_snapshot_idempotent",
        112,
        CanonicalityState::Canonical,
    );
    upsert_resources_without_snapshots(database.pool(), std::slice::from_ref(&resource)).await?;

    sqlx::query("UPDATE resources SET observed_at = $1 WHERE resource_id = $2")
        .bind(anchored_observed_at)
        .bind(resource_id)
        .execute(database.pool())
        .await
        .context("failed to anchor resource observed_at")?;

    upsert_resources_without_snapshots(database.pool(), std::slice::from_ref(&resource)).await?;
    assert_eq!(
        load_resource(database.pool(), resource_id).await?,
        Some(resource)
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
async fn no_snapshot_name_surface_upsert_repairs_stale_normalized_hash_path() -> Result<()> {
    let database = TestDatabase::new().await?;
    let logical_name_id = "ens:missioncontrol.2718.eth";
    let dns_encoded_name = vec![
        14, b'm', b'i', b's', b's', b'i', b'o', b'n', b'c', b'o', b'n', b't', b'r', b'o', b'l', 4,
        b'2', b'7', b'1', b'8', 3, b'e', b't', b'h', 0,
    ];
    let stale_namehash = "0x0bead95ae242ed57428f81738348205d9a8210ad066c01c3ea223626a2f99061";
    let repaired_namehash = "0x582f93f5b5d7aedd8945d00e042fcc92078dca21d5c1deb34f956b3672440b6e";
    let stale_labelhash = "0xf16eb6748d7b00704e9b7e5faa8f33e1036467f83e5d718aab952a9f82acc74f";
    let repaired_labelhash = "0xe141a2d34371b17fa7034fa2acf312fbb9cf042435cb81a0b9f962e959aae280";
    let parent_labelhash = "0xb73b6a51b3e5f8c36d7b7757496ee3eb11f6300d981e965cb578604ff8de772c";
    let tld_labelhash = "0x4f5b812789fc606be1b3b16908db13fc7a9adf7ca72641f84d75b47069d3d7f0";
    let provenance = json!({
        "adapter": "ens_v1_unwrapped_authority",
        "logical_name_id": logical_name_id,
        "source_event": "registrar_name_observation",
    });

    let mut stale_surface = name_surface(
        logical_name_id,
        "MissionControl.2718.eth",
        "missioncontrol.2718.eth",
        "surface_stale_hash_path",
        18178513,
        CanonicalityState::Finalized,
    );
    stale_surface.canonical_display_name = "missioncontrol.2718.eth".to_owned();
    stale_surface.dns_encoded_name = dns_encoded_name.clone();
    stale_surface.namehash = stale_namehash.to_owned();
    stale_surface.labelhashes = vec![
        stale_labelhash.to_owned(),
        parent_labelhash.to_owned(),
        tld_labelhash.to_owned(),
    ];
    stale_surface.provenance = provenance.clone();
    upsert_name_surfaces_without_snapshots(database.pool(), std::slice::from_ref(&stale_surface))
        .await?;

    let mut repaired_surface = stale_surface.clone();
    repaired_surface.input_name = "missioncontrol.2718.eth".to_owned();
    repaired_surface.namehash = repaired_namehash.to_owned();
    repaired_surface.labelhashes = vec![
        repaired_labelhash.to_owned(),
        parent_labelhash.to_owned(),
        tld_labelhash.to_owned(),
    ];
    repaired_surface.chain_id = "chain:surface_repaired_hash_path".to_owned();
    repaired_surface.block_hash = "0xsurface_repaired_hash_path_011585bf".to_owned();
    repaired_surface.block_number = 18182975;
    upsert_name_surfaces_without_snapshots(
        database.pool(),
        std::slice::from_ref(&repaired_surface),
    )
    .await?;

    let mut expected_surface = stale_surface;
    expected_surface.namehash = repaired_namehash.to_owned();
    expected_surface.labelhashes = vec![
        repaired_labelhash.to_owned(),
        parent_labelhash.to_owned(),
        tld_labelhash.to_owned(),
    ];
    assert_eq!(
        load_name_surface(database.pool(), logical_name_id).await?,
        Some(expected_surface)
    );

    database.cleanup().await
}

#[tokio::test]
async fn no_snapshot_name_surface_upsert_repairs_stale_dotted_registrar_surface() -> Result<()> {
    let database = TestDatabase::new().await?;
    let logical_name_id = "ens:3.1415.eth";
    let stale_dns_encoded_name = vec![
        6, b'3', b'.', b'1', b'4', b'1', b'5', 3, b'e', b't', b'h', 0,
    ];
    let repaired_dns_encoded_name =
        vec![1, b'3', 4, b'1', b'4', b'1', b'5', 3, b'e', b't', b'h', 0];
    let stale_namehash = "0x5f17755d1125039bb4039a841dba980dfc8096cb2a65a14655a10b8e123e5a75";
    let repaired_namehash = "0xa76a2d53db8c3cf609227f6d8dfe35ef54d9582e83a3f7ef63171705ba2cdfba";
    let stale_labelhash = "0x6b09de763b2c9b88e890b228c7f52ea055de1f0414272b7e4ed068113e2e7f76";
    let repaired_left_labelhash =
        "0x2a80e1ef1d7842f27f2e6be0972bb708b9a135c38860dbe73c27c3486c34f4de";
    let repaired_parent_labelhash =
        "0xde9c9651fee49e4c6fdbfdbe4bbabdb101af3264b63962d1b7e6fddd4168ac3c";
    let eth_labelhash = "0x4f5b812789fc606be1b3b16908db13fc7a9adf7ca72641f84d75b47069d3d7f0";
    let provenance = json!({
        "adapter": "ens_v1_unwrapped_authority",
        "logical_name_id": logical_name_id,
        "source_event": "registrar_name_observation",
    });

    let mut stale_surface = name_surface(
        logical_name_id,
        "3.1415.eth",
        "3.1415.eth",
        "surface_stale_dotted_registrar",
        15524287,
        CanonicalityState::Finalized,
    );
    stale_surface.dns_encoded_name = stale_dns_encoded_name;
    stale_surface.namehash = stale_namehash.to_owned();
    stale_surface.labelhashes = vec![stale_labelhash.to_owned(), eth_labelhash.to_owned()];
    stale_surface.provenance = provenance.clone();
    upsert_name_surfaces_without_snapshots(database.pool(), std::slice::from_ref(&stale_surface))
        .await?;

    let mut repaired_surface = stale_surface.clone();
    repaired_surface.dns_encoded_name = repaired_dns_encoded_name.clone();
    repaired_surface.namehash = repaired_namehash.to_owned();
    repaired_surface.labelhashes = vec![
        repaired_left_labelhash.to_owned(),
        repaired_parent_labelhash.to_owned(),
        eth_labelhash.to_owned(),
    ];
    repaired_surface.chain_id = "chain:surface_repaired_dotted_registrar".to_owned();
    repaired_surface.block_hash = "0xsurface_repaired_dotted_registrar_013bdc25".to_owned();
    repaired_surface.block_number = 20696837;
    upsert_name_surfaces_without_snapshots(
        database.pool(),
        std::slice::from_ref(&repaired_surface),
    )
    .await?;

    let mut expected_surface = stale_surface;
    expected_surface.dns_encoded_name = repaired_dns_encoded_name;
    expected_surface.namehash = repaired_namehash.to_owned();
    expected_surface.labelhashes = vec![
        repaired_left_labelhash.to_owned(),
        repaired_parent_labelhash.to_owned(),
        eth_labelhash.to_owned(),
    ];
    assert_eq!(
        load_name_surface(database.pool(), logical_name_id).await?,
        Some(expected_surface)
    );

    database.cleanup().await
}

#[tokio::test]
async fn no_snapshot_name_surface_upsert_skips_idempotent_rewrite() -> Result<()> {
    let database = TestDatabase::new().await?;
    let logical_name_id = "ens:no-snapshot-surface-idempotent.eth";
    let anchored_observed_at = timestamp(946_684_800);
    let surface = name_surface(
        logical_name_id,
        "no-snapshot-surface-idempotent.eth",
        "no-snapshot-surface-idempotent.eth",
        "surface_no_snapshot_idempotent",
        113,
        CanonicalityState::Canonical,
    );
    upsert_name_surfaces_without_snapshots(database.pool(), std::slice::from_ref(&surface)).await?;

    sqlx::query("UPDATE name_surfaces SET observed_at = $1 WHERE logical_name_id = $2")
        .bind(anchored_observed_at)
        .bind(logical_name_id)
        .execute(database.pool())
        .await
        .context("failed to anchor name surface observed_at")?;

    upsert_name_surfaces_without_snapshots(database.pool(), std::slice::from_ref(&surface)).await?;
    assert_eq!(
        load_name_surface(database.pool(), logical_name_id).await?,
        Some(surface)
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

#[tokio::test]
async fn identity_facade_statement_triggers_replace_row_level_feed_triggers() -> Result<()> {
    let database = TestDatabase::new().await?;

    let trigger_names = sqlx::query_scalar::<_, String>(
        r#"
        SELECT tgname
        FROM pg_trigger
        WHERE NOT tgisinternal
          AND tgrelid IN (
              'public.name_surfaces'::regclass,
              'public.resources'::regclass,
              'public.surface_bindings'::regclass,
              'public.token_lineages'::regclass
          )
          AND tgname IN (
              'address_names_current_identity_counts_binding_readability_updat',
              'address_names_current_identity_counts_binding_readability_update',
              'name_surfaces_identity_feed_after_change',
              'resources_identity_feed_after_change',
              'surface_bindings_identity_feed_after_change',
              'token_lineages_identity_feed_after_change'
          )
        ORDER BY tgname
        "#,
    )
    .fetch_all(database.pool())
    .await
    .context("failed to inspect identity facade feed triggers")?;
    assert!(
        trigger_names.is_empty(),
        "row-level identity feed triggers should be replaced by statement triggers, found {trigger_names:?}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn identity_feed_row_triggers_gate_metadata_only_updates() -> Result<()> {
    let database = TestDatabase::new().await?;

    let old_trigger_names = sqlx::query_scalar::<_, String>(
        r#"
        SELECT tgname
        FROM pg_trigger
        WHERE NOT tgisinternal
          AND tgrelid IN (
              'public.address_names_current'::regclass,
              'public.primary_names_current'::regclass
          )
          AND tgname IN (
              'address_names_current_identity_feed_after_change',
              'primary_names_current_identity_feed_after_change'
          )
        ORDER BY tgname
        "#,
    )
    .fetch_all(database.pool())
    .await
    .context("failed to inspect legacy identity feed row triggers")?;
    assert!(
        old_trigger_names.is_empty(),
        "broad identity feed row triggers must be replaced by column-gated triggers, found {old_trigger_names:?}"
    );

    let trigger_defs = sqlx::query_scalar::<_, String>(
        r#"
        SELECT pg_get_triggerdef(oid)
        FROM pg_trigger
        WHERE NOT tgisinternal
          AND tgname IN (
              'address_names_current_identity_feed_after_anchor_update',
              'primary_names_current_identity_feed_after_claim_update'
          )
        ORDER BY tgname
        "#,
    )
    .fetch_all(database.pool())
    .await
    .context("failed to inspect gated identity feed row triggers")?;

    assert_eq!(
        trigger_defs.len(),
        2,
        "address and primary-name feed update triggers must both be present"
    );
    assert!(
        trigger_defs
            .iter()
            .all(|definition| definition.contains("UPDATE OF ")),
        "identity feed update triggers must be UPDATE OF column-gated: {trigger_defs:?}"
    );
    assert!(
        trigger_defs
            .iter()
            .all(|definition| definition.contains("WHEN")),
        "identity feed update triggers must include value-change WHEN guards: {trigger_defs:?}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn identity_feed_row_trigger_recomputes_only_for_anchor_updates() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000f33";
    let logical_name_id = "ens:feed-row-trigger.eth";
    let resource_id = Uuid::from_u128(0xf031);
    let surface_binding_id = Uuid::from_u128(0xf032);

    upsert_name_surfaces(
        database.pool(),
        &[name_surface(
            logical_name_id,
            "feed-row-trigger.eth",
            "feed-row-trigger.eth",
            "feed_row_trigger_surface",
            830,
            CanonicalityState::Finalized,
        )],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[resource(
            resource_id,
            None,
            "ens",
            "feed_row_trigger_resource",
            831,
            CanonicalityState::Finalized,
        )],
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &[binding(BindingSeed {
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from: timestamp(1_717_173_300),
            active_to: None,
            source: "feed_row_trigger_relation",
            chain_label: "feed_row_trigger_binding",
            block_number: 832,
            canonicality_state: CanonicalityState::Finalized,
        })],
    )
    .await?;

    sqlx::query(
        r#"
        INSERT INTO name_current (
            logical_name_id,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            declared_summary,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version
        )
        VALUES (
            $1,
            'ens',
            'feed-row-trigger.eth',
            'feed-row-trigger.eth',
            'namehash:feed-row-trigger.eth',
            $2,
            $3,
            NULL,
            'declared_registry_path',
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            1
        )
        "#,
    )
    .bind(logical_name_id)
    .bind(surface_binding_id)
    .bind(resource_id)
    .execute(database.pool())
    .await
    .context("failed to seed name_current for identity feed row trigger test")?;
    sqlx::query(
        r#"
        INSERT INTO address_names_current (
            address,
            logical_name_id,
            relation,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version
        )
        VALUES (
            $1,
            $2,
            'registrant',
            'ens',
            'feed-row-trigger.eth',
            'feed-row-trigger.eth',
            'namehash:feed-row-trigger.eth',
            $3,
            $4,
            NULL,
            'declared_registry_path',
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            1
        )
        "#,
    )
    .bind(address)
    .bind(logical_name_id)
    .bind(surface_binding_id)
    .bind(resource_id)
    .execute(database.pool())
    .await
    .context("failed to seed address_names_current for identity feed row trigger test")?;

    sqlx::query("SELECT public.address_names_current_identity_feed_recompute_address($1)")
        .bind(address)
        .execute(database.pool())
        .await
        .context("failed to seed identity feed row trigger sidecar")?;
    assert_eq!(
        identity_feed_count(database.pool(), address, "owned").await?,
        1
    );
    let initial_recomputed_at =
        identity_feed_recomputed_at(database.pool(), address, "owned").await?;

    sleep(std::time::Duration::from_millis(10)).await;
    sqlx::query(
        r#"
        UPDATE address_names_current
        SET
            canonicality_summary = '{"status":"finalized"}'::jsonb,
            last_recomputed_at = $2
        WHERE address = $1
        "#,
    )
    .bind(address)
    .bind(timestamp(1_817_173_300))
    .execute(database.pool())
    .await
    .context("failed to update address_names_current metadata-only fields")?;

    assert_eq!(
        identity_feed_recomputed_at(database.pool(), address, "owned").await?,
        initial_recomputed_at,
        "metadata-only updates must not fire the identity feed row trigger"
    );

    sleep(std::time::Duration::from_millis(10)).await;
    sqlx::query(
        r#"
        UPDATE address_names_current
        SET canonical_display_name = 'feed-row-trigger-renamed.eth'
        WHERE address = $1
        "#,
    )
    .bind(address)
    .execute(database.pool())
    .await
    .context("failed to update address_names_current anchor field")?;

    let anchor_recomputed_at =
        identity_feed_recomputed_at(database.pool(), address, "owned").await?;
    assert!(
        anchor_recomputed_at > initial_recomputed_at,
        "anchor-column updates must fire the identity feed row trigger"
    );
    let feed_display_name = sqlx::query_scalar::<_, String>(
        r#"
        SELECT canonical_display_name
        FROM address_names_current_identity_feed
        WHERE address = $1 AND roles = 'owned' AND coin_type = ''
        "#,
    )
    .bind(address)
    .fetch_one(database.pool())
    .await
    .context("failed to load identity feed display name after anchor update")?;
    assert_eq!(feed_display_name, "feed-row-trigger-renamed.eth");

    database.cleanup().await
}

#[tokio::test]
async fn identity_count_statement_triggers_recompute_name_current_resource_eligibility()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let logical_name_id = "ens:count-trigger.eth";
    let relation_resource_id = Uuid::from_u128(0xf001);
    let name_current_resource_id = Uuid::from_u128(0xf002);
    let surface_binding_id = Uuid::from_u128(0xf003);

    upsert_name_surfaces(
        database.pool(),
        &[name_surface(
            logical_name_id,
            "count-trigger.eth",
            "count-trigger.eth",
            "count_trigger_surface",
            700,
            CanonicalityState::Finalized,
        )],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[
            resource(
                relation_resource_id,
                None,
                "ens",
                "count_trigger_relation_resource",
                701,
                CanonicalityState::Finalized,
            ),
            resource(
                name_current_resource_id,
                None,
                "ens",
                "count_trigger_name_resource",
                702,
                CanonicalityState::Finalized,
            ),
        ],
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &[binding(BindingSeed {
            surface_binding_id,
            logical_name_id,
            resource_id: relation_resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from: timestamp(1_717_172_900),
            active_to: None,
            source: "count_trigger_relation",
            chain_label: "count_trigger_binding",
            block_number: 703,
            canonicality_state: CanonicalityState::Finalized,
        })],
    )
    .await?;

    sqlx::query(
        r#"
        INSERT INTO name_current (
            logical_name_id,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            declared_summary,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version
        )
        VALUES (
            $1,
            'ens',
            'count-trigger.eth',
            'count-trigger.eth',
            'namehash:count-trigger.eth',
            $2,
            $3,
            NULL,
            'declared_registry_path',
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            1
        )
        "#,
    )
    .bind(logical_name_id)
    .bind(surface_binding_id)
    .bind(name_current_resource_id)
    .execute(database.pool())
    .await
    .context("failed to seed name_current for identity count trigger test")?;
    sqlx::query(
        r#"
        INSERT INTO address_names_current (
            address,
            logical_name_id,
            relation,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version
        )
        VALUES (
            $1,
            $2,
            'registrant',
            'ens',
            'count-trigger.eth',
            'count-trigger.eth',
            'namehash:count-trigger.eth',
            $3,
            $4,
            NULL,
            'declared_registry_path',
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            1
        )
        "#,
    )
    .bind(address)
    .bind(logical_name_id)
    .bind(surface_binding_id)
    .bind(relation_resource_id)
    .execute(database.pool())
    .await
    .context("failed to seed address_names_current for identity count trigger test")?;

    sqlx::query("SELECT public.address_names_current_identity_counts_recompute_address($1)")
        .bind(address)
        .execute(database.pool())
        .await
        .context("failed to seed identity counts")?;
    assert_eq!(identity_count(database.pool(), address, "owned").await?, 1);
    let count_updated_at = identity_count_updated_at(database.pool(), address, "owned").await?;

    sleep(std::time::Duration::from_millis(10)).await;
    sqlx::query("UPDATE resources SET canonicality_state = 'safe' WHERE resource_id = $1")
        .bind(name_current_resource_id)
        .execute(database.pool())
        .await
        .context("failed to promote name_current resource inside readable class")?;

    assert_eq!(identity_count(database.pool(), address, "owned").await?, 1);
    assert_eq!(
        identity_count_updated_at(database.pool(), address, "owned").await?,
        count_updated_at
    );

    sqlx::query("UPDATE resources SET canonicality_state = 'orphaned' WHERE resource_id = $1")
        .bind(name_current_resource_id)
        .execute(database.pool())
        .await
        .context("failed to orphan name_current resource")?;

    assert_eq!(identity_count(database.pool(), address, "owned").await?, 0);

    database.cleanup().await
}

#[tokio::test]
async fn identity_count_resource_observed_to_orphaned_does_not_recompute() -> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000ab0";
    let logical_name_id = "ens:observed-gate.eth";
    let resource_id = Uuid::from_u128(0xf051);
    let surface_binding_id = Uuid::from_u128(0xf052);
    let sentinel_updated_at = timestamp(1_717_174_000);

    upsert_name_surfaces(
        database.pool(),
        &[name_surface(
            logical_name_id,
            "observed-gate.eth",
            "observed-gate.eth",
            "observed_gate_surface",
            720,
            CanonicalityState::Finalized,
        )],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[resource(
            resource_id,
            None,
            "ens",
            "observed_gate_resource",
            721,
            CanonicalityState::Observed,
        )],
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &[binding(BindingSeed {
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from: timestamp(1_717_173_900),
            active_to: None,
            source: "observed_gate_relation",
            chain_label: "observed_gate_binding",
            block_number: 722,
            canonicality_state: CanonicalityState::Finalized,
        })],
    )
    .await?;

    sqlx::query(
        r#"
        INSERT INTO name_current (
            logical_name_id,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            declared_summary,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version
        )
        VALUES (
            $1,
            'ens',
            'observed-gate.eth',
            'observed-gate.eth',
            'namehash:observed-gate.eth',
            $2,
            $3,
            NULL,
            'declared_registry_path',
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            1
        )
        "#,
    )
    .bind(logical_name_id)
    .bind(surface_binding_id)
    .bind(resource_id)
    .execute(database.pool())
    .await
    .context("failed to seed name_current for observed gate test")?;
    sqlx::query(
        r#"
        INSERT INTO address_names_current (
            address,
            logical_name_id,
            relation,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version
        )
        VALUES (
            $1,
            $2,
            'registrant',
            'ens',
            'observed-gate.eth',
            'observed-gate.eth',
            'namehash:observed-gate.eth',
            $3,
            $4,
            NULL,
            'declared_registry_path',
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            1
        )
        "#,
    )
    .bind(address)
    .bind(logical_name_id)
    .bind(surface_binding_id)
    .bind(resource_id)
    .execute(database.pool())
    .await
    .context("failed to seed address_names_current for observed gate test")?;

    sqlx::query(
        r#"
        INSERT INTO address_names_current_identity_counts (
            address,
            roles,
            total_count,
            updated_at
        )
        VALUES ($1, 'owned', 0, $2)
        "#,
    )
    .bind(address)
    .bind(sentinel_updated_at)
    .execute(database.pool())
    .await
    .context("failed to seed identity count sentinel")?;

    sqlx::query("UPDATE resources SET canonicality_state = 'orphaned' WHERE resource_id = $1")
        .bind(resource_id)
        .execute(database.pool())
        .await
        .context("failed to orphan observed resource")?;

    assert_eq!(identity_count(database.pool(), address, "owned").await?, 0);
    assert_eq!(
        identity_count_updated_at(database.pool(), address, "owned").await?,
        sentinel_updated_at,
        "observed-to-orphaned resource updates must not fire the identity count recompute trigger"
    );

    database.cleanup().await
}

#[tokio::test]
async fn identity_feed_statement_triggers_recompute_name_current_resource_eligibility() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000fed";
    let logical_name_id = "ens:feed-trigger.eth";
    let relation_resource_id = Uuid::from_u128(0xf101);
    let name_current_resource_id = Uuid::from_u128(0xf102);
    let surface_binding_id = Uuid::from_u128(0xf103);

    upsert_name_surfaces(
        database.pool(),
        &[name_surface(
            logical_name_id,
            "feed-trigger.eth",
            "feed-trigger.eth",
            "feed_trigger_surface",
            800,
            CanonicalityState::Finalized,
        )],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[
            resource(
                relation_resource_id,
                None,
                "ens",
                "feed_trigger_relation_resource",
                801,
                CanonicalityState::Finalized,
            ),
            resource(
                name_current_resource_id,
                None,
                "ens",
                "feed_trigger_name_resource",
                802,
                CanonicalityState::Finalized,
            ),
        ],
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &[binding(BindingSeed {
            surface_binding_id,
            logical_name_id,
            resource_id: relation_resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from: timestamp(1_717_173_100),
            active_to: None,
            source: "feed_trigger_relation",
            chain_label: "feed_trigger_binding",
            block_number: 803,
            canonicality_state: CanonicalityState::Finalized,
        })],
    )
    .await?;

    sqlx::query(
        r#"
        INSERT INTO name_current (
            logical_name_id,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            declared_summary,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version
        )
        VALUES (
            $1,
            'ens',
            'feed-trigger.eth',
            'feed-trigger.eth',
            'namehash:feed-trigger.eth',
            $2,
            $3,
            NULL,
            'declared_registry_path',
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            1
        )
        "#,
    )
    .bind(logical_name_id)
    .bind(surface_binding_id)
    .bind(name_current_resource_id)
    .execute(database.pool())
    .await
    .context("failed to seed name_current for identity feed trigger test")?;
    sqlx::query(
        r#"
        INSERT INTO address_names_current (
            address,
            logical_name_id,
            relation,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version
        )
        VALUES (
            $1,
            $2,
            'registrant',
            'ens',
            'feed-trigger.eth',
            'feed-trigger.eth',
            'namehash:feed-trigger.eth',
            $3,
            $4,
            NULL,
            'declared_registry_path',
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            1
        )
        "#,
    )
    .bind(address)
    .bind(logical_name_id)
    .bind(surface_binding_id)
    .bind(relation_resource_id)
    .execute(database.pool())
    .await
    .context("failed to seed address_names_current for identity feed trigger test")?;

    sqlx::query("SELECT public.address_names_current_identity_feed_recompute_address($1)")
        .bind(address)
        .execute(database.pool())
        .await
        .context("failed to seed identity feed")?;
    assert_eq!(
        identity_feed_count(database.pool(), address, "owned").await?,
        1
    );
    let feed_recomputed_at = identity_feed_recomputed_at(database.pool(), address, "owned").await?;

    sleep(std::time::Duration::from_millis(10)).await;
    sqlx::query("UPDATE resources SET canonicality_state = 'safe' WHERE resource_id = $1")
        .bind(name_current_resource_id)
        .execute(database.pool())
        .await
        .context("failed to promote name_current resource inside readable class")?;

    assert_eq!(
        identity_feed_count(database.pool(), address, "owned").await?,
        1
    );
    assert_eq!(
        identity_feed_recomputed_at(database.pool(), address, "owned").await?,
        feed_recomputed_at
    );

    sqlx::query("UPDATE resources SET canonicality_state = 'orphaned' WHERE resource_id = $1")
        .bind(name_current_resource_id)
        .execute(database.pool())
        .await
        .context("failed to orphan name_current resource")?;

    assert_eq!(
        identity_feed_count(database.pool(), address, "owned").await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn surface_binding_projection_invalidations_fire_on_readable_canonicality_change()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let logical_name_id = "ens:projection-trigger.eth";
    let resource_id = Uuid::from_u128(0xf201);
    let surface_binding_id = Uuid::from_u128(0xf202);

    upsert_name_surfaces(
        database.pool(),
        &[name_surface(
            logical_name_id,
            "projection-trigger.eth",
            "projection-trigger.eth",
            "projection_trigger_surface",
            900,
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[resource(
            resource_id,
            None,
            "ens",
            "projection_trigger_resource",
            901,
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &[binding(BindingSeed {
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from: timestamp(1_717_173_300),
            active_to: None,
            source: "projection_trigger_relation",
            chain_label: "projection_trigger_binding",
            block_number: 902,
            canonicality_state: CanonicalityState::Canonical,
        })],
    )
    .await?;

    sqlx::query(
        r#"
        INSERT INTO address_names_current (
            address,
            logical_name_id,
            relation,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version
        )
        VALUES (
            $1,
            $2,
            'registrant',
            'ens',
            'projection-trigger.eth',
            'projection-trigger.eth',
            'namehash:projection-trigger.eth',
            $3,
            $4,
            NULL,
            'declared_registry_path',
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            '{}'::jsonb,
            1
        )
        "#,
    )
    .bind(address)
    .bind(logical_name_id)
    .bind(surface_binding_id)
    .bind(resource_id)
    .execute(database.pool())
    .await
    .context("failed to seed address_names_current for projection invalidation test")?;

    sqlx::query("DELETE FROM projection_invalidations")
        .execute(database.pool())
        .await
        .context("failed to clear projection invalidation queue")?;

    sqlx::query(
        r#"
        UPDATE surface_bindings
        SET canonicality_state = 'safe'
        WHERE surface_binding_id = $1
        "#,
    )
    .bind(surface_binding_id)
    .execute(database.pool())
    .await
    .context("failed to promote surface binding inside readable class")?;

    let name_current_invalidations = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM projection_invalidations
        WHERE projection = 'name_current'
          AND projection_key = $1
        "#,
    )
    .bind(logical_name_id)
    .fetch_one(database.pool())
    .await
    .context("failed to count name_current projection invalidations")?;
    let address_names_current_invalidations = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM projection_invalidations
        WHERE projection = 'address_names_current'
          AND projection_key = $1
        "#,
    )
    .bind(format!("{}:{}", address, logical_name_id))
    .fetch_one(database.pool())
    .await
    .context("failed to count address_names_current projection invalidations")?;

    assert_eq!(name_current_invalidations, 1);
    assert_eq!(address_names_current_invalidations, 1);

    database.cleanup().await
}
