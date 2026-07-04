use super::*;
use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use bigname_storage::{
    NormalizedEvent, default_database_url, upsert_normalized_events,
    upsert_resources_without_snapshots, upsert_surface_bindings_without_snapshots,
};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

static NEXT_MATERIALIZATION_TEST_ID: AtomicU64 = AtomicU64::new(0);

struct MaterializationTestDatabase {
    admin_pool: PgPool,
    pool: PgPool,
    database_name: String,
}

impl MaterializationTestDatabase {
    async fn new() -> Result<Self> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for materialization tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_MATERIALIZATION_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!("bn_ad_mat_{}_{}_{}", std::process::id(), sequence, unique);

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for materialization tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect test pool for materialization tests")?;

        bigname_storage::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for materialization tests")?;

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
fn coalesce_name_surfaces_for_upsert_keeps_first_identity() {
    let first = test_surface(
        "ens:missioncontrol.2718.eth",
        "MissionControl.2718.eth",
        "0x1111111111111111111111111111111111111111111111111111111111111111",
    );
    let other = test_surface(
        "ens:other.eth",
        "other.eth",
        "0x2222222222222222222222222222222222222222222222222222222222222222",
    );
    let duplicate = test_surface(
        "ens:missioncontrol.2718.eth",
        "missioncontrol.2718.eth",
        "0x3333333333333333333333333333333333333333333333333333333333333333",
    );

    let mut surfaces = vec![first.clone(), other.clone(), duplicate];
    coalesce_name_surfaces_for_upsert(&mut surfaces);

    assert_eq!(surfaces, vec![first, other]);
}

#[test]
fn name_surface_anchor_can_use_authority_binding_when_name_ref_is_missing() {
    let name = NameMetadata {
        namespace: "basenames".to_owned(),
        logical_name_id: "basenames:brian.base.eth".to_owned(),
        input_name: "brian.base.eth".to_owned(),
        canonical_display_name: "brian.base.eth".to_owned(),
        normalized_name: "brian.base.eth".to_owned(),
        dns_encoded_name: vec![
            5, b'b', b'r', b'i', b'a', b'n', 4, b'b', b'a', b's', b'e', 3, b'e', b't', b'h', 0,
        ],
        namehash: "0x381d5e3c853a86585a94e46b9be8022406adcb6582fe946860603c97a8c6e7af".to_owned(),
        labelhashes: vec![
            "0x61d644f0d6ba62b3b81f46d2291409f93244a931b0cf2556aad50391f3b67fb2".to_owned(),
            "0xf1f3eb40f5bc1ad1344716ced8b8a0431d840b5783aea1fd01786bc26f35ac0f".to_owned(),
            "0x4f5b812789fc606be1b3b16908db13fc7a9adf7ca72641f84d75b47069d3d7f0".to_owned(),
        ],
        normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
    };

    let surface = name_surface_from_anchor(
        &name,
        "base-mainnet",
        "0xf895452624ab7f8fe47f729b9b9dc5090b776e089ebb465f1216419eea77e15a",
        19_432_695,
        CanonicalityState::Finalized,
        "authority_binding_known_name",
    );

    assert_eq!(surface.logical_name_id, name.logical_name_id);
    assert_eq!(surface.namespace, "basenames");
    assert_eq!(surface.chain_id, "base-mainnet");
    assert_eq!(surface.block_number, 19_432_695);
    assert_eq!(surface.canonicality_state, CanonicalityState::Finalized);
    assert_eq!(
        surface
            .provenance
            .get("source_event")
            .and_then(Value::as_str),
        Some("authority_binding_known_name")
    );
}

#[test]
fn normalize_surface_bindings_closes_same_batch_open_intervals() -> Result<()> {
    let first = test_binding(
        Uuid::from_u128(0x100),
        "ens:missioncontrol.2718.eth",
        1_695_230_399,
        None,
    );
    let second = test_binding(
        Uuid::from_u128(0x200),
        "ens:missioncontrol.2718.eth",
        1_695_284_247,
        None,
    );

    let mut bindings = vec![second.clone(), first.clone()];
    normalize_surface_bindings_for_upsert(&mut bindings)?;

    assert_eq!(bindings.len(), 2);
    assert_eq!(bindings[0].surface_binding_id, first.surface_binding_id);
    assert_eq!(bindings[0].active_to, Some(second.active_from));
    assert_eq!(bindings[1].surface_binding_id, second.surface_binding_id);
    assert_eq!(bindings[1].active_to, None);

    Ok(())
}

#[test]
fn normalize_surface_bindings_drops_same_start_lower_precedence_binding() -> Result<()> {
    let registry_only = test_binding_with_authority_kind(
        Uuid::from_u128(0x100),
        "ens:same-start.eth",
        1_695_230_399,
        None,
        "registry_only",
    );
    let registrar = test_binding_with_authority_kind(
        Uuid::from_u128(0x200),
        "ens:same-start.eth",
        1_695_230_399,
        None,
        "registrar",
    );
    let later = test_binding_with_authority_kind(
        Uuid::from_u128(0x300),
        "ens:same-start.eth",
        1_695_284_247,
        None,
        "registry_only",
    );

    let mut bindings = vec![later.clone(), registry_only, registrar.clone()];
    normalize_surface_bindings_for_upsert(&mut bindings)?;

    assert_eq!(bindings.len(), 2);
    assert_eq!(bindings[0].surface_binding_id, registrar.surface_binding_id);
    assert_eq!(bindings[0].active_to, Some(later.active_from));
    assert_eq!(bindings[1].surface_binding_id, later.surface_binding_id);
    assert_eq!(bindings[1].active_to, None);

    Ok(())
}

#[test]
fn trim_incoming_bindings_at_existing_starts_closes_historical_open_interval() {
    let mut incoming = vec![test_binding(
        Uuid::from_u128(0x100),
        "ens:future.eth",
        1_695_230_399,
        None,
    )];
    let existing = vec![test_binding(
        Uuid::from_u128(0x200),
        "ens:future.eth",
        1_695_284_247,
        None,
    )];

    trim_incoming_bindings_at_existing_starts(&mut incoming, &existing);

    assert_eq!(incoming[0].active_to, Some(existing[0].active_from));
}

#[test]
fn existing_binding_closure_uses_next_later_incoming_segment() {
    let existing = test_binding(
        Uuid::from_u128(0x200),
        "ens:bancambios.eth",
        1_582_156_785,
        None,
    );
    let earlier_incoming = test_binding(
        Uuid::from_u128(0x100),
        "ens:bancambios.eth",
        1_581_795_431,
        Some(existing.active_from),
    );
    let later_incoming = test_binding(
        Uuid::from_u128(0x300),
        "ens:bancambios.eth",
        1_582_530_648,
        None,
    );

    let closures = existing_binding_closures_for_incoming(
        std::slice::from_ref(&existing),
        &[earlier_incoming, later_incoming.clone()],
    );

    assert_eq!(closures.len(), 1);
    assert_eq!(closures[0].surface_binding_id, existing.surface_binding_id);
    assert_eq!(closures[0].active_to, Some(later_incoming.active_from));
}

#[test]
fn stale_overlap_candidates_only_include_adapter_rows_without_matching_binding_id() {
    let incoming = vec![test_binding(
        Uuid::from_u128(0x100),
        "ens:retry.eth",
        1_695_230_399,
        None,
    )];
    let mut stale = test_binding(Uuid::from_u128(0x200), "ens:retry.eth", 1_695_230_399, None);
    stale.provenance = json!({
        "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
        "authority_key": "registrar:ethereum-mainnet:0xabc:1"
    });
    let mut same_id = stale.clone();
    same_id.surface_binding_id = incoming[0].surface_binding_id;
    let unrelated_adapter =
        test_binding(Uuid::from_u128(0x300), "ens:retry.eth", 1_695_230_399, None);

    let candidates = stale_overlapping_surface_binding_candidates(
        &incoming,
        &[stale.clone(), same_id, unrelated_adapter],
    );

    assert_eq!(candidates.len(), 1);
    assert_eq!(
        candidates
            .get(&stale.surface_binding_id)
            .map(|candidate| candidate.resource_id),
        Some(stale.resource_id)
    );
    assert_eq!(
        candidates
            .get(&stale.surface_binding_id)
            .and_then(|candidate| candidate.authority_key.as_deref()),
        Some("registrar:ethereum-mainnet:0xabc:1")
    );
    assert_eq!(
        candidates
            .get(&stale.surface_binding_id)
            .map(|candidate| candidate.active_from_epoch),
        Some(stale.active_from.unix_timestamp())
    );
}

#[test]
fn weaker_same_start_candidates_include_lower_precedence_existing_binding() {
    let incoming = vec![test_binding_with_authority_kind(
        Uuid::from_u128(0x100),
        "ens:retry.eth",
        1_695_230_399,
        None,
        "registrar",
    )];
    let mut weaker_existing = test_binding_with_authority_kind(
        Uuid::from_u128(0x200),
        "ens:retry.eth",
        1_695_230_399,
        None,
        "registry_only",
    );
    weaker_existing.provenance = json!({
        "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
        "authority_kind": "registry_only",
        "authority_key": "registry-only:ethereum-mainnet:0xabc"
    });
    let mut stronger_existing = test_binding_with_authority_kind(
        Uuid::from_u128(0x300),
        "ens:retry.eth",
        1_695_230_399,
        None,
        "wrapper",
    );
    stronger_existing.provenance = json!({
        "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
        "authority_kind": "wrapper",
        "authority_key": "wrapper:ethereum-mainnet:0xabc"
    });

    let candidates = weaker_same_start_surface_binding_candidates(
        &incoming,
        &[weaker_existing.clone(), stronger_existing],
    );

    assert_eq!(candidates.len(), 1);
    assert_eq!(
        candidates
            .get(&weaker_existing.surface_binding_id)
            .map(|candidate| candidate.resource_id),
        Some(weaker_existing.resource_id)
    );
    assert_eq!(
        candidates
            .get(&weaker_existing.surface_binding_id)
            .map(|candidate| candidate.active_from),
        Some(weaker_existing.active_from)
    );
}

#[tokio::test]
async fn weaker_same_start_orphaning_preserves_event_backed_binding() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = MaterializationTestDatabase::new().await?;

    let incoming_backed = adapter_binding_with_authority(
        Uuid::from_u128(0x1000),
        "ens:backed.eth",
        1_695_230_399,
        "registrar",
        "registrar:ethereum-mainnet:backed",
        "0x1000100010001000100010001000100010001000100010001000100010001000",
    );
    let incoming_unbacked = adapter_binding_with_authority(
        Uuid::from_u128(0x1001),
        "ens:unbacked.eth",
        1_695_230_399,
        "registrar",
        "registrar:ethereum-mainnet:unbacked",
        "0x1001100110011001100110011001100110011001100110011001100110011001",
    );
    let event_backed = adapter_binding_with_authority(
        Uuid::from_u128(0x2000),
        "ens:backed.eth",
        1_695_230_399,
        "registry_only",
        "registry-only:ethereum-mainnet:backed",
        "0x2000200020002000200020002000200020002000200020002000200020002000",
    );
    let unbacked = adapter_binding_with_authority(
        Uuid::from_u128(0x2001),
        "ens:unbacked.eth",
        1_695_230_399,
        "registry_only",
        "registry-only:ethereum-mainnet:unbacked",
        "0x2001200120012001200120012001200120012001200120012001200120012001",
    );

    upsert_name_surfaces_without_snapshots(
        database.pool(),
        &[
            test_surface(
                "ens:backed.eth",
                "backed.eth",
                "0x3000300030003000300030003000300030003000300030003000300030003000",
            ),
            test_surface(
                "ens:unbacked.eth",
                "unbacked.eth",
                "0x3001300130013001300130013001300130013001300130013001300130013001",
            ),
        ],
    )
    .await?;
    upsert_resources_without_snapshots(
        database.pool(),
        &[
            test_resource(
                event_backed.resource_id,
                "0x4000400040004000400040004000400040004000400040004000400040004000",
            ),
            test_resource(
                unbacked.resource_id,
                "0x4001400140014001400140014001400140014001400140014001400140014001",
            ),
        ],
    )
    .await?;
    upsert_surface_bindings_without_snapshots(
        database.pool(),
        &[event_backed.clone(), unbacked.clone()],
    )
    .await?;
    upsert_normalized_events(
        database.pool(),
        &[
            surface_bound_event(
                "event-backed-surface-bound",
                &event_backed,
                "registry-only:ethereum-mainnet:backed",
            ),
            registration_granted_event(
                "unbacked-registration-granted",
                "ens:unbacked.eth",
                unbacked.resource_id,
                "0x1111111111111111111111111111111111111111",
            ),
        ],
    )
    .await?;

    let mut existing = vec![event_backed.clone(), unbacked.clone()];
    let orphaned_count = orphan_weaker_same_start_surface_bindings(
        database.pool(),
        &[incoming_backed, incoming_unbacked],
        &mut existing,
    )
    .await?;

    assert_eq!(orphaned_count, 1);
    assert_eq!(
        existing
            .iter()
            .map(|binding| binding.surface_binding_id)
            .collect::<Vec<_>>(),
        vec![event_backed.surface_binding_id]
    );
    assert_eq!(
        surface_binding_canonicality(database.pool(), event_backed.surface_binding_id).await?,
        "canonical"
    );
    assert_eq!(
        surface_binding_canonicality(database.pool(), unbacked.surface_binding_id).await?,
        "orphaned"
    );
    assert_projection_invalidation(database.pool(), "name_current", "ens:unbacked.eth").await?;
    assert_projection_invalidation(
        database.pool(),
        "address_names_current",
        "0x1111111111111111111111111111111111111111:ens:unbacked.eth",
    )
    .await?;
    assert_no_projection_invalidation(database.pool(), "name_current", "ens:backed.eth").await?;

    database.cleanup().await
}

#[tokio::test]
async fn overlap_closure_queues_projection_invalidations_for_repaired_binding() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = MaterializationTestDatabase::new().await?;

    let existing = adapter_binding_with_authority(
        Uuid::from_u128(0x3000),
        "ens:closed.eth",
        1_695_230_399,
        "registry_only",
        "registry-only:ethereum-mainnet:closed",
        "0x3000300030003000300030003000300030003000300030003000300030003000",
    );
    let incoming = adapter_binding_with_authority(
        Uuid::from_u128(0x3001),
        "ens:closed.eth",
        1_695_284_247,
        "registrar",
        "registrar:ethereum-mainnet:closed",
        "0x3001300130013001300130013001300130013001300130013001300130013001",
    );

    upsert_name_surfaces_without_snapshots(
        database.pool(),
        &[test_surface(
            "ens:closed.eth",
            "closed.eth",
            "0x5000500050005000500050005000500050005000500050005000500050005000",
        )],
    )
    .await?;
    upsert_resources_without_snapshots(
        database.pool(),
        &[test_resource(
            existing.resource_id,
            "0x6000600060006000600060006000600060006000600060006000600060006000",
        )],
    )
    .await?;
    upsert_surface_bindings_without_snapshots(database.pool(), std::slice::from_ref(&existing))
        .await?;
    upsert_normalized_events(
        database.pool(),
        &[registration_granted_event(
            "closed-registration-granted",
            "ens:closed.eth",
            existing.resource_id,
            "0x2222222222222222222222222222222222222222",
        )],
    )
    .await?;

    let repaired_count = close_weaker_overlapping_existing_surface_bindings(
        database.pool(),
        std::slice::from_ref(&incoming),
    )
    .await?;

    assert_eq!(repaired_count, 1);
    assert_eq!(
        surface_binding_active_to(database.pool(), existing.surface_binding_id).await?,
        Some(incoming.active_from)
    );
    assert_projection_invalidation(database.pool(), "name_current", "ens:closed.eth").await?;
    assert_projection_invalidation(
        database.pool(),
        "address_names_current",
        "0x2222222222222222222222222222222222222222:ens:closed.eth",
    )
    .await?;

    database.cleanup().await
}

#[tokio::test]
async fn overlap_closure_tolerates_missing_projection_invalidation_queue() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = MaterializationTestDatabase::new().await?;

    let existing = adapter_binding_with_authority(
        Uuid::from_u128(0x3100),
        "ens:queue-missing.eth",
        1_695_230_399,
        "registry_only",
        "registry-only:ethereum-mainnet:queue-missing",
        "0x3100310031003100310031003100310031003100310031003100310031003100",
    );
    let incoming = adapter_binding_with_authority(
        Uuid::from_u128(0x3101),
        "ens:queue-missing.eth",
        1_695_284_247,
        "registrar",
        "registrar:ethereum-mainnet:queue-missing",
        "0x3101310131013101310131013101310131013101310131013101310131013101",
    );

    upsert_name_surfaces_without_snapshots(
        database.pool(),
        &[test_surface(
            "ens:queue-missing.eth",
            "queue-missing.eth",
            "0x5100510051005100510051005100510051005100510051005100510051005100",
        )],
    )
    .await?;
    upsert_resources_without_snapshots(
        database.pool(),
        &[test_resource(
            existing.resource_id,
            "0x6100610061006100610061006100610061006100610061006100610061006100",
        )],
    )
    .await?;
    upsert_surface_bindings_without_snapshots(database.pool(), std::slice::from_ref(&existing))
        .await?;
    sqlx::query("DROP TABLE projection_invalidations")
        .execute(database.pool())
        .await?;

    let repaired_count = close_weaker_overlapping_existing_surface_bindings(
        database.pool(),
        std::slice::from_ref(&incoming),
    )
    .await?;

    assert_eq!(repaired_count, 1);
    assert_eq!(
        surface_binding_active_to(database.pool(), existing.surface_binding_id).await?,
        Some(incoming.active_from)
    );

    database.cleanup().await
}

#[tokio::test]
async fn restricted_replay_keeps_later_registry_binding_after_open_registrar() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = MaterializationTestDatabase::new().await?;

    let existing = adapter_binding_with_authority(
        Uuid::from_u128(0x3200),
        "ens:diverged.eth",
        1_695_230_399,
        "registrar",
        "registrar:ethereum-mainnet:diverged",
        "0x3200320032003200320032003200320032003200320032003200320032003200",
    );
    let incoming = adapter_binding_with_authority(
        Uuid::from_u128(0x3201),
        "ens:diverged.eth",
        1_695_284_247,
        "registry_only",
        "registry-only:ethereum-mainnet:diverged",
        "0x3201320132013201320132013201320132013201320132013201320132013201",
    );

    upsert_name_surfaces_without_snapshots(
        database.pool(),
        &[test_surface(
            "ens:diverged.eth",
            "diverged.eth",
            "0x5200520052005200520052005200520052005200520052005200520052005200",
        )],
    )
    .await?;
    upsert_resources_without_snapshots(
        database.pool(),
        &[test_resource(
            existing.resource_id,
            "0x6200620062006200620062006200620062006200620062006200620062006200",
        )],
    )
    .await?;
    upsert_surface_bindings_without_snapshots(database.pool(), std::slice::from_ref(&existing))
        .await?;

    let mut bindings = vec![incoming.clone()];
    let closure_count = prepend_existing_open_binding_closures(database.pool(), &mut bindings)
        .await
        .context("failed to prepend existing binding closure")?;

    assert_eq!(closure_count, 0);
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].surface_binding_id, incoming.surface_binding_id);
    assert_eq!(bindings[0].active_to, None);

    database.cleanup().await
}

#[test]
fn normalize_surface_bindings_tightens_duplicate_active_to() -> Result<()> {
    let earlier_close =
        OffsetDateTime::from_unix_timestamp(1_695_284_247).expect("test timestamp is valid");
    let later_close =
        OffsetDateTime::from_unix_timestamp(1_695_370_647).expect("test timestamp is valid");
    let first = test_binding(
        Uuid::from_u128(0x100),
        "ens:missioncontrol.2718.eth",
        1_695_230_399,
        Some(later_close),
    );
    let second = test_binding(
        Uuid::from_u128(0x100),
        "ens:missioncontrol.2718.eth",
        1_695_230_399,
        Some(earlier_close),
    );

    let mut bindings = vec![first, second];
    normalize_surface_bindings_for_upsert(&mut bindings)?;

    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].active_to, Some(earlier_close));

    Ok(())
}

fn test_surface(logical_name_id: &str, input_name: &str, namehash: &str) -> NameSurface {
    let normalized_name = logical_name_id
        .strip_prefix("ens:")
        .expect("test logical name id uses ens namespace")
        .to_owned();

    NameSurface {
        logical_name_id: logical_name_id.to_owned(),
        namespace: "ens".to_owned(),
        input_name: input_name.to_owned(),
        canonical_display_name: normalized_name.clone(),
        normalized_name,
        dns_encoded_name: vec![3, b'e', b't', b'h', 0],
        namehash: namehash.to_owned(),
        labelhashes: vec![namehash.to_owned()],
        normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: namehash.to_owned(),
        block_number: 1,
        provenance: json!({}),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn test_binding(
    surface_binding_id: Uuid,
    logical_name_id: &str,
    active_from: i64,
    active_to: Option<OffsetDateTime>,
) -> SurfaceBinding {
    SurfaceBinding {
        surface_binding_id,
        logical_name_id: logical_name_id.to_owned(),
        resource_id: surface_binding_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: OffsetDateTime::from_unix_timestamp(active_from)
            .expect("test timestamp is valid"),
        active_to,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: format!("0x{surface_binding_id}"),
        block_number: active_from,
        provenance: json!({}),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn test_resource(resource_id: Uuid, block_hash: &str) -> Resource {
    Resource {
        resource_id,
        token_lineage_id: None,
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number: 1,
        provenance: json!({ "source": "materialization-test" }),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn test_binding_with_authority_kind(
    surface_binding_id: Uuid,
    logical_name_id: &str,
    active_from: i64,
    active_to: Option<OffsetDateTime>,
    authority_kind: &str,
) -> SurfaceBinding {
    let mut binding = test_binding(surface_binding_id, logical_name_id, active_from, active_to);
    binding.provenance = json!({ "authority_kind": authority_kind });
    binding
}

fn adapter_binding_with_authority(
    surface_binding_id: Uuid,
    logical_name_id: &str,
    active_from: i64,
    authority_kind: &str,
    authority_key: &str,
    block_hash: &str,
) -> SurfaceBinding {
    let mut binding = test_binding_with_authority_kind(
        surface_binding_id,
        logical_name_id,
        active_from,
        None,
        authority_kind,
    );
    binding.block_hash = block_hash.to_owned();
    binding.provenance = json!({
        "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
        "authority_kind": authority_kind,
        "authority_key": authority_key
    });
    binding
}

fn surface_bound_event(
    event_identity: &str,
    binding: &SurfaceBinding,
    authority_key: &str,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: Some(binding.logical_name_id.clone()),
        resource_id: Some(binding.resource_id),
        event_kind: EVENT_KIND_SURFACE_BOUND.to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRY_L1.to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some(binding.chain_id.clone()),
        block_number: Some(binding.block_number),
        block_hash: Some(binding.block_hash.clone()),
        transaction_hash: Some(
            "0xtxsurfacebound000000000000000000000000000000000000000000000000".to_owned(),
        ),
        log_index: Some(0),
        raw_fact_ref: json!({}),
        derivation_kind: DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY.to_owned(),
        canonicality_state: CanonicalityState::Canonical,
        before_state: json!({}),
        after_state: json!({
            "authority_key": authority_key,
            "active_from": binding.active_from.unix_timestamp()
        }),
    }
}

fn registration_granted_event(
    event_identity: &str,
    logical_name_id: &str,
    resource_id: Uuid,
    registrant: &str,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: Some(logical_name_id.to_owned()),
        resource_id: Some(resource_id),
        event_kind: EVENT_KIND_REGISTRATION_GRANTED.to_owned(),
        source_family: SOURCE_FAMILY_ENS_V1_REGISTRAR_L1.to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("ethereum-mainnet".to_owned()),
        block_number: Some(1),
        block_hash: Some(
            "0x7777777777777777777777777777777777777777777777777777777777777777".to_owned(),
        ),
        transaction_hash: Some(
            "0xtxregistration000000000000000000000000000000000000000000000".to_owned(),
        ),
        log_index: Some(0),
        raw_fact_ref: json!({}),
        derivation_kind: DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY.to_owned(),
        canonicality_state: CanonicalityState::Canonical,
        before_state: json!({}),
        after_state: json!({ "registrant": registrant }),
    }
}

async fn surface_binding_canonicality(pool: &PgPool, surface_binding_id: Uuid) -> Result<String> {
    sqlx::query_scalar::<_, String>(
        "SELECT canonicality_state::TEXT FROM surface_bindings WHERE surface_binding_id = $1",
    )
    .bind(surface_binding_id)
    .fetch_one(pool)
    .await
    .context("failed to load surface binding canonicality")
}

async fn surface_binding_active_to(
    pool: &PgPool,
    surface_binding_id: Uuid,
) -> Result<Option<OffsetDateTime>> {
    sqlx::query_scalar::<_, Option<OffsetDateTime>>(
        "SELECT active_to FROM surface_bindings WHERE surface_binding_id = $1",
    )
    .bind(surface_binding_id)
    .fetch_one(pool)
    .await
    .context("failed to load surface binding active_to")
}

async fn assert_projection_invalidation(
    pool: &PgPool,
    projection: &str,
    projection_key: &str,
) -> Result<()> {
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM projection_invalidations
        WHERE projection = $1
          AND projection_key = $2
        "#,
    )
    .bind(projection)
    .bind(projection_key)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!("failed to count projection invalidation {projection}:{projection_key}")
    })?;

    assert_eq!(count, 1);
    Ok(())
}

async fn assert_no_projection_invalidation(
    pool: &PgPool,
    projection: &str,
    projection_key: &str,
) -> Result<()> {
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM projection_invalidations
        WHERE projection = $1
          AND projection_key = $2
        "#,
    )
    .bind(projection)
    .bind(projection_key)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!("failed to count projection invalidation {projection}:{projection_key}")
    })?;

    assert_eq!(count, 0);
    Ok(())
}
