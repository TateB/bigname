use std::str::FromStr;

use anyhow::{Context, Result};
use bigname_test_support::{TestDatabase, TestDatabaseConfig, database_url_from_env};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

use super::*;

const DEPLOYMENT_PROFILE: &str = "mainnet";

struct FixtureIds {
    token_lineage_id: Uuid,
    resource_id: Uuid,
    surface_binding_id: Uuid,
    logical_name_id: &'static str,
}

async fn test_database() -> Result<TestDatabase> {
    TestDatabase::create_migrated(
        TestDatabaseConfig::new("bigname_storage_base_rederive_test")
            .admin_connect_context("failed to connect admin pool for Base rederive tests")
            .pool_connect_context("failed to connect Base rederive test pool"),
        &crate::MIGRATOR,
        "failed to apply migrations for Base rederive tests",
    )
    .await
}

#[tokio::test]
async fn dry_run_census_matches_seeded_fixture() -> Result<()> {
    let database = test_database().await?;
    let ids = seed_rederive_fixture(database.pool()).await?;

    let plan = load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE).await?;

    assert_eq!(plan.counts.normalized_events, 2);
    assert_eq!(plan.counts.resources, 1);
    assert_eq!(plan.counts.token_lineages, 1);
    assert_eq!(plan.counts.name_surfaces, 1);
    assert_eq!(plan.counts.surface_bindings, 1);
    assert_eq!(plan.counts.name_current, 1);
    assert_eq!(plan.counts.address_names_current, 1);
    assert_eq!(plan.counts.children_current, 1);
    assert_eq!(plan.counts.permissions_current, 1);
    assert_eq!(plan.counts.record_inventory_current, 1);
    assert_eq!(plan.counts.projection_normalized_event_changes, 2);
    assert_eq!(plan.counts.replay_cursor_rows, 1);
    assert_eq!(plan.counts.adapter_checkpoint_rows, 2);
    assert_eq!(plan.counts.adapter_checkpoint_item_rows, 2);
    assert_eq!(plan.raw_fact_completeness.log_derived_event_count, 1);
    assert_eq!(plan.raw_fact_completeness.boundary_event_count, 1);
    assert!(plan.raw_fact_completeness.is_complete_for_rerun());
    assert_eq!(
        plan.family_census
            .iter()
            .map(|family| (family.source_manifest_id, family.row_count))
            .collect::<Vec<_>>(),
        vec![(1, 1), (2, 0), (4, 1), (5, 0)]
    );
    assert_eq!(ids.logical_name_id, "basenames:alice.base.eth");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn execute_deletes_fk_safe_scope_and_resets_replay() -> Result<()> {
    let database = test_database().await?;
    let ids = seed_rederive_fixture(database.pool()).await?;
    let expected = load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE)
        .await?
        .counts;

    let outcome = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        BaseNormalizedRederiveExpectedCounts {
            counts: expected.clone(),
        },
    )
    .await?;

    assert_eq!(outcome.deleted, expected);
    assert_eq!(count_table(database.pool(), "raw_logs").await?, 2);
    assert_eq!(
        count_scalar(
            database.pool(),
            "SELECT COUNT(*) FROM resources WHERE resource_id = $1",
            ids.resource_id,
        )
        .await?,
        0
    );
    assert_eq!(
        count_scalar(
            database.pool(),
            "SELECT COUNT(*) FROM token_lineages WHERE token_lineage_id = $1",
            ids.token_lineage_id,
        )
        .await?,
        0
    );
    assert_eq!(
        count_scalar(
            database.pool(),
            "SELECT COUNT(*) FROM surface_bindings WHERE surface_binding_id = $1",
            ids.surface_binding_id,
        )
        .await?,
        0
    );
    assert_eq!(
        count_text_scalar(
            database.pool(),
            "SELECT COUNT(*) FROM name_surfaces WHERE logical_name_id = $1",
            ids.logical_name_id,
        )
        .await?,
        0
    );
    assert_eq!(
        count_table(database.pool(), "projection_normalized_event_changes").await?,
        3
    );
    assert_eq!(count_table(database.pool(), "normalized_events").await?, 3);

    let cursor = sqlx::query_as::<_, (i64, i64, i64)>(
        r#"
        SELECT range_start_block_number, next_block_number, target_block_number
        FROM normalized_replay_cursors
        WHERE deployment_profile = $1
          AND chain_id = 'base-mainnet'
          AND cursor_kind = 'raw_fact_normalized_events'
        "#,
    )
    .bind(DEPLOYMENT_PROFILE)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        cursor,
        (
            BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
            BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
            BASE_NORMALIZED_REDERIVE_REPLAY_TARGET_BLOCK
        )
    );
    assert_eq!(
        count_table(database.pool(), "normalized_replay_adapter_checkpoints").await?,
        0
    );
    assert_eq!(
        count_table(
            database.pool(),
            "normalized_replay_adapter_checkpoint_items"
        )
        .await?,
        0
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn execute_refuses_running_indexer_or_worker_session() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;
    let runtime_pool = runtime_named_pool(database.database_name(), "bigname-indexer").await?;

    let error =
        execute_base_normalized_rederive_drop(database.pool(), DEPLOYMENT_PROFILE, expected)
            .await
            .expect_err("running runtime session must block execution");
    assert!(format!("{error:?}").contains("runtime sessions are connected"));

    runtime_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn execute_refuses_runtime_shared_advisory_lock() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;
    let runtime_pool = runtime_named_pool(database.database_name(), "other-runtime").await?;
    let runtime_guard =
        hold_base_normalized_rederive_runtime_shared_lock(&runtime_pool, "other-runtime").await?;

    let error =
        execute_base_normalized_rederive_drop(database.pool(), DEPLOYMENT_PROFILE, expected)
            .await
            .expect_err("runtime shared advisory lock must block execution");
    assert!(format!("{error:?}").contains("advisory lock is already held"));

    drop(runtime_guard);
    runtime_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn execute_refuses_count_divergence_from_reviewed_census() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE)
        .await?
        .counts;
    seed_extra_scoped_resource(database.pool()).await?;

    let error = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        BaseNormalizedRederiveExpectedCounts { counts: expected },
    )
    .await
    .expect_err("count divergence must block execution");
    assert!(format!("{error:?}").contains("count divergence"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn execute_refuses_remaining_normalized_event_identity_anchors() -> Result<()> {
    let database = test_database().await?;
    let ids = seed_rederive_fixture(database.pool()).await?;
    seed_out_of_scope_event_referencing_scoped_identity(database.pool(), &ids).await?;
    let expected = reviewed_counts(database.pool()).await?;

    let error =
        execute_base_normalized_rederive_drop(database.pool(), DEPLOYMENT_PROFILE, expected)
            .await
            .expect_err("remaining normalized-event identity anchor must block execution");
    assert!(format!("{error:?}").contains("remaining_events_referencing_identity=1"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn dry_run_raw_fact_span_ignores_retained_logs_outside_rederive_window() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    seed_extra_retained_raw_logs_outside_window(database.pool()).await?;

    let plan = load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE).await?;

    assert_eq!(count_table(database.pool(), "raw_logs").await?, 4);
    assert_eq!(
        plan.raw_fact_completeness.canonical_raw_log_min_block,
        Some(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK)
    );
    assert_eq!(
        plan.raw_fact_completeness.canonical_raw_log_max_block,
        Some(BASE_NORMALIZED_REDERIVE_REPLAY_TARGET_BLOCK)
    );
    assert!(plan.raw_fact_completeness.is_complete_for_rerun());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn execute_refuses_raw_fact_completeness_gap() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;
    sqlx::query("DELETE FROM raw_logs WHERE block_hash = '0xbase-start'")
        .execute(database.pool())
        .await?;

    let error =
        execute_base_normalized_rederive_drop(database.pool(), DEPLOYMENT_PROFILE, expected)
            .await
            .expect_err("raw fact gap must block execution");
    assert!(format!("{error:?}").contains("raw-fact completeness check failed"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn execute_is_idempotent_after_initial_drop() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;
    execute_base_normalized_rederive_drop(database.pool(), DEPLOYMENT_PROFILE, expected).await?;

    let second_plan =
        load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE).await?;
    assert_eq!(second_plan.counts.normalized_events, 0);
    assert_eq!(second_plan.counts.resources, 0);
    assert_eq!(second_plan.counts.replay_cursor_rows, 1);
    let second = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        BaseNormalizedRederiveExpectedCounts {
            counts: second_plan.counts.clone(),
        },
    )
    .await?;
    assert_eq!(second.deleted.normalized_events, 0);
    assert_eq!(second.deleted.resources, 0);
    assert_eq!(second.deleted.replay_cursor_rows, 1);

    database.cleanup().await?;
    Ok(())
}

async fn seed_rederive_fixture(pool: &PgPool) -> Result<FixtureIds> {
    seed_manifests(pool).await?;
    seed_raw_facts(pool).await?;
    seed_normalized_events(pool).await?;
    let ids = seed_identity_and_projection_rows(pool).await?;
    seed_replay_state(pool).await?;
    Ok(ids)
}

async fn reviewed_counts(pool: &PgPool) -> Result<BaseNormalizedRederiveExpectedCounts> {
    Ok(BaseNormalizedRederiveExpectedCounts {
        counts: load_base_normalized_rederive_plan(pool, DEPLOYMENT_PROFILE)
            .await?
            .counts,
    })
}

async fn seed_manifests(pool: &PgPool) -> Result<()> {
    for (manifest_id, source_family) in [
        (1, "basenames_base_registry"),
        (2, "basenames_base_registrar"),
        (3, "basenames_l1_compat"),
        (4, "basenames_base_resolver"),
        (5, "basenames_base_primary"),
    ] {
        sqlx::query(
            r#"
            INSERT INTO manifest_versions (
                manifest_id, manifest_version, namespace, source_family, chain,
                deployment_epoch, rollout_status, normalizer_version, file_path, manifest_payload
            )
            OVERRIDING SYSTEM VALUE
            VALUES ($1, 1, 'basenames', $2, $3, 'bootstrap', 'active',
                    'ensip15@ens-normalize-0.1.1', $4, '{}'::jsonb)
            "#,
        )
        .bind(manifest_id)
        .bind(source_family)
        .bind(if manifest_id == 3 {
            "ethereum-mainnet"
        } else {
            "base-mainnet"
        })
        .bind(format!("manifests/basenames/{source_family}/v1.toml"))
        .execute(pool)
        .await
        .with_context(|| format!("failed to seed manifest {manifest_id}"))?;
    }
    Ok(())
}

async fn seed_raw_facts(pool: &PgPool) -> Result<()> {
    for (block_hash, parent_hash, block_number) in [
        (
            "0xbase-start",
            None,
            BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
        ),
        (
            "0xbase-mid",
            Some("0xbase-start"),
            BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK + 1,
        ),
        (
            "0xbase-target",
            Some("0xbase-mid"),
            BASE_NORMALIZED_REDERIVE_REPLAY_TARGET_BLOCK,
        ),
    ] {
        sqlx::query(
            r#"
            INSERT INTO chain_lineage (
                chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state
            )
            VALUES ('base-mainnet', $1, $2, $3, '2026-07-03T00:00:00Z', 'canonical')
            "#,
        )
        .bind(block_hash)
        .bind(parent_hash)
        .bind(block_number)
        .execute(pool)
        .await?;
    }
    for (block_hash, block_number, tx, log_index) in [
        (
            "0xbase-start",
            BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
            "0xtx-start",
            0_i64,
        ),
        (
            "0xbase-target",
            BASE_NORMALIZED_REDERIVE_REPLAY_TARGET_BLOCK,
            "0xtx-target",
            9_i64,
        ),
    ] {
        sqlx::query(
            r#"
            INSERT INTO raw_logs (
                chain_id, block_hash, block_number, transaction_hash,
                transaction_index, log_index, emitting_address, canonicality_state
            )
            VALUES ('base-mainnet', $1, $2, $3, 0, $4, '0xemitter', 'canonical')
            "#,
        )
        .bind(block_hash)
        .bind(block_number)
        .bind(tx)
        .bind(log_index)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn seed_normalized_events(pool: &PgPool) -> Result<()> {
    for (identity, source_manifest_id, block_number, block_hash, tx, log_index, derivation) in [
        (
            "scoped-log",
            1_i64,
            Some(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK),
            Some("0xbase-start"),
            Some("0xtx-start"),
            Some(0_i64),
            "ens_v1_unwrapped_authority",
        ),
        (
            "scoped-boundary",
            4_i64,
            Some(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK + 1),
            Some("0xbase-mid"),
            None,
            None,
            "ens_v1_unwrapped_authority",
        ),
        (
            "manifest-no-block",
            1_i64,
            None,
            None,
            None,
            None,
            "manifest_sync",
        ),
        (
            "out-of-range",
            1_i64,
            Some(BASE_NORMALIZED_REDERIVE_REPLAY_TARGET_BLOCK + 1),
            Some("0xafter"),
            None,
            None,
            "ens_v1_unwrapped_authority",
        ),
        (
            "wrong-manifest",
            3_i64,
            Some(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK),
            Some("0xbase-start"),
            None,
            None,
            "ens_v1_unwrapped_authority",
        ),
    ] {
        sqlx::query(
            r#"
            INSERT INTO normalized_events (
                event_identity, namespace, event_kind, source_family, manifest_version,
                source_manifest_id, chain_id, block_number, block_hash, transaction_hash,
                log_index, raw_fact_ref, derivation_kind, canonicality_state
            )
            VALUES ($1, 'basenames', 'RecordChanged', 'basenames_base_registry', 1,
                    $2, 'base-mainnet', $3, $4, $5, $6, '{}'::jsonb, $7, 'canonical')
            "#,
        )
        .bind(identity)
        .bind(source_manifest_id)
        .bind(block_number)
        .bind(block_hash)
        .bind(tx)
        .bind(log_index)
        .bind(derivation)
        .execute(pool)
        .await
        .with_context(|| format!("failed to seed normalized event {identity}"))?;
    }
    Ok(())
}

async fn seed_identity_and_projection_rows(pool: &PgPool) -> Result<FixtureIds> {
    let token_lineage_id = Uuid::from_u128(0x100);
    let resource_id = Uuid::from_u128(0x200);
    let surface_binding_id = Uuid::from_u128(0x300);
    let parent_resource_id = Uuid::from_u128(0x201);
    let parent_binding_id = Uuid::from_u128(0x301);
    let logical_name_id = "basenames:alice.base.eth";
    let parent_logical_name_id = "basenames:base.eth";

    sqlx::query(
        r#"
        INSERT INTO token_lineages (
            token_lineage_id, chain_id, block_hash, block_number, provenance, canonicality_state
        )
        VALUES ($1, 'base-mainnet', '0xbase-start', 17571485,
                '{"adapter":"ens_v1_unwrapped_authority"}'::jsonb, 'canonical')
        "#,
    )
    .bind(token_lineage_id)
    .execute(pool)
    .await?;
    for (resource, token, provenance) in [
        (
            resource_id,
            Some(token_lineage_id),
            r#"{"adapter":"ens_v1_unwrapped_authority"}"#,
        ),
        (parent_resource_id, None, r#"{"adapter":"other"}"#),
    ] {
        sqlx::query(
            r#"
            INSERT INTO resources (
                resource_id, token_lineage_id, chain_id, block_hash, block_number,
                provenance, canonicality_state
            )
            VALUES ($1, $2, 'base-mainnet', '0xbase-start', 17571485,
                    $3::jsonb, 'canonical')
            "#,
        )
        .bind(resource)
        .bind(token)
        .bind(provenance)
        .execute(pool)
        .await?;
    }
    for (logical, normalized, provenance) in [
        (
            logical_name_id,
            "alice.base.eth",
            r#"{"adapter":"ens_v1_unwrapped_authority"}"#,
        ),
        (parent_logical_name_id, "base.eth", r#"{"adapter":"other"}"#),
    ] {
        sqlx::query(
            r#"
            INSERT INTO name_surfaces (
                logical_name_id, namespace, input_name, canonical_display_name,
                normalized_name, dns_encoded_name, namehash, labelhashes,
                normalizer_version, chain_id, block_hash, block_number,
                provenance, canonicality_state
            )
            VALUES ($1, 'basenames', $2, $2, $2, '\x00'::bytea, $3, ARRAY['0xlabel'],
                    'ensip15@ens-normalize-0.1.1', 'base-mainnet', '0xbase-start',
                    17571485, $4::jsonb, 'canonical')
            "#,
        )
        .bind(logical)
        .bind(normalized)
        .bind(format!("0xhash-{normalized}"))
        .bind(provenance)
        .execute(pool)
        .await?;
    }
    for (binding, logical, resource, provenance) in [
        (
            surface_binding_id,
            logical_name_id,
            resource_id,
            r#"{"adapter":"ens_v1_unwrapped_authority"}"#,
        ),
        (
            parent_binding_id,
            parent_logical_name_id,
            parent_resource_id,
            r#"{"adapter":"other"}"#,
        ),
    ] {
        sqlx::query(
            r#"
            INSERT INTO surface_bindings (
                surface_binding_id, logical_name_id, resource_id, binding_kind,
                active_from, chain_id, block_hash, block_number, provenance, canonicality_state
            )
            VALUES ($1, $2, $3, 'declared_registry_path', '2026-07-03T00:00:00Z',
                    'base-mainnet', '0xbase-start', 17571485, $4::jsonb, 'canonical')
            "#,
        )
        .bind(binding)
        .bind(logical)
        .bind(resource)
        .bind(provenance)
        .execute(pool)
        .await?;
    }
    seed_projection_rows(
        pool,
        logical_name_id,
        parent_logical_name_id,
        resource_id,
        token_lineage_id,
        surface_binding_id,
    )
    .await?;
    Ok(FixtureIds {
        token_lineage_id,
        resource_id,
        surface_binding_id,
        logical_name_id,
    })
}

async fn seed_projection_rows(
    pool: &PgPool,
    logical_name_id: &str,
    parent_logical_name_id: &str,
    resource_id: Uuid,
    token_lineage_id: Uuid,
    surface_binding_id: Uuid,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO name_current (
            logical_name_id, namespace, canonical_display_name, normalized_name,
            namehash, surface_binding_id, resource_id, token_lineage_id,
            binding_kind, manifest_version
        )
        VALUES ($1, 'basenames', 'alice.base.eth', 'alice.base.eth', '0xname',
                $2, $3, $4, 'declared_registry_path', 1)
        "#,
    )
    .bind(logical_name_id)
    .bind(surface_binding_id)
    .bind(resource_id)
    .bind(token_lineage_id)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO address_names_current (
            address, logical_name_id, relation, namespace, canonical_display_name,
            normalized_name, namehash, surface_binding_id, resource_id, token_lineage_id,
            binding_kind, manifest_version
        )
        VALUES ('0xowner', $1, 'token_holder', 'basenames', 'alice.base.eth',
                'alice.base.eth', '0xname', $2, $3, $4, 'declared_registry_path', 1)
        "#,
    )
    .bind(logical_name_id)
    .bind(surface_binding_id)
    .bind(resource_id)
    .bind(token_lineage_id)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO children_current (
            parent_logical_name_id, child_logical_name_id, namespace,
            canonical_display_name, normalized_name, namehash, manifest_version
        )
        VALUES ($1, $2, 'basenames', 'alice.base.eth', 'alice.base.eth', '0xname', 1)
        "#,
    )
    .bind(parent_logical_name_id)
    .bind(logical_name_id)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO permissions_current (
            resource_id, subject, scope, scope_kind, manifest_version
        )
        VALUES ($1, '0xowner', 'registry', 'registry', 1)
        "#,
    )
    .bind(resource_id)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO record_inventory_current (
            resource_id, record_version_boundary_key, manifest_version
        )
        VALUES ($1, 'current', 1)
        "#,
    )
    .bind(resource_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_replay_state(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO normalized_replay_cursors (
            deployment_profile, chain_id, cursor_kind, range_start_block_number,
            next_block_number, target_block_number, last_completed_block_number
        )
        VALUES ($1, 'base-mainnet', 'raw_fact_normalized_events', 100, 200, 300, 199)
        "#,
    )
    .bind(DEPLOYMENT_PROFILE)
    .execute(pool)
    .await?;
    for (adapter, item_kind, item_key) in [
        (
            BASE_NORMALIZED_REDERIVE_DISCOVERY_ADAPTER,
            "registry_edge",
            "alice",
        ),
        (
            BASE_NORMALIZED_REDERIVE_ADAPTER,
            "name_history",
            "alice.base.eth",
        ),
    ] {
        sqlx::query(
            r#"
            INSERT INTO normalized_replay_adapter_checkpoints (
                deployment_profile, chain_id, cursor_kind, adapter, checkpoint_scope,
                replay_start_block_number, replay_target_block_number
            )
            VALUES ($1, 'base-mainnet', 'raw_fact_normalized_events',
                    $2, 'full_closure', 100, 300)
            "#,
        )
        .bind(DEPLOYMENT_PROFILE)
        .bind(adapter)
        .execute(pool)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO normalized_replay_adapter_checkpoint_items (
                deployment_profile, chain_id, cursor_kind, adapter, checkpoint_scope,
                item_kind, item_key
            )
            VALUES ($1, 'base-mainnet', 'raw_fact_normalized_events',
                    $2, 'full_closure', $3, $4)
            "#,
        )
        .bind(DEPLOYMENT_PROFILE)
        .bind(adapter)
        .bind(item_kind)
        .bind(item_key)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn seed_extra_retained_raw_logs_outside_window(pool: &PgPool) -> Result<()> {
    for (block_hash, block_number, tx) in [
        (
            "0xbase-before-retained",
            BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK - 10,
            "0xtx-before-retained",
        ),
        (
            "0xbase-after-retained",
            BASE_NORMALIZED_REDERIVE_REPLAY_TARGET_BLOCK + 10,
            "0xtx-after-retained",
        ),
    ] {
        sqlx::query(
            r#"
            INSERT INTO chain_lineage (
                chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state
            )
            VALUES ('base-mainnet', $1, NULL, $2, '2026-07-03T00:00:00Z', 'canonical')
            "#,
        )
        .bind(block_hash)
        .bind(block_number)
        .execute(pool)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO raw_logs (
                chain_id, block_hash, block_number, transaction_hash,
                transaction_index, log_index, emitting_address, canonicality_state
            )
            VALUES ('base-mainnet', $1, $2, $3, 0, 0, '0xemitter', 'canonical')
            "#,
        )
        .bind(block_hash)
        .bind(block_number)
        .bind(tx)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn seed_extra_scoped_resource(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO resources (
            resource_id, chain_id, block_hash, block_number, provenance, canonicality_state
        )
        VALUES ($1, 'base-mainnet', '0xbase-start', 17571485,
                '{"adapter":"ens_v1_unwrapped_authority"}'::jsonb, 'canonical')
        "#,
    )
    .bind(Uuid::from_u128(0x999))
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_out_of_scope_event_referencing_scoped_identity(
    pool: &PgPool,
    ids: &FixtureIds,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity, namespace, logical_name_id, resource_id, event_kind,
            source_family, manifest_version, source_manifest_id, chain_id, block_number,
            block_hash, raw_fact_ref, derivation_kind, canonicality_state
        )
        VALUES ('out-of-range-anchor', 'basenames', $1, $2, 'RecordChanged',
                'basenames_base_registry', 1, 1, 'base-mainnet', $3,
                '0xafter-anchor', '{}'::jsonb, 'ens_v1_unwrapped_authority', 'canonical')
        "#,
    )
    .bind(ids.logical_name_id)
    .bind(ids.resource_id)
    .bind(BASE_NORMALIZED_REDERIVE_REPLAY_TARGET_BLOCK + 1)
    .execute(pool)
    .await?;
    Ok(())
}

async fn runtime_named_pool(database_name: &str, application_name: &str) -> Result<PgPool> {
    let options = PgConnectOptions::from_str(&database_url_from_env())?
        .database(database_name)
        .application_name(application_name);
    PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .context("failed to connect named runtime test pool")
}

async fn count_table(pool: &PgPool, table: &str) -> Result<i64> {
    let sql = format!("SELECT COUNT(*)::BIGINT FROM {table}");
    Ok(sqlx::query_scalar::<_, i64>(&sql).fetch_one(pool).await?)
}

async fn count_scalar(pool: &PgPool, sql: &str, id: Uuid) -> Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(sql)
        .bind(id)
        .fetch_one(pool)
        .await?)
}

async fn count_text_scalar(pool: &PgPool, sql: &str, value: &str) -> Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(sql)
        .bind(value)
        .fetch_one(pool)
        .await?)
}
