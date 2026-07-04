use std::{str::FromStr, time::Duration};

use anyhow::{Context, Result};
use bigname_test_support::{TestDatabase, TestDatabaseConfig, database_url_from_env};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::time::timeout;
use uuid::Uuid;

use super::*;

const DEPLOYMENT_PROFILE: &str = "mainnet";
const FIXTURE_REPLAY_TARGET_BLOCK: i64 = 46_954_147;
const FIXTURE_OUT_OF_RANGE_BLOCK: i64 = FIXTURE_REPLAY_TARGET_BLOCK + 100;

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

    let plan =
        load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE, None).await?;
    let explicit_target_plan = load_base_normalized_rederive_plan(
        database.pool(),
        DEPLOYMENT_PROFILE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
    )
    .await?;

    assert_eq!(plan.replay_target_block, FIXTURE_REPLAY_TARGET_BLOCK);
    assert_eq!(plan.max_affected_block, Some(FIXTURE_REPLAY_TARGET_BLOCK));
    assert_eq!(
        plan.replay_target_floor_block,
        Some(FIXTURE_REPLAY_TARGET_BLOCK)
    );
    assert_eq!(explicit_target_plan.counts, plan.counts);
    assert_eq!(plan.counts.normalized_events, 6);
    assert_eq!(plan.counts.resources, 1);
    assert_eq!(plan.counts.token_lineages, 1);
    assert_eq!(plan.counts.name_surfaces, 1);
    assert_eq!(plan.counts.surface_bindings, 1);
    assert_eq!(plan.counts.name_current, 1);
    assert_eq!(plan.counts.address_names_current, 1);
    assert_eq!(plan.counts.children_current, 1);
    assert_eq!(plan.counts.permissions_current, 1);
    assert_eq!(plan.counts.record_inventory_current, 1);
    assert_eq!(plan.counts.projection_normalized_event_changes, 6);
    assert_eq!(plan.counts.current_projection_replay_status, 7);
    assert_eq!(plan.counts.replay_cursor_rows, 2);
    assert_eq!(plan.counts.adapter_checkpoint_rows, 6);
    assert_eq!(plan.counts.adapter_checkpoint_item_rows, 6);
    assert_eq!(plan.cursor_census.raw_fact_replay_cursor_rows, 1);
    assert_eq!(
        plan.cursor_census
            .post_replay_live_adapter_backlog_cursor_rows,
        1
    );
    assert_eq!(plan.raw_fact_completeness.log_derived_event_count, 2);
    assert_eq!(plan.raw_fact_completeness.boundary_event_count, 4);
    assert!(plan.raw_fact_completeness.is_complete_for_rerun());
    assert_eq!(
        plan.derivation_kind_census
            .iter()
            .map(|census| {
                (
                    census.derivation_kind.as_str(),
                    census.source_family.as_str(),
                    census.rederivable,
                    census.row_count,
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (
                "ens_v1_registry_resolver_changed",
                "basenames_base_registry",
                true,
                1
            ),
            ("ens_v1_reverse_claim", "basenames_base_primary", true, 1),
            (
                "ens_v1_subregistry_changed",
                "basenames_base_registry",
                true,
                1
            ),
            (
                "ens_v1_unwrapped_authority",
                "basenames_base_registry",
                true,
                3
            ),
            (
                "ens_v1_unwrapped_authority",
                "basenames_l1_compat",
                false,
                1
            ),
            (
                "raw_log_preimage_observation",
                "basenames_l1_compat",
                false,
                1
            ),
        ]
    );
    assert_eq!(
        plan.derivation_kind_census
            .iter()
            .filter(|census| !census.rederivable)
            .map(|census| census.row_count)
            .sum::<i64>(),
        2
    );
    assert_eq!(ids.logical_name_id, "basenames:alice.base.eth");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn execute_deletes_fk_safe_scope_and_resets_replay() -> Result<()> {
    let database = test_database().await?;
    let ids = seed_rederive_fixture(database.pool()).await?;
    let expected = load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE, None)
        .await?
        .counts;

    let outcome = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
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
        4
    );
    assert_eq!(count_table(database.pool(), "normalized_events").await?, 4);
    assert_eq!(
        count_text_table(
            database.pool(),
            "normalized_events",
            "event_identity",
            "null-source-boundary",
        )
        .await?,
        0
    );
    assert_eq!(
        count_text_table(
            database.pool(),
            "normalized_events",
            "event_identity",
            "preimage-observation",
        )
        .await?,
        1
    );
    assert_eq!(
        count_text_table(
            database.pool(),
            "normalized_events",
            "event_identity",
            "unsupported-source-family-authority",
        )
        .await?,
        1
    );

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
            FIXTURE_REPLAY_TARGET_BLOCK
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
    assert_eq!(
        count_text_table(
            database.pool(),
            "normalized_replay_cursors",
            "cursor_kind",
            BASE_NORMALIZED_REDERIVE_BACKLOG_CURSOR_KIND,
        )
        .await?,
        0
    );
    assert_eq!(
        count_affected_projection_replay_status(database.pool()).await?,
        0
    );
    assert_eq!(
        count_table(database.pool(), "current_projection_replay_status").await?,
        0
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn execute_refuses_unverified_deployment_profile_before_delete() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;

    let error = execute_base_normalized_rederive_drop(
        database.pool(),
        "mainnett",
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected,
    )
    .await
    .expect_err("mistyped deployment profile must fail before global Base delete");
    assert!(format!("{error:?}").contains("is not verified for the global Base delete"));
    assert_eq!(
        count_text_table(
            database.pool(),
            "normalized_events",
            "event_identity",
            "scoped-log",
        )
        .await?,
        1
    );
    assert_eq!(
        count_affected_projection_replay_status(database.pool()).await?,
        7
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

    let error = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected,
    )
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

    let error = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected,
    )
    .await
    .expect_err("runtime shared advisory lock must block execution");
    assert!(format!("{error:?}").contains("advisory lock is already held"));

    drop(runtime_guard);
    runtime_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn execute_runtime_session_check_uses_held_transaction_connection() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;
    let tight_pool = single_connection_pool(database.database_name()).await?;

    let outcome = timeout(
        Duration::from_secs(5),
        execute_base_normalized_rederive_drop(
            &tight_pool,
            DEPLOYMENT_PROFILE,
            Some(FIXTURE_REPLAY_TARGET_BLOCK),
            expected,
        ),
    )
    .await
    .expect(
        "single-connection execute timed out; runtime-session check likely acquired from pool",
    )?;
    assert_eq!(outcome.deleted.current_projection_replay_status, 7);

    tight_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn writer_guard_refuses_single_connection_pool() -> Result<()> {
    let error = crate::connect_with_base_normalized_rederive_writer_guard(
        &crate::DatabaseConfig {
            database_url: Some("postgres://bigname:bigname@127.0.0.1:1/bigname".to_owned()),
            max_connections: 1,
        },
        "bigname-indexer",
    )
    .await
    .expect_err("single-connection guarded writer pools must fail before connecting");

    assert!(format!("{error:?}").contains("requires at least 2 database connections"));
    Ok(())
}

#[tokio::test]
async fn execute_refuses_count_divergence_from_reviewed_census() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE, None)
        .await?
        .counts;
    seed_extra_scoped_resource(database.pool()).await?;

    let error = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        BaseNormalizedRederiveExpectedCounts { counts: expected },
    )
    .await
    .expect_err("count divergence must block execution");
    assert!(format!("{error:?}").contains("count divergence"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn execute_refuses_missing_reviewed_replay_target() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;

    let error =
        execute_base_normalized_rederive_drop(database.pool(), DEPLOYMENT_PROFILE, None, expected)
            .await
            .expect_err("execute must require a reviewed replay target block");
    assert!(format!("{error:?}").contains("requires reviewed replay target block"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn execute_refuses_remaining_normalized_event_identity_anchors() -> Result<()> {
    let database = test_database().await?;
    let ids = seed_rederive_fixture(database.pool()).await?;
    seed_out_of_scope_event_referencing_scoped_identity(database.pool(), &ids).await?;
    let expected = reviewed_counts(database.pool()).await?;

    let error = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected,
    )
    .await
    .expect_err("remaining normalized-event identity anchor must block execution");
    assert!(format!("{error:?}").contains("remaining_events_referencing_identity=1"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn dry_run_defaults_replay_target_to_canonical_raw_log_head() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    seed_retained_raw_logs_around_fixture_target(database.pool()).await?;

    let plan =
        load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE, None).await?;

    assert_eq!(count_table(database.pool(), "raw_logs").await?, 4);
    assert_eq!(plan.replay_target_block, FIXTURE_REPLAY_TARGET_BLOCK + 10);
    assert_eq!(plan.max_affected_block, Some(FIXTURE_REPLAY_TARGET_BLOCK));
    assert_eq!(
        plan.replay_target_floor_block,
        Some(FIXTURE_REPLAY_TARGET_BLOCK)
    );
    assert_eq!(
        plan.raw_fact_completeness.canonical_raw_log_min_block,
        Some(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK)
    );
    assert_eq!(
        plan.raw_fact_completeness.canonical_raw_log_max_block,
        Some(FIXTURE_REPLAY_TARGET_BLOCK + 10)
    );
    assert!(plan.raw_fact_completeness.is_complete_for_rerun());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn dry_run_validates_requested_target_range() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;

    let above_head = load_base_normalized_rederive_plan(
        database.pool(),
        DEPLOYMENT_PROFILE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK + 1),
    )
    .await
    .expect_err("requested replay target above the actual raw-log head must fail");
    assert!(format!("{above_head:?}").contains("must not exceed canonical raw-log head"));

    seed_retained_raw_logs_around_fixture_target(database.pool()).await?;
    mark_raw_replay_cursor_completed_from_closure(database.pool()).await?;

    let below_max_affected = load_base_normalized_rederive_plan(
        database.pool(),
        DEPLOYMENT_PROFILE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK - 1),
    )
    .await
    .expect_err("requested replay target below affected rows must fail");
    assert!(
        format!("{below_max_affected:?}").contains("is before max affected normalized-event block")
    );

    let plan = load_base_normalized_rederive_plan(
        database.pool(),
        DEPLOYMENT_PROFILE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
    )
    .await?;

    assert_eq!(plan.replay_target_block, FIXTURE_REPLAY_TARGET_BLOCK);
    assert_eq!(plan.max_affected_block, Some(FIXTURE_REPLAY_TARGET_BLOCK));
    assert_eq!(
        plan.replay_target_floor_block,
        Some(FIXTURE_REPLAY_TARGET_BLOCK)
    );
    assert_eq!(
        plan.raw_fact_completeness.canonical_raw_log_head_block,
        Some(FIXTURE_REPLAY_TARGET_BLOCK + 10)
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

    let error = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected,
    )
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
    execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected,
    )
    .await?;

    let second_plan =
        load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE, None).await?;
    assert_eq!(second_plan.counts.normalized_events, 0);
    assert_eq!(second_plan.counts.resources, 0);
    assert_eq!(second_plan.counts.replay_cursor_rows, 1);
    assert_eq!(second_plan.max_affected_block, None);
    assert_eq!(
        second_plan.replay_target_floor_block,
        Some(FIXTURE_REPLAY_TARGET_BLOCK)
    );
    let shrink_error = load_base_normalized_rederive_plan(
        database.pool(),
        DEPLOYMENT_PROFILE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK - 1),
    )
    .await
    .expect_err("post-drop rerun must not shrink the replay target below the prior reset target");
    assert!(format!("{shrink_error:?}").contains("is before max required replay target block"));

    let second = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        BaseNormalizedRederiveExpectedCounts {
            counts: second_plan.counts.clone(),
        },
    )
    .await?;
    assert_eq!(second.deleted.normalized_events, 0);
    assert_eq!(second.deleted.resources, 0);
    assert_eq!(second.deleted.replay_cursor_rows, 1);

    seed_partially_rederived_scoped_event(database.pool()).await?;
    let partial_plan =
        load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE, None).await?;
    assert_eq!(
        partial_plan.max_affected_block,
        Some(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK + 1)
    );
    assert_eq!(
        partial_plan.replay_target_floor_block,
        Some(FIXTURE_REPLAY_TARGET_BLOCK)
    );
    let partial_shrink_error = load_base_normalized_rederive_plan(
        database.pool(),
        DEPLOYMENT_PROFILE,
        Some(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK + 1),
    )
    .await
    .expect_err("partial post-drop rerun must not shrink below the prior reset target");
    assert!(
        format!("{partial_shrink_error:?}").contains("is before max required replay target block")
    );

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
        counts: load_base_normalized_rederive_plan(pool, DEPLOYMENT_PROFILE, None)
            .await?
            .counts,
    })
}

async fn seed_manifests(pool: &PgPool) -> Result<()> {
    for (manifest_id, source_family) in [
        (1, "basenames_base_primary"),
        (2, "basenames_base_registrar"),
        (3, "basenames_l1_compat"),
        (4, "basenames_base_registry"),
        (5, "basenames_base_resolver"),
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
            FIXTURE_REPLAY_TARGET_BLOCK,
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
            FIXTURE_REPLAY_TARGET_BLOCK,
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
    for (
        identity,
        source_family,
        source_manifest_id,
        block_number,
        block_hash,
        tx,
        log_index,
        derivation,
    ) in [
        (
            "scoped-log",
            "basenames_base_registry",
            Some(1_i64),
            Some(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK),
            Some("0xbase-start"),
            Some("0xtx-start"),
            Some(0_i64),
            "ens_v1_unwrapped_authority",
        ),
        (
            "scoped-boundary",
            "basenames_base_registry",
            Some(4_i64),
            Some(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK + 1),
            Some("0xbase-mid"),
            None,
            None,
            "ens_v1_unwrapped_authority",
        ),
        (
            "null-source-boundary",
            "basenames_base_registry",
            None,
            Some(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK + 1),
            Some("0xbase-mid"),
            None,
            None,
            "ens_v1_unwrapped_authority",
        ),
        (
            "reverse-claim-log",
            "basenames_base_primary",
            Some(1_i64),
            Some(FIXTURE_REPLAY_TARGET_BLOCK),
            Some("0xbase-target"),
            Some("0xtx-target"),
            Some(9_i64),
            "ens_v1_reverse_claim",
        ),
        (
            "subregistry-changed-boundary",
            "basenames_base_registry",
            Some(4_i64),
            Some(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK + 1),
            Some("0xbase-mid"),
            None,
            None,
            "ens_v1_subregistry_changed",
        ),
        (
            "registry-resolver-changed-boundary",
            "basenames_base_registry",
            Some(4_i64),
            Some(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK + 1),
            Some("0xbase-mid"),
            None,
            None,
            "ens_v1_registry_resolver_changed",
        ),
        (
            "unsupported-source-family-authority",
            "basenames_l1_compat",
            Some(3_i64),
            Some(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK + 1),
            Some("0xbase-mid"),
            None,
            None,
            "ens_v1_unwrapped_authority",
        ),
        (
            "manifest-no-block",
            "basenames_base_registry",
            Some(1_i64),
            None,
            None,
            None,
            None,
            "manifest_sync",
        ),
        (
            "out-of-range",
            "basenames_base_registry",
            Some(1_i64),
            Some(FIXTURE_OUT_OF_RANGE_BLOCK),
            Some("0xafter"),
            None,
            None,
            "ens_v1_unwrapped_authority",
        ),
        (
            "preimage-observation",
            "basenames_l1_compat",
            Some(3_i64),
            Some(FIXTURE_REPLAY_TARGET_BLOCK),
            Some("0xbase-target"),
            Some("0xtx-target"),
            Some(9_i64),
            "raw_log_preimage_observation",
        ),
    ] {
        sqlx::query(
            r#"
            INSERT INTO normalized_events (
                event_identity, namespace, event_kind, source_family, manifest_version,
                source_manifest_id, chain_id, block_number, block_hash, transaction_hash,
                log_index, raw_fact_ref, derivation_kind, canonicality_state
            )
            VALUES ($1, 'basenames', 'RecordChanged', $2, 1,
                    $3, 'base-mainnet', $4, $5, $6, $7, '{}'::jsonb, $8, 'canonical')
            "#,
        )
        .bind(identity)
        .bind(source_family)
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

async fn seed_partially_rederived_scoped_event(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity, namespace, event_kind, source_family, manifest_version,
            source_manifest_id, chain_id, block_number, block_hash, raw_fact_ref,
            derivation_kind, canonicality_state
        )
        VALUES ('partial-rederived-boundary', 'basenames', 'RecordChanged',
                'basenames_base_registry', 1, NULL, 'base-mainnet', $1, '0xbase-mid',
                '{}'::jsonb, 'ens_v1_unwrapped_authority', 'canonical')
        "#,
    )
    .bind(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK + 1)
    .execute(pool)
    .await
    .context("failed to seed partially rederived scoped event")?;
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
    sqlx::query(
        r#"
        INSERT INTO normalized_replay_cursors (
            deployment_profile, chain_id, cursor_kind, range_start_block_number,
            next_block_number, target_block_number, last_completed_block_number
        )
        VALUES ($1, 'base-mainnet', 'post_replay_live_adapter_backlog', 100, 250, 300, 249)
        "#,
    )
    .bind(DEPLOYMENT_PROFILE)
    .execute(pool)
    .await?;
    for cursor_kind in [
        BASE_NORMALIZED_REDERIVE_CURSOR_KIND,
        BASE_NORMALIZED_REDERIVE_BACKLOG_CURSOR_KIND,
    ] {
        for (adapter, item_kind, item_key) in [
            (
                BASE_NORMALIZED_REDERIVE_REVERSE_CLAIM_ADAPTER,
                "reverse_claim",
                "alice",
            ),
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
                VALUES ($1, 'base-mainnet', $2,
                        $3, 'full_closure', 100, 300)
                "#,
            )
            .bind(DEPLOYMENT_PROFILE)
            .bind(cursor_kind)
            .bind(adapter)
            .execute(pool)
            .await?;
            sqlx::query(
                r#"
                INSERT INTO normalized_replay_adapter_checkpoint_items (
                    deployment_profile, chain_id, cursor_kind, adapter, checkpoint_scope,
                    item_kind, item_key
                )
                VALUES ($1, 'base-mainnet', $2,
                        $3, 'full_closure', $4, $5)
                "#,
            )
            .bind(DEPLOYMENT_PROFILE)
            .bind(cursor_kind)
            .bind(adapter)
            .bind(item_kind)
            .bind(item_key)
            .execute(pool)
            .await?;
        }
    }
    for projection in [
        "address_names_current",
        "children_current",
        "name_current",
        "permissions_current",
        "record_inventory_current",
        "resolver_current",
        "primary_names_current",
    ] {
        sqlx::query(
            r#"
            INSERT INTO current_projection_replay_status (
                projection, replay_version, completed_normalized_target_block,
                requested_key_count, upserted_row_count, deleted_row_count
            )
            VALUES ($1, 6, $2, 1, 1, 0)
            "#,
        )
        .bind(projection)
        .bind(FIXTURE_REPLAY_TARGET_BLOCK)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn mark_raw_replay_cursor_completed_from_closure(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE normalized_replay_cursors
        SET range_start_block_number = $2,
            next_block_number = $3 + 1,
            target_block_number = $3,
            last_completed_block_number = $3
        WHERE deployment_profile = $1
          AND chain_id = 'base-mainnet'
          AND cursor_kind = 'raw_fact_normalized_events'
        "#,
    )
    .bind(DEPLOYMENT_PROFILE)
    .bind(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK)
    .bind(FIXTURE_REPLAY_TARGET_BLOCK + 10)
    .execute(pool)
    .await
    .context("failed to mark raw replay cursor completed from closure")?;
    Ok(())
}

async fn seed_retained_raw_logs_around_fixture_target(pool: &PgPool) -> Result<()> {
    for (block_hash, block_number, tx) in [
        (
            "0xbase-before-retained",
            BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK - 10,
            "0xtx-before-retained",
        ),
        (
            "0xbase-after-retained",
            FIXTURE_REPLAY_TARGET_BLOCK + 10,
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
    .bind(FIXTURE_OUT_OF_RANGE_BLOCK)
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

async fn single_connection_pool(database_name: &str) -> Result<PgPool> {
    let options = PgConnectOptions::from_str(&database_url_from_env())?.database(database_name);
    PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .context("failed to connect single-connection test pool")
}

async fn count_table(pool: &PgPool, table: &str) -> Result<i64> {
    let sql = format!("SELECT COUNT(*)::BIGINT FROM {table}");
    Ok(sqlx::query_scalar::<_, i64>(&sql).fetch_one(pool).await?)
}

async fn count_affected_projection_replay_status(pool: &PgPool) -> Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM current_projection_replay_status
        WHERE projection = ANY($1::TEXT[])
        "#,
    )
    .bind(current_projection_replay_status_projections())
    .fetch_one(pool)
    .await?)
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

async fn count_text_table(pool: &PgPool, table: &str, column: &str, value: &str) -> Result<i64> {
    let sql = format!("SELECT COUNT(*)::BIGINT FROM {table} WHERE {column} = $1");
    Ok(sqlx::query_scalar::<_, i64>(&sql)
        .bind(value)
        .fetch_one(pool)
        .await?)
}
