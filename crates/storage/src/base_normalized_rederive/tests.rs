use std::{collections::BTreeSet, str::FromStr, time::Duration};

use anyhow::{Context, Result};
use bigname_test_support::{TestDatabase, TestDatabaseConfig, database_url_from_env};
use sqlx::{
    ConnectOptions, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::time::timeout;
use uuid::Uuid;

use super::*;

const DEPLOYMENT_PROFILE: &str = "mainnet";
const RUN_ID: &str = "base-rederive-fixture-run";
const RESUME_RUN_ID: &str = "base-rederive-resume-run";
const SECOND_RUN_ID: &str = "base-rederive-second-run";
const FIXTURE_BATCH_SIZE: i64 = 2;
const FIXTURE_REPLAY_TARGET_BLOCK: i64 = 46_954_147;
const FIXTURE_OUT_OF_RANGE_BLOCK: i64 = FIXTURE_REPLAY_TARGET_BLOCK + 100;

struct FixtureIds {
    token_lineage_id: Uuid,
    resource_id: Uuid,
    surface_binding_id: Uuid,
    logical_name_id: &'static str,
}

#[test]
fn delete_predicate_pairs_match_scope_rule_pairs() {
    assert_eq!(scope_rule_pair_set(), delete_predicate_pair_set());
}

#[test]
fn replay_active_guard_sql_stays_pair_granularity() {
    let sql = guards::inactive_delete_scope_pairs_sql();
    assert!(sql.contains("scope_rule_pairs"));
    assert!(sql.contains("WHERE EXISTS"));
    assert!(sql.contains("WHERE NOT EXISTS"));
    assert!(sql.contains("LIMIT 1"));
    assert!(sql.contains("uncovered_basenames_registry_boundary_pairs"));
    assert!(sql.contains("uncovered_stored_family_boundary_pairs"));
    assert!(sql.contains("closure_boundary_rederive_families"));
    assert!(sql.contains("boundary_rederive_source_family"));
    assert!(sql.contains("active_targets"));
    assert!(sql.contains("ordered_active_targets"));
    assert!(sql.contains("covered_replay_pairs"));
    assert!(sql.contains("prior_max_to_block"));
    assert!(sql.contains("raw_fact_ref ->> 'kind' IS NOT DISTINCT FROM 'raw_block'"));
    assert!(sql.contains("covered.source_family = pair.boundary_rederive_source_family"));
    assert!(sql.contains("$8::TEXT[]"));
    assert!(!sql.contains("delete_scope_rows"));
    assert!(!sql.contains("JOIN normalized_events event"));
    assert!(!sql.contains("SELECT DISTINCT"));
    assert!(!sql.contains("covered_replay_adapters"));
    assert!(!sql.contains("normalized_event_id"));
    assert!(!sql.contains("raw_logs"));
    assert!(!sql.contains("watched_targets"));
    assert!(!sql.contains("manifest_declared_targets"));
}

#[test]
fn orphaned_emitter_guard_sql_is_bounded_and_uses_active_target_arrays() {
    let sql = guards::orphaned_delete_scope_emitters_sql();
    assert!(sql.contains("active_targets"));
    assert!(sql.contains("JOIN raw_logs raw_log"));
    assert!(sql.contains("NOT EXISTS"));
    assert!(sql.contains("LIMIT 10"));
    assert!(sql.contains("$8::TEXT[]"));
    assert!(sql.contains("target.from_block <= event.block_number"));
    assert!(sql.contains("target.to_block >= event.block_number"));
    assert!(!sql.contains("normalized_event_id"));
    assert!(!sql.contains("watched_targets"));
    assert!(!sql.contains("manifest_declared_targets"));
}

#[test]
fn base_rederive_scope_index_migration_is_no_transaction() {
    for version in [
        20260704130000,
        20260704130100,
        20260704130200,
        20260704130300,
        20260704130400,
        20260704130500,
        20260704130600,
    ] {
        let migration = crate::MIGRATOR
            .iter()
            .find(|migration| migration.version == version)
            .expect("base rederive scope index migration is registered");
        assert!(
            migration.no_tx,
            "migration {version} must not use a DDL transaction"
        );
    }
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
    assert_eq!(plan.active_replay_target_snapshot.len(), 5);
    assert_eq!(plan.active_manifest_snapshot.len(), 4);
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
    assert!(plan.raw_fact_safety_checks_deferred);
    assert!(plan.raw_fact_range_proof.is_empty());
    assert_eq!(
        plan.raw_fact_completeness.canonical_raw_log_head_block,
        Some(FIXTURE_REPLAY_TARGET_BLOCK)
    );
    assert!(!plan.raw_fact_completeness.is_complete_for_rerun());
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
    let expected_plan =
        load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE, None).await?;
    let expected = expected_from_plan(&expected_plan)?;

    let outcome = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RUN_ID,
        FIXTURE_BATCH_SIZE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected.clone(),
    )
    .await?;

    assert_eq!(outcome.deleted, expected.counts);
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
        SELECT
            range_start_block_number,
            next_block_number,
            target_block_number
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
async fn batched_execute_resumes_without_resetting_cursors_mid_run() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;

    let partial = execute_base_normalized_rederive_drop_with_batch_limit(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RESUME_RUN_ID,
        1,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected.clone(),
        3,
    )
    .await?;

    assert_eq!(partial.deleted.address_names_current, 1);
    assert_eq!(partial.deleted.name_current, 1);
    assert_eq!(partial.deleted.children_current, 1);
    assert_eq!(partial.deleted.normalized_events, 0);
    assert_eq!(
        count_affected_projection_replay_status(database.pool()).await?,
        7
    );
    assert_eq!(
        count_text_table(
            database.pool(),
            "normalized_replay_cursors",
            "cursor_kind",
            BASE_NORMALIZED_REDERIVE_BACKLOG_CURSOR_KIND,
        )
        .await?,
        1
    );
    assert_eq!(
        load_run_status(database.pool(), RESUME_RUN_ID).await?.0,
        "running"
    );
    assert_no_dangling_refs(database.pool()).await?;

    let completed = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RESUME_RUN_ID,
        1,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected.clone(),
    )
    .await?;

    assert_eq!(completed.deleted, expected.counts);
    assert_eq!(
        load_run_status(database.pool(), RESUME_RUN_ID).await?,
        ("completed".to_owned(), "completed".to_owned())
    );
    assert_eq!(
        count_affected_projection_replay_status(database.pool()).await?,
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

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn batched_resume_refuses_census_mismatch() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;

    execute_base_normalized_rederive_drop_with_batch_limit(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RESUME_RUN_ID,
        1,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected.clone(),
        1,
    )
    .await?;
    seed_extra_scoped_resource(database.pool()).await?;

    let error = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RESUME_RUN_ID,
        1,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected,
    )
    .await
    .expect_err("resume must stop when live+deleted census no longer matches review");
    assert!(format!("{error:?}").contains("resume census mismatch for resources"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn batched_resume_reruns_replay_active_guard_before_next_delete() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;

    execute_base_normalized_rederive_drop_with_batch_limit(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RESUME_RUN_ID,
        1,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected.clone(),
        1,
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE manifest_versions
        SET rollout_status = 'deprecated'
        WHERE chain = 'base-mainnet'
          AND source_family = 'basenames_base_primary'
        "#,
    )
    .execute(database.pool())
    .await?;

    let error = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RESUME_RUN_ID,
        1,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected,
    )
    .await
    .expect_err("resume must re-run replay-active coverage before deleting another batch");
    assert!(
        format!("{error:?}").contains("active replay target snapshot changed"),
        "unexpected error: {error:?}"
    );
    assert_eq!(count_table(database.pool(), "name_current").await?, 1);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn batched_resume_replay_active_guard_survives_after_event_delete_step() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;

    execute_base_normalized_rederive_drop_with_batch_limit(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RESUME_RUN_ID,
        100,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected.clone(),
        7,
    )
    .await?;
    assert_eq!(
        count_text_table(
            database.pool(),
            "normalized_events",
            "event_identity",
            "scoped-log",
        )
        .await?,
        0
    );
    sqlx::query(
        r#"
        UPDATE manifest_versions
        SET rollout_status = 'deprecated'
        WHERE chain = 'base-mainnet'
          AND source_family = 'basenames_base_primary'
        "#,
    )
    .execute(database.pool())
    .await?;

    let error = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RESUME_RUN_ID,
        100,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected,
    )
    .await
    .expect_err("resume must still detect replay target drift after scoped events are gone");
    assert!(
        format!("{error:?}").contains("active replay target snapshot changed"),
        "unexpected error: {error:?}"
    );
    assert_eq!(count_table(database.pool(), "surface_bindings").await?, 2);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn execute_refuses_active_replay_target_snapshot_drift_from_review() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;
    seed_extra_active_replay_target(database.pool(), 1).await?;

    let error = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RUN_ID,
        FIXTURE_BATCH_SIZE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected,
    )
    .await
    .expect_err("active replay target drift from reviewed dry-run must block execute");
    assert!(
        format!("{error:?}").contains("active replay target snapshot divergence"),
        "unexpected error: {error:?}"
    );
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

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn batched_resume_raw_fact_proof_survives_after_event_delete_step() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;

    execute_base_normalized_rederive_drop_with_batch_limit(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RESUME_RUN_ID,
        100,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected.clone(),
        7,
    )
    .await?;
    assert_eq!(count_table(database.pool(), "normalized_events").await?, 4);
    sqlx::query(
        "UPDATE raw_logs SET data = decode('abcd', 'hex') WHERE block_hash = '0xbase-target'",
    )
    .execute(database.pool())
    .await?;

    let error = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RESUME_RUN_ID,
        100,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected,
    )
    .await
    .expect_err("resume must still detect raw-fact drift after scoped events are gone");
    assert!(
        format!("{error:?}").contains("raw-fact range proof changed"),
        "unexpected error: {error:?}"
    );
    assert_eq!(count_table(database.pool(), "surface_bindings").await?, 2);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn batched_resume_refuses_legacy_guard_snapshot_drift_before_event_delete() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;
    let run_id = "base-rederive-legacy-drift-run";

    execute_base_normalized_rederive_drop_with_batch_limit(
        database.pool(),
        DEPLOYMENT_PROFILE,
        run_id,
        1,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected.clone(),
        1,
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE base_normalized_rederive_runs
        SET plan_snapshot = plan_snapshot - 'active_replay_target_snapshot' - 'raw_fact_range_proof'
        WHERE run_id = $1
        "#,
    )
    .bind(run_id)
    .execute(database.pool())
    .await?;
    seed_extra_active_replay_target(database.pool(), 1).await?;

    let error = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        run_id,
        1,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected,
    )
    .await
    .expect_err("legacy missing snapshot must not adopt active-target drift");
    assert!(
        format!("{error:?}").contains("legacy active replay target snapshot divergence"),
        "unexpected error: {error:?}"
    );
    assert_eq!(count_table(database.pool(), "name_current").await?, 1);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn batched_resume_upgrades_legacy_guard_snapshot_before_event_delete() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;

    execute_base_normalized_rederive_drop_with_batch_limit(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RESUME_RUN_ID,
        1,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected.clone(),
        1,
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE base_normalized_rederive_runs
        SET plan_snapshot = plan_snapshot - 'active_replay_target_snapshot' - 'raw_fact_range_proof'
        WHERE run_id = $1
        "#,
    )
    .bind(RESUME_RUN_ID)
    .execute(database.pool())
    .await?;

    let completed = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RESUME_RUN_ID,
        1,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected.clone(),
    )
    .await?;
    assert_eq!(completed.deleted, expected.counts);
    let persisted_upgrade = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT plan_snapshot ? 'active_replay_target_snapshot'
           AND plan_snapshot ? 'raw_fact_range_proof'
        FROM base_normalized_rederive_runs
        WHERE run_id = $1
        "#,
    )
    .bind(RESUME_RUN_ID)
    .fetch_one(database.pool())
    .await?;
    assert!(persisted_upgrade);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn batched_resume_reruns_raw_fact_completeness_before_next_delete() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;

    execute_base_normalized_rederive_drop_with_batch_limit(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RESUME_RUN_ID,
        1,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected.clone(),
        1,
    )
    .await?;
    sqlx::query(
        "UPDATE raw_logs SET transaction_hash = '0xtx-target-missing' WHERE block_hash = '0xbase-target'",
    )
        .execute(database.pool())
        .await?;

    let error = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RESUME_RUN_ID,
        1,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected,
    )
    .await
    .expect_err("resume must re-run raw-fact completeness before deleting another batch");
    assert!(
        format!("{error:?}").contains("raw-fact range proof changed"),
        "unexpected error: {error:?}"
    );
    assert_eq!(count_table(database.pool(), "name_current").await?, 1);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn batch_size_limits_delete_batches_not_final_reset() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;

    execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RUN_ID,
        1,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected,
    )
    .await?;

    assert_eq!(max_non_reset_batch_rows(database.pool(), RUN_ID).await?, 1);
    assert!(count_run_batches(database.pool(), RUN_ID).await? > 10);
    assert_eq!(
        final_reset_batch_rows(database.pool(), RUN_ID).await?,
        7 + 2 + 6 + 6
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
        RUN_ID,
        FIXTURE_BATCH_SIZE,
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
        RUN_ID,
        FIXTURE_BATCH_SIZE,
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
        RUN_ID,
        FIXTURE_BATCH_SIZE,
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
async fn execute_refuses_inactive_delete_scope_family_before_delete() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;
    sqlx::query(
        r#"
        UPDATE manifest_versions
        SET rollout_status = 'deprecated'
        WHERE chain = 'base-mainnet'
          AND source_family = 'basenames_base_primary'
        "#,
    )
    .execute(database.pool())
    .await?;

    let error = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RUN_ID,
        FIXTURE_BATCH_SIZE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected,
    )
    .await
    .expect_err("inactive current replay manifest family must stop before delete");
    assert!(
        format!("{error:?}").contains("current full-closure replay will not re-emit"),
        "unexpected error: {error:?}"
    );
    assert!(
        format!("{error:?}").contains("ens_v1_reverse_claim/basenames_base_primary"),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        count_text_table(
            database.pool(),
            "normalized_events",
            "event_identity",
            "reverse-claim-log",
        )
        .await?,
        1
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn dry_run_refuses_inactive_delete_scope_pair() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    sqlx::query(
        r#"
        UPDATE manifest_versions
        SET rollout_status = 'deprecated'
        WHERE chain = 'base-mainnet'
          AND source_family = 'basenames_base_primary'
        "#,
    )
    .execute(database.pool())
    .await?;

    let error = load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE, None)
        .await
        .expect_err("inactive current replay manifest family must stop dry-run");
    assert!(
        format!("{error:?}").contains("current full-closure replay will not re-emit"),
        "unexpected error: {error:?}"
    );
    assert!(
        format!("{error:?}").contains("ens_v1_reverse_claim/basenames_base_primary"),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        count_text_table(
            database.pool(),
            "normalized_events",
            "event_identity",
            "reverse-claim-log",
        )
        .await?,
        1
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn dry_run_accepts_adapter_closure_boundary_pair_without_source_family_target() -> Result<()>
{
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    seed_ens_v1_registry_l1_boundary_event(database.pool()).await?;

    let plan =
        load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE, None).await?;
    assert!(plan.active_replay_target_snapshot.iter().any(|target| {
        target.replay_adapter == "ens_v1_unwrapped_authority"
            && target.source_family == "basenames_base_registry"
    }));
    assert!(!plan.active_replay_target_snapshot.iter().any(|target| {
        target.replay_adapter == "ens_v1_unwrapped_authority"
            && target.source_family == "ens_v1_registry_l1"
    }));
    let pair = plan
        .derivation_kind_census
        .iter()
        .find(|census| {
            census.derivation_kind == "ens_v1_unwrapped_authority"
                && census.source_family == "ens_v1_registry_l1"
        })
        .context("ENSv1 registry boundary pair should be reported in the census")?;
    assert!(pair.rederivable);
    assert_eq!(pair.row_count, 1);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn dry_run_refuses_missing_raw_block_kind_without_source_family_target() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    seed_ens_v1_registry_l1_boundary_event_missing_kind(database.pool()).await?;

    let error = load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE, None)
        .await
        .expect_err("boundary-shaped rows without explicit raw_block facts need target coverage");
    assert!(
        format!("{error:?}").contains("current full-closure replay will not re-emit"),
        "unexpected error: {error:?}"
    );
    assert!(
        format!("{error:?}").contains("ens_v1_unwrapped_authority/ens_v1_registry_l1"),
        "unexpected error: {error:?}"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn dry_run_refuses_registry_boundary_with_only_resolver_target() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    move_fixture_basenames_registry_rows_out_of_delete_scope(database.pool()).await?;
    deactivate_base_source_family(database.pool(), "basenames_base_registry").await?;
    deactivate_base_source_family(database.pool(), "basenames_base_registrar").await?;
    seed_ens_v1_registry_l1_boundary_event(database.pool()).await?;

    let error = load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE, None)
        .await
        .expect_err("registry boundary rows need registry-family closure coverage");
    assert!(
        format!("{error:?}").contains("current full-closure replay will not re-emit"),
        "unexpected error: {error:?}"
    );
    assert!(
        format!("{error:?}").contains("ens_v1_unwrapped_authority/ens_v1_registry_l1"),
        "unexpected error: {error:?}"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn dry_run_refuses_log_derived_pair_without_source_family_target() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    seed_ens_v1_registry_l1_log_event(database.pool()).await?;

    let error = load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE, None)
        .await
        .expect_err("log-derived ENSv1 registry rows on Base still need target coverage");
    assert!(
        format!("{error:?}").contains("current full-closure replay will not re-emit"),
        "unexpected error: {error:?}"
    );
    assert!(
        format!("{error:?}").contains("ens_v1_unwrapped_authority/ens_v1_registry_l1"),
        "unexpected error: {error:?}"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn replay_active_guard_refuses_mixed_boundary_pair_without_rederive_family_target()
-> Result<()> {
    let database = test_database().await?;
    seed_ens_v1_registry_l1_boundary_event(database.pool()).await?;
    seed_ens_v1_registry_l1_boundary_event_missing_kind(database.pool()).await?;

    let error = guards::ensure_delete_scope_replay_active(
        database.pool(),
        FIXTURE_REPLAY_TARGET_BLOCK,
        &[BaseNormalizedRederiveReplayTargetSnapshot {
            replay_adapter: "ens_v1_unwrapped_authority".to_owned(),
            source_family: "ens_v1_registry_l1".to_owned(),
            address: "0x0000000000000000000000000000000000000001".to_owned(),
            from_block: BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
            to_block: FIXTURE_REPLAY_TARGET_BLOCK,
        }],
    )
    .await
    .expect_err("mixed boundary pairs still need rederive-family target coverage");
    assert!(
        format!("{error:?}").contains("current full-closure replay will not re-emit"),
        "unexpected error: {error:?}"
    );
    assert!(
        format!("{error:?}").contains("ens_v1_unwrapped_authority/ens_v1_registry_l1"),
        "unexpected error: {error:?}"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn dry_run_refuses_pair_when_replay_target_does_not_cover_full_range() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    sqlx::query(
        r#"
        UPDATE contract_instance_addresses cia
        SET active_from_block_number = $1
        FROM manifest_versions mv
        WHERE mv.manifest_id = cia.source_manifest_id
          AND mv.chain = 'base-mainnet'
          AND mv.source_family = 'basenames_base_primary'
        "#,
    )
    .bind(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK + 1)
    .execute(database.pool())
    .await?;

    let error = load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE, None)
        .await
        .expect_err(
            "overlapping replay target that does not cover the reviewed range must stop dry-run",
        );
    assert!(
        format!("{error:?}").contains("current full-closure replay will not re-emit"),
        "unexpected error: {error:?}"
    );
    assert!(
        format!("{error:?}").contains("ens_v1_reverse_claim/basenames_base_primary"),
        "unexpected error: {error:?}"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn dry_run_accepts_split_replay_target_ranges_when_union_covers_full_range() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let successor_address = seed_split_active_replay_target(
        database.pool(),
        4,
        BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK + 1,
        BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK + 2,
    )
    .await?;
    seed_successor_emitter_scoped_event(database.pool(), &successor_address).await?;

    let plan =
        load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE, None).await?;
    assert!(
        plan.active_replay_target_snapshot.iter().any(|target| {
            target.source_family == "basenames_base_registry"
                && target.from_block == BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK + 2
                && target.to_block == FIXTURE_REPLAY_TARGET_BLOCK
        }),
        "split successor replay target must be present in reviewed snapshot"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn dry_run_refuses_split_replay_target_ranges_with_coverage_gap() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    seed_split_active_replay_target(
        database.pool(),
        4,
        BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
        BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK + 2,
    )
    .await?;

    let error = load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE, None)
        .await
        .expect_err("gapped replay target union must stop dry-run");
    assert!(
        format!("{error:?}").contains("current full-closure replay will not re-emit"),
        "unexpected error: {error:?}"
    );
    assert!(
        format!("{error:?}").contains("basenames_base_registry"),
        "unexpected error: {error:?}"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn dry_run_refuses_orphaned_emitter_not_in_active_targets() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    sqlx::query(
        r#"
        UPDATE raw_logs
        SET emitting_address = '0x000000000000000000000000000000000000dead'
        WHERE chain_id = 'base-mainnet'
          AND transaction_hash = '0xtx-target'
          AND log_index = 9
        "#,
    )
    .execute(database.pool())
    .await?;

    let error = load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE, None)
        .await
        .expect_err("dry-run must refuse a scoped log from a non-active emitter");
    assert!(
        format!("{error:?}").contains("addresses not in the current active replay target set"),
        "unexpected error: {error:?}"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn execute_refuses_active_family_without_replay_target_before_delete() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;
    sqlx::query(
        r#"
        UPDATE contract_instance_addresses cia
        SET active_from_block_number = $1
        FROM manifest_versions mv
        WHERE mv.manifest_id = cia.source_manifest_id
          AND mv.chain = 'base-mainnet'
          AND mv.source_family = 'basenames_base_primary'
        "#,
    )
    .bind(FIXTURE_REPLAY_TARGET_BLOCK + 1)
    .execute(database.pool())
    .await?;

    let error = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RUN_ID,
        FIXTURE_BATCH_SIZE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected,
    )
    .await
    .expect_err("active manifest family without a replay target must stop before delete");
    assert!(
        format!("{error:?}").contains("current full-closure replay will not re-emit"),
        "unexpected error: {error:?}"
    );
    assert!(
        format!("{error:?}").contains("ens_v1_reverse_claim/basenames_base_primary"),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        count_text_table(
            database.pool(),
            "normalized_events",
            "event_identity",
            "reverse-claim-log",
        )
        .await?,
        1
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn dry_run_refuses_affected_rows_above_canonical_raw_log_head() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity, namespace, event_kind, source_family, manifest_version,
            source_manifest_id, chain_id, block_number, block_hash, raw_fact_ref,
            derivation_kind, canonicality_state
        )
        VALUES ('above-raw-head', 'basenames', 'RecordChanged',
                'basenames_base_registry', 1, 4, 'base-mainnet', $1,
                '0xabove-raw-head', '{}'::jsonb, 'ens_v1_unwrapped_authority',
                'canonical')
        "#,
    )
    .bind(FIXTURE_REPLAY_TARGET_BLOCK + 1)
    .execute(database.pool())
    .await?;

    let error = load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE, None)
        .await
        .expect_err("affected rows above retained raw-log head must stop dry-run");
    assert!(
        format!("{error:?}").contains("affected rows above canonical raw-log head"),
        "unexpected error: {error:?}"
    );

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
            RUN_ID,
            FIXTURE_BATCH_SIZE,
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
async fn writer_guard_refuses_incomplete_rederive_run() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;
    let _partial = super::batch::execute_base_normalized_rederive_drop_with_batch_limit(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RESUME_RUN_ID,
        FIXTURE_BATCH_SIZE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected,
        1,
    )
    .await?;
    let config = database_config(database.database_name())?;

    let error =
        crate::connect_with_base_normalized_rederive_writer_guard(&config, "bigname-indexer")
            .await
            .expect_err("incomplete rederive run must block guarded writers");
    let error = format!("{error:?}");
    assert!(error.contains("rederive run is incomplete"), "{error}");
    assert!(error.contains(RESUME_RUN_ID), "{error}");

    sqlx::query(
        r#"
        UPDATE base_normalized_rederive_runs
        SET status = 'completed',
            current_step = 'completed',
            completed_at = now()
        WHERE run_id = $1
        "#,
    )
    .bind(RESUME_RUN_ID)
    .execute(database.pool())
    .await?;

    let (guarded_pool, guard) =
        crate::connect_with_base_normalized_rederive_writer_guard(&config, "bigname-indexer")
            .await?;
    drop(guard);
    guarded_pool.close().await;

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn aborted_rederive_run_unblocks_writers_but_cannot_resume() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;
    let _partial = super::batch::execute_base_normalized_rederive_drop_with_batch_limit(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RESUME_RUN_ID,
        FIXTURE_BATCH_SIZE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected.clone(),
        1,
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE base_normalized_rederive_runs
        SET status = 'aborted',
            current_step = 'aborted',
            updated_at = now()
        WHERE run_id = $1
        "#,
    )
    .bind(RESUME_RUN_ID)
    .execute(database.pool())
    .await?;

    let config = database_config(database.database_name())?;
    let (guarded_pool, guard) =
        crate::connect_with_base_normalized_rederive_writer_guard(&config, "bigname-indexer")
            .await?;
    drop(guard);
    guarded_pool.close().await;

    let error = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RESUME_RUN_ID,
        FIXTURE_BATCH_SIZE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected,
    )
    .await
    .expect_err("aborted rederive runs must not be resumable");
    let error = format!("{error:?}");
    assert!(error.contains("is aborted"), "{error}");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn writer_guard_rechecks_incomplete_rederive_run_after_waiting_for_lock() -> Result<()> {
    let database = test_database().await?;
    let config = database_config(database.database_name())?;
    let mut exclusive_lock_connection = database
        .pool()
        .acquire()
        .await
        .context("failed to acquire exclusive-lock test connection")?;
    sqlx::query("SELECT pg_advisory_lock(hashtextextended($1::text, 0::bigint))")
        .bind(BASE_NORMALIZED_REDERIVE_ADVISORY_LOCK_KEY)
        .execute(&mut *exclusive_lock_connection)
        .await?;

    let guard_task = tokio::spawn(async move {
        crate::connect_with_base_normalized_rederive_writer_guard(&config, "bigname-indexer").await
    });
    let mut waiter_seen = false;
    for _ in 0..50 {
        waiter_seen = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND application_name = 'bigname-indexer'
                  AND wait_event_type = 'Lock'
            )
            "#,
        )
        .fetch_one(database.pool())
        .await?;
        if waiter_seen {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        waiter_seen,
        "writer guard task did not wait on the held rederive advisory lock"
    );
    sqlx::query(
        r#"
        INSERT INTO base_normalized_rederive_runs (
            run_id, deployment_profile, chain_id, replay_target_block, batch_size,
            status, current_step, expected_counts, plan_snapshot
        )
        VALUES ($1, $2, $3, $4, 1, 'running', 'address_names_current', '{}'::jsonb, '{}'::jsonb)
        "#,
    )
    .bind("base-rederive-waiting-writer-run")
    .bind(DEPLOYMENT_PROFILE)
    .bind(BASE_NORMALIZED_REDERIVE_CHAIN_ID)
    .bind(FIXTURE_REPLAY_TARGET_BLOCK)
    .execute(database.pool())
    .await?;
    let released = sqlx::query_scalar::<_, bool>(
        "SELECT pg_advisory_unlock(hashtextextended($1::text, 0::bigint))",
    )
    .bind(BASE_NORMALIZED_REDERIVE_ADVISORY_LOCK_KEY)
    .fetch_one(&mut *exclusive_lock_connection)
    .await?;
    assert!(released);

    let result = timeout(Duration::from_secs(5), guard_task)
        .await
        .context("writer guard task did not finish after exclusive lock released")?
        .context("writer guard task panicked")?;
    let error = match result {
        Ok((guarded_pool, guard)) => {
            drop(guard);
            guarded_pool.close().await;
            anyhow::bail!("writer guard acquired lock despite incomplete rederive run")
        }
        Err(error) => format!("{error:?}"),
    };
    assert!(error.contains("rederive run is incomplete"), "{error}");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn pending_rederive_replay_requires_reviewed_manifest_snapshot() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;

    execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RUN_ID,
        FIXTURE_BATCH_SIZE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected,
    )
    .await?;

    assert_eq!(
        pending_base_normalized_rederive_replay_target(
            database.pool(),
            DEPLOYMENT_PROFILE,
            BASE_NORMALIZED_REDERIVE_CHAIN_ID,
        )
        .await?,
        Some(FIXTURE_REPLAY_TARGET_BLOCK)
    );
    ensure_base_normalized_rederive_replay_manifest_snapshot_current(
        database.pool(),
        DEPLOYMENT_PROFILE,
        BASE_NORMALIZED_REDERIVE_CHAIN_ID,
        FIXTURE_REPLAY_TARGET_BLOCK,
    )
    .await?;

    sqlx::query(
        r#"
        UPDATE manifest_versions
        SET manifest_payload = jsonb_build_object('same_targets_manifest_change', true)
        WHERE manifest_id = 1
        "#,
    )
    .execute(database.pool())
    .await?;
    let error = ensure_base_normalized_rederive_replay_manifest_snapshot_current(
        database.pool(),
        DEPLOYMENT_PROFILE,
        BASE_NORMALIZED_REDERIVE_CHAIN_ID,
        FIXTURE_REPLAY_TARGET_BLOCK,
    )
    .await
    .expect_err(
        "pending Base correction replay must reject manifest payload drift even when replay targets are unchanged",
    );
    assert!(
        format!("{error:?}").contains("active manifest snapshot changed"),
        "unexpected error: {error:?}"
    );
    sqlx::query(
        "UPDATE manifest_versions SET manifest_payload = '{}'::jsonb WHERE manifest_id = 1",
    )
    .execute(database.pool())
    .await?;
    ensure_base_normalized_rederive_replay_manifest_snapshot_current(
        database.pool(),
        DEPLOYMENT_PROFILE,
        BASE_NORMALIZED_REDERIVE_CHAIN_ID,
        FIXTURE_REPLAY_TARGET_BLOCK,
    )
    .await?;

    seed_extra_active_replay_target(database.pool(), 1).await?;
    let error = ensure_base_normalized_rederive_replay_manifest_snapshot_current(
        database.pool(),
        DEPLOYMENT_PROFILE,
        BASE_NORMALIZED_REDERIVE_CHAIN_ID,
        FIXTURE_REPLAY_TARGET_BLOCK,
    )
    .await
    .expect_err("pending Base correction replay must be pinned to reviewed manifest snapshot");
    assert!(
        format!("{error:?}").contains("replay target snapshot changed"),
        "unexpected error: {error:?}"
    );

    sqlx::query(
        r#"
        UPDATE normalized_replay_cursors
        SET next_block_number = $4 + 1,
            target_block_number = $4 + 10
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
        "#,
    )
    .bind(DEPLOYMENT_PROFILE)
    .bind(BASE_NORMALIZED_REDERIVE_CHAIN_ID)
    .bind(BASE_NORMALIZED_REDERIVE_CURSOR_KIND)
    .bind(FIXTURE_REPLAY_TARGET_BLOCK)
    .execute(database.pool())
    .await?;
    assert_eq!(
        pending_base_normalized_rederive_replay_target(
            database.pool(),
            DEPLOYMENT_PROFILE,
            BASE_NORMALIZED_REDERIVE_CHAIN_ID,
        )
        .await?,
        None,
        "a later unrelated Base replay target must not be treated as the pending correction replay"
    );

    sqlx::query(
        r#"
        UPDATE normalized_replay_cursors
        SET target_block_number = $4,
            next_block_number = $4 + 1
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
        "#,
    )
    .bind(DEPLOYMENT_PROFILE)
    .bind(BASE_NORMALIZED_REDERIVE_CHAIN_ID)
    .bind(BASE_NORMALIZED_REDERIVE_CURSOR_KIND)
    .bind(FIXTURE_REPLAY_TARGET_BLOCK)
    .execute(database.pool())
    .await?;
    assert_eq!(
        pending_base_normalized_rederive_replay_target(
            database.pool(),
            DEPLOYMENT_PROFILE,
            BASE_NORMALIZED_REDERIVE_CHAIN_ID,
        )
        .await?,
        None
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn execute_refuses_count_divergence_from_reviewed_census() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected_plan =
        load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE, None).await?;
    let expected = expected_from_plan(&expected_plan)?;
    seed_extra_scoped_resource(database.pool()).await?;

    let error = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RUN_ID,
        FIXTURE_BATCH_SIZE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected,
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

    let error = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RUN_ID,
        FIXTURE_BATCH_SIZE,
        None,
        expected,
    )
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
        RUN_ID,
        FIXTURE_BATCH_SIZE,
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
    seed_retained_raw_log_after_fixture_target(database.pool()).await?;

    let plan =
        load_base_normalized_rederive_plan(database.pool(), DEPLOYMENT_PROFILE, None).await?;

    assert_eq!(count_table(database.pool(), "raw_logs").await?, 3);
    assert_eq!(plan.replay_target_block, FIXTURE_REPLAY_TARGET_BLOCK + 10);
    assert_eq!(plan.max_affected_block, Some(FIXTURE_REPLAY_TARGET_BLOCK));
    assert_eq!(
        plan.replay_target_floor_block,
        Some(FIXTURE_REPLAY_TARGET_BLOCK)
    );
    assert!(plan.raw_fact_safety_checks_deferred);
    assert_eq!(
        plan.raw_fact_completeness.canonical_raw_log_head_block,
        Some(FIXTURE_REPLAY_TARGET_BLOCK + 10)
    );
    assert!(!plan.raw_fact_completeness.is_complete_for_rerun());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn execute_refuses_retained_raw_logs_before_replay_boundary_before_delete() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;
    seed_retained_raw_log_before_boundary(database.pool()).await?;

    let error = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RUN_ID,
        FIXTURE_BATCH_SIZE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected,
    )
    .await
    .expect_err("retained raw logs before the correction boundary must stop execution");
    assert!(
        format!("{error:?}").contains("retained canonical raw-log floor"),
        "unexpected error: {error:?}"
    );
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
    let cursor_start = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT range_start_block_number
        FROM normalized_replay_cursors
        WHERE deployment_profile = $1
          AND chain_id = 'base-mainnet'
          AND cursor_kind = 'raw_fact_normalized_events'
        "#,
    )
    .bind(DEPLOYMENT_PROFILE)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(cursor_start, 100);

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

    seed_retained_raw_log_after_fixture_target(database.pool()).await?;
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
    assert!(plan.raw_fact_safety_checks_deferred);
    assert!(!plan.raw_fact_completeness.is_complete_for_rerun());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn execute_refuses_raw_fact_completeness_gap() -> Result<()> {
    let database = test_database().await?;
    seed_rederive_fixture(database.pool()).await?;
    let expected = reviewed_counts(database.pool()).await?;
    sqlx::query(
        "UPDATE raw_logs SET transaction_hash = '0xtx-target-missing' WHERE block_hash = '0xbase-target'",
    )
        .execute(database.pool())
        .await?;

    let error = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RUN_ID,
        FIXTURE_BATCH_SIZE,
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
        RUN_ID,
        FIXTURE_BATCH_SIZE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected.clone(),
    )
    .await?;
    let completed_batch_count = count_run_batches(database.pool(), RUN_ID).await?;
    let completed_repeat = execute_base_normalized_rederive_drop(
        database.pool(),
        DEPLOYMENT_PROFILE,
        RUN_ID,
        FIXTURE_BATCH_SIZE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected.clone(),
    )
    .await?;
    assert_eq!(completed_repeat.deleted, expected.counts);
    assert_eq!(
        count_run_batches(database.pool(), RUN_ID).await?,
        completed_batch_count
    );

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
        SECOND_RUN_ID,
        FIXTURE_BATCH_SIZE,
        Some(FIXTURE_REPLAY_TARGET_BLOCK),
        expected_from_plan(&second_plan)?,
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
    let plan = load_base_normalized_rederive_plan(pool, DEPLOYMENT_PROFILE, None).await?;
    expected_from_plan(&plan)
}

fn expected_from_plan(
    plan: &BaseNormalizedRederivePlan,
) -> Result<BaseNormalizedRederiveExpectedCounts> {
    Ok(BaseNormalizedRederiveExpectedCounts {
        counts: plan.counts.clone(),
        active_replay_target_snapshot_digest: Some(base_normalized_rederive_json_digest(
            &plan.active_replay_target_snapshot,
        )?),
        active_manifest_snapshot_digest: Some(base_normalized_rederive_json_digest(
            &plan.active_manifest_snapshot,
        )?),
    })
}

fn scope_rule_pair_set() -> BTreeSet<(String, String, String)> {
    base_normalized_rederive_scope_rules()
        .iter()
        .flat_map(|rule| {
            rule.derivation_kinds
                .iter()
                .flat_map(move |derivation_kind| {
                    rule.source_families.iter().map(move |source_family| {
                        (
                            rule.adapter.to_owned(),
                            (*derivation_kind).to_owned(),
                            (*source_family).to_owned(),
                        )
                    })
                })
        })
        .collect()
}

fn delete_predicate_pair_set() -> BTreeSet<(String, String, String)> {
    let mut pairs = BTreeSet::new();
    for source_family in reverse_claim_source_families() {
        pairs.insert((
            BASE_NORMALIZED_REDERIVE_REVERSE_CLAIM_ADAPTER.to_owned(),
            reverse_claim_derivation_kind(),
            source_family,
        ));
    }
    for derivation_kind in subregistry_derivation_kinds() {
        for source_family in subregistry_source_families() {
            pairs.insert((
                BASE_NORMALIZED_REDERIVE_DISCOVERY_ADAPTER.to_owned(),
                derivation_kind.clone(),
                source_family,
            ));
        }
    }
    for source_family in unwrapped_authority_source_families() {
        pairs.insert((
            BASE_NORMALIZED_REDERIVE_ADAPTER.to_owned(),
            unwrapped_authority_derivation_kind(),
            source_family,
        ));
    }
    pairs
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
        if manifest_id == 3 {
            continue;
        }
        let contract_instance_id = Uuid::from_u128(0x9000_u128 + manifest_id as u128);
        sqlx::query(
            r#"
            INSERT INTO contract_instances (
                contract_instance_id, chain_id, contract_kind, provenance
            )
            VALUES ($1, 'base-mainnet', 'test_replay_target', '{}'::jsonb)
            "#,
        )
        .bind(contract_instance_id)
        .execute(pool)
        .await
        .with_context(|| format!("failed to seed replay target contract {manifest_id}"))?;
        let address = replay_target_address(manifest_id);
        sqlx::query(
            r#"
            INSERT INTO contract_instance_addresses (
                contract_instance_id, chain_id, address, active_from_block_number,
                source_manifest_id, provenance
            )
            VALUES ($1, 'base-mainnet', $2, $3, $4, '{}'::jsonb)
            "#,
        )
        .bind(contract_instance_id)
        .bind(address)
        .bind(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK)
        .bind(manifest_id)
        .execute(pool)
        .await
        .with_context(|| format!("failed to seed replay target address {manifest_id}"))?;
        sqlx::query(
            r#"
            INSERT INTO manifest_contract_instances (
                manifest_id, declaration_kind, declaration_name, contract_instance_id,
                declared_address, role
            )
            VALUES ($1, 'contract', $2, $3, $4, $2)
            "#,
        )
        .bind(manifest_id)
        .bind(format!("replay_target_{manifest_id}"))
        .bind(contract_instance_id)
        .bind(address)
        .execute(pool)
        .await
        .with_context(|| format!("failed to seed replay manifest target {manifest_id}"))?;
    }
    Ok(())
}

fn replay_target_address(manifest_id: i64) -> &'static str {
    match manifest_id {
        1 => "0x0000000000000000000000000000000000000001",
        2 => "0x0000000000000000000000000000000000000002",
        4 => "0x0000000000000000000000000000000000000004",
        5 => "0x0000000000000000000000000000000000000005",
        _ => "0x00000000000000000000000000000000000000ff",
    }
}

async fn seed_extra_active_replay_target(pool: &PgPool, manifest_id: i64) -> Result<()> {
    let contract_instance_id = Uuid::from_u128(0xA000_u128 + manifest_id as u128);
    let address = format!("0x000000000000000000000000000000000000a{manifest_id:03x}");
    sqlx::query(
        r#"
        INSERT INTO contract_instances (
            contract_instance_id, chain_id, contract_kind, provenance
        )
        VALUES ($1, 'base-mainnet', 'test_replay_target_extra', '{}'::jsonb)
        "#,
    )
    .bind(contract_instance_id)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address, active_from_block_number,
            source_manifest_id, provenance
        )
        VALUES ($1, 'base-mainnet', $2, $3, $4, '{}'::jsonb)
        "#,
    )
    .bind(contract_instance_id)
    .bind(&address)
    .bind(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances (
            manifest_id, declaration_kind, declaration_name, contract_instance_id,
            declared_address, role
        )
        VALUES ($1, 'contract', $2, $3, $4, $2)
        "#,
    )
    .bind(manifest_id)
    .bind(format!("extra_replay_target_{manifest_id}"))
    .bind(contract_instance_id)
    .bind(address)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_split_active_replay_target(
    pool: &PgPool,
    manifest_id: i64,
    current_active_to_block: i64,
    successor_active_from_block: i64,
) -> Result<String> {
    let contract_instance_id = Uuid::from_u128(0xB000_u128 + manifest_id as u128);
    let address = format!("0x000000000000000000000000000000000000b{manifest_id:03x}");
    sqlx::query(
        r#"
        UPDATE contract_instance_addresses
        SET active_to_block_number = $2
        WHERE source_manifest_id = $1
          AND active_to_block_number IS NULL
        "#,
    )
    .bind(manifest_id)
    .bind(current_active_to_block)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instances (
            contract_instance_id, chain_id, contract_kind, provenance
        )
        VALUES ($1, 'base-mainnet', 'test_replay_target_split', '{}'::jsonb)
        "#,
    )
    .bind(contract_instance_id)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address, active_from_block_number,
            source_manifest_id, provenance
        )
        VALUES ($1, 'base-mainnet', $2, $3, $4, '{}'::jsonb)
        "#,
    )
    .bind(contract_instance_id)
    .bind(&address)
    .bind(successor_active_from_block)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances (
            manifest_id, declaration_kind, declaration_name, contract_instance_id,
            declared_address, role
        )
        VALUES ($1, 'contract', $2, $3, $4, $2)
        "#,
    )
    .bind(manifest_id)
    .bind(format!("split_replay_target_{manifest_id}"))
    .bind(contract_instance_id)
    .bind(&address)
    .execute(pool)
    .await?;
    Ok(address)
}

async fn seed_successor_emitter_scoped_event(pool: &PgPool, emitting_address: &str) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO raw_logs (
            chain_id, block_hash, block_number, transaction_hash,
            transaction_index, log_index, emitting_address, canonicality_state
        )
        VALUES ('base-mainnet', '0xbase-target', $1, '0xtx-target', 0, 10, $2, 'canonical')
        "#,
    )
    .bind(FIXTURE_REPLAY_TARGET_BLOCK)
    .bind(emitting_address)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity, namespace, event_kind, source_family, manifest_version,
            source_manifest_id, chain_id, block_number, block_hash, transaction_hash,
            log_index, raw_fact_ref, derivation_kind, canonicality_state
        )
        VALUES ('split-successor-scoped-log', 'basenames', 'RecordChanged',
                'basenames_base_registry', 1, 4, 'base-mainnet', $1, '0xbase-target',
                '0xtx-target', 10, '{}'::jsonb, 'ens_v1_unwrapped_authority', 'canonical')
        "#,
    )
    .bind(FIXTURE_REPLAY_TARGET_BLOCK)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_ens_v1_registry_l1_boundary_event(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity, namespace, logical_name_id, event_kind, source_family,
            manifest_version, source_manifest_id, chain_id, block_number, block_hash,
            transaction_hash, log_index, raw_fact_ref, derivation_kind, canonicality_state
        )
        VALUES (
            'ens-v1-registry-boundary',
            'basenames',
            'basenames:based1.base.eth',
            'SurfaceUnbound',
            'ens_v1_registry_l1',
            1,
            NULL,
            'base-mainnet',
            $1,
            '0xbase-mid',
            NULL,
            NULL,
            jsonb_build_object(
                'kind', 'raw_block',
                'chain_id', 'base-mainnet',
                'block_hash', '0xbase-mid',
                'block_number', $1
            ),
            'ens_v1_unwrapped_authority',
            'canonical'
        )
        "#,
    )
    .bind(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK + 1)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_ens_v1_registry_l1_boundary_event_missing_kind(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity, namespace, logical_name_id, event_kind, source_family,
            manifest_version, source_manifest_id, chain_id, block_number, block_hash,
            transaction_hash, log_index, raw_fact_ref, derivation_kind, canonicality_state
        )
        VALUES (
            'ens-v1-registry-boundary-missing-kind',
            'basenames',
            'basenames:based1.base.eth',
            'SurfaceUnbound',
            'ens_v1_registry_l1',
            1,
            NULL,
            'base-mainnet',
            $1,
            '0xbase-mid',
            NULL,
            NULL,
            '{}'::jsonb,
            'ens_v1_unwrapped_authority',
            'canonical'
        )
        "#,
    )
    .bind(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK + 1)
    .execute(pool)
    .await?;
    Ok(())
}

async fn move_fixture_basenames_registry_rows_out_of_delete_scope(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE normalized_events
        SET source_family = 'basenames_l1_compat'
        WHERE source_family = 'basenames_base_registry'
          AND chain_id = 'base-mainnet'
          AND block_hash IS NOT NULL
          AND (
              derivation_kind = 'ens_v1_unwrapped_authority'
              OR derivation_kind IN (
                  'ens_v1_subregistry_changed',
                  'ens_v1_registry_resolver_changed'
              )
          )
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn deactivate_base_source_family(pool: &PgPool, source_family: &str) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE manifest_versions
        SET rollout_status = 'deprecated'
        WHERE chain = 'base-mainnet'
          AND source_family = $1
        "#,
    )
    .bind(source_family)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_ens_v1_registry_l1_log_event(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO raw_logs (
            chain_id, block_hash, block_number, transaction_hash,
            transaction_index, log_index, emitting_address, canonicality_state
        )
        VALUES (
            'base-mainnet',
            '0xbase-target',
            $1,
            '0xtx-ens-v1-registry',
            0,
            11,
            $2,
            'canonical'
        )
        "#,
    )
    .bind(FIXTURE_REPLAY_TARGET_BLOCK)
    .bind(replay_target_address(4))
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity, namespace, logical_name_id, event_kind, source_family,
            manifest_version, source_manifest_id, chain_id, block_number, block_hash,
            transaction_hash, log_index, raw_fact_ref, derivation_kind, canonicality_state
        )
        VALUES (
            'ens-v1-registry-log',
            'basenames',
            'basenames:based1.base.eth',
            'RecordChanged',
            'ens_v1_registry_l1',
            1,
            NULL,
            'base-mainnet',
            $1,
            '0xbase-target',
            '0xtx-ens-v1-registry',
            11,
            jsonb_build_object(
                'kind', 'raw_log',
                'chain_id', 'base-mainnet',
                'block_hash', '0xbase-target',
                'block_number', $1,
                'transaction_hash', '0xtx-ens-v1-registry',
                'log_index', 11
            ),
            'ens_v1_unwrapped_authority',
            'canonical'
        )
        "#,
    )
    .bind(FIXTURE_REPLAY_TARGET_BLOCK)
    .execute(pool)
    .await?;
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
    for (block_hash, block_number, tx, log_index, emitting_address) in [
        (
            "0xbase-start",
            BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
            "0xtx-start",
            0_i64,
            replay_target_address(4),
        ),
        (
            "0xbase-target",
            FIXTURE_REPLAY_TARGET_BLOCK,
            "0xtx-target",
            9_i64,
            replay_target_address(1),
        ),
    ] {
        sqlx::query(
            r#"
            INSERT INTO raw_logs (
                chain_id, block_hash, block_number, transaction_hash,
                transaction_index, log_index, emitting_address, canonicality_state
            )
            VALUES ('base-mainnet', $1, $2, $3, 0, $4, $5, 'canonical')
            "#,
        )
        .bind(block_hash)
        .bind(block_number)
        .bind(tx)
        .bind(log_index)
        .bind(emitting_address)
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
            "basenames_l1_compat",
            Some(3_i64),
            Some(FIXTURE_OUT_OF_RANGE_BLOCK),
            Some("0xafter"),
            None,
            None,
            "raw_log_preimage_observation",
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

async fn seed_retained_raw_log_after_fixture_target(pool: &PgPool) -> Result<()> {
    seed_retained_raw_log(
        pool,
        "0xbase-after-retained",
        FIXTURE_REPLAY_TARGET_BLOCK + 10,
        "0xtx-after-retained",
    )
    .await
}

async fn seed_retained_raw_log_before_boundary(pool: &PgPool) -> Result<()> {
    seed_retained_raw_log(
        pool,
        "0xbase-before-retained",
        BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK - 10,
        "0xtx-before-retained",
    )
    .await
}

async fn seed_retained_raw_log(
    pool: &PgPool,
    block_hash: &str,
    block_number: i64,
    tx: &str,
) -> Result<()> {
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
                'basenames_l1_compat', 1, 3, 'base-mainnet', $3,
                '0xafter-anchor', '{}'::jsonb, 'raw_log_preimage_observation', 'canonical')
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

fn database_config(database_name: &str) -> Result<crate::DatabaseConfig> {
    let database_url = PgConnectOptions::from_str(&database_url_from_env())?
        .database(database_name)
        .to_url_lossy()
        .to_string();
    Ok(crate::DatabaseConfig {
        database_url: Some(database_url),
        max_connections: 2,
    })
}

async fn load_run_status(pool: &PgPool, run_id: &str) -> Result<(String, String)> {
    Ok(sqlx::query_as::<_, (String, String)>(
        "SELECT status, current_step FROM base_normalized_rederive_runs WHERE run_id = $1",
    )
    .bind(run_id)
    .fetch_one(pool)
    .await?)
}

async fn count_run_batches(pool: &PgPool, run_id: &str) -> Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT FROM base_normalized_rederive_run_batches WHERE run_id = $1",
    )
    .bind(run_id)
    .fetch_one(pool)
    .await?)
}

async fn max_non_reset_batch_rows(pool: &PgPool, run_id: &str) -> Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COALESCE(MAX(row_count), 0)::BIGINT
        FROM base_normalized_rederive_run_batches
        WHERE run_id = $1
          AND step <> 'final_replay_reset'
        "#,
    )
    .bind(run_id)
    .fetch_one(pool)
    .await?)
}

async fn final_reset_batch_rows(pool: &PgPool, run_id: &str) -> Result<i64> {
    Ok(sqlx::query_scalar::<_, i64>(
        r#"
        SELECT row_count
        FROM base_normalized_rederive_run_batches
        WHERE run_id = $1
          AND step = 'final_replay_reset'
        "#,
    )
    .bind(run_id)
    .fetch_one(pool)
    .await?)
}

async fn assert_no_dangling_refs(pool: &PgPool) -> Result<()> {
    let dangling = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT
            (
                SELECT COUNT(*)::BIGINT
                FROM projection_normalized_event_changes p
                WHERE NOT EXISTS (
                    SELECT 1 FROM normalized_events e
                    WHERE e.normalized_event_id = p.normalized_event_id
                )
            )
            + (
                SELECT COUNT(*)::BIGINT
                FROM normalized_events e
                WHERE (
                    e.resource_id IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM resources r WHERE r.resource_id = e.resource_id
                    )
                )
                OR (
                    e.logical_name_id IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM name_surfaces n WHERE n.logical_name_id = e.logical_name_id
                    )
                )
            )
            + (
                SELECT COUNT(*)::BIGINT
                FROM surface_bindings s
                WHERE NOT EXISTS (
                    SELECT 1 FROM resources r WHERE r.resource_id = s.resource_id
                )
                OR NOT EXISTS (
                    SELECT 1 FROM name_surfaces n WHERE n.logical_name_id = s.logical_name_id
                )
            )
            + (
                SELECT COUNT(*)::BIGINT
                FROM resources r
                WHERE r.token_lineage_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM token_lineages t
                      WHERE t.token_lineage_id = r.token_lineage_id
                  )
            )
            + (
                SELECT COUNT(*)::BIGINT
                FROM name_current p
                WHERE NOT EXISTS (
                    SELECT 1 FROM name_surfaces n WHERE n.logical_name_id = p.logical_name_id
                )
                OR (
                    p.resource_id IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM resources r WHERE r.resource_id = p.resource_id
                    )
                )
                OR (
                    p.surface_binding_id IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM surface_bindings s
                        WHERE s.surface_binding_id = p.surface_binding_id
                    )
                )
            )
        "#,
    )
    .fetch_one(pool)
    .await?;
    assert_eq!(dangling, 0);
    Ok(())
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
