use super::*;
use anyhow::{Context, Result};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};

async fn test_database() -> Result<TestDatabase> {
    TestDatabase::create_migrated(
        TestDatabaseConfig::new("bigname_worker_auto_replay_test")
            .parse_context("failed to parse database URL for automatic projection replay tests")
            .admin_connect_context(
                "failed to connect admin pool for automatic projection replay tests",
            )
            .pool_connect_context("failed to connect automatic projection replay test pool"),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for automatic projection replay tests",
    )
    .await
}

fn ready_status() -> ProjectionReplayReadiness {
    ProjectionReplayReadiness {
        normalized_replay_cursor_count: 1,
        incomplete_normalized_replay_cursor_count: 0,
        failed_normalized_replay_cursor_count: 0,
        active_index_build_count: 0,
        missing_projection_index_count: 0,
        normalized_replay_max_target_block: Some(42),
    }
}

#[test]
fn all_current_projection_pool_size_raises_low_default() {
    let database = all_current_projections_database_config(DatabaseConfig {
        database_url: None,
        max_connections: 10,
    });

    assert_eq!(database.max_connections, 64);
}

#[test]
fn all_current_projection_pool_size_preserves_higher_override() {
    let database = all_current_projections_database_config(DatabaseConfig {
        database_url: None,
        max_connections: 96,
    });

    assert_eq!(database.max_connections, 96);
}

#[test]
fn projection_replay_waits_for_normalized_replay_cursor() {
    let status = ProjectionReplayReadiness {
        normalized_replay_cursor_count: 0,
        ..ready_status()
    };

    assert!(!status.is_ready());
}

#[test]
fn projection_replay_waits_for_complete_normalized_replay() {
    let status = ProjectionReplayReadiness {
        incomplete_normalized_replay_cursor_count: 1,
        ..ready_status()
    };

    assert!(!status.is_ready());
}

#[test]
fn projection_replay_waits_for_projection_indexes() {
    let status = ProjectionReplayReadiness {
        active_index_build_count: 1,
        ..ready_status()
    };
    assert!(!status.is_ready());

    let status = ProjectionReplayReadiness {
        missing_projection_index_count: 1,
        ..ready_status()
    };
    assert!(!status.is_ready());
}

#[test]
fn active_index_build_probe_is_scoped_to_current_database() {
    assert!(
        ACTIVE_INDEX_BUILDS_QUERY.contains("datname = current_database()"),
        "projection replay readiness must ignore index builds in other databases"
    );
}

#[test]
fn projection_replay_runs_when_normalized_replay_and_indexes_are_ready() {
    assert!(ready_status().is_ready());
}

#[test]
fn apply_cursor_is_seeded_after_bootstrap_when_absent() {
    assert!(should_seed_apply_cursor_after_bootstrap(false));
    assert!(!should_seed_apply_cursor_after_bootstrap(true));
}

#[test]
fn bootstrap_target_covers_live_checkpoint_head() {
    assert_eq!(
        projection_bootstrap_replay_target_block(Some(10), Some(15)),
        Some(15)
    );
    assert_eq!(
        projection_bootstrap_replay_target_block(Some(15), Some(10)),
        Some(15)
    );
}

#[test]
fn restart_bootstrap_skip_requires_apply_cursor_and_all_current_markers() {
    let complete_marker_count = replay::ALL_CURRENT_PROJECTION_ORDER.len() as i64;

    assert!(should_skip_bootstrap_for_existing_apply_cursor(
        true,
        complete_marker_count
    ));
    assert!(!should_skip_bootstrap_for_existing_apply_cursor(
        false,
        complete_marker_count
    ));
    assert!(!should_skip_bootstrap_for_existing_apply_cursor(
        true,
        complete_marker_count - 1
    ));
}

#[tokio::test]
async fn restart_bootstrap_skip_requires_apply_cursor_even_with_target_covering_replay_markers()
-> Result<()> {
    let database = test_database().await?;
    seed_ready_normalized_replay_cursor(database.pool(), 20).await?;
    seed_chain_checkpoint(database.pool(), 20).await?;
    seed_replay_markers(database.pool(), 20).await?;

    assert!(
        !projection_bootstrap_already_handed_off_to_apply(database.pool()).await?,
        "replay markers must not hand off bootstrap before incremental apply has a cursor"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn restart_handoff_with_apply_cursor_ignores_later_chain_checkpoint() -> Result<()> {
    let database = test_database().await?;
    seed_apply_cursor(database.pool()).await?;
    seed_ready_normalized_replay_cursor(database.pool(), 20).await?;
    seed_chain_checkpoint(database.pool(), 20).await?;
    seed_replay_markers(database.pool(), 20).await?;

    assert!(
        projection_bootstrap_already_handed_off_to_apply(database.pool()).await?,
        "current replay markers and an apply cursor should hand off bootstrap"
    );

    advance_chain_checkpoint(database.pool(), 21).await?;

    assert!(
        projection_bootstrap_already_handed_off_to_apply(database.pool()).await?,
        "an existing apply cursor should own post-handoff checkpoint progress"
    );

    database.cleanup().await?;
    Ok(())
}

#[test]
fn primary_hydration_start_is_independent_from_text_hydration_completion() {
    assert_eq!(
        bootstrap_hydration_schedule(false, false),
        BootstrapHydrationSchedule {
            start_primary_hydration: true,
            run_text_hydration: true,
        }
    );
    assert_eq!(
        bootstrap_hydration_schedule(false, true),
        BootstrapHydrationSchedule {
            start_primary_hydration: false,
            run_text_hydration: true,
        }
    );
}

async fn seed_apply_cursor(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO projection_apply_cursors (cursor_name, last_change_id)
        VALUES ('normalized_events_to_projection_invalidations', 1)
        "#,
    )
    .execute(pool)
    .await
    .context("failed to seed projection apply cursor")?;
    Ok(())
}

async fn seed_ready_normalized_replay_cursor(pool: &PgPool, target_block: i64) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO normalized_replay_cursors (
            deployment_profile,
            chain_id,
            cursor_kind,
            range_start_block_number,
            next_block_number,
            target_block_number,
            last_completed_block_number
        )
        VALUES (
            'ens',
            'ethereum-mainnet',
            'raw_fact_normalized_events',
            0,
            $1 + 1,
            $1,
            $1
        )
        "#,
    )
    .bind(target_block)
    .execute(pool)
    .await
    .context("failed to seed normalized replay cursor")?;
    Ok(())
}

async fn seed_chain_checkpoint(pool: &PgPool, block_number: i64) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO chain_checkpoints (
            chain_id,
            canonical_block_hash,
            canonical_block_number
        )
        VALUES ('ethereum-mainnet', '0xcheckpoint', $1)
        "#,
    )
    .bind(block_number)
    .execute(pool)
    .await
    .context("failed to seed chain checkpoint")?;
    Ok(())
}

async fn advance_chain_checkpoint(pool: &PgPool, block_number: i64) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE chain_checkpoints
        SET canonical_block_hash = $1, canonical_block_number = $2
        WHERE chain_id = 'ethereum-mainnet'
        "#,
    )
    .bind(format!("0xcheckpoint{block_number}"))
    .bind(block_number)
    .execute(pool)
    .await
    .context("failed to advance chain checkpoint")?;
    Ok(())
}

async fn seed_replay_markers(pool: &PgPool, completed_target_block: i64) -> Result<()> {
    for projection in replay::ALL_CURRENT_PROJECTION_ORDER {
        sqlx::query(
            r#"
            INSERT INTO current_projection_replay_status (
                projection,
                replay_version,
                completed_normalized_target_block,
                requested_key_count,
                upserted_row_count,
                deleted_row_count
            )
            VALUES ($1, $2, $3, 0, 0, 0)
            ON CONFLICT (projection)
            DO UPDATE SET
                replay_version = EXCLUDED.replay_version,
                completed_normalized_target_block =
                    EXCLUDED.completed_normalized_target_block
            "#,
        )
        .bind(*projection)
        .bind(replay::CURRENT_PROJECTION_REPLAY_VERSION)
        .bind(completed_target_block)
        .execute(pool)
        .await
        .with_context(|| format!("failed to seed replay marker for {projection}"))?;
    }
    Ok(())
}
