use super::*;

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
