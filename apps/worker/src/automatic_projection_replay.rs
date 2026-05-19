use anyhow::{Context, Result};
use bigname_storage::DatabaseConfig;
use sqlx::{PgPool, Postgres, pool::PoolConnection};
use tokio::time::{Duration, sleep};
use tracing::{debug, info, warn};

use crate::{cli::RunArgs, projection_apply, record_inventory, replay};

const CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS: &str = "raw_fact_normalized_events";
const ALL_CURRENT_PROJECTIONS_MIN_DATABASE_CONNECTIONS: u32 = 64;
const ALL_CURRENT_PROJECTIONS_REPLAY_LOCK_KEY: i64 = 0x4249474e414d4501_i64;
const DEFERRED_NORMALIZED_EVENT_INDEXES: &[&str] = &[
    "normalized_events_namespace_idx",
    "normalized_events_kind_idx",
    "normalized_events_manifest_idx",
    "normalized_events_chain_position_idx",
    "normalized_events_name_projection_replay_idx",
    "normalized_events_resource_projection_replay_idx",
    "normalized_events_name_relevant_projection_idx",
    "normalized_events_record_inventory_resource_replay_idx",
];

pub(crate) fn all_current_projections_database_config(
    mut database: DatabaseConfig,
) -> DatabaseConfig {
    database.max_connections = database
        .max_connections
        .max(ALL_CURRENT_PROJECTIONS_MIN_DATABASE_CONNECTIONS);
    database
}

pub(crate) async fn run_worker(args: RunArgs) -> Result<()> {
    let database = all_current_projections_database_config(args.database);
    let pool = bigname_storage::connect(&database).await?;
    let text_hydration_config =
        record_inventory::RecordInventoryTextHydrationConfig::from_chain_rpc_url_entries(
            &args.chain_rpc_urls,
            args.text_hydration_multicall3_address,
            args.text_hydration_batch_size,
        )?;

    info!(
        service = "worker",
        phase = bigname_domain::bootstrap_phase(),
        execution_status = bigname_execution::bootstrap_status(),
        poll_interval_secs = args.poll_interval_secs,
        database_max_connections = database.max_connections,
        automatic_projection_replay = true,
        record_inventory_text_hydration = text_hydration_config.is_some(),
        "worker booted"
    );

    tokio::select! {
        () = run_automatic_current_projection_replay(
            pool,
            args.poll_interval_secs,
            text_hydration_config,
        ) => {}
        signal = tokio::signal::ctrl_c() => {
            signal.context("failed to listen for shutdown signal")?;
        }
    }

    info!(service = "worker", "shutdown signal received");
    Ok(())
}

pub(crate) async fn run_automatic_current_projection_replay(
    pool: PgPool,
    poll_interval_secs: u64,
    text_hydration_config: Option<record_inventory::RecordInventoryTextHydrationConfig>,
) {
    let poll_interval = Duration::from_secs(poll_interval_secs.max(1));
    let mut bootstrap_completed = false;
    let mut bootstrap_text_hydration_completed = text_hydration_config.is_none();

    loop {
        let mut progressed = false;
        if !bootstrap_completed {
            match projection_bootstrap_already_handed_off_to_apply(&pool).await {
                Ok(true) => {
                    bootstrap_completed = true;
                    progressed = true;
                    info!(
                        service = "worker",
                        replay = "all_current_projections",
                        "automatic all-current projection replay skipped because durable apply cursor and replay markers exist"
                    );
                }
                Ok(false) => {}
                Err(error) => {
                    warn!(
                        service = "worker",
                        replay = "all_current_projections",
                        error = %format!("{error:#}"),
                        "failed to inspect automatic all-current projection replay handoff state"
                    );
                }
            }
        }

        if !bootstrap_completed {
            match replay_all_current_projections_when_ready(&pool, text_hydration_config.as_ref())
                .await
            {
                Ok(true) => {
                    bootstrap_completed = true;
                    progressed = true;
                }
                Ok(false) => {}
                Err(error) => {
                    warn!(
                        service = "worker",
                        replay = "all_current_projections",
                        error = %format!("{error:#}"),
                        "automatic all-current projection replay failed"
                    );
                }
            }
        }

        if bootstrap_completed {
            if !bootstrap_text_hydration_completed {
                match hydrate_record_inventory_text_values_after_bootstrap(
                    &pool,
                    text_hydration_config.as_ref(),
                )
                .await
                {
                    Ok(()) => {
                        bootstrap_text_hydration_completed = true;
                        progressed = true;
                    }
                    Err(error) => {
                        warn!(
                            service = "worker",
                            projection = "record_inventory_current",
                            error = %format!("{error:#}"),
                            "automatic record_inventory_current text hydration failed"
                        );
                    }
                }
            }

            match projection_apply::run_once(&pool, text_hydration_config.as_ref()).await {
                Ok(summary) => {
                    progressed |= summary.made_progress();
                }
                Err(error) => {
                    warn!(
                        service = "worker",
                        projection_apply = true,
                        error = %format!("{error:#}"),
                        "continuous projection apply iteration failed"
                    );
                }
            }
        }

        if !progressed {
            sleep(poll_interval).await;
        }
    }
}

async fn hydrate_record_inventory_text_values_after_bootstrap(
    pool: &PgPool,
    text_hydration_config: Option<&record_inventory::RecordInventoryTextHydrationConfig>,
) -> Result<()> {
    let Some(config) = text_hydration_config else {
        return Ok(());
    };
    let summary =
        record_inventory::hydrate_record_inventory_text_values(pool, None, config.clone()).await?;
    record_inventory::log_text_hydration_summary(None, &summary);
    Ok(())
}

async fn replay_all_current_projections_when_ready(
    pool: &PgPool,
    text_hydration_config: Option<&record_inventory::RecordInventoryTextHydrationConfig>,
) -> Result<bool> {
    let readiness = load_projection_replay_readiness(pool).await?;
    if !readiness.is_ready() {
        debug!(
            service = "worker",
            replay = "all_current_projections",
            normalized_replay_cursor_count = readiness.normalized_replay_cursor_count,
            incomplete_normalized_replay_cursor_count =
                readiness.incomplete_normalized_replay_cursor_count,
            failed_normalized_replay_cursor_count = readiness.failed_normalized_replay_cursor_count,
            active_index_build_count = readiness.active_index_build_count,
            missing_projection_index_count = readiness.missing_projection_index_count,
            "automatic all-current projection replay is waiting for normalized replay readiness"
        );
        return Ok(false);
    }

    let Some(mut replay_lock) = try_acquire_replay_lock(pool).await? else {
        debug!(
            service = "worker",
            replay = "all_current_projections",
            "automatic all-current projection replay skipped because another worker holds the replay lock"
        );
        return Ok(false);
    };

    let readiness = load_projection_replay_readiness(pool).await?;
    if !readiness.is_ready() {
        release_replay_lock(&mut replay_lock).await?;
        return Ok(false);
    }

    let cursor_exists = projection_apply::normalized_event_cursor_exists(pool).await?;
    let should_seed_apply_cursor = should_seed_apply_cursor_after_bootstrap(cursor_exists);
    let bootstrap_watermark =
        projection_apply::load_normalized_event_change_watermark(pool).await?;
    let chain_checkpoint_max_block =
        projection_apply::load_chain_checkpoint_max_block(pool).await?;
    let replay_target_block = projection_bootstrap_replay_target_block(
        readiness.normalized_replay_max_target_block,
        chain_checkpoint_max_block,
    );
    info!(
        service = "worker",
        replay = "all_current_projections",
        normalized_replay_cursor_count = readiness.normalized_replay_cursor_count,
        normalized_replay_max_target_block = readiness.normalized_replay_max_target_block,
        chain_checkpoint_max_block,
        projection_replay_target_block = replay_target_block,
        bootstrap_change_watermark = bootstrap_watermark.change_id,
        "automatic all-current projection replay started"
    );
    let replay_result = replay::rebuild_pending_all_current_projections(
        pool,
        replay_target_block,
        text_hydration_config,
    )
    .await;
    release_replay_lock(&mut replay_lock).await?;

    let summary =
        replay_result.context("failed to automatically replay all current projections")?;
    if should_seed_apply_cursor {
        projection_apply::seed_normalized_event_cursor_if_absent(pool, bootstrap_watermark).await?;
    }
    info!(
        service = "worker",
        replay = "all_current_projections",
        projection_order = ?summary.projection_order(),
        projection_count = summary.steps.len(),
        total_requested_key_count = summary.total_requested_key_count(),
        total_upserted_row_count = summary.total_upserted_row_count(),
        total_deleted_row_count = summary.total_deleted_row_count(),
        "automatic all-current projection replay completed"
    );

    Ok(true)
}

async fn projection_bootstrap_already_handed_off_to_apply(pool: &PgPool) -> Result<bool> {
    let cursor_exists = projection_apply::normalized_event_cursor_exists(pool).await?;
    if !cursor_exists {
        return Ok(false);
    }

    let complete_marker_count = load_current_projection_replay_marker_count(pool).await?;
    Ok(should_skip_bootstrap_for_existing_apply_cursor(
        cursor_exists,
        complete_marker_count,
    ))
}

fn should_seed_apply_cursor_after_bootstrap(cursor_exists: bool) -> bool {
    !cursor_exists
}

fn should_skip_bootstrap_for_existing_apply_cursor(
    cursor_exists: bool,
    complete_marker_count: i64,
) -> bool {
    cursor_exists && complete_marker_count == replay::ALL_CURRENT_PROJECTION_ORDER.len() as i64
}

fn projection_bootstrap_replay_target_block(
    normalized_replay_target_block: Option<i64>,
    chain_checkpoint_max_block: Option<i64>,
) -> Option<i64> {
    match (normalized_replay_target_block, chain_checkpoint_max_block) {
        (Some(replay), Some(checkpoint)) => Some(replay.max(checkpoint)),
        (Some(replay), None) => Some(replay),
        (None, Some(checkpoint)) => Some(checkpoint),
        (None, None) => None,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProjectionReplayReadiness {
    normalized_replay_cursor_count: i64,
    incomplete_normalized_replay_cursor_count: i64,
    failed_normalized_replay_cursor_count: i64,
    active_index_build_count: i64,
    missing_projection_index_count: i64,
    normalized_replay_max_target_block: Option<i64>,
}

impl ProjectionReplayReadiness {
    fn is_ready(&self) -> bool {
        self.normalized_replay_cursor_count > 0
            && self.incomplete_normalized_replay_cursor_count == 0
            && self.failed_normalized_replay_cursor_count == 0
            && self.active_index_build_count == 0
            && self.missing_projection_index_count == 0
    }
}

async fn load_projection_replay_readiness(pool: &PgPool) -> Result<ProjectionReplayReadiness> {
    let cursor_status = sqlx::query_as::<_, (i64, i64, i64, Option<i64>)>(
        r#"
        SELECT
            COUNT(*)::bigint AS cursor_count,
            COUNT(*) FILTER (
                WHERE next_block_number <= target_block_number
            )::bigint AS incomplete_cursor_count,
            COUNT(*) FILTER (
                WHERE last_failure_reason IS NOT NULL
            )::bigint AS failed_cursor_count,
            MAX(target_block_number) AS max_target_block
        FROM normalized_replay_cursors
        WHERE cursor_kind = $1
        "#,
    )
    .bind(CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS)
    .fetch_one(pool)
    .await
    .context("failed to inspect normalized replay cursor readiness")?;

    let active_index_build_count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::bigint FROM pg_stat_progress_create_index")
            .fetch_one(pool)
            .await
            .context("failed to inspect active PostgreSQL index builds")?;

    let missing_projection_index_count = missing_projection_index_count(pool).await?;

    Ok(ProjectionReplayReadiness {
        normalized_replay_cursor_count: cursor_status.0,
        incomplete_normalized_replay_cursor_count: cursor_status.1,
        failed_normalized_replay_cursor_count: cursor_status.2,
        active_index_build_count,
        missing_projection_index_count,
        normalized_replay_max_target_block: cursor_status.3,
    })
}

async fn missing_projection_index_count(pool: &PgPool) -> Result<i64> {
    let required_indexes = DEFERRED_NORMALIZED_EVENT_INDEXES
        .iter()
        .map(|index| format!("('{index}')"))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "SELECT COUNT(*)::bigint \
         FROM (VALUES {required_indexes}) AS required(index_name) \
         WHERE to_regclass(required.index_name) IS NULL"
    );

    sqlx::query_scalar::<_, i64>(&query)
        .fetch_one(pool)
        .await
        .context("failed to inspect deferred normalized-event projection indexes")
}

async fn load_current_projection_replay_marker_count(pool: &PgPool) -> Result<i64> {
    let projections = replay::ALL_CURRENT_PROJECTION_ORDER
        .iter()
        .copied()
        .collect::<Vec<_>>();

    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(DISTINCT projection)::BIGINT
        FROM current_projection_replay_status
        WHERE replay_version = $1
          AND projection = ANY($2::TEXT[])
        "#,
    )
    .bind(replay::CURRENT_PROJECTION_REPLAY_VERSION)
    .bind(&projections)
    .fetch_one(pool)
    .await
    .context("failed to inspect current projection replay markers")
}

async fn try_acquire_replay_lock(pool: &PgPool) -> Result<Option<PoolConnection<Postgres>>> {
    let mut connection = pool
        .acquire()
        .await
        .context("failed to acquire all-current replay lock connection")?;
    let acquired = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
        .bind(ALL_CURRENT_PROJECTIONS_REPLAY_LOCK_KEY)
        .fetch_one(&mut *connection)
        .await
        .context("failed to acquire all-current replay advisory lock")?;

    Ok(acquired.then_some(connection))
}

async fn release_replay_lock(connection: &mut PoolConnection<Postgres>) -> Result<()> {
    let released = sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
        .bind(ALL_CURRENT_PROJECTIONS_REPLAY_LOCK_KEY)
        .fetch_one(&mut **connection)
        .await
        .context("failed to release all-current replay advisory lock")?;
    if !released {
        warn!(
            service = "worker",
            replay = "all_current_projections",
            "all-current projection replay advisory lock was already released"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
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
}
