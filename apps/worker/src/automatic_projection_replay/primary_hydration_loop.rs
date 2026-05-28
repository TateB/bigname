use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use sqlx::PgPool;
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
use tracing::{info, warn};

use super::primary_hydration;
use crate::{primary_name, projection_apply};

pub(super) fn spawn(
    pool: PgPool,
    poll_interval_secs: u64,
    config: primary_name::PrimaryNameLegacyReverseHydrationConfig,
    projection_apply_generation: Arc<AtomicU64>,
    projection_apply_hydration_lock: Arc<Mutex<()>>,
) {
    tokio::spawn(async move {
        run(
            pool,
            poll_interval_secs,
            config,
            projection_apply_generation,
            projection_apply_hydration_lock,
        )
        .await;
    });
}

async fn run(
    pool: PgPool,
    poll_interval_secs: u64,
    config: primary_name::PrimaryNameLegacyReverseHydrationConfig,
    projection_apply_generation: Arc<AtomicU64>,
    projection_apply_hydration_lock: Arc<Mutex<()>>,
) {
    let poll_interval = Duration::from_secs(poll_interval_secs.max(1));
    let mut bootstrap_completed = false;
    let mut last_trigger = primary_hydration::LegacyReverseHydrationTriggerState::default();
    let mut hydrated_projection_generation = projection_apply_generation.load(Ordering::Acquire);

    info!(
        service = "worker",
        projection = "primary_names_current",
        "primary_names_current legacy reverse-resolver hydration loop started"
    );

    loop {
        let mut progressed = false;
        let apply_hydration_guard = projection_apply_hydration_lock.lock().await;
        match projection_apply::has_primary_hydration_blocking_work(&pool).await {
            Ok(true) => {
                drop(apply_hydration_guard);
                sleep(poll_interval).await;
                continue;
            }
            Ok(false) => {}
            Err(error) => {
                warn!(
                    service = "worker",
                    projection = "primary_names_current",
                    error = %format!("{error:#}"),
                    "failed to inspect projection apply work before primary-name hydration"
                );
                drop(apply_hydration_guard);
                sleep(poll_interval).await;
                continue;
            }
        }

        if !bootstrap_completed {
            let hydration_generation = projection_apply_generation.load(Ordering::Acquire);
            match primary_hydration::hydrate_after_bootstrap(
                &pool,
                Some(&config),
                &mut last_trigger,
            )
            .await
            {
                Ok(summary) => {
                    bootstrap_completed = summary.failed_lookup_count == 0;
                    progressed |= primary_hydration::bootstrap_hydration_made_progress(&summary);
                    if bootstrap_completed {
                        hydrated_projection_generation = hydration_generation;
                    }
                }
                Err(error) => {
                    warn!(
                        service = "worker",
                        projection = "primary_names_current",
                        error = %format!("{error:#}"),
                        "automatic primary_names_current legacy reverse-resolver bootstrap hydration failed"
                    );
                }
            }
        } else {
            let current_generation = projection_apply_generation.load(Ordering::Acquire);
            let mut projection_apply_changed = current_generation != hydrated_projection_generation;
            match primary_hydration::hydrate_if_projection_changed_or_triggered(
                &pool,
                Some(&config),
                &mut last_trigger,
                &mut projection_apply_changed,
            )
            .await
            {
                Ok(summary) => {
                    if !projection_apply_changed {
                        hydrated_projection_generation = current_generation;
                    }
                    progressed |= summary.upserted_row_count > 0 || summary.deleted_row_count > 0;
                }
                Err(error) => {
                    warn!(
                        service = "worker",
                        projection = "primary_names_current",
                        error = %format!("{error:#}"),
                        "automatic primary_names_current legacy reverse-resolver hydration failed"
                    );
                }
            }
        }
        drop(apply_hydration_guard);

        if !progressed {
            sleep(poll_interval).await;
        }
    }
}
