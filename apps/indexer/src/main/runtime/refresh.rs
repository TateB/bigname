use anyhow::Result;
use bigname_manifests::{WatchedChainPlan, load_watched_chain_plan, load_watched_contract_summary};

use super::adapter_sync::sync_adapter_owned_raw_log_state;
use super::intake::{IntakeChainTask, sync_intake_chain_tasks};
use super::manifest::ManifestRuntimeState;

pub(crate) async fn refresh_manifest_normalized_events_from_storage(
    pool: &sqlx::PgPool,
    manifest_runtime_state: &ManifestRuntimeState,
) -> Result<Option<ManifestRuntimeState>> {
    let next_summary = bigname_adapters::sync_manifest_normalized_events(pool).await?;
    if next_summary.total_inserted_count == 0 {
        return Ok(None);
    }

    let mut next_manifest_runtime_state = manifest_runtime_state.clone();
    next_manifest_runtime_state.manifest_normalized_event_summary = next_summary;
    Ok(Some(next_manifest_runtime_state))
}

pub(crate) async fn refresh_watched_chain_plan(
    pool: &sqlx::PgPool,
    current_plan: &[WatchedChainPlan],
) -> Result<Option<Vec<WatchedChainPlan>>> {
    let next_plan = load_watched_chain_plan(pool).await?;
    if next_plan == current_plan {
        Ok(None)
    } else {
        Ok(Some(next_plan))
    }
}

pub(crate) async fn refresh_intake_chain_tasks(
    pool: &sqlx::PgPool,
    current_tasks: &[IntakeChainTask],
    watched_chain_plan: &[WatchedChainPlan],
) -> Result<Option<Vec<IntakeChainTask>>> {
    let next_tasks = sync_intake_chain_tasks(pool, watched_chain_plan).await?;
    if next_tasks == current_tasks {
        Ok(None)
    } else {
        Ok(Some(next_tasks))
    }
}

pub(crate) async fn refresh_runtime_state_from_storage_discovery(
    pool: &sqlx::PgPool,
    manifest_runtime_state: &ManifestRuntimeState,
) -> Result<Option<(ManifestRuntimeState, Vec<IntakeChainTask>)>> {
    sync_adapter_owned_raw_log_state(pool, &manifest_runtime_state.watched_chain_plan).await?;

    let Some(next_watched_chain_plan) =
        refresh_watched_chain_plan(pool, &manifest_runtime_state.watched_chain_plan).await?
    else {
        return Ok(None);
    };
    let next_intake_chain_tasks = sync_intake_chain_tasks(pool, &next_watched_chain_plan).await?;
    let mut next_manifest_runtime_state = manifest_runtime_state.clone();
    next_manifest_runtime_state.watched_contract_summary =
        load_watched_contract_summary(pool).await?;
    next_manifest_runtime_state.watched_chain_plan = next_watched_chain_plan;

    Ok(Some((next_manifest_runtime_state, next_intake_chain_tasks)))
}
