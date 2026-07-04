use anyhow::{Context, Result, bail, ensure};
use serde_json::Value;
use sqlx::Row;

use super::super::counts::load_counts_from;
use super::super::{
    BASE_NORMALIZED_REDERIVE_CHAIN_ID, BaseNormalizedRederiveCounts, BaseNormalizedRederivePlan,
};

pub(super) const RUN_STATUS_RUNNING: &str = "running";
pub(super) const RUN_STATUS_COMPLETED: &str = "completed";
pub(super) const RUN_STATUS_ABORTED: &str = "aborted";

#[derive(Clone, Debug)]
pub(super) struct RunState {
    pub(super) run_id: String,
    pub(super) deployment_profile: String,
    pub(super) replay_target_block: i64,
    pub(super) batch_size: i64,
    pub(super) status: String,
    pub(super) current_step: String,
    pub(super) expected_counts: BaseNormalizedRederiveCounts,
    pub(super) deleted_counts: BaseNormalizedRederiveCounts,
    pub(super) plan_snapshot: BaseNormalizedRederivePlan,
}

impl RunState {
    pub(super) fn is_completed(&self) -> bool {
        self.status == RUN_STATUS_COMPLETED
    }

    pub(super) fn is_aborted(&self) -> bool {
        self.status == RUN_STATUS_ABORTED
    }

    pub(super) fn advance_step(&mut self, step: Step) {
        self.current_step = step.as_str().to_owned();
    }

    pub(super) fn mark_completed(&mut self) {
        self.status = RUN_STATUS_COMPLETED.to_owned();
        self.current_step = Step::Completed.as_str().to_owned();
    }
}

pub(super) struct BatchProgress {
    pub(super) state: RunState,
    pub(super) step: Step,
    pub(super) deleted_rows: i64,
}

impl BatchProgress {
    pub(super) fn new(state: RunState, step: Step, deleted_rows: i64) -> Self {
        Self {
            state,
            step,
            deleted_rows,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Step {
    AddressNamesCurrent,
    NameCurrent,
    ChildrenCurrent,
    PermissionsCurrent,
    RecordInventoryCurrent,
    ProjectionNormalizedEventChanges,
    NormalizedEvents,
    SurfaceBindings,
    Resources,
    NameSurfaces,
    TokenLineages,
    FinalReplayReset,
    Completed,
}

impl Step {
    pub(super) fn first() -> Self {
        Self::AddressNamesCurrent
    }

    pub(super) fn parse(value: &str) -> Result<Self> {
        match value {
            "address_names_current" => Ok(Self::AddressNamesCurrent),
            "name_current" => Ok(Self::NameCurrent),
            "children_current" => Ok(Self::ChildrenCurrent),
            "permissions_current" => Ok(Self::PermissionsCurrent),
            "record_inventory_current" => Ok(Self::RecordInventoryCurrent),
            "projection_normalized_event_changes" => Ok(Self::ProjectionNormalizedEventChanges),
            "normalized_events" => Ok(Self::NormalizedEvents),
            "surface_bindings" => Ok(Self::SurfaceBindings),
            "resources" => Ok(Self::Resources),
            "name_surfaces" => Ok(Self::NameSurfaces),
            "token_lineages" => Ok(Self::TokenLineages),
            "final_replay_reset" => Ok(Self::FinalReplayReset),
            "completed" => Ok(Self::Completed),
            _ => bail!("unknown Base normalized-event rederive step {value:?}"),
        }
    }

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::AddressNamesCurrent => "address_names_current",
            Self::NameCurrent => "name_current",
            Self::ChildrenCurrent => "children_current",
            Self::PermissionsCurrent => "permissions_current",
            Self::RecordInventoryCurrent => "record_inventory_current",
            Self::ProjectionNormalizedEventChanges => "projection_normalized_event_changes",
            Self::NormalizedEvents => "normalized_events",
            Self::SurfaceBindings => "surface_bindings",
            Self::Resources => "resources",
            Self::NameSurfaces => "name_surfaces",
            Self::TokenLineages => "token_lineages",
            Self::FinalReplayReset => "final_replay_reset",
            Self::Completed => "completed",
        }
    }

    pub(super) fn next(self) -> Self {
        match self {
            Self::AddressNamesCurrent => Self::NameCurrent,
            Self::NameCurrent => Self::ChildrenCurrent,
            Self::ChildrenCurrent => Self::PermissionsCurrent,
            Self::PermissionsCurrent => Self::RecordInventoryCurrent,
            Self::RecordInventoryCurrent => Self::ProjectionNormalizedEventChanges,
            Self::ProjectionNormalizedEventChanges => Self::NormalizedEvents,
            Self::NormalizedEvents => Self::SurfaceBindings,
            Self::SurfaceBindings => Self::Resources,
            Self::Resources => Self::NameSurfaces,
            Self::NameSurfaces => Self::TokenLineages,
            Self::TokenLineages => Self::FinalReplayReset,
            Self::FinalReplayReset | Self::Completed => Self::Completed,
        }
    }

    fn count(self, counts: &BaseNormalizedRederiveCounts) -> i64 {
        match self {
            Self::AddressNamesCurrent => counts.address_names_current,
            Self::NameCurrent => counts.name_current,
            Self::ChildrenCurrent => counts.children_current,
            Self::PermissionsCurrent => counts.permissions_current,
            Self::RecordInventoryCurrent => counts.record_inventory_current,
            Self::ProjectionNormalizedEventChanges => counts.projection_normalized_event_changes,
            Self::NormalizedEvents => counts.normalized_events,
            Self::SurfaceBindings => counts.surface_bindings,
            Self::Resources => counts.resources,
            Self::NameSurfaces => counts.name_surfaces,
            Self::TokenLineages => counts.token_lineages,
            Self::FinalReplayReset => {
                counts.current_projection_replay_status
                    + counts.replay_cursor_rows
                    + counts.adapter_checkpoint_rows
                    + counts.adapter_checkpoint_item_rows
            }
            Self::Completed => 0,
        }
    }
}

pub(super) trait CountsExt {
    fn add_step(&mut self, step: Step, row_count: i64);
    fn add_reset_counts(&mut self, reset_counts: &BaseNormalizedRederiveCounts);
    fn reset_row_count(&self) -> i64;
}

impl CountsExt for BaseNormalizedRederiveCounts {
    fn add_step(&mut self, step: Step, row_count: i64) {
        match step {
            Step::AddressNamesCurrent => self.address_names_current += row_count,
            Step::NameCurrent => self.name_current += row_count,
            Step::ChildrenCurrent => self.children_current += row_count,
            Step::PermissionsCurrent => self.permissions_current += row_count,
            Step::RecordInventoryCurrent => self.record_inventory_current += row_count,
            Step::ProjectionNormalizedEventChanges => {
                self.projection_normalized_event_changes += row_count;
            }
            Step::NormalizedEvents => self.normalized_events += row_count,
            Step::SurfaceBindings => self.surface_bindings += row_count,
            Step::Resources => self.resources += row_count,
            Step::NameSurfaces => self.name_surfaces += row_count,
            Step::TokenLineages => self.token_lineages += row_count,
            Step::FinalReplayReset | Step::Completed => {}
        }
    }

    fn add_reset_counts(&mut self, reset_counts: &BaseNormalizedRederiveCounts) {
        self.current_projection_replay_status += reset_counts.current_projection_replay_status;
        self.replay_cursor_rows += reset_counts.replay_cursor_rows;
        self.adapter_checkpoint_rows += reset_counts.adapter_checkpoint_rows;
        self.adapter_checkpoint_item_rows += reset_counts.adapter_checkpoint_item_rows;
    }

    fn reset_row_count(&self) -> i64 {
        self.current_projection_replay_status
            + self.replay_cursor_rows
            + self.adapter_checkpoint_rows
            + self.adapter_checkpoint_item_rows
    }
}

pub(super) async fn load_run_for_update(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: &str,
) -> Result<Option<RunState>> {
    let row = sqlx::query(
        r#"
        SELECT run_id, deployment_profile, replay_target_block, batch_size, status,
               current_step, expected_counts, deleted_counts, plan_snapshot
        FROM base_normalized_rederive_runs
        WHERE run_id = $1
        FOR UPDATE
        "#,
    )
    .bind(run_id)
    .fetch_optional(&mut **transaction)
    .await
    .context("failed to load Base normalized-event rederive run state")?;
    row.map(run_state_from_row).transpose()
}

pub(super) async fn refuse_if_other_running_run(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: &str,
) -> Result<()> {
    let rows = sqlx::query(
        r#"
        SELECT run_id
        FROM base_normalized_rederive_runs
        WHERE chain_id = $1
          AND status = 'running'
          AND run_id <> $2
        ORDER BY updated_at DESC
        "#,
    )
    .bind(BASE_NORMALIZED_REDERIVE_CHAIN_ID)
    .bind(run_id)
    .fetch_all(&mut **transaction)
    .await
    .context("failed to inspect running Base normalized-event rederive runs")?;
    ensure!(
        rows.is_empty(),
        "Base normalized-event rederive cannot start run {run_id:?}; another run is still incomplete: {:?}",
        rows.iter()
            .map(|row| row.get::<String, _>("run_id"))
            .collect::<Vec<_>>()
    );
    Ok(())
}

pub(super) async fn insert_run(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: &str,
    deployment_profile: &str,
    replay_target_block: i64,
    batch_size: i64,
    expected_counts: &BaseNormalizedRederiveCounts,
    plan: &BaseNormalizedRederivePlan,
) -> Result<RunState> {
    let deleted_counts = BaseNormalizedRederiveCounts::default();
    sqlx::query(
        r#"
        INSERT INTO base_normalized_rederive_runs (
            run_id, deployment_profile, chain_id, replay_target_block, batch_size,
            status, current_step, expected_counts, deleted_counts, plan_snapshot
        )
        VALUES ($1, $2, $3, $4, $5, 'running', $6, $7, $8, $9)
        "#,
    )
    .bind(run_id)
    .bind(deployment_profile)
    .bind(BASE_NORMALIZED_REDERIVE_CHAIN_ID)
    .bind(replay_target_block)
    .bind(batch_size)
    .bind(Step::first().as_str())
    .bind(serde_json::to_value(expected_counts)?)
    .bind(serde_json::to_value(&deleted_counts)?)
    .bind(serde_json::to_value(plan)?)
    .execute(&mut **transaction)
    .await
    .context("failed to create Base normalized-event rederive run state")?;
    Ok(RunState {
        run_id: run_id.to_owned(),
        deployment_profile: deployment_profile.to_owned(),
        replay_target_block,
        batch_size,
        status: RUN_STATUS_RUNNING.to_owned(),
        current_step: Step::first().as_str().to_owned(),
        expected_counts: expected_counts.clone(),
        deleted_counts,
        plan_snapshot: plan.clone(),
    })
}

pub(super) async fn update_run_state(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    state: &RunState,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE base_normalized_rederive_runs
        SET status = $2,
            current_step = $3,
            deleted_counts = $4,
            plan_snapshot = $5,
            updated_at = now(),
            completed_at = CASE WHEN $2 = 'completed' THEN now() ELSE NULL END
        WHERE run_id = $1
        "#,
    )
    .bind(&state.run_id)
    .bind(&state.status)
    .bind(&state.current_step)
    .bind(serde_json::to_value(&state.deleted_counts)?)
    .bind(serde_json::to_value(&state.plan_snapshot)?)
    .execute(&mut **transaction)
    .await
    .context("failed to update Base normalized-event rederive run state")?;
    Ok(())
}

pub(super) async fn insert_batch_record(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run_id: &str,
    step: Step,
    range_start: Option<String>,
    range_end: Option<String>,
    row_count: i64,
    deleted_counts: &BaseNormalizedRederiveCounts,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO base_normalized_rederive_run_batches (
            run_id, step, range_start, range_end, row_count, deleted_counts
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(run_id)
    .bind(step.as_str())
    .bind(range_start)
    .bind(range_end)
    .bind(row_count)
    .bind(serde_json::to_value(deleted_counts)?)
    .execute(&mut **transaction)
    .await
    .context("failed to record Base normalized-event rederive batch")?;
    Ok(())
}

pub(super) fn ensure_run_matches(
    state: &RunState,
    deployment_profile: &str,
    batch_size: i64,
    requested_replay_target_block: Option<i64>,
    expected_counts: &BaseNormalizedRederiveCounts,
) -> Result<()> {
    ensure!(
        state.deployment_profile == deployment_profile,
        "Base normalized-event rederive run {:?} belongs to deployment profile {:?}, not {:?}",
        state.run_id,
        state.deployment_profile,
        deployment_profile
    );
    ensure!(
        state.batch_size == batch_size,
        "Base normalized-event rederive run {:?} was started with batch size {}, not {}",
        state.run_id,
        state.batch_size,
        batch_size
    );
    if let Some(requested) = requested_replay_target_block {
        ensure!(
            state.replay_target_block == requested,
            "Base normalized-event rederive run {:?} targets block {}, not requested block {}",
            state.run_id,
            state.replay_target_block,
            requested
        );
    }
    ensure!(
        &state.expected_counts == expected_counts,
        "Base normalized-event rederive run {:?} expected census mismatch: stored {:?}, requested {:?}",
        state.run_id,
        state.expected_counts,
        expected_counts
    );
    Ok(())
}

pub(super) async fn validate_resume_census(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    state: &RunState,
) -> Result<()> {
    let live_counts = load_counts_from(
        transaction,
        &state.deployment_profile,
        state.replay_target_block,
    )
    .await?;
    for (table, deleted, live, expected) in
        resume_census_parts(&state.deleted_counts, &live_counts, &state.expected_counts)
    {
        ensure!(
            deleted + live == expected,
            "Base normalized-event rederive resume census mismatch for {table}: deleted {deleted} + live {live} != reviewed {expected}"
        );
    }
    Ok(())
}

pub(super) fn ensure_step_not_overrun(
    step: Step,
    deleted: &BaseNormalizedRederiveCounts,
    expected: &BaseNormalizedRederiveCounts,
) -> Result<()> {
    ensure!(
        step.count(deleted) <= step.count(expected),
        "Base normalized-event rederive step {} deleted {} rows, exceeding reviewed {}",
        step.as_str(),
        step.count(deleted),
        step.count(expected)
    );
    Ok(())
}

pub(super) fn ensure_step_complete(
    step: Step,
    deleted: &BaseNormalizedRederiveCounts,
    expected: &BaseNormalizedRederiveCounts,
) -> Result<()> {
    ensure!(
        step.count(deleted) == step.count(expected),
        "Base normalized-event rederive step {} completed with {} deleted rows, expected {}",
        step.as_str(),
        step.count(deleted),
        step.count(expected)
    );
    Ok(())
}

fn run_state_from_row(row: sqlx::postgres::PgRow) -> Result<RunState> {
    let expected_counts: Value = row.try_get("expected_counts")?;
    let deleted_counts: Value = row.try_get("deleted_counts")?;
    let plan_snapshot: Value = row.try_get("plan_snapshot")?;
    Ok(RunState {
        run_id: row.try_get("run_id")?,
        deployment_profile: row.try_get("deployment_profile")?,
        replay_target_block: row.try_get("replay_target_block")?,
        batch_size: row.try_get("batch_size")?,
        status: row.try_get("status")?,
        current_step: row.try_get("current_step")?,
        expected_counts: serde_json::from_value(expected_counts)
            .context("failed to decode Base normalized-event rederive expected counts")?,
        deleted_counts: serde_json::from_value(deleted_counts)
            .context("failed to decode Base normalized-event rederive deleted counts")?,
        plan_snapshot: serde_json::from_value(plan_snapshot)
            .context("failed to decode Base normalized-event rederive plan snapshot")?,
    })
}

fn resume_census_parts<'a>(
    deleted: &'a BaseNormalizedRederiveCounts,
    live: &'a BaseNormalizedRederiveCounts,
    expected: &'a BaseNormalizedRederiveCounts,
) -> [(&'static str, i64, i64, i64); 15] {
    [
        (
            "normalized_events",
            deleted.normalized_events,
            live.normalized_events,
            expected.normalized_events,
        ),
        (
            "resources",
            deleted.resources,
            live.resources,
            expected.resources,
        ),
        (
            "token_lineages",
            deleted.token_lineages,
            live.token_lineages,
            expected.token_lineages,
        ),
        (
            "name_surfaces",
            deleted.name_surfaces,
            live.name_surfaces,
            expected.name_surfaces,
        ),
        (
            "surface_bindings",
            deleted.surface_bindings,
            live.surface_bindings,
            expected.surface_bindings,
        ),
        (
            "name_current",
            deleted.name_current,
            live.name_current,
            expected.name_current,
        ),
        (
            "address_names_current",
            deleted.address_names_current,
            live.address_names_current,
            expected.address_names_current,
        ),
        (
            "children_current",
            deleted.children_current,
            live.children_current,
            expected.children_current,
        ),
        (
            "permissions_current",
            deleted.permissions_current,
            live.permissions_current,
            expected.permissions_current,
        ),
        (
            "record_inventory_current",
            deleted.record_inventory_current,
            live.record_inventory_current,
            expected.record_inventory_current,
        ),
        (
            "projection_normalized_event_changes",
            deleted.projection_normalized_event_changes,
            live.projection_normalized_event_changes,
            expected.projection_normalized_event_changes,
        ),
        (
            "current_projection_replay_status",
            deleted.current_projection_replay_status,
            live.current_projection_replay_status,
            expected.current_projection_replay_status,
        ),
        (
            "replay_cursor_rows",
            deleted.replay_cursor_rows,
            live.replay_cursor_rows,
            expected.replay_cursor_rows,
        ),
        (
            "adapter_checkpoint_rows",
            deleted.adapter_checkpoint_rows,
            live.adapter_checkpoint_rows,
            expected.adapter_checkpoint_rows,
        ),
        (
            "adapter_checkpoint_item_rows",
            deleted.adapter_checkpoint_item_rows,
            live.adapter_checkpoint_item_rows,
            expected.adapter_checkpoint_item_rows,
        ),
    ]
}
