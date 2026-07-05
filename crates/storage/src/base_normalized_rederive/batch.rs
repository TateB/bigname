use anyhow::{Context, Result, ensure};
use sqlx::{Acquire, PgConnection, PgPool};
use tracing::info;

mod delete;
mod reset;
mod resume;
mod state;

use super::execution::{
    create_scope_tables, refuse_if_bigname_runtime_sessions,
    refuse_if_out_of_scope_identity_dependencies,
};
use super::guards::{
    ensure_delete_scope_replay_active_from, load_active_replay_target_snapshot_from,
};
use super::profile::validate_base_deployment_profile_owns_chain_from;
use super::{
    BASE_NORMALIZED_REDERIVE_ADVISORY_LOCK_KEY, BaseNormalizedRederiveExecutionOutcome,
    BaseNormalizedRederiveExpectedCounts, base_normalized_rederive_json_digest,
    load_plan_in_transaction, resolve_replay_target_block_from,
};
use delete::delete_step_batch;
use reset::reset_replay_state;
use resume::{
    load_completed_run_plan_with_verified_snapshot_digests, rerun_resume_guards,
    validate_or_upgrade_resume_guard_snapshots,
};
use state::{
    BatchProgress, CountsExt, RunState, Step, ensure_run_matches, ensure_step_complete,
    ensure_step_not_overrun, insert_batch_record, insert_run, load_run_for_update,
    refuse_if_other_running_run, update_run_state, validate_resume_census,
};

pub(super) async fn execute_base_normalized_rederive_drop_batched(
    pool: &PgPool,
    deployment_profile: &str,
    run_id: &str,
    batch_size: i64,
    requested_replay_target_block: Option<i64>,
    expected_counts: BaseNormalizedRederiveExpectedCounts,
    max_delete_batches: Option<usize>,
) -> Result<BaseNormalizedRederiveExecutionOutcome> {
    let mut connection = pool
        .acquire()
        .await
        .context("failed to acquire Base normalized-event rederive connection")?;
    let lock_acquired = sqlx::query_scalar::<_, bool>(
        "SELECT pg_try_advisory_lock(hashtextextended($1::text, 0::bigint))",
    )
    .bind(BASE_NORMALIZED_REDERIVE_ADVISORY_LOCK_KEY)
    .fetch_one(&mut *connection)
    .await
    .context("failed to acquire Base normalized-event rederive session advisory lock")?;
    ensure!(
        lock_acquired,
        "Base normalized-event rederive advisory lock is already held"
    );

    let result = execute_with_session_lock(
        &mut connection,
        deployment_profile,
        run_id,
        batch_size,
        requested_replay_target_block,
        expected_counts,
        max_delete_batches,
    )
    .await;
    let unlock_result = sqlx::query_scalar::<_, bool>(
        "SELECT pg_advisory_unlock(hashtextextended($1::text, 0::bigint))",
    )
    .bind(BASE_NORMALIZED_REDERIVE_ADVISORY_LOCK_KEY)
    .fetch_one(&mut *connection)
    .await
    .context("failed to release Base normalized-event rederive session advisory lock")
    .and_then(|released| {
        ensure!(
            released,
            "Base normalized-event rederive session advisory lock was already released"
        );
        Ok(())
    });

    match (result, unlock_result) {
        (Ok(outcome), Ok(())) => Ok(outcome),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) | (Err(_), Err(error)) => Err(error),
    }
}

#[cfg(test)]
pub(super) async fn execute_base_normalized_rederive_drop_with_batch_limit(
    pool: &PgPool,
    deployment_profile: &str,
    run_id: &str,
    batch_size: i64,
    requested_replay_target_block: Option<i64>,
    expected_counts: BaseNormalizedRederiveExpectedCounts,
    max_delete_batches: usize,
) -> Result<BaseNormalizedRederiveExecutionOutcome> {
    execute_base_normalized_rederive_drop_batched(
        pool,
        deployment_profile,
        run_id,
        batch_size,
        requested_replay_target_block,
        expected_counts,
        Some(max_delete_batches),
    )
    .await
}

async fn execute_with_session_lock(
    connection: &mut PgConnection,
    deployment_profile: &str,
    run_id: &str,
    batch_size: i64,
    requested_replay_target_block: Option<i64>,
    expected_counts: BaseNormalizedRederiveExpectedCounts,
    max_delete_batches: Option<usize>,
) -> Result<BaseNormalizedRederiveExecutionOutcome> {
    let (plan, mut state) = prepare_or_resume_run(
        connection,
        deployment_profile,
        run_id,
        batch_size,
        requested_replay_target_block,
        expected_counts,
    )
    .await?;
    let mut delete_batches = 0usize;

    loop {
        if state.is_completed() {
            return Ok(BaseNormalizedRederiveExecutionOutcome {
                plan,
                deleted: state.deleted_counts,
            });
        }
        if max_delete_batches.is_some_and(|limit| delete_batches >= limit) {
            return Ok(BaseNormalizedRederiveExecutionOutcome {
                plan,
                deleted: state.deleted_counts,
            });
        }
        let progress = execute_next_batch(connection, run_id).await?;
        state = progress.state;
        if progress.deleted_rows > 0 && progress.step != Step::FinalReplayReset {
            delete_batches += 1;
        }
    }
}

async fn prepare_or_resume_run(
    connection: &mut PgConnection,
    deployment_profile: &str,
    run_id: &str,
    batch_size: i64,
    requested_replay_target_block: Option<i64>,
    expected_counts: BaseNormalizedRederiveExpectedCounts,
) -> Result<(super::BaseNormalizedRederivePlan, RunState)> {
    let mut transaction = connection
        .begin()
        .await
        .context("failed to open Base normalized-event rederive start transaction")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *transaction)
        .await
        .context("failed to set Base normalized-event rederive start transaction isolation")?;
    refuse_if_bigname_runtime_sessions(&mut transaction).await?;

    if let Some(mut state) = load_run_for_update(&mut transaction, run_id).await? {
        ensure_run_matches(
            &state,
            deployment_profile,
            batch_size,
            requested_replay_target_block,
            &expected_counts.counts,
        )?;
        ensure!(
            !state.is_aborted(),
            "Base normalized-event rederive run {run_id:?} is aborted; restore the database to a reviewed consistent snapshot and start a new run id"
        );
        let expected_active_snapshot_digest = expected_counts
            .active_replay_target_snapshot_digest
            .as_deref()
            .context("Base normalized-event rederive resume requires reviewed active replay target snapshot digest")?;
        let expected_manifest_snapshot_digest =
            expected_counts
                .active_manifest_snapshot_digest
                .as_deref()
                .context(
                    "Base normalized-event rederive resume requires reviewed active manifest snapshot digest",
                )?;
        if !state.is_completed() {
            validate_or_upgrade_resume_guard_snapshots(
                &mut transaction,
                &mut state,
                expected_active_snapshot_digest,
                expected_manifest_snapshot_digest,
            )
            .await?;
            let plan = rerun_resume_guards(&mut transaction, &state)
                .await
                .context("Base normalized-event rederive resume guard failed")?;
            validate_resume_census(&mut transaction, &state).await?;
            transaction
                .commit()
                .await
                .context("failed to commit Base normalized-event rederive resume validation")?;
            return Ok((plan, state));
        }
        let plan = load_completed_run_plan_with_verified_snapshot_digests(
            &mut transaction,
            &state,
            expected_active_snapshot_digest,
            expected_manifest_snapshot_digest,
        )
        .await?;
        transaction
            .commit()
            .await
            .context("failed to commit Base normalized-event rederive resume validation")?;
        return Ok((plan, state));
    }

    refuse_if_other_running_run(&mut transaction, run_id).await?;
    validate_base_deployment_profile_owns_chain_from(&mut transaction, deployment_profile).await?;
    let (replay_target_block, max_affected_block, replay_target_floor_block, _) =
        resolve_replay_target_block_from(
            &mut transaction,
            deployment_profile,
            requested_replay_target_block,
        )
        .await
        .context("failed to resolve Base normalized-event rederive replay target")?;
    let active_replay_target_snapshot =
        load_active_replay_target_snapshot_from(&mut transaction, replay_target_block).await?;
    ensure_delete_scope_replay_active_from(
        &mut transaction,
        replay_target_block,
        &active_replay_target_snapshot,
    )
    .await?;
    create_scope_tables(&mut transaction, replay_target_block).await?;
    let plan = load_plan_in_transaction(
        &mut transaction,
        deployment_profile,
        replay_target_block,
        max_affected_block,
        replay_target_floor_block,
        active_replay_target_snapshot,
    )
    .await?;
    ensure!(
        plan.raw_fact_completeness.is_complete_for_rerun(),
        "Base normalized-event rederive raw-fact completeness check failed: {:?}",
        plan.raw_fact_completeness
    );
    ensure!(
        expected_counts.counts == plan.counts,
        "Base normalized-event rederive count divergence: expected {:?}, found {:?}",
        expected_counts.counts,
        plan.counts
    );
    let active_snapshot_digest =
        base_normalized_rederive_json_digest(&plan.active_replay_target_snapshot)?;
    let active_manifest_snapshot_digest =
        base_normalized_rederive_json_digest(&plan.active_manifest_snapshot)?;
    ensure!(
        expected_counts
            .active_replay_target_snapshot_digest
            .as_deref()
            == Some(active_snapshot_digest.as_str()),
        "Base normalized-event rederive active replay target snapshot divergence: expected {:?}, found {active_snapshot_digest}",
        expected_counts.active_replay_target_snapshot_digest
    );
    ensure!(
        expected_counts.active_manifest_snapshot_digest.as_deref()
            == Some(active_manifest_snapshot_digest.as_str()),
        "Base normalized-event rederive active manifest snapshot divergence: expected {:?}, found {active_manifest_snapshot_digest}",
        expected_counts.active_manifest_snapshot_digest
    );
    refuse_if_out_of_scope_identity_dependencies(&mut transaction).await?;
    let state = insert_run(
        &mut transaction,
        run_id,
        deployment_profile,
        replay_target_block,
        batch_size,
        &expected_counts.counts,
        &plan,
    )
    .await?;
    transaction
        .commit()
        .await
        .context("failed to commit Base normalized-event rederive run start")?;
    Ok((plan, state))
}

async fn execute_next_batch(connection: &mut PgConnection, run_id: &str) -> Result<BatchProgress> {
    let mut transaction = connection
        .begin()
        .await
        .context("failed to open Base normalized-event rederive batch transaction")?;
    let mut state = load_run_for_update(&mut transaction, run_id)
        .await?
        .with_context(|| format!("Base normalized-event rederive run {run_id:?} does not exist"))?;
    let step = Step::parse(&state.current_step)?;
    if step == Step::Completed || state.is_completed() {
        transaction
            .commit()
            .await
            .context("failed to commit completed Base normalized-event rederive batch")?;
        return Ok(BatchProgress::new(state, step, 0));
    }

    if step == Step::FinalReplayReset {
        let reset_counts = reset_replay_state(&mut transaction, &state).await?;
        state.deleted_counts.add_reset_counts(&reset_counts);
        ensure_step_complete(step, &state.deleted_counts, &state.expected_counts)?;
        state.mark_completed();
        update_run_state(&mut transaction, &state).await?;
        let reset_rows = reset_counts.reset_row_count();
        insert_batch_record(
            &mut transaction,
            run_id,
            step,
            None,
            None,
            reset_rows,
            &state.deleted_counts,
        )
        .await?;
        transaction
            .commit()
            .await
            .context("failed to commit Base normalized-event rederive final reset")?;
        return Ok(BatchProgress::new(state, step, reset_rows));
    }

    let deleted = delete_step_batch(
        &mut transaction,
        step,
        state.batch_size,
        state.replay_target_block,
    )
    .await?;
    if deleted.row_count > 0 {
        state.deleted_counts.add_step(step, deleted.row_count);
        ensure_step_not_overrun(step, &state.deleted_counts, &state.expected_counts)?;
        update_run_state(&mut transaction, &state).await?;
        insert_batch_record(
            &mut transaction,
            run_id,
            step,
            deleted.range_start.clone(),
            deleted.range_end.clone(),
            deleted.row_count,
            &state.deleted_counts,
        )
        .await?;
        info!(
            run_id,
            step = step.as_str(),
            rows = deleted.row_count,
            range_start = deleted.range_start,
            range_end = deleted.range_end,
            "Base normalized-event rederive batch committed"
        );
    } else {
        ensure_step_complete(step, &state.deleted_counts, &state.expected_counts)?;
        state.advance_step(step.next());
        update_run_state(&mut transaction, &state).await?;
    }
    transaction
        .commit()
        .await
        .context("failed to commit Base normalized-event rederive batch")?;
    Ok(BatchProgress::new(state, step, deleted.row_count))
}
