use anyhow::{Context, Result, ensure};

use super::super::guards::{
    ensure_delete_scope_replay_active_from, load_active_replay_target_snapshot_from,
};
use super::super::manifest_snapshot::load_active_manifest_snapshot_from;
use super::super::{
    BaseNormalizedRederivePlan, base_normalized_rederive_json_digest,
    load_raw_fact_completeness_from, load_raw_fact_range_proof_from,
    resolve_replay_target_block_from,
};
use super::state::{RunState, update_run_state};

pub(super) async fn load_completed_run_plan_with_verified_snapshot_digests(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    state: &RunState,
    expected_active_snapshot_digest: &str,
    expected_manifest_snapshot_digest: &str,
) -> Result<BaseNormalizedRederivePlan> {
    let current_replay_target_snapshot =
        load_active_replay_target_snapshot_from(transaction, state.replay_target_block).await?;
    let current_target_digest =
        base_normalized_rederive_json_digest(&current_replay_target_snapshot)?;
    let stored_target_digest = state
        .plan_snapshot
        .stored_active_replay_target_snapshot_digest()?
        .with_context(|| {
            format!(
                "completed Base normalized-event rederive run {:?} lacks durable active replay target snapshot digest",
                state.run_id
            )
        })?;
    ensure!(
        stored_target_digest == expected_active_snapshot_digest,
        "Base normalized-event rederive active replay target snapshot digest mismatch for completed run {:?}: stored {stored_target_digest}, requested {expected_active_snapshot_digest}",
        state.run_id
    );
    ensure!(
        current_target_digest == stored_target_digest,
        "Base normalized-event rederive active replay target snapshot changed since completed run {:?}: stored {stored_target_digest}, current {current_target_digest}",
        state.run_id
    );

    let current_manifest_snapshot = load_active_manifest_snapshot_from(transaction).await?;
    let current_manifest_digest = base_normalized_rederive_json_digest(&current_manifest_snapshot)?;
    let stored_manifest_digest = state
        .plan_snapshot
        .stored_active_manifest_snapshot_digest()?
        .with_context(|| {
            format!(
                "completed Base normalized-event rederive run {:?} lacks durable active manifest snapshot digest",
                state.run_id
            )
        })?;
    ensure!(
        stored_manifest_digest == expected_manifest_snapshot_digest,
        "Base normalized-event rederive active manifest snapshot digest mismatch for completed run {:?}: stored {stored_manifest_digest}, requested {expected_manifest_snapshot_digest}",
        state.run_id
    );
    ensure!(
        current_manifest_digest == stored_manifest_digest,
        "Base normalized-event rederive active manifest snapshot changed since completed run {:?}: stored {stored_manifest_digest}, current {current_manifest_digest}",
        state.run_id
    );

    Ok(state
        .plan_snapshot
        .to_plan_with_snapshots(current_replay_target_snapshot, current_manifest_snapshot))
}

pub(super) async fn validate_or_upgrade_resume_guard_snapshots(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    state: &mut RunState,
    expected_active_snapshot_digest: &str,
    expected_manifest_snapshot_digest: &str,
) -> Result<()> {
    let mut changed = false;
    changed |= validate_or_upgrade_active_replay_target_snapshot_digest(
        transaction,
        state,
        expected_active_snapshot_digest,
    )
    .await?;
    changed |= validate_or_upgrade_active_manifest_snapshot_digest(
        transaction,
        state,
        expected_manifest_snapshot_digest,
    )
    .await?;
    if state.plan_snapshot.raw_fact_range_proof.is_empty() {
        ensure!(
            state.deleted_counts.normalized_events == 0,
            "Base normalized-event rederive run {:?} lacks a durable raw-fact range proof and has already deleted normalized events; restart with a new reviewed run id after operator review",
            state.run_id
        );
        state.plan_snapshot.raw_fact_range_proof =
            load_raw_fact_range_proof_from(transaction, state.replay_target_block).await?;
        changed = true;
    }
    if changed {
        update_run_state(transaction, state).await?;
    }
    Ok(())
}

pub(super) async fn rerun_resume_guards(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    state: &RunState,
) -> Result<BaseNormalizedRederivePlan> {
    let (target, _, _, _) = resolve_replay_target_block_from(
        transaction,
        &state.deployment_profile,
        Some(state.replay_target_block),
    )
    .await?;
    ensure!(
        target == state.replay_target_block,
        "Base normalized-event rederive run {:?} resolved replay target {target}, not stored target {}",
        state.run_id,
        state.replay_target_block
    );
    let current_replay_target_snapshot =
        load_active_replay_target_snapshot_from(transaction, state.replay_target_block).await?;
    let current_target_digest =
        base_normalized_rederive_json_digest(&current_replay_target_snapshot)?;
    let stored_target_digest = state
        .plan_snapshot
        .stored_active_replay_target_snapshot_digest()?
        .with_context(|| {
            format!(
                "Base normalized-event rederive run {:?} lacks durable active replay target snapshot digest",
                state.run_id
            )
        })?;
    ensure!(
        current_target_digest == stored_target_digest,
        "Base normalized-event rederive active replay target snapshot changed during run {:?}: stored {stored_target_digest}, current {current_target_digest}",
        state.run_id
    );

    let current_manifest_snapshot = load_active_manifest_snapshot_from(transaction).await?;
    let current_manifest_digest = base_normalized_rederive_json_digest(&current_manifest_snapshot)?;
    let stored_manifest_digest = state
        .plan_snapshot
        .stored_active_manifest_snapshot_digest()?
        .with_context(|| {
            format!(
                "Base normalized-event rederive run {:?} lacks durable active manifest snapshot digest",
                state.run_id
            )
        })?;
    ensure!(
        current_manifest_digest == stored_manifest_digest,
        "Base normalized-event rederive active manifest snapshot changed during run {:?}: stored {stored_manifest_digest}, current {current_manifest_digest}",
        state.run_id
    );

    ensure_delete_scope_replay_active_from(
        transaction,
        state.replay_target_block,
        &current_replay_target_snapshot,
    )
    .await?;
    let current_raw_fact_range_proof =
        load_raw_fact_range_proof_from(transaction, state.replay_target_block).await?;
    ensure!(
        current_raw_fact_range_proof == state.plan_snapshot.raw_fact_range_proof,
        "Base normalized-event rederive raw-fact range proof changed during run {:?}: stored {:?}, current {:?}",
        state.run_id,
        state.plan_snapshot.raw_fact_range_proof,
        current_raw_fact_range_proof
    );
    let raw_fact_completeness =
        load_raw_fact_completeness_from(transaction, state.replay_target_block).await?;
    ensure!(
        raw_fact_completeness.is_complete_for_rerun(),
        "Base normalized-event rederive raw-fact completeness check failed on resume: {:?}",
        raw_fact_completeness
    );
    Ok(state
        .plan_snapshot
        .to_plan_with_snapshots(current_replay_target_snapshot, current_manifest_snapshot))
}

async fn validate_or_upgrade_active_replay_target_snapshot_digest(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    state: &mut RunState,
    expected_active_snapshot_digest: &str,
) -> Result<bool> {
    if let Some(stored_digest) = state
        .plan_snapshot
        .stored_active_replay_target_snapshot_digest()?
    {
        ensure!(
            stored_digest == expected_active_snapshot_digest,
            "Base normalized-event rederive active replay target snapshot digest mismatch for run {:?}: stored {stored_digest}, requested {expected_active_snapshot_digest}",
            state.run_id
        );
        if let Some(legacy_digest) = state
            .plan_snapshot
            .legacy_active_replay_target_snapshot_digest()?
        {
            ensure!(
                legacy_digest == stored_digest,
                "Base normalized-event rederive legacy active replay target snapshot digest mismatch for run {:?}: stored digest {stored_digest}, legacy rows {legacy_digest}",
                state.run_id
            );
            state
                .plan_snapshot
                .store_active_replay_target_snapshot_digest(stored_digest);
            return Ok(true);
        }
        return Ok(false);
    }

    ensure!(
        state.deleted_counts.normalized_events == 0,
        "Base normalized-event rederive run {:?} lacks a durable active replay target snapshot digest and has already deleted normalized events; restart with a new reviewed run id after operator review",
        state.run_id
    );
    let current_snapshot =
        load_active_replay_target_snapshot_from(transaction, state.replay_target_block).await?;
    let upgraded_digest = base_normalized_rederive_json_digest(&current_snapshot)?;
    ensure!(
        upgraded_digest == expected_active_snapshot_digest,
        "Base normalized-event rederive legacy active replay target snapshot divergence for run {:?}: reviewed {expected_active_snapshot_digest}, current {upgraded_digest}",
        state.run_id
    );
    state
        .plan_snapshot
        .store_active_replay_target_snapshot_digest(upgraded_digest);
    Ok(true)
}

async fn validate_or_upgrade_active_manifest_snapshot_digest(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    state: &mut RunState,
    expected_manifest_snapshot_digest: &str,
) -> Result<bool> {
    if let Some(stored_digest) = state
        .plan_snapshot
        .stored_active_manifest_snapshot_digest()?
    {
        ensure!(
            stored_digest == expected_manifest_snapshot_digest,
            "Base normalized-event rederive active manifest snapshot digest mismatch for run {:?}: stored {stored_digest}, requested {expected_manifest_snapshot_digest}",
            state.run_id
        );
        if let Some(legacy_digest) = state
            .plan_snapshot
            .legacy_active_manifest_snapshot_digest()?
        {
            ensure!(
                legacy_digest == stored_digest,
                "Base normalized-event rederive legacy active manifest snapshot digest mismatch for run {:?}: stored digest {stored_digest}, legacy rows {legacy_digest}",
                state.run_id
            );
            state
                .plan_snapshot
                .store_active_manifest_snapshot_digest(stored_digest);
            return Ok(true);
        }
        return Ok(false);
    }

    ensure!(
        state.deleted_counts.normalized_events == 0,
        "Base normalized-event rederive run {:?} lacks a durable active manifest snapshot digest and has already deleted normalized events; restart with a new reviewed run id after operator review",
        state.run_id
    );
    let current_snapshot = load_active_manifest_snapshot_from(transaction).await?;
    let upgraded_digest = base_normalized_rederive_json_digest(&current_snapshot)?;
    ensure!(
        upgraded_digest == expected_manifest_snapshot_digest,
        "Base normalized-event rederive legacy active manifest snapshot divergence for run {:?}: reviewed {expected_manifest_snapshot_digest}, current {upgraded_digest}",
        state.run_id
    );
    state
        .plan_snapshot
        .store_active_manifest_snapshot_digest(upgraded_digest);
    Ok(true)
}
