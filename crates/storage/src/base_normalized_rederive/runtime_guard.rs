use anyhow::{Context, Result, ensure};
use sqlx::{PgPool, Postgres, Row, pool::PoolConnection};
use tracing::info;

use super::guards::load_active_replay_target_snapshot;
use super::manifest_snapshot::load_active_manifest_snapshot;
use super::{
    BASE_NORMALIZED_REDERIVE_ADVISORY_LOCK_KEY, BASE_NORMALIZED_REDERIVE_CHAIN_ID,
    BASE_NORMALIZED_REDERIVE_CURSOR_KIND, BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK,
    BaseNormalizedRederiveReplayTargetSnapshot, base_normalized_rederive_json_digest,
};
use crate::BaseNormalizedRederiveActiveManifestSnapshot;

pub async fn hold_base_normalized_rederive_runtime_shared_lock(
    pool: &PgPool,
    service: &str,
) -> Result<PoolConnection<Postgres>> {
    let mut connection = pool
        .acquire()
        .await
        .context("failed to acquire runtime guard connection")?;
    refuse_if_incomplete_base_normalized_rederive_run(&mut connection).await?;
    sqlx::query("SELECT pg_advisory_lock_shared(hashtextextended($1::text, 0::bigint))")
        .bind(BASE_NORMALIZED_REDERIVE_ADVISORY_LOCK_KEY)
        .execute(&mut *connection)
        .await
        .context("failed to acquire Base normalized-event rederive runtime shared lock")?;
    refuse_if_incomplete_base_normalized_rederive_run(&mut connection).await?;
    info!(
        service,
        lock = BASE_NORMALIZED_REDERIVE_ADVISORY_LOCK_KEY,
        "runtime holds Base normalized-event rederive shared advisory lock"
    );
    Ok(connection)
}

pub async fn ensure_base_normalized_rederive_replay_manifest_snapshot_current(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    replay_target_block: i64,
) -> Result<()> {
    if chain != BASE_NORMALIZED_REDERIVE_CHAIN_ID {
        return Ok(());
    }
    if !run_table_exists(pool).await? {
        return Ok(());
    }

    let row = sqlx::query(
        r#"
        SELECT run_id,
               replay_target_block,
               plan_snapshot -> 'active_replay_target_snapshot' AS target_snapshot,
               plan_snapshot -> 'active_manifest_snapshot' AS manifest_snapshot
        FROM base_normalized_rederive_runs
        WHERE chain_id = $1
          AND deployment_profile = $2
          AND status = 'completed'
        ORDER BY completed_at DESC, updated_at DESC, run_id ASC
        LIMIT 1
        "#,
    )
    .bind(BASE_NORMALIZED_REDERIVE_CHAIN_ID)
    .bind(deployment_profile)
    .fetch_optional(pool)
    .await
    .context("failed to load completed Base normalized-event rederive manifest snapshot")?;
    let Some(row) = row else {
        return Ok(());
    };

    let run_id: String = row.try_get("run_id")?;
    let reviewed_target: i64 = row.try_get("replay_target_block")?;
    ensure!(
        reviewed_target == replay_target_block,
        "Base normalized-event rederive replay target {replay_target_block} does not match reviewed completed run {run_id:?} target {reviewed_target}; rerun dry-run/execute review before replay"
    );
    let reviewed_target_snapshot: Vec<BaseNormalizedRederiveReplayTargetSnapshot> =
        serde_json::from_value(row.try_get("target_snapshot")?).with_context(|| {
            format!(
                "completed Base normalized-event rederive run {run_id:?} has invalid reviewed replay target snapshot"
            )
        })?;
    let reviewed_target_digest = base_normalized_rederive_json_digest(&reviewed_target_snapshot)?;
    let current_snapshot = load_active_replay_target_snapshot(pool, replay_target_block)
        .await
        .context("failed to load current Base replay manifest snapshot")?;
    let current_target_digest = base_normalized_rederive_json_digest(&current_snapshot)?;
    ensure!(
        current_target_digest == reviewed_target_digest,
        "Base normalized-event rederive replay target snapshot changed since reviewed run {run_id:?}: reviewed {reviewed_target_digest}, current {current_target_digest}; pin replay to the reviewed stored manifest or repeat dry-run/execute review"
    );

    let reviewed_manifest_snapshot: Vec<BaseNormalizedRederiveActiveManifestSnapshot> =
        serde_json::from_value(row.try_get("manifest_snapshot")?).with_context(|| {
            format!(
                "completed Base normalized-event rederive run {run_id:?} has invalid reviewed active manifest snapshot"
            )
        })?;
    let reviewed_manifest_digest =
        base_normalized_rederive_json_digest(&reviewed_manifest_snapshot)?;
    let current_manifest_snapshot = load_active_manifest_snapshot(pool)
        .await
        .context("failed to load current Base active manifest snapshot")?;
    let current_manifest_digest = base_normalized_rederive_json_digest(&current_manifest_snapshot)?;
    ensure!(
        current_manifest_digest == reviewed_manifest_digest,
        "Base normalized-event rederive active manifest snapshot changed since reviewed run {run_id:?}: reviewed {reviewed_manifest_digest}, current {current_manifest_digest}; pin replay to the reviewed stored manifest or repeat dry-run/execute review"
    );
    Ok(())
}

pub async fn pending_base_normalized_rederive_replay_target(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
) -> Result<Option<i64>> {
    if chain != BASE_NORMALIZED_REDERIVE_CHAIN_ID {
        return Ok(None);
    }
    if !run_table_exists(pool).await? {
        return Ok(None);
    }

    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT r.replay_target_block
        FROM base_normalized_rederive_runs r
        JOIN normalized_replay_cursors c
          ON c.deployment_profile = r.deployment_profile
         AND c.chain_id = r.chain_id
         AND c.cursor_kind = $3
        WHERE r.chain_id = $1
          AND r.deployment_profile = $2
          AND r.status = 'completed'
          AND c.range_start_block_number = $4
          AND c.target_block_number = r.replay_target_block
          AND c.next_block_number <= c.target_block_number
        ORDER BY r.completed_at DESC, r.updated_at DESC, r.run_id ASC
        LIMIT 1
        "#,
    )
    .bind(BASE_NORMALIZED_REDERIVE_CHAIN_ID)
    .bind(deployment_profile)
    .bind(BASE_NORMALIZED_REDERIVE_CURSOR_KIND)
    .bind(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK)
    .fetch_optional(pool)
    .await
    .context("failed to load pending Base normalized-event rederive replay target")
}

pub async fn refuse_base_normalized_rederive_manifest_sync_during_pending_replay(
    pool: &PgPool,
) -> Result<()> {
    let pending = pending_base_normalized_rederive_replay_rows(pool).await?;
    ensure!(
        pending.is_empty(),
        "refusing manifest sync while Base normalized-event rederive replay is pending: {:?}. Complete the reviewed catch-up replay before syncing a different manifest snapshot.",
        pending
    );
    Ok(())
}

pub async fn base_normalized_rederive_manifest_sync_pending_replay(pool: &PgPool) -> Result<bool> {
    Ok(!pending_base_normalized_rederive_replay_rows(pool)
        .await?
        .is_empty())
}

async fn pending_base_normalized_rederive_replay_rows(pool: &PgPool) -> Result<Vec<String>> {
    if !run_table_exists(pool).await? {
        return Ok(Vec::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT r.run_id, r.deployment_profile, r.replay_target_block, c.next_block_number
        FROM base_normalized_rederive_runs r
        JOIN normalized_replay_cursors c
          ON c.deployment_profile = r.deployment_profile
         AND c.chain_id = r.chain_id
         AND c.cursor_kind = $2
        WHERE r.chain_id = $1
          AND r.status = 'completed'
          AND c.range_start_block_number = $3
          AND c.target_block_number = r.replay_target_block
          AND c.next_block_number <= c.target_block_number
        ORDER BY r.completed_at DESC, r.updated_at DESC, r.run_id ASC
        "#,
    )
    .bind(BASE_NORMALIZED_REDERIVE_CHAIN_ID)
    .bind(BASE_NORMALIZED_REDERIVE_CURSOR_KIND)
    .bind(BASE_NORMALIZED_REDERIVE_REPLAY_START_BLOCK)
    .fetch_all(pool)
    .await
    .context(
        "failed to inspect pending Base normalized-event rederive replay before manifest sync",
    )?;
    Ok(rows
        .iter()
        .map(|row| {
            format!(
                "{} profile={} next={} target={}",
                row.get::<String, _>("run_id"),
                row.get::<String, _>("deployment_profile"),
                row.get::<i64, _>("next_block_number"),
                row.get::<i64, _>("replay_target_block")
            )
        })
        .collect::<Vec<_>>())
}

async fn refuse_if_incomplete_base_normalized_rederive_run(
    connection: &mut PoolConnection<Postgres>,
) -> Result<()> {
    if !run_table_exists_on_connection(connection).await? {
        return Ok(());
    }

    let rows = sqlx::query(
        r#"
        SELECT run_id, status, current_step
        FROM base_normalized_rederive_runs
        WHERE chain_id = $1
          AND status NOT IN ('completed', 'aborted')
        ORDER BY updated_at DESC, run_id ASC
        "#,
    )
    .bind(BASE_NORMALIZED_REDERIVE_CHAIN_ID)
    .fetch_all(&mut **connection)
    .await
    .context("failed to inspect incomplete Base normalized-event rederive runs")?;
    ensure!(
        rows.is_empty(),
        "refusing to start writer while Base normalized-event rederive run is incomplete: {:?}. Resume and complete the run, or restore the database to a consistent pre-run snapshot and mark the run aborted before starting writers.",
        rows.iter()
            .map(|row| {
                format!(
                    "{} status={} step={}",
                    row.get::<String, _>("run_id"),
                    row.get::<String, _>("status"),
                    row.get::<String, _>("current_step")
                )
            })
            .collect::<Vec<_>>()
    );
    Ok(())
}

async fn run_table_exists(pool: &PgPool) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        "SELECT to_regclass('public.base_normalized_rederive_runs') IS NOT NULL",
    )
    .fetch_one(pool)
    .await
    .context("failed to inspect Base normalized-event rederive run-state table")
}

async fn run_table_exists_on_connection(connection: &mut PoolConnection<Postgres>) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        "SELECT to_regclass('public.base_normalized_rederive_runs') IS NOT NULL",
    )
    .fetch_one(&mut **connection)
    .await
    .context("failed to inspect Base normalized-event rederive run-state table")
}
