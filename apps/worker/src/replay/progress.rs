use anyhow::{Context, Result};
use sqlx::PgPool;

use super::CurrentProjectionReplayStepSummary;

pub const CURRENT_PROJECTION_REPLAY_VERSION: i32 = 1;

pub(super) async fn projection_replay_completed(
    pool: &PgPool,
    projection: &str,
    normalized_target_block: Option<i64>,
) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM current_projection_replay_status
            WHERE projection = $1
              AND replay_version = $2
              AND (
                  $3::BIGINT IS NULL
                  OR completed_normalized_target_block >= $3
              )
        )
        "#,
    )
    .bind(projection)
    .bind(CURRENT_PROJECTION_REPLAY_VERSION)
    .bind(normalized_target_block)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to inspect replay status for {projection}"))
}

pub(super) async fn clear_projection_replay_completed(
    pool: &PgPool,
    projection: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        DELETE FROM current_projection_replay_status
        WHERE projection = $1
        "#,
    )
    .bind(projection)
    .execute(pool)
    .await
    .with_context(|| format!("failed to clear replay status for {projection}"))?;

    Ok(())
}

pub(super) async fn mark_projection_replay_completed(
    pool: &PgPool,
    step: &CurrentProjectionReplayStepSummary,
    normalized_target_block: Option<i64>,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO current_projection_replay_status (
            projection,
            replay_version,
            completed_normalized_target_block,
            requested_key_count,
            upserted_row_count,
            deleted_row_count,
            completed_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, now())
        ON CONFLICT (projection)
        DO UPDATE SET
            replay_version = EXCLUDED.replay_version,
            completed_normalized_target_block = EXCLUDED.completed_normalized_target_block,
            requested_key_count = EXCLUDED.requested_key_count,
            upserted_row_count = EXCLUDED.upserted_row_count,
            deleted_row_count = EXCLUDED.deleted_row_count,
            completed_at = EXCLUDED.completed_at
        "#,
    )
    .bind(step.projection)
    .bind(CURRENT_PROJECTION_REPLAY_VERSION)
    .bind(normalized_target_block)
    .bind(i64::try_from(step.requested_key_count).context("requested key count overflow")?)
    .bind(i64::try_from(step.upserted_row_count).context("upserted row count overflow")?)
    .bind(i64::try_from(step.deleted_row_count).context("deleted row count overflow")?)
    .execute(pool)
    .await
    .with_context(|| format!("failed to mark replay status for {}", step.projection))?;

    Ok(())
}
