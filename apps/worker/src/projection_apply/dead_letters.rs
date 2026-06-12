use anyhow::{Context, Result};
use sqlx::PgPool;

use super::apply::ClaimedInvalidation;

pub(super) async fn dead_letter_invalidation(
    pool: &PgPool,
    invalidation: &ClaimedInvalidation,
    failure_reason: &str,
    failed_attempt_count: i64,
) -> Result<u64> {
    let inserted_count = sqlx::query_scalar::<_, i64>(
        r#"
        WITH dead_lettered AS (
            DELETE FROM projection_invalidations
            WHERE projection = $1
              AND projection_key = $2
              AND generation = $3
              AND claim_token = $4
            RETURNING
                projection,
                projection_key,
                key_payload,
                generation,
                first_change_id,
                last_change_id,
                first_normalized_event_id,
                last_normalized_event_id,
                last_changed_at,
                invalidated_at,
                claim_token,
                claimed_at
        ),
        inserted AS (
            INSERT INTO projection_invalidation_dead_letters (
                projection,
                projection_key,
                key_payload,
                generation,
                attempt_count,
                first_change_id,
                last_change_id,
                first_normalized_event_id,
                last_normalized_event_id,
                last_changed_at,
                invalidated_at,
                claim_token,
                claimed_at,
                last_failure_reason,
                last_failure_at,
                dead_lettered_at
            )
            SELECT
                projection,
                projection_key,
                key_payload,
                generation,
                $6,
                first_change_id,
                last_change_id,
                first_normalized_event_id,
                last_normalized_event_id,
                last_changed_at,
                invalidated_at,
                claim_token,
                claimed_at,
                $5,
                now(),
                now()
            FROM dead_lettered
            ON CONFLICT (projection, projection_key, generation)
            DO UPDATE SET
                key_payload = EXCLUDED.key_payload,
                attempt_count = EXCLUDED.attempt_count,
                first_change_id = EXCLUDED.first_change_id,
                last_change_id = EXCLUDED.last_change_id,
                first_normalized_event_id = EXCLUDED.first_normalized_event_id,
                last_normalized_event_id = EXCLUDED.last_normalized_event_id,
                last_changed_at = EXCLUDED.last_changed_at,
                invalidated_at = EXCLUDED.invalidated_at,
                claim_token = EXCLUDED.claim_token,
                claimed_at = EXCLUDED.claimed_at,
                last_failure_reason = EXCLUDED.last_failure_reason,
                last_failure_at = EXCLUDED.last_failure_at,
                dead_lettered_at = EXCLUDED.dead_lettered_at
            RETURNING 1
        )
        SELECT COUNT(*)::BIGINT FROM inserted
        "#,
    )
    .bind(&invalidation.projection)
    .bind(&invalidation.projection_key)
    .bind(invalidation.generation)
    .bind(invalidation.claim_token)
    .bind(failure_reason)
    .bind(failed_attempt_count)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to dead-letter projection invalidation {}:{}",
            invalidation.projection, invalidation.projection_key
        )
    })?;

    u64::try_from(inserted_count).context("dead-letter insert count must fit u64")
}
