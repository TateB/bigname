use anyhow::{Context, Result};
use sqlx::PgPool;
use sqlx::types::time::OffsetDateTime;

use crate::reconciliation::RawFactNormalizedEventReplayOutcome;

use super::{
    CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS, NormalizedReplayCursor, RawLogBounds,
    TargetRefreshPolicy,
};

pub(super) async fn ensure_cursor(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    cursor_kind: &str,
    bounds: RawLogBounds,
    target_refresh_policy: TargetRefreshPolicy,
) -> Result<NormalizedReplayCursor> {
    let row = sqlx::query_as::<_, (i64, i64, i64, Option<OffsetDateTime>)>(
        r#"
        INSERT INTO normalized_replay_cursors (
            deployment_profile,
            chain_id,
            cursor_kind,
            range_start_block_number,
            next_block_number,
            target_block_number
        )
        VALUES ($1, $2, $3, $4, $4, $5)
        ON CONFLICT (deployment_profile, chain_id, cursor_kind) DO UPDATE
        SET
            range_start_block_number = LEAST(
                normalized_replay_cursors.range_start_block_number,
                EXCLUDED.range_start_block_number
            ),
            next_block_number = CASE
                WHEN EXCLUDED.range_start_block_number < normalized_replay_cursors.range_start_block_number
                    THEN LEAST(
                        normalized_replay_cursors.next_block_number,
                        EXCLUDED.range_start_block_number
                    )
                ELSE normalized_replay_cursors.next_block_number
            END,
            target_block_number = CASE
                WHEN $6 THEN GREATEST(
                    normalized_replay_cursors.target_block_number,
                    EXCLUDED.target_block_number
                )
                ELSE normalized_replay_cursors.target_block_number
            END,
            updated_at = now()
        RETURNING
            range_start_block_number,
            next_block_number,
            target_block_number,
            last_replayed_at
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(cursor_kind)
    .bind(bounds.start_block)
    .bind(bounds.target_block)
    .bind(matches!(
        target_refresh_policy,
        TargetRefreshPolicy::RefreshToLatestRawLog
    ))
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to create or refresh normalized replay cursor for {deployment_profile}/{chain}"
        )
    })?;

    Ok(NormalizedReplayCursor {
        range_start_block_number: row.0,
        next_block_number: row.1,
        target_block_number: row.2,
        last_replayed_at: row.3,
    })
}

pub(super) async fn rewind_cursor_for_newly_observed_older_logs(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    cursor: NormalizedReplayCursor,
) -> Result<NormalizedReplayCursor> {
    let Some(last_replayed_at) = cursor.last_replayed_at else {
        return Ok(cursor);
    };

    let rewind_block = sqlx::query_scalar::<_, Option<i64>>(
        r#"
        SELECT MIN(raw_logs.block_number)
        FROM raw_logs
        JOIN chain_lineage AS lineage
          ON lineage.chain_id = raw_logs.chain_id
         AND lineage.block_hash = raw_logs.block_hash
        WHERE raw_logs.chain_id = $1
          AND raw_logs.block_number < $2
          AND raw_logs.observed_at > $3
          AND lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND raw_logs.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        "#,
    )
    .bind(chain)
    .bind(cursor.next_block_number)
    .bind(last_replayed_at)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to inspect newly observed older normalized replay work for {deployment_profile}/{chain}/{CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS}"
        )
    })?;

    let Some(rewind_block) = rewind_block else {
        return Ok(cursor);
    };

    let row = sqlx::query_as::<_, (i64, i64, i64, Option<OffsetDateTime>)>(
        r#"
        UPDATE normalized_replay_cursors
        SET
            range_start_block_number = LEAST(range_start_block_number, $4),
            next_block_number = LEAST(next_block_number, $4),
            updated_at = now()
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
        RETURNING
            range_start_block_number,
            next_block_number,
            target_block_number,
            last_replayed_at
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS)
    .bind(rewind_block)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to rewind normalized replay cursor for {deployment_profile}/{chain}/{CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS} to block {rewind_block}"
        )
    })?;

    Ok(NormalizedReplayCursor {
        range_start_block_number: row.0,
        next_block_number: row.1,
        target_block_number: row.2,
        last_replayed_at: row.3,
    })
}

pub(super) async fn advance_cursor(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    cursor_kind: &str,
    latest_target_block: i64,
    completed_to_block: i64,
    outcome: &RawFactNormalizedEventReplayOutcome,
) -> Result<()> {
    let next_block = completed_to_block
        .checked_add(1)
        .context("normalized replay cursor next block overflowed")?;
    let selected_block_count = i64::try_from(outcome.selected_block_count)
        .context("selected block count does not fit in i64")?;
    let canonical_raw_log_count = i64::try_from(outcome.canonical_raw_log_count)
        .context("canonical raw log count does not fit in i64")?;
    let scanned_raw_log_count = i64::try_from(outcome.scanned_raw_log_count)
        .context("scanned raw log count does not fit in i64")?;
    let matched_raw_log_count = i64::try_from(outcome.matched_raw_log_count)
        .context("matched raw log count does not fit in i64")?;
    let normalized_event_synced_count = i64::try_from(outcome.normalized_event_synced_count)
        .context("normalized event synced count does not fit in i64")?;
    let normalized_event_inserted_count = i64::try_from(outcome.normalized_event_inserted_count)
        .context("normalized event inserted count does not fit in i64")?;

    sqlx::query(
        r#"
        UPDATE normalized_replay_cursors
        SET
            next_block_number = GREATEST(next_block_number, $4),
            target_block_number = GREATEST(target_block_number, $5),
            last_completed_block_number = CASE
                WHEN last_completed_block_number IS NULL THEN $6
                ELSE GREATEST(last_completed_block_number, $6)
            END,
            last_selected_block_count = $7,
            last_canonical_raw_log_count = $8,
            last_scanned_raw_log_count = $9,
            last_matched_raw_log_count = $10,
            last_normalized_event_synced_count = $11,
            last_normalized_event_inserted_count = $12,
            last_replayed_at = now(),
            last_failure_reason = NULL,
            last_failure_at = NULL,
            updated_at = now()
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(cursor_kind)
    .bind(next_block)
    .bind(latest_target_block)
    .bind(completed_to_block)
    .bind(selected_block_count)
    .bind(canonical_raw_log_count)
    .bind(scanned_raw_log_count)
    .bind(matched_raw_log_count)
    .bind(normalized_event_synced_count)
    .bind(normalized_event_inserted_count)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to advance normalized replay cursor for {deployment_profile}/{chain} through block {completed_to_block}"
        )
    })?;

    Ok(())
}

pub(super) async fn record_cursor_failure(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    error: &anyhow::Error,
) -> Result<()> {
    let failure_reason = postgres_text_safe(&format!("{error:#}"));
    sqlx::query(
        r#"
        UPDATE normalized_replay_cursors
        SET
            last_failure_reason = $4,
            last_failure_at = now(),
            updated_at = now()
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS)
    .bind(failure_reason)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to record normalized replay cursor failure for {deployment_profile}/{chain}"
        )
    })?;

    Ok(())
}

fn postgres_text_safe(text: &str) -> String {
    text.replace('\0', "\\u0000")
}
