use anyhow::{Context, Result, bail};
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgPool, Row};

use super::{
    complete::set_backfill_job_completed,
    decode::decode_backfill_range,
    read::{
        incomplete_range_count, load_active_backfill_range_by_lease, load_backfill_job_for_update,
        load_backfill_range_for_update,
    },
    sql::backfill_range_returning_sql,
    types::{BackfillLifecycleStatus, BackfillRange},
    validate::{ensure_lease_is_active, ensure_lease_matches, validate_lease, validate_non_empty},
};

/// Atomically reserve the next pending, failed, or expired range for a job.
/// Reusing the same active lease token and owner returns the existing range.
pub async fn reserve_backfill_range(
    pool: &PgPool,
    backfill_job_id: i64,
    lease_owner: &str,
    lease_token: &str,
    lease_expires_at: OffsetDateTime,
) -> Result<Option<BackfillRange>> {
    validate_lease(lease_owner, lease_token, lease_expires_at)?;

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for backfill range reservation")?;

    let job = load_backfill_job_for_update(&mut *transaction, backfill_job_id)
        .await?
        .with_context(|| format!("missing backfill job {backfill_job_id}"))?;
    if job.status == BackfillLifecycleStatus::Completed {
        transaction
            .commit()
            .await
            .context("failed to commit completed backfill job reservation no-op")?;
        return Ok(None);
    }

    if let Some(existing) = load_active_backfill_range_by_lease(
        &mut *transaction,
        backfill_job_id,
        lease_owner,
        lease_token,
    )
    .await?
    {
        transaction
            .commit()
            .await
            .context("failed to commit duplicate backfill range reservation")?;
        return Ok(Some(existing));
    }

    let candidate = sqlx::query(
        r#"
        SELECT backfill_range_id
        FROM backfill_ranges
        WHERE backfill_job_id = $1
          AND (
            status IN ('pending'::backfill_lifecycle_status, 'failed'::backfill_lifecycle_status)
            OR (
              status IN ('reserved'::backfill_lifecycle_status, 'running'::backfill_lifecycle_status)
              AND lease_expires_at <= now()
            )
          )
        ORDER BY
          CASE
            WHEN status IN ('reserved'::backfill_lifecycle_status, 'running'::backfill_lifecycle_status) THEN 0
            WHEN status = 'pending'::backfill_lifecycle_status THEN 1
            WHEN status = 'failed'::backfill_lifecycle_status THEN 2
            ELSE 3
          END,
          range_start_block_number,
          range_end_block_number
        LIMIT 1
        FOR UPDATE SKIP LOCKED
        "#,
    )
    .bind(backfill_job_id)
    .fetch_optional(&mut *transaction)
    .await
    .with_context(|| format!("failed to select reservable range for backfill job {backfill_job_id}"))?;

    let Some(candidate) = candidate else {
        if incomplete_range_count(&mut *transaction, backfill_job_id).await? == 0 {
            set_backfill_job_completed(&mut transaction, backfill_job_id).await?;
        }
        transaction
            .commit()
            .await
            .context("failed to commit empty backfill range reservation")?;
        return Ok(None);
    };
    let backfill_range_id = candidate
        .try_get::<i64, _>("backfill_range_id")
        .context("missing backfill_range_id from reservable range row")?;

    let reserve_sql = backfill_range_returning_sql(
        r#"
        UPDATE backfill_ranges
        SET
            status = 'reserved'::backfill_lifecycle_status,
            lease_token = $2,
            lease_owner = $3,
            lease_expires_at = $4,
            attempt_count = attempt_count + 1,
            failure_reason = NULL,
            failure_metadata = '{}'::jsonb,
            completed_at = NULL,
            updated_at = now()
        WHERE backfill_range_id = $1
        "#,
    );
    let range = sqlx::query(&reserve_sql)
        .bind(backfill_range_id)
        .bind(lease_token)
        .bind(lease_owner)
        .bind(lease_expires_at)
        .fetch_one(&mut *transaction)
        .await
        .with_context(|| format!("failed to reserve backfill range {backfill_range_id}"))?;
    let range = decode_backfill_range(range)?;

    sqlx::query(
        r#"
        UPDATE backfill_jobs
        SET
            status = 'reserved'::backfill_lifecycle_status,
            failure_reason = NULL,
            failure_metadata = '{}'::jsonb,
            completed_at = NULL,
            updated_at = now()
        WHERE backfill_job_id = $1
          AND status <> 'completed'::backfill_lifecycle_status
        "#,
    )
    .bind(backfill_job_id)
    .execute(&mut *transaction)
    .await
    .with_context(|| format!("failed to mark backfill job {backfill_job_id} reserved"))?;

    transaction
        .commit()
        .await
        .context("failed to commit backfill range reservation")?;

    Ok(Some(range))
}

/// Move a leased range checkpoint forward monotonically.
pub async fn advance_backfill_range(
    pool: &PgPool,
    backfill_range_id: i64,
    lease_token: &str,
    checkpoint_block_number: i64,
) -> Result<BackfillRange> {
    validate_non_empty("lease_token", lease_token)?;

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for backfill range advance")?;

    let current = load_backfill_range_for_update(&mut *transaction, backfill_range_id)
        .await?
        .with_context(|| format!("missing backfill range {backfill_range_id}"))?;

    if current.status == BackfillLifecycleStatus::Completed {
        if checkpoint_block_number <= current.checkpoint_block_number {
            transaction
                .commit()
                .await
                .context("failed to commit completed backfill range advance no-op")?;
            return Ok(current);
        }
        bail!("completed backfill range {backfill_range_id} cannot advance beyond completion");
    }
    if current.status == BackfillLifecycleStatus::Failed {
        bail!("failed backfill range {backfill_range_id} must be reserved again before advancing");
    }
    ensure_lease_matches(&current, lease_token)?;
    ensure_lease_is_active(&current)?;
    if checkpoint_block_number > current.range_end_block_number {
        bail!(
            "backfill range {backfill_range_id} checkpoint {checkpoint_block_number} is beyond declared range end {}",
            current.range_end_block_number
        );
    }
    if checkpoint_block_number < current.checkpoint_block_number {
        transaction
            .commit()
            .await
            .context("failed to commit stale backfill range advance no-op")?;
        return Ok(current);
    }

    let advance_sql = backfill_range_returning_sql(
        r#"
        UPDATE backfill_ranges
        SET
            checkpoint_block_number = $2,
            status = 'running'::backfill_lifecycle_status,
            lease_expires_at = now() + GREATEST(
                lease_expires_at - updated_at,
                interval '5 seconds'
            ),
            updated_at = now()
        WHERE backfill_range_id = $1
        "#,
    );
    let range = sqlx::query(&advance_sql)
        .bind(backfill_range_id)
        .bind(checkpoint_block_number)
        .fetch_one(&mut *transaction)
        .await
        .with_context(|| format!("failed to advance backfill range {backfill_range_id}"))?;
    let range = decode_backfill_range(range)?;

    sqlx::query(
        r#"
        UPDATE backfill_jobs
        SET
            status = 'running'::backfill_lifecycle_status,
            updated_at = now()
        WHERE backfill_job_id = $1
          AND status <> 'completed'::backfill_lifecycle_status
        "#,
    )
    .bind(range.backfill_job_id)
    .execute(&mut *transaction)
    .await
    .with_context(|| {
        format!(
            "failed to mark backfill job {} running",
            range.backfill_job_id
        )
    })?;

    transaction
        .commit()
        .await
        .context("failed to commit backfill range advance")?;

    Ok(range)
}
