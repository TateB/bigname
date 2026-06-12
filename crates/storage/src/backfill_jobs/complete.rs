use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Postgres};

use super::{
    decode::{decode_backfill_job, decode_backfill_range},
    read::{
        incomplete_range_count, load_backfill_job_for_update, load_backfill_range_for_update,
        load_backfill_ranges_for_update,
    },
    sql::{backfill_job_returning_sql, backfill_range_returning_sql},
    types::{BackfillJob, BackfillLifecycleStatus, BackfillRange},
    validate::{ensure_lease_matches, ensure_ranges_ready_for_job_completion, validate_non_empty},
};

/// Complete a leased range after its checkpoint reaches the declared end.
pub async fn complete_backfill_range(
    pool: &PgPool,
    backfill_range_id: i64,
    lease_token: &str,
) -> Result<BackfillRange> {
    validate_non_empty("lease_token", lease_token)?;

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for backfill range completion")?;

    let current = load_backfill_range_for_update(&mut *transaction, backfill_range_id)
        .await?
        .with_context(|| format!("missing backfill range {backfill_range_id}"))?;
    if current.status == BackfillLifecycleStatus::Completed {
        transaction
            .commit()
            .await
            .context("failed to commit completed backfill range no-op")?;
        return Ok(current);
    }
    if current.status == BackfillLifecycleStatus::Failed {
        bail!("failed backfill range {backfill_range_id} must be reserved again before completion");
    }
    ensure_lease_matches(&current, lease_token)?;
    if current.checkpoint_block_number != current.range_end_block_number {
        bail!(
            "backfill range {backfill_range_id} checkpoint {} has not reached declared range end {}",
            current.checkpoint_block_number,
            current.range_end_block_number
        );
    }

    let complete_sql = backfill_range_returning_sql(
        r#"
        UPDATE backfill_ranges
        SET
            status = 'completed'::backfill_lifecycle_status,
            lease_token = NULL,
            lease_owner = NULL,
            lease_expires_at = NULL,
            failure_reason = NULL,
            failure_metadata = '{}'::jsonb,
            completed_at = COALESCE(completed_at, now()),
            updated_at = now()
        WHERE backfill_range_id = $1
        "#,
    );
    let range = sqlx::query(&complete_sql)
        .bind(backfill_range_id)
        .fetch_one(&mut *transaction)
        .await
        .with_context(|| format!("failed to complete backfill range {backfill_range_id}"))?;
    let range = decode_backfill_range(range)?;

    maybe_complete_backfill_job(&mut transaction, range.backfill_job_id).await?;

    transaction
        .commit()
        .await
        .context("failed to commit backfill range completion")?;

    Ok(range)
}

/// Complete a job when all child range checkpoints have reached their declared
/// ends. This is idempotent when the job is already complete.
pub async fn complete_backfill_job(pool: &PgPool, backfill_job_id: i64) -> Result<BackfillJob> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for backfill job completion")?;

    let current = load_backfill_job_for_update(&mut *transaction, backfill_job_id)
        .await?
        .with_context(|| format!("missing backfill job {backfill_job_id}"))?;
    if current.status == BackfillLifecycleStatus::Completed {
        transaction
            .commit()
            .await
            .context("failed to commit completed backfill job no-op")?;
        return Ok(current);
    }

    let ranges = load_backfill_ranges_for_update(&mut *transaction, backfill_job_id).await?;
    ensure_ranges_ready_for_job_completion(backfill_job_id, &ranges)?;

    sqlx::query(
        r#"
        UPDATE backfill_ranges
        SET
            status = 'completed'::backfill_lifecycle_status,
            lease_token = NULL,
            lease_owner = NULL,
            lease_expires_at = NULL,
            failure_reason = NULL,
            failure_metadata = '{}'::jsonb,
            completed_at = COALESCE(completed_at, now()),
            updated_at = now()
        WHERE backfill_job_id = $1
          AND status <> 'completed'::backfill_lifecycle_status
        "#,
    )
    .bind(backfill_job_id)
    .execute(&mut *transaction)
    .await
    .with_context(|| format!("failed to complete ranges for backfill job {backfill_job_id}"))?;

    let job = set_backfill_job_completed(&mut transaction, backfill_job_id).await?;

    transaction
        .commit()
        .await
        .context("failed to commit backfill job completion")?;

    Ok(job)
}

async fn maybe_complete_backfill_job(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    backfill_job_id: i64,
) -> Result<()> {
    let incomplete_count = incomplete_range_count(&mut **executor, backfill_job_id).await?;
    if incomplete_count != 0 {
        return Ok(());
    }

    set_backfill_job_completed(executor, backfill_job_id).await?;
    Ok(())
}

pub(super) async fn set_backfill_job_completed(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    backfill_job_id: i64,
) -> Result<BackfillJob> {
    let complete_sql = backfill_job_returning_sql(
        r#"
        UPDATE backfill_jobs
        SET
            status = 'completed'::backfill_lifecycle_status,
            failure_reason = NULL,
            failure_metadata = '{}'::jsonb,
            completed_at = COALESCE(completed_at, now()),
            updated_at = now()
        WHERE backfill_job_id = $1
        "#,
    );
    let row = sqlx::query(&complete_sql)
        .bind(backfill_job_id)
        .fetch_one(&mut **executor)
        .await
        .with_context(|| format!("failed to complete backfill job {backfill_job_id}"))?;

    decode_backfill_job(row)
}
