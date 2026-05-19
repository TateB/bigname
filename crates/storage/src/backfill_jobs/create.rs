use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres};

use super::{
    decode::{decode_backfill_job, decode_backfill_range},
    read::{load_backfill_job_by_idempotency_key_internal, load_backfill_ranges_internal},
    sql::backfill_range_returning_sql,
    types::{BackfillJobCreate, BackfillJobRecord, BackfillRange, BackfillRangeSpec},
    validate::{
        ensure_existing_job_matches_request, ensure_existing_ranges_match_specs,
        validate_backfill_job_create,
    },
};

/// Insert a bounded backfill job and its child ranges, or return the existing
/// matching job for the same idempotency key.
pub async fn create_backfill_job(
    pool: &PgPool,
    request: &BackfillJobCreate,
) -> Result<BackfillJobRecord> {
    let range_specs = validate_backfill_job_create(request)?;
    let source_identity = serde_json::to_string(&request.source_identity)
        .context("failed to serialize source_identity")?;

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for backfill job creation")?;

    let inserted = sqlx::query(
        r#"
        INSERT INTO backfill_jobs (
            deployment_profile,
            chain_id,
            source_identity,
            scan_mode,
            range_start_block_number,
            range_end_block_number,
            idempotency_key
        )
        VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7)
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING
            backfill_job_id,
            deployment_profile,
            chain_id,
            source_identity,
            scan_mode,
            range_start_block_number,
            range_end_block_number,
            idempotency_key,
            status::TEXT AS status,
            failure_reason,
            failure_metadata,
            created_at,
            updated_at,
            completed_at
        "#,
    )
    .bind(&request.deployment_profile)
    .bind(&request.chain_id)
    .bind(source_identity)
    .bind(&request.scan_mode)
    .bind(request.range_start_block_number)
    .bind(request.range_end_block_number)
    .bind(&request.idempotency_key)
    .fetch_optional(&mut *transaction)
    .await
    .with_context(|| {
        format!(
            "failed to insert backfill job for idempotency key {}",
            request.idempotency_key
        )
    })?;

    let job = match inserted {
        Some(row) => {
            let job = decode_backfill_job(row)?;
            for spec in &range_specs {
                insert_backfill_range(&mut transaction, job.backfill_job_id, spec).await?;
            }
            job
        }
        None => {
            let job = load_backfill_job_by_idempotency_key_internal(
                &mut *transaction,
                &request.idempotency_key,
            )
            .await?
            .with_context(|| {
                format!(
                    "backfill job idempotency key {} conflicted but no row was found",
                    request.idempotency_key
                )
            })?;
            ensure_existing_job_matches_request(&job, request)?;
            job
        }
    };

    let ranges = load_backfill_ranges_internal(&mut *transaction, job.backfill_job_id).await?;
    ensure_existing_ranges_match_specs(job.backfill_job_id, &ranges, &range_specs)?;

    transaction
        .commit()
        .await
        .context("failed to commit backfill job creation")?;

    Ok(BackfillJobRecord { job, ranges })
}

async fn insert_backfill_range(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    backfill_job_id: i64,
    spec: &BackfillRangeSpec,
) -> Result<BackfillRange> {
    let insert_sql = backfill_range_returning_sql(
        r#"
        INSERT INTO backfill_ranges (
            backfill_job_id,
            range_start_block_number,
            range_end_block_number,
            checkpoint_block_number
        )
        VALUES ($1, $2, $3, $2 - 1)
        "#,
    );
    let row = sqlx::query(&insert_sql)
        .bind(backfill_job_id)
        .bind(spec.range_start_block_number)
        .bind(spec.range_end_block_number)
        .fetch_one(&mut **executor)
        .await
        .with_context(|| {
            format!(
                "failed to insert backfill range {}..={} for job {backfill_job_id}",
                spec.range_start_block_number, spec.range_end_block_number
            )
        })?;

    decode_backfill_range(row)
}
