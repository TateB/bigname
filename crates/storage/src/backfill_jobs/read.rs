use anyhow::{Context, Result};
use sqlx::{Executor, PgPool, Postgres, Row};

use super::{
    decode::{decode_backfill_job, decode_backfill_range},
    sql::{backfill_job_select_sql, backfill_range_select_sql},
    types::{BackfillJob, BackfillRange},
};

/// Load one backfill job by stable row identity.
pub async fn load_backfill_job(pool: &PgPool, backfill_job_id: i64) -> Result<Option<BackfillJob>> {
    load_backfill_job_internal(pool, backfill_job_id).await
}

/// Load child ranges for one backfill job in declared range order.
pub async fn load_backfill_ranges(
    pool: &PgPool,
    backfill_job_id: i64,
) -> Result<Vec<BackfillRange>> {
    load_backfill_ranges_internal(pool, backfill_job_id).await
}

pub(super) async fn incomplete_range_count<'e, E>(executor: E, backfill_job_id: i64) -> Result<i64>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query(
        r#"
        SELECT COUNT(*)::BIGINT AS incomplete_count
        FROM backfill_ranges
        WHERE backfill_job_id = $1
          AND (
            status <> 'completed'::backfill_lifecycle_status
            OR checkpoint_block_number <> range_end_block_number
          )
        "#,
    )
    .bind(backfill_job_id)
    .fetch_one(executor)
    .await
    .with_context(|| {
        format!("failed to count incomplete ranges for backfill job {backfill_job_id}")
    })?
    .try_get::<i64, _>("incomplete_count")
    .context("missing incomplete_count")
}

pub(super) async fn load_backfill_job_internal<'e, E>(
    executor: E,
    backfill_job_id: i64,
) -> Result<Option<BackfillJob>>
where
    E: Executor<'e, Database = Postgres>,
{
    let select_sql = backfill_job_select_sql("WHERE backfill_job_id = $1", "");
    let row = sqlx::query(&select_sql)
        .bind(backfill_job_id)
        .fetch_optional(executor)
        .await
        .with_context(|| format!("failed to load backfill job {backfill_job_id}"))?;

    row.map(decode_backfill_job).transpose()
}

pub(super) async fn load_backfill_job_by_idempotency_key_internal<'e, E>(
    executor: E,
    idempotency_key: &str,
) -> Result<Option<BackfillJob>>
where
    E: Executor<'e, Database = Postgres>,
{
    let select_sql = backfill_job_select_sql("WHERE idempotency_key = $1", "");
    let row = sqlx::query(&select_sql)
        .bind(idempotency_key)
        .fetch_optional(executor)
        .await
        .with_context(|| {
            format!("failed to load backfill job for idempotency key {idempotency_key}")
        })?;

    row.map(decode_backfill_job).transpose()
}

pub(super) async fn load_backfill_job_for_update<'e, E>(
    executor: E,
    backfill_job_id: i64,
) -> Result<Option<BackfillJob>>
where
    E: Executor<'e, Database = Postgres>,
{
    let select_sql = backfill_job_select_sql("WHERE backfill_job_id = $1", "FOR UPDATE");
    let row = sqlx::query(&select_sql)
        .bind(backfill_job_id)
        .fetch_optional(executor)
        .await
        .with_context(|| format!("failed to lock backfill job {backfill_job_id}"))?;

    row.map(decode_backfill_job).transpose()
}

pub(super) async fn load_backfill_ranges_internal<'e, E>(
    executor: E,
    backfill_job_id: i64,
) -> Result<Vec<BackfillRange>>
where
    E: Executor<'e, Database = Postgres>,
{
    let select_sql = backfill_range_select_sql(
        "WHERE backfill_job_id = $1",
        "ORDER BY range_start_block_number, range_end_block_number",
    );
    let rows = sqlx::query(&select_sql)
        .bind(backfill_job_id)
        .fetch_all(executor)
        .await
        .with_context(|| format!("failed to load ranges for backfill job {backfill_job_id}"))?;

    rows.into_iter().map(decode_backfill_range).collect()
}

pub(super) async fn load_backfill_ranges_for_update<'e, E>(
    executor: E,
    backfill_job_id: i64,
) -> Result<Vec<BackfillRange>>
where
    E: Executor<'e, Database = Postgres>,
{
    let select_sql = backfill_range_select_sql(
        "WHERE backfill_job_id = $1",
        "ORDER BY range_start_block_number, range_end_block_number FOR UPDATE",
    );
    let rows = sqlx::query(&select_sql)
        .bind(backfill_job_id)
        .fetch_all(executor)
        .await
        .with_context(|| format!("failed to lock ranges for backfill job {backfill_job_id}"))?;

    rows.into_iter().map(decode_backfill_range).collect()
}

pub(super) async fn load_backfill_range_for_update<'e, E>(
    executor: E,
    backfill_range_id: i64,
) -> Result<Option<BackfillRange>>
where
    E: Executor<'e, Database = Postgres>,
{
    let select_sql = backfill_range_select_sql("WHERE backfill_range_id = $1", "FOR UPDATE");
    let row = sqlx::query(&select_sql)
        .bind(backfill_range_id)
        .fetch_optional(executor)
        .await
        .with_context(|| format!("failed to lock backfill range {backfill_range_id}"))?;

    row.map(decode_backfill_range).transpose()
}

pub(super) async fn load_active_backfill_range_by_lease<'e, E>(
    executor: E,
    backfill_job_id: i64,
    lease_owner: &str,
    lease_token: &str,
) -> Result<Option<BackfillRange>>
where
    E: Executor<'e, Database = Postgres>,
{
    let select_sql = backfill_range_select_sql(
        r#"
        WHERE backfill_job_id = $1
          AND lease_owner = $2
          AND lease_token = $3
          AND status IN ('reserved'::backfill_lifecycle_status, 'running'::backfill_lifecycle_status)
          AND lease_expires_at > now()
        "#,
        "FOR UPDATE",
    );
    let row = sqlx::query(&select_sql)
        .bind(backfill_job_id)
        .bind(lease_owner)
        .bind(lease_token)
        .fetch_optional(executor)
        .await
        .with_context(|| {
            format!("failed to load active lease {lease_token} for backfill job {backfill_job_id}")
        })?;

    row.map(decode_backfill_range).transpose()
}
