use anyhow::Result;
use sqlx::postgres::PgRow;

use super::types::{BackfillJob, BackfillLifecycleStatus, BackfillRange};

pub(super) fn decode_backfill_job(row: PgRow) -> Result<BackfillJob> {
    Ok(BackfillJob {
        backfill_job_id: crate::sql_row::get(&row, "backfill_job_id")?,
        deployment_profile: crate::sql_row::get(&row, "deployment_profile")?,
        chain_id: crate::sql_row::get(&row, "chain_id")?,
        source_identity: crate::sql_row::get(&row, "source_identity")?,
        scan_mode: crate::sql_row::get(&row, "scan_mode")?,
        range_start_block_number: crate::sql_row::get(&row, "range_start_block_number")?,
        range_end_block_number: crate::sql_row::get(&row, "range_end_block_number")?,
        idempotency_key: crate::sql_row::get(&row, "idempotency_key")?,
        status: BackfillLifecycleStatus::parse(&crate::sql_row::get::<String>(&row, "status")?)?,
        failure_reason: crate::sql_row::get(&row, "failure_reason")?,
        failure_metadata: crate::sql_row::get(&row, "failure_metadata")?,
        created_at: crate::sql_row::get(&row, "created_at")?,
        updated_at: crate::sql_row::get(&row, "updated_at")?,
        completed_at: crate::sql_row::get(&row, "completed_at")?,
    })
}

pub(super) fn decode_backfill_range(row: PgRow) -> Result<BackfillRange> {
    Ok(BackfillRange {
        backfill_range_id: crate::sql_row::get(&row, "backfill_range_id")?,
        backfill_job_id: crate::sql_row::get(&row, "backfill_job_id")?,
        range_start_block_number: crate::sql_row::get(&row, "range_start_block_number")?,
        range_end_block_number: crate::sql_row::get(&row, "range_end_block_number")?,
        checkpoint_block_number: crate::sql_row::get(&row, "checkpoint_block_number")?,
        status: BackfillLifecycleStatus::parse(&crate::sql_row::get::<String>(&row, "status")?)?,
        lease_token: crate::sql_row::get(&row, "lease_token")?,
        lease_owner: crate::sql_row::get(&row, "lease_owner")?,
        lease_expires_at: crate::sql_row::get(&row, "lease_expires_at")?,
        attempt_count: crate::sql_row::get(&row, "attempt_count")?,
        failure_reason: crate::sql_row::get(&row, "failure_reason")?,
        failure_metadata: crate::sql_row::get(&row, "failure_metadata")?,
        created_at: crate::sql_row::get(&row, "created_at")?,
        updated_at: crate::sql_row::get(&row, "updated_at")?,
        completed_at: crate::sql_row::get(&row, "completed_at")?,
    })
}
