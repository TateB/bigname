use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;

use super::types::{
    BackfillJob, BackfillJobCreate, BackfillLifecycleStatus, BackfillRange, BackfillRangeSpec,
};

pub(super) fn validate_backfill_job_create(
    request: &BackfillJobCreate,
) -> Result<Vec<BackfillRangeSpec>> {
    validate_non_empty("deployment_profile", &request.deployment_profile)?;
    validate_non_empty("chain_id", &request.chain_id)?;
    validate_non_empty("scan_mode", &request.scan_mode)?;
    validate_non_empty("idempotency_key", &request.idempotency_key)?;
    validate_range_bounds(
        request.range_start_block_number,
        request.range_end_block_number,
        "backfill job",
    )?;
    match &request.source_identity {
        Value::Object(_) | Value::Array(_) => {}
        _ => bail!("backfill job source_identity must be a JSON object or array"),
    }

    let mut specs = if request.ranges.is_empty() {
        vec![BackfillRangeSpec {
            range_start_block_number: request.range_start_block_number,
            range_end_block_number: request.range_end_block_number,
        }]
    } else {
        request.ranges.clone()
    };
    specs.sort_by_key(|spec| (spec.range_start_block_number, spec.range_end_block_number));

    let mut expected_start = request.range_start_block_number;
    for spec in &specs {
        validate_range_bounds(
            spec.range_start_block_number,
            spec.range_end_block_number,
            "backfill range",
        )?;
        if spec.range_start_block_number != expected_start {
            bail!(
                "backfill ranges must partition the declared job range contiguously; expected range start {expected_start}, got {}",
                spec.range_start_block_number
            );
        }
        if spec.range_end_block_number > request.range_end_block_number {
            bail!(
                "backfill range {}..={} exceeds declared job range end {}",
                spec.range_start_block_number,
                spec.range_end_block_number,
                request.range_end_block_number
            );
        }
        expected_start = spec
            .range_end_block_number
            .checked_add(1)
            .context("backfill range end overflowed while validating contiguous ranges")?;
    }

    if expected_start - 1 != request.range_end_block_number {
        bail!(
            "backfill ranges must cover the declared job range through end {}; covered through {}",
            request.range_end_block_number,
            expected_start - 1
        );
    }

    Ok(specs)
}

fn validate_range_bounds(start: i64, end: i64, label: &str) -> Result<()> {
    if start < 0 {
        bail!("{label} has negative range start {start}");
    }
    if end < start {
        bail!("{label} range end {end} is before range start {start}");
    }
    Ok(())
}

pub(super) fn validate_lease(
    lease_owner: &str,
    lease_token: &str,
    lease_expires_at: OffsetDateTime,
) -> Result<()> {
    validate_non_empty("lease_owner", lease_owner)?;
    validate_non_empty("lease_token", lease_token)?;
    if lease_expires_at <= OffsetDateTime::now_utc() {
        bail!("lease_expires_at must be in the future");
    }
    Ok(())
}

pub(super) fn validate_failure(failure_reason: &str, failure_metadata: &Value) -> Result<()> {
    validate_non_empty("failure_reason", failure_reason)?;
    if !failure_metadata.is_object() {
        bail!("failure_metadata must be a JSON object");
    }
    Ok(())
}

pub(super) fn validate_non_empty(field_name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field_name} must not be empty");
    }
    Ok(())
}

pub(super) fn ensure_existing_job_matches_request(
    existing: &BackfillJob,
    request: &BackfillJobCreate,
) -> Result<()> {
    if existing.deployment_profile != request.deployment_profile
        || existing.chain_id != request.chain_id
        || existing.source_identity != request.source_identity
        || existing.scan_mode != request.scan_mode
        || existing.range_start_block_number != request.range_start_block_number
        || existing.range_end_block_number != request.range_end_block_number
        || existing.idempotency_key != request.idempotency_key
    {
        bail!(
            "existing backfill job for idempotency key {} does not match requested immutable job identity",
            request.idempotency_key
        );
    }

    Ok(())
}

pub(super) fn ensure_existing_ranges_match_specs(
    backfill_job_id: i64,
    ranges: &[BackfillRange],
    specs: &[BackfillRangeSpec],
) -> Result<()> {
    let existing = ranges
        .iter()
        .map(|range| BackfillRangeSpec {
            range_start_block_number: range.range_start_block_number,
            range_end_block_number: range.range_end_block_number,
        })
        .collect::<Vec<_>>();
    if existing != specs {
        bail!("existing ranges for backfill job {backfill_job_id} do not match requested ranges");
    }

    Ok(())
}

pub(super) fn ensure_lease_matches(range: &BackfillRange, lease_token: &str) -> Result<()> {
    if range.lease_token.as_deref() != Some(lease_token) {
        bail!(
            "backfill range {} is not held by lease token {}",
            range.backfill_range_id,
            lease_token
        );
    }

    Ok(())
}

pub(super) fn ensure_lease_is_active(range: &BackfillRange) -> Result<()> {
    let Some(lease_expires_at) = range.lease_expires_at else {
        bail!(
            "backfill range {} has no active lease deadline",
            range.backfill_range_id
        );
    };
    if lease_expires_at <= OffsetDateTime::now_utc() {
        bail!(
            "backfill range {} lease expired at {}",
            range.backfill_range_id,
            lease_expires_at
        );
    }

    Ok(())
}

pub(super) fn ensure_ranges_ready_for_job_completion(
    backfill_job_id: i64,
    ranges: &[BackfillRange],
) -> Result<()> {
    let incomplete_count = ranges
        .iter()
        .filter(|range| range.checkpoint_block_number != range.range_end_block_number)
        .count();
    if incomplete_count != 0 {
        bail!(
            "backfill job {backfill_job_id} has {incomplete_count} range checkpoints that have not reached their declared ends"
        );
    }

    let failed_count = ranges
        .iter()
        .filter(|range| range.status == BackfillLifecycleStatus::Failed)
        .count();
    if failed_count != 0 {
        bail!(
            "backfill job {backfill_job_id} has {failed_count} failed ranges that must be retried before completion"
        );
    }

    Ok(())
}
