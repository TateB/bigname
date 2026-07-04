use alloy_primitives::keccak256;
use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::{Value, json};
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
        || !source_identity_matches_request(&existing.source_identity, &request.source_identity)
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

fn source_identity_matches_request(existing: &Value, requested: &Value) -> bool {
    existing == requested
        || source_identity_compact_full_equivalent(existing, requested)
        || source_identity_compact_full_equivalent(requested, existing)
}

fn source_identity_compact_full_equivalent(compact: &Value, full: &Value) -> bool {
    if compact
        .get("source_identity_payload_format")
        .and_then(Value::as_str)
        != Some("selected_targets_digest_v1")
    {
        return false;
    }
    if compact.get("selected_targets").is_some() {
        return false;
    }
    if source_identity_hash_field(compact).is_none() || source_identity_hash_field(full).is_none() {
        return false;
    }
    if compact.get("selector_kind") != full.get("selector_kind")
        || compact.get("source_family") != full.get("source_family")
        || compact.get("requested_watched_targets") != full.get("requested_watched_targets")
    {
        return false;
    }
    if !source_identity_common_fields_match(compact, full) {
        return false;
    }

    let Some(selected_targets) = full.get("selected_targets").and_then(Value::as_array) else {
        return false;
    };
    if compact.get("selected_target_count").and_then(Value::as_u64)
        != Some(selected_targets.len() as u64)
    {
        return false;
    }
    if compact
        .get("selected_targets_digest_algorithm")
        .and_then(Value::as_str)
        != Some("keccak256")
    {
        return false;
    }
    let Some(actual_digest) = compact
        .get("selected_targets_digest")
        .and_then(Value::as_str)
    else {
        return false;
    };
    if !selected_targets_digest_matches(actual_digest, selected_targets) {
        return false;
    }
    let Some(sample) = compact.get("selected_targets_sample") else {
        return false;
    };
    let expected = json!({
        "first": selected_targets.first(),
        "last": selected_targets.last(),
    });
    if sample != &expected {
        return false;
    }

    true
}

fn source_identity_common_fields_match(compact: &Value, full: &Value) -> bool {
    const COMPACT_ONLY_FIELDS: &[&str] = &[
        "source_identity_hash",
        "source_identity_payload_format",
        "selected_target_count",
        "selected_targets_digest_algorithm",
        "selected_targets_digest",
        "selected_targets_sample",
    ];
    const FULL_ONLY_FIELDS: &[&str] = &["source_identity_hash", "selected_targets"];

    source_identity_without_fields(compact, COMPACT_ONLY_FIELDS)
        == source_identity_without_fields(full, FULL_ONLY_FIELDS)
}

fn source_identity_without_fields(
    source_identity: &Value,
    fields_to_remove: &[&str],
) -> Option<Value> {
    let mut fields = source_identity.as_object()?.clone();
    for field in fields_to_remove {
        fields.remove(*field);
    }
    Some(Value::Object(fields))
}

fn source_identity_hash_field(source_identity: &Value) -> Option<&str> {
    source_identity
        .get("source_identity_hash")
        .and_then(Value::as_str)
}

pub(super) fn selected_targets_digest(selected_targets: &[Value]) -> String {
    let payload = serde_json::to_vec(&canonical_json_value(Value::Array(
        selected_targets.to_vec(),
    )))
    .expect("selected target identity must serialize");
    format!("keccak256:{}", keccak256(payload))
}

fn selected_targets_digest_matches(actual_digest: &str, selected_targets: &[Value]) -> bool {
    actual_digest == selected_targets_digest(selected_targets)
        || selected_targets_producer_order_digest(selected_targets).as_deref()
            == Some(actual_digest)
}

fn selected_targets_producer_order_digest(selected_targets: &[Value]) -> Option<String> {
    #[derive(Serialize)]
    struct WatchedBackfillTargetDigestInput<'a> {
        source_family: &'a Value,
        contract_instance_id: &'a Value,
        address: &'a Value,
        effective_from_block: &'a Value,
        effective_to_block: &'a Value,
    }

    let ordered_targets = selected_targets
        .iter()
        .map(|target| {
            let fields = target.as_object()?;
            Some(WatchedBackfillTargetDigestInput {
                source_family: fields.get("source_family")?,
                contract_instance_id: fields.get("contract_instance_id")?,
                address: fields.get("address")?,
                effective_from_block: fields.get("effective_from_block")?,
                effective_to_block: fields.get("effective_to_block")?,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    let payload =
        serde_json::to_vec(&ordered_targets).expect("selected target identity must serialize");
    Some(format!("keccak256:{}", keccak256(payload)))
}

fn canonical_json_value(value: Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.into_iter().map(canonical_json_value).collect()),
        Value::Object(fields) => {
            let mut fields = fields
                .into_iter()
                .map(|(key, value)| (key, canonical_json_value(value)))
                .collect::<Vec<_>>();
            fields.sort_by(|left, right| left.0.cmp(&right.0));

            let mut sorted = serde_json::Map::new();
            for (key, value) in fields {
                sorted.insert(key, value);
            }
            Value::Object(sorted)
        }
        value => value,
    }
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
