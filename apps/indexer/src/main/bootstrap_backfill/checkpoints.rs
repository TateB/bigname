use std::collections::BTreeSet;

use anyhow::{Context, Result};
use bigname_manifests::ManifestBootstrapTarget;
use serde_json::Value;
use sqlx::Row;

use crate::backfill::BackfillBlockRange;

pub(super) async fn load_bootstrap_segment_checkpoint(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    expected_source_identity: &Value,
    range: BackfillBlockRange,
    target_ids: &BTreeSet<String>,
) -> Result<Option<i64>> {
    let rows = sqlx::query(
        r#"
        SELECT
            bj.source_identity,
            br.range_start_block_number AS range_start_block_number,
            br.checkpoint_block_number AS checkpoint_block_number
        FROM backfill_jobs bj
        JOIN backfill_ranges br ON br.backfill_job_id = bj.backfill_job_id
        WHERE bj.deployment_profile = $1
          AND bj.chain_id = $2
          AND bj.scan_mode = 'hash_pinned_block'
          AND bj.status <> 'pending'::backfill_lifecycle_status
          AND br.status <> 'pending'::backfill_lifecycle_status
          AND (
                br.status = 'completed'::backfill_lifecycle_status
              OR br.lease_expires_at IS NULL
              OR br.lease_expires_at < now()
          )
          AND bj.idempotency_key LIKE 'indexer-bootstrap-backfill:%'
          AND br.range_start_block_number <= $4
          AND br.range_end_block_number >= $3
          AND bj.range_end_block_number >= $3
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(range.from_block)
    .bind(range.to_block)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load stored bootstrap backfill checkpoints for chain {chain} range {}..={}",
            range.from_block, range.to_block
        )
    })?;

    let mut checkpoint_rows = Vec::new();
    for row in rows {
        let source_identity = row
            .try_get::<Value, _>("source_identity")
            .context("failed to read bootstrap source_identity")?;
        checkpoint_rows.push(BootstrapTargetCheckpointRow {
            range_start_block_number: row
                .try_get("range_start_block_number")
                .context("failed to read bootstrap range_start_block_number")?,
            checkpoint_block_number: row
                .try_get("checkpoint_block_number")
                .context("failed to read bootstrap checkpoint_block_number")?,
            source_identity,
        });
    }

    contiguous_bootstrap_segment_checkpoint(
        checkpoint_rows,
        range,
        expected_source_identity,
        target_ids,
    )
}

pub(super) async fn load_bootstrap_target_checkpoint(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    expected_source_identity: &Value,
    range: BackfillBlockRange,
    target_id: &str,
) -> Result<Option<i64>> {
    let rows = sqlx::query(
        r#"
        SELECT
            bj.source_identity,
            br.range_start_block_number AS range_start_block_number,
            br.checkpoint_block_number AS checkpoint_block_number
        FROM backfill_jobs bj
        JOIN backfill_ranges br ON br.backfill_job_id = bj.backfill_job_id
        WHERE bj.deployment_profile = $1
          AND bj.chain_id = $2
          AND bj.scan_mode = 'hash_pinned_block'
          AND bj.status <> 'pending'::backfill_lifecycle_status
          AND br.status <> 'pending'::backfill_lifecycle_status
          AND (
                br.status = 'completed'::backfill_lifecycle_status
              OR br.lease_expires_at IS NULL
              OR br.lease_expires_at < now()
          )
          AND bj.idempotency_key LIKE 'indexer-bootstrap-backfill:%'
          AND br.range_start_block_number <= $4
          AND br.range_end_block_number >= $3
          AND bj.range_end_block_number >= $3
        ORDER BY br.range_start_block_number ASC, br.checkpoint_block_number ASC
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(range.from_block)
    .bind(range.to_block)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load stored bootstrap target checkpoints for chain {chain} target {target_id} range {}..={}",
            range.from_block, range.to_block
        )
    })?;

    let mut checkpoint_rows = Vec::new();
    for row in rows {
        let source_identity = row
            .try_get::<Value, _>("source_identity")
            .context("failed to read bootstrap target source_identity")?;
        checkpoint_rows.push(BootstrapTargetCheckpointRow {
            range_start_block_number: row
                .try_get("range_start_block_number")
                .context("failed to read bootstrap target range_start_block_number")?,
            checkpoint_block_number: row
                .try_get("checkpoint_block_number")
                .context("failed to read bootstrap target checkpoint_block_number")?,
            source_identity,
        });
    }

    contiguous_bootstrap_target_checkpoint(
        checkpoint_rows,
        range,
        expected_source_identity,
        target_id,
    )
}

pub(super) fn bootstrap_segment_target_ids(
    targets: &[ManifestBootstrapTarget],
) -> BTreeSet<String> {
    targets
        .iter()
        .map(|target| target.contract_instance_id.to_string())
        .collect()
}

fn source_identity_requested_target_ids(source_identity: &Value) -> Option<BTreeSet<String>> {
    let requested_targets = source_identity
        .get("requested_watched_targets")
        .and_then(Value::as_array)?;
    requested_targets
        .iter()
        .map(|target| {
            target
                .get("contract_instance_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .collect()
}

fn source_identity_hash_field(source_identity: &Value) -> Option<&str> {
    source_identity
        .get("source_identity_hash")
        .and_then(Value::as_str)
}

fn source_identity_selected_target_ids(source_identity: &Value) -> Option<BTreeSet<String>> {
    let selected_targets = source_identity
        .get("selected_targets")
        .and_then(Value::as_array)?;
    selected_targets
        .iter()
        .map(|target| {
            target
                .get("contract_instance_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .collect()
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SourceIdentitySelectedTarget {
    source_family: String,
    contract_instance_id: String,
    address: String,
    effective_from_block: i64,
    effective_to_block: i64,
}

fn source_identity_selected_target(
    source_identity: &Value,
    target_id: &str,
) -> Option<SourceIdentitySelectedTarget> {
    let selected_targets = source_identity
        .get("selected_targets")
        .and_then(Value::as_array)?;
    selected_targets.iter().find_map(|target| {
        if target.get("contract_instance_id").and_then(Value::as_str)? != target_id {
            return None;
        }
        Some(SourceIdentitySelectedTarget {
            source_family: target.get("source_family")?.as_str()?.to_owned(),
            contract_instance_id: target.get("contract_instance_id")?.as_str()?.to_owned(),
            address: target.get("address")?.as_str()?.to_ascii_lowercase(),
            effective_from_block: target.get("effective_from_block")?.as_i64()?,
            effective_to_block: target.get("effective_to_block")?.as_i64()?,
        })
    })
}

fn source_identity_generic_topic_scans(source_identity: &Value) -> Option<&Value> {
    source_identity.get("generic_topic_scans")
}

fn source_identity_selected_target_matches(
    source_target: &SourceIdentitySelectedTarget,
    expected_target: &SourceIdentitySelectedTarget,
) -> bool {
    source_target.source_family == expected_target.source_family
        && source_target.contract_instance_id == expected_target.contract_instance_id
        && source_target.address == expected_target.address
        && source_target.effective_from_block >= expected_target.effective_from_block
        && source_target.effective_to_block <= expected_target.effective_to_block
        && source_target.effective_from_block <= source_target.effective_to_block
}

fn source_identity_generic_topic_scan_matches_target(
    source_identity: &Value,
    expected_source_identity: &Value,
    target_id: &str,
) -> bool {
    source_identity_generic_topic_scans(source_identity).is_some()
        && source_identity_generic_topic_scans(source_identity)
            == source_identity_generic_topic_scans(expected_source_identity)
        && source_identity_requested_target_ids(source_identity)
            .is_some_and(|target_ids| target_ids.contains(target_id))
        && source_identity_requested_target_ids(expected_source_identity)
            .is_some_and(|target_ids| target_ids.contains(target_id))
}

fn source_identity_matches_expected_targets(
    source_identity: &Value,
    expected_source_identity: &Value,
    target_ids: &BTreeSet<String>,
    require_exact_target_set: bool,
) -> bool {
    if source_identity_hash_field(source_identity).is_some()
        && source_identity_hash_field(source_identity)
            == source_identity_hash_field(expected_source_identity)
    {
        return true;
    }

    if source_identity.get("selector_kind") != expected_source_identity.get("selector_kind")
        || source_identity.get("source_family") != expected_source_identity.get("source_family")
    {
        return false;
    }
    if require_exact_target_set {
        if source_identity_requested_target_ids(source_identity).as_ref() != Some(target_ids)
            || source_identity_requested_target_ids(expected_source_identity).as_ref()
                != Some(target_ids)
        {
            return false;
        }
        let Some(source_selected_target_ids) = source_identity_selected_target_ids(source_identity)
        else {
            return false;
        };
        let Some(expected_selected_target_ids) =
            source_identity_selected_target_ids(expected_source_identity)
        else {
            return false;
        };
        if source_selected_target_ids != expected_selected_target_ids
            || !source_selected_target_ids.is_subset(target_ids)
        {
            return false;
        }
    }

    target_ids.iter().all(|target_id| {
        match (
            source_identity_selected_target(source_identity, target_id),
            source_identity_selected_target(expected_source_identity, target_id),
        ) {
            (Some(source_target), Some(expected_target)) => {
                source_identity_selected_target_matches(&source_target, &expected_target)
            }
            (None, None) => source_identity_generic_topic_scan_matches_target(
                source_identity,
                expected_source_identity,
                target_id,
            ),
            _ => false,
        }
    })
}

#[derive(Clone, Debug)]
struct BootstrapTargetCheckpointRow {
    range_start_block_number: i64,
    checkpoint_block_number: i64,
    source_identity: Value,
}

fn contiguous_bootstrap_target_checkpoint(
    rows: Vec<BootstrapTargetCheckpointRow>,
    range: BackfillBlockRange,
    expected_source_identity: &Value,
    target_id: &str,
) -> Result<Option<i64>> {
    let target_ids = BTreeSet::from([target_id.to_owned()]);
    contiguous_bootstrap_checkpoint(rows, range, |source_identity| {
        source_identity_requested_target_ids(source_identity)
            .is_some_and(|target_ids| target_ids.contains(target_id))
            && source_identity_matches_expected_targets(
                source_identity,
                expected_source_identity,
                &target_ids,
                false,
            )
    })
}

fn contiguous_bootstrap_segment_checkpoint(
    rows: Vec<BootstrapTargetCheckpointRow>,
    range: BackfillBlockRange,
    expected_source_identity: &Value,
    target_ids: &BTreeSet<String>,
) -> Result<Option<i64>> {
    contiguous_bootstrap_checkpoint(rows, range, |source_identity| {
        source_identity_requested_target_ids(source_identity).as_ref() == Some(target_ids)
            && source_identity_matches_expected_targets(
                source_identity,
                expected_source_identity,
                target_ids,
                true,
            )
    })
}

fn contiguous_bootstrap_checkpoint(
    mut rows: Vec<BootstrapTargetCheckpointRow>,
    range: BackfillBlockRange,
    mut accepts_source_identity: impl FnMut(&Value) -> bool,
) -> Result<Option<i64>> {
    rows.sort_by_key(|row| (row.range_start_block_number, row.checkpoint_block_number));

    let mut next_required_block = range.from_block;
    let mut checkpoint = None;
    for row in rows {
        if !accepts_source_identity(&row.source_identity) {
            continue;
        }
        if row.range_start_block_number > next_required_block {
            break;
        }
        if row.checkpoint_block_number < next_required_block {
            continue;
        }

        let row_checkpoint = row.checkpoint_block_number.min(range.to_block);
        checkpoint = Some(row_checkpoint);
        if row_checkpoint >= range.to_block {
            break;
        }
        next_required_block = row_checkpoint.checked_add(1).with_context(|| {
            format!(
                "bootstrap checkpoint {row_checkpoint} overflowed while walking contiguous coverage"
            )
        })?;
    }

    Ok(checkpoint)
}

#[cfg(test)]
#[path = "checkpoints/tests.rs"]
mod tests;
