use std::{collections::BTreeSet, path::Path};

use anyhow::{Context, Result};
use bigname_manifests::ManifestBootstrapTarget;
use serde_json::Value;
use sqlx::Row;

use crate::backfill::BackfillBlockRange;

pub(super) async fn load_bootstrap_segment_checkpoint(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    manifests_root: &Path,
    chain: &str,
    range: BackfillBlockRange,
    target_ids: &BTreeSet<String>,
) -> Result<Option<i64>> {
    let idempotency_key_pattern = format!(
        "indexer-bootstrap-backfill:%:deployment_profile={deployment_profile}:manifest_root={}:chain={chain}:source_identity_hash=%",
        manifests_root.display()
    );
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
          AND bj.idempotency_key LIKE $3
          AND br.range_start_block_number <= $5
          AND br.range_end_block_number >= $4
          AND bj.range_end_block_number >= $4
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(idempotency_key_pattern)
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

    contiguous_bootstrap_segment_checkpoint(checkpoint_rows, range, target_ids)
}

pub(super) async fn load_bootstrap_target_checkpoint(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    manifests_root: &Path,
    chain: &str,
    range: BackfillBlockRange,
    target_id: &str,
) -> Result<Option<i64>> {
    let idempotency_key_pattern = format!(
        "indexer-bootstrap-backfill:%:deployment_profile={deployment_profile}:manifest_root={}:chain={chain}:source_identity_hash=%",
        manifests_root.display()
    );
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
          AND bj.idempotency_key LIKE $3
          AND br.range_start_block_number <= $5
          AND br.range_end_block_number >= $4
          AND bj.range_end_block_number >= $4
        ORDER BY br.range_start_block_number ASC, br.checkpoint_block_number ASC
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(idempotency_key_pattern)
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

    contiguous_bootstrap_target_checkpoint(checkpoint_rows, range, target_id)
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

#[derive(Clone, Debug)]
struct BootstrapTargetCheckpointRow {
    range_start_block_number: i64,
    checkpoint_block_number: i64,
    source_identity: Value,
}

fn contiguous_bootstrap_target_checkpoint(
    rows: Vec<BootstrapTargetCheckpointRow>,
    range: BackfillBlockRange,
    target_id: &str,
) -> Result<Option<i64>> {
    contiguous_bootstrap_checkpoint(rows, range, |source_identity| {
        source_identity_requested_target_ids(source_identity)
            .is_some_and(|target_ids| target_ids.contains(target_id))
    })
}

fn contiguous_bootstrap_segment_checkpoint(
    rows: Vec<BootstrapTargetCheckpointRow>,
    range: BackfillBlockRange,
    target_ids: &BTreeSet<String>,
) -> Result<Option<i64>> {
    contiguous_bootstrap_checkpoint(rows, range, |source_identity| {
        source_identity_requested_target_ids(source_identity).as_ref() == Some(target_ids)
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
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn target_checkpoint_walks_contiguous_jobs_for_same_target() -> Result<()> {
        let target_id = "00000000-0000-0000-0000-000000000001";
        let other_target_id = "00000000-0000-0000-0000-000000000002";
        let rows = vec![
            checkpoint_row(1, 10, &[target_id]),
            checkpoint_row(11, 20, &[target_id, other_target_id]),
        ];

        assert_eq!(
            contiguous_bootstrap_target_checkpoint(
                rows,
                BackfillBlockRange::new(1, 30)?,
                target_id,
            )?,
            Some(20)
        );
        Ok(())
    }

    #[test]
    fn target_checkpoint_stops_at_coverage_gap() -> Result<()> {
        let target_id = "00000000-0000-0000-0000-000000000001";
        let rows = vec![
            checkpoint_row(1, 10, &[target_id]),
            checkpoint_row(12, 20, &[target_id]),
        ];

        assert_eq!(
            contiguous_bootstrap_target_checkpoint(
                rows,
                BackfillBlockRange::new(1, 30)?,
                target_id,
            )?,
            Some(10)
        );
        Ok(())
    }

    #[test]
    fn segment_checkpoint_ignores_non_contiguous_parallel_range_progress() -> Result<()> {
        let target_id = "00000000-0000-0000-0000-000000000001";
        let target_ids = BTreeSet::from([target_id.to_owned()]);
        let rows = vec![
            checkpoint_row(1, 10, &[target_id]),
            checkpoint_row(21, 30, &[target_id]),
        ];

        assert_eq!(
            contiguous_bootstrap_segment_checkpoint(
                rows,
                BackfillBlockRange::new(1, 40)?,
                &target_ids,
            )?,
            Some(10)
        );
        Ok(())
    }

    fn checkpoint_row(
        range_start_block_number: i64,
        checkpoint_block_number: i64,
        target_ids: &[&str],
    ) -> BootstrapTargetCheckpointRow {
        BootstrapTargetCheckpointRow {
            range_start_block_number,
            checkpoint_block_number,
            source_identity: json!({
                "requested_watched_targets": target_ids
                    .iter()
                    .map(|target_id| json!({ "contract_instance_id": target_id }))
                    .collect::<Vec<_>>()
            }),
        }
    }
}
