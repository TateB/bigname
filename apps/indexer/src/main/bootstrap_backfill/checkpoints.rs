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
        "indexer-bootstrap-backfill:v1:deployment_profile={deployment_profile}:manifest_root={}:chain={chain}:source_identity_hash=%",
        manifests_root.display()
    );
    let rows = sqlx::query(
        r#"
        SELECT bj.source_identity, br.checkpoint_block_number
        FROM backfill_jobs bj
        JOIN backfill_ranges br ON br.backfill_job_id = bj.backfill_job_id
        WHERE bj.deployment_profile = $1
          AND bj.chain_id = $2
          AND bj.scan_mode = 'hash_pinned_block'
          AND bj.status = 'completed'::backfill_lifecycle_status
          AND br.status = 'completed'::backfill_lifecycle_status
          AND bj.idempotency_key LIKE $3
          AND bj.range_start_block_number >= $4
          AND bj.range_start_block_number <= $5
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

    let mut checkpoint = None;
    for row in rows {
        let source_identity = row
            .try_get::<Value, _>("source_identity")
            .context("failed to read bootstrap source_identity")?;
        if source_identity_requested_target_ids(&source_identity).as_ref() != Some(target_ids) {
            continue;
        }
        let row_checkpoint = row
            .try_get::<i64, _>("checkpoint_block_number")
            .context("failed to read bootstrap checkpoint_block_number")?;
        checkpoint =
            Some(checkpoint.map_or(row_checkpoint, |current: i64| current.max(row_checkpoint)));
    }

    Ok(checkpoint)
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
        "indexer-bootstrap-backfill:v1:deployment_profile={deployment_profile}:manifest_root={}:chain={chain}:source_identity_hash=%",
        manifests_root.display()
    );
    let rows = sqlx::query(
        r#"
        SELECT bj.source_identity, br.checkpoint_block_number
        FROM backfill_jobs bj
        JOIN backfill_ranges br ON br.backfill_job_id = bj.backfill_job_id
        WHERE bj.deployment_profile = $1
          AND bj.chain_id = $2
          AND bj.scan_mode = 'hash_pinned_block'
          AND bj.status = 'completed'::backfill_lifecycle_status
          AND br.status = 'completed'::backfill_lifecycle_status
          AND bj.idempotency_key LIKE $3
          AND bj.range_start_block_number >= $4
          AND bj.range_start_block_number <= $5
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
            "failed to load stored bootstrap target checkpoints for chain {chain} target {target_id} range {}..={}",
            range.from_block, range.to_block
        )
    })?;

    let mut checkpoint = None;
    for row in rows {
        let source_identity = row
            .try_get::<Value, _>("source_identity")
            .context("failed to read bootstrap target source_identity")?;
        let Some(target_ids) = source_identity_requested_target_ids(&source_identity) else {
            continue;
        };
        if !target_ids.contains(target_id) {
            continue;
        }
        let row_checkpoint = row
            .try_get::<i64, _>("checkpoint_block_number")
            .context("failed to read bootstrap target checkpoint_block_number")?;
        checkpoint =
            Some(checkpoint.map_or(row_checkpoint, |current: i64| current.max(row_checkpoint)));
    }

    Ok(checkpoint)
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
