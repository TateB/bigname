use anyhow::{Context, Result};

use crate::{
    backfill::{BackfillBlockRange, backfill_job_source_identity_payload},
    reconciliation::RawFactNormalizedEventReplaySourceScope,
    source_scope::SourceScope,
};

pub(super) fn replay_source_scope_from_source_plan(
    source_plan: &bigname_manifests::WatchedSourceSelectorPlan,
    from_block: i64,
    to_block: i64,
) -> Vec<RawFactNormalizedEventReplaySourceScope> {
    SourceScope::from_watched_source_plan(source_plan, from_block, to_block).into_targets()
}

pub(super) fn source_identity_hash_for_backfill(
    source_plan: &bigname_manifests::WatchedSourceSelectorPlan,
) -> Result<String> {
    let payload = backfill_job_source_identity_payload(source_plan)?;
    payload
        .get("source_identity_hash")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .context("backfill source identity payload is missing source_identity_hash")
}

pub(crate) fn bootstrap_backfill_idempotency_key(
    deployment_profile: &str,
    chain: &str,
    source_identity_hash: &str,
    range: BackfillBlockRange,
) -> String {
    format!(
        "indexer-bootstrap-backfill:v3:deployment_profile={deployment_profile}:chain={chain}:source_identity_hash={source_identity_hash}:from={}:to={}",
        range.from_block, range.to_block
    )
}

pub(super) fn partitioned_bootstrap_backfill_idempotency_key(
    deployment_profile: &str,
    chain: &str,
    source_identity_hash: &str,
    range: BackfillBlockRange,
    range_blocks: i64,
) -> String {
    format!(
        "indexer-bootstrap-backfill:v3:deployment_profile={deployment_profile}:chain={chain}:source_identity_hash={source_identity_hash}:from={}:to={}:range_blocks={range_blocks}",
        range.from_block, range.to_block
    )
}
