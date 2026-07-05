use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    BaseNormalizedRederiveActiveManifestSnapshot, BaseNormalizedRederiveCounts,
    BaseNormalizedRederiveCursorCensus, BaseNormalizedRederiveDerivationKindCensus,
    BaseNormalizedRederivePlan, BaseNormalizedRederiveRatifiedDroppedEmitterCensus,
    BaseNormalizedRederiveRawFactCompleteness, BaseNormalizedRederiveRawFactRangeProof,
    BaseNormalizedRederiveReplayTargetSnapshot, base_normalized_rederive_json_digest,
};

pub(super) const POSTGRES_BINARY_PROTOCOL_VALUE_LIMIT_BYTES: usize = 2_147_483_647;
pub(super) const MAX_RUN_STATE_JSON_BIND_BYTES: usize = 1_000_000;
pub(super) const MAX_RUN_STATE_TEXT_BIND_BYTES: usize = 4096;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct BaseNormalizedRederiveRunPlanSnapshot {
    pub(super) deployment_profile: String,
    pub(super) replay_target_block: i64,
    pub(super) max_affected_block: Option<i64>,
    pub(super) replay_target_floor_block: Option<i64>,
    pub(super) derivation_kind_census: Vec<BaseNormalizedRederiveDerivationKindCensus>,
    #[serde(default)]
    pub(super) ratified_dropped_orphan_emitter_census:
        Vec<BaseNormalizedRederiveRatifiedDroppedEmitterCensus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) active_replay_target_snapshot_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(super) active_replay_target_snapshot: Vec<BaseNormalizedRederiveReplayTargetSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) active_manifest_snapshot_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(super) active_manifest_snapshot: Vec<BaseNormalizedRederiveActiveManifestSnapshot>,
    #[serde(default)]
    pub(super) raw_fact_range_proof: BaseNormalizedRederiveRawFactRangeProof,
    #[serde(default)]
    pub(super) raw_fact_safety_checks_deferred: bool,
    pub(super) cursor_census: BaseNormalizedRederiveCursorCensus,
    pub(super) counts: BaseNormalizedRederiveCounts,
    pub(super) raw_fact_completeness: BaseNormalizedRederiveRawFactCompleteness,
}

impl BaseNormalizedRederiveRunPlanSnapshot {
    pub(super) fn from_plan(plan: &BaseNormalizedRederivePlan) -> Result<Self> {
        Ok(Self {
            deployment_profile: plan.deployment_profile.clone(),
            replay_target_block: plan.replay_target_block,
            max_affected_block: plan.max_affected_block,
            replay_target_floor_block: plan.replay_target_floor_block,
            derivation_kind_census: plan.derivation_kind_census.clone(),
            ratified_dropped_orphan_emitter_census: plan
                .ratified_dropped_orphan_emitter_census
                .clone(),
            active_replay_target_snapshot_digest: Some(base_normalized_rederive_json_digest(
                &plan.active_replay_target_snapshot,
            )?),
            active_replay_target_snapshot: Vec::new(),
            active_manifest_snapshot_digest: Some(base_normalized_rederive_json_digest(
                &plan.active_manifest_snapshot,
            )?),
            active_manifest_snapshot: Vec::new(),
            raw_fact_range_proof: plan.raw_fact_range_proof.clone(),
            raw_fact_safety_checks_deferred: plan.raw_fact_safety_checks_deferred,
            cursor_census: plan.cursor_census.clone(),
            counts: plan.counts.clone(),
            raw_fact_completeness: plan.raw_fact_completeness.clone(),
        })
    }

    pub(super) fn to_plan_with_snapshots(
        &self,
        active_replay_target_snapshot: Vec<BaseNormalizedRederiveReplayTargetSnapshot>,
        active_manifest_snapshot: Vec<BaseNormalizedRederiveActiveManifestSnapshot>,
    ) -> BaseNormalizedRederivePlan {
        BaseNormalizedRederivePlan {
            deployment_profile: self.deployment_profile.clone(),
            replay_target_block: self.replay_target_block,
            max_affected_block: self.max_affected_block,
            replay_target_floor_block: self.replay_target_floor_block,
            derivation_kind_census: self.derivation_kind_census.clone(),
            ratified_dropped_orphan_emitter_census: self
                .ratified_dropped_orphan_emitter_census
                .clone(),
            active_replay_target_snapshot,
            active_manifest_snapshot,
            raw_fact_range_proof: self.raw_fact_range_proof.clone(),
            raw_fact_safety_checks_deferred: self.raw_fact_safety_checks_deferred,
            cursor_census: self.cursor_census.clone(),
            counts: self.counts.clone(),
            raw_fact_completeness: self.raw_fact_completeness.clone(),
        }
    }

    pub(super) fn stored_active_replay_target_snapshot_digest(&self) -> Result<Option<String>> {
        if let Some(digest) = &self.active_replay_target_snapshot_digest {
            return Ok(Some(digest.clone()));
        }
        if self.active_replay_target_snapshot.is_empty() {
            return Ok(None);
        }
        base_normalized_rederive_json_digest(&self.active_replay_target_snapshot).map(Some)
    }

    pub(super) fn stored_active_manifest_snapshot_digest(&self) -> Result<Option<String>> {
        if let Some(digest) = &self.active_manifest_snapshot_digest {
            return Ok(Some(digest.clone()));
        }
        if self.active_manifest_snapshot.is_empty() {
            return Ok(None);
        }
        base_normalized_rederive_json_digest(&self.active_manifest_snapshot).map(Some)
    }

    pub(super) fn legacy_active_replay_target_snapshot_digest(&self) -> Result<Option<String>> {
        if self.active_replay_target_snapshot.is_empty() {
            return Ok(None);
        }
        base_normalized_rederive_json_digest(&self.active_replay_target_snapshot).map(Some)
    }

    pub(super) fn legacy_active_manifest_snapshot_digest(&self) -> Result<Option<String>> {
        if self.active_manifest_snapshot.is_empty() {
            return Ok(None);
        }
        base_normalized_rederive_json_digest(&self.active_manifest_snapshot).map(Some)
    }

    pub(super) fn store_active_replay_target_snapshot_digest(&mut self, digest: String) {
        self.active_replay_target_snapshot_digest = Some(digest);
        self.active_replay_target_snapshot.clear();
    }

    pub(super) fn store_active_manifest_snapshot_digest(&mut self, digest: String) {
        self.active_manifest_snapshot_digest = Some(digest);
        self.active_manifest_snapshot.clear();
    }
}

pub(super) fn run_state_json_bind_value<T>(label: &str, value: &T) -> Result<Value>
where
    T: Serialize + ?Sized,
{
    let value = serde_json::to_value(value)
        .with_context(|| format!("failed to encode Base rederive {label} bind"))?;
    let size = serialized_json_value_size_bytes(&value)?;
    ensure!(
        size <= MAX_RUN_STATE_JSON_BIND_BYTES,
        "Base normalized-event rederive {label} bind is too large: {size} bytes exceeds {MAX_RUN_STATE_JSON_BIND_BYTES}"
    );
    ensure!(
        size <= POSTGRES_BINARY_PROTOCOL_VALUE_LIMIT_BYTES,
        "Base normalized-event rederive {label} bind is too large for PostgreSQL binary protocol: {size} bytes exceeds {POSTGRES_BINARY_PROTOCOL_VALUE_LIMIT_BYTES}"
    );
    Ok(value)
}

pub(super) fn ensure_run_state_text_bind_size(label: &str, value: &str) -> Result<()> {
    let size = value.len();
    ensure!(
        size <= MAX_RUN_STATE_TEXT_BIND_BYTES,
        "Base normalized-event rederive {label} bind is too large: {size} bytes exceeds {MAX_RUN_STATE_TEXT_BIND_BYTES}"
    );
    ensure!(
        size <= POSTGRES_BINARY_PROTOCOL_VALUE_LIMIT_BYTES,
        "Base normalized-event rederive {label} bind is too large for PostgreSQL binary protocol: {size} bytes exceeds {POSTGRES_BINARY_PROTOCOL_VALUE_LIMIT_BYTES}"
    );
    Ok(())
}

pub(super) fn serialized_json_size_bytes<T>(value: &T) -> Result<usize>
where
    T: Serialize + ?Sized,
{
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .context("failed to measure Base rederive JSON bind")
}

pub(super) fn serialized_json_value_size_bytes(value: &Value) -> Result<usize> {
    serialized_json_size_bytes(value)
}
