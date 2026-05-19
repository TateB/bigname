#[path = "backfill/concurrent_execution.rs"]
mod concurrent_execution;
#[path = "backfill/failure_recording.rs"]
mod failure_recording;
#[path = "backfill/fetching.rs"]
mod fetching;
#[path = "backfill/range_resolution.rs"]
mod range_resolution;
#[path = "backfill/reservation_execution.rs"]
mod reservation_execution;
#[path = "backfill/selection.rs"]
mod selection;

use anyhow::{Result, bail};
use bigname_manifests::WatchedSourceSelectorPlan;
use sqlx::types::time::OffsetDateTime;

use crate::reconciliation::HeaderAuditMode;

pub(crate) use concurrent_execution::run_resumable_hash_pinned_backfill_job_concurrently;
#[allow(unused_imports)]
pub(crate) use fetching::run_hash_pinned_backfill_range;
#[cfg(test)]
pub(crate) use reservation_execution::COMPACT_SOURCE_IDENTITY_SELECTED_TARGET_THRESHOLD;
pub(crate) use reservation_execution::{
    DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS, backfill_job_source_identity_payload,
    create_hash_pinned_backfill_job, hash_pinned_backfill_range_specs,
    run_resumable_hash_pinned_backfill_job,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BackfillBlockRange {
    pub(crate) from_block: i64,
    pub(crate) to_block: i64,
}

impl BackfillBlockRange {
    pub(crate) fn new(from_block: i64, to_block: i64) -> Result<Self> {
        if from_block < 0 {
            bail!("backfill from block cannot be negative: {from_block}");
        }
        if to_block < 0 {
            bail!("backfill to block cannot be negative: {to_block}");
        }
        if from_block > to_block {
            bail!("backfill range start {from_block} is after end {to_block}");
        }

        Ok(Self {
            from_block,
            to_block,
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum BackfillAdapterSyncMode {
    #[default]
    Auto,
    Inline,
    RawOnly,
}

impl BackfillAdapterSyncMode {
    pub(crate) fn parse(value: &str) -> Result<Self> {
        match value.trim() {
            "" | "auto" => Ok(Self::Auto),
            "inline" => Ok(Self::Inline),
            "raw-only" | "raw_only" => Ok(Self::RawOnly),
            value => bail!(
                "hash-pinned backfill adapter sync mode must be auto, inline, or raw-only, got {value}"
            ),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Inline => "inline",
            Self::RawOnly => "raw-only",
        }
    }

    pub(crate) fn hash_pinned_backfill_mode(self) -> Self {
        match self {
            Self::Auto => Self::Inline,
            Self::Inline | Self::RawOnly => self,
        }
    }

    pub(crate) fn startup_hash_pinned_backfill_mode(self) -> Self {
        match self {
            Self::Auto => Self::RawOnly,
            Self::Inline | Self::RawOnly => self,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BackfillJobRunConfig {
    pub(crate) deployment_profile: String,
    pub(crate) idempotency_key: String,
    pub(crate) range: BackfillBlockRange,
    pub(crate) lease_owner: String,
    pub(crate) lease_token: String,
    pub(crate) lease_expires_at: OffsetDateTime,
    pub(crate) hash_pinned_chunk_blocks: i64,
    pub(crate) adapter_sync_mode: BackfillAdapterSyncMode,
    pub(crate) header_audit_mode: HeaderAuditMode,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BackfillOutcome {
    pub(crate) chain: String,
    pub(crate) from_block: i64,
    pub(crate) to_block: i64,
    pub(crate) resolved_block_count: usize,
    pub(crate) raw_block_count: usize,
    pub(crate) raw_transaction_count: usize,
    pub(crate) raw_receipt_count: usize,
    pub(crate) raw_log_count: usize,
    pub(crate) raw_code_hash_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BackfillJobRunOutcome {
    pub(crate) backfill_job_id: i64,
    pub(crate) chain: String,
    pub(crate) from_block: i64,
    pub(crate) to_block: i64,
    pub(crate) idempotency_key: String,
    pub(crate) reserved_range_count: usize,
    pub(crate) completed_range_count: usize,
    pub(crate) resolved_block_count: usize,
    pub(crate) raw_block_count: usize,
    pub(crate) raw_transaction_count: usize,
    pub(crate) raw_receipt_count: usize,
    pub(crate) raw_log_count: usize,
    pub(crate) raw_code_hash_count: usize,
}

impl BackfillJobRunOutcome {
    fn new(
        backfill_job_id: i64,
        source_plan: &WatchedSourceSelectorPlan,
        config: &BackfillJobRunConfig,
    ) -> Self {
        Self {
            backfill_job_id,
            chain: source_plan.watched_chain_plan.chain.clone(),
            from_block: config.range.from_block,
            to_block: config.range.to_block,
            idempotency_key: config.idempotency_key.clone(),
            reserved_range_count: 0,
            completed_range_count: 0,
            resolved_block_count: 0,
            raw_block_count: 0,
            raw_transaction_count: 0,
            raw_receipt_count: 0,
            raw_log_count: 0,
            raw_code_hash_count: 0,
        }
    }

    fn add_range_outcome(&mut self, outcome: &BackfillOutcome) {
        self.resolved_block_count += outcome.resolved_block_count;
        self.raw_block_count += outcome.raw_block_count;
        self.raw_transaction_count += outcome.raw_transaction_count;
        self.raw_receipt_count += outcome.raw_receipt_count;
        self.raw_log_count += outcome.raw_log_count;
        self.raw_code_hash_count += outcome.raw_code_hash_count;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adapter_sync_auto_uses_inline_for_manual_hash_pinned_backfill() -> Result<()> {
        let auto = BackfillAdapterSyncMode::parse("")?;

        assert_eq!(auto, BackfillAdapterSyncMode::Auto);
        assert_eq!(
            BackfillAdapterSyncMode::parse("auto")?.hash_pinned_backfill_mode(),
            BackfillAdapterSyncMode::Inline
        );
        assert_eq!(
            BackfillAdapterSyncMode::parse("auto")?.startup_hash_pinned_backfill_mode(),
            BackfillAdapterSyncMode::RawOnly
        );
        assert_eq!(
            BackfillAdapterSyncMode::Inline.hash_pinned_backfill_mode(),
            BackfillAdapterSyncMode::Inline
        );
        assert_eq!(
            BackfillAdapterSyncMode::RawOnly.hash_pinned_backfill_mode(),
            BackfillAdapterSyncMode::RawOnly
        );

        Ok(())
    }
}
