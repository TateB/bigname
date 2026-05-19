use bigname_storage::CheckpointBlockRef;

use crate::provider::ProviderBlock;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum HeaderAuditMode {
    #[default]
    Minimal,
    RetainAuditFields,
}

impl HeaderAuditMode {
    pub(crate) fn from_retain_audit_fields(retain_audit_fields: bool) -> Self {
        if retain_audit_fields {
            Self::RetainAuditFields
        } else {
            Self::Minimal
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Minimal => "minimal",
            Self::RetainAuditFields => "retain-audit-fields",
        }
    }

    pub(crate) fn retains_audit_fields(self) -> bool {
        self == Self::RetainAuditFields
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CanonicalReconciliationStatus {
    Initialized,
    Unchanged,
    Appended,
    GapBackfilled,
    ReorgReconciled,
    AwaitingAncestor,
}

impl CanonicalReconciliationStatus {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Initialized => "initialized",
            Self::Unchanged => "unchanged",
            Self::Appended => "appended",
            Self::GapBackfilled => "gap_backfilled",
            Self::ReorgReconciled => "reorg_reconciled",
            Self::AwaitingAncestor => "awaiting_ancestor",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CanonicalReconciliation {
    pub(crate) status: CanonicalReconciliationStatus,
    pub(crate) canonical: Option<CheckpointBlockRef>,
    pub(crate) fetched_parent_count: usize,
    pub(crate) orphaned_block_count: usize,
    pub(crate) reconciled_blocks: Vec<ProviderBlock>,
    pub(crate) raw_orphan_stop_before_hash: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HeadChangeSet {
    pub(crate) canonical_head_changed: bool,
    pub(crate) safe_head_changed: bool,
    pub(crate) finalized_head_changed: bool,
}

impl HeadChangeSet {
    pub(super) fn requires_raw_payload_refresh(
        self,
        canonical_status: CanonicalReconciliationStatus,
    ) -> bool {
        canonical_status != CanonicalReconciliationStatus::Unchanged
            || self.safe_head_changed
            || self.finalized_head_changed
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ChainReconciliationOutcome {
    pub(crate) chain: String,
    pub(crate) canonical_status: CanonicalReconciliationStatus,
    pub(crate) canonical_head_changed: bool,
    pub(crate) safe_head_changed: bool,
    pub(crate) finalized_head_changed: bool,
    pub(crate) fetched_parent_count: usize,
    pub(crate) orphaned_block_count: usize,
    pub(crate) canonical_block_number: Option<i64>,
    pub(crate) safe_block_number: Option<i64>,
    pub(crate) finalized_block_number: Option<i64>,
}

pub(crate) type RawFactNormalizedEventReplaySourceScope = crate::source_scope::SourceScopeTarget;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RawFactNormalizedEventReplaySelection {
    BlockRange {
        from_block: i64,
        to_block: i64,
    },
    ScopedBlockRange {
        from_block: i64,
        to_block: i64,
        source_scope: Vec<RawFactNormalizedEventReplaySourceScope>,
    },
    BlockHashes(Vec<String>),
}

impl RawFactNormalizedEventReplaySelection {
    pub(super) fn as_str(&self) -> &'static str {
        match self {
            Self::BlockRange { .. } => "block_range",
            Self::ScopedBlockRange { .. } => "scoped_block_range",
            Self::BlockHashes(_) => "block_hashes",
        }
    }

    pub(super) fn source_scope_target_count(&self) -> usize {
        match self {
            Self::ScopedBlockRange { source_scope, .. } => source_scope.len(),
            Self::BlockRange { .. } | Self::BlockHashes(_) => 0,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RawFactNormalizedEventReplayRequest {
    pub(crate) deployment_profile: String,
    pub(crate) chain: String,
    pub(crate) selection: RawFactNormalizedEventReplaySelection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RawFactNormalizedEventReplayOutcome {
    pub(crate) deployment_profile: String,
    pub(crate) chain: String,
    pub(crate) selection_kind: &'static str,
    pub(crate) source_scope_target_count: usize,
    pub(crate) selected_block_count: usize,
    pub(crate) canonical_raw_log_count: usize,
    pub(crate) scanned_raw_log_count: usize,
    pub(crate) matched_raw_log_count: usize,
    pub(crate) normalized_event_synced_count: usize,
    pub(crate) normalized_event_inserted_count: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PersistedRawPayloadAdapterSyncSummary {
    pub(crate) scanned_log_count: usize,
    pub(crate) matched_log_count: usize,
    pub(crate) total_synced_count: usize,
    pub(crate) total_inserted_count: usize,
}

impl PersistedRawPayloadAdapterSyncSummary {
    pub(super) fn add_counts(
        &mut self,
        scanned_log_count: usize,
        matched_log_count: usize,
        total_synced_count: usize,
        total_inserted_count: usize,
    ) {
        self.scanned_log_count += scanned_log_count;
        self.matched_log_count += matched_log_count;
        self.total_synced_count += total_synced_count;
        self.total_inserted_count += total_inserted_count;
    }
}
