use std::collections::BTreeMap;

/// Sync summary for normalized events derived from stored active manifests.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestNormalizedEventSyncSummary {
    pub total_synced_count: usize,
    pub total_inserted_count: usize,
    pub by_kind: BTreeMap<String, ManifestNormalizedEventKindSyncSummary>,
}

/// Per-kind sync summary for logging.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestNormalizedEventKindSyncSummary {
    pub synced_count: usize,
    pub inserted_count: usize,
}

#[derive(Clone, Debug)]
pub(super) struct ActiveCapabilityRow {
    pub(super) capability_name: String,
    pub(super) status: String,
    pub(super) notes: Option<String>,
}
