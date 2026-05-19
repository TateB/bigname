use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV2RegistrarSyncSummary {
    pub scanned_log_count: usize,
    pub matched_log_count: usize,
    pub total_synced_count: usize,
    pub total_inserted_count: usize,
    pub by_kind: BTreeMap<String, EnsV2RegistrarKindSyncSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV2RegistrarKindSyncSummary {
    pub synced_count: usize,
    pub inserted_count: usize,
}

pub(super) fn empty_summary(scanned_log_count: usize) -> EnsV2RegistrarSyncSummary {
    EnsV2RegistrarSyncSummary {
        scanned_log_count,
        matched_log_count: 0,
        total_synced_count: 0,
        total_inserted_count: 0,
        by_kind: BTreeMap::new(),
    }
}
