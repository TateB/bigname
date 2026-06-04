use std::collections::BTreeMap;

use super::EnsV1UnwrappedAuthoritySyncSummary;

pub(super) fn empty_summary(scanned_log_count: usize) -> EnsV1UnwrappedAuthoritySyncSummary {
    EnsV1UnwrappedAuthoritySyncSummary {
        scanned_log_count,
        matched_log_count: 0,
        total_name_surface_count: 0,
        total_resource_count: 0,
        total_surface_binding_count: 0,
        total_normalized_event_count: 0,
        total_normalized_event_inserted_count: 0,
        by_kind: Default::default(),
    }
}

pub(super) fn build_summary(
    scanned_log_count: usize,
    matched_log_count: usize,
    materialized_counts: (usize, usize, usize),
    flushed_event_counts: (usize, usize),
    normalized_event_counts: (usize, usize),
    by_kind: BTreeMap<String, usize>,
) -> EnsV1UnwrappedAuthoritySyncSummary {
    EnsV1UnwrappedAuthoritySyncSummary {
        scanned_log_count,
        matched_log_count,
        total_name_surface_count: materialized_counts.0,
        total_resource_count: materialized_counts.1,
        total_surface_binding_count: materialized_counts.2,
        total_normalized_event_count: flushed_event_counts.0 + normalized_event_counts.0,
        total_normalized_event_inserted_count: flushed_event_counts.1 + normalized_event_counts.1,
        by_kind,
    }
}
