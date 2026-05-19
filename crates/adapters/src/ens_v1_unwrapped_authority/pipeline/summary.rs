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
