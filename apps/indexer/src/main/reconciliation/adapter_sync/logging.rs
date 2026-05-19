use tracing::info;

pub(super) fn log_adapter_call_timing(
    chain: &str,
    adapter: &'static str,
    function: &'static str,
    block_hash_count: usize,
    source_scope_target_count: usize,
    scanned_log_count: usize,
    matched_log_count: usize,
    normalized_event_synced_count: usize,
    normalized_event_inserted_count: usize,
    elapsed_ms: u128,
) {
    info!(
        service = "indexer",
        chain,
        adapter,
        function,
        block_hash_count,
        source_scope_target_count,
        scanned_log_count,
        matched_log_count,
        normalized_event_synced_count,
        normalized_event_inserted_count,
        elapsed_ms,
        "normalized-event replay adapter timing completed"
    );
}
