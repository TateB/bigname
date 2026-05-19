const BASENAMES_NAMESPACE: &str = bigname_storage::BASENAMES_NAMESPACE;
const BASENAMES_COMPAT_SOURCE_CHAIN_ID: &str = bigname_storage::BASE_MAINNET_CHAIN_ID;
const BASENAMES_COMPAT_TARGET_CHAIN_ID: &str = bigname_storage::ETHEREUM_MAINNET_CHAIN_ID;
const BASENAMES_COMPAT_CONTRACT_ADDRESS: &str = bigname_storage::BASENAMES_L1_RESOLVER_ADDRESS;

mod resolution_verified {
    use super::*;

    mod topology {
        use super::super::*;

        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/responses/resolution_verified/topology.rs"
        ));
    }

    mod execution_summary {
        use super::super::*;

        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/responses/resolution_verified/execution_summary.rs"
        ));
    }

    mod readback {
        use super::super::*;

        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/responses/resolution_verified/readback.rs"
        ));
    }

    mod response {
        use super::super::*;

        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/responses/resolution_verified/response.rs"
        ));
    }

    pub(super) use readback::ResolutionVerifiedOutcomeLookup;

    pub(super) fn build_resolution_declared_state(
        row: &NameCurrentRow,
        record_inventory_row: Option<&RecordInventoryCurrentRow>,
        records: &[ResolutionRecordKey],
    ) -> JsonValue {
        response::build_resolution_declared_state(row, record_inventory_row, records)
    }

    pub(super) fn build_resolution_verified_state(
        row: &NameCurrentRow,
        records: &[ResolutionRecordKey],
        persisted_outcome: Option<&ExecutionOutcome>,
    ) -> Result<JsonValue> {
        response::build_resolution_verified_state(row, records, persisted_outcome)
    }

    pub(super) fn build_resolution_execution_explain_verified_state(
        row: &NameCurrentRow,
        records: &[ResolutionRecordKey],
        trace: &ExecutionTrace,
        outcome: &ExecutionOutcome,
    ) -> Result<JsonValue> {
        response::build_resolution_execution_explain_verified_state(row, records, trace, outcome)
    }

    pub(super) async fn lookup_resolution_verified_outcome(
        pool: &PgPool,
        row: &NameCurrentRow,
        records: &[ResolutionRecordKey],
        record_inventory_row: Option<&RecordInventoryCurrentRow>,
        selected_snapshot: &SelectedSnapshot,
    ) -> std::result::Result<readback::ResolutionVerifiedOutcomeLookup, SnapshotSelectionError>
    {
        readback::lookup_resolution_verified_outcome(
            pool,
            row,
            records,
            record_inventory_row,
            selected_snapshot,
        )
        .await
    }

    pub(super) fn build_resolution_execution_cache_key(
        row: &NameCurrentRow,
        records: &[ResolutionRecordKey],
        record_inventory_row: Option<&RecordInventoryCurrentRow>,
        chain_positions: JsonValue,
    ) -> Result<ExecutionCacheKey> {
        readback::build_resolution_execution_cache_key(
            row,
            records,
            record_inventory_row,
            chain_positions,
        )
    }

    pub(super) fn resolution_execution_cache_lookup_records(
        row: &NameCurrentRow,
        records: &[ResolutionRecordKey],
    ) -> Vec<ResolutionRecordKey> {
        readback::resolution_execution_cache_lookup_records(row, records)
    }

    pub(super) async fn load_supported_record_inventory_current(
        pool: &PgPool,
        row: &NameCurrentRow,
    ) -> Result<Option<RecordInventoryCurrentRow>> {
        readback::load_supported_record_inventory_current(pool, row).await
    }

    pub(super) async fn load_supported_record_inventory_current_for_snapshot(
        pool: &PgPool,
        row: &NameCurrentRow,
        selected_snapshot: &SelectedSnapshot,
    ) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
        readback::load_supported_record_inventory_current_for_snapshot(pool, row, selected_snapshot)
            .await
    }

    pub(super) async fn load_explicit_unsupported_record_inventory_current(
        pool: &PgPool,
        row: &NameCurrentRow,
    ) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
        readback::load_explicit_unsupported_record_inventory_current(pool, row).await
    }

    pub(super) async fn load_record_inventory_current_matching_selected_snapshot(
        pool: &PgPool,
        row: &NameCurrentRow,
        selected_snapshot: &SelectedSnapshot,
        allow_selected_superset: bool,
    ) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
        readback::load_record_inventory_current_matching_selected_snapshot(
            pool,
            row,
            selected_snapshot,
            allow_selected_superset,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) fn record_inventory_chain_positions_match_selected_snapshot(
        projected: &ChainPositions,
        selected_snapshot: &SelectedSnapshot,
        allow_selected_superset: bool,
    ) -> bool {
        readback::record_inventory_chain_positions_match_selected_snapshot(
            projected,
            selected_snapshot,
            allow_selected_superset,
        )
    }

    pub(super) fn resolution_verified_support_boundary(
        row: &NameCurrentRow,
        record_inventory_row: Option<&RecordInventoryCurrentRow>,
    ) -> Option<bigname_storage::VerifiedResolutionSupportBoundary> {
        readback::resolution_verified_support_boundary(row, record_inventory_row)
    }
}

use self::resolution_verified::{
    build_resolution_declared_state, build_resolution_execution_cache_key,
    build_resolution_execution_explain_verified_state, build_resolution_verified_state,
    lookup_resolution_verified_outcome, load_explicit_unsupported_record_inventory_current,
    load_record_inventory_current_matching_selected_snapshot,
    load_supported_record_inventory_current, load_supported_record_inventory_current_for_snapshot,
    ResolutionVerifiedOutcomeLookup, resolution_execution_cache_lookup_records,
    resolution_verified_support_boundary,
};

#[cfg(test)]
pub(crate) use self::resolution_verified::record_inventory_chain_positions_match_selected_snapshot;
