use super::*;

pub(crate) async fn load_or_execute_resolution_verified_outcome(
    state: &AppState,
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    selected_snapshot: &SelectedSnapshot,
    use_latest_block_tag: bool,
    persist_execution: bool,
) -> std::result::Result<Option<ExecutionOutcome>, SnapshotSelectionError> {
    load_or_execute_resolution_verified_outcome_inner(
        VerifiedOutcomeExecutionRequest {
            state,
            row,
            records,
            record_inventory_row,
            selected_snapshot,
            use_latest_block_tag,
            persist_execution,
        },
        false,
    )
    .await
}

pub(crate) async fn load_or_execute_resolution_verified_outcome_treating_partial_compact_hit_as_miss(
    state: &AppState,
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    selected_snapshot: &SelectedSnapshot,
    use_latest_block_tag: bool,
    persist_execution: bool,
) -> std::result::Result<Option<ExecutionOutcome>, SnapshotSelectionError> {
    load_or_execute_resolution_verified_outcome_inner(
        VerifiedOutcomeExecutionRequest {
            state,
            row,
            records,
            record_inventory_row,
            selected_snapshot,
            use_latest_block_tag,
            persist_execution,
        },
        true,
    )
    .await
}

struct VerifiedOutcomeExecutionRequest<'a> {
    state: &'a AppState,
    row: &'a NameCurrentRow,
    records: &'a [ResolutionRecordKey],
    record_inventory_row: Option<&'a RecordInventoryCurrentRow>,
    selected_snapshot: &'a SelectedSnapshot,
    use_latest_block_tag: bool,
    persist_execution: bool,
}

async fn load_or_execute_resolution_verified_outcome_inner(
    request: VerifiedOutcomeExecutionRequest<'_>,
    treat_partial_compact_hit_as_miss: bool,
) -> std::result::Result<Option<ExecutionOutcome>, SnapshotSelectionError> {
    let lookup = if treat_partial_compact_hit_as_miss {
        lookup_resolution_verified_outcome_treating_partial_compact_hit_as_miss(
            &request.state.pool,
            request.row,
            request.records,
            request.record_inventory_row,
            request.selected_snapshot,
        )
        .await?
    } else {
        lookup_resolution_verified_outcome(
            &request.state.pool,
            request.row,
            request.records,
            request.record_inventory_row,
            request.selected_snapshot,
        )
        .await?
    };

    match lookup {
        ResolutionVerifiedOutcomeLookup::Found(outcome) => Ok(Some(outcome)),
        ResolutionVerifiedOutcomeLookup::NotSupported => Ok(None),
        ResolutionVerifiedOutcomeLookup::CacheMiss => Ok(Some(
            execute_ens_verified_resolution_cache_miss(
                &request.state.pool,
                &request.state.chain_rpc_urls,
                request.row,
                request.records,
                request.record_inventory_row,
                request.selected_snapshot,
                request.use_latest_block_tag,
                request.persist_execution,
            )
            .await?,
        )),
    }
}

async fn execute_ens_verified_resolution_cache_miss(
    pool: &PgPool,
    chain_rpc_urls: &bigname_execution::ChainRpcUrls,
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    selected_snapshot: &SelectedSnapshot,
    use_latest_block_tag: bool,
    persist_execution: bool,
) -> std::result::Result<ExecutionOutcome, SnapshotSelectionError> {
    if row.namespace != bigname_storage::ENS_NAMESPACE {
        return Err(SnapshotSelectionError::stale(
            "persisted verified resolution output is not available for the selected snapshot"
                .to_owned(),
        ));
    }
    let execution_records = records
        .iter()
        .map(|record| {
            bigname_execution::EnsResolutionRecord::new(
                record.record_key.clone(),
                record.record_family.clone(),
                record.selector_key.clone(),
            )
        })
        .collect::<Vec<_>>();

    bigname_execution::execute_ens_universal_resolver_verified_resolution(
        pool,
        bigname_execution::OnDemandEnsResolutionRequest {
            row,
            records: &execution_records,
            record_inventory_row,
            chain_positions: selected_snapshot.chain_positions_value(),
            chain_rpc_urls,
            use_latest_block_tag,
            persist_execution,
        },
    )
    .await
    .map_err(|error| match error.kind() {
        bigname_execution::OnDemandEnsResolutionErrorKind::Configuration => {
            SnapshotSelectionError::stale(error.message().to_owned())
        }
        bigname_execution::OnDemandEnsResolutionErrorKind::Unsupported => {
            SnapshotSelectionError::stale(
                "persisted verified resolution output is not available for the selected snapshot"
                    .to_owned(),
            )
        }
        bigname_execution::OnDemandEnsResolutionErrorKind::Persistence => {
            SnapshotSelectionError::stale(format!(
                "on-demand verified resolution output could not be persisted for the selected snapshot: {}",
                error.message()
            ))
        }
    })
}
