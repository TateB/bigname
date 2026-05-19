use super::*;

pub(super) async fn load_or_execute_resolution_verified_outcome(
    state: &AppState,
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    selected_snapshot: &SelectedSnapshot,
    use_latest_block_tag: bool,
    persist_execution: bool,
) -> std::result::Result<Option<ExecutionOutcome>, SnapshotSelectionError> {
    match lookup_resolution_verified_outcome(
        &state.pool,
        row,
        records,
        record_inventory_row,
        selected_snapshot,
    )
    .await?
    {
        ResolutionVerifiedOutcomeLookup::Found(outcome) => Ok(Some(outcome)),
        ResolutionVerifiedOutcomeLookup::NotSupported => Ok(None),
        ResolutionVerifiedOutcomeLookup::CacheMiss => Ok(Some(
            execute_ens_verified_resolution_cache_miss(
                &state.pool,
                &state.chain_rpc_urls,
                row,
                records,
                record_inventory_row,
                selected_snapshot,
                use_latest_block_tag,
                persist_execution,
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
