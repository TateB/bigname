#[path = "readback/record_inventory.rs"]
mod record_inventory;

pub(super) fn supported_resolution_verified_readback_records(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
) -> Vec<ResolutionRecordKey> {
    bigname_storage::supported_resolution_verified_readback_records(row, records)
}

pub(crate) enum ResolutionVerifiedOutcomeLookup {
    Found(ExecutionOutcome),
    CacheMiss,
    NotSupported,
}

struct ResolutionVerifiedCacheLookupPlan {
    compact_selector_records: Vec<ResolutionRecordKey>,
    full_selector_records: Vec<ResolutionRecordKey>,
}

impl ResolutionVerifiedCacheLookupPlan {
    fn new(row: &NameCurrentRow, supported_records: Vec<ResolutionRecordKey>) -> Self {
        Self {
            compact_selector_records: resolution_execution_cache_lookup_records(
                row,
                &supported_records,
            ),
            full_selector_records: supported_records,
        }
    }

    fn should_probe_full_selector_fallback(&self) -> bool {
        self.compact_selector_records != self.full_selector_records
    }
}

pub(super) async fn lookup_resolution_verified_outcome(
    pool: &PgPool,
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    selected_snapshot: &SelectedSnapshot,
) -> std::result::Result<ResolutionVerifiedOutcomeLookup, SnapshotSelectionError> {
    if resolution_verified_support_boundary(row, record_inventory_row).is_none() {
        return Ok(ResolutionVerifiedOutcomeLookup::NotSupported);
    }

    let supported_records = supported_resolution_verified_readback_records(row, records);
    if supported_records.is_empty() {
        return Ok(ResolutionVerifiedOutcomeLookup::NotSupported);
    }
    let cache_lookup = ResolutionVerifiedCacheLookupPlan::new(row, supported_records);
    let outcome = load_resolution_verified_outcome_with_full_selector_fallback(
        pool,
        row,
        record_inventory_row,
        selected_snapshot,
        &cache_lookup,
    )
    .await?;

    match outcome {
        Some(outcome) => {
            validate_loaded_resolution_verified_outcome(row, records, &outcome)?;
            Ok(ResolutionVerifiedOutcomeLookup::Found(outcome))
        }
        None => Ok(ResolutionVerifiedOutcomeLookup::CacheMiss),
    }
}

async fn load_resolution_verified_outcome_with_full_selector_fallback(
    pool: &PgPool,
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    selected_snapshot: &SelectedSnapshot,
    cache_lookup: &ResolutionVerifiedCacheLookupPlan,
) -> std::result::Result<Option<ExecutionOutcome>, SnapshotSelectionError> {
    let compact_outcome = load_resolution_verified_outcome_for_records(
        pool,
        row,
        &cache_lookup.compact_selector_records,
        record_inventory_row,
        selected_snapshot,
        "persisted",
        "persisted verified resolution outcome",
    )
    .await?;

    if compact_outcome.is_some() || !cache_lookup.should_probe_full_selector_fallback() {
        return Ok(compact_outcome);
    }

    load_resolution_verified_outcome_for_records(
        pool,
        row,
        &cache_lookup.full_selector_records,
        record_inventory_row,
        selected_snapshot,
        "full-selector",
        "full-selector persisted verified resolution outcome",
    )
    .await
}

async fn load_resolution_verified_outcome_for_records(
    pool: &PgPool,
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    selected_snapshot: &SelectedSnapshot,
    cache_key_label: &'static str,
    load_label: &'static str,
) -> std::result::Result<Option<ExecutionOutcome>, SnapshotSelectionError> {
    let cache_key = build_resolution_execution_cache_key(
        row,
        records,
        record_inventory_row,
        selected_snapshot.chain_positions_value(),
    )
    .map_err(|error| {
        SnapshotSelectionError::internal(format!(
            "failed to derive {cache_key_label} verified resolution cache key for {}: {error}",
            row.logical_name_id
        ))
    })?;

    load_execution_outcome(pool, &cache_key).await.map_err(|error| {
        SnapshotSelectionError::internal(format!(
            "failed to load {load_label} for {}: {error}",
            row.logical_name_id
        ))
    })
}

fn validate_loaded_resolution_verified_outcome(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    outcome: &ExecutionOutcome,
) -> std::result::Result<(), SnapshotSelectionError> {
    let supported_records = supported_resolution_verified_readback_records(row, records);
    if supported_records.is_empty() {
        return Ok(());
    }

    let Ok(persisted_queries) = persisted_verified_queries_by_record_key(outcome) else {
        return Ok(());
    };

    for record in supported_records {
        if !persisted_queries.contains_key(&record.record_key) {
            return Err(SnapshotSelectionError::stale(
                "persisted verified resolution output is not available for the selected snapshot"
                    .to_owned(),
            ));
        }
    }

    Ok(())
}

pub(super) fn reordered_persisted_verified_queries(
    outcome: &ExecutionOutcome,
    records: &[ResolutionRecordKey],
) -> Result<JsonValue> {
    let queries_by_record_key = persisted_verified_queries_by_record_key(outcome)?;

    let requested_record_keys = records
        .iter()
        .map(|record| record.record_key.clone())
        .collect::<BTreeSet<_>>();
    if queries_by_record_key.len() != requested_record_keys.len()
        || queries_by_record_key
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            != requested_record_keys
    {
        bail!("persisted execution outcome selector set did not match requested records");
    }

    Ok(JsonValue::Array(
        records
            .iter()
            .map(|record| {
                queries_by_record_key
                    .get(&record.record_key)
                    .cloned()
                    .with_context(|| {
                        format!(
                            "persisted execution outcome did not include selector {}",
                            record.record_key
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?,
    ))
}

pub(super) fn persisted_verified_queries_by_record_key(
    outcome: &ExecutionOutcome,
) -> Result<BTreeMap<String, JsonValue>> {
    let outcome_payload = outcome
        .outcome_payload
        .as_ref()
        .context("persisted execution outcome must set outcome_payload")?;
    let verified_queries = provenance_field(outcome_payload, "verified_queries")
        .and_then(JsonValue::as_array)
        .context("persisted execution outcome must set verified_queries")?;

    let mut queries_by_record_key = BTreeMap::new();
    for query in verified_queries {
        let record_key = string_field(provenance_field(query, "record_key"))
            .context("persisted verified query must include record_key")?;
        if queries_by_record_key
            .insert(record_key.clone(), query.clone())
            .is_some()
        {
            bail!("persisted execution outcome contained duplicate verified query {record_key}");
        }
    }

    Ok(queries_by_record_key)
}
pub(super) fn build_resolution_execution_cache_key(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    chain_positions: JsonValue,
) -> Result<ExecutionCacheKey> {
    bigname_storage::build_resolution_execution_cache_key(
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
    bigname_storage::resolution_execution_cache_lookup_records(row, records)
}
pub(super) async fn load_supported_record_inventory_current(
    pool: &PgPool,
    row: &NameCurrentRow,
) -> Result<Option<RecordInventoryCurrentRow>> {
    record_inventory::load_supported_record_inventory_current(pool, row).await
}

pub(super) async fn load_supported_record_inventory_current_for_snapshot(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    record_inventory::load_supported_record_inventory_current_for_snapshot(
        pool,
        row,
        selected_snapshot,
    )
    .await
}

pub(super) async fn load_explicit_unsupported_record_inventory_current(
    pool: &PgPool,
    row: &NameCurrentRow,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    record_inventory::load_explicit_unsupported_record_inventory_current(pool, row).await
}

pub(super) async fn load_record_inventory_current_matching_selected_snapshot(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
    allow_selected_superset: bool,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    record_inventory::load_record_inventory_current_matching_selected_snapshot(
        pool,
        row,
        selected_snapshot,
        allow_selected_superset,
    )
    .await
}

#[cfg(test)]
pub(super) fn record_inventory_chain_positions_match_selected_snapshot(
    projected: &ChainPositions,
    selected_snapshot: &SelectedSnapshot,
    allow_selected_superset: bool,
) -> bool {
    record_inventory::record_inventory_chain_positions_match_selected_snapshot(
        projected,
        selected_snapshot,
        allow_selected_superset,
    )
}

pub(super) fn resolution_verified_support_boundary(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Option<bigname_storage::VerifiedResolutionSupportBoundary> {
    record_inventory::resolution_verified_support_boundary(row, record_inventory_row)
}
