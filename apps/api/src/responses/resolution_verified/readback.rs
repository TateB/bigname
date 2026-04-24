pub(super) fn supported_resolution_verified_readback_records(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
) -> Vec<ResolutionRecordKey> {
    bigname_storage::supported_resolution_verified_readback_records(row, records)
}

pub(super) async fn load_resolution_verified_outcome(
    pool: &PgPool,
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    selected_snapshot: &SelectedSnapshot,
) -> std::result::Result<Option<ExecutionOutcome>, SnapshotSelectionError> {
    if resolution_verified_support_boundary(row, record_inventory_row).is_none() {
        return Ok(None);
    }
    if record_inventory_blocks_verified_entrypoint(record_inventory_row) {
        return Ok(None);
    }

    let supported_records = supported_resolution_verified_readback_records(row, records);
    if supported_records.is_empty() {
        return Ok(None);
    }
    let cache_key_records = resolution_execution_cache_lookup_records(row, &supported_records);

    let cache_key = build_resolution_execution_cache_key(
        row,
        &cache_key_records,
        record_inventory_row,
        selected_snapshot.chain_positions_value(),
    )
    .map_err(|error| {
        SnapshotSelectionError::internal(format!(
            "failed to derive persisted verified resolution cache key for {}: {error}",
            row.logical_name_id
        ))
    })?;
    let outcome = load_execution_outcome(pool, &cache_key)
        .await
        .map_err(|error| {
            SnapshotSelectionError::internal(format!(
                "failed to load persisted verified resolution outcome for {}: {error}",
                row.logical_name_id
            ))
        })?;

    match outcome {
        Some(outcome) => {
            validate_loaded_resolution_verified_outcome(row, records, &outcome)?;
            Ok(Some(outcome))
        }
        None => Err(SnapshotSelectionError::stale(format!(
            "persisted verified resolution output is not available for the selected snapshot"
        ))),
    }
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
            return Err(SnapshotSelectionError::stale(format!(
                "persisted verified resolution output is not available for the selected snapshot"
            )));
        }
    }

    Ok(())
}

fn record_inventory_blocks_verified_entrypoint(
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> bool {
    record_inventory_row.is_some_and(|row| {
        string_field(provenance_field(&row.coverage, "unsupported_reason")).is_some()
    })
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
    let Some((resource_id, record_version_boundary)) = record_inventory_lookup_key(row) else {
        return Ok(None);
    };

    if let Some(record_inventory_row) =
        load_record_inventory_current(pool, resource_id, &record_version_boundary).await?
    {
        return Ok(Some(record_inventory_row));
    }

    if record_version_boundary_has_pointer(&record_version_boundary) {
        return Ok(None);
    }

    let Some(persisted_boundary) =
        find_supported_record_inventory_boundary(pool, resource_id, &record_version_boundary)
            .await?
    else {
        return Ok(None);
    };

    load_record_inventory_current(pool, resource_id, &persisted_boundary)
        .await?
        .with_context(|| {
            format!(
                "matched record_inventory_current boundary for resource_id {resource_id} but the projection row was not loadable"
            )
        })
        .map(Some)
}

pub(super) async fn load_supported_record_inventory_current_for_snapshot(
    pool: &PgPool,
    row: &NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> std::result::Result<Option<RecordInventoryCurrentRow>, SnapshotSelectionError> {
    let Some((resource_id, record_version_boundary)) = record_inventory_lookup_key(row) else {
        return Ok(None);
    };

    match load_record_inventory_current_for_snapshot(
        pool,
        resource_id,
        &record_version_boundary,
        &selected_snapshot.chain_positions,
    )
    .await?
    {
        SnapshotProjectionRead::Found(record_inventory_row) => {
            return Ok(Some(record_inventory_row));
        }
        SnapshotProjectionRead::NotFound => {}
    }

    if record_version_boundary_has_pointer(&record_version_boundary) {
        return Ok(None);
    }

    let Some(persisted_boundary) =
        find_supported_record_inventory_boundary(pool, resource_id, &record_version_boundary)
            .await
            .map_err(|error| {
                SnapshotSelectionError::internal(format!(
                    "failed to locate supported record_inventory_current boundary for resource_id {resource_id}: {error}"
                ))
            })?
    else {
        return Ok(None);
    };

    match load_record_inventory_current_for_snapshot(
        pool,
        resource_id,
        &persisted_boundary,
        &selected_snapshot.chain_positions,
    )
    .await?
    {
        SnapshotProjectionRead::Found(record_inventory_row) => Ok(Some(record_inventory_row)),
        SnapshotProjectionRead::NotFound => Err(SnapshotSelectionError::internal(format!(
            "matched record_inventory_current boundary for resource_id {resource_id} but the projection row was not loadable"
        ))),
    }
}

pub(super) fn record_inventory_lookup_key(row: &NameCurrentRow) -> Option<(Uuid, JsonValue)> {
    bigname_storage::resolution_record_inventory_lookup_key(row)
}
pub(super) fn resolution_verified_support_boundary(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Option<bigname_storage::VerifiedResolutionSupportBoundary> {
    bigname_storage::resolution_verified_support_boundary(row, record_inventory_row)
}

pub(super) fn record_version_boundary_has_pointer(record_version_boundary: &JsonValue) -> bool {
    provenance_field(record_version_boundary, "normalized_event_id")
        .is_some_and(|value| !value.is_null())
        && provenance_field(record_version_boundary, "event_kind")
            .is_some_and(|value| !value.is_null())
}

pub(super) async fn find_supported_record_inventory_boundary(
    pool: &PgPool,
    resource_id: Uuid,
    record_version_boundary: &JsonValue,
) -> Result<Option<JsonValue>> {
    let logical_name_id = string_field(provenance_field(record_version_boundary, "logical_name_id"))
        .with_context(|| {
            format!(
                "supported record version boundary for resource_id {resource_id} must include logical_name_id"
            )
        })?;
    let chain_position = provenance_field(record_version_boundary, "chain_position").with_context(
        || {
            format!(
                "supported record version boundary for resource_id {resource_id} must include chain_position"
            )
        },
    )?;
    let chain_id = string_field(provenance_field(chain_position, "chain_id")).with_context(|| {
        format!(
            "supported record version boundary for resource_id {resource_id} must include chain_position.chain_id"
        )
    })?;
    let block_number = provenance_field(chain_position, "block_number")
        .and_then(JsonValue::as_i64)
        .with_context(|| {
            format!(
                "supported record version boundary for resource_id {resource_id} must include chain_position.block_number"
            )
        })?;
    let block_hash = string_field(provenance_field(chain_position, "block_hash")).with_context(|| {
        format!(
            "supported record version boundary for resource_id {resource_id} must include chain_position.block_hash"
        )
    })?;
    let timestamp = string_field(provenance_field(chain_position, "timestamp")).with_context(|| {
        format!(
            "supported record version boundary for resource_id {resource_id} must include chain_position.timestamp"
        )
    })?;

    let boundaries = sqlx::query(
        r#"
        SELECT record_version_boundary
        FROM record_inventory_current
        WHERE resource_id = $1
          AND record_version_boundary ->> 'logical_name_id' = $2
          AND record_version_boundary -> 'chain_position' ->> 'chain_id' = $3
          AND (record_version_boundary -> 'chain_position' ->> 'block_number')::bigint = $4
          AND record_version_boundary -> 'chain_position' ->> 'block_hash' = $5
          AND record_version_boundary -> 'chain_position' ->> 'timestamp' = $6
        ORDER BY
          (record_version_boundary ->> 'normalized_event_id') IS NULL ASC,
          (record_version_boundary ->> 'normalized_event_id')::bigint DESC NULLS LAST
        LIMIT 2
        "#,
    )
    .bind(resource_id)
    .bind(logical_name_id)
    .bind(chain_id)
    .bind(block_number)
    .bind(block_hash)
    .bind(timestamp)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to locate supported record_inventory_current boundary for resource_id {resource_id}"
        )
    })?
    .into_iter()
    .map(|row| {
        row.try_get("record_version_boundary").with_context(|| {
            format!(
                "supported record_inventory_current lookup for resource_id {resource_id} returned a row without record_version_boundary"
            )
        })
    })
    .collect::<Result<Vec<JsonValue>>>()?;

    let Some(first_boundary) = boundaries.first().cloned() else {
        return Ok(None);
    };
    let second_boundary = boundaries.get(1);
    if let Some(second_boundary) = second_boundary
        && (!record_version_boundary_has_pointer(&first_boundary)
            || record_version_boundary_has_pointer(second_boundary))
    {
        anyhow::bail!(
            "supported record_inventory_current lookup for resource_id {} found multiple projection rows for the same boundary anchor",
            resource_id
        );
    }

    Ok(Some(first_boundary))
}
