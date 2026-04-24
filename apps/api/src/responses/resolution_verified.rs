const BASENAMES_NAMESPACE: &str = bigname_storage::BASENAMES_NAMESPACE;
const BASENAMES_COMPAT_SOURCE_CHAIN_ID: &str = bigname_storage::BASE_MAINNET_CHAIN_ID;
const BASENAMES_COMPAT_TARGET_CHAIN_ID: &str = bigname_storage::ETHEREUM_MAINNET_CHAIN_ID;
const BASENAMES_COMPAT_CONTRACT_ADDRESS: &str = bigname_storage::BASENAMES_L1_RESOLVER_ADDRESS;

impl bigname_storage::VerifiedResolutionRecord for ResolutionRecordKey {
    fn record_key(&self) -> &str {
        &self.record_key
    }

    fn record_family(&self) -> &str {
        &self.record_family
    }

    fn selector_key(&self) -> Option<&str> {
        self.selector_key.as_deref()
    }
}

fn build_resolution_declared_state(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    records: &[ResolutionRecordKey],
) -> JsonValue {
    let mut declared_state = empty_object();
    let topology = build_resolution_topology(row, record_inventory_row);
    let mut record_cache = build_record_cache_section(
        record_inventory_row,
        records,
        "declared resolution record cache is not yet projected",
    );
    if classify_supported_resolution_topology(&row.namespace, &row.logical_name_id, &topology)
        == Some(bigname_storage::VerifiedResolutionPathClass::BasenamesTransportDirect)
    {
        mark_basenames_transport_direct_unretained_record_cache_values(&mut record_cache);
    }
    insert_value_field(&mut declared_state, "topology", topology);
    insert_value_field(
        &mut declared_state,
        "record_inventory",
        build_record_inventory_section(
            record_inventory_row,
            "declared resolution record inventory is not yet projected",
        ),
    );
    insert_value_field(&mut declared_state, "record_cache", record_cache);
    declared_state
}

fn mark_basenames_transport_direct_unretained_record_cache_values(record_cache: &mut JsonValue) {
    let Some(entries) = record_cache
        .as_object_mut()
        .and_then(|object| object.get_mut("entries"))
        .and_then(JsonValue::as_array_mut)
    else {
        return;
    };

    for entry in entries {
        let is_missing_numeric_addr = string_field(provenance_field(entry, "status"))
            .is_some_and(|status| status == "not_found")
            && string_field(provenance_field(entry, "record_family"))
                .is_some_and(|family| family == "addr")
            && string_field(provenance_field(entry, "selector_key"))
                .is_some_and(|selector| selector.as_bytes().iter().all(u8::is_ascii_digit));
        if !is_missing_numeric_addr {
            continue;
        }

        insert_string_field(entry, "status", "unsupported".to_owned());
        insert_string_field(
            entry,
            "unsupported_reason",
            "value_not_retained_in_normalized_events".to_owned(),
        );
    }
}

fn build_resolution_verified_state(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    persisted_outcome: Option<&ExecutionOutcome>,
) -> Result<JsonValue> {
    let mut verified_state = empty_object();
    let persisted_queries_by_record_key = persisted_outcome
        .map(|outcome| -> Result<BTreeMap<String, JsonValue>> {
            let supported_records = supported_resolution_verified_readback_records(row, records);
            let persisted_queries = persisted_verified_queries_by_record_key(outcome)?;
            Ok(supported_records
                .into_iter()
                .filter_map(|record| {
                    persisted_queries
                        .get(&record.record_key)
                        .cloned()
                        .map(|query| (record.record_key, query))
                })
                .collect::<BTreeMap<_, _>>())
        })
        .transpose()?
        .unwrap_or_default();
    insert_value_field(
        &mut verified_state,
        "verified_queries",
        JsonValue::Array(
            records
                .iter()
                .map(|record| {
                    persisted_queries_by_record_key
                        .get(&record.record_key)
                        .cloned()
                        .unwrap_or_else(|| build_resolution_verified_query(record))
                })
                .collect(),
        ),
    );
    Ok(verified_state)
}

fn build_resolution_execution_explain_verified_state(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
) -> Result<JsonValue> {
    let mut verified_state = empty_object();
    insert_value_field(
        &mut verified_state,
        "execution",
        build_resolution_execution_summary(row, trace, outcome)?,
    );
    insert_value_field(
        &mut verified_state,
        "verified_queries",
        reordered_persisted_verified_queries(outcome, records)?,
    );
    Ok(verified_state)
}

fn build_resolution_verified_query(record: &ResolutionRecordKey) -> JsonValue {
    let mut query = empty_object();
    insert_string_field(&mut query, "record_key", record.record_key.clone());
    insert_string_field(&mut query, "status", "unsupported".to_owned());
    insert_string_field(
        &mut query,
        "unsupported_reason",
        "verified resolution entrypoint is not yet supported".to_owned(),
    );
    query
}

fn supported_resolution_verified_readback_records(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
) -> Vec<ResolutionRecordKey> {
    bigname_storage::supported_resolution_verified_readback_records(row, records)
}

async fn load_resolution_verified_outcome(
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
    let outcome = load_execution_outcome(pool, &cache_key).await.map_err(|error| {
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

fn build_resolution_execution_summary(
    row: &NameCurrentRow,
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
) -> Result<JsonValue> {
    if trace.request_type != VERIFIED_RESOLUTION_REQUEST_TYPE
        || outcome.request_type != VERIFIED_RESOLUTION_REQUEST_TYPE
    {
        bail!(
            "persisted execution explain requires request_type {VERIFIED_RESOLUTION_REQUEST_TYPE}"
        );
    }

    let mut execution = empty_object();
    insert_string_field(
        &mut execution,
        "execution_trace_id",
        trace.execution_trace_id.to_string(),
    );
    insert_value_field(
        &mut execution,
        "selected_entrypoint",
        build_resolution_selected_entrypoint(trace),
    );
    insert_value_field(
        &mut execution,
        "resolver_discovery_path",
        build_resolution_execution_resolver_discovery_path(row, trace),
    );
    insert_value_field(
        &mut execution,
        "wildcard",
        build_resolution_execution_wildcard(trace),
    );
    insert_value_field(
        &mut execution,
        "alias",
        build_resolution_execution_alias(trace),
    );
    insert_value_field(
        &mut execution,
        "steps",
        JsonValue::Array(
            trace
                .steps
                .iter()
                .map(build_execution_step_summary)
                .collect(),
        ),
    );
    insert_string_field(
        &mut execution,
        "finished_at",
        format_timestamp(trace.finished_at.unwrap_or(outcome.finished_at)),
    );

    Ok(execution)
}

fn build_resolution_selected_entrypoint(trace: &ExecutionTrace) -> JsonValue {
    let source_family = provenance_field(&trace.manifest_context, "manifest_versions")
        .and_then(JsonValue::as_array)
        .and_then(|items| {
            items
                .iter()
                .find_map(|item| string_field(provenance_field(item, "source_family")))
        });
    let role =
        string_field(provenance_field(&trace.request_metadata, "entrypoint")).or_else(|| {
            trace
                .steps
                .iter()
                .find_map(|step| string_field(provenance_field(&step.step_payload, "entrypoint")))
        });
    let contract_call = trace
        .contracts_called
        .as_array()
        .and_then(|items| items.iter().find(|item| item.is_object()));

    let chain_id = string_field(contract_call.and_then(|item| provenance_field(item, "chain_id")));
    let contract_address = string_field(provenance_field(
        &trace.request_metadata,
        "contract_address",
    ))
    .or_else(|| {
        trace
            .steps
            .iter()
            .find_map(|step| string_field(provenance_field(&step.step_payload, "resolver")))
    })
    .or_else(|| {
        string_field(contract_call.and_then(|item| provenance_field(item, "contract_address")))
    });

    let mut selected_entrypoint = empty_object();
    insert_nullable_string_field(&mut selected_entrypoint, "source_family", source_family);
    insert_nullable_string_field(&mut selected_entrypoint, "role", role);
    insert_nullable_string_field(&mut selected_entrypoint, "chain_id", chain_id);
    insert_nullable_string_field(
        &mut selected_entrypoint,
        "contract_address",
        contract_address,
    );
    selected_entrypoint
}

fn build_resolution_execution_resolver_discovery_path(
    row: &NameCurrentRow,
    trace: &ExecutionTrace,
) -> JsonValue {
    if let Some(resolver_path) = projected_resolution_resolver_path(&row.declared_summary) {
        return resolver_path;
    }

    let declared_resolver = provenance_field(&row.declared_summary, "resolver");
    let chain_id = trace
        .contracts_called
        .as_array()
        .and_then(|items| items.iter().find(|item| item.is_object()))
        .and_then(|item| string_field(provenance_field(item, "chain_id")))
        .or_else(|| {
            string_field(declared_resolver.and_then(|value| provenance_field(value, "chain_id")))
        });
    let address = trace
        .steps
        .iter()
        .find_map(|step| string_field(provenance_field(&step.step_payload, "resolver")))
        .or_else(|| {
            string_field(declared_resolver.and_then(|value| provenance_field(value, "address")))
        });
    let latest_event_kind = string_field(
        declared_resolver.and_then(|value| provenance_field(value, "latest_event_kind")),
    );

    JsonValue::Array(vec![build_resolution_resolver_hop(
        row,
        chain_id,
        address,
        latest_event_kind,
    )])
}

fn build_resolution_execution_wildcard(trace: &ExecutionTrace) -> JsonValue {
    persisted_trace_detail_object(trace, "wildcard").unwrap_or_else(|| {
        json!({
            "source": null,
            "matched_labels": [],
        })
    })
}

fn build_resolution_execution_alias(trace: &ExecutionTrace) -> JsonValue {
    persisted_trace_detail_object(trace, "alias").unwrap_or_else(|| {
        json!({
            "final_target": null,
            "hops": [],
        })
    })
}

fn build_execution_step_summary(step: &bigname_storage::ExecutionTraceStep) -> JsonValue {
    let mut summary = empty_object();
    insert_value_field(
        &mut summary,
        "step_index",
        JsonValue::Number(step.step_index.into()),
    );
    insert_string_field(&mut summary, "step_kind", step.step_kind.clone());
    insert_nullable_string_field(&mut summary, "input_digest", step.input_digest.clone());
    insert_nullable_string_field(&mut summary, "output_digest", step.output_digest.clone());
    insert_value_field(
        &mut summary,
        "latency",
        step.latency_ms
            .map(|value| JsonValue::Number(value.into()))
            .unwrap_or(JsonValue::Null),
    );
    insert_value_field(
        &mut summary,
        "canonicality_dependency",
        ensure_object(&step.canonicality_dependency),
    );
    summary
}

fn reordered_persisted_verified_queries(
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

fn persisted_verified_queries_by_record_key(
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

fn build_resolution_topology(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> JsonValue {
    if let Some(projected_topology) = projected_resolution_topology(&row.declared_summary) {
        return projected_topology;
    }

    build_legacy_resolution_topology(row, record_inventory_row)
}

fn build_legacy_resolution_topology(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> JsonValue {
    if !matches!(row.namespace.as_str(), "ens" | BASENAMES_NAMESPACE)
        || row.binding_kind != Some(SurfaceBindingKind::DeclaredRegistryPath)
        || row.resource_id.is_none()
    {
        return unsupported_section("declared resolution topology is not yet projected");
    }

    let Some(resolver_summary) = provenance_field(&row.declared_summary, "resolver")
        .filter(|value| value.is_object())
        .filter(|value| !summary_is_unsupported(Some(value)))
    else {
        return unsupported_section("declared resolution topology is not yet projected");
    };

    let resolver_chain_id = string_field(provenance_field(resolver_summary, "chain_id"));
    let resolver_address = string_field(provenance_field(resolver_summary, "address"));
    if resolver_chain_id.is_some() != resolver_address.is_some() {
        return unsupported_section("declared resolution topology is not yet projected");
    }

    let Some(boundary) = resolution_record_version_boundary(row, record_inventory_row) else {
        return unsupported_section("declared resolution topology is not yet projected");
    };

    let registry_ref = build_resolution_name_ref(row);
    let resolver_hop = build_resolution_resolver_hop(
        row,
        resolver_chain_id,
        resolver_address,
        string_field(provenance_field(resolver_summary, "latest_event_kind")),
    );

    let mut wildcard = empty_object();
    insert_value_field(&mut wildcard, "source", JsonValue::Null);
    insert_value_field(
        &mut wildcard,
        "matched_labels",
        JsonValue::Array(Vec::new()),
    );

    let mut alias = empty_object();
    insert_value_field(&mut alias, "final_target", JsonValue::Null);
    insert_value_field(&mut alias, "hops", JsonValue::Array(Vec::new()));

    let mut version_boundaries = empty_object();
    insert_value_field(
        &mut version_boundaries,
        "topology_version_boundary",
        boundary.clone(),
    );
    insert_value_field(&mut version_boundaries, "record_version_boundary", boundary);

    let mut topology = empty_object();
    insert_value_field(
        &mut topology,
        "registry_path",
        JsonValue::Array(vec![registry_ref]),
    );
    insert_value_field(
        &mut topology,
        "subregistry_path",
        JsonValue::Array(Vec::new()),
    );
    insert_value_field(
        &mut topology,
        "resolver_path",
        JsonValue::Array(vec![resolver_hop]),
    );
    insert_value_field(&mut topology, "wildcard", wildcard);
    insert_value_field(&mut topology, "alias", alias);
    insert_value_field(&mut topology, "version_boundaries", version_boundaries);
    insert_value_field(&mut topology, "transport", build_resolution_transport(row));
    topology
}

fn build_resolution_name_ref(row: &NameCurrentRow) -> JsonValue {
    let mut name_ref = empty_object();
    insert_string_field(
        &mut name_ref,
        "logical_name_id",
        row.logical_name_id.clone(),
    );
    insert_string_field(&mut name_ref, "namespace", row.namespace.clone());
    insert_string_field(
        &mut name_ref,
        "normalized_name",
        row.normalized_name.clone(),
    );
    insert_string_field(
        &mut name_ref,
        "canonical_display_name",
        row.canonical_display_name.clone(),
    );
    insert_string_field(&mut name_ref, "namehash", row.namehash.clone());
    insert_optional_string_field(
        &mut name_ref,
        "resource_id",
        row.resource_id.map(|value| value.to_string()),
    );
    insert_optional_string_field(
        &mut name_ref,
        "binding_kind",
        row.binding_kind.map(|value| value.as_str().to_owned()),
    );
    name_ref
}

fn build_resolution_resolver_hop(
    row: &NameCurrentRow,
    chain_id: Option<String>,
    address: Option<String>,
    latest_event_kind: Option<String>,
) -> JsonValue {
    let mut hop = empty_object();
    insert_string_field(&mut hop, "logical_name_id", row.logical_name_id.clone());
    insert_string_field(&mut hop, "namespace", row.namespace.clone());
    insert_string_field(&mut hop, "normalized_name", row.normalized_name.clone());
    insert_string_field(
        &mut hop,
        "canonical_display_name",
        row.canonical_display_name.clone(),
    );
    insert_optional_string_field(
        &mut hop,
        "resource_id",
        row.resource_id.map(|value| value.to_string()),
    );
    insert_nullable_string_field(&mut hop, "chain_id", chain_id);
    insert_nullable_string_field(&mut hop, "address", address);
    insert_nullable_string_field(&mut hop, "latest_event_kind", latest_event_kind);
    hop
}

fn resolution_record_version_boundary(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Option<JsonValue> {
    bigname_storage::resolution_record_version_boundary(row, record_inventory_row)
}

fn build_resolution_execution_cache_key(
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

fn resolution_execution_cache_lookup_records(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
) -> Vec<ResolutionRecordKey> {
    bigname_storage::resolution_execution_cache_lookup_records(row, records)
}

fn persisted_trace_detail_object(trace: &ExecutionTrace, key: &str) -> Option<JsonValue> {
    provenance_field(&trace.request_metadata, key)
        .filter(|value| value.is_object())
        .cloned()
        .or_else(|| {
            trace
                .steps
                .iter()
                .find_map(|step| {
                    provenance_field(&step.step_payload, key).filter(|value| value.is_object())
                })
                .cloned()
        })
}

async fn load_supported_record_inventory_current(
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

async fn load_supported_record_inventory_current_for_snapshot(
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

fn record_inventory_lookup_key(row: &NameCurrentRow) -> Option<(Uuid, JsonValue)> {
    bigname_storage::resolution_record_inventory_lookup_key(row)
}

fn projected_resolution_topology(summary: &JsonValue) -> Option<JsonValue> {
    bigname_storage::projected_resolution_topology(summary)
}

fn projected_resolution_resolver_path(summary: &JsonValue) -> Option<JsonValue> {
    projected_resolution_topology(summary).and_then(|topology| {
        provenance_field(&topology, "resolver_path")
            .filter(|value| value.is_array())
            .cloned()
    })
}

fn resolution_verified_support_boundary(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
) -> Option<bigname_storage::VerifiedResolutionSupportBoundary> {
    bigname_storage::resolution_verified_support_boundary(row, record_inventory_row)
}

fn classify_supported_resolution_topology(
    namespace: &str,
    logical_name_id: &str,
    topology: &JsonValue,
) -> Option<bigname_storage::VerifiedResolutionPathClass> {
    bigname_storage::classify_supported_resolution_topology(namespace, logical_name_id, topology)
}

fn build_resolution_transport(row: &NameCurrentRow) -> JsonValue {
    if row.namespace == BASENAMES_NAMESPACE {
        return json!({
            "source_chain_id": BASENAMES_COMPAT_SOURCE_CHAIN_ID,
            "target_chain_id": BASENAMES_COMPAT_TARGET_CHAIN_ID,
            "contract_address": BASENAMES_COMPAT_CONTRACT_ADDRESS,
            "latest_event_kind": JsonValue::Null,
        });
    }

    let mut transport = empty_object();
    insert_value_field(&mut transport, "source_chain_id", JsonValue::Null);
    insert_value_field(&mut transport, "target_chain_id", JsonValue::Null);
    insert_value_field(&mut transport, "contract_address", JsonValue::Null);
    insert_value_field(&mut transport, "latest_event_kind", JsonValue::Null);
    transport
}

fn record_version_boundary_has_pointer(record_version_boundary: &JsonValue) -> bool {
    provenance_field(record_version_boundary, "normalized_event_id")
        .is_some_and(|value| !value.is_null())
        && provenance_field(record_version_boundary, "event_kind")
            .is_some_and(|value| !value.is_null())
}

async fn find_supported_record_inventory_boundary(
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
