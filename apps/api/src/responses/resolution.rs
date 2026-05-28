fn build_name_response(
    row: NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    selected_snapshot: &SelectedSnapshot,
) -> NameResponse {
    let declared_state = build_name_declared_state(&row, record_inventory_row);

    build_name_declared_response(row, declared_state, selected_snapshot, false)
}

fn build_name_coverage_response(
    row: NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> NameResponse {
    let declared_state = build_name_coverage_declared_state(&row.coverage);

    build_name_declared_response(row, declared_state, selected_snapshot, true)
}

fn build_name_surface_binding_explain_response(
    row: NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> NameResponse {
    let declared_state = build_name_surface_binding_explain_declared_state(&row);

    build_name_declared_response(row, declared_state, selected_snapshot, true)
}

fn build_name_authority_control_explain_response(
    row: NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> NameResponse {
    let declared_state = build_name_authority_control_explain_declared_state(&row);

    build_name_declared_response(row, declared_state, selected_snapshot, true)
}

fn build_name_declared_response(
    row: NameCurrentRow,
    declared_state: JsonValue,
    selected_snapshot: &SelectedSnapshot,
    include_provenance: bool,
) -> NameResponse {
    NameResponse {
        data: build_name_data(&row),
        declared_state,
        verified_state: None,
        provenance: if include_provenance {
            build_name_provenance(&row.provenance)
        } else {
            JsonValue::Null
        },
        coverage: build_name_coverage(&row.coverage),
        chain_positions: selected_snapshot.chain_positions_value(),
        consistency: selected_snapshot.consistency.as_str().to_owned(),
        last_updated: format_timestamp(row.last_recomputed_at),
    }
}

fn build_resolution_response(
    row: NameCurrentRow,
    mode: ResolutionMode,
    records: &[ResolutionRecordKey],
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    persisted_verified_outcome: Option<&ExecutionOutcome>,
    selected_snapshot: &SelectedSnapshot,
    include_full_metadata: bool,
) -> Result<ResolutionResponse> {
    let data = if include_full_metadata {
        build_name_data(&row)
    } else {
        build_profile_name_data(&row)
    };
    let declared_state = mode.includes_declared().then(|| {
        if include_full_metadata {
            build_resolution_declared_state(&row, record_inventory_row, records)
        } else {
            build_compact_resolution_declared_state(&row, record_inventory_row, records)
        }
    });
    let verified_state = mode
        .includes_verified()
        .then(|| {
            if include_full_metadata {
                build_resolution_verified_state(&row, records, persisted_verified_outcome)
            } else {
                build_compact_resolution_verified_state(&row, records, persisted_verified_outcome)
            }
        })
        .transpose()?;
    let provenance = if include_full_metadata {
        build_name_provenance_with_execution_trace(
            &row.provenance,
            persisted_verified_outcome.map(|outcome| outcome.execution_trace_id),
        )
    } else {
        JsonValue::Null
    };
    let coverage = if include_full_metadata {
        build_name_coverage(&row.coverage)
    } else {
        JsonValue::Null
    };
    let chain_positions = if include_full_metadata {
        selected_snapshot.chain_positions_value()
    } else {
        JsonValue::Null
    };
    let consistency = include_full_metadata
        .then(|| selected_snapshot.consistency.as_str().to_owned())
        .unwrap_or_default();
    let last_updated = include_full_metadata
        .then(|| format_timestamp(row.last_recomputed_at))
        .unwrap_or_default();

    Ok(ResolutionResponse {
        data,
        declared_state,
        verified_state,
        provenance,
        coverage,
        chain_positions,
        consistency,
        last_updated,
    })
}

fn build_profile_name_data(row: &NameCurrentRow) -> JsonValue {
    let mut data = empty_object();
    insert_string_field(&mut data, "name", row.normalized_name.clone());
    insert_string_field(&mut data, "namespace", row.namespace.clone());
    insert_string_field(&mut data, "namehash", row.namehash.clone());
    insert_optional_string_field(
        &mut data,
        "resource_id",
        row.resource_id.map(|value| value.to_string()),
    );
    data
}

fn build_compact_resolution_declared_state(
    row: &NameCurrentRow,
    record_inventory_row: Option<&RecordInventoryCurrentRow>,
    records: &[ResolutionRecordKey],
) -> JsonValue {
    let full = build_resolution_declared_state(row, record_inventory_row, records);
    let mut declared_state = empty_object();
    if let Some(topology) = provenance_field(&full, "topology") {
        insert_value_field(&mut declared_state, "topology", compact_resolution_topology(topology));
    }
    if let Some(inventory) = provenance_field(&full, "record_inventory") {
        insert_value_field(
            &mut declared_state,
            "record_inventory",
            compact_resolution_record_inventory(inventory),
        );
    }
    if let Some(cache) = provenance_field(&full, "record_cache") {
        insert_value_field(
            &mut declared_state,
            "record_cache",
            compact_resolution_record_cache(cache),
        );
    }
    declared_state
}

fn compact_resolution_topology(topology: &JsonValue) -> JsonValue {
    let mut compact = topology.clone();
    if let Some(object) = compact.as_object_mut() {
        object.remove("version_boundaries");
    }
    compact
}

fn compact_resolution_record_inventory(inventory: &JsonValue) -> JsonValue {
    let mut compact = empty_object();
    if summary_is_unsupported(Some(inventory)) {
        if let Some(status) = provenance_field(inventory, "status").cloned() {
            insert_value_field(&mut compact, "status", status);
        }
        if let Some(unsupported_reason) = provenance_field(inventory, "unsupported_reason").cloned()
        {
            insert_value_field(&mut compact, "unsupported_reason", unsupported_reason);
        }
        return compact;
    }
    if let Some(selectors) = provenance_field(inventory, "selectors").cloned() {
        insert_value_field(&mut compact, "selectors", selectors);
    }
    if let Some(explicit_gaps) = provenance_field(inventory, "explicit_gaps").cloned() {
        insert_value_field(&mut compact, "explicit_gaps", explicit_gaps);
    }
    if let Some(unsupported_families) = provenance_field(inventory, "unsupported_families").cloned()
        && unsupported_families.as_array().is_some_and(|items| !items.is_empty())
    {
        insert_value_field(&mut compact, "unsupported_families", unsupported_families);
    }
    compact
}

fn compact_resolution_record_cache(cache: &JsonValue) -> JsonValue {
    let mut compact = empty_object();
    if summary_is_unsupported(Some(cache)) {
        if let Some(status) = provenance_field(cache, "status").cloned() {
            insert_value_field(&mut compact, "status", status);
        }
        if let Some(unsupported_reason) = provenance_field(cache, "unsupported_reason").cloned() {
            insert_value_field(&mut compact, "unsupported_reason", unsupported_reason);
        }
        return compact;
    }
    insert_value_field(
        &mut compact,
        "entries",
        provenance_field(cache, "entries")
            .cloned()
            .unwrap_or_else(|| JsonValue::Array(Vec::new())),
    );
    compact
}

fn build_compact_resolution_verified_state(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    persisted_outcome: Option<&ExecutionOutcome>,
) -> Result<JsonValue> {
    let mut verified_state = build_resolution_verified_state(row, records, persisted_outcome)?;
    if let Some(queries) = verified_state
        .as_object_mut()
        .and_then(|object| object.get_mut("verified_queries"))
        .and_then(JsonValue::as_array_mut)
    {
        for query in queries {
            if let Some(object) = query.as_object_mut() {
                object.remove("provenance");
            }
        }
    }
    Ok(verified_state)
}

fn build_resolution_execution_explain_response(
    row: NameCurrentRow,
    records: &[ResolutionRecordKey],
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
) -> Result<ResolutionResponse> {
    let data = build_name_data(&row);
    let verified_state =
        build_resolution_execution_explain_verified_state(&row, records, trace, outcome)?;
    let provenance =
        build_name_provenance_with_execution_trace(&row.provenance, Some(trace.execution_trace_id));
    let coverage = build_name_coverage(&row.coverage);
    let chain_positions = ensure_object(&row.chain_positions);
    let consistency = canonicality_consistency(&row.canonicality_summary).to_owned();
    let last_updated = format_timestamp(row.last_recomputed_at);

    Ok(ResolutionResponse {
        data,
        declared_state: None,
        verified_state: Some(verified_state),
        provenance,
        coverage,
        chain_positions,
        consistency,
        last_updated,
    })
}

fn build_primary_name_response(
    address: String,
    namespace: String,
    coin_type: String,
    mode: ResolutionMode,
    lookup_state: &PrimaryNameLookupState,
) -> PrimaryNameResponse {
    let data = json!({
        "address": address,
        "namespace": namespace,
        "coin_type": coin_type,
    });
    let declared_state = mode
        .includes_declared()
        .then(|| json!({ "claimed_primary_name": primary_name_claim_result(lookup_state) }));
    let verified_state = mode
        .includes_verified()
        .then(|| json!({ "verified_primary_name": primary_name_verified_result(lookup_state) }));

    PrimaryNameResponse {
        data,
        declared_state,
        verified_state,
        provenance: JsonValue::Null,
        coverage: primary_name_route_coverage(&namespace, lookup_state),
        chain_positions: empty_object(),
        consistency: "head".to_owned(),
        last_updated: primary_name_last_updated(lookup_state.persisted_verified.as_ref()),
    }
}

fn primary_name_claim_result(lookup_state: &PrimaryNameLookupState) -> JsonValue {
    match &lookup_state.tuple_state {
        PrimaryNameTupleState::ProjectionUnavailable => primary_name_unsupported_result(
            "declared primary-name claim surface is not yet supported",
        ),
        PrimaryNameTupleState::TupleMissing => {
            if let OnDemandPrimaryNameClaimState::Found(on_demand_claim) =
                &lookup_state.on_demand_claim
            {
                return json!({
                    "status": "success",
                    "name": on_demand_claim.normalized_name.clone(),
                    "provenance": {
                        "source_family": "ens_reverse_rpc",
                        "resolver_address": on_demand_claim.resolver_address.clone(),
                    },
                });
            }
            primary_name_not_found_result()
        }
        PrimaryNameTupleState::TuplePresent(row) => {
            let mut result = json!({
                "status": row.claim_status.as_str(),
                "provenance": primary_name_claim_provenance(row),
            });
            if row.claim_status == PrimaryNameClaimStatus::Success
                && let Some(normalized_claim_name) = lookup_state.normalized_claim_name.as_deref()
            {
                insert_string_field(&mut result, "name", normalized_claim_name.to_owned());
            }
            if row.claim_status == PrimaryNameClaimStatus::InvalidName {
                insert_string_field(
                    &mut result,
                    "raw_claim_name",
                    row.raw_claim_name
                        .clone()
                        .expect("invalid_name primary-name rows must include raw_claim_name"),
                );
            }
            result
        }
    }
}

fn primary_name_claim_provenance(row: &PrimaryNameCurrentRow) -> JsonValue {
    let mut provenance = row
        .claim_provenance
        .as_object()
        .cloned()
        .unwrap_or_default();
    provenance.remove(VERIFIED_PRIMARY_NAME_LOOKUP_KEY);
    provenance.remove(VERIFIED_PRIMARY_NAME_INVALIDATION_KEY);
    provenance.remove("execution_trace_id");
    JsonValue::Object(provenance)
}

fn primary_name_verified_result(lookup_state: &PrimaryNameLookupState) -> JsonValue {
    if let Some(persisted_verified) = lookup_state.persisted_verified.as_ref() {
        let mut verified_primary_name = persisted_verified.verified_primary_name.clone();
        insert_value_field(
            &mut verified_primary_name,
            "provenance",
            ensure_object(&persisted_verified.provenance),
        );
        return verified_primary_name;
    }

    match lookup_state.tuple_state {
        PrimaryNameTupleState::TupleMissing => {
            if let OnDemandPrimaryNameVerificationState::Verified(on_demand_verified) =
                &lookup_state.on_demand_verified
            {
                return on_demand_verified.clone();
            }
            primary_name_not_found_result()
        }
        PrimaryNameTupleState::ProjectionUnavailable | PrimaryNameTupleState::TuplePresent(_) => {
            primary_name_unsupported_result("verified primary-name entrypoint is not yet supported")
        }
    }
}

fn primary_name_not_found_result() -> JsonValue {
    json!({ "status": "not_found" })
}

fn primary_name_unsupported_result(reason: &str) -> JsonValue {
    json!({
        "status": "unsupported",
        "unsupported_reason": reason,
    })
}

fn primary_name_verified_readback_provenance(
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> ApiResult<JsonValue> {
    if let Some(trace_manifest_versions) = trace.manifest_context.get("manifest_versions") {
        let cache_manifest_versions =
            outcome
                .cache_key
                .manifest_versions
                .as_array()
                .ok_or_else(|| {
                    error!(
                        service = "api",
                        address = %address,
                        namespace = %namespace,
                        coin_type = %coin_type,
                        execution_trace_id = %trace.execution_trace_id,
                        manifest_versions = ?outcome.cache_key.manifest_versions,
                        "persisted verified primary-name outcome manifest_versions malformed"
                    );
                    ApiError::internal_error(format!(
                        "persisted verified primary-name provenance mismatch for address {address}"
                    ))
                })?;
        let trace_manifest_versions = trace_manifest_versions.as_array().ok_or_else(|| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                execution_trace_id = %trace.execution_trace_id,
                manifest_context = ?trace.manifest_context,
                "persisted verified primary-name trace manifest_versions malformed"
            );
            ApiError::internal_error(format!(
                "persisted verified primary-name provenance mismatch for address {address}"
            ))
        })?;
        if trace_manifest_versions != cache_manifest_versions {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                execution_trace_id = %trace.execution_trace_id,
                trace_manifest_versions = ?trace_manifest_versions,
                outcome_manifest_versions = ?cache_manifest_versions,
                "persisted verified primary-name manifest_versions mismatch"
            );
            return Err(ApiError::internal_error(format!(
                "persisted verified primary-name provenance mismatch for address {address}"
            )));
        }
    }

    let mut provenance = empty_object();
    insert_value_field(
        &mut provenance,
        "manifest_versions",
        array_or_empty(Some(&outcome.cache_key.manifest_versions)),
    );
    insert_string_field(
        &mut provenance,
        "execution_trace_id",
        trace.execution_trace_id.to_string(),
    );
    Ok(provenance)
}

fn primary_name_route_coverage(
    namespace: &str,
    lookup_state: &PrimaryNameLookupState,
) -> JsonValue {
    if matches!(
        lookup_state.tuple_state,
        PrimaryNameTupleState::TuplePresent(_)
    ) && lookup_state.persisted_verified.is_some()
    {
        match namespace {
            "ens" => {
                return primary_name_exact_tuple_coverage(&["ens_v1_reverse_l1", "ens_execution"]);
            }
            "basenames" => {
                return primary_name_exact_tuple_coverage(&[
                    "basenames_base_primary",
                    "basenames_execution",
                ]);
            }
            _ => {}
        }
    }

    if matches!(
        lookup_state.on_demand_verified,
        OnDemandPrimaryNameVerificationState::Verified(_)
    ) && namespace == "ens"
    {
        return primary_name_exact_tuple_coverage(&["ens_reverse_rpc", "ens_execution_rpc"]);
    }

    if matches!(
        lookup_state.on_demand_claim,
        OnDemandPrimaryNameClaimState::Found(_) | OnDemandPrimaryNameClaimState::NotFound
    ) && namespace == "ens"
    {
        return primary_name_exact_tuple_coverage(&["ens_reverse_rpc"]);
    }

    primary_name_unsupported_exact_tuple_coverage()
}

fn primary_name_exact_tuple_coverage(source_classes: &[&str]) -> JsonValue {
    json!({
        "status": "partial",
        "exhaustiveness": "non_enumerable",
        "source_classes_considered": source_classes,
        "enumeration_basis": "primary_name_lookup",
        "unsupported_reason": null,
    })
}

fn primary_name_unsupported_exact_tuple_coverage() -> JsonValue {
    json!({
        "status": "unsupported",
        "exhaustiveness": "not_applicable",
        "source_classes_considered": [],
        "enumeration_basis": "primary_name_lookup",
        "unsupported_reason": "primary-name exact-tuple persisted readback is not supported for the requested tuple",
    })
}

fn primary_name_last_updated(
    persisted_verified: Option<&PersistedPrimaryNameVerifiedReadback>,
) -> String {
    persisted_verified
        .map(|persisted_verified| format_timestamp(persisted_verified.finished_at))
        .unwrap_or_else(|| format_timestamp(OffsetDateTime::now_utc()))
}
