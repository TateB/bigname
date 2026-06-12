impl AddressNamesResponseSupplement {
    fn has_provenance_inputs(&self) -> bool {
        !self.provenances.is_empty()
    }

    fn push_name_current(&mut self, row: &NameCurrentRow) {
        self.provenances.push(row.provenance.clone());
        self.chain_positions.push(row.chain_positions.clone());
        self.canonicality_summaries
            .push(row.canonicality_summary.clone());
        self.last_recomputed_at.push(row.last_recomputed_at);
    }

    fn push_permissions(&mut self, rows: &[PermissionsCurrentRow]) {
        self.provenances
            .extend(rows.iter().map(|row| row.provenance.clone()));
        self.chain_positions
            .extend(rows.iter().map(|row| row.chain_positions.clone()));
        self.canonicality_summaries
            .extend(rows.iter().map(|row| row.canonicality_summary.clone()));
        self.last_recomputed_at
            .extend(rows.iter().map(|row| row.last_recomputed_at));
    }

    fn push_children_summary(&mut self, summary: &bigname_storage::ChildrenCurrentSummary) {
        self.provenances
            .extend(summary.provenance_inputs.iter().cloned());
        self.chain_positions
            .extend(summary.chain_positions.iter().cloned());
        self.canonicality_summaries
            .extend(summary.canonicality_summaries.iter().cloned());
        self.last_recomputed_at
            .extend(summary.last_recomputed_at.iter().copied());
    }
}

fn build_address_names_response_from_summary(
    summary: &bigname_storage::AddressNamesCurrentSummary,
    data: Vec<JsonValue>,
    supplement: AddressNamesResponseSupplement,
    page: HistoryPageResponse,
) -> AddressNamesResponse {
    let last_updated = summary
        .last_recomputed_at
        .into_iter()
        .chain(supplement.last_recomputed_at.iter().copied())
        .max()
        .map(format_timestamp)
        .unwrap_or_else(|| format_timestamp(OffsetDateTime::now_utc()));
    let provenance = if supplement.has_provenance_inputs() {
        build_address_names_expanded_provenance(summary, &supplement)
    } else {
        JsonValue::Null
    };

    AddressNamesResponse {
        data,
        declared_state: empty_object(),
        verified_state: None,
        provenance,
        coverage: CoverageResponse {
            status: "full".to_owned(),
            exhaustiveness: "authoritative".to_owned(),
            source_classes_considered: vec!["ensv1_registry_path".to_owned()],
            enumeration_basis: "surface_current_relations".to_owned(),
            unsupported_reason: None,
        },
        chain_positions: build_chain_positions_from_values(
            std::iter::once(&summary.chain_positions).chain(supplement.chain_positions.iter()),
        ),
        page,
        consistency: address_names_collection_consistency(
            &summary.consistency,
            supplement.canonicality_summaries.iter(),
        ),
        last_updated,
    }
}

fn build_history_response(
    summary: &HistorySummary,
    page_rows: &[HistoryEvent],
    scope: HistoryScope,
    page: HistoryPageResponse,
) -> HistoryResponse {
    HistoryResponse {
        data: page_rows.iter().map(build_history_item).collect(),
        declared_state: empty_object(),
        verified_state: None,
        provenance: build_history_provenance(summary),
        coverage: build_history_coverage(scope),
        chain_positions: build_history_chain_positions(summary),
        page,
        consistency: "head".to_owned(),
        last_updated: history_last_updated(summary),
    }
}

fn build_resource_permissions_response_from_summary(
    summary: &bigname_storage::PermissionsCurrentFullFilterSummary,
    page_rows: &[PermissionsCurrentRow],
    page: HistoryPageResponse,
) -> ResourcePermissionsResponse {
    let last_updated = summary
        .last_recomputed_at
        .map(format_timestamp)
        .unwrap_or_else(|| format_timestamp(OffsetDateTime::now_utc()));

    ResourcePermissionsResponse {
        data: page_rows.iter().map(build_permission_item).collect(),
        declared_state: empty_object(),
        verified_state: None,
        provenance: build_collection_provenance_from_inputs(
            &summary.provenance,
            "permissions_current_rebuild",
        ),
        coverage: build_permissions_coverage_from_sample(summary.coverage.as_ref()),
        chain_positions: build_chain_positions_from_values(summary.chain_positions.iter()),
        page,
        consistency: collection_consistency(summary.canonicality_summaries.iter()).to_owned(),
        last_updated,
    }
}

fn build_children_response_from_summary(
    summary: &bigname_storage::ChildrenCurrentSummary,
    page_rows: &[ChildrenCurrentRow],
    include_counts: bool,
    page: HistoryPageResponse,
) -> ChildrenResponse {
    let last_updated = summary
        .last_recomputed_at
        .map(format_timestamp)
        .unwrap_or_else(|| format_timestamp(OffsetDateTime::now_utc()));

    ChildrenResponse {
        data: page_rows.iter().map(build_child_item).collect(),
        declared_state: build_children_declared_state_from_count(
            u64::try_from(summary.child_count).unwrap_or_default(),
            include_counts,
        ),
        verified_state: None,
        provenance: build_collection_provenance_from_inputs(&summary.provenance_inputs, "declared"),
        coverage: CoverageResponse {
            status: "full".to_owned(),
            exhaustiveness: "authoritative".to_owned(),
            source_classes_considered: vec!["declared".to_owned()],
            enumeration_basis: "declared_direct_children".to_owned(),
            unsupported_reason: None,
        },
        chain_positions: build_chain_positions_from_values(summary.chain_positions.iter()),
        page,
        consistency: collection_consistency(summary.canonicality_summaries.iter()).to_owned(),
        last_updated,
    }
}

include!("collections_children.rs");

fn build_child_item(row: &ChildrenCurrentRow) -> JsonValue {
    let mut value = empty_object();
    insert_string_field(
        &mut value,
        "logical_name_id",
        row.child_logical_name_id.clone(),
    );
    insert_string_field(&mut value, "namespace", row.namespace.clone());
    insert_string_field(&mut value, "normalized_name", row.normalized_name.clone());
    insert_string_field(
        &mut value,
        "canonical_display_name",
        row.canonical_display_name.clone(),
    );
    insert_string_field(&mut value, "namehash", row.namehash.clone());
    insert_string_field(&mut value, "surface_class", row.surface_class.clone());
    value
}

fn build_permission_item(row: &PermissionsCurrentRow) -> JsonValue {
    let mut value = empty_object();
    insert_string_field(&mut value, "resource_id", row.resource_id.to_string());
    insert_string_field(&mut value, "subject", row.subject.clone());
    insert_value_field(
        &mut value,
        "scope",
        build_permission_scope_value(&row.scope),
    );
    insert_value_field(&mut value, "effective_powers", row.effective_powers.clone());
    insert_value_field(&mut value, "grant_source", row.grant_source.clone());
    insert_value_field(
        &mut value,
        "revocation_source",
        row.revocation_source.clone().unwrap_or(JsonValue::Null),
    );
    insert_value_field(&mut value, "inheritance_path", row.inheritance_path.clone());
    insert_value_field(
        &mut value,
        "transfer_behavior",
        row.transfer_behavior.clone(),
    );
    value
}

fn build_address_name_item(entry: &AddressNameCurrentEntry) -> JsonValue {
    let mut value = empty_object();
    insert_string_field(&mut value, "logical_name_id", entry.logical_name_id.clone());
    insert_string_field(&mut value, "namespace", entry.namespace.clone());
    insert_string_field(&mut value, "normalized_name", entry.normalized_name.clone());
    insert_string_field(
        &mut value,
        "canonical_display_name",
        entry.canonical_display_name.clone(),
    );
    insert_string_field(&mut value, "namehash", entry.namehash.clone());
    insert_string_field(&mut value, "resource_id", entry.resource_id.to_string());
    insert_string_field(
        &mut value,
        "binding_kind",
        entry.binding_kind.as_str().to_owned(),
    );
    insert_value_field(
        &mut value,
        "relation_facets",
        JsonValue::Array(
            entry
                .relations
                .iter()
                .map(|relation| JsonValue::String(relation.as_str().to_owned()))
                .collect(),
        ),
    );
    value
}

fn build_address_name_item_with_role_summary(
    entry: &AddressNameCurrentEntry,
    name_row: Option<&NameCurrentRow>,
    permissions: &[PermissionsCurrentRow],
    child_count: u64,
) -> JsonValue {
    let mut value = build_address_name_item(entry);
    let facts = name_row
        .map(build_address_name_expansion_facts)
        .unwrap_or_default();

    insert_value_field(
        &mut value,
        "role_summary",
        build_address_name_role_summary(permissions),
    );
    insert_value_field(
        &mut value,
        "subname_count",
        JsonValue::Number(child_count.into()),
    );
    insert_value_field(&mut value, "record_count", facts.record_count);
    insert_value_field(&mut value, "status", facts.status);
    insert_value_field(&mut value, "expiry", facts.expiry);
    value
}

fn build_children_declared_state_from_count(child_count: u64, include_counts: bool) -> JsonValue {
    let mut declared_state = empty_object();
    if include_counts {
        insert_value_field(
            &mut declared_state,
            "subname_count",
            JsonValue::Number(child_count.into()),
        );
    }
    declared_state
}

fn build_address_names_expanded_provenance(
    summary: &bigname_storage::AddressNamesCurrentSummary,
    supplement: &AddressNamesResponseSupplement,
) -> JsonValue {
    let summary_provenance = address_names_summary_provenance_value(&summary.provenance);
    let mut provenances = Vec::with_capacity(supplement.provenances.len() + 1);
    provenances.push(&summary_provenance);
    provenances.extend(supplement.provenances.iter());
    build_collection_provenance_from_refs(&provenances, "address_names_current_rebuild")
}

fn address_names_summary_provenance_value(
    summary: &bigname_storage::AddressNamesCurrentProvenanceSummary,
) -> JsonValue {
    let mut value = empty_object();
    insert_value_field(
        &mut value,
        "normalized_event_ids",
        summary.normalized_event_ids.clone(),
    );
    insert_value_field(&mut value, "raw_fact_refs", summary.raw_fact_refs.clone());
    insert_value_field(
        &mut value,
        "manifest_versions",
        summary.manifest_versions.clone(),
    );
    if let Some(derivation_kind) = summary.derivation_kind.clone() {
        insert_string_field(&mut value, "derivation_kind", derivation_kind);
    }
    value
}

fn build_collection_provenance_from_inputs(
    provenances: &[JsonValue],
    default_derivation_kind: &str,
) -> JsonValue {
    let provenances = provenances.iter().collect::<Vec<_>>();
    build_collection_provenance_from_refs(&provenances, default_derivation_kind)
}

fn build_collection_provenance_from_refs(
    provenances: &[&JsonValue],
    default_derivation_kind: &str,
) -> JsonValue {
    let mut value = empty_object();
    insert_value_field(
        &mut value,
        "normalized_event_ids",
        JsonValue::Array(
            collect_collection_provenance_values(provenances, "normalized_event_ids")
                .into_iter()
                .filter_map(|value| value_to_string(&value).map(JsonValue::String))
                .collect(),
        ),
    );
    insert_value_field(
        &mut value,
        "raw_fact_refs",
        JsonValue::Array(collect_collection_provenance_values(
            provenances,
            "raw_fact_refs",
        )),
    );
    insert_value_field(
        &mut value,
        "manifest_versions",
        JsonValue::Array(collect_collection_provenance_values(
            provenances,
            "manifest_versions",
        )),
    );
    // Collection provenance is a route-level summary; if execution-backed rows
    // are later admitted, the first non-null trace id is the representative id.
    if let Some(execution_trace_id) = provenances
        .iter()
        .filter_map(|provenance| string_field(provenance_field(provenance, "execution_trace_id")))
        .next()
    {
        insert_string_field(&mut value, "execution_trace_id", execution_trace_id);
    }
    insert_string_field(
        &mut value,
        "derivation_kind",
        provenances
            .iter()
            .filter_map(|provenance| string_field(provenance_field(provenance, "derivation_kind")))
            .next()
            .unwrap_or_else(|| default_derivation_kind.to_owned()),
    );
    value
}

fn collect_collection_provenance_values(provenances: &[&JsonValue], key: &str) -> Vec<JsonValue> {
    let mut deduped = Vec::new();
    for provenance in provenances {
        let Some(JsonValue::Array(values)) = provenance_field(provenance, key) else {
            continue;
        };
        for value in values {
            if !deduped.contains(value) {
                deduped.push(value.clone());
            }
        }
    }
    deduped
}

fn build_chain_positions_from_values<'a>(values: impl Iterator<Item = &'a JsonValue>) -> JsonValue {
    let mut chain_positions = BTreeMap::<String, ChainPositionResponse>::new();
    for position_value in values {
        let Some(position_values) = position_value.as_object() else {
            continue;
        };

        for (slot, position_value) in position_values {
            let Some(candidate) = chain_position_from_value(position_value) else {
                continue;
            };
            merge_chain_position(&mut chain_positions, slot.clone(), candidate);
        }
    }

    serde_json::to_value(chain_positions).expect("address names chain positions must serialize")
}

fn build_permissions_coverage_from_sample(sample: Option<&JsonValue>) -> CoverageResponse {
    CoverageResponse {
        status: string_field(sample.and_then(|value| provenance_field(value, "status")))
            .unwrap_or_else(|| "full".to_owned()),
        exhaustiveness: string_field(
            sample.and_then(|value| provenance_field(value, "exhaustiveness")),
        )
        .unwrap_or_else(|| "authoritative".to_owned()),
        source_classes_considered: match sample
            .and_then(|value| provenance_field(value, "source_classes_considered"))
        {
            Some(JsonValue::Array(values)) => values.iter().filter_map(value_to_string).collect(),
            _ => vec!["permissions_current".to_owned()],
        },
        enumeration_basis: string_field(
            sample.and_then(|value| provenance_field(value, "enumeration_basis")),
        )
        .unwrap_or_else(|| "resource_permissions".to_owned()),
        unsupported_reason: string_field(
            sample.and_then(|value| provenance_field(value, "unsupported_reason")),
        ),
    }
}

fn address_names_collection_consistency<'a>(
    base_consistency: &str,
    _row_expansion_summaries: impl Iterator<Item = &'a JsonValue>,
) -> String {
    base_consistency.to_owned()
}

fn build_permission_scope_value(scope: &PermissionScope) -> JsonValue {
    let mut value = empty_object();
    insert_string_field(&mut value, "kind", scope.kind().to_owned());
    insert_value_field(&mut value, "detail", scope.detail());
    value
}

fn build_address_name_role_summary(rows: &[PermissionsCurrentRow]) -> JsonValue {
    let mut subjects = BTreeMap::<String, Vec<&PermissionsCurrentRow>>::new();

    for row in rows {
        subjects.entry(row.subject.clone()).or_default().push(row);
    }

    json!({
        "subjects": subjects
            .into_iter()
            .map(|(subject, mut rows)| {
                rows.sort_by(|left, right| left.scope.storage_key().cmp(&right.scope.storage_key()));
                json!({
                    "subject": subject,
                    "scopes": rows
                        .into_iter()
                        .map(|row| {
                            json!({
                                "scope": build_permission_scope_value(&row.scope),
                                "effective_powers": row.effective_powers.clone(),
                            })
                        })
                        .collect::<Vec<_>>(),
                })
            })
            .collect::<Vec<_>>(),
    })
}
