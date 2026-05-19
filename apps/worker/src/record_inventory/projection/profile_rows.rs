use super::*;
use crate::projection_json::projection_coverage;

pub(super) async fn build_profile_gated_row(
    pool: &PgPool,
    resource_id: Uuid,
    resolver_event: &RelevantEvent,
    boundary_anchor: &RelevantEvent,
    latest_resolver_record_statuses: Option<&ResolverRecordFamilyStatuses>,
) -> Result<Option<RecordInventoryCurrentRow>> {
    let provenance_events = pending_profile_events(resolver_event, boundary_anchor);
    let supplemental_chain_positions =
        load_basenames_transport_chain_positions(pool, &provenance_events).await?;
    let has_record_version_boundary_pointer =
        boundary_anchor.event_kind == EVENT_KIND_RECORD_VERSION_CHANGED;

    Ok(Some(RecordInventoryCurrentRow {
        resource_id,
        record_version_boundary: build_record_version_boundary(
            boundary_anchor,
            has_record_version_boundary_pointer,
        )?,
        enumeration_basis: json!({
            "observed_selectors": false,
            "capability_declared_families": true,
            "globally_enumerable": false,
        }),
        selectors: Value::Array(vec![]),
        explicit_gaps: Value::Array(vec![gap_value(
            UNSUPPORTED_CONTENTHASH_RECORD_KEY,
            UNSUPPORTED_CONTENTHASH_RECORD_FAMILY,
            None,
        )]),
        unsupported_families: Value::Array(vec![
            resolver_family_value_for_status(
                SUPPORTED_ADDR_RECORD_FAMILY,
                latest_resolver_record_statuses.map(|statuses| statuses.addr.as_str()),
            ),
            resolver_family_value_for_status(
                SUPPORTED_TEXT_RECORD_FAMILY,
                latest_resolver_record_statuses.map(|statuses| statuses.text.as_str()),
            ),
        ]),
        last_change: Some(build_last_change(boundary_anchor)?),
        entries: Value::Array(vec![]),
        provenance: build_provenance(&provenance_events)?,
        coverage: profile_gated_coverage(&provenance_events, latest_resolver_record_statuses),
        chain_positions: build_chain_positions(&provenance_events, supplemental_chain_positions),
        canonicality_summary: build_canonicality_summary(&provenance_events),
        manifest_version: provenance_events
            .iter()
            .map(|event| event.manifest_version)
            .max()
            .unwrap_or(1),
        last_recomputed_at: provenance_events
            .iter()
            .filter_map(|event| event.block_timestamp)
            .max()
            .unwrap_or(OffsetDateTime::UNIX_EPOCH),
    }))
}

fn pending_profile_events(
    resolver_event: &RelevantEvent,
    boundary_anchor: &RelevantEvent,
) -> Vec<RelevantEvent> {
    let mut events = vec![resolver_event.clone()];
    if boundary_anchor.normalized_event_id != resolver_event.normalized_event_id {
        events.push(boundary_anchor.clone());
    }
    events
}

pub(super) fn build_row_unsupported_families(
    latest_resolver_record_statuses: Option<&ResolverRecordFamilyStatuses>,
    record_change_events: &[&RelevantEvent],
) -> Result<Vec<Value>> {
    let mut unsupported_families = build_unsupported_families(record_change_events)?;
    if let Some(statuses) = latest_resolver_record_statuses {
        for unsupported_family in &mut unsupported_families {
            let Some(record_family) = unsupported_family
                .get("record_family")
                .and_then(Value::as_str)
            else {
                continue;
            };
            let Some(status) = statuses.status_for_record_family(record_family) else {
                continue;
            };
            if status != RESOLVER_PROFILE_STATUS_SUPPORTED {
                unsupported_family["unsupported_reason"] =
                    json!(resolver_family_reason(Some(status)));
            }
        }
        for (record_family, status) in statuses.non_supported_families() {
            unsupported_families.push(resolver_family_value_for_status(
                record_family,
                Some(status),
            ));
        }
    }
    unsupported_families.sort_by(|left, right| {
        left["record_family"]
            .as_str()
            .cmp(&right["record_family"].as_str())
    });
    unsupported_families.dedup_by(|left, right| left["record_family"] == right["record_family"]);
    Ok(unsupported_families)
}

pub(super) fn build_row_coverage(
    latest_resolver_record_statuses: Option<&ResolverRecordFamilyStatuses>,
    boundary_anchor: &RelevantEvent,
    provenance_events: &[RelevantEvent],
) -> Value {
    if let Some(statuses) = latest_resolver_record_statuses
        && !statuses.all_supported()
    {
        return projection_coverage(
            "partial",
            "best_effort",
            provenance_events
                .iter()
                .map(|event| event.source_family.clone())
                .chain(std::iter::once(boundary_anchor.source_family.clone())),
            Some(resolver_family_coverage_reason(Some(statuses))),
            RECORD_INVENTORY_ENUMERATION_BASIS,
        );
    }

    build_coverage(provenance_events)
}

fn profile_gated_coverage(
    provenance_events: &[RelevantEvent],
    latest_resolver_record_statuses: Option<&ResolverRecordFamilyStatuses>,
) -> Value {
    projection_coverage(
        "partial",
        "best_effort",
        provenance_events
            .iter()
            .map(|event| event.source_family.clone()),
        Some(resolver_family_coverage_reason(
            latest_resolver_record_statuses,
        )),
        RECORD_INVENTORY_ENUMERATION_BASIS,
    )
}

fn resolver_family_value_for_status(record_family: &str, status: Option<&str>) -> Value {
    resolver_family_status_value(record_family, resolver_family_reason(status))
}

fn resolver_family_reason(status: Option<&str>) -> &'static str {
    match status {
        Some(RESOLVER_PROFILE_STATUS_UNSUPPORTED) => RESOLVER_FAMILY_UNSUPPORTED_REASON,
        _ => RESOLVER_FAMILY_PENDING_REASON,
    }
}

fn resolver_family_coverage_reason(
    statuses: Option<&ResolverRecordFamilyStatuses>,
) -> &'static str {
    let Some(statuses) = statuses else {
        return RESOLVER_FAMILY_PENDING_REASON;
    };
    let non_supported = statuses.non_supported_families();
    if non_supported
        .iter()
        .all(|(_, status)| *status == RESOLVER_PROFILE_STATUS_UNSUPPORTED)
    {
        RESOLVER_FAMILY_UNSUPPORTED_REASON
    } else {
        RESOLVER_FAMILY_PENDING_REASON
    }
}

pub(super) fn filter_explicit_gaps(
    explicit_gaps: Vec<Value>,
    latest_resolver_record_statuses: Option<&ResolverRecordFamilyStatuses>,
) -> Vec<Value> {
    let Some(statuses) = latest_resolver_record_statuses else {
        return explicit_gaps;
    };

    explicit_gaps
        .into_iter()
        .filter(|gap| {
            gap.get("record_family")
                .and_then(Value::as_str)
                .is_none_or(|record_family| record_family_supported(statuses, record_family))
        })
        .collect()
}

fn record_family_supported(statuses: &ResolverRecordFamilyStatuses, record_family: &str) -> bool {
    match record_family {
        SUPPORTED_ADDR_RECORD_FAMILY => statuses.addr == RESOLVER_PROFILE_STATUS_SUPPORTED,
        SUPPORTED_TEXT_RECORD_FAMILY => statuses.text == RESOLVER_PROFILE_STATUS_SUPPORTED,
        _ => true,
    }
}
