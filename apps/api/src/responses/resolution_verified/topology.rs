pub(super) fn build_resolution_topology(
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

pub(super) fn build_resolution_resolver_hop(
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
fn projected_resolution_topology(summary: &JsonValue) -> Option<JsonValue> {
    bigname_storage::projected_resolution_topology(summary)
}

pub(super) fn projected_resolution_resolver_path(summary: &JsonValue) -> Option<JsonValue> {
    projected_resolution_topology(summary).and_then(|topology| {
        provenance_field(&topology, "resolver_path")
            .filter(|value| value.is_array())
            .cloned()
    })
}
pub(super) fn classify_supported_resolution_topology(
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
