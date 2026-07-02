use std::collections::{BTreeMap, BTreeSet};

use axum::Json;
use axum::extract::{Path, State};
use bigname_storage::{BASENAMES_NAMESPACE, NameCurrentRow, RecordInventoryCurrentRow};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    AppState, load_name_current_for_selected_snapshot, map_internal_api_error,
    normalize_inferred_route_name, snapshot_selection_api_error,
};

use super::{
    Envelope, Meta, QueryParamAllowlist, QueryParams, RequestSource, SnapshotReadResource,
    StrictQueryParams, V2Error, V2Result, api_error_to_v2, api_error_to_v2_for_resource,
    resolve_v2_snapshot_for, snapshot_meta, v2_exact_name_snapshot_scope_with_resolution_auxiliary,
    vocab::{RegistrationStatus, Resolver, Source, Status},
};

#[path = "name_record/inventory.rs"]
mod inventory;
#[path = "name_record/values.rs"]
mod values;
#[path = "name_record/verified.rs"]
mod verified;

use inventory::load_name_record_inventory;
use values::{
    json_address_at_paths, json_chain_id, json_string_at_paths, json_timestamp_at_paths,
    json_value_present, network, object_field, response_chain_id,
};
pub(super) use values::{string_field, value_to_string};

pub(crate) struct NameRecordQueryParams;

impl QueryParamAllowlist for NameRecordQueryParams {
    const ALLOWED: &'static [&'static str] = &["namespace", "at", "finality", "source"];
}

pub(crate) type NameRecordQuery = StrictQueryParams<NameRecordQueryParams>;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct NameRecord {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) registration_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) token_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) manager: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) registrant: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) registered_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) expires_at: Option<String>,
    pub(crate) registration_status: RegistrationStatus,
    pub(crate) name: String,
    pub(crate) display_name: String,
    pub(crate) namespace: String,
    pub(crate) namehash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) resolver: Option<Resolver>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) addresses: Option<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) text_records: Option<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) content_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) primary_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) primary_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) chain_id: Option<u64>,
    pub(crate) network: String,
    pub(crate) status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) unsupported_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) failure_reason: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) unsupported_fields: Vec<String>,
}

pub(crate) async fn get_name_record(
    Path(input_name): Path<String>,
    params: NameRecordQuery,
    State(state): State<AppState>,
) -> V2Result<Json<Envelope<NameRecord>>> {
    let params = params.into_inner();
    let normalized = normalize_inferred_route_name(&input_name)
        .map_err(|error| V2Error::invalid_input(error.message))?;
    let namespace = params
        .namespace
        .clone()
        .unwrap_or_else(|| normalized.namespace.to_owned());
    let route_source = route_source(params.source)?;

    let include_resolution_auxiliary =
        namespace == BASENAMES_NAMESPACE && route_source == Source::Verified;
    let scope = v2_exact_name_snapshot_scope_with_resolution_auxiliary(
        &state,
        &namespace,
        params.at.as_ref(),
        include_resolution_auxiliary,
    )
    .await?;
    let selected_snapshot = resolve_v2_snapshot_for(
        &state.pool,
        &scope,
        params.at.as_ref(),
        params.finality,
        SnapshotReadResource::Name,
    )
    .await?;
    let row = load_name_current_for_selected_snapshot(
        &state.pool,
        &namespace,
        &normalized.normalized_name,
        &selected_snapshot,
    )
    .await
    .map_err(|error| {
        api_error_to_v2_for_resource(
            map_internal_api_error(
                error,
                format!(
                    "failed to load name profile for {}/{}",
                    namespace, normalized.normalized_name
                ),
            ),
            SnapshotReadResource::Name,
        )
    })?;

    let record_inventory = load_name_record_inventory(
        &state.pool,
        &row,
        &selected_snapshot,
        include_resolution_auxiliary,
    )
    .await
    .map_err(|error| {
        api_error_to_v2_for_resource(
            snapshot_selection_api_error(error),
            SnapshotReadResource::Name,
        )
    })?;
    let chain_id = response_chain_id(&selected_snapshot);
    let record = verified::build_name_record_for_source(
        &state,
        &row,
        record_inventory.as_ref(),
        chain_id,
        &selected_snapshot,
        route_source,
    )
    .await?;
    let mut meta = if record.uses_on_demand_fallback {
        Meta::default()
    } else {
        snapshot_meta(&selected_snapshot)?
    };
    meta.source = Some(route_source);

    Ok(Json(Envelope {
        data: record.record,
        page: None,
        meta,
    }))
}

pub(crate) fn build_name_record(
    row: &NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
    chain_id: Option<u64>,
    status: Status,
) -> NameRecord {
    let registration = name_registration_fields(Some(row), &row.namespace);
    let unsupported_fields = unsupported_fields(record_inventory);
    let field_supported = |field: &str| {
        !unsupported_fields
            .iter()
            .any(|unsupported| unsupported == field)
    };
    let addresses = field_supported("addresses").then(|| record_addresses(record_inventory));
    let text_records =
        field_supported("text_records").then(|| record_text_records(record_inventory));
    let content_hash = field_supported("content_hash")
        .then(|| record_content_hash(record_inventory))
        .flatten();
    let primary_address = field_supported("primary_address")
        .then(|| {
            addresses
                .as_ref()
                .and_then(|addresses| addresses.get("60").cloned())
        })
        .flatten();

    NameRecord {
        registration_id: row.resource_id.map(|value| value.to_string()),
        token_id: declared_token_id(row),
        owner: registration.owner.clone(),
        manager: None,
        registrant: registration.registrant,
        registered_at: registration.registered_at,
        created_at: registration.created_at,
        expires_at: registration.expires_at,
        registration_status: registration.registration_status,
        name: row.normalized_name.clone(),
        display_name: row.canonical_display_name.clone(),
        namespace: row.namespace.clone(),
        namehash: row.namehash.clone(),
        resolver: resolver(&row.declared_summary),
        addresses,
        text_records,
        content_hash,
        primary_name: json_string_at_paths(
            &row.declared_summary,
            &[
                &["primary_name"],
                &["primary_name", "name"],
                &["primary", "name"],
            ],
        ),
        primary_address,
        chain_id,
        network: network(row),
        status,
        unsupported_reason: None,
        failure_reason: None,
        unsupported_fields,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct NameRegistrationFields {
    pub(super) owner: Option<String>,
    pub(super) registrant: Option<String>,
    pub(super) registered_at: Option<String>,
    pub(super) created_at: Option<String>,
    pub(super) expires_at: Option<String>,
    pub(super) registration_status: RegistrationStatus,
}

pub(super) fn name_registration_fields(
    row: Option<&NameCurrentRow>,
    namespace: &str,
) -> NameRegistrationFields {
    let Some(row) = row else {
        return empty_registration_fields(namespace);
    };

    registration_fields_from_parts(
        &row.namespace,
        &row.declared_summary,
        &row.chain_positions,
        has_name_binding(row),
    )
}

pub(super) fn identity_name_registration_fields(
    row: Option<&bigname_storage::IdentityNameCurrentRow>,
    namespace: &str,
) -> NameRegistrationFields {
    let Some(row) = row else {
        return empty_registration_fields(namespace);
    };

    registration_fields_from_parts(
        &row.namespace,
        &row.declared_summary,
        &row.chain_positions,
        row.resource_id.is_some(),
    )
}

fn empty_registration_fields(namespace: &str) -> NameRegistrationFields {
    NameRegistrationFields {
        owner: None,
        registrant: None,
        registered_at: None,
        created_at: None,
        expires_at: None,
        registration_status: classify_registration_status(namespace, None, None, false),
    }
}

fn registration_fields_from_parts(
    namespace: &str,
    declared_summary: &Value,
    chain_positions: &Value,
    has_binding: bool,
) -> NameRegistrationFields {
    let registration = declared_registration(declared_summary);
    let owner = declared_owner(declared_summary);

    NameRegistrationFields {
        registrant: declared_registrant(declared_summary),
        registered_at: declared_registered_at(declared_summary),
        created_at: declared_created_at(declared_summary)
            .or_else(|| chain_positions_created_at(chain_positions)),
        expires_at: declared_expires_at(declared_summary),
        registration_status: classify_registration_status(
            namespace,
            registration,
            owner.as_deref(),
            has_binding,
        ),
        owner,
    }
}

pub(super) fn declared_registration(summary: &Value) -> Option<&Value> {
    object_field(summary, "registration")
}

pub(super) fn declared_owner(summary: &Value) -> Option<String> {
    json_address_at_paths(
        summary,
        &[&["control", "owner"], &["control", "registry_owner"]],
    )
}

pub(super) fn declared_registrant(summary: &Value) -> Option<String> {
    json_address_at_paths(
        summary,
        &[&["registration", "registrant"], &["control", "registrant"]],
    )
}

pub(super) fn declared_registered_at(summary: &Value) -> Option<String> {
    json_timestamp_at_paths(
        summary,
        &[
            &["registration", "registered_at"],
            &["registration", "registration_date"],
        ],
    )
}

pub(super) fn declared_created_at(summary: &Value) -> Option<String> {
    json_timestamp_at_paths(
        summary,
        &[&["registration", "created_at"], &["history", "created_at"]],
    )
}

pub(super) fn declared_expires_at(summary: &Value) -> Option<String> {
    json_timestamp_at_paths(
        summary,
        &[
            &["registration", "expires_at"],
            &["registration", "expiry_date"],
            &["registration", "expiry"],
            &["control", "expires_at"],
            &["control", "expiry_date"],
            &["control", "expiry"],
        ],
    )
}

fn has_name_binding(row: &NameCurrentRow) -> bool {
    row.surface_binding_id.is_some() || row.resource_id.is_some() || row.binding_kind.is_some()
}

pub(super) fn declared_token_id(row: &NameCurrentRow) -> Option<String> {
    declared_token_id_from_parts(
        &row.declared_summary,
        &row.namespace,
        &row.normalized_name,
        None,
    )
}

pub(super) fn identity_declared_token_id(
    row: &bigname_storage::IdentityNameCurrentRow,
) -> Option<String> {
    let labelhash = row.labelhash.as_deref().filter(|value| {
        row.labelhash_count
            .is_none_or(|label_count| label_count == 2)
            && !value.trim().is_empty()
    });
    declared_token_id_from_parts(
        &row.declared_summary,
        &row.namespace,
        &row.normalized_name,
        labelhash,
    )
}

fn declared_token_id_from_parts(
    summary: &Value,
    namespace: &str,
    normalized_name: &str,
    labelhash: Option<&str>,
) -> Option<String> {
    json_string_at_paths(
        summary,
        &[
            &["authority", "token_id"],
            &["registration", "token_id"],
            &["registration", "upstream_resource"],
            &["control", "token_id"],
        ],
    )
    .or_else(|| eth_2ld_labelhash_token_id(namespace, normalized_name, labelhash))
}

fn eth_2ld_labelhash_token_id(
    namespace: &str,
    normalized_name: &str,
    labelhash: Option<&str>,
) -> Option<String> {
    if namespace != "ens" {
        return None;
    }
    let mut labels = normalized_name.split('.');
    let label = labels.next()?;
    if labels.next() != Some("eth") || labels.next().is_some() || label.trim().is_empty() {
        return None;
    }
    let labelhash = labelhash.map(str::to_owned).unwrap_or_else(|| {
        format!(
            "0x{}",
            alloy_primitives::hex::encode(alloy_primitives::keccak256(label.as_bytes()))
        )
    });
    let hex = labelhash.strip_prefix("0x").unwrap_or(&labelhash);
    alloy_primitives::U256::from_str_radix(hex, 16)
        .ok()
        .map(|value| value.to_string())
}

fn chain_positions_created_at(chain_positions: &Value) -> Option<String> {
    chain_positions
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(_, position)| json_timestamp_at_paths(position, &[&["timestamp"]]))
        .min()
}

pub(crate) fn classify_registration_status(
    namespace: &str,
    registration: Option<&Value>,
    owner: Option<&str>,
    has_binding: bool,
) -> RegistrationStatus {
    // Classification is state at the indexed head. A registrar name past grace
    // whose release block is not indexed yet still reads active with a past
    // expires_at until reprojection observes released_at/status=released.
    if !has_binding {
        return RegistrationStatus::Unregistered;
    }

    let status = registration.and_then(|value| string_field(value.get("status")));
    let authority_kind = registration.and_then(|value| string_field(value.get("authority_kind")));
    let released_at = registration.and_then(|value| value.get("released_at"));

    if released_at.is_some_and(json_value_present) || status.as_deref() == Some("released") {
        return RegistrationStatus::Released;
    }

    match authority_kind.as_deref() {
        Some("registrar") => RegistrationStatus::Active,
        Some("registry_only") if owner.is_some_and(|value| !value.trim().is_empty()) => {
            RegistrationStatus::Registered
        }
        Some("ens_v2_registry") if owner.is_some_and(|value| !value.trim().is_empty()) => {
            RegistrationStatus::Registered
        }
        Some("wrapper") if namespace != bigname_storage::BASENAMES_NAMESPACE => {
            RegistrationStatus::Wrapped
        }
        // At this point the name is bound; an unrecognized authority_kind or
        // missing required owner evidence cannot be classified as registered.
        _ => RegistrationStatus::Unregistered,
    }
}

pub(super) fn resolver(summary: &Value) -> Option<Resolver> {
    let resolver = object_field(summary, "resolver")?;
    let address = string_field(resolver.get("address"))
        .map(|value| value.to_ascii_lowercase())
        .filter(|value| !value.trim().is_empty())?;
    let chain_id = resolver.get("chain_id").and_then(json_chain_id)?;

    Some(Resolver { chain_id, address })
}

pub(super) fn record_addresses(
    record_inventory: Option<&RecordInventoryCurrentRow>,
) -> BTreeMap<String, String> {
    success_record_entries(record_inventory, "addr")
        .filter_map(|entry| {
            let coin_type = string_field(entry.get("selector_key")).or_else(|| {
                entry
                    .get("value")
                    .and_then(|value| string_field(value.get("coin_type")))
            })?;
            let coin_type = bigname_storage::canonical_addr_coin_type(&coin_type)?;
            let value = record_value_string(entry)?;
            Some((coin_type, value))
        })
        .collect()
}

pub(super) fn record_text_records(
    record_inventory: Option<&RecordInventoryCurrentRow>,
) -> BTreeMap<String, String> {
    let mut records = BTreeMap::new();
    for entry in success_record_entries(record_inventory, "text") {
        let Some(key) = string_field(entry.get("selector_key")).or_else(|| {
            entry
                .get("value")
                .and_then(|value| string_field(value.get("key")))
        }) else {
            continue;
        };
        if let Some(value) = record_value_string(entry) {
            records.insert(key, value);
        }
    }
    for entry in success_record_entries(record_inventory, "avatar") {
        if let Some(value) = record_value_string(entry) {
            records.insert("avatar".to_owned(), value);
        }
    }
    records
}

pub(super) fn record_content_hash(
    record_inventory: Option<&RecordInventoryCurrentRow>,
) -> Option<String> {
    success_record_entries(record_inventory, "contenthash").find_map(record_value_string)
}

pub(super) fn success_record_entries<'a>(
    record_inventory: Option<&'a RecordInventoryCurrentRow>,
    record_family: &'static str,
) -> impl Iterator<Item = &'a Value> {
    record_inventory
        .and_then(|inventory| inventory.entries.as_array())
        .into_iter()
        .flatten()
        .filter(move |entry| {
            string_field(entry.get("record_family")).as_deref() == Some(record_family)
                && string_field(entry.get("status")).as_deref() == Some("success")
        })
}

pub(super) fn record_value_string(entry: &Value) -> Option<String> {
    let value = entry.get("value")?;
    value
        .get("value")
        .and_then(value_to_string)
        .or_else(|| value_to_string(value))
}

fn unsupported_fields(record_inventory: Option<&RecordInventoryCurrentRow>) -> Vec<String> {
    let mut fields = BTreeSet::new();
    let Some(record_inventory) = record_inventory else {
        fields.insert("addresses".to_owned());
        fields.insert("primary_address".to_owned());
        fields.insert("text_records".to_owned());
        fields.insert("content_hash".to_owned());
        return fields.into_iter().collect();
    };

    for family in record_inventory
        .unsupported_families
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|family| string_field(family.get("record_family")))
    {
        match family.as_str() {
            "addr" => {
                fields.insert("addresses".to_owned());
                fields.insert("primary_address".to_owned());
            }
            "text" | "avatar" => {
                fields.insert("text_records".to_owned());
            }
            "contenthash" => {
                fields.insert("content_hash".to_owned());
            }
            _ => {}
        }
    }

    fields.into_iter().collect()
}

fn route_source(source: RequestSource) -> V2Result<Source> {
    match source {
        RequestSource::Indexed => Ok(Source::Indexed),
        RequestSource::Verified => Ok(Source::Verified),
        RequestSource::Auto => Err(V2Error::invalid_input(
            "source must be one of: indexed, verified",
        )),
    }
}

#[cfg(test)]
mod tests;
