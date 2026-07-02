use std::collections::{BTreeMap, BTreeSet};

use axum::{
    Json,
    extract::{Path, State},
};
use bigname_storage::{NameCurrentRow, RecordInventoryCurrentRow, SelectedSnapshot};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::types::time::{OffsetDateTime, UtcOffset};

use crate::{
    AppState, load_name_current_for_selected_snapshot,
    load_supported_record_inventory_current_for_snapshot, map_internal_api_error,
    normalize_inferred_route_name, snapshot_selection_api_error,
};

use super::{
    Envelope, Meta, QueryParamAllowlist, QueryParams, RequestSource, StrictQueryParams, V2Error,
    V2Result, api_error_to_v2, as_of_meta,
    chains::slug_to_numeric,
    resolve_v2_snapshot, v2_exact_name_snapshot_scope,
    vocab::{RegistrationStatus, Resolver, Source, Status},
};

pub(crate) struct NameRecordQueryParams;

impl QueryParamAllowlist for NameRecordQueryParams {
    const ALLOWED: &'static [&'static str] = &["namespace", "at", "finality", "source"];
}

pub(crate) type NameRecordQuery = StrictQueryParams<NameRecordQueryParams>;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct NameRecord {
    pub(crate) registration_id: Option<String>,
    pub(crate) token_id: Option<String>,
    pub(crate) owner: Option<String>,
    pub(crate) manager: Option<String>,
    pub(crate) registrant: Option<String>,
    pub(crate) registered_at: Option<String>,
    pub(crate) created_at: Option<String>,
    pub(crate) expires_at: Option<String>,
    pub(crate) registration_status: RegistrationStatus,
    pub(crate) name: String,
    pub(crate) display_name: String,
    pub(crate) namespace: String,
    pub(crate) namehash: String,
    pub(crate) resolver: Option<Resolver>,
    pub(crate) addresses: BTreeMap<String, String>,
    pub(crate) text_records: BTreeMap<String, String>,
    pub(crate) content_hash: Option<String>,
    pub(crate) primary_name: Option<String>,
    pub(crate) primary_address: Option<String>,
    pub(crate) chain_id: Option<u64>,
    pub(crate) network: String,
    pub(crate) status: Status,
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

    let scope = v2_exact_name_snapshot_scope(&state, &namespace, params.at.as_ref()).await?;
    let selected_snapshot =
        resolve_v2_snapshot(&state.pool, &scope, params.at.as_ref(), params.finality).await?;
    let row = load_name_current_for_selected_snapshot(
        &state.pool,
        &namespace,
        &normalized.normalized_name,
        &selected_snapshot,
    )
    .await
    .map_err(|error| {
        api_error_to_v2(map_internal_api_error(
            error,
            format!(
                "failed to load name profile for {}/{}",
                namespace, normalized.normalized_name
            ),
        ))
    })?;

    let record_inventory =
        load_supported_record_inventory_current_for_snapshot(&state.pool, &row, &selected_snapshot)
            .await
            .map_err(|error| api_error_to_v2(snapshot_selection_api_error(error)))?;
    let chain_id = response_chain_id(&selected_snapshot);
    let mut data = build_name_record(
        &row,
        record_inventory.as_ref(),
        chain_id,
        if route_source == Source::Verified {
            Status::Failed
        } else {
            Status::Ok
        },
    );
    if route_source == Source::Verified {
        mark_unserved_verified_fields(&mut data);
    }
    let meta = Meta {
        as_of: Some(as_of_meta(&selected_snapshot)?),
        source: Some(route_source),
        ..Meta::default()
    };

    Ok(Json(Envelope {
        data,
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
    let addresses = record_addresses(record_inventory);
    let text_records = record_text_records(record_inventory);
    let content_hash = record_content_hash(record_inventory);
    let primary_address = addresses.get("60").cloned();
    let unsupported_fields = unsupported_fields(record_inventory);

    NameRecord {
        registration_id: row.resource_id.map(|value| value.to_string()),
        // The current worker-emitted declared_summary has no token_id or
        // manager/controller source; keep these null until projection enrichment
        // adds canonical fields.
        token_id: None,
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
        return NameRegistrationFields {
            owner: None,
            registrant: None,
            registered_at: None,
            created_at: None,
            expires_at: None,
            registration_status: classify_registration_status(namespace, None, None, false),
        };
    };

    let registration = declared_registration(&row.declared_summary);
    let owner = declared_owner(&row.declared_summary);

    NameRegistrationFields {
        registrant: declared_registrant(&row.declared_summary),
        registered_at: declared_registered_at(&row.declared_summary),
        created_at: declared_created_at(&row.declared_summary),
        expires_at: declared_expires_at(&row.declared_summary),
        registration_status: classify_registration_status(
            &row.namespace,
            registration,
            owner.as_deref(),
            has_binding(row),
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
    json_timestamp_at_paths(summary, &[&["registration", "registered_at"]])
}

pub(super) fn declared_created_at(summary: &Value) -> Option<String> {
    json_timestamp_at_paths(summary, &[&["registration", "created_at"]])
}

pub(super) fn declared_expires_at(summary: &Value) -> Option<String> {
    json_timestamp_at_paths(
        summary,
        &[
            &["registration", "expires_at"],
            &["registration", "expiry"],
            &["control", "expires_at"],
            &["control", "expiry"],
        ],
    )
}

fn has_binding(row: &NameCurrentRow) -> bool {
    row.surface_binding_id.is_some() || row.resource_id.is_some() || row.binding_kind.is_some()
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

fn mark_unserved_verified_fields(record: &mut NameRecord) {
    for field in [
        "addresses",
        "content_hash",
        "primary_address",
        "text_records",
    ] {
        if !record.unsupported_fields.iter().any(|value| value == field) {
            record.unsupported_fields.push(field.to_owned());
        }
    }
    record.unsupported_fields.sort();
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

fn response_chain_id(selected_snapshot: &SelectedSnapshot) -> Option<u64> {
    selected_snapshot
        .chain_positions
        .as_map()
        .values()
        .find_map(|position| super::slug_to_numeric(&position.chain_id))
}

fn network(row: &NameCurrentRow) -> String {
    match row.namespace.as_str() {
        "basenames" if has_chain_position(&row.chain_positions, "base-sepolia") => {
            "base-sepolia".to_owned()
        }
        "basenames" => "base".to_owned(),
        "ens" if has_chain_position(&row.chain_positions, "ethereum-sepolia") => {
            "ethereum-sepolia".to_owned()
        }
        "ens" => "ethereum".to_owned(),
        namespace => namespace.to_owned(),
    }
}

fn has_chain_position(chain_positions: &Value, chain_id: &str) -> bool {
    chain_positions
        .as_object()
        .into_iter()
        .flatten()
        .any(|(slot, value)| {
            slot == chain_id || string_field(value.get("chain_id")).as_deref() == Some(chain_id)
        })
}

fn json_chain_id(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(value) => value.parse::<u64>().ok().or_else(|| slug_to_numeric(value)),
        _ => None,
    }
}

fn object_field<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    value.get(key).filter(|value| value.is_object())
}

fn json_string_at_paths(value: &Value, paths: &[&[&str]]) -> Option<String> {
    paths
        .iter()
        .find_map(|path| json_path(value, path).and_then(value_to_string))
        .filter(|value| !value.trim().is_empty())
}

fn json_address_at_paths(value: &Value, paths: &[&[&str]]) -> Option<String> {
    json_string_at_paths(value, paths).map(|value| value.to_ascii_lowercase())
}

fn json_timestamp_at_paths(value: &Value, paths: &[&[&str]]) -> Option<String> {
    for path in paths {
        let Some(value) = json_path(value, path) else {
            continue;
        };
        match value {
            Value::String(value) if !value.trim().is_empty() => return Some(value.clone()),
            Value::Number(number) => {
                if let Some(timestamp) = number.as_i64().and_then(format_unix_timestamp) {
                    return Some(timestamp);
                }
            }
            _ => {}
        }
    }
    None
}

fn json_path<'a>(mut value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    for key in path {
        value = value.get(*key)?;
    }
    Some(value)
}

pub(super) fn string_field(value: Option<&Value>) -> Option<String> {
    value.and_then(value_to_string)
}

pub(super) fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn json_value_present(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::String(value) => !value.trim().is_empty(),
        _ => true,
    }
}

fn format_unix_timestamp(timestamp: i64) -> Option<String> {
    let value = OffsetDateTime::from_unix_timestamp(timestamp).ok()?;
    Some(format_timestamp(value))
}

fn format_timestamp(value: OffsetDateTime) -> String {
    let value = value.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        value.year(),
        value.month() as u8,
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    )
}

#[cfg(test)]
mod tests;
