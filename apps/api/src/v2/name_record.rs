use std::collections::{BTreeMap, BTreeSet};

use bigname_storage::{NameCurrentRow, RecordInventoryCurrentRow};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::types::time::{OffsetDateTime, UtcOffset};

use super::{
    chains::slug_to_numeric,
    vocab::{RegistrationStatus, Resolver, Status},
};

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

pub(crate) fn build_name_record(
    row: &NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
    chain_id: Option<u64>,
    status: Status,
) -> NameRecord {
    let registration = object_field(&row.declared_summary, "registration");

    let owner = json_address_at_paths(
        &row.declared_summary,
        &[&["control", "owner"], &["control", "registry_owner"]],
    );
    let registrant = json_address_at_paths(
        &row.declared_summary,
        &[&["registration", "registrant"], &["control", "registrant"]],
    );

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
        owner: owner.clone(),
        manager: None,
        registrant,
        registered_at: json_timestamp_at_paths(
            &row.declared_summary,
            &[&["registration", "registered_at"]],
        ),
        created_at: json_timestamp_at_paths(
            &row.declared_summary,
            &[&["registration", "created_at"]],
        ),
        expires_at: json_timestamp_at_paths(
            &row.declared_summary,
            &[
                &["registration", "expires_at"],
                &["registration", "expiry"],
                &["control", "expires_at"],
                &["control", "expiry"],
            ],
        ),
        registration_status: classify_registration_status(
            &row.namespace,
            registration,
            owner.as_deref(),
            row.surface_binding_id.is_some()
                || row.resource_id.is_some()
                || row.binding_kind.is_some(),
        ),
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

fn resolver(summary: &Value) -> Option<Resolver> {
    let resolver = object_field(summary, "resolver")?;
    let address = string_field(resolver.get("address"))
        .map(|value| value.to_ascii_lowercase())
        .filter(|value| !value.trim().is_empty())?;
    let chain_id = resolver.get("chain_id").and_then(json_chain_id)?;

    Some(Resolver { chain_id, address })
}

fn record_addresses(
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

fn record_text_records(
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

fn record_content_hash(record_inventory: Option<&RecordInventoryCurrentRow>) -> Option<String> {
    success_record_entries(record_inventory, "contenthash").find_map(record_value_string)
}

fn success_record_entries<'a>(
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

fn record_value_string(entry: &Value) -> Option<String> {
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

fn string_field(value: Option<&Value>) -> Option<String> {
    value.and_then(value_to_string)
}

fn value_to_string(value: &Value) -> Option<String> {
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
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn registration_status_classifier_covers_authority_kind_domain() {
        let active = json!({
            "status": "active",
            "authority_kind": "registrar",
            "released_at": null,
            "expiry": "2000-01-01T00:00:00Z"
        });
        assert_eq!(
            classify_registration_status("ens", Some(&active), Some("0xabc"), true),
            RegistrationStatus::Active
        );
        assert_eq!(
            classify_registration_status("basenames", Some(&active), Some("0xabc"), true),
            RegistrationStatus::Active
        );

        let registered = json!({
            "status": "active",
            "authority_kind": "registry_only",
            "released_at": null
        });
        assert_eq!(
            classify_registration_status("ens", Some(&registered), Some("0xabc"), true),
            RegistrationStatus::Registered
        );

        let ens_v2_registered = json!({
            "status": "active",
            "authority_kind": "ens_v2_registry",
            "released_at": null
        });
        assert_eq!(
            classify_registration_status("ens", Some(&ens_v2_registered), Some("0xabc"), true),
            RegistrationStatus::Registered
        );

        let wrapped = json!({
            "status": "active",
            "authority_kind": "wrapper",
            "released_at": null
        });
        assert_eq!(
            classify_registration_status("ens", Some(&wrapped), Some("0xabc"), true),
            RegistrationStatus::Wrapped
        );
        assert_eq!(
            classify_registration_status("basenames", Some(&wrapped), Some("0xabc"), true),
            RegistrationStatus::Unregistered
        );

        let released = json!({
            "status": "released",
            "authority_kind": "registrar",
            "released_at": "2026-06-14T00:00:00Z"
        });
        assert_eq!(
            classify_registration_status("ens", Some(&released), Some("0xabc"), true),
            RegistrationStatus::Released
        );

        let unregistered = json!({
            "status": "active",
            "authority_kind": "unknown_authority",
            "released_at": null
        });
        assert_eq!(
            classify_registration_status("ens", Some(&active), Some("0xabc"), false),
            RegistrationStatus::Unregistered
        );
        assert_eq!(
            classify_registration_status("ens", Some(&unregistered), Some("0xabc"), true),
            RegistrationStatus::Unregistered
        );
    }

    #[test]
    fn resolver_omits_unknown_chain_id_instead_of_guessing_mainnet() {
        let missing_chain = json!({
            "resolver": {
                "address": "0x0000000000000000000000000000000000000abc"
            }
        });
        assert_eq!(resolver(&missing_chain), None);

        let unknown_chain = json!({
            "resolver": {
                "chain_id": "unknown-mainnet",
                "address": "0x0000000000000000000000000000000000000abc"
            }
        });
        assert_eq!(resolver(&unknown_chain), None);
    }
}
