use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;
use sqlx::types::time::{OffsetDateTime, UtcOffset};

use super::{cursor::reverse_identity_is_primary, dto::LookupRecord};
use crate::v2::{RegistrationStatus, Relation, Resolver, Status, classify_registration_status};

pub(super) fn build_forward_detail_record(
    record: &bigname_storage::IdentityNameRecordRow,
) -> LookupRecord {
    build_detail_record(record, "60", None, Vec::new())
}

pub(super) fn build_forward_feed_record(
    record: &bigname_storage::IdentityNameRecordRow,
) -> LookupRecord {
    LookupRecord {
        name: record.row.normalized_name.clone(),
        display_name: record.row.canonical_display_name.clone(),
        namespace: record.row.namespace.clone(),
        namehash: record.row.namehash.clone(),
        registration_id: None,
        token_id: None,
        owner: None,
        manager: None,
        registrant: None,
        registered_at: None,
        created_at: None,
        expires_at: None,
        registration_status: None,
        resolver: None,
        addresses: BTreeMap::new(),
        text_records: BTreeMap::new(),
        content_hash: None,
        primary_name: None,
        primary_address: None,
        chain_id: chain_id_from_positions(&record.row.chain_positions),
        network: identity_network(&record.row.namespace, &record.row.chain_positions),
        is_primary: None,
        relations: Vec::new(),
        status: identity_record_status(&record.row.coverage),
        unsupported_fields: Vec::new(),
    }
}

pub(super) fn build_reverse_detail_record(
    record: &bigname_storage::ReverseIdentityRecordRow,
) -> LookupRecord {
    build_detail_record(
        &record.name_record,
        &record.requested_coin_type,
        Some(reverse_identity_is_primary(record)),
        lookup_relations(&record.relation_facets),
    )
}

pub(super) fn build_reverse_feed_record(
    record: &bigname_storage::ReverseIdentityFeedRecordRow,
) -> LookupRecord {
    LookupRecord {
        name: record.normalized_name.clone(),
        display_name: record.canonical_display_name.clone(),
        namespace: record.namespace.clone(),
        namehash: record.namehash.clone(),
        registration_id: None,
        token_id: None,
        owner: None,
        manager: None,
        registrant: None,
        registered_at: None,
        created_at: None,
        expires_at: None,
        registration_status: None,
        resolver: None,
        addresses: BTreeMap::new(),
        text_records: BTreeMap::new(),
        content_hash: None,
        primary_name: None,
        primary_address: None,
        chain_id: chain_id_from_positions(&record.chain_positions),
        network: identity_network(&record.namespace, &record.chain_positions),
        is_primary: Some(record.is_primary),
        relations: lookup_relations(&record.relation_facets),
        status: identity_record_status(&record.coverage),
        unsupported_fields: Vec::new(),
    }
}

pub(super) fn lookup_address_status(records: &[LookupRecord]) -> Status {
    if records.iter().any(|record| record.status == Status::Stale) {
        return Status::Stale;
    }
    if !records.is_empty()
        && records
            .iter()
            .all(|record| record.status == Status::Unsupported)
    {
        return Status::Unsupported;
    }
    Status::Ok
}

fn build_detail_record(
    record: &bigname_storage::IdentityNameRecordRow,
    primary_coin_type: &str,
    is_primary: Option<bool>,
    relations: Vec<Relation>,
) -> LookupRecord {
    let addresses = identity_addresses(record.record_inventory_current.as_ref());
    let text_records = identity_text_records(record.record_inventory_current.as_ref());
    let content_hash = identity_content_hash(record.record_inventory_current.as_ref());
    let mut unsupported_fields = identity_unsupported_fields(record);
    let owner = identity_relation_subject(
        &record.relations,
        &[bigname_storage::AddressNameRelation::TokenHolder],
    )
    .or_else(|| identity_json_address(&record.row.declared_summary, &[&["control", "owner"]]))
    .or_else(|| {
        identity_json_address(
            &record.row.declared_summary,
            &[&["control", "registry_owner"]],
        )
    });
    let manager = identity_relation_subject(
        &record.relations,
        &[bigname_storage::AddressNameRelation::EffectiveController],
    );
    let registrant = identity_json_address(
        &record.row.declared_summary,
        &[&["registration", "registrant"], &["control", "registrant"]],
    )
    .or_else(|| {
        identity_relation_subject(
            &record.relations,
            &[bigname_storage::AddressNameRelation::Registrant],
        )
    });
    let expires_at = identity_json_timestamp(
        &record.row.declared_summary,
        &[
            &["registration", "expires_at"],
            &["registration", "expiry_date"],
            &["registration", "expiry"],
            &["control", "expires_at"],
            &["control", "expiry_date"],
            &["control", "expiry"],
        ],
    );
    let token_id = identity_json_string(
        &record.row.declared_summary,
        &[
            &["authority", "token_id"],
            &["registration", "token_id"],
            &["registration", "upstream_resource"],
            &["control", "token_id"],
        ],
    )
    .or_else(|| identity_labelhash_token_id(&record.row));

    for (field, missing) in [
        ("owner", owner.is_none()),
        ("manager", manager.is_none()),
        ("registrant", registrant.is_none()),
        ("expires_at", expires_at.is_none()),
        ("token_id", token_id.is_none()),
    ] {
        if missing {
            unsupported_fields.insert(field.to_owned());
        }
    }

    let registration = object_field(&record.row.declared_summary, "registration");
    let registration_status = classify_registration_status(
        &record.row.namespace,
        registration,
        owner.as_deref(),
        record.row.resource_id.is_some(),
    );

    LookupRecord {
        name: record.row.normalized_name.clone(),
        display_name: record.row.canonical_display_name.clone(),
        namespace: record.row.namespace.clone(),
        namehash: record.row.namehash.clone(),
        registration_id: record.row.resource_id.map(|value| value.to_string()),
        token_id,
        owner,
        manager,
        registrant,
        registered_at: identity_json_timestamp(
            &record.row.declared_summary,
            &[&["registration", "registered_at"]],
        ),
        created_at: identity_json_timestamp(
            &record.row.declared_summary,
            &[&["registration", "created_at"]],
        ),
        expires_at,
        registration_status: Some(registration_status),
        resolver: resolver(&record.row.declared_summary),
        primary_address: addresses.get(primary_coin_type).cloned(),
        addresses,
        text_records,
        content_hash,
        primary_name: identity_json_string(
            &record.row.declared_summary,
            &[
                &["primary_name"],
                &["primary_name", "name"],
                &["primary", "name"],
            ],
        ),
        chain_id: chain_id_from_positions(&record.row.chain_positions),
        network: identity_network(&record.row.namespace, &record.row.chain_positions),
        is_primary,
        relations,
        status: identity_record_status(&record.row.coverage),
        unsupported_fields: unsupported_fields.into_iter().collect(),
    }
}

fn identity_addresses(
    inventory: Option<&bigname_storage::IdentityRecordInventoryRow>,
) -> BTreeMap<String, String> {
    identity_success_record_entries(inventory, "addr")
        .filter_map(|entry| {
            let coin_type = string_field(entry.get("selector_key")).or_else(|| {
                entry
                    .get("value")
                    .and_then(|value| string_field(value.get("coin_type")))
            })?;
            let coin_type = bigname_storage::canonical_addr_coin_type(&coin_type)?;
            let value = identity_record_value_string(entry)?;
            Some((coin_type, value))
        })
        .collect()
}

fn identity_text_records(
    inventory: Option<&bigname_storage::IdentityRecordInventoryRow>,
) -> BTreeMap<String, String> {
    let mut records = BTreeMap::new();
    for entry in identity_success_record_entries(inventory, "text") {
        let Some(key) = string_field(entry.get("selector_key")).or_else(|| {
            entry
                .get("value")
                .and_then(|value| string_field(value.get("key")))
        }) else {
            continue;
        };
        if let Some(value) = identity_record_value_string(entry) {
            records.insert(key, value);
        }
    }
    for entry in identity_success_record_entries(inventory, "avatar") {
        if let Some(value) = identity_record_value_string(entry) {
            records.insert("avatar".to_owned(), value);
        }
    }
    records
}

fn identity_content_hash(
    inventory: Option<&bigname_storage::IdentityRecordInventoryRow>,
) -> Option<String> {
    identity_success_record_entries(inventory, "contenthash").find_map(identity_record_value_string)
}

fn identity_success_record_entries<'a>(
    inventory: Option<&'a bigname_storage::IdentityRecordInventoryRow>,
    record_family: &'static str,
) -> impl Iterator<Item = &'a Value> {
    inventory
        .and_then(|inventory| inventory.entries.as_array())
        .into_iter()
        .flatten()
        .filter(move |entry| {
            string_field(entry.get("record_family")).as_deref() == Some(record_family)
                && string_field(entry.get("status")).as_deref() == Some("success")
        })
}

fn identity_record_value_string(entry: &Value) -> Option<String> {
    let value = entry.get("value")?;
    value
        .get("value")
        .and_then(value_to_string)
        .or_else(|| value_to_string(value))
}

fn identity_unsupported_fields(
    record: &bigname_storage::IdentityNameRecordRow,
) -> BTreeSet<String> {
    let mut fields = BTreeSet::new();
    let Some(inventory) = record.record_inventory_current.as_ref() else {
        fields.insert("addresses".to_owned());
        fields.insert("primary_address".to_owned());
        fields.insert("text_records".to_owned());
        fields.insert("content_hash".to_owned());
        return fields;
    };

    for family in inventory
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
    fields
}

fn identity_relation_subject(
    relations: &[bigname_storage::IdentityAddressRelationRow],
    accepted: &[bigname_storage::AddressNameRelation],
) -> Option<String> {
    let subjects = relations
        .iter()
        .filter(|relation| accepted.contains(&relation.relation))
        .map(|relation| relation.address.clone())
        .collect::<BTreeSet<_>>();
    (subjects.len() == 1)
        .then(|| subjects.into_iter().next())
        .flatten()
}

pub(super) fn lookup_relations(
    relations: &[bigname_storage::AddressNameRelation],
) -> Vec<Relation> {
    let has_owner = relations.contains(&bigname_storage::AddressNameRelation::TokenHolder);
    let has_manager =
        relations.contains(&bigname_storage::AddressNameRelation::EffectiveController);
    let has_registrant = relations.contains(&bigname_storage::AddressNameRelation::Registrant);

    [
        (has_owner, Relation::Owner),
        (has_manager, Relation::Manager),
        (has_registrant, Relation::Registrant),
    ]
    .into_iter()
    .filter_map(|(present, relation)| present.then_some(relation))
    .collect()
}

fn resolver(summary: &Value) -> Option<Resolver> {
    let resolver = object_field(summary, "resolver")?;
    let address = string_field(resolver.get("address"))
        .map(|value| value.to_ascii_lowercase())
        .filter(|value| !value.trim().is_empty())?;
    let chain_id = resolver.get("chain_id").and_then(json_chain_id)?;
    Some(Resolver { chain_id, address })
}

fn identity_record_status(coverage: &Value) -> Status {
    match string_field(coverage.get("status")).as_deref() {
        Some("stale") => Status::Stale,
        Some("unsupported") => Status::Unsupported,
        Some("failed") => Status::Failed,
        _ => Status::Ok,
    }
}

fn identity_network(namespace: &str, chain_positions: &Value) -> String {
    match namespace {
        "basenames" if has_chain_position(chain_positions, "base-sepolia") => {
            "base-sepolia".to_owned()
        }
        "basenames" => "base".to_owned(),
        "ens" if has_chain_position(chain_positions, "ethereum-sepolia") => {
            "ethereum-sepolia".to_owned()
        }
        "ens" => "ethereum".to_owned(),
        namespace => namespace.to_owned(),
    }
}

fn chain_id_from_positions(chain_positions: &Value) -> Option<u64> {
    chain_positions
        .as_object()
        .into_iter()
        .flatten()
        .find_map(|(_, value)| {
            value
                .get("chain_id")
                .and_then(value_to_string)
                .and_then(|value| crate::v2::slug_to_numeric(&value))
        })
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

fn identity_labelhash_token_id(row: &bigname_storage::IdentityNameCurrentRow) -> Option<String> {
    if row.namespace != "ens"
        || row.labelhash_count != Some(2)
        || !row.normalized_name.ends_with(".eth")
        || row.normalized_name.split('.').count() != 2
    {
        return None;
    }
    let labelhash = row.labelhash.as_deref()?;
    let hex = labelhash.strip_prefix("0x").unwrap_or(labelhash);
    alloy_primitives::U256::from_str_radix(hex, 16)
        .ok()
        .map(|value| value.to_string())
}

fn identity_json_string(value: &Value, paths: &[&[&str]]) -> Option<String> {
    paths
        .iter()
        .find_map(|path| json_path(value, path).and_then(value_to_string))
        .filter(|value| !value.trim().is_empty())
}

fn identity_json_address(value: &Value, paths: &[&[&str]]) -> Option<String> {
    identity_json_string(value, paths).map(|value| value.to_ascii_lowercase())
}

fn identity_json_timestamp(value: &Value, paths: &[&[&str]]) -> Option<String> {
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

fn object_field<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    value.get(key).filter(|value| value.is_object())
}

fn json_chain_id(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(value) => value
            .parse::<u64>()
            .ok()
            .or_else(|| crate::v2::slug_to_numeric(value)),
        _ => None,
    }
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

#[allow(dead_code)]
fn _registration_status_type_guard(_: RegistrationStatus) {}
