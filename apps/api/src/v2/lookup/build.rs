use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use super::{cursor::reverse_identity_is_primary, dto::LookupRecord};
use crate::v2::{
    PRODUCT_PIPELINE_TERMS, Relation, Status, V2Error, V2Result, contains_pipeline_vocabulary,
    name_record,
};

const MISSING_UNSUPPORTED_REASON: &str = "unsupported_reason_missing";

pub(super) fn build_forward_detail_record(
    record: &bigname_storage::IdentityNameRecordRow,
) -> V2Result<LookupRecord> {
    build_detail_record(record, "60", None, Vec::new())
}

pub(super) fn build_forward_feed_record(
    record: &bigname_storage::IdentityNameRecordRow,
) -> V2Result<LookupRecord> {
    let status = identity_record_status(&record.row.coverage);
    Ok(LookupRecord {
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
        addresses: None,
        text_records: None,
        content_hash: None,
        primary_name: None,
        primary_address: None,
        chain_id: chain_id_from_positions(&record.row.chain_positions),
        network: identity_network(&record.row.namespace, &record.row.chain_positions),
        is_primary: None,
        relations: Vec::new(),
        status,
        unsupported_reason: identity_record_unsupported_reason(&record.row.coverage, status)?,
        failure_reason: identity_record_failure_reason(&record.row.coverage, status)?,
        unsupported_fields: Vec::new(),
    })
}

pub(super) fn build_reverse_detail_record(
    record: &bigname_storage::ReverseIdentityRecordRow,
) -> V2Result<LookupRecord> {
    build_detail_record(
        &record.name_record,
        &record.requested_coin_type,
        Some(reverse_identity_is_primary(record)),
        lookup_relations(&record.relation_facets),
    )
}

pub(super) fn build_reverse_feed_record(
    record: &bigname_storage::ReverseIdentityRecordRow,
) -> V2Result<LookupRecord> {
    let status = identity_record_status(&record.name_record.row.coverage);
    Ok(LookupRecord {
        name: record.name_record.row.normalized_name.clone(),
        display_name: record.name_record.row.canonical_display_name.clone(),
        namespace: record.name_record.row.namespace.clone(),
        namehash: record.name_record.row.namehash.clone(),
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
        addresses: None,
        text_records: None,
        content_hash: None,
        primary_name: None,
        primary_address: None,
        chain_id: chain_id_from_positions(&record.name_record.row.chain_positions),
        network: identity_network(
            &record.name_record.row.namespace,
            &record.name_record.row.chain_positions,
        ),
        is_primary: Some(reverse_identity_is_primary(record)),
        relations: lookup_relations(&record.relation_facets),
        status,
        unsupported_reason: identity_record_unsupported_reason(
            &record.name_record.row.coverage,
            status,
        )?,
        failure_reason: identity_record_failure_reason(&record.name_record.row.coverage, status)?,
        unsupported_fields: Vec::new(),
    })
}

pub(super) fn lookup_address_status(records: &[LookupRecord]) -> Status {
    if records.iter().any(|record| record.status == Status::Failed) {
        return Status::Failed;
    }
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
) -> V2Result<LookupRecord> {
    let addresses = identity_addresses(record.record_inventory_current.as_ref());
    let text_records = identity_text_records(record.record_inventory_current.as_ref());
    let content_hash = identity_content_hash(record.record_inventory_current.as_ref());
    let unsupported_fields = identity_unsupported_fields(record);
    let registration =
        name_record::identity_name_registration_fields(Some(&record.row), &record.row.namespace);
    let token_id = name_record::identity_declared_token_id(&record.row);
    let addresses = (!unsupported_fields.contains("addresses")).then_some(addresses);
    let text_records = (!unsupported_fields.contains("text_records")).then_some(text_records);
    let content_hash = (!unsupported_fields.contains("content_hash"))
        .then_some(content_hash)
        .flatten();
    let primary_address = addresses
        .as_ref()
        .filter(|_| !unsupported_fields.contains("primary_address"))
        .and_then(|addresses| addresses.get(primary_coin_type).cloned());
    let status = identity_record_status(&record.row.coverage);

    Ok(LookupRecord {
        name: record.row.normalized_name.clone(),
        display_name: record.row.canonical_display_name.clone(),
        namespace: record.row.namespace.clone(),
        namehash: record.row.namehash.clone(),
        registration_id: record.row.resource_id.map(|value| value.to_string()),
        token_id,
        owner: registration.owner,
        manager: None,
        registrant: registration.registrant,
        registered_at: registration.registered_at,
        created_at: registration.created_at,
        expires_at: registration.expires_at,
        registration_status: Some(registration.registration_status),
        resolver: name_record::resolver(&record.row.declared_summary),
        primary_address,
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
        status,
        unsupported_reason: identity_record_unsupported_reason(&record.row.coverage, status)?,
        failure_reason: identity_record_failure_reason(&record.row.coverage, status)?,
        unsupported_fields: unsupported_fields.into_iter().collect(),
    })
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

fn identity_record_status(coverage: &Value) -> Status {
    match string_field(coverage.get("status")).as_deref() {
        Some("stale") => Status::Stale,
        Some("unsupported") => Status::Unsupported,
        Some("failed") => Status::Failed,
        _ => Status::Ok,
    }
}

fn identity_record_unsupported_reason(
    coverage: &Value,
    status: Status,
) -> V2Result<Option<String>> {
    if status != Status::Unsupported {
        return Ok(None);
    }

    let reason = string_field(coverage.get("unsupported_reason"))
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| MISSING_UNSUPPORTED_REASON.to_owned());
    product_lookup_reason(&reason).map(Some)
}

fn identity_record_failure_reason(coverage: &Value, status: Status) -> V2Result<Option<String>> {
    if !matches!(status, Status::Failed | Status::NotFound | Status::Mismatch) {
        return Ok(None);
    }

    string_field(coverage.get("failure_reason"))
        .filter(|value| !value.trim().is_empty())
        .map(|reason| product_lookup_reason(&reason))
        .transpose()
}

fn product_lookup_reason(reason: &str) -> V2Result<String> {
    match reason {
        "projection_read_failed" => Ok("read_failed".to_owned()),
        "ensv2_exact_name_profile_shadow" => Ok("exact_name_profile_not_supported".to_owned()),
        "mixed_ensv1_ensv2_exact_name_corpus" => Ok("mixed_exact_name_corpus".to_owned()),
        _ if lookup_reason_contains_pipeline_vocabulary(reason) => {
            tracing::error!(%reason, "rejected lookup reason containing pipeline vocabulary");
            Err(V2Error::internal_error(
                "failed to map lookup reason vocabulary",
            ))
        }
        _ => Ok(reason.to_owned()),
    }
}

fn lookup_reason_contains_pipeline_vocabulary(reason: &str) -> bool {
    contains_pipeline_vocabulary(reason, PRODUCT_PIPELINE_TERMS)
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

fn identity_json_string(value: &Value, paths: &[&[&str]]) -> Option<String> {
    paths
        .iter()
        .find_map(|path| json_path(value, path).and_then(value_to_string))
        .filter(|value| !value.trim().is_empty())
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
