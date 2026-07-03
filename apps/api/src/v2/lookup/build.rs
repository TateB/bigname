use std::collections::BTreeSet;

use serde_json::Value;

use super::{cursor::reverse_identity_is_primary, dto::LookupRecord};
use crate::{
    V2_RECORD_UNSUPPORTED_FIELD_NAMES, direct_json_field, record_addresses_from_entries,
    record_content_hash_from_entries, record_text_records_from_entries, record_unsupported_fields,
    v2::{
        Relation, Status, V2Result,
        name_record::{
            self, chain_id_from_positions, json_string_at_paths, network_from_parts, string_field,
        },
        shared_product_reason,
    },
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
        network: network_from_parts(&record.row.namespace, &record.row.chain_positions),
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
        network: network_from_parts(
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
        primary_name: json_string_at_paths(
            &record.row.declared_summary,
            &[
                &["primary_name"][..],
                &["primary_name", "name"][..],
                &["primary", "name"][..],
            ],
        ),
        chain_id: chain_id_from_positions(&record.row.chain_positions),
        network: network_from_parts(&record.row.namespace, &record.row.chain_positions),
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
) -> std::collections::BTreeMap<String, String> {
    record_addresses_from_entries(
        inventory.map(|inventory| &inventory.entries),
        direct_json_field,
    )
}

fn identity_text_records(
    inventory: Option<&bigname_storage::IdentityRecordInventoryRow>,
) -> std::collections::BTreeMap<String, String> {
    record_text_records_from_entries(
        inventory.map(|inventory| &inventory.entries),
        direct_json_field,
    )
}

fn identity_content_hash(
    inventory: Option<&bigname_storage::IdentityRecordInventoryRow>,
) -> Option<String> {
    record_content_hash_from_entries(
        inventory.map(|inventory| &inventory.entries),
        direct_json_field,
    )
}

fn identity_unsupported_fields(
    record: &bigname_storage::IdentityNameRecordRow,
) -> BTreeSet<String> {
    record_unsupported_fields(
        record.record_inventory_current.is_some(),
        record
            .record_inventory_current
            .as_ref()
            .map(|inventory| &inventory.unsupported_families),
        direct_json_field,
        V2_RECORD_UNSUPPORTED_FIELD_NAMES,
    )
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
    shared_product_reason(
        reason,
        "rejected lookup reason containing pipeline vocabulary",
        "failed to map lookup reason vocabulary",
    )
}
