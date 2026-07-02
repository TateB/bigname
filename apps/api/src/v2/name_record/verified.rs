use std::collections::{BTreeMap, BTreeSet};

use bigname_storage::{NameCurrentRow, RecordInventoryCurrentRow, SelectedSnapshot};

use crate::{AppState, ResolutionRecordKey};

use super::super::{
    SnapshotReadResource, Source, Status, V2Result, default_requested_records,
    name_records::{
        RecordAnswer, VERIFIED_NOT_SUPPORTED_REASON, VerifiedRecordLookup,
        build_verified_name_records, load_verified_record_lookup_for_resource,
    },
};
use super::{NameRecord, build_name_record, string_field};

const PROFILE_FALLBACK_RECORD_KEYS: &[&str] = &[
    "addr:60",
    "avatar",
    "contenthash",
    "text:description",
    "text:url",
    "text:email",
];

pub(super) struct VerifiedNameRecord {
    pub(super) record: NameRecord,
    pub(super) uses_on_demand_fallback: bool,
}

pub(super) async fn build_name_record_for_source(
    state: &AppState,
    row: &NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
    chain_id: Option<u64>,
    selected_snapshot: &SelectedSnapshot,
    source: Source,
) -> V2Result<VerifiedNameRecord> {
    match source {
        Source::Indexed => Ok(VerifiedNameRecord {
            record: build_name_record(row, record_inventory, chain_id, Status::Ok),
            uses_on_demand_fallback: false,
        }),
        Source::Verified => {
            build_verified_name_record(state, row, record_inventory, chain_id, selected_snapshot)
                .await
        }
    }
}

async fn build_verified_name_record(
    state: &AppState,
    row: &NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
    chain_id: Option<u64>,
    selected_snapshot: &SelectedSnapshot,
) -> V2Result<VerifiedNameRecord> {
    let requested_records = profile_verified_requested_records(record_inventory);
    let verified_lookup = load_verified_record_lookup_for_resource(
        state,
        row,
        record_inventory,
        &requested_records,
        selected_snapshot,
        SnapshotReadResource::Name,
    )
    .await?;
    let uses_on_demand_fallback = verified_lookup
        .as_ref()
        .is_some_and(VerifiedRecordLookup::uses_on_demand_fallback);
    let mut verified_records = build_verified_name_records(
        row,
        record_inventory,
        Some(&requested_records),
        verified_lookup,
        false,
    )?;
    let answers = verified_records
        .records
        .as_ref()
        .expect("verified profile requested records must produce an answer map");

    let mut record = build_name_record(row, record_inventory, chain_id, Status::Ok);
    let addresses = std::mem::take(&mut verified_records.addresses);
    let text_records = std::mem::take(&mut verified_records.text_records);
    let content_hash = verified_records.content_hash.take();
    let primary_address = addresses.get("60").cloned();
    let addresses_unserved = field_could_not_serve(&requested_records, answers, is_address_record);
    let text_records_unserved = field_could_not_serve(&requested_records, answers, is_text_record);
    let content_hash_unserved =
        field_could_not_serve(&requested_records, answers, is_content_hash_record);
    let primary_address_unserved =
        field_could_not_serve(&requested_records, answers, is_primary_address_record);

    let unsupported_fields = verified_unsupported_fields(
        addresses_unserved,
        text_records_unserved,
        content_hash_unserved,
        primary_address_unserved,
    );
    let status = verified_profile_status(answers, &unsupported_fields);

    record.addresses = (!addresses_unserved)
        .then(|| dictionary_field(addresses, &requested_records, answers, is_address_record))
        .flatten();
    record.text_records = (!text_records_unserved)
        .then(|| dictionary_field(text_records, &requested_records, answers, is_text_record))
        .flatten();
    record.content_hash = (!content_hash_unserved).then_some(content_hash).flatten();
    record.primary_address = (!primary_address_unserved)
        .then_some(primary_address)
        .flatten();
    record.status = status;
    record.unsupported_reason = verified_profile_unsupported_reason(answers, status);
    record.failure_reason = verified_profile_failure_reason(answers, status);
    record.unsupported_fields = unsupported_fields;
    Ok(VerifiedNameRecord {
        record,
        uses_on_demand_fallback,
    })
}

fn profile_verified_requested_records(
    record_inventory: Option<&RecordInventoryCurrentRow>,
) -> Vec<ResolutionRecordKey> {
    let records = default_requested_records(record_inventory);
    if !records.is_empty() || !should_use_profile_fallback_records(record_inventory) {
        records
    } else {
        profile_fallback_requested_records()
    }
}

fn should_use_profile_fallback_records(
    record_inventory: Option<&RecordInventoryCurrentRow>,
) -> bool {
    let Some(record_inventory) = record_inventory else {
        return false;
    };
    string_field(record_inventory.coverage.get("status")).as_deref() != Some("unsupported")
}

fn profile_fallback_requested_records() -> Vec<ResolutionRecordKey> {
    PROFILE_FALLBACK_RECORD_KEYS
        .iter()
        .map(|record_key| {
            crate::parse_resolution_record_key(record_key)
                .expect("profile fallback record selector must be valid")
        })
        .collect()
}

fn dictionary_field(
    values: BTreeMap<String, String>,
    requested_records: &[ResolutionRecordKey],
    answers: &BTreeMap<String, RecordAnswer>,
    predicate: fn(&ResolutionRecordKey) -> bool,
) -> Option<BTreeMap<String, String>> {
    if !values.is_empty() || field_has_served_answer(requested_records, answers, predicate) {
        Some(values)
    } else {
        None
    }
}

fn verified_unsupported_fields(
    addresses_unserved: bool,
    text_records_unserved: bool,
    content_hash_unserved: bool,
    primary_address_unserved: bool,
) -> Vec<String> {
    let mut fields = BTreeSet::new();

    if addresses_unserved {
        fields.insert("addresses".to_owned());
    }
    if content_hash_unserved {
        fields.insert("content_hash".to_owned());
    }
    if primary_address_unserved {
        fields.insert("primary_address".to_owned());
    }
    if text_records_unserved {
        fields.insert("text_records".to_owned());
    }

    fields.into_iter().collect()
}

fn field_could_not_serve(
    requested_records: &[ResolutionRecordKey],
    answers: &BTreeMap<String, RecordAnswer>,
    predicate: fn(&ResolutionRecordKey) -> bool,
) -> bool {
    let mut has_relevant_record = false;
    let mut has_problem_answer = false;
    for record in requested_records.iter().filter(|record| predicate(record)) {
        has_relevant_record = true;
        match answers.get(&record.record_key) {
            Some(answer) if answer_is_problem(answer) => has_problem_answer = true,
            Some(_) => {}
            None => has_problem_answer = true,
        }
    }

    !has_relevant_record || has_problem_answer
}

fn field_has_served_answer(
    requested_records: &[ResolutionRecordKey],
    answers: &BTreeMap<String, RecordAnswer>,
    predicate: fn(&ResolutionRecordKey) -> bool,
) -> bool {
    requested_records
        .iter()
        .filter(|record| predicate(record))
        .filter_map(|record| answers.get(&record.record_key))
        .any(answer_is_served)
}

fn verified_profile_status(
    answers: &BTreeMap<String, RecordAnswer>,
    unsupported_fields: &[String],
) -> Status {
    if answers
        .values()
        .any(|answer| answer.status == Status::Stale)
    {
        Status::Stale
    } else if answers
        .values()
        .any(|answer| answer.status == Status::Failed)
    {
        Status::Failed
    } else if !unsupported_fields.is_empty()
        && (answers.is_empty()
            || answers
                .values()
                .any(|answer| answer.status == Status::Unsupported))
    {
        Status::Unsupported
    } else {
        Status::Ok
    }
}

fn verified_profile_failure_reason(
    answers: &BTreeMap<String, RecordAnswer>,
    status: Status,
) -> Option<String> {
    match status {
        Status::Failed | Status::Stale => answers
            .values()
            .find(|answer| answer.status == status)
            .and_then(|answer| answer.failure_reason.clone()),
        _ => None,
    }
}

fn verified_profile_unsupported_reason(
    answers: &BTreeMap<String, RecordAnswer>,
    status: Status,
) -> Option<String> {
    if status != Status::Unsupported {
        return None;
    }

    Some(
        answers
            .values()
            .find(|answer| answer.status == Status::Unsupported)
            .and_then(|answer| answer.unsupported_reason.clone())
            .unwrap_or_else(|| VERIFIED_NOT_SUPPORTED_REASON.to_owned()),
    )
}

fn answer_is_served(answer: &RecordAnswer) -> bool {
    matches!(answer.status, Status::Ok | Status::NotFound)
}

fn answer_is_problem(answer: &RecordAnswer) -> bool {
    matches!(
        answer.status,
        Status::Unsupported | Status::Stale | Status::Failed
    )
}

fn is_address_record(record: &ResolutionRecordKey) -> bool {
    record.record_family == "addr"
}

fn is_primary_address_record(record: &ResolutionRecordKey) -> bool {
    record.record_key == "addr:60"
}

fn is_text_record(record: &ResolutionRecordKey) -> bool {
    matches!(record.record_family.as_str(), "text" | "avatar")
}

fn is_content_hash_record(record: &ResolutionRecordKey) -> bool {
    record.record_key == "contenthash"
}
