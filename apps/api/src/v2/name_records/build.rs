use std::collections::BTreeMap;

use bigname_storage::{NameCurrentRow, RecordInventoryCurrentRow};
use serde_json::Value;
use tracing::error;

use crate::{ResolutionRecordKey, build_resolution_verified_state};

use super::super::{
    PRODUCT_PIPELINE_TERMS, Source, Status, V2Error, V2Result, contains_boundary_vocabulary,
    name_record::{
        record_addresses, record_content_hash, record_text_records, record_value_string, resolver,
        string_field, value_to_string,
    },
    name_records_inventory::{
        inventory_item_for_record, inventory_summary, unsupported_family_reason,
    },
};
use super::{NameRecords, RecordAnswer, VerifiedRecordLookup};

const INDEXED_INVENTORY_UNAVAILABLE_REASON: &str = "inventory_not_available";
pub(crate) const VERIFIED_NOT_SUPPORTED_REASON: &str = "verified_records_not_supported";

pub(crate) fn build_indexed_name_records(
    row: &NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
    requested_records: Option<&[ResolutionRecordKey]>,
    include_inventory: bool,
) -> V2Result<NameRecords> {
    let record_answers = requested_records
        .map(|records| {
            records
                .iter()
                .map(|record| {
                    Ok((
                        record.record_key.clone(),
                        indexed_record_answer(record_inventory, record)?,
                    ))
                })
                .collect::<V2Result<BTreeMap<_, _>>>()
        })
        .transpose()?;
    let values = match requested_records {
        Some(records) => RecordValues::from_answers(
            records,
            record_answers
                .as_ref()
                .expect("requested indexed records must build an answer map"),
        ),
        None => RecordValues {
            addresses: record_addresses(record_inventory),
            text_records: record_text_records(record_inventory),
            content_hash: record_content_hash(record_inventory),
        },
    };

    Ok(NameRecords {
        namespace: row.namespace.clone(),
        resolver: resolver(&row.declared_summary),
        addresses: values.addresses,
        text_records: values.text_records,
        content_hash: values.content_hash,
        records: record_answers,
        inventory: include_inventory
            .then(|| inventory_summary(record_inventory, requested_records)),
    })
}

pub(crate) fn indexed_records_requiring_verified_fallback(
    row: &NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
    requested_records: &[ResolutionRecordKey],
) -> V2Result<Vec<ResolutionRecordKey>> {
    let mut fallback_records = Vec::new();
    for record in requested_records {
        if indexed_satisfying_record_answer(row, record_inventory, record)?.is_none() {
            fallback_records.push(record.clone());
        }
    }
    Ok(fallback_records)
}

pub(crate) fn build_auto_name_records(
    row: &NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
    requested_records: &[ResolutionRecordKey],
    verified_lookup: Option<VerifiedRecordLookup>,
    include_inventory: bool,
) -> V2Result<(Source, NameRecords)> {
    let mut fallback_records = Vec::new();
    let mut answers = BTreeMap::new();

    for record in requested_records {
        if let Some(answer) = indexed_satisfying_record_answer(row, record_inventory, record)? {
            answers.insert(record.record_key.clone(), answer);
        } else {
            fallback_records.push(record.clone());
        }
    }

    let source = if fallback_records.is_empty() {
        Source::Indexed
    } else {
        let verified_answers = verified_record_answers(row, &fallback_records, verified_lookup)?;
        for record in &fallback_records {
            let answer = match verified_answers.get(&record.record_key).cloned() {
                Some(answer) => answer,
                None => unsupported_answer(VERIFIED_NOT_SUPPORTED_REASON)?,
            };
            answers.insert(record.record_key.clone(), answer);
        }
        Source::Verified
    };
    let values = RecordValues::from_answers(requested_records, &answers);

    Ok((
        source,
        NameRecords {
            namespace: row.namespace.clone(),
            resolver: resolver(&row.declared_summary),
            addresses: values.addresses,
            text_records: values.text_records,
            content_hash: values.content_hash,
            records: Some(answers),
            inventory: include_inventory
                .then(|| inventory_summary(record_inventory, Some(requested_records))),
        },
    ))
}

pub(crate) fn build_verified_name_records(
    row: &NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
    requested_records: Option<&[ResolutionRecordKey]>,
    verified_lookup: Option<VerifiedRecordLookup>,
    include_inventory: bool,
) -> V2Result<NameRecords> {
    let records = requested_records
        .map(|records| verified_record_answers(row, records, verified_lookup))
        .transpose()?;
    let values = requested_records
        .zip(records.as_ref())
        .map(|(records, answers)| RecordValues::from_answers(records, answers))
        .unwrap_or_default();

    Ok(NameRecords {
        namespace: row.namespace.clone(),
        resolver: resolver(&row.declared_summary),
        addresses: values.addresses,
        text_records: values.text_records,
        content_hash: values.content_hash,
        records,
        inventory: include_inventory
            .then(|| inventory_summary(record_inventory, requested_records)),
    })
}

#[derive(Default)]
struct RecordValues {
    addresses: BTreeMap<String, String>,
    text_records: BTreeMap<String, String>,
    content_hash: Option<String>,
}

impl RecordValues {
    fn from_answers(
        records: &[ResolutionRecordKey],
        answers: &BTreeMap<String, RecordAnswer>,
    ) -> Self {
        let mut values = Self::default();
        for record in records {
            let Some(value) = answers
                .get(&record.record_key)
                .filter(|answer| answer.status == Status::Ok)
                .and_then(|answer| answer.value.as_ref())
                .and_then(value_to_string)
            else {
                continue;
            };

            match record.record_family.as_str() {
                "addr" => {
                    if let Some(coin_type) = record.selector_key.clone() {
                        values.addresses.insert(coin_type, value);
                    }
                }
                "text" => {
                    if let Some(key) = record.selector_key.clone() {
                        values.text_records.insert(key, value);
                    }
                }
                "avatar" => {
                    values.text_records.insert("avatar".to_owned(), value);
                }
                "contenthash" => {
                    values.content_hash = Some(value);
                }
                _ => {}
            }
        }
        values
    }
}

fn indexed_record_answer(
    record_inventory: Option<&RecordInventoryCurrentRow>,
    record: &ResolutionRecordKey,
) -> V2Result<RecordAnswer> {
    let Some(record_inventory) = record_inventory else {
        return unsupported_answer(INDEXED_INVENTORY_UNAVAILABLE_REASON);
    };

    if let Some(entry) = indexed_entry_for_record(record_inventory, record) {
        let answer = answer_from_inventory_entry(entry)?;
        if record.record_key != "avatar" || answer.status == Status::Ok {
            return Ok(answer);
        }
    }

    if record.record_key == "avatar" {
        let text_avatar = ResolutionRecordKey {
            record_key: "text:avatar".to_owned(),
            record_family: "text".to_owned(),
            selector_key: Some("avatar".to_owned()),
        };
        if let Some(entry) = indexed_entry_for_record(record_inventory, &text_avatar) {
            let answer = answer_from_inventory_entry(entry)?;
            if answer.status == Status::Ok {
                return Ok(answer);
            }
        }
    }

    if let Some(gap) = inventory_item_for_record(&record_inventory.explicit_gaps, record) {
        return Ok(RecordAnswer {
            status: Status::NotFound,
            value: None,
            unsupported_reason: None,
            failure_reason: string_field(gap.get("gap_reason"))
                .map(|reason| product_record_reason(&reason))
                .transpose()?,
        });
    }

    if let Some(reason) = unsupported_family_reason(record_inventory, &record.record_family) {
        return unsupported_answer(&reason);
    }

    not_found_answer(None)
}

fn indexed_satisfying_record_answer(
    row: &NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
    record: &ResolutionRecordKey,
) -> V2Result<Option<RecordAnswer>> {
    if terminal_no_declared_resolver(row) {
        return Ok(Some(not_found_answer(None)?));
    }
    if !indexed_inventory_is_authoritative(record_inventory) {
        return Ok(None);
    }

    let answer = indexed_record_answer(record_inventory, record)?;
    Ok(matches!(answer.status, Status::Ok | Status::NotFound).then_some(answer))
}

fn indexed_entry_for_record<'a>(
    record_inventory: &'a RecordInventoryCurrentRow,
    record: &ResolutionRecordKey,
) -> Option<&'a Value> {
    inventory_item_for_record(&record_inventory.entries, record)
}

fn answer_from_inventory_entry(entry: &Value) -> V2Result<RecordAnswer> {
    let status = string_field(entry.get("status")).unwrap_or_else(|| "unsupported".to_owned());
    match status.as_str() {
        "success" => Ok(RecordAnswer {
            status: Status::Ok,
            value: record_value_string(entry).map(Value::String),
            unsupported_reason: None,
            failure_reason: None,
        }),
        "not_found" => not_found_answer(string_field(entry.get("failure_reason"))),
        "unsupported" => unsupported_answer(
            &string_field(entry.get("unsupported_reason"))
                .unwrap_or_else(|| "record_not_supported".to_owned()),
        ),
        "execution_failed" | "failed" => failed_answer(
            string_field(entry.get("failure_reason"))
                .unwrap_or_else(|| "record_read_failed".to_owned()),
        ),
        _ => failed_answer("record_read_failed"),
    }
}

fn verified_record_answers(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    verified_lookup: Option<VerifiedRecordLookup>,
) -> V2Result<BTreeMap<String, RecordAnswer>> {
    match verified_lookup {
        Some(VerifiedRecordLookup::Found { outcome, .. }) => {
            let state = build_resolution_verified_state(row, records, Some(outcome.as_ref()))
                .map_err(|error| {
                    error!(
                        service = "api",
                        logical_name_id = %row.logical_name_id,
                        error = ?error,
                        "failed to build v2 verified name records"
                    );
                    V2Error::internal_error("failed to build verified name records")
                })?;
            verified_queries_from_state(&state, records)
        }
        Some(VerifiedRecordLookup::Stale(reason)) => {
            let supported = supported_verified_record_keys(row, records);
            records
                .iter()
                .map(|record| {
                    let answer = if supported.contains(&record.record_key) {
                        stale_answer(reason.clone())
                    } else {
                        unsupported_answer(VERIFIED_NOT_SUPPORTED_REASON)
                    }?;
                    Ok((record.record_key.clone(), answer))
                })
                .collect()
        }
        Some(VerifiedRecordLookup::NotSupported) | None => records
            .iter()
            .map(|record| {
                Ok((
                    record.record_key.clone(),
                    unsupported_answer(VERIFIED_NOT_SUPPORTED_REASON)?,
                ))
            })
            .collect(),
    }
}

fn verified_queries_from_state(
    state: &Value,
    records: &[ResolutionRecordKey],
) -> V2Result<BTreeMap<String, RecordAnswer>> {
    let mut queries = BTreeMap::new();
    for query in state
        .get("verified_queries")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(record_key) = string_field(query.get("record_key")) else {
            continue;
        };
        queries.insert(record_key, verified_answer_from_query(query)?);
    }

    records
        .iter()
        .map(|record| {
            let answer = match queries.get(&record.record_key).cloned() {
                Some(answer) => answer,
                None => unsupported_answer(VERIFIED_NOT_SUPPORTED_REASON)?,
            };
            Ok((record.record_key.clone(), answer))
        })
        .collect()
}

fn verified_answer_from_query(query: &Value) -> V2Result<RecordAnswer> {
    let status = string_field(query.get("status")).unwrap_or_else(|| "unsupported".to_owned());
    match status.as_str() {
        "success" => Ok(RecordAnswer {
            status: Status::Ok,
            value: query
                .get("value")
                .and_then(verified_value_string)
                .map(Value::String),
            unsupported_reason: None,
            failure_reason: None,
        }),
        "not_found" => not_found_answer(string_field(query.get("failure_reason"))),
        "unsupported" => unsupported_answer(
            &string_field(query.get("unsupported_reason"))
                .unwrap_or_else(|| VERIFIED_NOT_SUPPORTED_REASON.to_owned()),
        ),
        "execution_failed" | "failed" => failed_answer(
            string_field(query.get("failure_reason"))
                .unwrap_or_else(|| "verified_record_read_failed".to_owned()),
        ),
        _ => failed_answer("verified_record_read_failed"),
    }
}

fn verified_value_string(value: &Value) -> Option<String> {
    value
        .get("value")
        .and_then(value_to_string)
        .or_else(|| value_to_string(value))
}

fn supported_verified_record_keys(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
) -> std::collections::BTreeSet<String> {
    bigname_storage::supported_resolution_verified_readback_records(row, records)
        .into_iter()
        .map(|record| record.record_key)
        .collect()
}

fn indexed_inventory_is_authoritative(
    record_inventory: Option<&RecordInventoryCurrentRow>,
) -> bool {
    let Some(record_inventory) = record_inventory else {
        return false;
    };
    string_field(record_inventory.coverage.get("unsupported_reason")).is_none()
        && string_field(record_inventory.coverage.get("status")).as_deref() == Some("full")
}

fn terminal_no_declared_resolver(row: &NameCurrentRow) -> bool {
    let Some(resolver) = row
        .declared_summary
        .get("resolver")
        .filter(|value| value.is_object())
    else {
        return false;
    };
    if string_field(resolver.get("status")).as_deref() == Some("unsupported") {
        return false;
    }

    string_field(resolver.get("chain_id")).is_none()
        && string_field(resolver.get("address")).is_none()
}

fn not_found_answer(failure_reason: Option<String>) -> V2Result<RecordAnswer> {
    Ok(RecordAnswer {
        status: Status::NotFound,
        value: None,
        unsupported_reason: None,
        failure_reason: failure_reason
            .map(|reason| product_record_reason(&reason))
            .transpose()?,
    })
}

fn unsupported_answer(reason: &str) -> V2Result<RecordAnswer> {
    Ok(RecordAnswer {
        status: Status::Unsupported,
        value: None,
        unsupported_reason: Some(product_record_reason(reason)?),
        failure_reason: None,
    })
}

fn stale_answer(reason: impl Into<String>) -> V2Result<RecordAnswer> {
    let reason = reason.into();
    Ok(RecordAnswer {
        status: Status::Stale,
        value: None,
        unsupported_reason: None,
        failure_reason: Some(product_record_reason(&reason)?),
    })
}

fn failed_answer(reason: impl Into<String>) -> V2Result<RecordAnswer> {
    let reason = reason.into();
    Ok(RecordAnswer {
        status: Status::Failed,
        value: None,
        unsupported_reason: None,
        failure_reason: Some(product_record_reason(&reason)?),
    })
}

fn product_record_reason(reason: &str) -> V2Result<String> {
    match reason {
        "value_not_retained_in_normalized_events" => Ok("value_not_retained".to_owned()),
        "record_family_not_supported_in_phase6_projection" => {
            Ok("record_family_not_supported".to_owned())
        }
        _ if contains_boundary_vocabulary(reason, PRODUCT_PIPELINE_TERMS) => {
            error!(%reason, "rejected record reason containing pipeline vocabulary");
            Err(V2Error::internal_error(
                "failed to map product record reason vocabulary",
            ))
        }
        _ => Ok(reason.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::ErrorCode;

    #[test]
    fn product_record_reason_maps_storage_projection_reasons() {
        assert_eq!(
            product_record_reason("value_not_retained_in_normalized_events")
                .expect("known reason must map"),
            "value_not_retained"
        );
        assert_eq!(
            product_record_reason("record_family_not_supported_in_phase6_projection")
                .expect("known reason must map"),
            "record_family_not_supported"
        );
        assert_eq!(
            product_record_reason("resolver_family_pending").expect("product reason must pass"),
            "resolver_family_pending"
        );
    }

    #[test]
    fn product_record_reason_rejects_unmapped_pipeline_vocabulary() {
        for reason in ["raw_log_missing_record_cache", "record_sidecar_missing"] {
            let error =
                product_record_reason(reason).expect_err("pipeline vocabulary must fail loudly");

            assert_eq!(error.code(), ErrorCode::InternalError);
        }
    }
}
