use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use bigname_storage::{
    ExecutionOutcome, ExecutionTrace,
    SupportedVerifiedResolutionRecordKey as SupportedVerifiedRecordKey,
    parse_supported_verified_resolution_record_key,
};
use serde_json::Value;

use crate::json_helpers::{
    ensure_absent, optional_nonempty_string_field, required_array, required_coin_type_field,
    required_nonempty_string_field, required_object, required_string,
};
use crate::validation::{RequestedSelectorSet, VerifiedQueryStatus, VerifiedQuerySummary};

pub(crate) fn extract_requested_selectors(trace: &ExecutionTrace) -> Result<RequestedSelectorSet> {
    let request_metadata = required_object(
        Some(&trace.request_metadata),
        "ENS direct-path verified resolution trace.request_metadata",
    )?;
    let surface = required_string(
        request_metadata,
        "surface",
        "ENS direct-path verified resolution trace.request_metadata",
    )?
    .to_owned();

    let ordered_record_keys = match (
        request_metadata.get("record_keys"),
        request_metadata.get("record_key"),
    ) {
        (Some(record_keys), Some(record_key)) => {
            let parsed_record_keys = parse_requested_record_keys(
                record_keys,
                "ENS direct-path verified resolution trace.request_metadata.record_keys",
            )?;
            let singular_record_key = record_key
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .context(
                    "ENS direct-path verified resolution trace.request_metadata must include non-empty string field record_key",
                )?;
            if parsed_record_keys.len() != 1 || parsed_record_keys[0] != singular_record_key {
                bail!(
                    "ENS direct-path verified resolution trace.request_metadata.record_key must match record_keys when both are present"
                );
            }
            parsed_record_keys
        }
        (Some(record_keys), None) => parse_requested_record_keys(
            record_keys,
            "ENS direct-path verified resolution trace.request_metadata.record_keys",
        )?,
        (None, Some(_)) => vec![
            required_string(
                request_metadata,
                "record_key",
                "ENS direct-path verified resolution trace.request_metadata",
            )?
            .to_owned(),
        ],
        (None, None) => bail!(
            "ENS direct-path verified resolution trace.request_metadata must include record_key or record_keys"
        ),
    };

    validate_ordered_record_keys(
        &ordered_record_keys,
        "ENS direct-path verified resolution trace.request_metadata",
    )?;

    let binding_kind = optional_nonempty_string_field(
        request_metadata,
        "binding_kind",
        "ENS direct-path verified resolution trace.request_metadata",
    )?;

    Ok(RequestedSelectorSet {
        surface,
        ordered_record_keys,
        binding_kind,
    })
}

fn parse_requested_record_keys(value: &Value, context: &str) -> Result<Vec<String>> {
    let items = required_array(Some(value), context)?;
    if items.is_empty() {
        bail!("{context} must include at least one selector");
    }

    let mut record_keys = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        record_keys.push(
            item.as_str()
                .filter(|value| !value.trim().is_empty())
                .with_context(|| format!("{context}[{index}] must be a non-empty string"))?
                .to_owned(),
        );
    }
    Ok(record_keys)
}

fn validate_ordered_record_keys(record_keys: &[String], context: &str) -> Result<()> {
    if record_keys.is_empty() {
        bail!("{context} must include at least one selector");
    }

    let mut seen = BTreeSet::new();
    for record_key in record_keys {
        let selector = parse_canonical_supported_record_key(record_key, context)?;
        let canonical_record_key = canonical_supported_record_key(record_key, &selector);
        if !seen.insert(canonical_record_key.clone()) {
            bail!("{context} must not contain duplicate selectors ({canonical_record_key})");
        }
    }

    Ok(())
}

pub(crate) fn extract_supported_verified_queries(
    outcome: &ExecutionOutcome,
) -> Result<Vec<VerifiedQuerySummary>> {
    let outcome_payload = outcome
        .outcome_payload
        .as_ref()
        .context("ENS direct-path verified resolution outcome must set outcome_payload")?;
    extract_verified_queries_from_payload(
        outcome_payload,
        "ENS direct-path verified resolution outcome_payload",
    )
}

pub(super) fn extract_verified_queries_from_payload(
    payload: &Value,
    context: &str,
) -> Result<Vec<VerifiedQuerySummary>> {
    let payload = required_object(Some(payload), context)?;
    let verified_queries = required_array(
        payload.get("verified_queries"),
        &format!("{context}.verified_queries"),
    )?;
    if verified_queries.is_empty() {
        bail!("{context} must include at least one verified query");
    }

    let mut queries = Vec::with_capacity(verified_queries.len());
    let mut seen_record_keys = BTreeSet::new();
    for (index, query) in verified_queries.iter().enumerate() {
        let query_context = format!("{context}.verified_queries[{index}]");
        let query = required_object(Some(query), &query_context)?;

        let record_key = required_string(query, "record_key", &query_context)?.to_owned();
        let selector = parse_canonical_supported_record_key(&record_key, &query_context)?;
        let canonical_record_key = canonical_supported_record_key(&record_key, &selector);
        if !seen_record_keys.insert(canonical_record_key.clone()) {
            bail!(
                "{context}.verified_queries must not contain duplicate selectors ({canonical_record_key})"
            );
        }
        let (status, value, failure_reason) = match required_string(
            query,
            "status",
            &query_context,
        )? {
            "success" => {
                let value = required_object(query.get("value"), &format!("{query_context}.value"))?;
                ensure_absent(query, "unsupported_reason", &query_context)?;
                if let SupportedVerifiedRecordKey::Addr { coin_type } = &selector {
                    let value_coin_type = required_coin_type_field(
                        value,
                        "coin_type",
                        &format!("{query_context}.value"),
                    )?;
                    if value_coin_type != *coin_type {
                        bail!(
                            "ENS direct-path verified resolution query value coin_type {} does not match record_key {}",
                            value_coin_type,
                            record_key
                        );
                    }
                }
                let resolved_value = required_nonempty_string_field(
                    value,
                    "value",
                    &format!("{query_context}.value"),
                )?;
                if query.contains_key("failure_reason") {
                    bail!(
                        "ENS direct-path verified resolution success query must not set failure_reason"
                    );
                }
                (VerifiedQueryStatus::Success, Some(resolved_value), None)
            }
            "not_found" => {
                ensure_absent(query, "value", &query_context)?;
                ensure_absent(query, "unsupported_reason", &query_context)?;
                let failure_reason =
                    optional_nonempty_string_field(query, "failure_reason", &query_context)?;
                (VerifiedQueryStatus::NotFound, None, failure_reason)
            }
            "unsupported" => {
                ensure_absent(query, "value", &query_context)?;
                ensure_absent(query, "failure_reason", &query_context)?;
                let unsupported_reason =
                    required_nonempty_string_field(query, "unsupported_reason", &query_context)?;
                (
                    VerifiedQueryStatus::Unsupported,
                    None,
                    Some(unsupported_reason),
                )
            }
            "execution_failed" => {
                ensure_absent(query, "value", &query_context)?;
                ensure_absent(query, "unsupported_reason", &query_context)?;
                let failure_reason =
                    required_nonempty_string_field(query, "failure_reason", &query_context)?;
                (
                    VerifiedQueryStatus::ExecutionFailed,
                    None,
                    Some(failure_reason),
                )
            }
            status => bail!(
                "ENS direct-path verified resolution only supports success, not_found, unsupported, and execution_failed selector results; found {status}"
            ),
        };

        queries.push(VerifiedQuerySummary {
            record_key,
            selector,
            status,
            value,
            failure_reason,
        });
    }

    Ok(queries)
}

pub(super) fn ensure_requested_selectors_match_queries(
    requested_selectors: &RequestedSelectorSet,
    queries: &[VerifiedQuerySummary],
) -> Result<()> {
    if requested_selectors.ordered_record_keys.len() != queries.len() {
        bail!(
            "ENS direct-path verified resolution trace.request_metadata selectors {} do not match outcome verified query count {}",
            requested_selectors.ordered_record_keys.len(),
            queries.len()
        );
    }

    for (index, (requested_record_key, query)) in requested_selectors
        .ordered_record_keys
        .iter()
        .zip(queries.iter())
        .enumerate()
    {
        if requested_record_key != &query.record_key {
            bail!(
                "ENS direct-path verified resolution trace.request_metadata.record_keys[{index}] {} does not match outcome verified_queries[{index}] {}",
                requested_record_key,
                query.record_key
            );
        }
    }

    Ok(())
}

fn parse_supported_verified_record_key(record_key: &str) -> Result<SupportedVerifiedRecordKey> {
    parse_supported_verified_resolution_record_key(record_key)
}

fn parse_canonical_supported_record_key(
    record_key: &str,
    context: &str,
) -> Result<SupportedVerifiedRecordKey> {
    let selector = parse_supported_verified_record_key(record_key)?;
    let canonical_record_key = canonical_supported_record_key(record_key, &selector);
    if canonical_record_key != record_key {
        bail!("{context} selector {record_key} must use canonical selector {canonical_record_key}");
    }
    Ok(selector)
}

fn canonical_supported_record_key(
    record_key: &str,
    selector: &SupportedVerifiedRecordKey,
) -> String {
    match selector {
        SupportedVerifiedRecordKey::Addr { coin_type } => format!("addr:{coin_type}"),
        SupportedVerifiedRecordKey::Avatar => "avatar".to_owned(),
        SupportedVerifiedRecordKey::Contenthash => "contenthash".to_owned(),
        SupportedVerifiedRecordKey::Text => record_key.to_owned(),
    }
}
