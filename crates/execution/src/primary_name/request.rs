use anyhow::Result;
use bigname_storage::{ExecutionOutcome, ExecutionTrace};

use crate::persistence::PersistEnsVerifiedPrimaryNameRequest;

use super::ValidatedVerifiedPrimaryName;
use super::outcome::validate_verified_primary_outcome;
use super::payload::{
    extract_verified_primary_name_section, extract_verified_primary_tuple,
    validate_verified_primary_cache_identity,
};
use super::trace::validate_verified_primary_trace;

pub(crate) fn validate_verified_primary_request(
    request: &PersistEnsVerifiedPrimaryNameRequest,
) -> Result<ValidatedVerifiedPrimaryName> {
    let tuple = extract_verified_primary_tuple(&request.trace)?;
    let verified_primary_name = extract_verified_primary_name_section(
        request.outcome.outcome_payload.as_ref(),
        "verified-primary outcome_payload",
        &tuple.namespace,
    )?;
    validate_verified_primary_trace(
        &request.trace,
        &request.outcome,
        &tuple,
        &verified_primary_name,
    )?;
    validate_verified_primary_outcome(
        &request.outcome,
        &request.trace,
        &tuple,
        &verified_primary_name,
    )?;
    validate_verified_primary_cache_identity(&request.trace, &request.outcome, &tuple)?;

    Ok(ValidatedVerifiedPrimaryName {
        tuple,
        verified_primary_name,
    })
}

pub(crate) fn validate_verified_primary_trace_and_outcome(
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
) -> Result<ValidatedVerifiedPrimaryName> {
    let tuple = extract_verified_primary_tuple(trace)?;
    let verified_primary_name = extract_verified_primary_name_section(
        outcome.outcome_payload.as_ref(),
        "verified-primary outcome_payload",
        &tuple.namespace,
    )?;
    validate_verified_primary_trace(trace, outcome, &tuple, &verified_primary_name)?;
    validate_verified_primary_outcome(outcome, trace, &tuple, &verified_primary_name)?;
    validate_verified_primary_cache_identity(trace, outcome, &tuple)?;

    Ok(ValidatedVerifiedPrimaryName {
        tuple,
        verified_primary_name,
    })
}
