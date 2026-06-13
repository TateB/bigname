use serde_json::Value;

mod anchor;
mod context;
mod keys;
mod name_ref;
mod outcome;
mod payload;
mod readback;
mod request;
mod trace;

pub(crate) use anchor::{
    ensure_primary_name_anchor_matches, ensure_primary_name_anchor_matches_in_transaction,
};
pub(crate) use context::verified_primary_context_label;
pub(crate) use name_ref::validate_verified_primary_name_ref;
pub(crate) use readback::extract_verified_primary_readback_provenance;
pub(crate) use request::{
    validate_verified_primary_request, validate_verified_primary_trace_and_outcome,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VerifiedPrimaryNameStatus {
    Success,
    NotFound,
    Mismatch,
    InvalidName,
    ExecutionFailed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VerifiedPrimaryNameTuple {
    pub(crate) namespace: String,
    pub(crate) normalized_address: String,
    pub(crate) coin_type: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VerifiedPrimaryNameSection {
    pub(crate) section: Value,
    pub(crate) status: VerifiedPrimaryNameStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedVerifiedPrimaryName {
    pub(crate) tuple: VerifiedPrimaryNameTuple,
    pub(crate) verified_primary_name: VerifiedPrimaryNameSection,
}

pub(crate) fn normalized_verified_primary_name_request_key(
    namespace: &str,
    normalized_address: &str,
    coin_type: &str,
) -> String {
    keys::normalized_verified_primary_name_request_key(namespace, normalized_address, coin_type)
}
