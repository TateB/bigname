use serde_json::Value;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct PrimaryNameTupleKey {
    pub(super) address: String,
    pub(super) namespace: String,
    pub(super) coin_type: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ReverseClaimTuple {
    pub(super) key: PrimaryNameTupleKey,
    pub(super) claim_provenance: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct NameClaimObservation {
    pub(super) key: PrimaryNameTupleKey,
    pub(super) raw_name: Option<String>,
    pub(super) primary_claim_source: Value,
}
