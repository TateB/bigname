use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::v2::{Page, RegistrationStatus, Relation, Resolver, Status};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct LookupRequest {
    pub(super) profile: Option<String>,
    pub(super) namespace: Option<String>,
    pub(super) inputs: Vec<LookupInput>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(untagged)]
pub(super) enum LookupInput {
    Name(LookupNameInput),
    Address(LookupAddressInput),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct LookupNameInput {
    pub(super) id: Option<String>,
    pub(super) name: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct LookupAddressInput {
    pub(super) id: Option<String>,
    pub(super) address: String,
    pub(super) coin_type: Option<u64>,
    pub(super) relation: Option<String>,
    pub(super) page_size: Option<u64>,
    pub(super) cursor: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum LookupKind {
    Name,
    Address,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct LookupResult {
    pub(super) input: LookupResultInput,
    pub(super) kind: LookupKind,
    pub(super) status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) unsupported_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) failure_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) normalization: Option<NormalizationInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) record: Option<LookupRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) records: Option<Vec<LookupRecord>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) page: Option<Page>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct LookupResultInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) coin_type: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) relation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) page_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct NormalizationInfo {
    pub(super) changed: bool,
    pub(super) input_name: String,
    pub(super) reason: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct LookupRecord {
    pub(crate) name: String,
    pub(crate) display_name: String,
    pub(crate) namespace: String,
    pub(crate) namehash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) registration_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) token_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) manager: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) registrant: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) registered_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) expires_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) registration_status: Option<RegistrationStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) resolver: Option<Resolver>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) addresses: Option<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) text_records: Option<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) content_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) primary_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) primary_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) chain_id: Option<u64>,
    pub(crate) network: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) is_primary: Option<bool>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) relations: Vec<Relation>,
    pub(crate) status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) unsupported_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) failure_reason: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) unsupported_fields: Vec<String>,
}
