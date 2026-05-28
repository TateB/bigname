use serde_json::Value;

pub const ENS_NAMESPACE: &str = "ens";
pub const BASENAMES_NAMESPACE: &str = "basenames";
pub const BASE_MAINNET_CHAIN_ID: &str = "base-mainnet";
pub const ETHEREUM_MAINNET_CHAIN_ID: &str = "ethereum-mainnet";
pub const BASENAMES_L1_RESOLVER_ADDRESS: &str = "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31";
pub const ENS_LEGACY_EVENT_SILENT_REVERSE_RESOLVER_ADDRESSES: &[&str] =
    &["0xa2c122be93b0074270ebee7f6b7292c7deb45047"];

pub trait VerifiedResolutionRecord {
    fn record_key(&self) -> &str;
    fn record_family(&self) -> &str;
    fn selector_key(&self) -> Option<&str>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VerifiedResolutionPathClass {
    Direct,
    AliasOnly,
    WildcardDerived,
    BasenamesTransportDirect,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedResolutionSupportBoundary {
    pub path_class: VerifiedResolutionPathClass,
    pub topology_version_boundary: Value,
    pub record_version_boundary: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedResolutionRequestedChainPosition {
    pub chain_id: String,
    pub block_number: i64,
    pub block_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolutionProjectionChainPosition {
    pub(crate) chain_id: String,
    pub(crate) block_number: i64,
    pub(crate) block_hash: String,
    pub(crate) timestamp: String,
}

pub(crate) fn resolution_projection_chain_position_from_value(
    value: &Value,
) -> Option<ResolutionProjectionChainPosition> {
    Some(ResolutionProjectionChainPosition {
        chain_id: json_string_field(json_field(value, "chain_id"))?,
        block_number: json_field(value, "block_number")?.as_i64()?,
        block_hash: json_string_field(json_field(value, "block_hash"))?,
        timestamp: json_string_field(json_field(value, "timestamp"))?,
    })
}

pub(crate) fn array_or_empty(value: Option<&Value>) -> Value {
    value
        .and_then(Value::as_array)
        .map(|items| Value::Array(items.clone()))
        .unwrap_or_else(|| Value::Array(Vec::new()))
}

pub(crate) fn summary_is_unsupported(section: Option<&Value>) -> bool {
    matches!(
        json_string_field(section.and_then(|value| json_field(value, "status"))).as_deref(),
        Some("unsupported")
    ) && json_string_field(section.and_then(|value| json_field(value, "unsupported_reason")))
        .is_some()
}

pub(crate) fn json_field<'a>(value: &'a Value, field_name: &str) -> Option<&'a Value> {
    value.as_object()?.get(field_name)
}

pub(crate) fn json_string_field(value: Option<&Value>) -> Option<String> {
    value?.as_str().map(str::to_owned)
}
