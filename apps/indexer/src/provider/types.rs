use anyhow::{Result, bail};
use serde_json::{Value, json};

use super::decode::normalize_hash;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderBlockBundle {
    pub block: ProviderBlock,
    pub transactions: Vec<ProviderTransaction>,
    pub logs: Vec<ProviderLog>,
    pub receipts: Vec<ProviderReceipt>,
    pub raw_payloads: Vec<ProviderRawPayloadCacheMetadata>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderResolvedBlock {
    pub block_number: i64,
    pub block_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderBlockCodeObservationRequest {
    pub block_hash: String,
    pub addresses: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderTransactionReceiptRequest {
    pub transaction_hash: String,
    pub block_hash: String,
    pub block_number: i64,
    pub transaction_index: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderTransactionReceiptBundle {
    pub transaction: ProviderTransaction,
    pub receipt: ProviderReceipt,
}

#[allow(dead_code, reason = "staged for exact block log fetch callers")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderBlockLogRequest {
    pub block_number: i64,
    pub block_hash: String,
    pub addresses: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderBlockCodeObservations {
    pub block_hash: String,
    pub observations: Vec<ProviderCodeObservation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderRawPayloadCacheMetadata {
    pub payload_kind: String,
    pub digest_algorithm: String,
    pub retained_digest: String,
    pub payload_size_bytes: i64,
    pub cache_metadata: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderTransaction {
    pub transaction_hash: String,
    pub block_hash: String,
    pub block_number: i64,
    pub transaction_index: i64,
    pub from: String,
    pub to: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderReceipt {
    pub transaction_hash: String,
    pub block_hash: String,
    pub block_number: i64,
    pub transaction_index: i64,
    pub contract_address: Option<String>,
    pub status: Option<i64>,
    pub cumulative_gas_used: Option<i64>,
    pub gas_used: Option<i64>,
    pub logs_bloom: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderLog {
    pub block_hash: String,
    pub block_number: i64,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub log_index: i64,
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderBlockTag {
    Latest,
    Safe,
    Finalized,
}

impl ProviderBlockTag {
    fn as_json_rpc_tag(self) -> &'static str {
        match self {
            Self::Latest => "latest",
            Self::Safe => "safe",
            Self::Finalized => "finalized",
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderBlockSelection {
    Number(i64),
    Hash(String),
    Tag(ProviderBlockTag),
}

impl ProviderBlockSelection {
    pub(super) fn json_rpc_parameter(self) -> Result<Value> {
        match self {
            Self::Number(number) => {
                if number < 0 {
                    bail!("provider block selection number cannot be negative: {number}");
                }

                Ok(Value::String(format!("0x{number:x}")))
            }
            Self::Hash(block_hash) => {
                let block_hash = normalize_hash(&block_hash);
                if block_hash.is_empty() {
                    bail!("provider block selection hash cannot be empty");
                }

                Ok(json!({ "blockHash": block_hash }))
            }
            Self::Tag(tag) => Ok(Value::String(tag.as_json_rpc_tag().to_owned())),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderCodeObservation {
    pub address: String,
    pub code: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ProviderHeadHashSnapshot {
    pub(super) canonical: String,
    pub(super) safe: Option<String>,
    pub(super) finalized: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderHeadSnapshot {
    pub canonical: ProviderBlock,
    pub safe: Option<ProviderBlock>,
    pub finalized: Option<ProviderBlock>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderBlock {
    pub block_hash: String,
    pub parent_hash: Option<String>,
    pub block_number: i64,
    pub block_timestamp_unix_secs: i64,
    pub logs_bloom: Option<Vec<u8>>,
    pub transactions_root: Option<String>,
    pub receipts_root: Option<String>,
    pub state_root: Option<String>,
}
