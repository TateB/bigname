use alloy_rpc_types_eth::BlockNumberOrTag;
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use super::decode::{address_hex_from_str, hash_hex_from_str, normalize_hash};

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
    fn as_block_number_or_tag(self) -> BlockNumberOrTag {
        match self {
            Self::Latest => BlockNumberOrTag::Latest,
            Self::Safe => BlockNumberOrTag::Safe,
            Self::Finalized => BlockNumberOrTag::Finalized,
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

                serde_json::to_value(BlockNumberOrTag::Number(number as u64))
                    .context("failed to encode provider block selection number")
            }
            Self::Hash(block_hash) => {
                let block_hash = normalize_hash(&block_hash);
                if block_hash.is_empty() {
                    bail!("provider block selection hash cannot be empty");
                }
                let block_hash = hash_hex_from_str(&block_hash, "provider block selection hash")?;

                Ok(json!({ "blockHash": block_hash }))
            }
            Self::Tag(tag) => serde_json::to_value(tag.as_block_number_or_tag())
                .context("failed to encode provider block selection tag"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ProviderLogFilter {
    block: ProviderLogFilterBlock,
    addresses: Vec<String>,
    topic0s: Vec<String>,
}

impl ProviderLogFilter {
    pub(super) fn block_hash(block_hash: &str) -> Result<Self> {
        Ok(Self {
            block: ProviderLogFilterBlock::Hash(hash_hex_from_str(
                block_hash,
                "provider log filter block hash",
            )?),
            addresses: Vec::new(),
            topic0s: Vec::new(),
        })
    }

    pub(super) fn block_range(from_block: i64, to_block: i64) -> Self {
        Self {
            block: ProviderLogFilterBlock::Range {
                from_block,
                to_block,
            },
            addresses: Vec::new(),
            topic0s: Vec::new(),
        }
    }

    pub(super) fn with_addresses(mut self, addresses: &[String]) -> Result<Self> {
        self.addresses = addresses
            .iter()
            .map(|address| address_hex_from_str(address))
            .collect::<Result<Vec<_>>>()?;
        Ok(self)
    }

    pub(super) fn with_topic0s(mut self, topic0s: &[String]) -> Result<Self> {
        self.topic0s = topic0s
            .iter()
            .map(|topic| hash_hex_from_str(topic, "provider log filter topic0"))
            .collect::<Result<Vec<_>>>()?;
        Ok(self)
    }

    pub(super) fn json_rpc_parameter(&self) -> Result<Value> {
        let mut filter = serde_json::Map::new();
        match &self.block {
            ProviderLogFilterBlock::Hash(block_hash) => {
                filter.insert("blockHash".to_owned(), Value::String(block_hash.clone()));
            }
            ProviderLogFilterBlock::Range {
                from_block,
                to_block,
            } => {
                filter.insert(
                    "fromBlock".to_owned(),
                    ProviderBlockSelection::Number(*from_block).json_rpc_parameter()?,
                );
                filter.insert(
                    "toBlock".to_owned(),
                    ProviderBlockSelection::Number(*to_block).json_rpc_parameter()?,
                );
            }
        }
        if !self.addresses.is_empty() {
            filter.insert(
                "address".to_owned(),
                Value::Array(self.addresses.iter().cloned().map(Value::String).collect()),
            );
        }
        if !self.topic0s.is_empty() {
            filter.insert(
                "topics".to_owned(),
                Value::Array(vec![Value::Array(
                    self.topic0s.iter().cloned().map(Value::String).collect(),
                )]),
            );
        }

        Ok(Value::Object(filter))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ProviderLogFilterBlock {
    Hash(String),
    Range { from_block: i64, to_block: i64 },
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
