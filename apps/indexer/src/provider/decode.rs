use alloy_primitives::{hex, keccak256};
use anyhow::{Context, Result, bail};
use serde_json::{Map, Value};

use super::{
    ProviderBlock, ProviderBlockBundle, ProviderLog, ProviderReceipt, ProviderTransaction,
    ZERO_HASH,
};

impl ProviderBlock {
    pub(super) fn from_value(value: Value) -> Result<Self> {
        let block_hash = block_hash_from_value(&value)?;
        let object = rpc_object(&value, "block")?;
        let parent_hash = normalize_parent_hash(required_str(object, "parentHash", "parent hash")?);
        let block_number = required_hex_i64(object, "number", "block number")?;
        let block_timestamp_unix_secs = required_hex_i64(object, "timestamp", "block timestamp")?;

        Ok(Self {
            block_hash,
            parent_hash,
            block_number,
            block_timestamp_unix_secs,
            logs_bloom: optional_hex_bytes(object, "logsBloom")?,
            transactions_root: optional_normalized_hash(object, "transactionsRoot"),
            receipts_root: optional_normalized_hash(object, "receiptsRoot"),
            state_root: optional_normalized_hash(object, "stateRoot"),
        })
    }
}

impl ProviderBlockBundle {
    pub(super) fn from_value(value: Value) -> Result<Self> {
        let block = ProviderBlock::from_value(value.clone())?;
        let object = rpc_object(&value, "block")?;
        let transactions = required_array(object, "transactions", "transactions")?
            .iter()
            .map(ProviderTransaction::from_value)
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            block,
            transactions,
            logs: Vec::new(),
            receipts: Vec::new(),
            raw_payloads: Vec::new(),
        })
    }
}

impl ProviderTransaction {
    pub(super) fn from_value(value: &Value) -> Result<Self> {
        let object = rpc_object(value, "transaction")?;
        let transaction_hash = required_str(object, "hash", "transaction hash")?;
        let block_hash = required_str(object, "blockHash", "transaction block hash")?;
        let block_number = required_hex_i64(object, "blockNumber", "transaction block number")?;
        let transaction_index = required_hex_i64(object, "transactionIndex", "transaction index")?;
        let from = required_str(object, "from", "transaction from address")?;

        Ok(Self {
            transaction_hash: normalize_hash(transaction_hash),
            block_hash: normalize_hash(block_hash),
            block_number,
            transaction_index,
            from: normalize_address(from),
            to: optional_normalized_address(object, "to"),
        })
    }
}

impl ProviderReceipt {
    pub(super) fn from_value(value: &Value) -> Result<Self> {
        let object = rpc_object(value, "receipt")?;
        let transaction_hash = required_str(object, "transactionHash", "receipt transaction hash")?;
        let block_hash = required_str(object, "blockHash", "receipt block hash")?;
        let block_number = required_hex_i64(object, "blockNumber", "receipt block number")?;
        let transaction_index =
            required_hex_i64(object, "transactionIndex", "receipt transaction index")?;

        Ok(Self {
            transaction_hash: normalize_hash(transaction_hash),
            block_hash: normalize_hash(block_hash),
            block_number,
            transaction_index,
            contract_address: optional_normalized_address(object, "contractAddress"),
            status: optional_hex_i64(object, "status")?,
            cumulative_gas_used: optional_hex_i64(object, "cumulativeGasUsed")?,
            gas_used: optional_hex_i64(object, "gasUsed")?,
            logs_bloom: optional_hex_bytes(object, "logsBloom")?,
        })
    }
}

impl ProviderLog {
    pub(super) fn from_value(
        value: &Value,
        block_hash: &str,
        expected_block_number: i64,
    ) -> Result<Self> {
        let object = rpc_object(value, "log")?;
        let log_block_hash = required_str(object, "blockHash", "log block hash")?;
        let block_number = required_hex_i64(object, "blockNumber", "log block number")?;
        let transaction_hash = required_str(object, "transactionHash", "log transaction hash")?;
        let transaction_index =
            required_hex_i64(object, "transactionIndex", "log transaction index")?;
        let log_index = required_hex_i64(object, "logIndex", "log index")?;
        let address = required_str(object, "address", "log address")?;
        let topics = required_array(object, "topics", "log topics")?
            .iter()
            .map(|topic| {
                topic
                    .as_str()
                    .context("expected log topic string in JSON-RPC result")
                    .map(normalize_hash)
            })
            .collect::<Result<Vec<_>>>()?;
        let data = required_str(object, "data", "log data")?;

        if normalize_hash(log_block_hash) != block_hash {
            bail!(
                "provider returned log {} for block {} with mismatched block hash {}",
                log_index,
                block_hash,
                normalize_hash(log_block_hash)
            );
        }
        if block_number != expected_block_number {
            bail!(
                "provider returned log {} for block {} with mismatched block number {}",
                log_index,
                block_hash,
                block_number
            );
        }

        Ok(Self {
            block_hash: normalize_hash(log_block_hash),
            block_number,
            transaction_hash: normalize_hash(transaction_hash),
            transaction_index,
            log_index,
            address: normalize_address(address),
            topics,
            data: data.to_owned(),
        })
    }

    pub(super) fn block_number_from_value(value: &Value) -> Result<i64> {
        let object = rpc_object(value, "log")?;
        required_hex_i64(object, "blockNumber", "log block number")
    }
}

pub(super) fn block_hash_from_value(value: &Value) -> Result<String> {
    let object = rpc_object(value, "block")?;
    let block_hash = required_str(object, "hash", "block hash")?;

    Ok(normalize_hash(block_hash))
}

fn rpc_object<'a>(value: &'a Value, label: &'static str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .with_context(|| format!("expected {label} object in JSON-RPC result"))
}

fn required_str<'a>(
    object: &'a Map<String, Value>,
    field: &str,
    label: &'static str,
) -> Result<&'a str> {
    object
        .get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("missing {label} in JSON-RPC result"))
}

fn required_array<'a>(
    object: &'a Map<String, Value>,
    field: &str,
    label: &'static str,
) -> Result<&'a Vec<Value>> {
    object
        .get(field)
        .and_then(Value::as_array)
        .with_context(|| format!("missing {label} in JSON-RPC result"))
}

fn required_hex_i64(object: &Map<String, Value>, field: &str, label: &'static str) -> Result<i64> {
    parse_hex_i64(required_str(object, field, label)?)
}

fn optional_hex_i64(object: &Map<String, Value>, field: &str) -> Result<Option<i64>> {
    object
        .get(field)
        .and_then(Value::as_str)
        .map(parse_hex_i64)
        .transpose()
}

fn optional_hex_bytes(object: &Map<String, Value>, field: &str) -> Result<Option<Vec<u8>>> {
    object
        .get(field)
        .and_then(Value::as_str)
        .map(parse_hex_bytes)
        .transpose()
}

fn optional_normalized_hash(object: &Map<String, Value>, field: &str) -> Option<String> {
    object
        .get(field)
        .and_then(Value::as_str)
        .map(normalize_hash)
}

fn optional_normalized_address(object: &Map<String, Value>, field: &str) -> Option<String> {
    object
        .get(field)
        .and_then(Value::as_str)
        .map(normalize_address)
}

pub(super) fn parse_hex_i64(value: &str) -> Result<i64> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    i64::from_str_radix(value, 16).with_context(|| format!("failed to parse hex integer {value}"))
}

pub(super) fn parse_hex_bytes(value: &str) -> Result<Vec<u8>> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if !value.len().is_multiple_of(2) {
        bail!("invalid hex byte string with odd length");
    }

    hex::decode(value).with_context(|| format!("failed to parse hex bytes {value}"))
}

pub(super) fn keccak256_hex(bytes: &[u8]) -> String {
    hex_string(keccak256(bytes).as_slice())
}

fn hex_string(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

pub(super) fn normalize_hash(value: &str) -> String {
    value.to_ascii_lowercase()
}

fn normalize_parent_hash(value: &str) -> Option<String> {
    let value = normalize_hash(value);
    if value == ZERO_HASH || value.is_empty() {
        None
    } else {
        Some(value)
    }
}

pub(super) fn normalize_address(value: &str) -> String {
    value.to_ascii_lowercase()
}
