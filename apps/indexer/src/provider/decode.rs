use std::str::FromStr;

use alloy_primitives::{Address, B256, Bytes, U256, hex, keccak256};
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::Value;

use super::{
    ProviderBlock, ProviderBlockBundle, ProviderLog, ProviderReceipt, ProviderTransaction,
    ZERO_HASH,
};

impl ProviderBlock {
    pub(super) fn from_value(value: Value) -> Result<Self> {
        Self::from_rpc_block(decode_rpc_value(value, "block")?)
    }

    fn from_rpc_block(block: RpcBlock) -> Result<Self> {
        Ok(Self {
            block_hash: normalize_hash(&block.hash),
            parent_hash: normalize_parent_hash(&block.parent_hash),
            block_number: u256_i64(block.number, "block number")?,
            block_timestamp_unix_secs: u256_i64(block.timestamp, "block timestamp")?,
            logs_bloom: block.logs_bloom.map(|bytes| bytes.to_vec()),
            transactions_root: block.transactions_root.map(|value| normalize_hash(&value)),
            receipts_root: block.receipts_root.map(|value| normalize_hash(&value)),
            state_root: block.state_root.map(|value| normalize_hash(&value)),
        })
    }
}

impl ProviderBlockBundle {
    pub(super) fn from_value(value: Value) -> Result<Self> {
        let mut rpc_block = decode_rpc_value::<RpcBlock>(value, "block")?;
        let transactions = rpc_block
            .transactions
            .take()
            .context("missing transactions in JSON-RPC result")?
            .into_iter()
            .map(|transaction| {
                ProviderTransaction::from_rpc_transaction(transaction.into_transaction()?)
            })
            .collect::<Result<Vec<_>>>()?;
        let block = ProviderBlock::from_rpc_block(rpc_block)?;

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
        Self::from_rpc_transaction(decode_rpc_ref(value, "transaction")?)
    }

    fn from_rpc_transaction(transaction: RpcTransaction) -> Result<Self> {
        Ok(Self {
            transaction_hash: hash_hex(transaction.hash),
            block_hash: hash_hex(transaction.block_hash),
            block_number: u256_i64(transaction.block_number, "transaction block number")?,
            transaction_index: u256_i64(transaction.transaction_index, "transaction index")?,
            from: address_hex(transaction.from),
            to: transaction.to.map(address_hex),
        })
    }
}

impl ProviderReceipt {
    pub(super) fn from_value(value: &Value) -> Result<Self> {
        let receipt = decode_rpc_ref::<RpcReceipt>(value, "receipt")?;

        Ok(Self {
            transaction_hash: hash_hex(receipt.transaction_hash),
            block_hash: hash_hex(receipt.block_hash),
            block_number: u256_i64(receipt.block_number, "receipt block number")?,
            transaction_index: u256_i64(receipt.transaction_index, "receipt transaction index")?,
            contract_address: receipt.contract_address.map(address_hex),
            status: optional_u256_i64(receipt.status, "receipt status")?,
            cumulative_gas_used: optional_u256_i64(
                receipt.cumulative_gas_used,
                "receipt cumulative gas used",
            )?,
            gas_used: optional_u256_i64(receipt.gas_used, "receipt gas used")?,
            logs_bloom: receipt.logs_bloom.map(|bytes| bytes.to_vec()),
        })
    }
}

impl ProviderLog {
    pub(super) fn from_value(
        value: &Value,
        block_hash: &str,
        expected_block_number: i64,
    ) -> Result<Self> {
        let log = decode_rpc_ref::<RpcLog>(value, "log")?;
        let log_block_hash = hash_hex(log.block_hash);
        let block_number = u256_i64(log.block_number, "log block number")?;
        let log_index = u256_i64(log.log_index, "log index")?;

        if log_block_hash != block_hash {
            bail!(
                "provider returned log {} for block {} with mismatched block hash {}",
                log_index,
                block_hash,
                log_block_hash
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
            block_hash: log_block_hash,
            block_number,
            transaction_hash: hash_hex(log.transaction_hash),
            transaction_index: u256_i64(log.transaction_index, "log transaction index")?,
            log_index,
            address: address_hex(log.address),
            topics: log.topics.into_iter().map(hash_hex).collect(),
            data: bytes_hex(log.data.as_ref()),
        })
    }

    pub(super) fn block_number_from_value(value: &Value) -> Result<i64> {
        let log = decode_rpc_ref::<RpcLogBlockNumber>(value, "log")?;
        u256_i64(log.block_number, "log block number")
    }
}

pub(super) fn block_hash_from_value(value: &Value) -> Result<String> {
    let block = decode_rpc_ref::<RpcBlockHash>(value, "block")?;
    Ok(normalize_hash(&block.hash))
}

#[cfg(test)]
pub(super) fn parse_hex_i64(value: &str) -> Result<i64> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    i64::from_str_radix(value, 16).with_context(|| format!("failed to parse hex integer {value}"))
}

pub(super) fn parse_hex_bytes(value: &str) -> Result<Vec<u8>> {
    parse_rpc_bytes(value).map(|bytes| bytes.to_vec())
}

fn parse_rpc_bytes(value: &str) -> Result<Bytes> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if !value.len().is_multiple_of(2) {
        bail!("invalid hex byte string with odd length");
    }

    let bytes = hex::decode(value).with_context(|| format!("failed to parse hex bytes {value}"))?;
    Ok(Bytes::from(bytes))
}

pub(super) fn keccak256_hex(bytes: &[u8]) -> String {
    hash_hex(keccak256(bytes))
}

pub(super) fn bytes_hex(bytes: &[u8]) -> String {
    hex::encode_prefixed(bytes)
}

pub(super) fn normalize_hash(value: &str) -> String {
    value.to_ascii_lowercase()
}

pub(super) fn normalize_address(value: &str) -> String {
    value.to_ascii_lowercase()
}

pub(super) fn parse_b256(value: &str, label: &str) -> Result<B256> {
    let value = normalize_hash(value);
    if value.is_empty() {
        bail!("{label} cannot be empty");
    }

    B256::from_str(&value).with_context(|| format!("failed to parse {label} {value}"))
}

pub(super) fn parse_address(value: &str) -> Result<Address> {
    let value = normalize_address(value);
    if value.is_empty() {
        bail!("address cannot be empty");
    }

    Address::from_str(&value).with_context(|| format!("failed to parse address {value}"))
}

pub(super) fn hash_hex_from_str(value: &str, label: &str) -> Result<String> {
    parse_b256(value, label).map(hash_hex)
}

pub(super) fn address_hex_from_str(value: &str) -> Result<String> {
    parse_address(value).map(address_hex)
}

fn decode_rpc_value<T>(value: Value, label: &'static str) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(value)
        .with_context(|| format!("failed to decode {label} JSON-RPC result"))
}

fn decode_rpc_ref<T>(value: &Value, label: &'static str) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    decode_rpc_value(value.clone(), label)
}

pub(super) fn hash_hex(value: B256) -> String {
    format!("{value}")
}

pub(super) fn address_hex(value: Address) -> String {
    format!("{value:#x}")
}

fn normalize_parent_hash(value: &str) -> Option<String> {
    let value = normalize_hash(value);
    if value == ZERO_HASH || value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn optional_u256_i64(value: Option<U256>, label: &'static str) -> Result<Option<i64>> {
    value.map(|value| u256_i64(value, label)).transpose()
}

fn u256_i64(value: U256, label: &'static str) -> Result<i64> {
    let value = u64::try_from(value).with_context(|| format!("{label} exceeds u64"))?;
    i64::try_from(value).with_context(|| format!("{label} exceeds i64"))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcBlock {
    hash: String,
    parent_hash: String,
    number: U256,
    timestamp: U256,
    #[serde(default)]
    logs_bloom: Option<Bytes>,
    #[serde(default)]
    transactions_root: Option<String>,
    #[serde(default)]
    receipts_root: Option<String>,
    #[serde(default)]
    state_root: Option<String>,
    #[serde(default)]
    transactions: Option<Vec<RpcBlockTransaction>>,
}

#[derive(Debug, Deserialize)]
struct RpcBlockHash {
    hash: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RpcBlockTransaction {
    Hash(B256),
    Full(RpcTransaction),
}

impl RpcBlockTransaction {
    fn into_transaction(self) -> Result<RpcTransaction> {
        match self {
            Self::Full(transaction) => Ok(transaction),
            Self::Hash(hash) => {
                bail!(
                    "expected full transaction object in block bundle response, got transaction hash {}",
                    hash_hex(hash)
                )
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcTransaction {
    hash: B256,
    block_hash: B256,
    block_number: U256,
    transaction_index: U256,
    from: Address,
    to: Option<Address>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcReceipt {
    transaction_hash: B256,
    block_hash: B256,
    block_number: U256,
    transaction_index: U256,
    #[serde(default)]
    contract_address: Option<Address>,
    #[serde(default)]
    status: Option<U256>,
    #[serde(default)]
    cumulative_gas_used: Option<U256>,
    #[serde(default)]
    gas_used: Option<U256>,
    #[serde(default)]
    logs_bloom: Option<Bytes>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcLog {
    block_hash: B256,
    block_number: U256,
    transaction_hash: B256,
    transaction_index: U256,
    log_index: U256,
    address: Address,
    topics: Vec<B256>,
    data: Bytes,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcLogBlockNumber {
    block_number: U256,
}
