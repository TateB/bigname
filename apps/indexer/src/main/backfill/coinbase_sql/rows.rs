use std::{collections::BTreeMap, str::FromStr};

use alloy_primitives::{Address, B256, hex};
use anyhow::{Context, Result, bail};
use serde_json::Value;

use crate::provider::{ProviderLog, ProviderReceipt};

use super::query::CoinbaseSqlFilterPack;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CoinbaseSqlLogRow {
    pub(super) block_number: i64,
    pub(super) block_hash: String,
    pub(super) transaction_hash: String,
    pub(super) transaction_index: i64,
    pub(super) log_index: i64,
    pub(super) emitting_address: String,
    pub(super) topics: Vec<String>,
    pub(super) data: String,
    pub(super) receipt_status: Option<i64>,
    pub(super) receipt_contract_address: Option<String>,
    pub(super) receipt_gas_used: Option<i64>,
    pub(super) receipt_cumulative_gas_used: Option<i64>,
    pub(super) receipt_logs_bloom: Option<Vec<u8>>,
}

impl CoinbaseSqlLogRow {
    pub(super) fn from_value(value: Value) -> Result<Self> {
        let object = value
            .as_object()
            .context("Coinbase SQL result row must be a JSON object")?;
        let topics = if let Some(topics) = object.get("topics") {
            parse_string_array(topics, "topics")?
        } else {
            ["topic0", "topic1", "topic2", "topic3"]
                .into_iter()
                .filter_map(|field| optional_string(object, field).transpose())
                .collect::<Result<Vec<_>>>()?
        };

        Ok(Self {
            block_number: required_i64(object, "block_number")?,
            block_hash: normalize_hash(&required_string(object, "block_hash")?, "block_hash")?,
            transaction_hash: normalize_hash(
                &required_string(object, "transaction_hash")?,
                "transaction_hash",
            )?,
            transaction_index: required_i64(object, "transaction_index")?,
            log_index: required_i64(object, "log_index")?,
            emitting_address: normalize_address(
                &required_string(object, "emitting_address")
                    .or_else(|_| required_string(object, "address"))?,
            )?,
            topics: topics
                .into_iter()
                .map(|topic| normalize_hash(&topic, "topic"))
                .collect::<Result<Vec<_>>>()?,
            data: optional_string(object, "data")?
                .map(|data| normalize_bytes_hex(&data, "data"))
                .transpose()?
                .unwrap_or_else(|| "0x".to_owned()),
            receipt_status: optional_i64(object, "receipt_status")?,
            receipt_contract_address: optional_normalized_address(
                object,
                "receipt_contract_address",
            )?,
            receipt_gas_used: optional_i64(object, "receipt_gas_used")?,
            receipt_cumulative_gas_used: optional_i64(object, "receipt_cumulative_gas_used")?,
            receipt_logs_bloom: optional_bytes(object, "receipt_logs_bloom")?,
        })
    }

    pub(super) fn validate_against_filter_pack(
        &self,
        pack: &CoinbaseSqlFilterPack,
        resolved_by_number: &BTreeMap<i64, String>,
    ) -> Result<()> {
        if self.block_number < pack.from_block || self.block_number > pack.to_block {
            bail!(
                "Coinbase SQL returned block {} outside requested filter pack {}..={}",
                self.block_number,
                pack.from_block,
                pack.to_block
            );
        }
        let expected_hash = resolved_by_number
            .get(&self.block_number)
            .with_context(|| {
                format!(
                    "Coinbase SQL returned block {} that was not resolved by validation provider",
                    self.block_number
                )
            })?;
        if !self.block_hash.eq_ignore_ascii_case(expected_hash) {
            bail!(
                "Coinbase SQL returned block {} hash {}, validation provider resolved {}",
                self.block_number,
                self.block_hash,
                expected_hash
            );
        }
        if self.transaction_index < 0 {
            bail!(
                "Coinbase SQL returned negative transaction index {}",
                self.transaction_index
            );
        }
        if self.log_index < 0 {
            bail!(
                "Coinbase SQL returned negative log index {}",
                self.log_index
            );
        }
        if !pack.scan_all_emitters
            && !pack
                .addresses
                .iter()
                .any(|address| address.eq_ignore_ascii_case(&self.emitting_address))
        {
            bail!(
                "Coinbase SQL returned address {} outside requested filter pack",
                self.emitting_address
            );
        }
        if let (Some(topic0), false) = (self.topics.first(), pack.topic0s.is_empty())
            && !topic0.starts_with("0x")
        {
            bail!("Coinbase SQL returned malformed topic0 {topic0}");
        }
        if !pack.topic0s.is_empty()
            && !self
                .topics
                .first()
                .is_some_and(|topic0| pack.topic0s.iter().any(|topic| topic == topic0))
        {
            bail!("Coinbase SQL returned topic0 outside requested filter pack");
        }

        Ok(())
    }

    pub(super) fn to_provider_log(&self) -> Result<ProviderLog> {
        Ok(ProviderLog {
            block_hash: self.block_hash.clone(),
            block_number: self.block_number,
            transaction_hash: self.transaction_hash.clone(),
            transaction_index: self.transaction_index,
            log_index: self.log_index,
            address: self.emitting_address.clone(),
            topics: self.topics.clone(),
            data: self.data.clone(),
        })
    }

    #[allow(
        dead_code,
        reason = "Coinbase receipt fields are optional in the public schema"
    )]
    pub(super) fn to_provider_receipt(&self) -> Option<ProviderReceipt> {
        Some(ProviderReceipt {
            transaction_hash: self.transaction_hash.clone(),
            block_hash: self.block_hash.clone(),
            block_number: self.block_number,
            transaction_index: self.transaction_index,
            contract_address: self.receipt_contract_address.clone(),
            status: self.receipt_status,
            cumulative_gas_used: self.receipt_cumulative_gas_used,
            gas_used: self.receipt_gas_used,
            logs_bloom: self.receipt_logs_bloom.clone(),
        })
    }
}

fn required_string(object: &serde_json::Map<String, Value>, field: &str) -> Result<String> {
    optional_string(object, field)?.with_context(|| format!("missing Coinbase SQL field {field}"))
}

fn optional_string(object: &serde_json::Map<String, Value>, field: &str) -> Result<Option<String>> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    match value {
        Value::Null => Ok(None),
        Value::String(value) if value.trim().is_empty() => Ok(None),
        Value::String(value) => Ok(Some(value.clone())),
        Value::Number(number) => Ok(Some(number.to_string())),
        other => bail!("Coinbase SQL field {field} must be string-like, got {other}"),
    }
}

fn required_i64(object: &serde_json::Map<String, Value>, field: &str) -> Result<i64> {
    optional_i64(object, field)?.with_context(|| format!("missing Coinbase SQL field {field}"))
}

fn optional_i64(object: &serde_json::Map<String, Value>, field: &str) -> Result<Option<i64>> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    match value {
        Value::Null => Ok(None),
        Value::Number(number) => number
            .as_i64()
            .with_context(|| format!("Coinbase SQL field {field} exceeds i64"))
            .map(Some),
        Value::String(value) if value.trim().is_empty() => Ok(None),
        Value::String(value) => value
            .parse::<i64>()
            .with_context(|| format!("failed to parse Coinbase SQL field {field} value {value}"))
            .map(Some),
        other => bail!("Coinbase SQL field {field} must be integer-like, got {other}"),
    }
}

fn parse_string_array(value: &Value, field: &str) -> Result<Vec<String>> {
    let values = value
        .as_array()
        .with_context(|| format!("Coinbase SQL field {field} must be an array"))?;
    values
        .iter()
        .map(|value| match value {
            Value::String(value) => Ok(value.clone()),
            other => bail!("Coinbase SQL field {field} item must be a string, got {other}"),
        })
        .collect()
}

fn optional_normalized_address(
    object: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<Option<String>> {
    optional_string(object, field)?
        .map(|value| normalize_address(&value))
        .transpose()
}

fn optional_bytes(object: &serde_json::Map<String, Value>, field: &str) -> Result<Option<Vec<u8>>> {
    optional_string(object, field)?
        .map(|value| parse_bytes(&value, field))
        .transpose()
}

fn normalize_hash(value: &str, label: &str) -> Result<String> {
    let value = value.to_ascii_lowercase();
    B256::from_str(&value)
        .with_context(|| format!("failed to parse Coinbase SQL {label} {value}"))?;
    Ok(value)
}

fn normalize_address(value: &str) -> Result<String> {
    let value = value.to_ascii_lowercase();
    Address::from_str(&value)
        .with_context(|| format!("failed to parse Coinbase SQL address {value}"))?;
    Ok(value)
}

fn normalize_bytes_hex(value: &str, label: &str) -> Result<String> {
    parse_bytes(value, label)?;
    let value = value.to_ascii_lowercase();
    if value.starts_with("0x") {
        Ok(value)
    } else {
        Ok(format!("0x{value}"))
    }
}

fn parse_bytes(value: &str, label: &str) -> Result<Vec<u8>> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if !value.len().is_multiple_of(2) {
        bail!("Coinbase SQL {label} hex string has odd length");
    }
    hex::decode(value).with_context(|| format!("failed to parse Coinbase SQL {label} hex bytes"))
}
