use std::{collections::BTreeMap, str::FromStr};

use alloy_primitives::{Address, B256, U256, hex};
use anyhow::{Context, Result, bail};
use serde_json::Value;

use crate::provider::{ProviderLog, ProviderReceipt};

use super::query::CoinbaseSqlFilterPack;

const ABI_WORD_BYTES: usize = 32;
const BASENAMES_NAME_REGISTERED_SIGNATURE: &str = "NameRegistered(string,bytes32,address,uint256)";
const BASENAMES_NAME_RENEWED_SIGNATURE: &str = "NameRenewed(string,bytes32,uint256)";
const NAME_FOR_ADDR_CHANGED_SIGNATURE: &str = "NameForAddrChanged(address,string)";
const TRANSFER_SIGNATURE: &str = "Transfer(address,address,uint256)";
const REGISTRY_NEW_OWNER_SIGNATURE: &str = "NewOwner(bytes32,bytes32,address)";
const REGISTRY_TRANSFER_SIGNATURE: &str = "Transfer(bytes32,address)";
const REGISTRY_NEW_RESOLVER_SIGNATURE: &str = "NewResolver(bytes32,address)";
const REGISTRY_NEW_TTL_SIGNATURE: &str = "NewTTL(bytes32,uint64)";
const RESOLVER_ADDR_CHANGED_SIGNATURE: &str = "AddrChanged(bytes32,address)";
const RESOLVER_ADDRESS_CHANGED_SIGNATURE: &str = "AddressChanged(bytes32,uint256,bytes)";
const RESOLVER_NAME_CHANGED_SIGNATURE: &str = "NameChanged(bytes32,string)";
const RESOLVER_TEXT_CHANGED_SIGNATURE: &str = "TextChanged(bytes32,string,string,string)";
const RESOLVER_VERSION_CHANGED_SIGNATURE: &str = "VersionChanged(bytes32,uint64)";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CoinbaseSqlLogRow {
    pub(super) block_number: i64,
    pub(super) block_hash: String,
    pub(super) transaction_hash: String,
    pub(super) transaction_index: i64,
    pub(super) log_index: i64,
    pub(super) emitting_address: String,
    pub(super) event_signature: Option<String>,
    pub(super) topics: Vec<String>,
    pub(super) data: String,
    pub(super) requires_validation_provider_data: bool,
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

        let event_signature = optional_string(object, "event_signature")?;
        let parameters = optional_object(object, "parameters")?;
        let data = optional_string(object, "data")?
            .map(|data| normalize_bytes_hex(&data, "data"))
            .transpose()?;
        let log_data =
            coinbase_sql_log_data(data, event_signature.as_deref(), parameters.as_ref())?;

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
            event_signature,
            topics: topics
                .into_iter()
                .map(|topic| normalize_hash(&topic, "topic"))
                .collect::<Result<Vec<_>>>()?,
            data: log_data.data,
            requires_validation_provider_data: log_data.requires_validation_provider_data,
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
        resolved_by_number: Option<&BTreeMap<i64, String>>,
    ) -> Result<()> {
        if self.block_number < pack.from_block || self.block_number > pack.to_block {
            bail!(
                "Coinbase SQL returned block {} outside requested filter pack {}..={}",
                self.block_number,
                pack.from_block,
                pack.to_block
            );
        }
        if let Some(resolved_by_number) = resolved_by_number {
            self.validate_against_resolved_blocks(resolved_by_number)?;
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

    pub(super) fn validate_against_resolved_blocks(
        &self,
        resolved_by_number: &BTreeMap<i64, String>,
    ) -> Result<()> {
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct CoinbaseSqlLogData {
    data: String,
    requires_validation_provider_data: bool,
}

fn coinbase_sql_log_data(
    data: Option<String>,
    event_signature: Option<&str>,
    parameters: Option<&serde_json::Map<String, Value>>,
) -> Result<CoinbaseSqlLogData> {
    if let Some(data) = data {
        return Ok(CoinbaseSqlLogData {
            data,
            requires_validation_provider_data: false,
        });
    }

    match event_signature {
        Some(BASENAMES_NAME_REGISTERED_SIGNATURE) | Some(BASENAMES_NAME_RENEWED_SIGNATURE) => {
            let signature = event_signature.expect("matched event signature must be present");
            let parameters = parameters
                .with_context(|| format!("Coinbase SQL {signature} row is missing parameters"))?;
            let name = required_parameter_string(parameters, "name", signature)?;
            let expires = required_parameter_u256(parameters, "expires", signature)?;
            Ok(CoinbaseSqlLogData {
                data: abi_encode_string_u256(&name, expires),
                requires_validation_provider_data: false,
            })
        }
        Some(REGISTRY_NEW_OWNER_SIGNATURE)
        | Some(REGISTRY_TRANSFER_SIGNATURE)
        | Some(REGISTRY_NEW_RESOLVER_SIGNATURE)
        | Some(RESOLVER_ADDR_CHANGED_SIGNATURE) => {
            let signature = event_signature.expect("matched event signature must be present");
            let parameters = parameters
                .with_context(|| format!("Coinbase SQL {signature} row is missing parameters"))?;
            let field = match signature {
                REGISTRY_NEW_RESOLVER_SIGNATURE => "resolver",
                RESOLVER_ADDR_CHANGED_SIGNATURE => "a",
                _ => "owner",
            };
            Ok(CoinbaseSqlLogData {
                data: abi_encode_address(&required_parameter_string(
                    parameters, field, signature,
                )?)?,
                requires_validation_provider_data: false,
            })
        }
        Some(REGISTRY_NEW_TTL_SIGNATURE) | Some(RESOLVER_VERSION_CHANGED_SIGNATURE) => {
            let signature = event_signature.expect("matched event signature must be present");
            let parameters = parameters
                .with_context(|| format!("Coinbase SQL {signature} row is missing parameters"))?;
            let field = if signature == REGISTRY_NEW_TTL_SIGNATURE {
                "ttl"
            } else {
                "newVersion"
            };
            Ok(CoinbaseSqlLogData {
                data: abi_encode_u256(required_parameter_u256(parameters, field, signature)?),
                requires_validation_provider_data: false,
            })
        }
        Some(RESOLVER_ADDRESS_CHANGED_SIGNATURE) => {
            let signature = event_signature.expect("matched event signature must be present");
            let Some(parameters) = parameters else {
                return Ok(validation_provider_log_data());
            };
            let decoded_payload = (|| {
                let coin_type = required_parameter_u256(parameters, "coinType", signature)?;
                let new_address = required_parameter_bytes(parameters, "newAddress", signature)?;
                Ok::<_, anyhow::Error>(abi_encode_u256_bytes(coin_type, &new_address))
            })();
            match decoded_payload {
                Ok(data) => Ok(CoinbaseSqlLogData {
                    data,
                    requires_validation_provider_data: false,
                }),
                Err(_) => Ok(validation_provider_log_data()),
            }
        }
        Some(RESOLVER_NAME_CHANGED_SIGNATURE) | Some(NAME_FOR_ADDR_CHANGED_SIGNATURE) => {
            let signature = event_signature.expect("matched event signature must be present");
            let parameters = parameters
                .with_context(|| format!("Coinbase SQL {signature} row is missing parameters"))?;
            let name = required_parameter_string(parameters, "name", signature)?;
            Ok(CoinbaseSqlLogData {
                data: abi_encode_string(&name),
                requires_validation_provider_data: false,
            })
        }
        Some(RESOLVER_TEXT_CHANGED_SIGNATURE) => {
            let signature = event_signature.expect("matched event signature must be present");
            let parameters = parameters
                .with_context(|| format!("Coinbase SQL {signature} row is missing parameters"))?;
            let key = required_parameter_string(parameters, "key", signature)?;
            let value = required_parameter_string(parameters, "value", signature)?;
            Ok(CoinbaseSqlLogData {
                data: abi_encode_string_string(&key, &value),
                requires_validation_provider_data: false,
            })
        }
        Some(TRANSFER_SIGNATURE) => Ok(CoinbaseSqlLogData {
            data: "0x".to_owned(),
            requires_validation_provider_data: false,
        }),
        _ => Ok(validation_provider_log_data()),
    }
}

fn validation_provider_log_data() -> CoinbaseSqlLogData {
    CoinbaseSqlLogData {
        data: "0x".to_owned(),
        requires_validation_provider_data: true,
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

fn optional_object(
    object: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<Option<serde_json::Map<String, Value>>> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    match value {
        Value::Null => Ok(None),
        Value::Object(value) => Ok(Some(value.clone())),
        Value::String(value) if value.trim().is_empty() => Ok(None),
        Value::String(value) => match serde_json::from_str::<Value>(value)
            .with_context(|| format!("Coinbase SQL field {field} must be a JSON object string"))?
        {
            Value::Null => Ok(None),
            Value::Object(value) => Ok(Some(value)),
            other => bail!("Coinbase SQL field {field} must be an object, got {other}"),
        },
        other => bail!("Coinbase SQL field {field} must be an object, got {other}"),
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

fn required_parameter_string(
    parameters: &serde_json::Map<String, Value>,
    field: &str,
    event_signature: &str,
) -> Result<String> {
    let value = parameters.get(field).with_context(|| {
        format!("Coinbase SQL {event_signature} row is missing parameter {field}")
    })?;
    variant_value_to_string(value).with_context(|| {
        format!("Coinbase SQL {event_signature} parameter {field} must be string-like")
    })
}

fn required_parameter_u256(
    parameters: &serde_json::Map<String, Value>,
    field: &str,
    event_signature: &str,
) -> Result<U256> {
    let value = parameters.get(field).with_context(|| {
        format!("Coinbase SQL {event_signature} row is missing parameter {field}")
    })?;
    let value = variant_value_to_string(value).with_context(|| {
        format!("Coinbase SQL {event_signature} parameter {field} must be integer-like")
    })?;
    U256::from_str(&value)
        .with_context(|| format!("failed to parse Coinbase SQL {event_signature} {field} {value}"))
}

fn required_parameter_bytes(
    parameters: &serde_json::Map<String, Value>,
    field: &str,
    event_signature: &str,
) -> Result<Vec<u8>> {
    let value = required_parameter_string(parameters, field, event_signature)?;
    parse_bytes(&value, field)
        .with_context(|| format!("failed to parse Coinbase SQL {event_signature} {field} bytes"))
}

fn variant_value_to_string(value: &Value) -> Result<String> {
    match value {
        Value::Null => bail!("null value"),
        Value::String(value) => Ok(value.clone()),
        Value::Number(value) => Ok(value.to_string()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Object(object) => {
            if let Some(value) = object.get("value") {
                return variant_value_to_string(value);
            }
            if object.len() == 1 {
                let value = object
                    .values()
                    .next()
                    .expect("single-entry object must have one value");
                return variant_value_to_string(value);
            }
            bail!("object variant value has no unambiguous scalar payload")
        }
        Value::Array(_) => bail!("array value"),
    }
}

fn abi_encode_address(value: &str) -> Result<String> {
    Ok(format!("0x{}", hex::encode(abi_word_address(value)?)))
}

fn abi_encode_u256(value: U256) -> String {
    format!("0x{}", hex::encode(abi_word_u256(value)))
}

fn abi_encode_string(value: &str) -> String {
    let mut encoded = Vec::new();
    encoded.extend(abi_word_u256(U256::from(ABI_WORD_BYTES as u64)));
    encoded.extend(abi_encode_string_tail(value));
    format!("0x{}", hex::encode(encoded))
}

fn abi_encode_string_u256(value: &str, uint_value: U256) -> String {
    let mut encoded = Vec::new();
    encoded.extend(abi_word_u256(U256::from((ABI_WORD_BYTES * 2) as u64)));
    encoded.extend(abi_word_u256(uint_value));
    encoded.extend(abi_encode_string_tail(value));
    format!("0x{}", hex::encode(encoded))
}

fn abi_encode_u256_bytes(uint_value: U256, bytes: &[u8]) -> String {
    let mut encoded = Vec::new();
    encoded.extend(abi_word_u256(uint_value));
    encoded.extend(abi_word_u256(U256::from((ABI_WORD_BYTES * 2) as u64)));
    encoded.extend(abi_encode_bytes_tail(bytes));
    format!("0x{}", hex::encode(encoded))
}

fn abi_encode_string_string(first: &str, second: &str) -> String {
    let first_tail = abi_encode_string_tail(first);
    let second_tail = abi_encode_string_tail(second);
    let second_offset = ABI_WORD_BYTES * 2 + first_tail.len();

    let mut encoded = Vec::new();
    encoded.extend(abi_word_u256(U256::from((ABI_WORD_BYTES * 2) as u64)));
    encoded.extend(abi_word_u256(U256::from(second_offset as u64)));
    encoded.extend(first_tail);
    encoded.extend(second_tail);
    format!("0x{}", hex::encode(encoded))
}

fn abi_encode_string_tail(value: &str) -> Vec<u8> {
    abi_encode_bytes_tail(value.as_bytes())
}

fn abi_encode_bytes_tail(bytes: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend(abi_word_u256(U256::from(bytes.len() as u64)));
    encoded.extend(bytes);
    let padding = (ABI_WORD_BYTES - (bytes.len() % ABI_WORD_BYTES)) % ABI_WORD_BYTES;
    encoded.extend(std::iter::repeat_n(0u8, padding));
    encoded
}

fn abi_word_u256(value: U256) -> [u8; ABI_WORD_BYTES] {
    value.to_be_bytes::<ABI_WORD_BYTES>()
}

fn abi_word_address(value: &str) -> Result<[u8; ABI_WORD_BYTES]> {
    let normalized = normalize_address(value)?;
    let address_bytes = hex::decode(normalized.trim_start_matches("0x"))
        .with_context(|| format!("failed to parse Coinbase SQL address {normalized}"))?;
    let mut word = [0u8; ABI_WORD_BYTES];
    word[ABI_WORD_BYTES - address_bytes.len()..].copy_from_slice(&address_bytes);
    Ok(word)
}
