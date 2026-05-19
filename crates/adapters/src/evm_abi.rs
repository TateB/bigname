use std::str::FromStr;

use alloy_primitives::{Address, B256, LogData, U256, hex, keccak256};
use alloy_sol_types::SolEvent;
use anyhow::{Context, Result, bail};

const ABI_WORD_BYTES: usize = 32;

pub(crate) fn decode_event_log<E>(
    topics: &[String],
    data: &[u8],
    context: &'static str,
) -> Result<E>
where
    E: SolEvent,
{
    let log_data = alloy_log_data(topics, data)?;
    E::decode_log_data_validate(&log_data).context(context)
}

pub(crate) fn address_hex(address: Address) -> String {
    hex_string(address.as_slice())
}

pub(crate) fn address_hex_from_word(word: &[u8]) -> Result<String> {
    let word = exact_word(word)?;
    let address = Address::from_slice(&word[12..]);
    Ok(format!("0x{}", hex::encode(address.as_slice())))
}

pub(crate) fn topic_address_hex(value: &str) -> Result<String> {
    address_hex_from_word(&hex_32(value)?)
}

pub(crate) fn u256_decimal(value: U256) -> String {
    value.to_string()
}

pub(crate) fn u256_i64(value: U256, label: &str) -> Result<i64> {
    let value = u64::try_from(value).with_context(|| format!("{label} exceeds u64"))?;
    i64::try_from(value).with_context(|| format!("{label} exceeds i64"))
}

pub(crate) fn u256_word_hex(value: U256) -> String {
    hex_string(value.to_be_bytes::<ABI_WORD_BYTES>())
}

pub(crate) fn hex_32(value: &str) -> Result<[u8; ABI_WORD_BYTES]> {
    let normalized = normalize_hex_32(value)?;
    let mut output = [0u8; ABI_WORD_BYTES];
    hex::decode_to_slice(&normalized[2..], &mut output)
        .with_context(|| format!("invalid 32-byte hex value {normalized}"))?;
    Ok(output)
}

pub(crate) fn normalize_hex_32(value: &str) -> Result<String> {
    let normalized = value.to_ascii_lowercase();
    let normalized = if normalized.starts_with("0x") {
        normalized
    } else {
        format!("0x{normalized}")
    };
    if normalized.len() != 66 {
        bail!("expected 32-byte hex value, got {normalized}");
    }
    Ok(normalized)
}

pub(crate) fn keccak_signature_hex(signature: &str) -> String {
    keccak256_hex(signature.as_bytes())
}

pub(crate) fn keccak256_hex(bytes: &[u8]) -> String {
    hex_string(keccak256_bytes(bytes))
}

pub(crate) fn keccak256_bytes(bytes: &[u8]) -> [u8; ABI_WORD_BYTES] {
    let digest = keccak256(bytes);
    let mut output = [0u8; ABI_WORD_BYTES];
    output.copy_from_slice(digest.as_slice());
    output
}

pub(crate) fn namehash_hex(labels: &[Vec<u8>]) -> String {
    hex_string(namehash_bytes(labels))
}

pub(crate) fn child_namehash_hex(parent_node: &str, labelhash: &str) -> Result<String> {
    let mut bytes = [0u8; ABI_WORD_BYTES * 2];
    bytes[..ABI_WORD_BYTES].copy_from_slice(&hex_32(parent_node)?);
    bytes[ABI_WORD_BYTES..].copy_from_slice(&hex_32(labelhash)?);
    Ok(keccak256_hex(&bytes))
}

pub(crate) fn hex_string(bytes: impl AsRef<[u8]>) -> String {
    format!("0x{}", hex_string_without_prefix(bytes))
}

pub(crate) fn hex_string_without_prefix(bytes: impl AsRef<[u8]>) -> String {
    hex::encode(bytes)
}

fn alloy_log_data(topics: &[String], data: &[u8]) -> Result<LogData> {
    let topics = topics
        .iter()
        .map(|topic| {
            let normalized = normalize_hex_32(topic)?;
            B256::from_str(&normalized).with_context(|| format!("invalid EVM log topic {topic}"))
        })
        .collect::<Result<Vec<_>>>()?;
    LogData::new(topics, data.to_vec().into()).context("EVM log has more than four topics")
}

fn exact_word(word: &[u8]) -> Result<&[u8; ABI_WORD_BYTES]> {
    if word.len() != ABI_WORD_BYTES {
        bail!("ABI word must be exactly 32 bytes");
    }
    word.try_into().context("ABI word must be exactly 32 bytes")
}

pub(crate) fn namehash_bytes(labels: &[Vec<u8>]) -> [u8; ABI_WORD_BYTES] {
    let mut node = [0u8; ABI_WORD_BYTES];
    for label in labels.iter().rev() {
        let mut combined = [0u8; ABI_WORD_BYTES * 2];
        combined[..ABI_WORD_BYTES].copy_from_slice(&node);
        combined[ABI_WORD_BYTES..].copy_from_slice(&keccak256_bytes(label));
        node = keccak256_bytes(&combined);
    }
    node
}
