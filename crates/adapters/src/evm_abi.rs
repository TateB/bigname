use alloy_primitives::{Address, U256, hex, keccak256};
use alloy_sol_types::{SolType, abi::TokenSeq};
use anyhow::{Context, Result, bail};

const ABI_WORD_BYTES: usize = 32;

pub(crate) fn abi_decode_params<'de, T>(
    data: &'de [u8],
    context: &'static str,
) -> Result<T::RustType>
where
    T: SolType,
    T::Token<'de>: TokenSeq<'de>,
{
    T::abi_decode_params_validate(data).context(context)
}

pub(crate) fn address_hex(address: Address) -> String {
    hex_string(address.as_slice())
}

pub(crate) fn dynamic_string(data: &[u8], offset_word_index: usize) -> Result<String> {
    String::from_utf8(dynamic_bytes(data, offset_word_index)?)
        .context("dynamic string payload is not valid UTF-8")
}

pub(crate) fn dynamic_bytes(data: &[u8], offset_word_index: usize) -> Result<Vec<u8>> {
    let offset = usize_word(data, offset_word_index).context("invalid ABI dynamic offset")?;
    let length = usize_at(data, offset).context("invalid ABI dynamic length")?;
    let start = offset
        .checked_add(ABI_WORD_BYTES)
        .context("ABI dynamic payload start overflow")?;
    let end = start
        .checked_add(length)
        .context("ABI dynamic payload end overflow")?;
    if data.len() < end {
        bail!("ABI data is shorter than the declared dynamic payload");
    }
    Ok(data[start..end].to_vec())
}

pub(crate) fn address_word_hex(data: &[u8], word_index: usize) -> Result<String> {
    address_hex_from_word(word_at(data, word_index)?)
}

pub(crate) fn address_hex_from_word(word: &[u8]) -> Result<String> {
    let word = exact_word(word)?;
    let address = Address::from_slice(&word[12..]);
    Ok(format!("0x{}", hex::encode(address.as_slice())))
}

pub(crate) fn topic_address_hex(value: &str) -> Result<String> {
    address_hex_from_word(&hex_32(value)?)
}

pub(crate) fn u64_topic(value: &str) -> Result<i64> {
    i64_from_u64_word(&hex_32(value)?)
}

pub(crate) fn i64_from_u64_word(word: &[u8]) -> Result<i64> {
    let word = exact_word(word)?;
    if word[..24].iter().any(|byte| *byte != 0) {
        bail!("u64 ABI word exceeds supported width");
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&word[24..]);
    i64::try_from(u64::from_be_bytes(bytes)).context("u64 ABI word does not fit in i64")
}

pub(crate) fn usize_from_word(word: &[u8]) -> Result<usize> {
    let word = exact_word(word)?;
    if word[..24].iter().any(|byte| *byte != 0) {
        bail!("ABI word exceeds supported usize width");
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&word[24..]);
    usize::try_from(u64::from_be_bytes(bytes)).context("ABI word does not fit in usize")
}

pub(crate) fn u256_decimal(value: U256) -> String {
    value.to_string()
}

pub(crate) fn u256_word_hex(value: U256) -> String {
    hex_string(value.to_be_bytes::<ABI_WORD_BYTES>())
}

pub(crate) fn u256_topic_decimal(value: &str) -> Result<String> {
    Ok(U256::from_be_bytes(hex_32(value)?).to_string())
}

pub(crate) fn word_at(data: &[u8], word_index: usize) -> Result<&[u8; ABI_WORD_BYTES]> {
    let offset = word_index
        .checked_mul(ABI_WORD_BYTES)
        .context("ABI word index overflow")?;
    word_at_offset(data, offset)
}

pub(crate) fn word_at_offset(data: &[u8], offset: usize) -> Result<&[u8; ABI_WORD_BYTES]> {
    let end = offset
        .checked_add(ABI_WORD_BYTES)
        .context("ABI word offset overflow")?;
    let word = data
        .get(offset..end)
        .with_context(|| format!("ABI data missing word at byte offset {offset}"))?;
    exact_word(word)
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

fn usize_word(data: &[u8], word_index: usize) -> Result<usize> {
    usize_from_word(word_at(data, word_index)?)
}

fn usize_at(data: &[u8], offset: usize) -> Result<usize> {
    usize_from_word(word_at_offset(data, offset)?)
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
