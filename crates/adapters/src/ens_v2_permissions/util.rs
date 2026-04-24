use anyhow::{Context, Result, bail};
use bigname_storage::CanonicalityState;
use sha3::{Digest, Keccak256};
use sqlx::types::Uuid;

pub(super) fn resource_is_root(resource: &str) -> bool {
    resource == "0x0000000000000000000000000000000000000000000000000000000000000000"
}

pub(super) fn decode_dynamic_string(data: &[u8], offset_word_index: usize) -> Result<String> {
    String::from_utf8(decode_dynamic_bytes(data, offset_word_index)?)
        .context("dynamic string is not valid UTF-8")
}

pub(super) fn decode_dynamic_bytes(data: &[u8], offset_word_index: usize) -> Result<Vec<u8>> {
    let offset = decode_usize_word(data, offset_word_index)?;
    if data.len() < offset + 32 {
        bail!("dynamic bytes payload is missing length word");
    }
    let length = decode_usize_at(data, offset)?;
    let start = offset + 32;
    let end = start + length;
    if data.len() < end {
        bail!("dynamic bytes payload is shorter than declared length");
    }
    Ok(data[start..end].to_vec())
}

fn decode_usize_word(data: &[u8], word_index: usize) -> Result<usize> {
    let word = word_at(data, word_index)?;
    decode_usize(word)
}

fn decode_usize_at(data: &[u8], offset: usize) -> Result<usize> {
    if data.len() < offset + 32 {
        bail!("ABI word offset is outside payload");
    }
    decode_usize(&data[offset..offset + 32])
}

fn decode_usize(word: &[u8]) -> Result<usize> {
    if word.len() != 32 {
        bail!("ABI word must be exactly 32 bytes");
    }
    if word[..24].iter().any(|byte| *byte != 0) {
        bail!("ABI word exceeds supported usize width");
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&word[24..32]);
    usize::try_from(u64::from_be_bytes(bytes)).context("ABI word does not fit in usize")
}

pub(super) fn word_at(data: &[u8], word_index: usize) -> Result<&[u8]> {
    let start = word_index
        .checked_mul(32)
        .context("ABI word index overflow")?;
    let end = start + 32;
    data.get(start..end)
        .with_context(|| format!("ABI data missing word {word_index}"))
}

pub(super) fn normalize_hex_32(value: &str) -> Result<String> {
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

pub(super) fn normalize_hex_32_word(word: &[u8]) -> Result<String> {
    if word.len() != 32 {
        bail!("ABI word must be exactly 32 bytes");
    }
    Ok(format!("0x{}", hex_string(word)))
}

pub(super) fn decode_hex_32(value: &str) -> Result<[u8; 32]> {
    let normalized = normalize_hex_32(value)?;
    let mut output = [0u8; 32];
    for (index, chunk) in normalized.as_bytes()[2..].chunks(2).enumerate() {
        let hex = std::str::from_utf8(chunk).context("hex chunk must be UTF-8")?;
        output[index] =
            u8::from_str_radix(hex, 16).with_context(|| format!("invalid hex byte {hex}"))?;
    }
    Ok(output)
}

pub(super) fn normalize_topic_address(value: &str) -> Result<String> {
    let normalized = normalize_hex_32(value)?;
    Ok(format!("0x{}", &normalized[26..]))
}

pub(super) fn decode_u256_topic_decimal(value: &str) -> Result<String> {
    let bytes = decode_hex_32(value)?;
    Ok(decimal_string_from_be_bytes(&bytes))
}

pub(super) fn normalize_address(value: &str) -> String {
    value.to_ascii_lowercase()
}

pub(super) fn parse_canonicality_state(value: &str) -> Result<CanonicalityState> {
    match value {
        "observed" => Ok(CanonicalityState::Observed),
        "canonical" => Ok(CanonicalityState::Canonical),
        "safe" => Ok(CanonicalityState::Safe),
        "finalized" => Ok(CanonicalityState::Finalized),
        "orphaned" => Ok(CanonicalityState::Orphaned),
        _ => bail!("unknown canonicality_state value {value}"),
    }
}

pub(super) fn dns_decode(bytes: &[u8]) -> Result<String> {
    let mut labels = Vec::new();
    let mut index = 0usize;
    while index < bytes.len() {
        let length = bytes[index] as usize;
        index += 1;
        if length == 0 {
            if index != bytes.len() {
                bail!("DNS-encoded name has trailing bytes");
            }
            return Ok(labels.join(".").to_ascii_lowercase());
        }
        let end = index + length;
        if end > bytes.len() {
            bail!("DNS-encoded name label exceeds payload length");
        }
        labels.push(
            String::from_utf8(bytes[index..end].to_vec())
                .context("DNS-encoded label is not valid UTF-8")?,
        );
        index = end;
    }
    bail!("DNS-encoded name is missing root label")
}

fn decimal_string_from_be_bytes(bytes: &[u8]) -> String {
    let mut digits = vec![0u8];
    for byte in bytes {
        let mut carry = *byte as u32;
        for digit in digits.iter_mut().rev() {
            let value = (*digit as u32) * 256 + carry;
            *digit = (value % 10) as u8;
            carry = value / 10;
        }
        while carry > 0 {
            digits.insert(0, (carry % 10) as u8);
            carry /= 10;
        }
    }
    let value = digits
        .into_iter()
        .skip_while(|digit| *digit == 0)
        .map(|digit| char::from(b'0' + digit))
        .collect::<String>();
    if value.is_empty() {
        "0".to_owned()
    } else {
        value
    }
}

pub(super) fn keccak_signature_hex(signature: &str) -> String {
    format!("0x{}", hex_string(keccak256_bytes(signature.as_bytes())))
}

fn keccak256_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&digest);
    output
}

pub(super) fn deterministic_uuid(seed: &str) -> Uuid {
    let mut digest = Keccak256::new();
    digest.update(seed.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.finalize()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

pub(super) fn hex_string(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
