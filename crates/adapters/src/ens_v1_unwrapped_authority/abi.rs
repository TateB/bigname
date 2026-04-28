use super::*;

pub(super) fn decode_first_dynamic_string(data: &[u8]) -> Result<String> {
    decode_nth_dynamic_string(data, 0)
}

pub(super) fn decode_first_dynamic_bytes(data: &[u8]) -> Result<Vec<u8>> {
    decode_nth_dynamic_bytes(data, 0)
}

pub(super) fn decode_nth_dynamic_string(data: &[u8], parameter_index: usize) -> Result<String> {
    String::from_utf8(decode_nth_dynamic_bytes(data, parameter_index)?)
        .context("dynamic string payload is not valid UTF-8")
}

pub(super) fn decode_nth_dynamic_bytes(data: &[u8], parameter_index: usize) -> Result<Vec<u8>> {
    let offset_start = parameter_index
        .checked_mul(32)
        .context("dynamic ABI parameter index overflowed")?;
    if data.len() < 64 {
        bail!("event data is too short to decode a dynamic bytes parameter");
    }
    let offset = word_to_usize(
        data.get(offset_start..offset_start + 32)
            .context("event data is missing dynamic bytes offset")?,
    )
    .context("invalid ABI offset")?;
    if data.len() < offset + 32 {
        bail!("event data is missing dynamic bytes length");
    }
    let byte_length = word_to_usize(&data[offset..offset + 32]).context("invalid ABI length")?;
    let bytes_start = offset + 32;
    let bytes_end = bytes_start + byte_length;
    if data.len() < bytes_end {
        bail!("event data does not contain the full dynamic bytes payload");
    }
    Ok(data[bytes_start..bytes_end].to_vec())
}

pub(super) fn abi_word_to_usize(word: &[u8]) -> Result<usize> {
    word_to_usize(word)
}

fn word_to_usize(word: &[u8]) -> Result<usize> {
    if word.len() != 32 {
        bail!("ABI word must be 32 bytes");
    }
    if word[..24].iter().any(|byte| *byte != 0) {
        bail!("ABI word exceeds supported usize width");
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&word[24..32]);
    usize::try_from(u64::from_be_bytes(bytes)).context("ABI word does not fit in usize")
}

pub(super) fn abi_word_to_i64(word: &[u8]) -> Result<i64> {
    if word.len() != 32 {
        bail!("ABI word must be 32 bytes");
    }
    if word[..24].iter().any(|byte| *byte != 0) {
        bail!("ABI word exceeds supported i64 width");
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&word[24..32]);
    i64::try_from(u64::from_be_bytes(bytes)).context("ABI word does not fit in i64")
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

pub(super) fn decode_owner_address(data: &[u8]) -> Result<String> {
    let word = data
        .get(..32)
        .context("owner address payload is missing the first ABI word")?;
    let mut output = String::from("0x");
    for byte in &word[12..32] {
        output.push_str(&format!("{byte:02x}"));
    }
    Ok(output)
}

pub(super) fn normalize_topic_address(value: &str) -> Result<String> {
    let normalized = normalize_hex_32(value)?;
    Ok(format!("0x{}", &normalized[26..]))
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
