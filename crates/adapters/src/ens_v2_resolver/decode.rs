use anyhow::{Context, Result, bail};

use super::{
    constants::*,
    types::{ResolverObservation, ResolverRawLogRow},
    util::{decimal_string_from_be_bytes, keccak_signature_hex, normalize_hex_32},
};

pub(super) fn build_resolver_observation(
    raw_log: &ResolverRawLogRow,
) -> Result<Option<ResolverObservation>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(ADDRESS_CHANGED_SIGNATURE)) {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("AddressChanged missing node topic")?,
        )?;
        let coin_type = decode_u256_word_decimal(&raw_log.data, 0)?;
        let address_bytes = decode_dynamic_bytes(&raw_log.data, 1)?;
        return Ok(Some(ResolverObservation::AddressChanged {
            node,
            coin_type,
            address_bytes,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(TEXT_CHANGED_SIGNATURE)) {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("TextChanged missing node topic")?,
        )?;
        let key = decode_dynamic_string(&raw_log.data, 0)?;
        let value = decode_dynamic_string(&raw_log.data, 1)?;
        return Ok(Some(ResolverObservation::TextChanged { node, key, value }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(CONTENTHASH_CHANGED_SIGNATURE)) {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("ContenthashChanged missing node topic")?,
        )?;
        let hash = decode_dynamic_bytes(&raw_log.data, 0)?;
        return Ok(Some(ResolverObservation::ContenthashChanged { node, hash }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAME_CHANGED_SIGNATURE)) {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NameChanged missing node topic")?,
        )?;
        let name = decode_dynamic_string(&raw_log.data, 0)?;
        return Ok(Some(ResolverObservation::NameChanged { node, name }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(VERSION_CHANGED_SIGNATURE)) {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("VersionChanged missing node topic")?,
        )?;
        let version = decode_u64_word(&raw_log.data, 0)?;
        return Ok(Some(ResolverObservation::VersionChanged { node, version }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(ALIAS_CHANGED_SIGNATURE)) {
        let from_name = decode_dynamic_bytes(&raw_log.data, 0)?;
        let to_name = decode_dynamic_bytes(&raw_log.data, 1)?;
        return Ok(Some(ResolverObservation::AliasChanged {
            from_name,
            to_name,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAMED_RESOURCE_SIGNATURE)) {
        let name = decode_dynamic_bytes(&raw_log.data, 0)?;
        return Ok(Some(ResolverObservation::NamedResource { name }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAMED_TEXT_RESOURCE_SIGNATURE)) {
        let name = decode_dynamic_bytes(&raw_log.data, 0)?;
        return Ok(Some(ResolverObservation::NamedTextResource { name }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAMED_ADDR_RESOURCE_SIGNATURE)) {
        let name = decode_dynamic_bytes(&raw_log.data, 0)?;
        return Ok(Some(ResolverObservation::NamedAddrResource { name }));
    }

    Ok(None)
}

fn decode_dynamic_string(data: &[u8], offset_word_index: usize) -> Result<String> {
    String::from_utf8(decode_dynamic_bytes(data, offset_word_index)?)
        .context("dynamic string is not valid UTF-8")
}

fn decode_dynamic_bytes(data: &[u8], offset_word_index: usize) -> Result<Vec<u8>> {
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

fn decode_u256_word_decimal(data: &[u8], word_index: usize) -> Result<String> {
    let word = word_at(data, word_index)?;
    Ok(decimal_string_from_be_bytes(word))
}

fn decode_u64_word(data: &[u8], word_index: usize) -> Result<i64> {
    let word = word_at(data, word_index)?;
    if word[..24].iter().any(|byte| *byte != 0) {
        bail!("u64 ABI word exceeds supported width");
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&word[24..32]);
    i64::try_from(u64::from_be_bytes(bytes)).context("u64 ABI word does not fit in i64")
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

fn word_at(data: &[u8], word_index: usize) -> Result<&[u8]> {
    let start = word_index
        .checked_mul(32)
        .context("ABI word index overflow")?;
    let end = start + 32;
    data.get(start..end)
        .with_context(|| format!("ABI data missing word {word_index}"))
}
