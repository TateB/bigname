use anyhow::{Context, Result};

use super::constants::{
    EAC_ROLES_CHANGED_SIGNATURE, NAMED_ADDR_RESOURCE_SIGNATURE, NAMED_RESOURCE_SIGNATURE,
    NAMED_TEXT_RESOURCE_SIGNATURE,
};
use super::types::{PermissionsObservation, PermissionsRawLogRow};
use super::util::{
    decode_dynamic_bytes, decode_dynamic_string, decode_u256_topic_decimal, keccak_signature_hex,
    normalize_hex_32, normalize_hex_32_word, normalize_topic_address, word_at,
};

pub(super) fn build_permissions_observation(
    raw_log: &PermissionsRawLogRow,
) -> Result<Option<PermissionsObservation>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAMED_RESOURCE_SIGNATURE)) {
        let resource = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NamedResource missing resource topic")?,
        )?;
        let name = decode_dynamic_bytes(&raw_log.data, 0)?;
        return Ok(Some(PermissionsObservation::NamedResource {
            resource,
            name,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAMED_TEXT_RESOURCE_SIGNATURE)) {
        let resource = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NamedTextResource missing resource topic")?,
        )?;
        let key_hash = normalize_hex_32(
            raw_log
                .topics
                .get(2)
                .context("NamedTextResource missing key hash topic")?,
        )?;
        let name = decode_dynamic_bytes(&raw_log.data, 0)?;
        let key = decode_dynamic_string(&raw_log.data, 1)?;
        return Ok(Some(PermissionsObservation::NamedTextResource {
            resource,
            name,
            key_hash,
            key,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAMED_ADDR_RESOURCE_SIGNATURE)) {
        let resource = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NamedAddrResource missing resource topic")?,
        )?;
        let coin_type = decode_u256_topic_decimal(
            raw_log
                .topics
                .get(2)
                .context("NamedAddrResource missing coin type topic")?,
        )?;
        let name = decode_dynamic_bytes(&raw_log.data, 0)?;
        return Ok(Some(PermissionsObservation::NamedAddrResource {
            resource,
            name,
            coin_type,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(EAC_ROLES_CHANGED_SIGNATURE)) {
        let resource = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("EACRolesChanged missing resource topic")?,
        )?;
        let account = normalize_topic_address(
            raw_log
                .topics
                .get(2)
                .context("EACRolesChanged missing account topic")?,
        )?;
        let old_role_bitmap = normalize_hex_32_word(word_at(&raw_log.data, 0)?)?;
        let new_role_bitmap = normalize_hex_32_word(word_at(&raw_log.data, 1)?)?;
        return Ok(Some(PermissionsObservation::EacRolesChanged {
            resource,
            account,
            old_role_bitmap,
            new_role_bitmap,
        }));
    }

    Ok(None)
}
