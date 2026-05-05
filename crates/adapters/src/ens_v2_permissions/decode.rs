use alloy_sol_types::sol_data::{Bytes as SolBytes, String as SolString, Uint};
use anyhow::{Context, Result};

use crate::evm_abi::{
    abi_decode_params, normalize_hex_32, topic_address_hex, u256_topic_decimal, u256_word_hex,
};

use super::constants::{
    EAC_ROLES_CHANGED_SIGNATURE, NAMED_ADDR_RESOURCE_SIGNATURE, NAMED_RESOURCE_SIGNATURE,
    NAMED_TEXT_RESOURCE_SIGNATURE,
};
use super::types::{PermissionsObservation, PermissionsRawLogRow};
use super::util::keccak_signature_hex;

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
        let (name,) =
            abi_decode_params::<(SolBytes,)>(&raw_log.data, "NamedResource data is malformed")?;
        return Ok(Some(PermissionsObservation::NamedResource {
            resource,
            name: name.to_vec(),
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
        let (name, key) = abi_decode_params::<(SolBytes, SolString)>(
            &raw_log.data,
            "NamedTextResource data is malformed",
        )?;
        return Ok(Some(PermissionsObservation::NamedTextResource {
            resource,
            name: name.to_vec(),
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
        let coin_type = u256_topic_decimal(
            raw_log
                .topics
                .get(2)
                .context("NamedAddrResource missing coin type topic")?,
        )?;
        let (name,) =
            abi_decode_params::<(SolBytes,)>(&raw_log.data, "NamedAddrResource data is malformed")?;
        return Ok(Some(PermissionsObservation::NamedAddrResource {
            resource,
            name: name.to_vec(),
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
        let account = topic_address_hex(
            raw_log
                .topics
                .get(2)
                .context("EACRolesChanged missing account topic")?,
        )?;
        let (old_role_bitmap, new_role_bitmap) = abi_decode_params::<(Uint<256>, Uint<256>)>(
            &raw_log.data,
            "EACRolesChanged data is malformed",
        )?;
        return Ok(Some(PermissionsObservation::EacRolesChanged {
            resource,
            account,
            old_role_bitmap: u256_word_hex(old_role_bitmap),
            new_role_bitmap: u256_word_hex(new_role_bitmap),
        }));
    }

    Ok(None)
}
