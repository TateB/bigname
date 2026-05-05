use alloy_sol_types::sol_data::{Bytes as SolBytes, String as SolString, Uint};
use anyhow::{Context, Result};

use crate::evm_abi::{abi_decode_params, normalize_hex_32, u256_decimal};

use super::{
    constants::*,
    types::{ResolverObservation, ResolverRawLogRow},
    util::keccak_signature_hex,
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
        let (coin_type, address_bytes) = abi_decode_params::<(Uint<256>, SolBytes)>(
            &raw_log.data,
            "AddressChanged data is malformed",
        )?;
        return Ok(Some(ResolverObservation::AddressChanged {
            node,
            coin_type: u256_decimal(coin_type),
            address_bytes: address_bytes.to_vec(),
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(TEXT_CHANGED_SIGNATURE)) {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("TextChanged missing node topic")?,
        )?;
        let (key, value) = abi_decode_params::<(SolString, SolString)>(
            &raw_log.data,
            "TextChanged data is malformed",
        )?;
        return Ok(Some(ResolverObservation::TextChanged { node, key, value }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(CONTENTHASH_CHANGED_SIGNATURE)) {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("ContenthashChanged missing node topic")?,
        )?;
        let (hash,) = abi_decode_params::<(SolBytes,)>(
            &raw_log.data,
            "ContenthashChanged data is malformed",
        )?;
        return Ok(Some(ResolverObservation::ContenthashChanged {
            node,
            hash: hash.to_vec(),
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAME_CHANGED_SIGNATURE)) {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NameChanged missing node topic")?,
        )?;
        let (name,) =
            abi_decode_params::<(SolString,)>(&raw_log.data, "NameChanged data is malformed")?;
        return Ok(Some(ResolverObservation::NameChanged { node, name }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(VERSION_CHANGED_SIGNATURE)) {
        let node = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("VersionChanged missing node topic")?,
        )?;
        let (version,) =
            abi_decode_params::<(Uint<64>,)>(&raw_log.data, "VersionChanged data is malformed")?;
        return Ok(Some(ResolverObservation::VersionChanged {
            node,
            version: i64::try_from(version).context("VersionChanged version exceeds i64")?,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(ALIAS_CHANGED_SIGNATURE)) {
        let (from_name, to_name) = abi_decode_params::<(SolBytes, SolBytes)>(
            &raw_log.data,
            "AliasChanged data is malformed",
        )?;
        return Ok(Some(ResolverObservation::AliasChanged {
            from_name: from_name.to_vec(),
            to_name: to_name.to_vec(),
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAMED_RESOURCE_SIGNATURE)) {
        let (name,) =
            abi_decode_params::<(SolBytes,)>(&raw_log.data, "NamedResource data is malformed")?;
        return Ok(Some(ResolverObservation::NamedResource {
            name: name.to_vec(),
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAMED_TEXT_RESOURCE_SIGNATURE)) {
        let (name,) =
            abi_decode_params::<(SolBytes,)>(&raw_log.data, "NamedTextResource data is malformed")?;
        return Ok(Some(ResolverObservation::NamedTextResource {
            name: name.to_vec(),
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAMED_ADDR_RESOURCE_SIGNATURE)) {
        let (name,) =
            abi_decode_params::<(SolBytes,)>(&raw_log.data, "NamedAddrResource data is malformed")?;
        return Ok(Some(ResolverObservation::NamedAddrResource {
            name: name.to_vec(),
        }));
    }

    Ok(None)
}
