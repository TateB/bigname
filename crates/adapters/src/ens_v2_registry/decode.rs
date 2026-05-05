use alloy_sol_types::sol_data::{Address as SolAddress, String as SolString, Uint};
use anyhow::{Context, Result};

use crate::evm_abi::{
    abi_decode_params, address_hex, normalize_hex_32, topic_address_hex, u64_topic,
};

use super::{
    constants::*,
    types::{RegistryObservation, RegistryRawLogRow},
    util::keccak_signature_hex,
};

pub(super) fn build_registry_observation(
    raw_log: &RegistryRawLogRow,
) -> Result<Option<RegistryObservation>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };
    let reference = raw_log.reference();

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(LABEL_REGISTERED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("LabelRegistered missing tokenId topic")?,
        )?;
        let labelhash = normalize_hex_32(
            raw_log
                .topics
                .get(2)
                .context("LabelRegistered missing labelHash topic")?,
        )?;
        let sender = topic_address_hex(
            raw_log
                .topics
                .get(3)
                .context("LabelRegistered missing sender topic")?,
        )?;
        let (label, owner, expiry) = abi_decode_params::<(SolString, SolAddress, Uint<64>)>(
            &raw_log.data,
            "LabelRegistered data is malformed",
        )?;
        return Ok(Some(RegistryObservation::LabelRegistered {
            token_id,
            labelhash,
            label,
            owner: address_hex(owner),
            expiry: i64::try_from(expiry).context("LabelRegistered expiry exceeds i64")?,
            sender,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(LABEL_RESERVED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("LabelReserved missing tokenId topic")?,
        )?;
        let labelhash = normalize_hex_32(
            raw_log
                .topics
                .get(2)
                .context("LabelReserved missing labelHash topic")?,
        )?;
        let sender = topic_address_hex(
            raw_log
                .topics
                .get(3)
                .context("LabelReserved missing sender topic")?,
        )?;
        let (label, expiry) = abi_decode_params::<(SolString, Uint<64>)>(
            &raw_log.data,
            "LabelReserved data is malformed",
        )?;
        return Ok(Some(RegistryObservation::LabelReserved {
            token_id,
            labelhash,
            label,
            expiry: i64::try_from(expiry).context("LabelReserved expiry exceeds i64")?,
            sender,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(LABEL_UNREGISTERED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("LabelUnregistered missing tokenId topic")?,
        )?;
        let sender = topic_address_hex(
            raw_log
                .topics
                .get(2)
                .context("LabelUnregistered missing sender topic")?,
        )?;
        return Ok(Some(RegistryObservation::LabelUnregistered {
            token_id,
            sender,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(EXPIRY_UPDATED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("ExpiryUpdated missing tokenId topic")?,
        )?;
        let new_expiry = u64_topic(
            raw_log
                .topics
                .get(2)
                .context("ExpiryUpdated missing newExpiry topic")?,
        )?;
        let sender = topic_address_hex(
            raw_log
                .topics
                .get(3)
                .context("ExpiryUpdated missing sender topic")?,
        )?;
        return Ok(Some(RegistryObservation::ExpiryUpdated {
            token_id,
            new_expiry,
            sender,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(SUBREGISTRY_UPDATED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("SubregistryUpdated missing tokenId topic")?,
        )?;
        let subregistry = topic_address_hex(
            raw_log
                .topics
                .get(2)
                .context("SubregistryUpdated missing subregistry topic")?,
        )?;
        let sender = topic_address_hex(
            raw_log
                .topics
                .get(3)
                .context("SubregistryUpdated missing sender topic")?,
        )?;
        return Ok(Some(RegistryObservation::SubregistryUpdated {
            token_id,
            subregistry,
            sender,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(RESOLVER_UPDATED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("ResolverUpdated missing tokenId topic")?,
        )?;
        let resolver = topic_address_hex(
            raw_log
                .topics
                .get(2)
                .context("ResolverUpdated missing resolver topic")?,
        )?;
        let sender = topic_address_hex(
            raw_log
                .topics
                .get(3)
                .context("ResolverUpdated missing sender topic")?,
        )?;
        return Ok(Some(RegistryObservation::ResolverUpdated {
            token_id,
            resolver,
            sender,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(TOKEN_RESOURCE_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("TokenResource missing tokenId topic")?,
        )?;
        let upstream_resource = normalize_hex_32(
            raw_log
                .topics
                .get(2)
                .context("TokenResource missing resource topic")?,
        )?;
        return Ok(Some(RegistryObservation::TokenResource {
            token_id,
            upstream_resource,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(TOKEN_REGENERATED_SIGNATURE)) {
        let old_token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("TokenRegenerated missing oldTokenId topic")?,
        )?;
        let new_token_id = normalize_hex_32(
            raw_log
                .topics
                .get(2)
                .context("TokenRegenerated missing newTokenId topic")?,
        )?;
        return Ok(Some(RegistryObservation::TokenRegenerated {
            old_token_id,
            new_token_id,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(PARENT_UPDATED_SIGNATURE)) {
        let parent = topic_address_hex(
            raw_log
                .topics
                .get(1)
                .context("ParentUpdated missing parent topic")?,
        )?;
        let sender = topic_address_hex(
            raw_log
                .topics
                .get(2)
                .context("ParentUpdated missing sender topic")?,
        )?;
        let (label,) =
            abi_decode_params::<(SolString,)>(&raw_log.data, "ParentUpdated data is malformed")?;
        return Ok(Some(RegistryObservation::ParentUpdated {
            parent,
            label,
            sender,
            reference,
        }));
    }

    Ok(None)
}
