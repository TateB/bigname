use alloy_sol_types::sol;
use anyhow::{Context, Result};

use crate::adapter_manifest::ActiveManifestEventTopic0sBySignature;
use crate::evm_abi::{
    address_hex, decode_event_log, hex_string as prefixed_hex_string, u256_word_hex,
};

use super::{
    constants::*,
    types::{RegistryObservation, RegistryRawLogRow},
};

sol! {
    #[derive(Debug)]
    event LabelRegistered(
        uint256 indexed tokenId,
        bytes32 indexed labelHash,
        string label,
        address owner,
        uint64 expiry,
        address indexed sender
    );

    #[derive(Debug)]
    event LabelReserved(
        uint256 indexed tokenId,
        bytes32 indexed labelHash,
        string label,
        uint64 expiry,
        address indexed sender
    );

    #[derive(Debug)]
    event LabelUnregistered(uint256 indexed tokenId, address indexed sender);

    #[derive(Debug)]
    event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender);

    #[derive(Debug)]
    event SubregistryUpdated(
        uint256 indexed tokenId,
        address indexed subregistry,
        address indexed sender
    );

    #[derive(Debug)]
    event ResolverUpdated(
        uint256 indexed tokenId,
        address indexed resolver,
        address indexed sender
    );

    #[derive(Debug)]
    event TokenResource(uint256 indexed tokenId, uint256 indexed resource);

    #[derive(Debug)]
    event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId);

    #[derive(Debug)]
    event ParentUpdated(address indexed parent, string label, address indexed sender);
}

pub(super) fn build_registry_observation(
    raw_log: &RegistryRawLogRow,
    event_topics: &ActiveManifestEventTopic0sBySignature,
) -> Result<Option<RegistryObservation>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };
    let reference = raw_log.reference();

    if event_topics.matches(ABI_EVENT_LABEL_REGISTERED_SIGNATURE, topic0)? {
        let event = decode_event_log::<LabelRegistered>(
            &raw_log.topics,
            &raw_log.data,
            "LabelRegistered log is malformed",
        )?;
        return Ok(Some(RegistryObservation::LabelRegistered {
            token_id: u256_word_hex(event.tokenId),
            labelhash: prefixed_hex_string(event.labelHash.as_slice()),
            label: event.label,
            owner: address_hex(event.owner),
            expiry: i64::try_from(event.expiry).context("LabelRegistered expiry exceeds i64")?,
            sender: address_hex(event.sender),
            reference,
        }));
    }

    if event_topics.matches(ABI_EVENT_LABEL_RESERVED_SIGNATURE, topic0)? {
        let event = decode_event_log::<LabelReserved>(
            &raw_log.topics,
            &raw_log.data,
            "LabelReserved log is malformed",
        )?;
        return Ok(Some(RegistryObservation::LabelReserved {
            token_id: u256_word_hex(event.tokenId),
            labelhash: prefixed_hex_string(event.labelHash.as_slice()),
            label: event.label,
            expiry: i64::try_from(event.expiry).context("LabelReserved expiry exceeds i64")?,
            sender: address_hex(event.sender),
            reference,
        }));
    }

    if event_topics.matches(ABI_EVENT_LABEL_UNREGISTERED_SIGNATURE, topic0)? {
        let event = decode_event_log::<LabelUnregistered>(
            &raw_log.topics,
            &raw_log.data,
            "LabelUnregistered log is malformed",
        )?;
        return Ok(Some(RegistryObservation::LabelUnregistered {
            token_id: u256_word_hex(event.tokenId),
            sender: address_hex(event.sender),
            reference,
        }));
    }

    if event_topics.matches(ABI_EVENT_EXPIRY_UPDATED_SIGNATURE, topic0)? {
        let event = decode_event_log::<ExpiryUpdated>(
            &raw_log.topics,
            &raw_log.data,
            "ExpiryUpdated log is malformed",
        )?;
        return Ok(Some(RegistryObservation::ExpiryUpdated {
            token_id: u256_word_hex(event.tokenId),
            new_expiry: i64::try_from(event.newExpiry)
                .context("ExpiryUpdated new expiry exceeds i64")?,
            sender: address_hex(event.sender),
            reference,
        }));
    }

    if event_topics.matches(ABI_EVENT_SUBREGISTRY_UPDATED_SIGNATURE, topic0)? {
        let event = decode_event_log::<SubregistryUpdated>(
            &raw_log.topics,
            &raw_log.data,
            "SubregistryUpdated log is malformed",
        )?;
        return Ok(Some(RegistryObservation::SubregistryUpdated {
            token_id: u256_word_hex(event.tokenId),
            subregistry: address_hex(event.subregistry),
            sender: address_hex(event.sender),
            reference,
        }));
    }

    if event_topics.matches(ABI_EVENT_RESOLVER_UPDATED_SIGNATURE, topic0)? {
        let event = decode_event_log::<ResolverUpdated>(
            &raw_log.topics,
            &raw_log.data,
            "ResolverUpdated log is malformed",
        )?;
        return Ok(Some(RegistryObservation::ResolverUpdated {
            token_id: u256_word_hex(event.tokenId),
            resolver: address_hex(event.resolver),
            sender: address_hex(event.sender),
            reference,
        }));
    }

    if event_topics.matches(ABI_EVENT_TOKEN_RESOURCE_SIGNATURE, topic0)? {
        let event = decode_event_log::<TokenResource>(
            &raw_log.topics,
            &raw_log.data,
            "TokenResource log is malformed",
        )?;
        return Ok(Some(RegistryObservation::TokenResource {
            token_id: u256_word_hex(event.tokenId),
            upstream_resource: u256_word_hex(event.resource),
            reference,
        }));
    }

    if event_topics.matches(ABI_EVENT_TOKEN_REGENERATED_SIGNATURE, topic0)? {
        let event = decode_event_log::<TokenRegenerated>(
            &raw_log.topics,
            &raw_log.data,
            "TokenRegenerated log is malformed",
        )?;
        return Ok(Some(RegistryObservation::TokenRegenerated {
            old_token_id: u256_word_hex(event.oldTokenId),
            new_token_id: u256_word_hex(event.newTokenId),
            reference,
        }));
    }

    if event_topics.matches(ABI_EVENT_PARENT_UPDATED_SIGNATURE, topic0)? {
        let event = decode_event_log::<ParentUpdated>(
            &raw_log.topics,
            &raw_log.data,
            "ParentUpdated log is malformed",
        )?;
        return Ok(Some(RegistryObservation::ParentUpdated {
            parent: address_hex(event.parent),
            label: event.label,
            sender: address_hex(event.sender),
            reference,
        }));
    }

    Ok(None)
}
