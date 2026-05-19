use alloy_sol_types::sol;
use anyhow::{Context, Result};

use crate::adapter_manifest::ActiveManifestEventTopic0sBySignature;
use crate::evm_abi::{decode_event_log, hex_string as prefixed_hex_string, u256_decimal};

use super::{
    constants::*,
    types::{ResolverObservation, ResolverRawLogRow},
};

sol! {
    #[derive(Debug)]
    event AddressChanged(bytes32 indexed node, uint256 coinType, bytes newAddress);

    #[derive(Debug)]
    event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value);

    #[derive(Debug)]
    event ContenthashChanged(bytes32 indexed node, bytes hash);

    #[derive(Debug)]
    event NameChanged(bytes32 indexed node, string name);

    #[derive(Debug)]
    event VersionChanged(bytes32 indexed node, uint64 newVersion);

    #[derive(Debug)]
    event AliasChanged(bytes indexed indexedFromName, bytes indexed indexedToName, bytes fromName, bytes toName);

    #[derive(Debug)]
    event NamedResource(uint256 indexed resource, bytes name);

    #[derive(Debug)]
    event NamedTextResource(uint256 indexed resource, bytes name, bytes32 indexed keyHash, string key);

    #[derive(Debug)]
    event NamedAddrResource(uint256 indexed resource, bytes name, uint256 indexed coinType);
}

pub(super) fn build_resolver_observation(
    raw_log: &ResolverRawLogRow,
    event_topics: &ActiveManifestEventTopic0sBySignature,
) -> Result<Option<ResolverObservation>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };

    if event_topics.matches(ABI_EVENT_ADDRESS_CHANGED_SIGNATURE, topic0)? {
        let event = decode_event_log::<AddressChanged>(
            &raw_log.topics,
            &raw_log.data,
            "AddressChanged log is malformed",
        )?;
        return Ok(Some(ResolverObservation::AddressChanged {
            node: prefixed_hex_string(event.node.as_slice()),
            coin_type: u256_decimal(event.coinType),
            address_bytes: event.newAddress.to_vec(),
        }));
    }

    if event_topics.matches(ABI_EVENT_TEXT_CHANGED_SIGNATURE, topic0)? {
        let event = decode_event_log::<TextChanged>(
            &raw_log.topics,
            &raw_log.data,
            "TextChanged log is malformed",
        )?;
        return Ok(Some(ResolverObservation::TextChanged {
            node: prefixed_hex_string(event.node.as_slice()),
            key: event.key,
            value: event.value,
        }));
    }

    if event_topics.matches(ABI_EVENT_CONTENTHASH_CHANGED_SIGNATURE, topic0)? {
        let event = decode_event_log::<ContenthashChanged>(
            &raw_log.topics,
            &raw_log.data,
            "ContenthashChanged log is malformed",
        )?;
        return Ok(Some(ResolverObservation::ContenthashChanged {
            node: prefixed_hex_string(event.node.as_slice()),
            hash: event.hash.to_vec(),
        }));
    }

    if event_topics.matches(ABI_EVENT_NAME_CHANGED_SIGNATURE, topic0)? {
        let event = decode_event_log::<NameChanged>(
            &raw_log.topics,
            &raw_log.data,
            "NameChanged log is malformed",
        )?;
        return Ok(Some(ResolverObservation::NameChanged {
            node: prefixed_hex_string(event.node.as_slice()),
            name: event.name,
        }));
    }

    if event_topics.matches(ABI_EVENT_VERSION_CHANGED_SIGNATURE, topic0)? {
        let event = decode_event_log::<VersionChanged>(
            &raw_log.topics,
            &raw_log.data,
            "VersionChanged log is malformed",
        )?;
        return Ok(Some(ResolverObservation::VersionChanged {
            node: prefixed_hex_string(event.node.as_slice()),
            version: i64::try_from(event.newVersion)
                .context("VersionChanged version exceeds i64")?,
        }));
    }

    if event_topics.matches(ABI_EVENT_ALIAS_CHANGED_SIGNATURE, topic0)? {
        let event = decode_event_log::<AliasChanged>(
            &raw_log.topics,
            &raw_log.data,
            "AliasChanged log is malformed",
        )?;
        return Ok(Some(ResolverObservation::AliasChanged {
            from_name: event.fromName.to_vec(),
            to_name: event.toName.to_vec(),
        }));
    }

    if event_topics.matches(ABI_EVENT_NAMED_RESOURCE_SIGNATURE, topic0)? {
        let event = decode_event_log::<NamedResource>(
            &raw_log.topics,
            &raw_log.data,
            "NamedResource log is malformed",
        )?;
        return Ok(Some(ResolverObservation::NamedResource {
            name: event.name.to_vec(),
        }));
    }

    if event_topics.matches(ABI_EVENT_NAMED_TEXT_RESOURCE_SIGNATURE, topic0)? {
        let event = decode_event_log::<NamedTextResource>(
            &raw_log.topics,
            &raw_log.data,
            "NamedTextResource log is malformed",
        )?;
        return Ok(Some(ResolverObservation::NamedTextResource {
            name: event.name.to_vec(),
        }));
    }

    if event_topics.matches(ABI_EVENT_NAMED_ADDR_RESOURCE_SIGNATURE, topic0)? {
        let event = decode_event_log::<NamedAddrResource>(
            &raw_log.topics,
            &raw_log.data,
            "NamedAddrResource log is malformed",
        )?;
        return Ok(Some(ResolverObservation::NamedAddrResource {
            name: event.name.to_vec(),
        }));
    }

    Ok(None)
}
