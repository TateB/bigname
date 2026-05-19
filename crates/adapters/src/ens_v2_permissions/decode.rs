use alloy_sol_types::sol;
use anyhow::Result;

use crate::adapter_manifest::ActiveManifestEventTopic0sBySignature;
use crate::evm_abi::{
    address_hex, decode_event_log, hex_string as prefixed_hex_string, u256_decimal, u256_word_hex,
};

use super::constants::*;
use super::types::{PermissionsObservation, PermissionsRawLogRow};

sol! {
    #[derive(Debug)]
    event NamedResource(uint256 indexed resource, bytes name);

    #[derive(Debug)]
    event NamedTextResource(uint256 indexed resource, bytes name, bytes32 indexed keyHash, string key);

    #[derive(Debug)]
    event NamedAddrResource(uint256 indexed resource, bytes name, uint256 indexed coinType);

    #[derive(Debug)]
    event EACRolesChanged(
        uint256 indexed resource,
        address indexed account,
        uint256 oldRoles,
        uint256 newRoles
    );
}

pub(super) fn build_permissions_observation(
    raw_log: &PermissionsRawLogRow,
    event_topics: &ActiveManifestEventTopic0sBySignature,
) -> Result<Option<PermissionsObservation>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };

    if event_topics.matches(ABI_EVENT_NAMED_RESOURCE_SIGNATURE, topic0)? {
        let event = decode_event_log::<NamedResource>(
            &raw_log.topics,
            &raw_log.data,
            "NamedResource log is malformed",
        )?;
        return Ok(Some(PermissionsObservation::NamedResource {
            resource: u256_word_hex(event.resource),
            name: event.name.to_vec(),
        }));
    }

    if event_topics.matches(ABI_EVENT_NAMED_TEXT_RESOURCE_SIGNATURE, topic0)? {
        let event = decode_event_log::<NamedTextResource>(
            &raw_log.topics,
            &raw_log.data,
            "NamedTextResource log is malformed",
        )?;
        return Ok(Some(PermissionsObservation::NamedTextResource {
            resource: u256_word_hex(event.resource),
            name: event.name.to_vec(),
            key_hash: prefixed_hex_string(event.keyHash.as_slice()),
            key: event.key,
        }));
    }

    if event_topics.matches(ABI_EVENT_NAMED_ADDR_RESOURCE_SIGNATURE, topic0)? {
        let event = decode_event_log::<NamedAddrResource>(
            &raw_log.topics,
            &raw_log.data,
            "NamedAddrResource log is malformed",
        )?;
        return Ok(Some(PermissionsObservation::NamedAddrResource {
            resource: u256_word_hex(event.resource),
            name: event.name.to_vec(),
            coin_type: u256_decimal(event.coinType),
        }));
    }

    if event_topics.matches(ABI_EVENT_EAC_ROLES_CHANGED_SIGNATURE, topic0)? {
        let event = decode_event_log::<EACRolesChanged>(
            &raw_log.topics,
            &raw_log.data,
            "EACRolesChanged log is malformed",
        )?;
        return Ok(Some(PermissionsObservation::EacRolesChanged {
            resource: u256_word_hex(event.resource),
            account: address_hex(event.account),
            old_role_bitmap: u256_word_hex(event.oldRoles),
            new_role_bitmap: u256_word_hex(event.newRoles),
        }));
    }

    Ok(None)
}
