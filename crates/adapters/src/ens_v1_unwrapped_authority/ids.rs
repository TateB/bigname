use super::*;
use crate::evm_abi;
pub(super) use crate::evm_abi::{child_namehash_hex, hex_string, keccak256_hex, namehash_hex};

pub(super) fn deterministic_uuid(seed: &str) -> Uuid {
    let digest = evm_abi::keccak256_bytes(seed.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

pub(super) fn eth_node() -> String {
    namehash_hex(&[b"eth".to_vec()])
}

pub(super) fn base_eth_node() -> String {
    namehash_hex(&[b"base".to_vec(), b"eth".to_vec()])
}

#[cfg(test)]
pub(super) fn name_registered_topic0() -> String {
    keccak256_hex(NAME_REGISTERED_SIGNATURE.as_bytes())
}

#[cfg(test)]
pub(super) fn wrapped_name_registered_topic0() -> String {
    keccak256_hex(WRAPPED_NAME_REGISTERED_SIGNATURE.as_bytes())
}

#[cfg(test)]
pub(super) fn unwrapped_name_registered_topic0() -> String {
    keccak256_hex(UNWRAPPED_NAME_REGISTERED_SIGNATURE.as_bytes())
}

#[cfg(test)]
pub(super) fn basenames_name_registered_topic0() -> String {
    keccak256_hex(BASENAMES_NAME_REGISTERED_SIGNATURE.as_bytes())
}

#[cfg(test)]
pub(super) fn unwrapped_name_renewed_topic0() -> String {
    keccak256_hex(UNWRAPPED_NAME_RENEWED_SIGNATURE.as_bytes())
}

#[cfg(test)]
pub(super) fn transfer_topic0() -> String {
    keccak256_hex(TRANSFER_SIGNATURE.as_bytes())
}

#[cfg(test)]
pub(super) fn registry_transfer_topic0() -> String {
    keccak256_hex(REGISTRY_TRANSFER_SIGNATURE.as_bytes())
}

#[cfg(test)]
pub(super) fn new_owner_topic0() -> String {
    keccak256_hex(NEW_OWNER_SIGNATURE.as_bytes())
}

#[cfg(test)]
pub(super) fn new_ttl_topic0() -> String {
    keccak256_hex(NEW_TTL_SIGNATURE.as_bytes())
}

#[cfg(test)]
pub(super) fn new_resolver_topic0() -> String {
    keccak256_hex(NEW_RESOLVER_SIGNATURE.as_bytes())
}

#[cfg(test)]
pub(super) fn name_changed_topic0() -> String {
    keccak256_hex(NAME_CHANGED_SIGNATURE.as_bytes())
}

#[cfg(test)]
pub(super) fn addr_changed_topic0() -> String {
    keccak256_hex(ADDR_CHANGED_SIGNATURE.as_bytes())
}

#[cfg(test)]
pub(super) fn address_changed_topic0() -> String {
    keccak256_hex(ADDRESS_CHANGED_SIGNATURE.as_bytes())
}

#[cfg(test)]
pub(super) fn text_changed_topic0() -> String {
    keccak256_hex(TEXT_CHANGED_WITHOUT_VALUE_SIGNATURE.as_bytes())
}

#[cfg(test)]
pub(super) fn text_changed_with_value_topic0() -> String {
    keccak256_hex(TEXT_CHANGED_WITH_VALUE_SIGNATURE.as_bytes())
}

#[cfg(test)]
pub(super) fn data_changed_topic0() -> String {
    keccak256_hex(DATA_CHANGED_SIGNATURE.as_bytes())
}

#[cfg(test)]
pub(super) fn pubkey_changed_topic0() -> String {
    keccak256_hex(PUBKEY_CHANGED_SIGNATURE.as_bytes())
}

#[cfg(test)]
pub(super) fn version_changed_topic0() -> String {
    keccak256_hex(VERSION_CHANGED_SIGNATURE.as_bytes())
}

#[cfg(test)]
pub(super) fn name_wrapped_topic0() -> String {
    keccak256_hex(NAME_WRAPPED_SIGNATURE.as_bytes())
}

#[cfg(test)]
pub(super) fn name_unwrapped_topic0() -> String {
    keccak256_hex(NAME_UNWRAPPED_SIGNATURE.as_bytes())
}

#[cfg(test)]
pub(super) fn fuses_set_topic0() -> String {
    keccak256_hex(FUSES_SET_SIGNATURE.as_bytes())
}

#[cfg(test)]
pub(super) fn expiry_extended_topic0() -> String {
    keccak256_hex(EXPIRY_EXTENDED_SIGNATURE.as_bytes())
}

#[cfg(test)]
pub(super) fn transfer_single_topic0() -> String {
    keccak256_hex(TRANSFER_SINGLE_SIGNATURE.as_bytes())
}
