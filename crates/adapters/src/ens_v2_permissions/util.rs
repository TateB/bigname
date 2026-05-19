use sqlx::types::Uuid;

use crate::ens_v2_common::keccak256_bytes;
pub(super) use crate::ens_v2_common::{dns_decode, hex_string};
pub(super) use crate::evm_abi::hex_32 as decode_hex_32;

pub(super) fn resource_is_root(resource: &str) -> bool {
    resource == "0x0000000000000000000000000000000000000000000000000000000000000000"
}

pub(super) fn deterministic_uuid(seed: &str) -> Uuid {
    let digest = keccak256_bytes(seed.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}
