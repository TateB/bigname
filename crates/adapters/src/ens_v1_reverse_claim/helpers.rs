use alloy_sol_types::{SolEvent, sol};
use anyhow::{Result, bail};

use crate::evm_abi;
#[cfg(test)]
pub(super) use crate::evm_abi::hex_string;
pub(super) use crate::evm_abi::namehash_hex;

use super::{SOURCE_FAMILY_BASENAMES_BASE_PRIMARY, SOURCE_FAMILY_ENS_V1_REVERSE_L1};

sol! {
    #[derive(Debug)]
    event ReverseClaimed(address indexed addr, bytes32 indexed node);
}

pub(super) fn supports_reverse_claim_source_family(source_family: &str) -> bool {
    matches!(
        source_family,
        SOURCE_FAMILY_ENS_V1_REVERSE_L1 | SOURCE_FAMILY_BASENAMES_BASE_PRIMARY
    )
}

pub(super) fn normalize_address(value: &str) -> Result<String> {
    let normalized = value.to_ascii_lowercase();
    if !normalized.starts_with("0x") || normalized.len() != 42 {
        bail!("expected 20-byte address, got {value}");
    }
    Ok(normalized)
}

pub(super) fn reverse_label_for_address(address: &str) -> Result<String> {
    Ok(normalize_address(address)?
        .trim_start_matches("0x")
        .to_owned())
}

pub(super) fn reverse_node_for_address(address: &str) -> Result<String> {
    let reverse_label = reverse_label_for_address(address)?;
    Ok(namehash_hex(&[
        reverse_label.into_bytes(),
        b"addr".to_vec(),
        b"reverse".to_vec(),
    ]))
}

pub(super) fn normalize_hex_32(value: &str) -> Result<String> {
    evm_abi::normalize_hex_32(value)
}

pub(super) fn normalize_topic_address(value: &str) -> Result<String> {
    evm_abi::topic_address_hex(value)
}

pub(super) fn reverse_claimed_topic0() -> String {
    evm_abi::hex_string(ReverseClaimed::SIGNATURE_HASH.as_slice())
}
