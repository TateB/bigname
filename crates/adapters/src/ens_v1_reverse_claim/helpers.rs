use alloy_sol_types::{SolEvent, sol};
use anyhow::{Result, bail};

use crate::evm_abi;
#[cfg(test)]
pub(super) use crate::evm_abi::hex_string;
pub(super) use crate::evm_abi::namehash_hex;

use super::{SOURCE_FAMILY_BASENAMES_BASE_PRIMARY, SOURCE_FAMILY_ENS_V1_REVERSE_L1};

const BASENAMES_BASE_REVERSE_ROOT_NODE: &str =
    "0x08d9b0993eb8c4da57c37a4b84a6e384c2623114ff4e9370ed51c9b8935109ba";
const BASENAMES_BASE_REVERSE_ROOT_NAME: &str = "80002105.reverse";

sol! {
    #[derive(Debug)]
    event ReverseClaimed(address indexed addr, bytes32 indexed node);

    #[derive(Debug)]
    event BaseReverseClaimed(address indexed addr, bytes32 indexed node);
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

pub(super) fn basenames_base_reverse_node_for_address(address: &str) -> Result<String> {
    let reverse_label = reverse_label_for_address(address)?;
    let label_hash = evm_abi::keccak256_hex(reverse_label.as_bytes());
    evm_abi::child_namehash_hex(BASENAMES_BASE_REVERSE_ROOT_NODE, &label_hash)
}

pub(super) fn reverse_node_for_source_family(source_family: &str, address: &str) -> Result<String> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_REVERSE_L1 => reverse_node_for_address(address),
        SOURCE_FAMILY_BASENAMES_BASE_PRIMARY => basenames_base_reverse_node_for_address(address),
        _ => bail!("unsupported reverse claim source family {source_family}"),
    }
}

pub(super) fn reverse_name_for_source_family(source_family: &str, address: &str) -> Result<String> {
    let reverse_label = reverse_label_for_address(address)?;
    match source_family {
        SOURCE_FAMILY_ENS_V1_REVERSE_L1 => Ok(format!("{reverse_label}.addr.reverse")),
        SOURCE_FAMILY_BASENAMES_BASE_PRIMARY => Ok(format!(
            "{reverse_label}.{BASENAMES_BASE_REVERSE_ROOT_NAME}"
        )),
        _ => bail!("unsupported reverse claim source family {source_family}"),
    }
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

pub(super) fn base_reverse_claimed_topic0() -> String {
    evm_abi::hex_string(BaseReverseClaimed::SIGNATURE_HASH.as_slice())
}

pub(super) fn reverse_claimed_topic0_for_source_family(source_family: &str) -> Option<String> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_REVERSE_L1 => Some(reverse_claimed_topic0()),
        SOURCE_FAMILY_BASENAMES_BASE_PRIMARY => Some(base_reverse_claimed_topic0()),
        _ => None,
    }
}
