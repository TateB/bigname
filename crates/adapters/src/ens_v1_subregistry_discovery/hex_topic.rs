use anyhow::{Context, Result, bail};
use sha3::{Digest, Keccak256};

const NEW_OWNER_SIGNATURE: &str = "NewOwner(bytes32,bytes32,address)";
const NEW_RESOLVER_SIGNATURE: &str = "NewResolver(bytes32,address)";
#[cfg(test)]
pub(super) const ZERO_NODE: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000000";
pub(super) const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

pub(super) fn decode_owner_address(data: &[u8]) -> Result<String> {
    if data.len() < 32 {
        bail!("NewOwner log data must be at least 32 bytes");
    }

    Ok(format!("0x{}", hex_string(&data[12..32])))
}

pub(super) fn child_node(parent_node: &str, labelhash: &str) -> Result<String> {
    let parent_node = decode_hex_32(parent_node)?;
    let labelhash = decode_hex_32(labelhash)?;
    let mut hasher = Keccak256::new();
    hasher.update(parent_node);
    hasher.update(labelhash);
    Ok(format!("0x{}", hex_string(hasher.finalize())))
}

pub(super) fn decode_hex_32(value: &str) -> Result<[u8; 32]> {
    let normalized = normalize_hex_32(value)?;
    let mut output = [0u8; 32];
    for (index, chunk) in normalized.as_bytes()[2..].chunks(2).enumerate() {
        let hex = std::str::from_utf8(chunk).context("hex topic chunk must be utf-8")?;
        output[index] =
            u8::from_str_radix(hex, 16).with_context(|| format!("invalid hex byte {hex}"))?;
    }
    Ok(output)
}

pub(super) fn normalize_hex_32(value: &str) -> Result<String> {
    let normalized = value.to_ascii_lowercase();
    let normalized = if normalized.starts_with("0x") {
        normalized
    } else {
        format!("0x{normalized}")
    };
    if normalized.len() != 66 {
        bail!("expected 32-byte hex value, got {value}");
    }
    Ok(normalized)
}

pub(super) fn normalize_address(value: &str) -> String {
    value.to_ascii_lowercase()
}

pub(super) fn new_owner_topic0() -> String {
    keccak_signature_hex(NEW_OWNER_SIGNATURE)
}

pub(super) fn new_resolver_topic0() -> String {
    keccak_signature_hex(NEW_RESOLVER_SIGNATURE)
}

fn keccak_signature_hex(signature: &str) -> String {
    let mut hasher = Keccak256::new();
    hasher.update(signature.as_bytes());
    format!("0x{}", hex_string(hasher.finalize()))
}

pub(super) fn null_if_zero_address(address: &str) -> Option<String> {
    if normalize_address(address) == ZERO_ADDRESS {
        None
    } else {
        Some(normalize_address(address))
    }
}

pub(super) fn hex_string(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
