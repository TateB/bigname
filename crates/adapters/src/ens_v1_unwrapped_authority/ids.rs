use super::*;

pub(super) fn deterministic_uuid(seed: &str) -> Uuid {
    let mut digest = Keccak256::new();
    digest.update(seed.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.finalize()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

pub(super) fn keccak256_hex(bytes: &[u8]) -> String {
    let mut hasher = Keccak256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    hex_string(&digest)
}

pub(super) fn namehash_hex(labels: &[Vec<u8>]) -> String {
    let mut node = [0u8; 32];
    for label in labels.iter().rev() {
        let label_hash = {
            let mut hasher = Keccak256::new();
            hasher.update(label);
            let digest = hasher.finalize();
            let mut output = [0u8; 32];
            output.copy_from_slice(&digest);
            output
        };
        let mut combined = [0u8; 64];
        combined[..32].copy_from_slice(&node);
        combined[32..].copy_from_slice(&label_hash);
        let mut hasher = Keccak256::new();
        hasher.update(combined);
        node.copy_from_slice(&hasher.finalize());
    }
    hex_string(&node)
}

pub(super) fn eth_node() -> String {
    namehash_hex(&[b"eth".to_vec()])
}

pub(super) fn base_eth_node() -> String {
    namehash_hex(&[b"base".to_vec(), b"eth".to_vec()])
}

pub(super) fn name_registered_topic0() -> String {
    keccak256_hex(NAME_REGISTERED_SIGNATURE.as_bytes())
}

pub(super) fn wrapped_name_registered_topic0() -> String {
    keccak256_hex(WRAPPED_NAME_REGISTERED_SIGNATURE.as_bytes())
}

pub(super) fn unwrapped_name_registered_topic0() -> String {
    keccak256_hex(UNWRAPPED_NAME_REGISTERED_SIGNATURE.as_bytes())
}

pub(super) fn name_renewed_topic0() -> String {
    keccak256_hex(NAME_RENEWED_SIGNATURE.as_bytes())
}

pub(super) fn unwrapped_name_renewed_topic0() -> String {
    keccak256_hex(UNWRAPPED_NAME_RENEWED_SIGNATURE.as_bytes())
}

pub(super) fn registrar_name_registered_expiry_word_start(topic0: &str) -> Option<usize> {
    if topic0.eq_ignore_ascii_case(&name_registered_topic0()) {
        Some(64)
    } else if topic0.eq_ignore_ascii_case(&wrapped_name_registered_topic0())
        || topic0.eq_ignore_ascii_case(&unwrapped_name_registered_topic0())
    {
        Some(96)
    } else {
        None
    }
}

pub(super) fn registrar_name_renewed_expiry_word_start(topic0: &str) -> Option<usize> {
    if topic0.eq_ignore_ascii_case(&name_renewed_topic0())
        || topic0.eq_ignore_ascii_case(&unwrapped_name_renewed_topic0())
    {
        Some(64)
    } else {
        None
    }
}

pub(super) fn transfer_topic0() -> String {
    keccak256_hex(TRANSFER_SIGNATURE.as_bytes())
}

pub(super) fn registry_transfer_topic0() -> String {
    keccak256_hex(REGISTRY_TRANSFER_SIGNATURE.as_bytes())
}

pub(super) fn new_owner_topic0() -> String {
    keccak256_hex(NEW_OWNER_SIGNATURE.as_bytes())
}

pub(super) fn new_ttl_topic0() -> String {
    keccak256_hex(NEW_TTL_SIGNATURE.as_bytes())
}

pub(super) fn new_resolver_topic0() -> String {
    keccak256_hex(NEW_RESOLVER_SIGNATURE.as_bytes())
}

pub(super) fn name_changed_topic0() -> String {
    keccak256_hex(NAME_CHANGED_SIGNATURE.as_bytes())
}

pub(super) fn addr_changed_topic0() -> String {
    keccak256_hex(ADDR_CHANGED_SIGNATURE.as_bytes())
}

pub(super) fn address_changed_topic0() -> String {
    keccak256_hex(ADDRESS_CHANGED_SIGNATURE.as_bytes())
}

pub(super) fn text_changed_topic0() -> String {
    keccak256_hex(TEXT_CHANGED_SIGNATURE.as_bytes())
}

pub(super) fn version_changed_topic0() -> String {
    keccak256_hex(VERSION_CHANGED_SIGNATURE.as_bytes())
}

pub(super) fn name_wrapped_topic0() -> String {
    keccak256_hex(NAME_WRAPPED_SIGNATURE.as_bytes())
}

pub(super) fn name_unwrapped_topic0() -> String {
    keccak256_hex(NAME_UNWRAPPED_SIGNATURE.as_bytes())
}

pub(super) fn fuses_set_topic0() -> String {
    keccak256_hex(FUSES_SET_SIGNATURE.as_bytes())
}

pub(super) fn expiry_extended_topic0() -> String {
    keccak256_hex(EXPIRY_EXTENDED_SIGNATURE.as_bytes())
}

pub(super) fn transfer_single_topic0() -> String {
    keccak256_hex(TRANSFER_SINGLE_SIGNATURE.as_bytes())
}

pub(super) fn hex_string(bytes: &[u8]) -> String {
    let mut output = String::from("0x");
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}
