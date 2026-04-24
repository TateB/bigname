use anyhow::{Context, Result, bail};
use bigname_storage::CanonicalityState;
use sha3::{Digest, Keccak256};
use sqlx::types::time::OffsetDateTime;

use super::types::{PreimageObservation, ResolverRawLogRow};

pub(super) fn normalize_hex_32(value: &str) -> Result<String> {
    let normalized = value.to_ascii_lowercase();
    let normalized = if normalized.starts_with("0x") {
        normalized
    } else {
        format!("0x{normalized}")
    };
    if normalized.len() != 66 {
        bail!("expected 32-byte hex value, got {normalized}");
    }
    Ok(normalized)
}

pub(super) fn normalize_address(value: &str) -> String {
    value.to_ascii_lowercase()
}

pub(super) fn parse_canonicality_state(value: &str) -> Result<CanonicalityState> {
    match value {
        "observed" => Ok(CanonicalityState::Observed),
        "canonical" => Ok(CanonicalityState::Canonical),
        "safe" => Ok(CanonicalityState::Safe),
        "finalized" => Ok(CanonicalityState::Finalized),
        "orphaned" => Ok(CanonicalityState::Orphaned),
        _ => bail!("unknown canonicality_state value {value}"),
    }
}

pub(super) fn event_position_timestamp(raw_log: &ResolverRawLogRow) -> OffsetDateTime {
    raw_log.event_position_timestamp
}

pub(super) fn dns_decode_optional(bytes: &[u8]) -> Result<Option<String>> {
    if bytes.is_empty() {
        Ok(None)
    } else {
        dns_decode(bytes).map(Some)
    }
}

fn dns_decode(bytes: &[u8]) -> Result<String> {
    let mut labels = Vec::new();
    let mut index = 0usize;
    while index < bytes.len() {
        let length = bytes[index] as usize;
        index += 1;
        if length == 0 {
            if index != bytes.len() {
                bail!("DNS-encoded name has trailing bytes");
            }
            return Ok(labels.join(".").to_ascii_lowercase());
        }
        let end = index + length;
        if end > bytes.len() {
            bail!("DNS-encoded name label exceeds payload length");
        }
        labels.push(
            String::from_utf8(bytes[index..end].to_vec())
                .context("DNS-encoded label is not valid UTF-8")?,
        );
        index = end;
    }
    bail!("DNS-encoded name is missing root label")
}

pub(super) fn observe_dns_encoded_name(bytes: &[u8]) -> Result<PreimageObservation> {
    if bytes.is_empty() {
        bail!("DNS-encoded name payload must not be empty");
    }

    let mut labels = Vec::<Vec<u8>>::new();
    let mut cursor = 0usize;
    loop {
        if cursor >= bytes.len() {
            bail!("DNS-encoded name payload is missing root label");
        }
        let label_length = usize::from(bytes[cursor]);
        cursor += 1;
        if label_length == 0 {
            if cursor != bytes.len() {
                bail!("DNS-encoded name payload has trailing bytes");
            }
            break;
        }
        if cursor + label_length > bytes.len() {
            bail!("DNS-encoded name label exceeds payload length");
        }
        labels.push(bytes[cursor..cursor + label_length].to_vec());
        cursor += label_length;
    }

    let decoded_name = labels
        .iter()
        .map(|label| String::from_utf8(label.clone()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .ok()
        .map(|labels| labels.join("."));
    let labelhashes = labels
        .iter()
        .map(|label| keccak256_hex(label))
        .collect::<Vec<_>>();

    Ok(PreimageObservation {
        dns_encoded_name: format!("0x{}", hex_string(bytes)),
        decoded_name,
        labelhashes,
        namehash: namehash_hex(&labels),
    })
}

fn namehash_hex(labels: &[Vec<u8>]) -> String {
    let mut node = [0u8; 32];
    for label in labels.iter().rev() {
        let label_hash = keccak256_bytes(label);
        let mut combined = [0u8; 64];
        combined[..32].copy_from_slice(&node);
        combined[32..].copy_from_slice(&label_hash);
        node = keccak256_bytes(&combined);
    }
    format!("0x{}", hex_string(node))
}

pub(super) fn display_name(name: &str) -> String {
    let mut labels = name.split('.');
    let Some(first) = labels.next() else {
        return name.to_owned();
    };
    let mut first_chars = first.chars();
    let display_first = match first_chars.next() {
        Some(first_char) => format!(
            "{}{}",
            first_char.to_uppercase(),
            first_chars.as_str().to_ascii_lowercase()
        ),
        None => first.to_owned(),
    };
    std::iter::once(display_first)
        .chain(labels.map(|label| label.to_ascii_lowercase()))
        .collect::<Vec<_>>()
        .join(".")
}

pub(super) fn logical_name_id(namespace: &str, name: &str) -> String {
    if name.is_empty() {
        format!("{namespace}:")
    } else {
        format!("{namespace}:{}", name.to_ascii_lowercase())
    }
}

pub(super) fn decimal_string_from_be_bytes(bytes: &[u8]) -> String {
    let mut digits = vec![0u8];
    for byte in bytes {
        let mut carry = *byte as u32;
        for digit in digits.iter_mut().rev() {
            let value = (*digit as u32) * 256 + carry;
            *digit = (value % 10) as u8;
            carry = value / 10;
        }
        while carry > 0 {
            digits.insert(0, (carry % 10) as u8);
            carry /= 10;
        }
    }
    digits
        .into_iter()
        .skip_while(|digit| *digit == 0)
        .map(|digit| char::from(b'0' + digit))
        .collect::<String>()
        .if_empty_then_zero()
}

trait EmptyThenZero {
    fn if_empty_then_zero(self) -> Self;
}

impl EmptyThenZero for String {
    fn if_empty_then_zero(self) -> Self {
        if self.is_empty() {
            "0".to_owned()
        } else {
            self
        }
    }
}

pub(super) fn keccak_signature_hex(signature: &str) -> String {
    format!("0x{}", hex_string(keccak256_bytes(signature.as_bytes())))
}

fn keccak256_hex(bytes: &[u8]) -> String {
    format!("0x{}", hex_string(keccak256_bytes(bytes)))
}

fn keccak256_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&digest);
    output
}

pub(super) fn hex_string(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
