use anyhow::{Result, bail};
use sqlx::types::time::OffsetDateTime;

use crate::ens_v2_common::keccak256_hex;
use crate::evm_abi::namehash_hex;

use super::types::{PreimageObservation, ResolverRawLogRow};
pub(super) use crate::ens_v2_common::{dns_decode_optional, hex_string};

pub(super) fn event_position_timestamp(raw_log: &ResolverRawLogRow) -> OffsetDateTime {
    raw_log.event_position_timestamp
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
