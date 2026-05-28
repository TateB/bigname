use anyhow::{Result, bail};
use bigname_domain::normalization::normalize_dns_encoded_name;
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

    let normalized = normalize_dns_encoded_name(bytes)?;
    let normalized_labels = normalized
        .normalized_labels
        .iter()
        .map(|label| label.as_bytes().to_vec())
        .collect::<Vec<_>>();
    let labelhashes = normalized_labels
        .iter()
        .map(|label| keccak256_hex(label))
        .collect::<Vec<_>>();

    Ok(PreimageObservation {
        dns_encoded_name: format!("0x{}", hex_string(bytes)),
        decoded_name: Some(normalized.normalized_name),
        labelhashes,
        namehash: namehash_hex(&normalized_labels),
    })
}

pub(super) fn logical_name_id(namespace: &str, name: &str) -> String {
    if name.is_empty() {
        format!("{namespace}:")
    } else {
        format!("{namespace}:{name}")
    }
}
