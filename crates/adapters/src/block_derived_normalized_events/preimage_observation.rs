use anyhow::{Result, bail};
use bigname_domain::normalization::{normalize_dns_encoded_name, normalize_label_under_suffix};

use super::decoding::{hex_string, keccak256_hex, namehash_hex};
use super::types::PreimageObservation;

const MAX_DNS_LABEL_OCTETS: usize = u8::MAX as usize;

pub(super) fn can_observe_dns_label(label: &str) -> bool {
    !label.is_empty()
        && !label.contains('\0')
        && !label.contains('.')
        && label.len() <= MAX_DNS_LABEL_OCTETS
}

pub(super) fn observe_dns_encoded_name(bytes: &[u8]) -> Result<PreimageObservation> {
    if bytes.is_empty() {
        bail!("dns-encoded name payload must not be empty");
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
    let namehash = namehash_hex(&normalized_labels);

    Ok(PreimageObservation {
        dns_encoded_name: hex_string(bytes),
        decoded_name: Some(normalized.normalized_name),
        labelhashes,
        namehash,
    })
}

pub(super) fn observe_registrar_eth_name(label: &str) -> Result<PreimageObservation> {
    if label.is_empty() {
        bail!("registrar label must not be empty");
    }

    let normalized = normalize_label_under_suffix(label, &["eth"])?;

    observe_dns_encoded_name(&normalized.dns_encoded_name)
}

pub(super) fn observe_registrar_base_name(label: &str) -> Result<PreimageObservation> {
    if label.is_empty() {
        bail!("registrar label must not be empty");
    }

    let normalized = normalize_label_under_suffix(label, &["base", "eth"])?;

    observe_dns_encoded_name(&normalized.dns_encoded_name)
}

pub(super) fn observe_single_label(label: &str) -> Result<PreimageObservation> {
    if label.is_empty() {
        bail!("label must not be empty");
    }

    let normalized = normalize_label_under_suffix(label, &[])?;

    observe_dns_encoded_name(&normalized.dns_encoded_name)
}
