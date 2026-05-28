use super::*;
use bigname_domain::normalization::{
    NormalizedEnsName, normalize_dns_encoded_name, normalize_label_under_suffix, normalize_name,
};

const MAX_DNS_LABEL_OCTETS: usize = u8::MAX as usize;

pub(super) fn can_observe_registrar_label(label: &str) -> bool {
    !label.is_empty()
        && !label.contains('\0')
        && !label.contains('.')
        && label.len() <= MAX_DNS_LABEL_OCTETS
}

pub(super) fn observe_registrar_name_with_reference(
    label: &str,
    reference: &ObservationRef,
    normalizer_version: &str,
) -> Result<NameMetadata> {
    authority_profile_for_source_family(&reference.source_family)
        .with_context(|| {
            format!(
                "unsupported authority source family {}",
                reference.source_family
            )
        })?
        .observe_name(label, normalizer_version)
}

pub(super) fn observe_registrar_name_with_version(
    label: &str,
    profile: AuthorityProfile,
    normalizer_version: &str,
) -> Result<NameMetadata> {
    if label.is_empty() {
        bail!("registrar label must not be empty");
    }
    if !can_observe_registrar_label(label) {
        bail!(
            "registrar label is empty, contains a NUL byte, contains a dot, or exceeds DNS length"
        );
    }
    let suffix_labels = match profile {
        AuthorityProfile::Ens => &["eth"][..],
        AuthorityProfile::Basenames => &["base", "eth"][..],
    };
    let normalized = normalize_label_under_suffix(label, suffix_labels)
        .with_context(|| format!("failed to ENSIP-15 normalize registrar label {label:?}"))?;
    Ok(name_metadata(
        profile.namespace(),
        normalized,
        normalizer_version,
    ))
}

pub(super) fn observe_dns_encoded_name_with_reference(
    bytes: &[u8],
    reference: &ObservationRef,
    normalizer_version: &str,
) -> Result<NameMetadata> {
    let normalized = normalize_dns_encoded_name(bytes)
        .context("failed to ENSIP-15 normalize DNS-encoded name")?;
    Ok(name_metadata(
        &reference.namespace,
        normalized,
        normalizer_version,
    ))
}

pub(super) fn observe_text_name_with_reference(
    raw_name: &str,
    reference: &ObservationRef,
    normalizer_version: &str,
) -> Result<NameMetadata> {
    let normalized = normalize_name(raw_name)
        .with_context(|| format!("failed to ENSIP-15 normalize text name preimage {raw_name:?}"))?;
    Ok(name_metadata(
        &reference.namespace,
        normalized,
        normalizer_version,
    ))
}

fn name_metadata(
    namespace: &str,
    normalized: NormalizedEnsName,
    _normalizer_version: &str,
) -> NameMetadata {
    let normalized_label_bytes = normalized
        .normalized_labels
        .iter()
        .map(|label| label.as_bytes().to_vec())
        .collect::<Vec<_>>();
    let labelhashes = normalized_label_bytes
        .iter()
        .map(|label| keccak256_hex(label))
        .collect::<Vec<_>>();
    NameMetadata {
        namespace: namespace.to_owned(),
        logical_name_id: format!("{namespace}:{}", normalized.normalized_name),
        input_name: normalized.input_name,
        canonical_display_name: normalized.canonical_display_name,
        normalized_name: normalized.normalized_name,
        dns_encoded_name: normalized.dns_encoded_name,
        namehash: namehash_hex(&normalized_label_bytes),
        labelhashes,
        normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
    }
}

#[cfg(test)]
pub(super) fn observe_registrar_eth_name_with_version(
    label: &str,
    normalizer_version: &str,
) -> Result<NameMetadata> {
    observe_registrar_name_with_version(label, AuthorityProfile::Ens, normalizer_version)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_reference() -> ObservationRef {
        ObservationRef {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            block_number: 1,
            block_timestamp: OffsetDateTime::UNIX_EPOCH,
            transaction_hash: Some(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
            ),
            transaction_index: Some(0),
            log_index: Some(0),
            canonicality_state: CanonicalityState::Canonical,
            namespace: "ens".to_owned(),
            source_manifest_id: 1,
            source_family: "ens_v1_resolver_l1".to_owned(),
            manifest_version: 1,
        }
    }

    #[test]
    fn registrar_labels_with_nul_are_not_observable_names() {
        assert!(!can_observe_registrar_label("bad\0label"));
        assert!(
            observe_registrar_name_with_version(
                "bad\0label",
                AuthorityProfile::Ens,
                ENS_NORMALIZER_VERSION,
            )
            .is_err()
        );
    }

    #[test]
    fn registrar_labels_with_dots_are_not_observable_names() {
        assert!(!can_observe_registrar_label("sub.name"));
        assert!(
            observe_registrar_name_with_version(
                "sub.name",
                AuthorityProfile::Ens,
                ENS_NORMALIZER_VERSION,
            )
            .is_err()
        );
    }

    #[test]
    fn resolver_text_name_preimages_with_nul_are_not_observable_names() {
        assert!(
            observe_text_name_with_reference(
                "bad\0label.eth",
                &test_reference(),
                ENS_NORMALIZER_VERSION,
            )
            .is_err()
        );
    }

    #[test]
    fn wrapper_dns_labels_with_nul_are_not_observable_names() {
        let dns_name = [3, b'b', 0, b'd', 3, b'e', b't', b'h', 0];

        assert!(
            observe_dns_encoded_name_with_reference(
                &dns_name,
                &test_reference(),
                ENS_NORMALIZER_VERSION,
            )
            .is_err()
        );
    }

    #[test]
    fn wrapper_dns_name_preserves_onchain_hash_and_normalizes_display() -> Result<()> {
        let dns_name = [5, b'A', b'l', b'i', b'c', b'e', 3, b'e', b't', b'h', 0];

        let wrapper_name = observe_dns_encoded_name_with_reference(
            &dns_name,
            &test_reference(),
            ENS_NORMALIZER_VERSION,
        )?;

        assert_eq!(wrapper_name.input_name, "Alice.eth");
        assert_eq!(wrapper_name.normalized_name, "alice.eth");
        assert_eq!(
            wrapper_name.namehash,
            namehash_hex(&[b"alice".to_vec(), b"eth".to_vec()])
        );
        assert_eq!(wrapper_name.labelhashes[0], keccak256_hex(b"alice"));
        Ok(())
    }
}
