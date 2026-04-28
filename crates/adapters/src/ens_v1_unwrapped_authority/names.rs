use super::*;

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
    let normalized_label = label.to_ascii_lowercase();
    let safe_label = postgres_text_safe(label);
    let safe_normalized_label = postgres_text_safe(&normalized_label);
    let (normalized_name, input_name, parent_labels) = match profile {
        AuthorityProfile::Ens => (
            format!("{safe_normalized_label}.eth"),
            format!("{safe_label}.eth"),
            vec![b"eth".to_vec()],
        ),
        AuthorityProfile::Basenames => (
            format!("{safe_normalized_label}.base.eth"),
            format!("{safe_label}.base.eth"),
            vec![b"base".to_vec(), b"eth".to_vec()],
        ),
    };
    let label_length =
        u8::try_from(normalized_label.len()).context("registrar label exceeds DNS length")?;
    let dns_capacity = 2
        + normalized_label.len()
        + parent_labels
            .iter()
            .map(|label| 1 + label.len())
            .sum::<usize>();
    let mut dns_name = Vec::with_capacity(dns_capacity);
    dns_name.push(label_length);
    dns_name.extend_from_slice(normalized_label.as_bytes());
    for label in &parent_labels {
        dns_name
            .push(u8::try_from(label.len()).context("registrar suffix label exceeds DNS length")?);
        dns_name.extend_from_slice(label);
    }
    dns_name.push(0);
    let mut namehash_labels = Vec::with_capacity(1 + parent_labels.len());
    namehash_labels.push(normalized_label.as_bytes().to_vec());
    namehash_labels.extend(parent_labels.iter().cloned());
    let mut labelhashes = Vec::with_capacity(namehash_labels.len());
    for label in &namehash_labels {
        labelhashes.push(keccak256_hex(label));
    }
    Ok(NameMetadata {
        namespace: profile.namespace().to_owned(),
        logical_name_id: format!("{}:{normalized_name}", profile.namespace()),
        input_name: input_name.clone(),
        canonical_display_name: normalized_name.clone(),
        normalized_name: normalized_name.clone(),
        dns_encoded_name: dns_name.clone(),
        namehash: namehash_hex(&namehash_labels),
        labelhashes,
        normalizer_version: normalizer_version.to_owned(),
    })
}

pub(super) fn observe_dns_encoded_name_with_reference(
    bytes: &[u8],
    reference: &ObservationRef,
    normalizer_version: &str,
) -> Result<NameMetadata> {
    let labels = decode_dns_encoded_labels(bytes)?;
    if labels.is_empty() {
        bail!("wrapper name must not be the DNS root");
    }
    let input_labels = labels
        .iter()
        .map(|label| String::from_utf8(label.clone()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("wrapper DNS name labels must be valid UTF-8")?;
    let normalized_labels = input_labels
        .iter()
        .map(|label| label.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let normalized_label_bytes = normalized_labels
        .iter()
        .map(|label| label.as_bytes().to_vec())
        .collect::<Vec<_>>();
    let mut dns_name = Vec::new();
    for label in &normalized_label_bytes {
        dns_name.push(u8::try_from(label.len()).context("wrapper label exceeds DNS length")?);
        dns_name.extend_from_slice(label);
    }
    dns_name.push(0);
    let normalized_name = normalized_labels
        .iter()
        .map(|label| postgres_text_safe(label))
        .collect::<Vec<_>>()
        .join(".");
    let input_name = input_labels
        .iter()
        .map(|label| postgres_text_safe(label))
        .collect::<Vec<_>>()
        .join(".");
    Ok(NameMetadata {
        namespace: reference.namespace.clone(),
        logical_name_id: format!("{}:{normalized_name}", reference.namespace),
        input_name,
        canonical_display_name: normalized_name.clone(),
        normalized_name,
        dns_encoded_name: dns_name,
        namehash: namehash_hex(&labels),
        labelhashes: labels.iter().map(|label| keccak256_hex(label)).collect(),
        normalizer_version: normalizer_version.to_owned(),
    })
}

fn postgres_text_safe(text: &str) -> String {
    text.replace('\0', "\\u0000")
}

fn decode_dns_encoded_labels(bytes: &[u8]) -> Result<Vec<Vec<u8>>> {
    if bytes.is_empty() {
        bail!("dns-encoded name payload must not be empty");
    }

    let mut labels = Vec::<Vec<u8>>::new();
    let mut cursor = 0usize;
    loop {
        if cursor >= bytes.len() {
            bail!("dns-encoded name payload is missing the root terminator");
        }
        let label_length = usize::from(bytes[cursor]);
        cursor += 1;
        if label_length == 0 {
            if cursor != bytes.len() {
                bail!("dns-encoded name payload has trailing bytes after the root terminator");
            }
            break;
        }
        if cursor + label_length > bytes.len() {
            bail!("dns-encoded name label exceeds the available payload");
        }
        labels.push(bytes[cursor..cursor + label_length].to_vec());
        cursor += label_length;
    }
    Ok(labels)
}

#[cfg(test)]
pub(super) fn observe_registrar_eth_name_with_version(
    label: &str,
    normalizer_version: &str,
) -> Result<NameMetadata> {
    observe_registrar_name_with_version(label, AuthorityProfile::Ens, normalizer_version)
}
