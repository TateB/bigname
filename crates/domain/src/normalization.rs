use std::{error::Error, fmt};

pub const ENS_NORMALIZER_VERSION: &str = "ensip15@ens-normalize-0.1.1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedEnsName {
    pub input_name: String,
    pub canonical_display_name: String,
    pub normalized_name: String,
    pub normalized_labels: Vec<String>,
    pub dns_encoded_name: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsNameNormalizationError {
    message: String,
}

impl EnsNameNormalizationError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for EnsNameNormalizationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for EnsNameNormalizationError {}

pub type Result<T> = std::result::Result<T, EnsNameNormalizationError>;

pub fn normalize_name(input_name: &str) -> Result<NormalizedEnsName> {
    let normalized_name = ens_normalize::ens_normalize(input_name)
        .map_err(|error| EnsNameNormalizationError::new(error.message()))?;
    normalized_name_metadata(input_name, normalized_name)
}

pub fn normalize_label_under_suffix(
    label: &str,
    suffix_labels: &[&str],
) -> Result<NormalizedEnsName> {
    if label.contains('.') {
        return Err(EnsNameNormalizationError::new(
            "name label must not contain dots",
        ));
    }
    if suffix_labels
        .iter()
        .any(|suffix_label| suffix_label.is_empty() || suffix_label.contains('.'))
    {
        return Err(EnsNameNormalizationError::new(
            "name suffix must contain non-empty labels without dots",
        ));
    }

    let input_name = std::iter::once(label)
        .chain(suffix_labels.iter().copied())
        .collect::<Vec<_>>()
        .join(".");
    normalize_name(&input_name)
}

pub fn normalize_dns_encoded_name(bytes: &[u8]) -> Result<NormalizedEnsName> {
    let input_name = decode_dns_encoded_name(bytes)?;
    normalize_name(&input_name)
}

fn normalized_name_metadata(
    input_name: &str,
    normalized_name: String,
) -> Result<NormalizedEnsName> {
    let normalized_labels = normalized_labels(&normalized_name)?;
    let dns_encoded_name = dns_encode_labels(&normalized_labels)?;
    let canonical_display_name = ens_normalize::ens_beautify(input_name)
        .map_err(|error| EnsNameNormalizationError::new(error.message()))?;

    Ok(NormalizedEnsName {
        input_name: input_name.to_owned(),
        canonical_display_name,
        normalized_name,
        normalized_labels,
        dns_encoded_name,
    })
}

fn normalized_labels(normalized_name: &str) -> Result<Vec<String>> {
    if normalized_name.is_empty()
        || normalized_name.starts_with('.')
        || normalized_name.ends_with('.')
    {
        return Err(EnsNameNormalizationError::new(
            "name must contain non-empty dot-separated labels",
        ));
    }

    let labels = normalized_name
        .split('.')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if labels.iter().any(|label| label.is_empty()) {
        return Err(EnsNameNormalizationError::new(
            "name must contain non-empty dot-separated labels",
        ));
    }
    Ok(labels)
}

fn dns_encode_labels(labels: &[String]) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    for label in labels {
        let length = u8::try_from(label.len())
            .map_err(|_| EnsNameNormalizationError::new("name label exceeds DNS length"))?;
        if length == 0 {
            return Err(EnsNameNormalizationError::new(
                "name must contain non-empty dot-separated labels",
            ));
        }
        output.push(length);
        output.extend_from_slice(label.as_bytes());
    }
    output.push(0);
    Ok(output)
}

fn decode_dns_encoded_name(bytes: &[u8]) -> Result<String> {
    if bytes.is_empty() {
        return Err(EnsNameNormalizationError::new(
            "DNS-encoded name payload must not be empty",
        ));
    }

    let mut labels = Vec::new();
    let mut cursor = 0usize;
    loop {
        if cursor >= bytes.len() {
            return Err(EnsNameNormalizationError::new(
                "DNS-encoded name payload is missing the root terminator",
            ));
        }
        let label_length = usize::from(bytes[cursor]);
        cursor += 1;
        if label_length == 0 {
            if cursor != bytes.len() {
                return Err(EnsNameNormalizationError::new(
                    "DNS-encoded name payload has trailing bytes after the root terminator",
                ));
            }
            break;
        }
        if cursor + label_length > bytes.len() {
            return Err(EnsNameNormalizationError::new(
                "DNS-encoded name label exceeds the available payload",
            ));
        }
        let label =
            String::from_utf8(bytes[cursor..cursor + label_length].to_vec()).map_err(|_| {
                EnsNameNormalizationError::new("DNS-encoded name labels must be valid UTF-8")
            })?;
        if label.contains('.') {
            return Err(EnsNameNormalizationError::new(
                "DNS-encoded name labels must not contain dots",
            ));
        }
        labels.push(label);
        cursor += label_length;
    }

    if labels.is_empty() {
        return Err(EnsNameNormalizationError::new(
            "DNS-encoded name must not be the root",
        ));
    }
    Ok(labels.join("."))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_ascii_and_tracks_display() {
        let name = normalize_name("RaFfY.eth").expect("name normalizes");

        assert_eq!(name.input_name, "RaFfY.eth");
        assert_eq!(name.normalized_name, "raffy.eth");
        assert_eq!(name.canonical_display_name, "raffy.eth");
        assert_eq!(name.normalized_labels, ["raffy", "eth"]);
        assert_eq!(
            name.dns_encoded_name,
            [5, b'r', b'a', b'f', b'f', b'y', 3, b'e', b't', b'h', 0]
        );
    }

    #[test]
    fn rejects_disallowed_joiner_sequence() {
        let error = normalize_name("Ni\u{200d}ck.eth").expect_err("name rejects");

        assert!(!error.message().is_empty());
    }

    #[test]
    fn normalizes_emoji_presentation() {
        let name = normalize_name("🅰️🅱.eth").expect("emoji name normalizes");

        assert_eq!(name.normalized_name, "🅰🅱.eth");
        assert_eq!(name.canonical_display_name, "🅰️🅱️.eth");
    }

    #[test]
    fn decodes_dns_encoded_names_through_ensip15() {
        let name =
            normalize_dns_encoded_name(&[5, b'A', b'l', b'i', b'c', b'e', 3, b'e', b't', b'h', 0])
                .expect("DNS name normalizes");

        assert_eq!(name.input_name, "Alice.eth");
        assert_eq!(name.normalized_name, "alice.eth");
        assert_eq!(
            name.dns_encoded_name,
            [5, b'a', b'l', b'i', b'c', b'e', 3, b'e', b't', b'h', 0]
        );
    }

    #[test]
    fn label_normalization_rejects_dot_inside_label() {
        let error = normalize_label_under_suffix("sub.name", &["eth"])
            .expect_err("dot-containing label rejects");

        assert_eq!(error.message(), "name label must not contain dots");
    }

    #[test]
    fn dns_normalization_rejects_dot_inside_label() {
        let error = normalize_dns_encoded_name(&[3, b'a', b'.', b'b', 3, b'e', b't', b'h', 0])
            .expect_err("dot-containing DNS label rejects");

        assert_eq!(
            error.message(),
            "DNS-encoded name labels must not contain dots"
        );
    }
}
