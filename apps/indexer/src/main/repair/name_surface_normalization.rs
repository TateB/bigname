use std::fmt::Write as _;

use alloy_primitives::keccak256;
use anyhow::{Context, Result, bail};
use bigname_domain::normalization::{ENS_NORMALIZER_VERSION, normalize_name};
use serde_json::{Value, json};
use sqlx::PgPool;
use tracing::info;

#[path = "name_surface_normalization/storage.rs"]
mod storage;

use storage::{
    clear_compatible_name_surface_findings, count_name_surfaces_with_old_normalizer,
    load_name_surface_normalization_page, update_compatible_name_surfaces,
    upsert_name_surface_normalization_findings,
};

pub(crate) const DEFAULT_NAME_SURFACE_NORMALIZATION_REPAIR_PAGE_SIZE: i64 = 10_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NameSurfaceNormalizationRepairConfig {
    pub(crate) expected_normalizer_version: String,
    pub(crate) page_size: i64,
    pub(crate) limit: Option<i64>,
    pub(crate) apply_compatible: bool,
    pub(crate) record_findings: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct NameSurfaceNormalizationRepairOutcome {
    pub(crate) scanned_count: i64,
    pub(crate) compatible_count: i64,
    pub(crate) updated_compatible_count: i64,
    pub(crate) rejected_count: i64,
    pub(crate) incompatible_count: i64,
    pub(crate) recorded_finding_count: i64,
    pub(crate) remaining_old_normalizer_count: i64,
}

#[derive(Clone, Debug)]
struct NameSurfaceNormalizationRow {
    logical_name_id: String,
    namespace: String,
    input_name: String,
    canonical_display_name: String,
    normalized_name: String,
    dns_encoded_name: Vec<u8>,
    namehash: String,
    labelhashes: Vec<String>,
    normalizer_version: String,
    normalization_errors: Value,
}

#[derive(Clone, Debug)]
struct CompatibleNameSurfaceUpdate {
    logical_name_id: String,
    input_name: String,
    canonical_display_name: String,
    namespace: String,
    normalized_name: String,
    dns_encoded_name: Vec<u8>,
    namehash: String,
    labelhashes: Vec<String>,
}

#[derive(Clone, Debug)]
struct NameSurfaceNormalizationFinding {
    logical_name_id: String,
    expected_normalizer_version: String,
    finding_kind: &'static str,
    current_normalizer_version: String,
    namespace: String,
    input_name: String,
    current_normalized_name: String,
    candidate_logical_name_id: Option<String>,
    candidate_normalized_name: Option<String>,
    error_message: Option<String>,
    details: Value,
}

#[derive(Clone, Debug)]
struct NameSurfaceCandidate {
    logical_name_id: String,
    input_name: String,
    canonical_display_name: String,
    normalized_name: String,
    dns_encoded_name: Vec<u8>,
    namehash: String,
    labelhashes: Vec<String>,
}

enum NameSurfaceNormalizationAction {
    Compatible(CompatibleNameSurfaceUpdate),
    Finding(NameSurfaceNormalizationFinding),
}

pub(crate) async fn repair_name_surface_normalization(
    pool: &PgPool,
    config: NameSurfaceNormalizationRepairConfig,
) -> Result<NameSurfaceNormalizationRepairOutcome> {
    validate_name_surface_normalization_repair_config(&config)?;

    let mut outcome = NameSurfaceNormalizationRepairOutcome::default();
    let mut after_logical_name_id = None::<String>;

    while config
        .limit
        .is_none_or(|limit| outcome.scanned_count < limit)
    {
        let remaining_limit = config
            .limit
            .map(|limit| limit.saturating_sub(outcome.scanned_count));
        let page_size = remaining_limit
            .map(|remaining| remaining.min(config.page_size))
            .unwrap_or(config.page_size);
        if page_size <= 0 {
            break;
        }

        let page = load_name_surface_normalization_page(
            pool,
            &config.expected_normalizer_version,
            after_logical_name_id.as_deref(),
            page_size,
        )
        .await?;
        if page.is_empty() {
            break;
        }
        after_logical_name_id = page.last().map(|row| row.logical_name_id.clone());

        let mut compatible = Vec::new();
        let mut findings = Vec::new();
        for row in page {
            outcome.scanned_count += 1;
            match classify_name_surface_normalization(&row, &config.expected_normalizer_version) {
                NameSurfaceNormalizationAction::Compatible(update) => {
                    outcome.compatible_count += 1;
                    compatible.push(update);
                }
                NameSurfaceNormalizationAction::Finding(finding) => {
                    match finding.finding_kind {
                        "rejected" => outcome.rejected_count += 1,
                        _ => outcome.incompatible_count += 1,
                    }
                    findings.push(finding);
                }
            }
        }

        if config.record_findings && (!compatible.is_empty() || !findings.is_empty()) {
            let mut transaction = pool
                .begin()
                .await
                .context("failed to open name-surface normalization repair transaction")?;
            if config.apply_compatible && !compatible.is_empty() {
                let updated_logical_name_ids = update_compatible_name_surfaces(
                    &mut transaction,
                    &compatible,
                    &config.expected_normalizer_version,
                )
                .await?;
                outcome.updated_compatible_count +=
                    i64::try_from(updated_logical_name_ids.len())
                        .context("updated compatible name-surface count overflowed")?;
                clear_compatible_name_surface_findings(
                    &mut transaction,
                    &updated_logical_name_ids,
                    &config.expected_normalizer_version,
                )
                .await?;
            }
            if !findings.is_empty() {
                let recorded =
                    upsert_name_surface_normalization_findings(&mut transaction, &findings).await?;
                outcome.recorded_finding_count += recorded;
            }
            transaction
                .commit()
                .await
                .context("failed to commit name-surface normalization repair page")?;
        }

        info!(
            service = "indexer",
            command = "repair name-surface-normalization",
            scanned_count = outcome.scanned_count,
            compatible_count = outcome.compatible_count,
            updated_compatible_count = outcome.updated_compatible_count,
            rejected_count = outcome.rejected_count,
            incompatible_count = outcome.incompatible_count,
            recorded_finding_count = outcome.recorded_finding_count,
            "name-surface normalization repair page completed"
        );
    }

    outcome.remaining_old_normalizer_count =
        count_name_surfaces_with_old_normalizer(pool, &config.expected_normalizer_version).await?;

    Ok(outcome)
}

fn validate_name_surface_normalization_repair_config(
    config: &NameSurfaceNormalizationRepairConfig,
) -> Result<()> {
    if config.expected_normalizer_version.trim().is_empty() {
        bail!("name-surface normalization repair expected normalizer must not be empty");
    }
    if config.expected_normalizer_version != ENS_NORMALIZER_VERSION {
        bail!(
            "name-surface normalization repair expected normalizer {} does not match build normalizer {}",
            config.expected_normalizer_version,
            ENS_NORMALIZER_VERSION
        );
    }
    if config.apply_compatible && !config.record_findings {
        bail!("name-surface normalization repair --apply-compatible requires --record-findings");
    }
    if config.page_size <= 0 {
        bail!(
            "name-surface normalization repair page_size must be positive, got {}",
            config.page_size
        );
    }
    if let Some(limit) = config.limit
        && limit <= 0
    {
        bail!("name-surface normalization repair limit must be positive, got {limit}");
    }
    Ok(())
}

fn classify_name_surface_normalization(
    row: &NameSurfaceNormalizationRow,
    expected_normalizer_version: &str,
) -> NameSurfaceNormalizationAction {
    let candidate = match normalize_name(&row.input_name) {
        Ok(normalized) => NameSurfaceCandidate {
            logical_name_id: format!("{}:{}", row.namespace, normalized.normalized_name),
            input_name: normalized.input_name,
            canonical_display_name: normalized.canonical_display_name,
            normalized_name: normalized.normalized_name,
            dns_encoded_name: normalized.dns_encoded_name,
            namehash: namehash_hex(&normalized.normalized_labels),
            labelhashes: normalized
                .normalized_labels
                .iter()
                .map(|label| keccak256_hex(label.as_bytes()))
                .collect(),
        },
        Err(error) => {
            return NameSurfaceNormalizationAction::Finding(NameSurfaceNormalizationFinding {
                logical_name_id: row.logical_name_id.clone(),
                expected_normalizer_version: expected_normalizer_version.to_owned(),
                finding_kind: "rejected",
                current_normalizer_version: row.normalizer_version.clone(),
                namespace: row.namespace.clone(),
                input_name: row.input_name.clone(),
                current_normalized_name: row.normalized_name.clone(),
                candidate_logical_name_id: None,
                candidate_normalized_name: None,
                error_message: Some(error.message().to_owned()),
                details: json!({
                    "current": current_row_details(row),
                }),
            });
        }
    };

    let normalization_errors_empty = row
        .normalization_errors
        .as_array()
        .is_some_and(|errors| errors.is_empty());
    let compatible = row.logical_name_id == candidate.logical_name_id
        && row.normalized_name == candidate.normalized_name
        && row.dns_encoded_name == candidate.dns_encoded_name
        && row.namehash.eq_ignore_ascii_case(&candidate.namehash)
        && string_arrays_equal_ignore_ascii_case(&row.labelhashes, &candidate.labelhashes)
        && normalization_errors_empty;

    if compatible {
        return NameSurfaceNormalizationAction::Compatible(CompatibleNameSurfaceUpdate {
            logical_name_id: row.logical_name_id.clone(),
            input_name: candidate.input_name,
            canonical_display_name: candidate.canonical_display_name,
            namespace: row.namespace.clone(),
            normalized_name: row.normalized_name.clone(),
            dns_encoded_name: row.dns_encoded_name.clone(),
            namehash: row.namehash.clone(),
            labelhashes: row.labelhashes.clone(),
        });
    }

    NameSurfaceNormalizationAction::Finding(NameSurfaceNormalizationFinding {
        logical_name_id: row.logical_name_id.clone(),
        expected_normalizer_version: expected_normalizer_version.to_owned(),
        finding_kind: "incompatible",
        current_normalizer_version: row.normalizer_version.clone(),
        namespace: row.namespace.clone(),
        input_name: row.input_name.clone(),
        current_normalized_name: row.normalized_name.clone(),
        candidate_logical_name_id: Some(candidate.logical_name_id.clone()),
        candidate_normalized_name: Some(candidate.normalized_name.clone()),
        error_message: None,
        details: json!({
            "current": current_row_details(row),
            "candidate": {
                "logical_name_id": candidate.logical_name_id,
                "canonical_display_name": candidate.canonical_display_name,
                "normalized_name": candidate.normalized_name,
                "dns_encoded_name_hex": hex_bytes(&candidate.dns_encoded_name),
                "namehash": candidate.namehash,
                "labelhashes": candidate.labelhashes,
            },
            "mismatch": {
                "logical_name_id": row.logical_name_id != candidate.logical_name_id,
                "normalized_name": row.normalized_name != candidate.normalized_name,
                "dns_encoded_name": row.dns_encoded_name != candidate.dns_encoded_name,
                "namehash": !row.namehash.eq_ignore_ascii_case(&candidate.namehash),
                "labelhashes": !string_arrays_equal_ignore_ascii_case(
                    &row.labelhashes,
                    &candidate.labelhashes,
                ),
                "normalization_errors": !normalization_errors_empty,
            }
        }),
    })
}

fn current_row_details(row: &NameSurfaceNormalizationRow) -> Value {
    json!({
        "logical_name_id": row.logical_name_id,
        "canonical_display_name": row.canonical_display_name,
        "normalized_name": row.normalized_name,
        "dns_encoded_name_hex": hex_bytes(&row.dns_encoded_name),
        "namehash": row.namehash,
        "labelhashes": row.labelhashes,
        "normalization_errors": row.normalization_errors,
    })
}

fn namehash_hex(labels: &[String]) -> String {
    let mut node = [0u8; 32];
    for label in labels.iter().rev() {
        let label_hash = keccak256(label.as_bytes());
        let mut combined = [0u8; 64];
        combined[..32].copy_from_slice(&node);
        combined[32..].copy_from_slice(label_hash.as_slice());
        node.copy_from_slice(keccak256(combined).as_slice());
    }
    hex_bytes(&node)
}

fn keccak256_hex(bytes: &[u8]) -> String {
    let digest = keccak256(bytes);
    hex_bytes(digest.as_slice())
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(2 + bytes.len() * 2);
    output.push_str("0x");
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn string_arrays_equal_ignore_ascii_case(left: &[String], right: &[String]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(input_name: &str, normalized_name: &str) -> NameSurfaceNormalizationRow {
        NameSurfaceNormalizationRow {
            logical_name_id: format!("ens:{normalized_name}"),
            namespace: "ens".to_owned(),
            input_name: input_name.to_owned(),
            canonical_display_name: normalized_name.to_owned(),
            normalized_name: normalized_name.to_owned(),
            dns_encoded_name: vec![5, b'a', b'l', b'i', b'c', b'e', 3, b'e', b't', b'h', 0],
            namehash: namehash_hex(&["alice".to_owned(), "eth".to_owned()]),
            labelhashes: vec![keccak256_hex(b"alice"), keccak256_hex(b"eth")],
            normalizer_version: "ensip15@2026-04-16".to_owned(),
            normalization_errors: json!([]),
        }
    }

    #[test]
    fn classifies_compatible_case_only_surface() {
        let action = classify_name_surface_normalization(
            &row("Alice.eth", "alice.eth"),
            ENS_NORMALIZER_VERSION,
        );
        assert!(matches!(
            action,
            NameSurfaceNormalizationAction::Compatible(_)
        ));
    }

    #[test]
    fn classifies_rejected_surface() {
        let action = classify_name_surface_normalization(
            &row("bad name.eth", "bad name.eth"),
            ENS_NORMALIZER_VERSION,
        );
        assert!(matches!(
            action,
            NameSurfaceNormalizationAction::Finding(NameSurfaceNormalizationFinding {
                finding_kind: "rejected",
                ..
            })
        ));
    }

    #[test]
    fn apply_compatible_requires_record_findings() {
        let error = validate_name_surface_normalization_repair_config(
            &NameSurfaceNormalizationRepairConfig {
                expected_normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
                page_size: DEFAULT_NAME_SURFACE_NORMALIZATION_REPAIR_PAGE_SIZE,
                limit: None,
                apply_compatible: true,
                record_findings: false,
            },
        )
        .expect_err("apply without findings rejects");

        assert!(
            error
                .to_string()
                .contains("--apply-compatible requires --record-findings")
        );
    }
}
