use anyhow::{Context, Result, bail};

use super::types::{PrimaryNameClaimStatus, PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot};

pub(super) fn validate_primary_name_current_row(row: &PrimaryNameCurrentRow) -> Result<()> {
    if row.address.trim().is_empty() {
        bail!("primary_names_current row must include address");
    }
    if row.namespace.trim().is_empty() {
        bail!(
            "primary_names_current row for address {} must include namespace",
            row.address
        );
    }
    if row.coin_type.trim().is_empty() {
        bail!(
            "primary_names_current row for address {} namespace {} must include coin_type",
            row.address,
            row.namespace
        );
    }
    match row.claim_status {
        PrimaryNameClaimStatus::InvalidName => {
            let raw_claim_name = row
                .raw_claim_name
                .as_deref()
                .context("primary_names_current invalid_name rows must include raw_claim_name")?;
            if raw_claim_name.trim().is_empty() {
                bail!("primary_names_current invalid_name raw_claim_name must not be blank");
            }
        }
        _ if row.raw_claim_name.is_some() => {
            bail!(
                "primary_names_current rows may include raw_claim_name only for claim_status invalid_name"
            );
        }
        _ => {}
    }
    if !row.claim_provenance.is_object() {
        bail!(
            "primary_names_current row for address {} namespace {} coin_type {} must store claim_provenance as a JSON object",
            row.address,
            row.namespace,
            row.coin_type
        );
    }

    Ok(())
}

pub(super) fn validate_primary_name_current_snapshot(
    snapshot: &PrimaryNameCurrentSnapshot,
) -> Result<()> {
    validate_primary_name_current_row(&snapshot.row)?;

    let normalized_claim_name = snapshot.normalized_claim_name.as_deref();
    if normalized_claim_name.is_some()
        && snapshot.row.claim_status != PrimaryNameClaimStatus::Success
    {
        bail!(
            "primary_names_current normalized_claim_name may appear only for claim_status success"
        );
    }
    if normalized_claim_name.is_some_and(|value| value.trim().is_empty()) {
        bail!(
            "primary_names_current normalized_claim_name for address {} namespace {} coin_type {} must not be blank",
            snapshot.row.address,
            snapshot.row.namespace,
            snapshot.row.coin_type
        );
    }
    if let Some(value) = normalized_claim_name {
        let normalized = bigname_domain::normalization::normalize_name(value).map_err(|error| {
            anyhow::anyhow!(
                "primary_names_current normalized_claim_name for address {} namespace {} coin_type {} is not ENSIP-15-normalized: {}",
                snapshot.row.address,
                snapshot.row.namespace,
                snapshot.row.coin_type,
                error
            )
        })?;
        if normalized.normalized_name != value {
            bail!(
                "primary_names_current normalized_claim_name for address {} namespace {} coin_type {} must already be ENSIP-15-normalized",
                snapshot.row.address,
                snapshot.row.namespace,
                snapshot.row.coin_type
            );
        }
    }

    Ok(())
}
