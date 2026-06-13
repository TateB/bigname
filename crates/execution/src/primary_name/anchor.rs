use anyhow::{Context, Result, bail};
use bigname_storage::{PrimaryNameClaimStatus, load_primary_name_current_snapshot};
use sqlx::{PgPool, Postgres, Transaction};

use super::context::verified_primary_context_label;
use super::{VerifiedPrimaryNameSection, VerifiedPrimaryNameStatus, VerifiedPrimaryNameTuple};

pub(crate) async fn ensure_primary_name_anchor_matches(
    pool: &PgPool,
    tuple: &VerifiedPrimaryNameTuple,
    verified_primary_name: &VerifiedPrimaryNameSection,
) -> Result<()> {
    let context = verified_primary_context_label(&tuple.namespace)?;
    let Some(snapshot) = load_primary_name_current_snapshot(
        pool,
        &tuple.normalized_address,
        &tuple.namespace,
        &tuple.coin_type,
    )
    .await?
    else {
        bail!(
            "{context} persistence requires primary_names_current anchor for address {} namespace {} coin_type {}",
            tuple.normalized_address,
            tuple.namespace,
            tuple.coin_type
        );
    };

    ensure_anchor_content_matches(
        context,
        tuple,
        verified_primary_name,
        snapshot.row.claim_status.as_str(),
        snapshot.normalized_claim_name.as_deref(),
    )
}

pub(crate) async fn ensure_primary_name_anchor_matches_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    tuple: &VerifiedPrimaryNameTuple,
    verified_primary_name: &VerifiedPrimaryNameSection,
) -> Result<()> {
    let context = verified_primary_context_label(&tuple.namespace)?;
    let Some(anchor) = sqlx::query_as::<_, LockedPrimaryNameAnchor>(
        r#"
        SELECT claim_status, normalized_claim_name
        FROM primary_names_current
        WHERE address = $1
          AND namespace = $2
          AND coin_type = $3
        FOR UPDATE
        "#,
    )
    .bind(&tuple.normalized_address)
    .bind(&tuple.namespace)
    .bind(&tuple.coin_type)
    .fetch_optional(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to lock primary_names_current anchor for address {} namespace {} coin_type {}",
            tuple.normalized_address, tuple.namespace, tuple.coin_type
        )
    })?
    else {
        bail!(
            "{context} persistence requires primary_names_current anchor for address {} namespace {} coin_type {}",
            tuple.normalized_address,
            tuple.namespace,
            tuple.coin_type
        );
    };

    ensure_anchor_content_matches(
        context,
        tuple,
        verified_primary_name,
        &anchor.claim_status,
        anchor.normalized_claim_name.as_deref(),
    )
}

#[derive(sqlx::FromRow)]
struct LockedPrimaryNameAnchor {
    claim_status: String,
    normalized_claim_name: Option<String>,
}

fn ensure_anchor_content_matches(
    context: &str,
    tuple: &VerifiedPrimaryNameTuple,
    verified_primary_name: &VerifiedPrimaryNameSection,
    current_claim_status: &str,
    current_normalized_claim_name: Option<&str>,
) -> Result<()> {
    let expected_claim_status = match verified_primary_name.status {
        VerifiedPrimaryNameStatus::Success
        | VerifiedPrimaryNameStatus::Mismatch
        | VerifiedPrimaryNameStatus::NotFound
        | VerifiedPrimaryNameStatus::ExecutionFailed => PrimaryNameClaimStatus::Success.as_str(),
        VerifiedPrimaryNameStatus::InvalidName => PrimaryNameClaimStatus::InvalidName.as_str(),
    };
    if current_claim_status != expected_claim_status {
        bail!(
            "{context} persistence claim content changed for address {} namespace {} coin_type {}: current claim_status {} does not match verified status {:?}",
            tuple.normalized_address,
            tuple.namespace,
            tuple.coin_type,
            current_claim_status,
            verified_primary_name.status
        );
    }

    // Payload validation keeps name absent for not_found, invalid_name, and execution_failed.
    // Those statuses can only be fenced by claim existence/status here; full name fencing for
    // stale error outcomes would require carrying the claimed name in the verified-primary
    // error payload contract.
    if matches!(
        verified_primary_name.status,
        VerifiedPrimaryNameStatus::Success | VerifiedPrimaryNameStatus::Mismatch
    ) {
        let Some(current_claim_name) = current_normalized_claim_name else {
            return Ok(());
        };
        let verified_claim_name = verified_primary_name
            .section
            .get("name")
            .and_then(|name| name.get("normalized_name"))
            .and_then(serde_json::Value::as_str)
            .context("verified-primary success/mismatch payload missing name.normalized_name")?;
        if current_claim_name != verified_claim_name {
            bail!(
                "{context} persistence claim content changed for address {} namespace {} coin_type {}: current claim name {} does not match verified claim name {}",
                tuple.normalized_address,
                tuple.namespace,
                tuple.coin_type,
                current_claim_name,
                verified_claim_name
            );
        }
    }

    Ok(())
}
