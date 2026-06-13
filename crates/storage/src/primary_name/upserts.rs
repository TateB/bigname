use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres};

use super::rows::decode_primary_name_current_snapshot;
use super::types::{PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot, normalize_address};
use super::validation::validate_primary_name_current_snapshot;

/// Insert or replace declared primary-name claim-state rows.
pub async fn upsert_primary_name_current_rows(
    pool: &PgPool,
    rows: &[PrimaryNameCurrentRow],
) -> Result<Vec<PrimaryNameCurrentRow>> {
    let snapshots = rows
        .iter()
        .cloned()
        .map(|row| PrimaryNameCurrentSnapshot {
            row,
            normalized_claim_name: None,
        })
        .collect::<Vec<_>>();

    upsert_primary_name_current_snapshots(pool, &snapshots)
        .await
        .map(|snapshots| {
            snapshots
                .into_iter()
                .map(|snapshot| snapshot.row)
                .collect::<Vec<_>>()
        })
}

/// Insert or replace declared primary-name claim-state snapshots atomically.
pub async fn upsert_primary_name_current_snapshots(
    pool: &PgPool,
    snapshots: &[PrimaryNameCurrentSnapshot],
) -> Result<Vec<PrimaryNameCurrentSnapshot>> {
    if snapshots.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for primary_names_current snapshot upsert")?;

    let persisted =
        upsert_primary_name_current_snapshots_in_transaction(&mut transaction, snapshots).await?;

    transaction
        .commit()
        .await
        .context("failed to commit primary_names_current snapshot upsert")?;

    Ok(persisted)
}

/// Insert or replace declared primary-name claim-state snapshots in a caller-owned transaction.
pub async fn upsert_primary_name_current_snapshots_in_transaction(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    snapshots: &[PrimaryNameCurrentSnapshot],
) -> Result<Vec<PrimaryNameCurrentSnapshot>> {
    if snapshots.is_empty() {
        return Ok(Vec::new());
    }

    let mut ordered_snapshots = snapshots.iter().enumerate().collect::<Vec<_>>();
    ordered_snapshots.sort_by(|(_, left), (_, right)| {
        (
            normalize_address(&left.row.address),
            left.row.namespace.as_str(),
            left.row.coin_type.as_str(),
        )
            .cmp(&(
                normalize_address(&right.row.address),
                right.row.namespace.as_str(),
                right.row.coin_type.as_str(),
            ))
    });

    let mut persisted = vec![None; snapshots.len()];
    for (input_index, snapshot) in ordered_snapshots {
        validate_primary_name_current_snapshot(snapshot)?;
        persisted[input_index] = Some(
            upsert_primary_name_current_snapshot(transaction, snapshot)
                .await
                .with_context(|| {
                    format!(
                        "failed to upsert sorted primary_names_current snapshot at input index {input_index}"
                    )
                })?,
        );
    }

    persisted
        .into_iter()
        .enumerate()
        .map(|(input_index, snapshot)| {
            snapshot.with_context(|| {
                format!("missing primary_names_current upsert result at input index {input_index}")
            })
        })
        .collect()
}

async fn upsert_primary_name_current_snapshot(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    snapshot: &PrimaryNameCurrentSnapshot,
) -> Result<PrimaryNameCurrentSnapshot> {
    let claim_provenance = serde_json::to_string(&snapshot.row.claim_provenance)
        .context("failed to serialize primary_names_current claim_provenance")?;

    let persisted = sqlx::query(
        r#"
        INSERT INTO primary_names_current (
            address,
            coin_type,
            namespace,
            claim_status,
            raw_claim_name,
            normalized_claim_name,
            claim_provenance
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
        ON CONFLICT (address, coin_type, namespace) DO UPDATE
        SET
            claim_status = EXCLUDED.claim_status,
            raw_claim_name = EXCLUDED.raw_claim_name,
            normalized_claim_name = EXCLUDED.normalized_claim_name,
            claim_provenance = EXCLUDED.claim_provenance
        RETURNING
            address,
            namespace,
            coin_type,
            claim_status,
            raw_claim_name,
            normalized_claim_name,
            claim_provenance
        "#,
    )
    .bind(normalize_address(&snapshot.row.address))
    .bind(&snapshot.row.coin_type)
    .bind(&snapshot.row.namespace)
    .bind(snapshot.row.claim_status.as_str())
    .bind(&snapshot.row.raw_claim_name)
    .bind(&snapshot.normalized_claim_name)
    .bind(claim_provenance)
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to upsert primary_names_current snapshot for address {} namespace {} coin_type {}",
            snapshot.row.address, snapshot.row.namespace, snapshot.row.coin_type
        )
    })?;

    decode_primary_name_current_snapshot(persisted)
}
