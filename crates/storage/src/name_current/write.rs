use anyhow::{Context, Result};
use sqlx::PgPool;

use crate::SurfaceBindingKind;

use super::row::{NameCurrentRow, decode_name_current_row, validate_name_current_row};

/// Insert or replace projection rows for exact-name current reads.
pub async fn upsert_name_current_rows(
    pool: &PgPool,
    rows: &[NameCurrentRow],
) -> Result<Vec<NameCurrentRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for name_current upsert")?;
    let snapshots = upsert_name_current_rows_in_transaction(&mut transaction, rows).await?;
    transaction
        .commit()
        .await
        .context("failed to commit name_current upsert")?;

    Ok(snapshots)
}

/// Atomically publish a full replacement set for the exact-name current projection.
pub async fn replace_name_current_rows(
    pool: &PgPool,
    rows: &[NameCurrentRow],
    logical_name_ids: &[String],
) -> Result<(usize, u64)> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for name_current replacement")?;
    let upserted_row_count = upsert_name_current_rows_in_transaction(&mut transaction, rows)
        .await?
        .len();
    let deleted_row_count =
        delete_stale_name_current_rows_in_transaction(&mut transaction, logical_name_ids).await?;
    transaction
        .commit()
        .await
        .context("failed to commit name_current replacement")?;

    Ok((upserted_row_count, deleted_row_count))
}

async fn upsert_name_current_rows_in_transaction(
    executor: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    rows: &[NameCurrentRow],
) -> Result<Vec<NameCurrentRow>> {
    let mut snapshots = Vec::with_capacity(rows.len());
    for row in rows {
        validate_name_current_row(row)?;
        snapshots.push(upsert_name_current_row(executor, row).await?);
    }
    Ok(snapshots)
}

/// Delete one current exact-name projection row so a worker can rebuild the key.
pub async fn delete_name_current(pool: &PgPool, logical_name_id: &str) -> Result<u64> {
    sqlx::query(
        r#"
        DELETE FROM name_current
        WHERE logical_name_id = $1
        "#,
    )
    .bind(logical_name_id)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to delete name_current row for logical_name_id {logical_name_id}")
    })
    .map(|result| result.rows_affected())
}

/// Clear the exact-name current projection so a worker can perform a one-shot rebuild.
pub async fn clear_name_current(pool: &PgPool) -> Result<u64> {
    sqlx::query("DELETE FROM name_current")
        .execute(pool)
        .await
        .context("failed to clear name_current rows")
        .map(|result| result.rows_affected())
}

async fn delete_stale_name_current_rows_in_transaction(
    executor: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    logical_name_ids: &[String],
) -> Result<u64> {
    if logical_name_ids.is_empty() {
        return sqlx::query("DELETE FROM name_current")
            .execute(&mut **executor)
            .await
            .context("failed to clear name_current rows during replacement")
            .map(|result| result.rows_affected());
    }

    sqlx::query(
        r#"
        DELETE FROM name_current current
        WHERE NOT EXISTS (
            SELECT 1
            FROM UNNEST($1::TEXT[]) AS replacement(logical_name_id)
            WHERE replacement.logical_name_id = current.logical_name_id
        )
        "#,
    )
    .bind(logical_name_ids)
    .execute(&mut **executor)
    .await
    .context("failed to delete stale name_current rows during replacement")
    .map(|result| result.rows_affected())
}

async fn upsert_name_current_row(
    executor: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    row: &NameCurrentRow,
) -> Result<NameCurrentRow> {
    let declared_summary = serde_json::to_string(&row.declared_summary)
        .context("failed to serialize name_current declared_summary")?;
    let provenance = serde_json::to_string(&row.provenance)
        .context("failed to serialize name_current provenance")?;
    let coverage = serde_json::to_string(&row.coverage)
        .context("failed to serialize name_current coverage")?;
    let chain_positions = serde_json::to_string(&row.chain_positions)
        .context("failed to serialize name_current chain_positions")?;
    let canonicality_summary = serde_json::to_string(&row.canonicality_summary)
        .context("failed to serialize name_current canonicality_summary")?;

    let snapshot = sqlx::query(
        r#"
        INSERT INTO name_current (
            logical_name_id,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            declared_summary,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9,
            $10::jsonb, $11::jsonb, $12::jsonb, $13::jsonb, $14::jsonb, $15, $16
        )
        ON CONFLICT (logical_name_id) DO UPDATE
        SET
            namespace = EXCLUDED.namespace,
            canonical_display_name = EXCLUDED.canonical_display_name,
            normalized_name = EXCLUDED.normalized_name,
            namehash = EXCLUDED.namehash,
            surface_binding_id = EXCLUDED.surface_binding_id,
            resource_id = EXCLUDED.resource_id,
            token_lineage_id = EXCLUDED.token_lineage_id,
            binding_kind = EXCLUDED.binding_kind,
            declared_summary = EXCLUDED.declared_summary,
            provenance = EXCLUDED.provenance,
            coverage = EXCLUDED.coverage,
            chain_positions = EXCLUDED.chain_positions,
            canonicality_summary = EXCLUDED.canonicality_summary,
            manifest_version = EXCLUDED.manifest_version,
            last_recomputed_at = EXCLUDED.last_recomputed_at
        RETURNING
            logical_name_id,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            declared_summary,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        "#,
    )
    .bind(&row.logical_name_id)
    .bind(&row.namespace)
    .bind(&row.canonical_display_name)
    .bind(&row.normalized_name)
    .bind(&row.namehash)
    .bind(row.surface_binding_id)
    .bind(row.resource_id)
    .bind(row.token_lineage_id)
    .bind(row.binding_kind.map(SurfaceBindingKind::as_str))
    .bind(declared_summary)
    .bind(provenance)
    .bind(coverage)
    .bind(chain_positions)
    .bind(canonicality_summary)
    .bind(row.manifest_version)
    .bind(row.last_recomputed_at)
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to upsert name_current row for logical_name_id {}",
            row.logical_name_id
        )
    })?;

    decode_name_current_row(snapshot)
}
