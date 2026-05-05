use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgPool, Postgres, QueryBuilder, postgres::PgRow};

use crate::projection_helpers::{
    POSTGRES_MAX_BIND_PARAMETERS, remap_input_indexed_rows, require_json_object,
    serialize_jsonb_field,
};

const RESOLVER_CURRENT_UPSERT_BIND_COLUMNS: usize = 9;
const RESOLVER_CURRENT_MAX_ROWS_PER_CHUNK: usize =
    POSTGRES_MAX_BIND_PARAMETERS / RESOLVER_CURRENT_UPSERT_BIND_COLUMNS;

/// Persisted resolver-overview projection row keyed by resolver target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolverCurrentRow {
    pub chain_id: String,
    pub resolver_address: String,
    pub declared_summary: Value,
    pub provenance: Value,
    pub coverage: Value,
    pub chain_positions: Value,
    pub canonicality_summary: Value,
    pub manifest_version: i64,
    pub last_recomputed_at: OffsetDateTime,
}

/// Load one resolver-overview projection row by chain and resolver address.
pub async fn load_resolver_current(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
) -> Result<Option<ResolverCurrentRow>> {
    let normalized_address = normalize_resolver_address(resolver_address);
    let row = sqlx::query(
        r#"
        SELECT
            chain_id,
            resolver_address,
            declared_summary,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        FROM resolver_current
        WHERE chain_id = $1
          AND resolver_address = $2
        "#,
    )
    .bind(chain_id)
    .bind(&normalized_address)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load resolver_current row for chain_id {chain_id} resolver_address {normalized_address}"
        )
    })?;

    row.map(decode_resolver_current_row).transpose()
}

/// Insert or replace resolver-overview projection rows.
pub async fn upsert_resolver_current_rows(
    pool: &PgPool,
    rows: &[ResolverCurrentRow],
) -> Result<Vec<ResolverCurrentRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let prepared_rows = rows
        .iter()
        .map(prepare_resolver_current_row)
        .collect::<Result<Vec<_>>>()?;

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for resolver_current upsert")?;

    let mut snapshots = Vec::with_capacity(rows.len());
    let mut chunk_start = 0;
    while chunk_start < prepared_rows.len() {
        let chunk_end = resolver_current_chunk_end(&prepared_rows, chunk_start);
        snapshots.extend(
            upsert_resolver_current_batch(&mut transaction, &prepared_rows[chunk_start..chunk_end])
                .await?,
        );
        chunk_start = chunk_end;
    }

    transaction
        .commit()
        .await
        .context("failed to commit resolver_current upsert")?;

    Ok(snapshots)
}

/// Delete one resolver-overview row so a worker can rebuild the key.
pub async fn delete_resolver_current(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
) -> Result<u64> {
    let normalized_address = normalize_resolver_address(resolver_address);
    sqlx::query(
        r#"
        DELETE FROM resolver_current
        WHERE chain_id = $1
          AND resolver_address = $2
        "#,
    )
    .bind(chain_id)
    .bind(&normalized_address)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to delete resolver_current row for chain_id {chain_id} resolver_address {normalized_address}"
        )
    })
    .map(|result| result.rows_affected())
}

/// Clear the resolver-overview projection so a worker can perform a one-shot rebuild.
pub async fn clear_resolver_current(pool: &PgPool) -> Result<u64> {
    sqlx::query("DELETE FROM resolver_current")
        .execute(pool)
        .await
        .context("failed to clear resolver_current rows")
        .map(|result| result.rows_affected())
}

#[derive(Debug)]
struct PreparedResolverCurrentRow {
    chain_id: String,
    resolver_address: String,
    declared_summary: String,
    provenance: String,
    coverage: String,
    chain_positions: String,
    canonicality_summary: String,
    manifest_version: i64,
    last_recomputed_at: OffsetDateTime,
}

fn prepare_resolver_current_row(row: &ResolverCurrentRow) -> Result<PreparedResolverCurrentRow> {
    validate_resolver_current_row(row)?;

    Ok(PreparedResolverCurrentRow {
        chain_id: row.chain_id.clone(),
        resolver_address: normalize_resolver_address(&row.resolver_address),
        declared_summary: serialize_jsonb_field(
            &row.declared_summary,
            "failed to serialize resolver_current declared_summary",
        )?,
        provenance: serialize_jsonb_field(
            &row.provenance,
            "failed to serialize resolver_current provenance",
        )?,
        coverage: serialize_jsonb_field(
            &row.coverage,
            "failed to serialize resolver_current coverage",
        )?,
        chain_positions: serialize_jsonb_field(
            &row.chain_positions,
            "failed to serialize resolver_current chain_positions",
        )?,
        canonicality_summary: serialize_jsonb_field(
            &row.canonicality_summary,
            "failed to serialize resolver_current canonicality_summary",
        )?,
        manifest_version: row.manifest_version,
        last_recomputed_at: row.last_recomputed_at,
    })
}

fn resolver_current_chunk_end(rows: &[PreparedResolverCurrentRow], start: usize) -> usize {
    let limit = rows.len().min(start + RESOLVER_CURRENT_MAX_ROWS_PER_CHUNK);
    let mut seen_keys = BTreeSet::new();
    let mut end = start;

    while end < limit {
        let row = &rows[end];
        let key = (row.chain_id.as_str(), row.resolver_address.as_str());
        if !seen_keys.insert(key) {
            break;
        }
        end += 1;
    }

    end.max(start + 1)
}

async fn upsert_resolver_current_batch(
    executor: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    rows: &[PreparedResolverCurrentRow],
) -> Result<Vec<ResolverCurrentRow>> {
    let expected_len = rows.len();
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        WITH input_rows (
            input_index,
            chain_id,
            resolver_address,
            declared_summary,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        ) AS (
            VALUES
        "#,
    );

    for (input_index, row) in rows.iter().enumerate() {
        if input_index > 0 {
            builder.push(", ");
        }
        builder.push("(");
        builder.push(input_index.to_string());
        builder.push("::BIGINT, ");
        builder.push_bind(row.chain_id.as_str());
        builder.push(", ");
        builder.push_bind(row.resolver_address.as_str());
        builder.push(", ");
        builder.push_bind(row.declared_summary.as_str());
        builder.push("::jsonb, ");
        builder.push_bind(row.provenance.as_str());
        builder.push("::jsonb, ");
        builder.push_bind(row.coverage.as_str());
        builder.push("::jsonb, ");
        builder.push_bind(row.chain_positions.as_str());
        builder.push("::jsonb, ");
        builder.push_bind(row.canonicality_summary.as_str());
        builder.push("::jsonb, ");
        builder.push_bind(row.manifest_version);
        builder.push(", ");
        builder.push_bind(row.last_recomputed_at);
        builder.push(")");
    }

    builder.push(
        r#"
        ),
        upserted AS (
        INSERT INTO resolver_current (
            chain_id,
            resolver_address,
            declared_summary,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        )
        SELECT
            chain_id,
            resolver_address,
            declared_summary,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        FROM input_rows
        ON CONFLICT (chain_id, resolver_address) DO UPDATE
        SET
            declared_summary = EXCLUDED.declared_summary,
            provenance = EXCLUDED.provenance,
            coverage = EXCLUDED.coverage,
            chain_positions = EXCLUDED.chain_positions,
            canonicality_summary = EXCLUDED.canonicality_summary,
            manifest_version = EXCLUDED.manifest_version,
            last_recomputed_at = EXCLUDED.last_recomputed_at
        RETURNING
            chain_id,
            resolver_address,
            declared_summary,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        )
        SELECT
            input_rows.input_index,
            upserted.chain_id,
            upserted.resolver_address,
            upserted.declared_summary,
            upserted.provenance,
            upserted.coverage,
            upserted.chain_positions,
            upserted.canonicality_summary,
            upserted.manifest_version,
            upserted.last_recomputed_at
        FROM upserted
        INNER JOIN input_rows
          ON input_rows.chain_id = upserted.chain_id
         AND input_rows.resolver_address = upserted.resolver_address
        "#,
    );

    let returned_rows = builder
        .build()
        .fetch_all(&mut **executor)
        .await
        .with_context(|| {
            format!(
                "failed to upsert resolver_current batch containing {} rows",
                rows.len()
            )
        })?;

    decode_resolver_current_batch(returned_rows, expected_len)
}

fn decode_resolver_current_batch(
    rows: Vec<PgRow>,
    expected_len: usize,
) -> Result<Vec<ResolverCurrentRow>> {
    remap_input_indexed_rows(
        rows,
        expected_len,
        "resolver_current",
        decode_resolver_current_row,
    )
}

fn validate_resolver_current_row(row: &ResolverCurrentRow) -> Result<()> {
    if row.chain_id.trim().is_empty() {
        bail!("resolver_current row must include chain_id");
    }
    if row.resolver_address.trim().is_empty() {
        bail!(
            "resolver_current row for chain_id {} must include resolver_address",
            row.chain_id
        );
    }
    if row.manifest_version <= 0 {
        bail!(
            "resolver_current row for chain_id {} resolver_address {} has non-positive manifest_version {}",
            row.chain_id,
            row.resolver_address,
            row.manifest_version
        );
    }

    require_json_object(&row.declared_summary, || {
        format!(
            "resolver_current row for chain_id {} resolver_address {} field declared_summary must be a JSON object",
            row.chain_id, row.resolver_address
        )
    })?;
    require_json_object(&row.provenance, || {
        format!(
            "resolver_current row for chain_id {} resolver_address {} field provenance must be a JSON object",
            row.chain_id, row.resolver_address
        )
    })?;
    require_json_object(&row.coverage, || {
        format!(
            "resolver_current row for chain_id {} resolver_address {} field coverage must be a JSON object",
            row.chain_id, row.resolver_address
        )
    })?;
    require_json_object(&row.chain_positions, || {
        format!(
            "resolver_current row for chain_id {} resolver_address {} field chain_positions must be a JSON object",
            row.chain_id, row.resolver_address
        )
    })?;
    require_json_object(&row.canonicality_summary, || {
        format!(
            "resolver_current row for chain_id {} resolver_address {} field canonicality_summary must be a JSON object",
            row.chain_id, row.resolver_address
        )
    })?;

    Ok(())
}

fn decode_resolver_current_row(row: PgRow) -> Result<ResolverCurrentRow> {
    Ok(ResolverCurrentRow {
        chain_id: crate::sql_row::get(&row, "chain_id")?,
        resolver_address: crate::sql_row::get::<String>(&row, "resolver_address")?
            .to_ascii_lowercase(),
        declared_summary: crate::sql_row::get(&row, "declared_summary")?,
        provenance: crate::sql_row::get(&row, "provenance")?,
        coverage: crate::sql_row::get(&row, "coverage")?,
        chain_positions: crate::sql_row::get(&row, "chain_positions")?,
        canonicality_summary: crate::sql_row::get(&row, "canonicality_summary")?,
        manifest_version: crate::sql_row::get(&row, "manifest_version")?,
        last_recomputed_at: crate::sql_row::get(&row, "last_recomputed_at")?,
    })
}

fn normalize_resolver_address(resolver_address: &str) -> String {
    resolver_address.to_ascii_lowercase()
}

#[cfg(test)]
mod tests;
