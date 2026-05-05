use anyhow::{Context, Result, bail};
use bigname_storage::sql_row;
use bigname_storage::{NameCurrentRow, RecordInventoryCurrentRow, SurfaceBindingKind};
use serde_json::Value;
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};
use uuid::Uuid;

use crate::json_helpers::{json_field, json_string_field};

pub(super) async fn load_supported_record_inventory_current_for_revalidation(
    transaction: &mut Transaction<'_, Postgres>,
    row: &NameCurrentRow,
    request_record_version_boundary: &Value,
) -> Result<Option<RecordInventoryCurrentRow>> {
    let Some((resource_id, record_version_boundary)) =
        bigname_storage::resolution_record_inventory_lookup_key_for_revalidation(row)?
    else {
        return Ok(None);
    };

    if let Some(record_inventory_row) = load_record_inventory_current_for_revalidation(
        transaction,
        resource_id,
        request_record_version_boundary,
    )
    .await?
    {
        return Ok(Some(record_inventory_row));
    }

    if let Some(record_inventory_row) = load_record_inventory_current_for_revalidation(
        transaction,
        resource_id,
        &record_version_boundary,
    )
    .await?
    {
        return Ok(Some(record_inventory_row));
    }

    if record_version_boundary_has_pointer(&record_version_boundary) {
        return Ok(None);
    }

    let Some(persisted_boundary) = find_supported_record_inventory_boundary_for_revalidation(
        transaction,
        resource_id,
        &record_version_boundary,
    )
    .await?
    else {
        return Ok(None);
    };

    load_record_inventory_current_for_revalidation(transaction, resource_id, &persisted_boundary)
        .await?
        .with_context(|| {
            format!(
                "matched record_inventory_current boundary for resource_id {resource_id} but the projection row was not loadable"
            )
        })
        .map(Some)
}

async fn find_supported_record_inventory_boundary_for_revalidation(
    transaction: &mut Transaction<'_, Postgres>,
    resource_id: Uuid,
    record_version_boundary: &Value,
) -> Result<Option<Value>> {
    let logical_name_id =
        json_string_field(json_field(record_version_boundary, "logical_name_id")).with_context(
            || {
                format!(
                    "supported record version boundary for resource_id {resource_id} must include logical_name_id"
                )
            },
        )?;
    let chain_position = json_field(record_version_boundary, "chain_position").with_context(|| {
        format!(
            "supported record version boundary for resource_id {resource_id} must include chain_position"
        )
    })?;
    let chain_id = json_string_field(json_field(chain_position, "chain_id")).with_context(|| {
        format!(
            "supported record version boundary for resource_id {resource_id} must include chain_position.chain_id"
        )
    })?;
    let block_number = json_field(chain_position, "block_number")
        .and_then(Value::as_i64)
        .with_context(|| {
            format!(
                "supported record version boundary for resource_id {resource_id} must include chain_position.block_number"
            )
        })?;
    let block_hash = json_string_field(json_field(chain_position, "block_hash")).with_context(|| {
        format!(
            "supported record version boundary for resource_id {resource_id} must include chain_position.block_hash"
        )
    })?;
    let timestamp = json_string_field(json_field(chain_position, "timestamp")).with_context(|| {
        format!(
            "supported record version boundary for resource_id {resource_id} must include chain_position.timestamp"
        )
    })?;

    let boundaries = sqlx::query(
        r#"
        SELECT record_version_boundary
        FROM record_inventory_current
        WHERE resource_id = $1
          AND record_version_boundary ->> 'logical_name_id' = $2
          AND record_version_boundary -> 'chain_position' ->> 'chain_id' = $3
          AND (record_version_boundary -> 'chain_position' ->> 'block_number')::bigint = $4
          AND record_version_boundary -> 'chain_position' ->> 'block_hash' = $5
          AND record_version_boundary -> 'chain_position' ->> 'timestamp' = $6
        ORDER BY
          (record_version_boundary ->> 'normalized_event_id') IS NULL ASC,
          (record_version_boundary ->> 'normalized_event_id')::bigint DESC NULLS LAST
        LIMIT 2
        "#,
    )
    .bind(resource_id)
    .bind(logical_name_id)
    .bind(chain_id)
    .bind(block_number)
    .bind(block_hash)
    .bind(timestamp)
    .fetch_all(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to locate supported record_inventory_current boundary for resource_id {resource_id}"
        )
    })?
    .into_iter()
    .map(|row| {
        row.try_get("record_version_boundary").with_context(|| {
            format!(
                "supported record_inventory_current lookup for resource_id {resource_id} returned a row without record_version_boundary"
            )
        })
    })
    .collect::<Result<Vec<Value>, _>>()?;

    let Some(first_boundary) = boundaries.first().cloned() else {
        return Ok(None);
    };
    if let Some(second_boundary) = boundaries.get(1)
        && (!record_version_boundary_has_pointer(&first_boundary)
            || record_version_boundary_has_pointer(second_boundary))
    {
        bail!(
            "supported record_inventory_current lookup for resource_id {} found multiple projection rows for the same boundary anchor",
            resource_id
        );
    }

    Ok(Some(first_boundary))
}

fn record_version_boundary_has_pointer(record_version_boundary: &Value) -> bool {
    bigname_storage::record_version_boundary_has_pointer(record_version_boundary)
}

pub(super) async fn load_name_current_for_revalidation(
    transaction: &mut Transaction<'_, Postgres>,
    logical_name_id: &str,
) -> Result<Option<NameCurrentRow>> {
    let row = sqlx::query(
        r#"
        SELECT
            nc.logical_name_id,
            nc.namespace,
            nc.canonical_display_name,
            nc.normalized_name,
            nc.namehash,
            nc.surface_binding_id,
            nc.resource_id,
            nc.token_lineage_id,
            nc.binding_kind,
            nc.declared_summary,
            nc.provenance,
            nc.coverage,
            nc.chain_positions,
            nc.canonicality_summary,
            nc.manifest_version,
            nc.last_recomputed_at
        FROM name_current nc
        JOIN name_surfaces surface
          ON surface.logical_name_id = nc.logical_name_id
        LEFT JOIN resources resource
          ON resource.resource_id = nc.resource_id
        LEFT JOIN surface_bindings binding
          ON binding.surface_binding_id = nc.surface_binding_id
        LEFT JOIN token_lineages token_lineage
          ON token_lineage.token_lineage_id = nc.token_lineage_id
        WHERE nc.logical_name_id = $1
          AND surface.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND (
              nc.surface_binding_id IS NULL
              OR (
                  resource.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
                  AND binding.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
                  AND (
                      nc.token_lineage_id IS NULL
                      OR token_lineage.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                  )
              )
          )
        "#,
    )
    .bind(logical_name_id)
    .fetch_optional(&mut **transaction)
    .await
    .with_context(|| {
        format!("failed to load name_current row for logical_name_id {logical_name_id}")
    })?;

    row.map(decode_name_current_row_for_revalidation)
        .transpose()
}

fn decode_name_current_row_for_revalidation(row: PgRow) -> Result<NameCurrentRow> {
    Ok(NameCurrentRow {
        logical_name_id: sql_row::get(&row, "logical_name_id")?,
        namespace: sql_row::get(&row, "namespace")?,
        canonical_display_name: sql_row::get(&row, "canonical_display_name")?,
        normalized_name: sql_row::get(&row, "normalized_name")?,
        namehash: sql_row::get(&row, "namehash")?,
        surface_binding_id: sql_row::get(&row, "surface_binding_id")?,
        resource_id: sql_row::get(&row, "resource_id")?,
        token_lineage_id: sql_row::get(&row, "token_lineage_id")?,
        binding_kind: row
            .try_get::<Option<String>, _>("binding_kind")
            .context("missing binding_kind")?
            .map(|value| SurfaceBindingKind::parse(&value))
            .transpose()?,
        declared_summary: sql_row::get(&row, "declared_summary")?,
        provenance: sql_row::get(&row, "provenance")?,
        coverage: sql_row::get(&row, "coverage")?,
        chain_positions: sql_row::get(&row, "chain_positions")?,
        canonicality_summary: sql_row::get(&row, "canonicality_summary")?,
        manifest_version: sql_row::get(&row, "manifest_version")?,
        last_recomputed_at: sql_row::get(&row, "last_recomputed_at")?,
    })
}

async fn load_record_inventory_current_for_revalidation(
    transaction: &mut Transaction<'_, Postgres>,
    resource_id: Uuid,
    record_version_boundary: &Value,
) -> Result<Option<RecordInventoryCurrentRow>> {
    let record_version_boundary_key = serde_json::to_string(record_version_boundary)
        .context("failed to serialize revalidation record_version_boundary")?;

    let row = sqlx::query(
        r#"
        SELECT
            ric.resource_id,
            ric.record_version_boundary,
            ric.enumeration_basis,
            ric.selectors,
            ric.explicit_gaps,
            ric.unsupported_families,
            ric.last_change,
            ric.entries,
            ric.provenance,
            ric.coverage,
            ric.chain_positions,
            ric.canonicality_summary,
            ric.manifest_version,
            ric.last_recomputed_at
        FROM record_inventory_current ric
        JOIN resources resource
          ON resource.resource_id = ric.resource_id
        WHERE ric.resource_id = $1
          AND ric.record_version_boundary = $2::JSONB
          AND resource.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        "#,
    )
    .bind(resource_id)
    .bind(record_version_boundary_key)
    .fetch_optional(&mut **transaction)
    .await
    .with_context(|| {
        format!("failed to load record_inventory_current row for resource_id {resource_id}")
    })?;

    row.map(decode_record_inventory_current_row_for_revalidation)
        .transpose()
}

fn decode_record_inventory_current_row_for_revalidation(
    row: PgRow,
) -> Result<RecordInventoryCurrentRow> {
    Ok(RecordInventoryCurrentRow {
        resource_id: sql_row::get(&row, "resource_id")?,
        record_version_boundary: sql_row::get(&row, "record_version_boundary")?,
        enumeration_basis: sql_row::get(&row, "enumeration_basis")?,
        selectors: sql_row::get(&row, "selectors")?,
        explicit_gaps: sql_row::get(&row, "explicit_gaps")?,
        unsupported_families: sql_row::get(&row, "unsupported_families")?,
        last_change: sql_row::get(&row, "last_change")?,
        entries: sql_row::get(&row, "entries")?,
        provenance: sql_row::get(&row, "provenance")?,
        coverage: sql_row::get(&row, "coverage")?,
        chain_positions: sql_row::get(&row, "chain_positions")?,
        canonicality_summary: sql_row::get(&row, "canonicality_summary")?,
        manifest_version: sql_row::get(&row, "manifest_version")?,
        last_recomputed_at: sql_row::get(&row, "last_recomputed_at")?,
    })
}
