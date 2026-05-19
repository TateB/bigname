use anyhow::{Result, bail};
use serde_json::Value;
use sqlx::postgres::PgRow;
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

use crate::SurfaceBindingKind;

/// Persisted current exact-name projection row served by API reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameCurrentRow {
    pub logical_name_id: String,
    pub namespace: String,
    pub canonical_display_name: String,
    pub normalized_name: String,
    pub namehash: String,
    pub surface_binding_id: Option<Uuid>,
    pub resource_id: Option<Uuid>,
    pub token_lineage_id: Option<Uuid>,
    pub binding_kind: Option<SurfaceBindingKind>,
    pub declared_summary: Value,
    pub provenance: Value,
    pub coverage: Value,
    pub chain_positions: Value,
    pub canonicality_summary: Value,
    pub manifest_version: i64,
    pub last_recomputed_at: OffsetDateTime,
}

impl NameCurrentRow {
    /// Load current exact-name projection rows keyed by logical name identity.
    ///
    /// Missing rows are omitted. Duplicate requested ids collapse into one map entry, and map
    /// iteration is sorted by `logical_name_id`; callers that need page order should iterate the
    /// original page and look up rows in the returned map.
    pub async fn load_by_logical_name_ids(
        pool: &sqlx::PgPool,
        logical_name_ids: &[String],
    ) -> Result<std::collections::BTreeMap<String, NameCurrentRow>> {
        super::load_name_current_by_logical_name_ids(pool, logical_name_ids).await
    }
}

pub(super) fn validate_name_current_row(row: &NameCurrentRow) -> Result<()> {
    if row.logical_name_id.trim().is_empty() {
        bail!("name_current row must include logical_name_id");
    }
    if row.namespace.trim().is_empty() {
        bail!(
            "name_current row {} must include namespace",
            row.logical_name_id
        );
    }
    if row.normalized_name.trim().is_empty() {
        bail!(
            "name_current row {} must include normalized_name",
            row.logical_name_id
        );
    }
    if row.canonical_display_name.trim().is_empty() {
        bail!(
            "name_current row {} must include canonical_display_name",
            row.logical_name_id
        );
    }
    if row.namehash.trim().is_empty() {
        bail!(
            "name_current row {} must include namehash",
            row.logical_name_id
        );
    }
    if row.logical_name_id != format!("{}:{}", row.namespace, row.normalized_name) {
        bail!(
            "name_current row {} does not match namespace {} and normalized_name {}",
            row.logical_name_id,
            row.namespace,
            row.normalized_name
        );
    }
    if row.manifest_version <= 0 {
        bail!(
            "name_current row {} has non-positive manifest_version {}",
            row.logical_name_id,
            row.manifest_version
        );
    }

    let has_binding_ref =
        row.surface_binding_id.is_some() || row.resource_id.is_some() || row.binding_kind.is_some();
    if has_binding_ref
        && (row.surface_binding_id.is_none()
            || row.resource_id.is_none()
            || row.binding_kind.is_none())
    {
        bail!(
            "name_current row {} must provide surface_binding_id, resource_id, and binding_kind together",
            row.logical_name_id
        );
    }
    if row.token_lineage_id.is_some() && row.resource_id.is_none() {
        bail!(
            "name_current row {} cannot set token_lineage_id without resource_id",
            row.logical_name_id
        );
    }

    ensure_json_object(
        &row.declared_summary,
        "declared_summary",
        &row.logical_name_id,
    )?;
    ensure_json_object(&row.provenance, "provenance", &row.logical_name_id)?;
    ensure_json_object(&row.coverage, "coverage", &row.logical_name_id)?;
    ensure_json_object(
        &row.chain_positions,
        "chain_positions",
        &row.logical_name_id,
    )?;
    ensure_json_object(
        &row.canonicality_summary,
        "canonicality_summary",
        &row.logical_name_id,
    )?;

    Ok(())
}

fn ensure_json_object(value: &Value, field_name: &str, logical_name_id: &str) -> Result<()> {
    if !value.is_object() {
        bail!(
            "name_current row {} field {} must be a JSON object",
            logical_name_id,
            field_name
        );
    }

    Ok(())
}

pub(super) fn decode_name_current_row(row: PgRow) -> Result<NameCurrentRow> {
    let binding_kind = crate::sql_row::get(&row, "binding_kind")?;

    Ok(NameCurrentRow {
        logical_name_id: crate::sql_row::get(&row, "logical_name_id")?,
        namespace: crate::sql_row::get(&row, "namespace")?,
        canonical_display_name: crate::sql_row::get(&row, "canonical_display_name")?,
        normalized_name: crate::sql_row::get(&row, "normalized_name")?,
        namehash: crate::sql_row::get(&row, "namehash")?,
        surface_binding_id: crate::sql_row::get(&row, "surface_binding_id")?,
        resource_id: crate::sql_row::get(&row, "resource_id")?,
        token_lineage_id: crate::sql_row::get(&row, "token_lineage_id")?,
        binding_kind,
        declared_summary: crate::sql_row::get(&row, "declared_summary")?,
        provenance: crate::sql_row::get(&row, "provenance")?,
        coverage: crate::sql_row::get(&row, "coverage")?,
        chain_positions: crate::sql_row::get(&row, "chain_positions")?,
        canonicality_summary: crate::sql_row::get(&row, "canonicality_summary")?,
        manifest_version: crate::sql_row::get(&row, "manifest_version")?,
        last_recomputed_at: crate::sql_row::get(&row, "last_recomputed_at")?,
    })
}
