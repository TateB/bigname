use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use sqlx::{Row, postgres::PgRow};
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
    let binding_kind = row
        .try_get::<Option<String>, _>("binding_kind")
        .context("missing binding_kind")?
        .map(|value| parse_surface_binding_kind(&value))
        .transpose()?;

    Ok(NameCurrentRow {
        logical_name_id: row
            .try_get("logical_name_id")
            .context("missing logical_name_id")?,
        namespace: row.try_get("namespace").context("missing namespace")?,
        canonical_display_name: row
            .try_get("canonical_display_name")
            .context("missing canonical_display_name")?,
        normalized_name: row
            .try_get("normalized_name")
            .context("missing normalized_name")?,
        namehash: row.try_get("namehash").context("missing namehash")?,
        surface_binding_id: row
            .try_get("surface_binding_id")
            .context("missing surface_binding_id")?,
        resource_id: row.try_get("resource_id").context("missing resource_id")?,
        token_lineage_id: row
            .try_get("token_lineage_id")
            .context("missing token_lineage_id")?,
        binding_kind,
        declared_summary: row
            .try_get("declared_summary")
            .context("missing declared_summary")?,
        provenance: row.try_get("provenance").context("missing provenance")?,
        coverage: row.try_get("coverage").context("missing coverage")?,
        chain_positions: row
            .try_get("chain_positions")
            .context("missing chain_positions")?,
        canonicality_summary: row
            .try_get("canonicality_summary")
            .context("missing canonicality_summary")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
        last_recomputed_at: row
            .try_get("last_recomputed_at")
            .context("missing last_recomputed_at")?,
    })
}

fn parse_surface_binding_kind(value: &str) -> Result<SurfaceBindingKind> {
    match value {
        "declared_registry_path" => Ok(SurfaceBindingKind::DeclaredRegistryPath),
        "linked_subregistry_path" => Ok(SurfaceBindingKind::LinkedSubregistryPath),
        "resolver_alias_path" => Ok(SurfaceBindingKind::ResolverAliasPath),
        "observed_wildcard_path" => Ok(SurfaceBindingKind::ObservedWildcardPath),
        "migration_rebind" => Ok(SurfaceBindingKind::MigrationRebind),
        "observed_only" => Ok(SurfaceBindingKind::ObservedOnly),
        _ => bail!("unknown surface binding kind {value}"),
    }
}
