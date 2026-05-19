use anyhow::{Context, Result};
use sqlx::{Executor, PgPool, Postgres, postgres::PgRow};
use uuid::Uuid;

use super::types::{NameSurface, Resource, SurfaceBinding, TokenLineage};

const DEFAULT_IDENTITY_READ_FILTER: &str = r#"
  AND canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
"#;

/// Load one token lineage anchor by stable identity from the default canonical read set.
pub async fn load_token_lineage(
    pool: &PgPool,
    token_lineage_id: Uuid,
) -> Result<Option<TokenLineage>> {
    load_token_lineage_internal(pool, token_lineage_id, false).await
}

/// Load one token lineage anchor by stable identity, including observed and orphaned rows.
pub async fn load_token_lineage_including_noncanonical(
    pool: &PgPool,
    token_lineage_id: Uuid,
) -> Result<Option<TokenLineage>> {
    load_token_lineage_internal(pool, token_lineage_id, true).await
}

/// Load one backing resource by stable identity.
pub async fn load_resource(pool: &PgPool, resource_id: Uuid) -> Result<Option<Resource>> {
    load_resource_internal(pool, resource_id, false).await
}

/// Load one backing resource by stable identity, including observed and orphaned rows.
pub async fn load_resource_including_noncanonical(
    pool: &PgPool,
    resource_id: Uuid,
) -> Result<Option<Resource>> {
    load_resource_internal(pool, resource_id, true).await
}

/// Load one canonical surface row by deterministic logical name identity.
pub async fn load_name_surface(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Option<NameSurface>> {
    load_name_surface_internal(pool, logical_name_id, false).await
}

/// Load one surface row by deterministic logical name identity, including observed and orphaned rows.
pub async fn load_name_surface_including_noncanonical(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Option<NameSurface>> {
    load_name_surface_internal(pool, logical_name_id, true).await
}

/// Load one time-ranged surface binding by stable identity.
pub async fn load_surface_binding(
    pool: &PgPool,
    surface_binding_id: Uuid,
) -> Result<Option<SurfaceBinding>> {
    load_surface_binding_internal(pool, surface_binding_id, false).await
}

/// Load one time-ranged surface binding by stable identity, including observed and orphaned rows.
pub async fn load_surface_binding_including_noncanonical(
    pool: &PgPool,
    surface_binding_id: Uuid,
) -> Result<Option<SurfaceBinding>> {
    load_surface_binding_internal(pool, surface_binding_id, true).await
}

/// Load all bindings for one logical surface in chronological order from the default canonical read set.
pub async fn load_surface_bindings_by_logical_name_id(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Vec<SurfaceBinding>> {
    load_surface_bindings_by_logical_name_id_internal(pool, logical_name_id, false).await
}

/// Load all bindings for one logical surface in chronological order, including observed and orphaned rows.
pub async fn load_surface_bindings_by_logical_name_id_including_noncanonical(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Vec<SurfaceBinding>> {
    load_surface_bindings_by_logical_name_id_internal(pool, logical_name_id, true).await
}

/// Load all bindings for one backing resource in chronological order from the default canonical read set.
pub async fn load_surface_bindings_by_resource_id(
    pool: &PgPool,
    resource_id: Uuid,
) -> Result<Vec<SurfaceBinding>> {
    load_surface_bindings_by_resource_id_internal(pool, resource_id, false).await
}

/// Load all bindings for one backing resource in chronological order, including observed and orphaned rows.
pub async fn load_surface_bindings_by_resource_id_including_noncanonical(
    pool: &PgPool,
    resource_id: Uuid,
) -> Result<Vec<SurfaceBinding>> {
    load_surface_bindings_by_resource_id_internal(pool, resource_id, true).await
}

pub(super) async fn load_token_lineage_internal<'e, E>(
    executor: E,
    token_lineage_id: Uuid,
    include_noncanonical: bool,
) -> Result<Option<TokenLineage>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(&format!(
        r#"
        SELECT
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        FROM token_lineages
        WHERE token_lineage_id = $1
        {}
        "#,
        identity_read_filter(include_noncanonical),
    ))
    .bind(token_lineage_id)
    .fetch_optional(executor)
    .await
    .with_context(|| format!("failed to load token lineage {token_lineage_id}"))?;

    row.map(decode_token_lineage).transpose()
}

pub(super) async fn load_resource_internal<'e, E>(
    executor: E,
    resource_id: Uuid,
    include_noncanonical: bool,
) -> Result<Option<Resource>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(&format!(
        r#"
        SELECT
            resource_id,
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        FROM resources
        WHERE resource_id = $1
        {}
        "#,
        identity_read_filter(include_noncanonical),
    ))
    .bind(resource_id)
    .fetch_optional(executor)
    .await
    .with_context(|| format!("failed to load resource {resource_id}"))?;

    row.map(decode_resource).transpose()
}

pub(super) async fn load_name_surface_internal<'e, E>(
    executor: E,
    logical_name_id: &str,
    include_noncanonical: bool,
) -> Result<Option<NameSurface>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(&format!(
        r#"
        SELECT
            logical_name_id,
            namespace,
            input_name,
            canonical_display_name,
            normalized_name,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version,
            normalization_warnings,
            normalization_errors,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        FROM name_surfaces
        WHERE logical_name_id = $1
        {}
        "#,
        identity_read_filter(include_noncanonical),
    ))
    .bind(logical_name_id)
    .fetch_optional(executor)
    .await
    .with_context(|| format!("failed to load name surface {logical_name_id}"))?;

    row.map(decode_name_surface).transpose()
}

pub(super) async fn load_surface_binding_internal<'e, E>(
    executor: E,
    surface_binding_id: Uuid,
    include_noncanonical: bool,
) -> Result<Option<SurfaceBinding>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(&format!(
        r#"
        SELECT
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            active_from,
            active_to,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        FROM surface_bindings
        WHERE surface_binding_id = $1
        {}
        "#,
        identity_read_filter(include_noncanonical),
    ))
    .bind(surface_binding_id)
    .fetch_optional(executor)
    .await
    .with_context(|| format!("failed to load surface binding {surface_binding_id}"))?;

    row.map(decode_surface_binding).transpose()
}

async fn load_surface_bindings_by_logical_name_id_internal<'e, E>(
    executor: E,
    logical_name_id: &str,
    include_noncanonical: bool,
) -> Result<Vec<SurfaceBinding>>
where
    E: Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(&format!(
        r#"
        SELECT
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            active_from,
            active_to,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        FROM surface_bindings
        WHERE logical_name_id = $1
        {}
        ORDER BY active_from, active_to NULLS LAST, surface_binding_id
        "#,
        identity_read_filter(include_noncanonical),
    ))
    .bind(logical_name_id)
    .fetch_all(executor)
    .await
    .with_context(|| {
        format!("failed to load surface bindings for logical name {logical_name_id}")
    })?;

    rows.into_iter().map(decode_surface_binding).collect()
}

async fn load_surface_bindings_by_resource_id_internal<'e, E>(
    executor: E,
    resource_id: Uuid,
    include_noncanonical: bool,
) -> Result<Vec<SurfaceBinding>>
where
    E: Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(&format!(
        r#"
        SELECT
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            active_from,
            active_to,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        FROM surface_bindings
        WHERE resource_id = $1
        {}
        ORDER BY active_from, active_to NULLS LAST, logical_name_id, surface_binding_id
        "#,
        identity_read_filter(include_noncanonical),
    ))
    .bind(resource_id)
    .fetch_all(executor)
    .await
    .with_context(|| format!("failed to load surface bindings for resource {resource_id}"))?;

    rows.into_iter().map(decode_surface_binding).collect()
}

fn identity_read_filter(include_noncanonical: bool) -> &'static str {
    if include_noncanonical {
        ""
    } else {
        DEFAULT_IDENTITY_READ_FILTER
    }
}

pub(super) fn decode_token_lineage(row: PgRow) -> Result<TokenLineage> {
    Ok(TokenLineage {
        token_lineage_id: crate::sql_row::get(&row, "token_lineage_id")?,
        chain_id: crate::sql_row::get(&row, "chain_id")?,
        block_hash: crate::sql_row::get(&row, "block_hash")?,
        block_number: crate::sql_row::get(&row, "block_number")?,
        provenance: crate::sql_row::get(&row, "provenance")?,
        canonicality_state: crate::sql_row::get(&row, "canonicality_state")?,
    })
}

pub(super) fn decode_resource(row: PgRow) -> Result<Resource> {
    Ok(Resource {
        resource_id: crate::sql_row::get(&row, "resource_id")?,
        token_lineage_id: crate::sql_row::get(&row, "token_lineage_id")?,
        chain_id: crate::sql_row::get(&row, "chain_id")?,
        block_hash: crate::sql_row::get(&row, "block_hash")?,
        block_number: crate::sql_row::get(&row, "block_number")?,
        provenance: crate::sql_row::get(&row, "provenance")?,
        canonicality_state: crate::sql_row::get(&row, "canonicality_state")?,
    })
}

pub(super) fn decode_name_surface(row: PgRow) -> Result<NameSurface> {
    Ok(NameSurface {
        logical_name_id: crate::sql_row::get(&row, "logical_name_id")?,
        namespace: crate::sql_row::get(&row, "namespace")?,
        input_name: crate::sql_row::get(&row, "input_name")?,
        canonical_display_name: crate::sql_row::get(&row, "canonical_display_name")?,
        normalized_name: crate::sql_row::get(&row, "normalized_name")?,
        dns_encoded_name: crate::sql_row::get(&row, "dns_encoded_name")?,
        namehash: crate::sql_row::get(&row, "namehash")?,
        labelhashes: crate::sql_row::get(&row, "labelhashes")?,
        normalizer_version: crate::sql_row::get(&row, "normalizer_version")?,
        normalization_warnings: crate::sql_row::get(&row, "normalization_warnings")?,
        normalization_errors: crate::sql_row::get(&row, "normalization_errors")?,
        chain_id: crate::sql_row::get(&row, "chain_id")?,
        block_hash: crate::sql_row::get(&row, "block_hash")?,
        block_number: crate::sql_row::get(&row, "block_number")?,
        provenance: crate::sql_row::get(&row, "provenance")?,
        canonicality_state: crate::sql_row::get(&row, "canonicality_state")?,
    })
}

pub(super) fn decode_surface_binding(row: PgRow) -> Result<SurfaceBinding> {
    Ok(SurfaceBinding {
        surface_binding_id: crate::sql_row::get(&row, "surface_binding_id")?,
        logical_name_id: crate::sql_row::get(&row, "logical_name_id")?,
        resource_id: crate::sql_row::get(&row, "resource_id")?,
        binding_kind: crate::sql_row::get(&row, "binding_kind")?,
        active_from: crate::sql_row::get(&row, "active_from")?,
        active_to: crate::sql_row::get(&row, "active_to")?,
        chain_id: crate::sql_row::get(&row, "chain_id")?,
        block_hash: crate::sql_row::get(&row, "block_hash")?,
        block_number: crate::sql_row::get(&row, "block_number")?,
        provenance: crate::sql_row::get(&row, "provenance")?,
        canonicality_state: crate::sql_row::get(&row, "canonicality_state")?,
    })
}
