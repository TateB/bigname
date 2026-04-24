use anyhow::{Context, Result};
use sqlx::PgPool;

use super::types::{NameSurface, Resource, SurfaceBinding, TokenLineage};
use super::validate::{
    validate_name_surface, validate_resource, validate_surface_binding, validate_token_lineage,
};
use super::write_rows::{
    upsert_name_surface, upsert_resource, upsert_surface_binding, upsert_token_lineage,
};

/// Insert missing token lineage rows or refresh canonicality on re-observation.
pub async fn upsert_token_lineages(
    pool: &PgPool,
    token_lineages: &[TokenLineage],
) -> Result<Vec<TokenLineage>> {
    if token_lineages.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for token-lineage upsert")?;

    let mut snapshots = Vec::with_capacity(token_lineages.len());
    for token_lineage in token_lineages {
        validate_token_lineage(token_lineage)?;
        snapshots.push(upsert_token_lineage(&mut transaction, token_lineage).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit token-lineage upsert")?;

    Ok(snapshots)
}

/// Insert missing resource rows or anchor an existing resource to a token lineage.
pub async fn upsert_resources(pool: &PgPool, resources: &[Resource]) -> Result<Vec<Resource>> {
    if resources.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for resource upsert")?;

    let mut snapshots = Vec::with_capacity(resources.len());
    for resource in resources {
        validate_resource(resource)?;
        snapshots.push(upsert_resource(&mut transaction, resource).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit resource upsert")?;

    Ok(snapshots)
}

/// Insert missing canonical surface rows or refresh canonicality on re-observation.
pub async fn upsert_name_surfaces(
    pool: &PgPool,
    name_surfaces: &[NameSurface],
) -> Result<Vec<NameSurface>> {
    if name_surfaces.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for name-surface upsert")?;

    let mut snapshots = Vec::with_capacity(name_surfaces.len());
    for name_surface in name_surfaces {
        validate_name_surface(name_surface)?;
        snapshots.push(upsert_name_surface(&mut transaction, name_surface).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit name-surface upsert")?;

    Ok(snapshots)
}

/// Insert missing surface-binding rows or close an existing open interval.
pub async fn upsert_surface_bindings(
    pool: &PgPool,
    bindings: &[SurfaceBinding],
) -> Result<Vec<SurfaceBinding>> {
    if bindings.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for surface-binding upsert")?;

    let mut snapshots = Vec::with_capacity(bindings.len());
    for binding in bindings {
        validate_surface_binding(binding)?;
        snapshots.push(upsert_surface_binding(&mut transaction, binding).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit surface-binding upsert")?;

    Ok(snapshots)
}
