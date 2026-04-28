use std::collections::HashMap;

use anyhow::{Context, Result};
use sqlx::PgPool;

use super::types::{NameSurface, Resource, SurfaceBinding, TokenLineage};
use super::validate::{
    validate_name_surface, validate_resource, validate_surface_binding, validate_token_lineage,
};
use super::write_fast::{
    insert_name_surfaces_do_nothing, insert_resources_do_nothing,
    insert_surface_bindings_do_nothing, insert_token_lineages_do_nothing,
    load_existing_surface_binding_ids,
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

    for token_lineage in token_lineages {
        validate_token_lineage(token_lineage)?;
    }
    let mut inserted_ids =
        insert_token_lineages_do_nothing(&mut transaction, token_lineages).await?;
    let mut snapshots = Vec::with_capacity(token_lineages.len());
    for token_lineage in token_lineages {
        if inserted_ids.remove(&token_lineage.token_lineage_id) {
            snapshots.push(token_lineage.clone());
        } else {
            snapshots.push(upsert_token_lineage(&mut transaction, token_lineage).await?);
        }
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

    for resource in resources {
        validate_resource(resource)?;
    }
    let mut inserted_ids = insert_resources_do_nothing(&mut transaction, resources).await?;
    let mut snapshots = Vec::with_capacity(resources.len());
    for resource in resources {
        if inserted_ids.remove(&resource.resource_id) {
            snapshots.push(resource.clone());
        } else {
            snapshots.push(upsert_resource(&mut transaction, resource).await?);
        }
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

    for name_surface in name_surfaces {
        validate_name_surface(name_surface)?;
    }
    let mut inserted_ids = insert_name_surfaces_do_nothing(&mut transaction, name_surfaces).await?;
    let mut snapshots = Vec::with_capacity(name_surfaces.len());
    for name_surface in name_surfaces {
        if inserted_ids.remove(&name_surface.logical_name_id) {
            snapshots.push(name_surface.clone());
        } else {
            snapshots.push(upsert_name_surface(&mut transaction, name_surface).await?);
        }
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

    for binding in bindings {
        validate_surface_binding(binding)?;
    }
    let existing_ids = load_existing_surface_binding_ids(&mut transaction, bindings).await?;
    let mut existing_snapshots = HashMap::new();
    for binding in bindings
        .iter()
        .filter(|binding| existing_ids.contains(&binding.surface_binding_id))
    {
        existing_snapshots.insert(
            binding.surface_binding_id,
            upsert_surface_binding(&mut transaction, binding).await?,
        );
    }
    let new_bindings = bindings
        .iter()
        .filter(|binding| !existing_ids.contains(&binding.surface_binding_id))
        .cloned()
        .collect::<Vec<_>>();
    let mut inserted_ids =
        insert_surface_bindings_do_nothing(&mut transaction, &new_bindings).await?;
    let mut snapshots = Vec::with_capacity(bindings.len());
    for binding in bindings {
        if let Some(snapshot) = existing_snapshots.remove(&binding.surface_binding_id) {
            snapshots.push(snapshot);
        } else if inserted_ids.remove(&binding.surface_binding_id) {
            snapshots.push(binding.clone());
        } else {
            snapshots.push(upsert_surface_binding(&mut transaction, binding).await?);
        }
    }

    transaction
        .commit()
        .await
        .context("failed to commit surface-binding upsert")?;

    Ok(snapshots)
}
