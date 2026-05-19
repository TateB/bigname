use anyhow::{Context, Result};
use sqlx::Postgres;

use super::merge::{
    StableObservationInput, merge_stable_row_observation, merge_token_lineage_anchor,
};
use super::read::{
    decode_name_surface, decode_resource, decode_token_lineage, load_name_surface_internal,
    load_resource_internal, load_token_lineage_internal,
};
use super::types::{NameSurface, Resource, TokenLineage};
use super::validate::{
    ensure_name_surface_identity_matches, ensure_resource_identity_matches,
    ensure_token_lineage_identity_matches,
};

mod surface_binding;

pub(super) use surface_binding::upsert_surface_binding;

pub(super) async fn upsert_token_lineage(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    token_lineage: &TokenLineage,
) -> Result<TokenLineage> {
    let provenance = serde_json::to_string(&token_lineage.provenance)
        .context("failed to serialize token-lineage provenance")?;

    if let Some(snapshot) = sqlx::query(
        r#"
        INSERT INTO token_lineages (
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5::jsonb, $6::canonicality_state)
        ON CONFLICT (token_lineage_id) DO NOTHING
        RETURNING
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(token_lineage.token_lineage_id)
    .bind(&token_lineage.chain_id)
    .bind(&token_lineage.block_hash)
    .bind(token_lineage.block_number)
    .bind(provenance)
    .bind(token_lineage.canonicality_state.as_str())
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert token lineage {}",
            token_lineage.token_lineage_id
        )
    })? {
        return decode_token_lineage(snapshot);
    }

    let existing =
        load_token_lineage_internal(&mut **executor, token_lineage.token_lineage_id, true)
            .await?
            .with_context(|| {
                format!(
                    "failed to reload existing token lineage {} after insert conflict",
                    token_lineage.token_lineage_id
                )
            })?;

    ensure_token_lineage_identity_matches(&existing, token_lineage)?;
    let next_observation = merge_stable_row_observation(
        existing.canonicality_state,
        StableObservationInput {
            chain_id: &existing.chain_id,
            block_hash: &existing.block_hash,
            block_number: existing.block_number,
            provenance: &existing.provenance,
        },
        StableObservationInput {
            chain_id: &token_lineage.chain_id,
            block_hash: &token_lineage.block_hash,
            block_number: token_lineage.block_number,
            provenance: &token_lineage.provenance,
        },
    )
    .with_context(|| {
        format!(
            "token lineage {} cannot refresh observation metadata",
            token_lineage.token_lineage_id
        )
    })?;
    let next_state = existing
        .canonicality_state
        .merge_observation(token_lineage.canonicality_state);

    let snapshot = sqlx::query(
        r#"
        UPDATE token_lineages
        SET
            chain_id = $2,
            block_hash = $3,
            block_number = $4,
            provenance = $5::jsonb,
            canonicality_state = $6::canonicality_state,
            observed_at = now()
        WHERE token_lineage_id = $1
        RETURNING
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(token_lineage.token_lineage_id)
    .bind(&next_observation.chain_id)
    .bind(&next_observation.block_hash)
    .bind(next_observation.block_number)
    .bind(next_observation.provenance)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh existing token lineage {}",
            token_lineage.token_lineage_id
        )
    })?;

    decode_token_lineage(snapshot)
}

pub(super) async fn upsert_resource(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    resource: &Resource,
) -> Result<Resource> {
    let provenance = serde_json::to_string(&resource.provenance)
        .context("failed to serialize resource provenance")?;

    if let Some(snapshot) = sqlx::query(
        r#"
        INSERT INTO resources (
            resource_id,
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::canonicality_state)
        ON CONFLICT (resource_id) DO NOTHING
        RETURNING
            resource_id,
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(resource.resource_id)
    .bind(resource.token_lineage_id)
    .bind(&resource.chain_id)
    .bind(&resource.block_hash)
    .bind(resource.block_number)
    .bind(provenance)
    .bind(resource.canonicality_state.as_str())
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| format!("failed to insert resource {}", resource.resource_id))?
    {
        return decode_resource(snapshot);
    }

    let existing = load_resource_internal(&mut **executor, resource.resource_id, true)
        .await?
        .with_context(|| {
            format!(
                "failed to reload existing resource {} after insert conflict",
                resource.resource_id
            )
        })?;

    ensure_resource_identity_matches(&existing, resource)?;
    let next_token_lineage_id =
        merge_token_lineage_anchor(existing.token_lineage_id, resource.token_lineage_id)?;
    let next_observation = merge_stable_row_observation(
        existing.canonicality_state,
        StableObservationInput {
            chain_id: &existing.chain_id,
            block_hash: &existing.block_hash,
            block_number: existing.block_number,
            provenance: &existing.provenance,
        },
        StableObservationInput {
            chain_id: &resource.chain_id,
            block_hash: &resource.block_hash,
            block_number: resource.block_number,
            provenance: &resource.provenance,
        },
    )
    .with_context(|| {
        format!(
            "resource {} cannot refresh observation metadata",
            resource.resource_id
        )
    })?;
    let next_state = existing
        .canonicality_state
        .merge_observation(resource.canonicality_state);

    let snapshot = sqlx::query(
        r#"
        UPDATE resources
        SET
            token_lineage_id = $2,
            chain_id = $3,
            block_hash = $4,
            block_number = $5,
            provenance = $6::jsonb,
            canonicality_state = $7::canonicality_state,
            observed_at = now()
        WHERE resource_id = $1
        RETURNING
            resource_id,
            token_lineage_id,
            chain_id,
            block_hash,
            block_number,
            provenance,
            canonicality_state::TEXT AS canonicality_state
        "#,
    )
    .bind(resource.resource_id)
    .bind(next_token_lineage_id)
    .bind(&next_observation.chain_id)
    .bind(&next_observation.block_hash)
    .bind(next_observation.block_number)
    .bind(next_observation.provenance)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh existing resource {}",
            resource.resource_id
        )
    })?;

    decode_resource(snapshot)
}

pub(super) async fn upsert_name_surface(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    name_surface: &NameSurface,
) -> Result<NameSurface> {
    let normalization_warnings = serde_json::to_string(&name_surface.normalization_warnings)
        .context("failed to serialize name-surface normalization_warnings")?;
    let normalization_errors = serde_json::to_string(&name_surface.normalization_errors)
        .context("failed to serialize name-surface normalization_errors")?;
    let provenance = serde_json::to_string(&name_surface.provenance)
        .context("failed to serialize name-surface provenance")?;

    if let Some(snapshot) = sqlx::query(
        r#"
        INSERT INTO name_surfaces (
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
            canonicality_state
        )
        VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9,
            $10::jsonb,
            $11::jsonb,
            $12,
            $13,
            $14,
            $15::jsonb,
            $16::canonicality_state
        )
        ON CONFLICT (logical_name_id) DO NOTHING
        RETURNING
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
        "#,
    )
    .bind(&name_surface.logical_name_id)
    .bind(&name_surface.namespace)
    .bind(&name_surface.input_name)
    .bind(&name_surface.canonical_display_name)
    .bind(&name_surface.normalized_name)
    .bind(&name_surface.dns_encoded_name)
    .bind(&name_surface.namehash)
    .bind(&name_surface.labelhashes)
    .bind(&name_surface.normalizer_version)
    .bind(normalization_warnings)
    .bind(normalization_errors)
    .bind(&name_surface.chain_id)
    .bind(&name_surface.block_hash)
    .bind(name_surface.block_number)
    .bind(provenance)
    .bind(name_surface.canonicality_state.as_str())
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert name surface {}",
            name_surface.logical_name_id
        )
    })? {
        return decode_name_surface(snapshot);
    }

    let existing = load_name_surface_internal(&mut **executor, &name_surface.logical_name_id, true)
        .await?
        .with_context(|| {
            format!(
                "failed to reload existing name surface {} after insert conflict",
                name_surface.logical_name_id
            )
        })?;

    ensure_name_surface_identity_matches(&existing, name_surface)?;
    let next_observation = merge_stable_row_observation(
        existing.canonicality_state,
        StableObservationInput {
            chain_id: &existing.chain_id,
            block_hash: &existing.block_hash,
            block_number: existing.block_number,
            provenance: &existing.provenance,
        },
        StableObservationInput {
            chain_id: &name_surface.chain_id,
            block_hash: &name_surface.block_hash,
            block_number: name_surface.block_number,
            provenance: &name_surface.provenance,
        },
    )
    .with_context(|| {
        format!(
            "name surface {} cannot refresh observation metadata",
            name_surface.logical_name_id
        )
    })?;
    let next_state = existing
        .canonicality_state
        .merge_observation(name_surface.canonicality_state);

    let snapshot = sqlx::query(
        r#"
        UPDATE name_surfaces
        SET
            chain_id = $2,
            block_hash = $3,
            block_number = $4,
            provenance = $5::jsonb,
            canonicality_state = $6::canonicality_state,
            observed_at = now()
        WHERE logical_name_id = $1
        RETURNING
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
        "#,
    )
    .bind(&name_surface.logical_name_id)
    .bind(&next_observation.chain_id)
    .bind(&next_observation.block_hash)
    .bind(next_observation.block_number)
    .bind(next_observation.provenance)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh existing name surface {}",
            name_surface.logical_name_id
        )
    })?;

    decode_name_surface(snapshot)
}
