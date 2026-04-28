use std::collections::HashSet;

use anyhow::{Context, Result};
use sqlx::Postgres;
use uuid::Uuid;

use super::types::{NameSurface, Resource, SurfaceBinding, TokenLineage};

const IDENTITY_FAST_INSERT_BATCH_SIZE: usize = 10_000;

pub(super) async fn insert_token_lineages_do_nothing(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    token_lineages: &[TokenLineage],
) -> Result<HashSet<Uuid>> {
    let mut inserted_ids = HashSet::new();
    for chunk in token_lineages.chunks(IDENTITY_FAST_INSERT_BATCH_SIZE) {
        let mut token_lineage_ids = Vec::with_capacity(chunk.len());
        let mut chain_ids = Vec::with_capacity(chunk.len());
        let mut block_hashes = Vec::with_capacity(chunk.len());
        let mut block_numbers = Vec::with_capacity(chunk.len());
        let mut provenances = Vec::with_capacity(chunk.len());
        let mut canonicality_states = Vec::with_capacity(chunk.len());

        for token_lineage in chunk {
            token_lineage_ids.push(token_lineage.token_lineage_id);
            chain_ids.push(token_lineage.chain_id.clone());
            block_hashes.push(token_lineage.block_hash.clone());
            block_numbers.push(token_lineage.block_number);
            provenances.push(
                serde_json::to_string(&token_lineage.provenance)
                    .context("failed to serialize token-lineage provenance")?,
            );
            canonicality_states.push(token_lineage.canonicality_state.as_str().to_owned());
        }

        let rows = sqlx::query_scalar::<_, Uuid>(
            r#"
            INSERT INTO token_lineages (
                token_lineage_id,
                chain_id,
                block_hash,
                block_number,
                provenance,
                canonicality_state
            )
            SELECT
                token_lineage_id,
                chain_id,
                block_hash,
                block_number,
                provenance::jsonb,
                canonicality_state::canonicality_state
            FROM unnest(
                $1::UUID[],
                $2::TEXT[],
                $3::TEXT[],
                $4::BIGINT[],
                $5::TEXT[],
                $6::TEXT[]
            ) AS input(
                token_lineage_id,
                chain_id,
                block_hash,
                block_number,
                provenance,
                canonicality_state
            )
            ON CONFLICT (token_lineage_id) DO NOTHING
            RETURNING token_lineage_id
            "#,
        )
        .bind(&token_lineage_ids)
        .bind(&chain_ids)
        .bind(&block_hashes)
        .bind(&block_numbers)
        .bind(&provenances)
        .bind(&canonicality_states)
        .fetch_all(&mut **executor)
        .await
        .context("failed to bulk insert token lineages")?;

        inserted_ids.extend(rows);
    }

    Ok(inserted_ids)
}

pub(super) async fn insert_resources_do_nothing(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    resources: &[Resource],
) -> Result<HashSet<Uuid>> {
    let mut inserted_ids = HashSet::new();
    for chunk in resources.chunks(IDENTITY_FAST_INSERT_BATCH_SIZE) {
        let mut resource_ids = Vec::with_capacity(chunk.len());
        let mut token_lineage_ids = Vec::with_capacity(chunk.len());
        let mut chain_ids = Vec::with_capacity(chunk.len());
        let mut block_hashes = Vec::with_capacity(chunk.len());
        let mut block_numbers = Vec::with_capacity(chunk.len());
        let mut provenances = Vec::with_capacity(chunk.len());
        let mut canonicality_states = Vec::with_capacity(chunk.len());

        for resource in chunk {
            resource_ids.push(resource.resource_id);
            token_lineage_ids.push(resource.token_lineage_id);
            chain_ids.push(resource.chain_id.clone());
            block_hashes.push(resource.block_hash.clone());
            block_numbers.push(resource.block_number);
            provenances.push(
                serde_json::to_string(&resource.provenance)
                    .context("failed to serialize resource provenance")?,
            );
            canonicality_states.push(resource.canonicality_state.as_str().to_owned());
        }

        let rows = sqlx::query_scalar::<_, Uuid>(
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
            SELECT
                resource_id,
                token_lineage_id,
                chain_id,
                block_hash,
                block_number,
                provenance::jsonb,
                canonicality_state::canonicality_state
            FROM unnest(
                $1::UUID[],
                $2::UUID[],
                $3::TEXT[],
                $4::TEXT[],
                $5::BIGINT[],
                $6::TEXT[],
                $7::TEXT[]
            ) AS input(
                resource_id,
                token_lineage_id,
                chain_id,
                block_hash,
                block_number,
                provenance,
                canonicality_state
            )
            ON CONFLICT (resource_id) DO NOTHING
            RETURNING resource_id
            "#,
        )
        .bind(&resource_ids)
        .bind(&token_lineage_ids)
        .bind(&chain_ids)
        .bind(&block_hashes)
        .bind(&block_numbers)
        .bind(&provenances)
        .bind(&canonicality_states)
        .fetch_all(&mut **executor)
        .await
        .context("failed to bulk insert resources")?;

        inserted_ids.extend(rows);
    }

    Ok(inserted_ids)
}

pub(super) async fn insert_name_surfaces_do_nothing(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    name_surfaces: &[NameSurface],
) -> Result<HashSet<String>> {
    let mut inserted_ids = HashSet::new();
    for chunk in name_surfaces.chunks(IDENTITY_FAST_INSERT_BATCH_SIZE) {
        let mut logical_name_ids = Vec::with_capacity(chunk.len());
        let mut namespaces = Vec::with_capacity(chunk.len());
        let mut input_names = Vec::with_capacity(chunk.len());
        let mut canonical_display_names = Vec::with_capacity(chunk.len());
        let mut normalized_names = Vec::with_capacity(chunk.len());
        let mut dns_encoded_names = Vec::with_capacity(chunk.len());
        let mut namehashes = Vec::with_capacity(chunk.len());
        let mut labelhashes = Vec::with_capacity(chunk.len());
        let mut normalizer_versions = Vec::with_capacity(chunk.len());
        let mut normalization_warnings = Vec::with_capacity(chunk.len());
        let mut normalization_errors = Vec::with_capacity(chunk.len());
        let mut chain_ids = Vec::with_capacity(chunk.len());
        let mut block_hashes = Vec::with_capacity(chunk.len());
        let mut block_numbers = Vec::with_capacity(chunk.len());
        let mut provenances = Vec::with_capacity(chunk.len());
        let mut canonicality_states = Vec::with_capacity(chunk.len());

        for surface in chunk {
            logical_name_ids.push(surface.logical_name_id.clone());
            namespaces.push(surface.namespace.clone());
            input_names.push(surface.input_name.clone());
            canonical_display_names.push(surface.canonical_display_name.clone());
            normalized_names.push(surface.normalized_name.clone());
            dns_encoded_names.push(surface.dns_encoded_name.clone());
            namehashes.push(surface.namehash.clone());
            labelhashes.push(
                serde_json::to_string(&surface.labelhashes)
                    .context("failed to serialize name-surface labelhashes")?,
            );
            normalizer_versions.push(surface.normalizer_version.clone());
            normalization_warnings.push(
                serde_json::to_string(&surface.normalization_warnings)
                    .context("failed to serialize name-surface normalization_warnings")?,
            );
            normalization_errors.push(
                serde_json::to_string(&surface.normalization_errors)
                    .context("failed to serialize name-surface normalization_errors")?,
            );
            chain_ids.push(surface.chain_id.clone());
            block_hashes.push(surface.block_hash.clone());
            block_numbers.push(surface.block_number);
            provenances.push(
                serde_json::to_string(&surface.provenance)
                    .context("failed to serialize name-surface provenance")?,
            );
            canonicality_states.push(surface.canonicality_state.as_str().to_owned());
        }

        let rows = sqlx::query_scalar::<_, String>(
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
            SELECT
                logical_name_id,
                namespace,
                input_name,
                canonical_display_name,
                normalized_name,
                dns_encoded_name,
                namehash,
                ARRAY(SELECT jsonb_array_elements_text(labelhashes::jsonb)),
                normalizer_version,
                normalization_warnings::jsonb,
                normalization_errors::jsonb,
                chain_id,
                block_hash,
                block_number,
                provenance::jsonb,
                canonicality_state::canonicality_state
            FROM unnest(
                $1::TEXT[],
                $2::TEXT[],
                $3::TEXT[],
                $4::TEXT[],
                $5::TEXT[],
                $6::BYTEA[],
                $7::TEXT[],
                $8::TEXT[],
                $9::TEXT[],
                $10::TEXT[],
                $11::TEXT[],
                $12::TEXT[],
                $13::TEXT[],
                $14::BIGINT[],
                $15::TEXT[],
                $16::TEXT[]
            ) AS input(
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
            ON CONFLICT (logical_name_id) DO NOTHING
            RETURNING logical_name_id
            "#,
        )
        .bind(&logical_name_ids)
        .bind(&namespaces)
        .bind(&input_names)
        .bind(&canonical_display_names)
        .bind(&normalized_names)
        .bind(&dns_encoded_names)
        .bind(&namehashes)
        .bind(&labelhashes)
        .bind(&normalizer_versions)
        .bind(&normalization_warnings)
        .bind(&normalization_errors)
        .bind(&chain_ids)
        .bind(&block_hashes)
        .bind(&block_numbers)
        .bind(&provenances)
        .bind(&canonicality_states)
        .fetch_all(&mut **executor)
        .await
        .context("failed to bulk insert name surfaces")?;

        inserted_ids.extend(rows);
    }

    Ok(inserted_ids)
}

pub(super) async fn load_existing_surface_binding_ids(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    bindings: &[SurfaceBinding],
) -> Result<HashSet<Uuid>> {
    let surface_binding_ids = bindings
        .iter()
        .map(|binding| binding.surface_binding_id)
        .collect::<Vec<_>>();
    if surface_binding_ids.is_empty() {
        return Ok(HashSet::new());
    }

    let rows = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT surface_binding_id
        FROM surface_bindings
        WHERE surface_binding_id = ANY($1::UUID[])
        "#,
    )
    .bind(&surface_binding_ids)
    .fetch_all(&mut **executor)
    .await
    .context("failed to load existing surface binding ids for batch upsert")?;

    Ok(rows.into_iter().collect())
}

pub(super) async fn insert_surface_bindings_do_nothing(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    bindings: &[SurfaceBinding],
) -> Result<HashSet<Uuid>> {
    let mut inserted_ids = HashSet::new();
    for chunk in bindings.chunks(IDENTITY_FAST_INSERT_BATCH_SIZE) {
        let mut surface_binding_ids = Vec::with_capacity(chunk.len());
        let mut logical_name_ids = Vec::with_capacity(chunk.len());
        let mut resource_ids = Vec::with_capacity(chunk.len());
        let mut binding_kinds = Vec::with_capacity(chunk.len());
        let mut active_froms = Vec::with_capacity(chunk.len());
        let mut active_tos = Vec::with_capacity(chunk.len());
        let mut chain_ids = Vec::with_capacity(chunk.len());
        let mut block_hashes = Vec::with_capacity(chunk.len());
        let mut block_numbers = Vec::with_capacity(chunk.len());
        let mut provenances = Vec::with_capacity(chunk.len());
        let mut canonicality_states = Vec::with_capacity(chunk.len());

        for binding in chunk {
            surface_binding_ids.push(binding.surface_binding_id);
            logical_name_ids.push(binding.logical_name_id.clone());
            resource_ids.push(binding.resource_id);
            binding_kinds.push(binding.binding_kind.as_str().to_owned());
            active_froms.push(binding.active_from);
            active_tos.push(binding.active_to);
            chain_ids.push(binding.chain_id.clone());
            block_hashes.push(binding.block_hash.clone());
            block_numbers.push(binding.block_number);
            provenances.push(
                serde_json::to_string(&binding.provenance)
                    .context("failed to serialize surface-binding provenance")?,
            );
            canonicality_states.push(binding.canonicality_state.as_str().to_owned());
        }

        let rows = sqlx::query_scalar::<_, Uuid>(
            r#"
            INSERT INTO surface_bindings (
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
                canonicality_state
            )
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
                provenance::jsonb,
                canonicality_state::canonicality_state
            FROM unnest(
                $1::UUID[],
                $2::TEXT[],
                $3::UUID[],
                $4::TEXT[],
                $5::TIMESTAMPTZ[],
                $6::TIMESTAMPTZ[],
                $7::TEXT[],
                $8::TEXT[],
                $9::BIGINT[],
                $10::TEXT[],
                $11::TEXT[]
            ) AS input(
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
                canonicality_state
            )
            ON CONFLICT (surface_binding_id) DO NOTHING
            RETURNING surface_binding_id
            "#,
        )
        .bind(&surface_binding_ids)
        .bind(&logical_name_ids)
        .bind(&resource_ids)
        .bind(&binding_kinds)
        .bind(&active_froms)
        .bind(&active_tos)
        .bind(&chain_ids)
        .bind(&block_hashes)
        .bind(&block_numbers)
        .bind(&provenances)
        .bind(&canonicality_states)
        .fetch_all(&mut **executor)
        .await
        .context("failed to bulk insert surface bindings")?;

        inserted_ids.extend(rows);
    }

    Ok(inserted_ids)
}
