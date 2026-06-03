use std::collections::HashSet;

use anyhow::{Context, Result, bail};
use sqlx::Postgres;

use super::{
    IDENTITY_FAST_INSERT_BATCH_SIZE, canonicality_merge_sql, canonicality_merge_sql_from,
    stable_anchor_refresh_required_sql, stable_later_anchor_canonicality_refresh_allowed_sql,
    stable_provenance_merge_sql, unique_string_count,
};
use crate::identity::types::NameSurface;

pub(in crate::identity) async fn insert_name_surfaces_do_nothing(
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

pub(in crate::identity) async fn bulk_upsert_name_surfaces_without_snapshots(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    name_surfaces: &[NameSurface],
) -> Result<()> {
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

        let expected_count = unique_string_count(logical_name_ids.iter().map(String::as_str));
        let accepted_canonicality_merge = canonicality_merge_sql_from(
            "name_surfaces",
            "input_rows.canonicality_state::canonicality_state",
        );
        let sql = format!(
            r#"
            WITH input_rows AS (
                SELECT DISTINCT ON (logical_name_id)
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
                ) WITH ORDINALITY AS input(
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
                    canonicality_state,
                    ordinality
                )
                ORDER BY logical_name_id, ordinality DESC
            ),
            upserted AS (
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
            FROM input_rows
            ON CONFLICT (logical_name_id) DO UPDATE
            SET
                chain_id = CASE WHEN {anchor_refresh} THEN EXCLUDED.chain_id ELSE name_surfaces.chain_id END,
                block_hash = CASE WHEN {anchor_refresh} THEN EXCLUDED.block_hash ELSE name_surfaces.block_hash END,
                block_number = CASE WHEN {anchor_refresh} THEN EXCLUDED.block_number ELSE name_surfaces.block_number END,
                provenance = CASE WHEN {anchor_refresh} THEN {provenance_merge} ELSE name_surfaces.provenance END,
                canonicality_state = {canonicality_merge},
                observed_at = CASE WHEN {anchor_refresh} THEN now() ELSE name_surfaces.observed_at END
            WHERE
                name_surfaces.namespace = EXCLUDED.namespace
                AND name_surfaces.normalized_name = EXCLUDED.normalized_name
                AND name_surfaces.dns_encoded_name = EXCLUDED.dns_encoded_name
                AND name_surfaces.namehash = EXCLUDED.namehash
                AND name_surfaces.labelhashes = EXCLUDED.labelhashes
                AND name_surfaces.normalization_errors = EXCLUDED.normalization_errors
                AND (
                    {anchor_refresh}
                    OR {later_anchor_canonicality_refresh}
                )
            RETURNING logical_name_id
            ),
            accepted_existing AS (
                SELECT input_rows.logical_name_id
                FROM input_rows
                JOIN name_surfaces
                  ON name_surfaces.logical_name_id = input_rows.logical_name_id
                WHERE
                    name_surfaces.namespace = input_rows.namespace
                    AND name_surfaces.normalized_name = input_rows.normalized_name
                    AND name_surfaces.dns_encoded_name = input_rows.dns_encoded_name
                    AND name_surfaces.namehash = input_rows.namehash
                    AND name_surfaces.labelhashes = ARRAY(SELECT jsonb_array_elements_text(input_rows.labelhashes::jsonb))
                    AND name_surfaces.normalization_errors = input_rows.normalization_errors::jsonb
                    AND name_surfaces.canonicality_state IS NOT DISTINCT FROM {accepted_canonicality_merge}
                    AND NOT EXISTS (
                        SELECT 1
                        FROM upserted
                        WHERE upserted.logical_name_id = input_rows.logical_name_id
                    )
            )
            SELECT logical_name_id FROM upserted
            UNION ALL
            SELECT logical_name_id FROM accepted_existing
            "#,
            provenance_merge = stable_provenance_merge_sql("name_surfaces"),
            canonicality_merge = canonicality_merge_sql("name_surfaces"),
            anchor_refresh = stable_anchor_refresh_required_sql("name_surfaces"),
            later_anchor_canonicality_refresh =
                stable_later_anchor_canonicality_refresh_allowed_sql("name_surfaces"),
            accepted_canonicality_merge = accepted_canonicality_merge,
        );

        let upserted_ids = sqlx::query_scalar::<_, String>(&sql)
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
            .context("failed to bulk upsert name surfaces without snapshots")?;

        if upserted_ids.len() != expected_count {
            let rejected_samples = diagnose_skipped_name_surface_upsert_rows(
                executor,
                &logical_name_ids,
                &namespaces,
                &input_names,
                &canonical_display_names,
                &normalized_names,
                &dns_encoded_names,
                &namehashes,
                &labelhashes,
                &normalizer_versions,
                &normalization_warnings,
                &normalization_errors,
                &chain_ids,
                &block_hashes,
                &block_numbers,
                &provenances,
                &canonicality_states,
                &accepted_canonicality_merge,
            )
            .await?;
            bail!(
                "bulk name-surface upsert skipped {} rows because existing identities or observations were incompatible: {}",
                expected_count.saturating_sub(upserted_ids.len()),
                rejected_samples
            );
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn diagnose_skipped_name_surface_upsert_rows(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    logical_name_ids: &[String],
    namespaces: &[String],
    input_names: &[String],
    canonical_display_names: &[String],
    normalized_names: &[String],
    dns_encoded_names: &[Vec<u8>],
    namehashes: &[String],
    labelhashes: &[String],
    normalizer_versions: &[String],
    normalization_warnings: &[String],
    normalization_errors: &[String],
    chain_ids: &[String],
    block_hashes: &[String],
    block_numbers: &[i64],
    provenances: &[String],
    canonicality_states: &[String],
    accepted_canonicality_merge: &str,
) -> Result<serde_json::Value> {
    let diagnostics_sql = format!(
        r#"
        WITH input_rows AS (
            SELECT DISTINCT ON (logical_name_id)
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
            ) WITH ORDINALITY AS input(
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
                canonicality_state,
                ordinality
            )
            ORDER BY logical_name_id, ordinality DESC
        ),
        rejected AS (
            SELECT jsonb_build_object(
                'logical_name_id', input_rows.logical_name_id,
                'incoming', jsonb_build_object(
                    'namespace', input_rows.namespace,
                    'input_name', input_rows.input_name,
                    'canonical_display_name', input_rows.canonical_display_name,
                    'normalized_name', input_rows.normalized_name,
                    'dns_encoded_name_hex', encode(input_rows.dns_encoded_name, 'hex'),
                    'namehash', input_rows.namehash,
                    'labelhashes', input_rows.labelhashes::jsonb,
                    'normalization_errors', input_rows.normalization_errors::jsonb,
                    'chain_id', input_rows.chain_id,
                    'block_hash', input_rows.block_hash,
                    'block_number', input_rows.block_number,
                    'provenance', input_rows.provenance::jsonb,
                    'canonicality_state', input_rows.canonicality_state
                ),
                'existing',
                    CASE
                        WHEN name_surfaces.logical_name_id IS NULL THEN NULL
                        ELSE jsonb_build_object(
                            'namespace', name_surfaces.namespace,
                            'input_name', name_surfaces.input_name,
                            'canonical_display_name', name_surfaces.canonical_display_name,
                            'normalized_name', name_surfaces.normalized_name,
                            'dns_encoded_name_hex', encode(name_surfaces.dns_encoded_name, 'hex'),
                            'namehash', name_surfaces.namehash,
                            'labelhashes', to_jsonb(name_surfaces.labelhashes),
                            'normalization_errors', name_surfaces.normalization_errors,
                            'chain_id', name_surfaces.chain_id,
                            'block_hash', name_surfaces.block_hash,
                            'block_number', name_surfaces.block_number,
                            'provenance', name_surfaces.provenance,
                            'canonicality_state', name_surfaces.canonicality_state
                        )
                    END,
                'mismatch', jsonb_build_object(
                    'missing_existing', name_surfaces.logical_name_id IS NULL,
                    'namespace', name_surfaces.namespace IS DISTINCT FROM input_rows.namespace,
                    'normalized_name', name_surfaces.normalized_name IS DISTINCT FROM input_rows.normalized_name,
                    'dns_encoded_name', name_surfaces.dns_encoded_name IS DISTINCT FROM input_rows.dns_encoded_name,
                    'namehash', name_surfaces.namehash IS DISTINCT FROM input_rows.namehash,
                    'labelhashes', name_surfaces.labelhashes IS DISTINCT FROM ARRAY(SELECT jsonb_array_elements_text(input_rows.labelhashes::jsonb)),
                    'normalization_errors', name_surfaces.normalization_errors IS DISTINCT FROM input_rows.normalization_errors::jsonb,
                    'canonicality_state', name_surfaces.canonicality_state IS DISTINCT FROM {accepted_canonicality_merge}
                )
            ) AS sample
            FROM input_rows
            LEFT JOIN name_surfaces
              ON name_surfaces.logical_name_id = input_rows.logical_name_id
            WHERE
                name_surfaces.logical_name_id IS NULL
                OR NOT (
                    name_surfaces.namespace = input_rows.namespace
                    AND name_surfaces.normalized_name = input_rows.normalized_name
                    AND name_surfaces.dns_encoded_name = input_rows.dns_encoded_name
                    AND name_surfaces.namehash = input_rows.namehash
                    AND name_surfaces.labelhashes = ARRAY(SELECT jsonb_array_elements_text(input_rows.labelhashes::jsonb))
                    AND name_surfaces.normalization_errors = input_rows.normalization_errors::jsonb
                    AND name_surfaces.canonicality_state IS NOT DISTINCT FROM {accepted_canonicality_merge}
                )
            ORDER BY input_rows.logical_name_id
            LIMIT 10
        )
        SELECT COALESCE(jsonb_agg(sample), '[]'::jsonb)
        FROM rejected
        "#,
        accepted_canonicality_merge = accepted_canonicality_merge,
    );
    sqlx::query_scalar::<_, serde_json::Value>(&diagnostics_sql)
        .bind(logical_name_ids)
        .bind(namespaces)
        .bind(input_names)
        .bind(canonical_display_names)
        .bind(normalized_names)
        .bind(dns_encoded_names)
        .bind(namehashes)
        .bind(labelhashes)
        .bind(normalizer_versions)
        .bind(normalization_warnings)
        .bind(normalization_errors)
        .bind(chain_ids)
        .bind(block_hashes)
        .bind(block_numbers)
        .bind(provenances)
        .bind(canonicality_states)
        .fetch_one(&mut **executor)
        .await
        .context("failed to diagnose skipped name-surface upsert rows")
}
