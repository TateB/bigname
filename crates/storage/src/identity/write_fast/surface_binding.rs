use anyhow::{Context, Result, bail};
use sqlx::Postgres;
use uuid::Uuid;

use super::{
    IDENTITY_FAST_INSERT_BATCH_SIZE,
    sql::{
        canonicality_merge_sql, canonicality_merge_sql_from, surface_binding_active_to_merge_sql,
        surface_binding_provenance_compatible_sql, surface_binding_provenance_merge_sql,
    },
    unique_uuid_count,
};
use crate::identity::types::SurfaceBinding;

pub(in crate::identity) async fn bulk_upsert_surface_bindings_without_snapshots(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    bindings: &[SurfaceBinding],
) -> Result<()> {
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

        let expected_count = unique_uuid_count(surface_binding_ids.iter().copied());
        let active_to_merge = surface_binding_active_to_merge_sql("surface_bindings", "EXCLUDED");
        let accepted_active_to_merge =
            surface_binding_active_to_merge_sql("surface_bindings", "input_rows");
        let canonicality_merge = canonicality_merge_sql("surface_bindings");
        let accepted_canonicality_merge = canonicality_merge_sql_from(
            "surface_bindings",
            "input_rows.canonicality_state::canonicality_state",
        );
        let provenance_merge = surface_binding_provenance_merge_sql(
            "surface_bindings.provenance",
            "EXCLUDED.provenance",
        );
        let provenance_compatible = surface_binding_provenance_compatible_sql(
            "surface_bindings.provenance",
            "EXCLUDED.provenance",
        );
        let accepted_provenance_compatible = surface_binding_provenance_compatible_sql(
            "surface_bindings.provenance",
            "input_rows.provenance::jsonb",
        );
        let sql = format!(
            r#"
            WITH input_rows AS (
                SELECT DISTINCT ON (surface_binding_id)
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
                ) WITH ORDINALITY AS input(
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
                    canonicality_state,
                    ordinality
                )
                ORDER BY surface_binding_id, ordinality DESC
            ),
            upserted AS (
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
            FROM input_rows
            ON CONFLICT (surface_binding_id) DO UPDATE
            SET
                active_to = {active_to_merge},
                provenance = {provenance_merge},
                canonicality_state = {canonicality_merge},
                observed_at = now()
            WHERE
                surface_bindings.logical_name_id = EXCLUDED.logical_name_id
                AND surface_bindings.resource_id = EXCLUDED.resource_id
                AND surface_bindings.binding_kind = EXCLUDED.binding_kind
                AND surface_bindings.active_from = EXCLUDED.active_from
                AND surface_bindings.chain_id = EXCLUDED.chain_id
                AND surface_bindings.block_hash = EXCLUDED.block_hash
                AND surface_bindings.block_number = EXCLUDED.block_number
                AND {provenance_compatible}
                AND (
                    surface_bindings.active_to IS DISTINCT FROM {active_to_merge}
                    OR surface_bindings.provenance IS DISTINCT FROM {provenance_merge}
                    OR surface_bindings.canonicality_state IS DISTINCT FROM {canonicality_merge}
                )
            RETURNING surface_binding_id
            ),
            accepted_existing AS (
                SELECT input_rows.surface_binding_id
                FROM input_rows
                JOIN surface_bindings
                  ON surface_bindings.surface_binding_id = input_rows.surface_binding_id
                WHERE
                    surface_bindings.logical_name_id = input_rows.logical_name_id
                    AND surface_bindings.resource_id = input_rows.resource_id
                    AND surface_bindings.binding_kind = input_rows.binding_kind
                    AND surface_bindings.active_from = input_rows.active_from
                    AND surface_bindings.chain_id = input_rows.chain_id
                    AND surface_bindings.block_hash = input_rows.block_hash
                    AND surface_bindings.block_number = input_rows.block_number
                    AND {accepted_provenance_compatible}
                    AND surface_bindings.active_to IS NOT DISTINCT FROM {accepted_active_to_merge}
                    AND surface_bindings.canonicality_state IS NOT DISTINCT FROM {accepted_canonicality_merge}
                    AND NOT EXISTS (
                        SELECT 1
                        FROM upserted
                        WHERE upserted.surface_binding_id = input_rows.surface_binding_id
                    )
            )
            SELECT surface_binding_id FROM upserted
            UNION ALL
            SELECT surface_binding_id FROM accepted_existing
            "#,
            active_to_merge = active_to_merge,
            accepted_active_to_merge = accepted_active_to_merge,
            provenance_merge = provenance_merge,
            provenance_compatible = provenance_compatible,
            accepted_provenance_compatible = accepted_provenance_compatible,
            canonicality_merge = canonicality_merge,
            accepted_canonicality_merge = accepted_canonicality_merge,
        );

        let upserted_ids = sqlx::query_scalar::<_, Uuid>(&sql)
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
            .context("failed to bulk upsert surface bindings without snapshots")?;

        if upserted_ids.len() != expected_count {
            bail!(
                "bulk surface-binding upsert skipped {} rows because existing identities were incompatible",
                expected_count.saturating_sub(upserted_ids.len())
            );
        }
    }

    Ok(())
}
