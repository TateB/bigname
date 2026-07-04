use anyhow::{Context, Result};
use sqlx::Postgres;

use super::super::{
    merge::merge_binding_active_to,
    read::{decode_surface_binding, load_surface_binding_internal},
    types::SurfaceBinding,
    validate::ensure_surface_binding_identity_matches,
};

pub(in crate::identity) async fn upsert_surface_binding(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    binding: &SurfaceBinding,
) -> Result<SurfaceBinding> {
    let provenance = serde_json::to_string(&binding.provenance)
        .context("failed to serialize surface-binding provenance")?;

    if let Some(snapshot) = sqlx::query(
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
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11::canonicality_state)
        ON CONFLICT (surface_binding_id) DO NOTHING
        RETURNING
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
        "#,
    )
    .bind(binding.surface_binding_id)
    .bind(&binding.logical_name_id)
    .bind(binding.resource_id)
    .bind(binding.binding_kind.as_str())
    .bind(binding.active_from)
    .bind(binding.active_to)
    .bind(&binding.chain_id)
    .bind(&binding.block_hash)
    .bind(binding.block_number)
    .bind(provenance)
    .bind(binding.canonicality_state.as_str())
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert surface binding {}",
            binding.surface_binding_id
        )
    })? {
        return decode_surface_binding(snapshot);
    }

    let existing =
        load_surface_binding_internal(&mut **executor, binding.surface_binding_id, true, true)
            .await?
            .with_context(|| {
                format!(
                    "failed to reload existing surface binding {} after insert conflict",
                    binding.surface_binding_id
                )
            })?;

    #[cfg(test)]
    super::test_hooks::maybe_wait_after_reload(
        "surface_bindings",
        binding.surface_binding_id.to_string(),
    )
    .await;

    ensure_surface_binding_identity_matches(&existing, binding)?;
    let next_active_to = merge_binding_active_to(existing.active_to, binding.active_to)?;
    let next_state = existing
        .canonicality_state
        .merge_observation(binding.canonicality_state);

    let snapshot = sqlx::query(
        r#"
        UPDATE surface_bindings
        SET
            active_to = $2,
            canonicality_state = $3::canonicality_state,
            observed_at = now()
        WHERE surface_binding_id = $1
        RETURNING
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
        "#,
    )
    .bind(binding.surface_binding_id)
    .bind(next_active_to)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh existing surface binding {}",
            binding.surface_binding_id
        )
    })?;

    decode_surface_binding(snapshot)
}
