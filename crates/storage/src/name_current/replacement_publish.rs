use anyhow::{Context, Result};
use sqlx::{Postgres, Transaction};

pub(super) async fn publish_name_current_replacement_rows(
    executor: &mut Transaction<'_, Postgres>,
) -> Result<usize> {
    let identity_update_count = update_identity_changed_rows(executor).await?;
    let metadata_update_count = update_metadata_changed_rows(executor).await?;
    let insert_count = insert_missing_rows(executor).await?;
    let rows_affected = identity_update_count + metadata_update_count + insert_count;

    usize::try_from(rows_affected).context("name_current replacement row count exceeds usize")
}

async fn update_identity_changed_rows(executor: &mut Transaction<'_, Postgres>) -> Result<u64> {
    sqlx::query(
        r#"
        UPDATE name_current AS target
        SET
            namespace = replacement.namespace,
            canonical_display_name = replacement.canonical_display_name,
            normalized_name = replacement.normalized_name,
            namehash = replacement.namehash,
            surface_binding_id = replacement.surface_binding_id,
            resource_id = replacement.resource_id,
            token_lineage_id = replacement.token_lineage_id,
            binding_kind = replacement.binding_kind,
            declared_summary = replacement.declared_summary,
            provenance = replacement.provenance,
            coverage = replacement.coverage,
            chain_positions = replacement.chain_positions,
            canonicality_summary = replacement.canonicality_summary,
            manifest_version = replacement.manifest_version,
            last_recomputed_at = replacement.last_recomputed_at
        FROM name_current_replacement AS replacement
        WHERE target.logical_name_id = replacement.logical_name_id
          AND (
              target.surface_binding_id IS DISTINCT FROM replacement.surface_binding_id
              OR target.resource_id IS DISTINCT FROM replacement.resource_id
              OR target.token_lineage_id IS DISTINCT FROM replacement.token_lineage_id
          )
        "#,
    )
    .execute(&mut **executor)
    .await
    .context("failed to publish identity-changing name_current replacement rows")
    .map(|result| result.rows_affected())
}

async fn update_metadata_changed_rows(executor: &mut Transaction<'_, Postgres>) -> Result<u64> {
    sqlx::query(
        r#"
        UPDATE name_current AS target
        SET
            namespace = replacement.namespace,
            canonical_display_name = replacement.canonical_display_name,
            normalized_name = replacement.normalized_name,
            namehash = replacement.namehash,
            binding_kind = replacement.binding_kind,
            declared_summary = replacement.declared_summary,
            provenance = replacement.provenance,
            coverage = replacement.coverage,
            chain_positions = replacement.chain_positions,
            canonicality_summary = replacement.canonicality_summary,
            manifest_version = replacement.manifest_version,
            last_recomputed_at = replacement.last_recomputed_at
        FROM name_current_replacement AS replacement
        WHERE target.logical_name_id = replacement.logical_name_id
          AND target.surface_binding_id IS NOT DISTINCT FROM replacement.surface_binding_id
          AND target.resource_id IS NOT DISTINCT FROM replacement.resource_id
          AND target.token_lineage_id IS NOT DISTINCT FROM replacement.token_lineage_id
          AND (
              target.namespace IS DISTINCT FROM replacement.namespace
              OR target.canonical_display_name IS DISTINCT FROM replacement.canonical_display_name
              OR target.normalized_name IS DISTINCT FROM replacement.normalized_name
              OR target.namehash IS DISTINCT FROM replacement.namehash
              OR target.binding_kind IS DISTINCT FROM replacement.binding_kind
              OR target.declared_summary IS DISTINCT FROM replacement.declared_summary
              OR target.provenance IS DISTINCT FROM replacement.provenance
              OR target.coverage IS DISTINCT FROM replacement.coverage
              OR target.chain_positions IS DISTINCT FROM replacement.chain_positions
              OR target.canonicality_summary IS DISTINCT FROM replacement.canonicality_summary
              OR target.manifest_version IS DISTINCT FROM replacement.manifest_version
              OR target.last_recomputed_at IS DISTINCT FROM replacement.last_recomputed_at
          )
        "#,
    )
    .execute(&mut **executor)
    .await
    .context("failed to publish metadata-only name_current replacement rows")
    .map(|result| result.rows_affected())
}

async fn insert_missing_rows(executor: &mut Transaction<'_, Postgres>) -> Result<u64> {
    sqlx::query(
        r#"
        INSERT INTO name_current (
            logical_name_id,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            declared_summary,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        )
        SELECT
            replacement.logical_name_id,
            replacement.namespace,
            replacement.canonical_display_name,
            replacement.normalized_name,
            replacement.namehash,
            replacement.surface_binding_id,
            replacement.resource_id,
            replacement.token_lineage_id,
            replacement.binding_kind,
            replacement.declared_summary,
            replacement.provenance,
            replacement.coverage,
            replacement.chain_positions,
            replacement.canonicality_summary,
            replacement.manifest_version,
            replacement.last_recomputed_at
        FROM name_current_replacement AS replacement
        WHERE NOT EXISTS (
            SELECT 1
            FROM name_current AS target
            WHERE target.logical_name_id = replacement.logical_name_id
        )
        ON CONFLICT (logical_name_id) DO NOTHING
        "#,
    )
    .execute(&mut **executor)
    .await
    .context("failed to publish new name_current replacement rows")
    .map(|result| result.rows_affected())
}
