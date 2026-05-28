use std::collections::BTreeSet;

use anyhow::{Context, Result};
use bigname_storage::{
    HistoryScope, load_name_history_head, load_surface_bindings_by_logical_name_id,
};
use sqlx::PgPool;
use uuid::Uuid;

use super::types::{CurrentBindingContext, HistoryHeads, NameSurfaceSeed, RelevantEvent};
use super::{
    BASENAMES_NAMESPACE, CANONICAL_STATE_FILTER, ENS_V1_AUTHORITY_DERIVATION_KIND,
    ENS_V2_REGISTRAR_DERIVATION_KIND, ENS_V2_REGISTRY_DERIVATION_KIND,
    ENS_V2_RESOLVER_DERIVATION_KIND, RELEVANT_EVENT_KINDS, SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
    SOURCE_FAMILY_BASENAMES_BASE_REGISTRY, SOURCE_FAMILY_BASENAMES_BASE_RESOLVER,
};

pub(super) async fn load_history_heads(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<HistoryHeads> {
    let resource_ids = load_name_resource_ids(pool, logical_name_id).await?;
    let surface_head = load_name_history_head(
        pool,
        logical_name_id,
        &resource_ids,
        HistoryScope::Surface,
        true,
    )
    .await
    .with_context(|| {
        format!("failed to load surface history head for logical_name_id {logical_name_id}")
    })?;
    let resource_head = load_name_history_head(
        pool,
        logical_name_id,
        &resource_ids,
        HistoryScope::Resource,
        true,
    )
    .await
    .with_context(|| {
        format!("failed to load resource history head for logical_name_id {logical_name_id}")
    })?;

    Ok(HistoryHeads {
        surface_head,
        resource_head,
    })
}

pub(super) async fn load_name_resource_ids(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Vec<Uuid>> {
    let bindings = load_surface_bindings_by_logical_name_id(pool, logical_name_id)
        .await
        .with_context(|| {
            format!("failed to load resource ids for logical_name_id {logical_name_id}")
        })?;

    Ok(bindings
        .into_iter()
        .map(|binding| binding.resource_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

pub(super) async fn load_canonical_name_surfaces(pool: &PgPool) -> Result<Vec<NameSurfaceSeed>> {
    let rows = sqlx::query_as::<_, NameSurfaceSeed>(&format!(
        r#"
        SELECT
            ns.logical_name_id,
            ns.namespace,
            ns.canonical_display_name,
            ns.normalized_name,
            ns.namehash,
            ns.chain_id,
            ns.block_hash,
            ns.block_number,
            rb.block_timestamp,
            ns.canonicality_state::TEXT AS canonicality_state
        FROM name_surfaces ns
        LEFT JOIN chain_lineage rb
          ON rb.chain_id = ns.chain_id
         AND rb.block_hash = ns.block_hash
        WHERE ns.canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY ns.logical_name_id
        "#
    ))
    .fetch_all(pool)
    .await
    .context("failed to load canonical name_surfaces for name_current rebuild")?;

    Ok(rows)
}

pub(super) async fn load_canonical_name_surface(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Option<NameSurfaceSeed>> {
    let row = sqlx::query_as::<_, NameSurfaceSeed>(&format!(
        r#"
        SELECT
            ns.logical_name_id,
            ns.namespace,
            ns.canonical_display_name,
            ns.normalized_name,
            ns.namehash,
            ns.chain_id,
            ns.block_hash,
            ns.block_number,
            rb.block_timestamp,
            ns.canonicality_state::TEXT AS canonicality_state
        FROM name_surfaces ns
        LEFT JOIN chain_lineage rb
          ON rb.chain_id = ns.chain_id
         AND rb.block_hash = ns.block_hash
        WHERE ns.logical_name_id = $1
          AND ns.canonicality_state {CANONICAL_STATE_FILTER}
        "#
    ))
    .bind(logical_name_id)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!("failed to load canonical name_surface {logical_name_id} for name_current rebuild")
    })?;

    Ok(row)
}

pub(super) async fn load_current_binding_context(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Option<CurrentBindingContext>> {
    let row = sqlx::query_as::<_, CurrentBindingContext>(&format!(
        r#"
        SELECT
            sb.surface_binding_id,
            sb.resource_id,
            r.token_lineage_id,
            sb.binding_kind::TEXT AS binding_kind,
            sb.chain_id,
            sb.block_hash,
            sb.block_number,
            rb.block_timestamp,
            sb.canonicality_state::TEXT AS surface_binding_state,
            r.canonicality_state::TEXT AS resource_state,
            tl.canonicality_state::TEXT AS token_lineage_state
        FROM surface_bindings sb
        JOIN resources r
          ON r.resource_id = sb.resource_id
         AND r.canonicality_state {CANONICAL_STATE_FILTER}
        LEFT JOIN token_lineages tl
          ON tl.token_lineage_id = r.token_lineage_id
         AND tl.canonicality_state {CANONICAL_STATE_FILTER}
        LEFT JOIN chain_lineage rb
          ON rb.chain_id = sb.chain_id
         AND rb.block_hash = sb.block_hash
        WHERE sb.logical_name_id = $1
          AND sb.active_to IS NULL
          AND sb.canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY sb.active_from DESC, sb.surface_binding_id DESC
        LIMIT 1
        "#
    ))
    .bind(logical_name_id)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!("failed to load current binding context for logical_name_id {logical_name_id}")
    })?;

    Ok(row)
}

pub(super) async fn load_relevant_events(
    pool: &PgPool,
    name: &NameSurfaceSeed,
    current_binding: Option<&CurrentBindingContext>,
) -> Result<Vec<RelevantEvent>> {
    let event_kinds = RELEVANT_EVENT_KINDS
        .iter()
        .map(|kind| (*kind).to_owned())
        .collect::<Vec<_>>();
    let derivation_kinds = vec![
        ENS_V1_AUTHORITY_DERIVATION_KIND.to_owned(),
        ENS_V2_REGISTRY_DERIVATION_KIND.to_owned(),
        ENS_V2_REGISTRAR_DERIVATION_KIND.to_owned(),
        ENS_V2_RESOLVER_DERIVATION_KIND.to_owned(),
    ];
    let current_resource_id = current_binding.map(|binding| binding.resource_id);
    let rows = if name.namespace == BASENAMES_NAMESPACE {
        let source_families = [
            SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR.to_owned(),
            SOURCE_FAMILY_BASENAMES_BASE_REGISTRY.to_owned(),
            SOURCE_FAMILY_BASENAMES_BASE_RESOLVER.to_owned(),
        ];
        sqlx::query_as::<_, RelevantEvent>(&format!(
            r#"
            SELECT
                ne.normalized_event_id,
                ne.resource_id,
                ne.event_kind,
                ne.source_family,
                ne.manifest_version,
                ne.source_manifest_id,
                mv.manifest_version AS source_manifest_version,
                mv.namespace AS source_manifest_namespace,
                mv.source_family AS source_manifest_source_family,
                mv.chain AS source_manifest_chain,
                mv.deployment_epoch AS source_manifest_deployment_epoch,
                mv.rollout_status::TEXT AS source_manifest_rollout_status,
                mcf.status::TEXT AS exact_name_profile_status,
                ne.chain_id,
                ne.block_number,
                ne.block_hash,
                rb.block_timestamp,
                ne.raw_fact_ref,
                ne.canonicality_state::TEXT AS canonicality_state,
                ne.after_state
            FROM normalized_events ne
            LEFT JOIN chain_lineage rb
              ON rb.chain_id = ne.chain_id
             AND rb.block_hash = ne.block_hash
            LEFT JOIN manifest_versions mv
              ON mv.manifest_id = ne.source_manifest_id
            LEFT JOIN manifest_capability_flags mcf
              ON mcf.manifest_id = ne.source_manifest_id
             AND mcf.capability_name = 'exact_name_profile'
            WHERE ne.namespace = $1
              AND ne.logical_name_id = $2
              AND ne.derivation_kind = ANY($3::TEXT[])
              AND ne.event_kind = ANY($4::TEXT[])
              AND ne.source_family = ANY($5::TEXT[])
              AND (
                  ne.event_kind <> 'PermissionChanged'
                  OR ne.resource_id = $6
              )
              AND ne.canonicality_state {CANONICAL_STATE_FILTER}
            ORDER BY
                ne.block_number NULLS FIRST,
                CASE
                    WHEN ne.event_kind = 'ResolverChanged'
                     AND ne.after_state ->> 'source_event' = 'AuthorityEpochChanged'
                    THEN -1
                    ELSE COALESCE(ne.log_index, 2147483647)
                END,
                COALESCE(ne.log_index, 2147483647),
                ne.event_identity
            "#
        ))
        .bind(&name.namespace)
        .bind(&name.logical_name_id)
        .bind(&derivation_kinds)
        .bind(&event_kinds)
        .bind(&source_families)
        .bind(current_resource_id)
        .fetch_all(pool)
        .await
    } else {
        sqlx::query_as::<_, RelevantEvent>(&format!(
            r#"
            SELECT
                ne.normalized_event_id,
                ne.resource_id,
                ne.event_kind,
                ne.source_family,
                ne.manifest_version,
                ne.source_manifest_id,
                mv.manifest_version AS source_manifest_version,
                mv.namespace AS source_manifest_namespace,
                mv.source_family AS source_manifest_source_family,
                mv.chain AS source_manifest_chain,
                mv.deployment_epoch AS source_manifest_deployment_epoch,
                mv.rollout_status::TEXT AS source_manifest_rollout_status,
                mcf.status::TEXT AS exact_name_profile_status,
                ne.chain_id,
                ne.block_number,
                ne.block_hash,
                rb.block_timestamp,
                ne.raw_fact_ref,
                ne.canonicality_state::TEXT AS canonicality_state,
                ne.after_state
            FROM normalized_events ne
            LEFT JOIN chain_lineage rb
              ON rb.chain_id = ne.chain_id
             AND rb.block_hash = ne.block_hash
            LEFT JOIN manifest_versions mv
              ON mv.manifest_id = ne.source_manifest_id
            LEFT JOIN manifest_capability_flags mcf
              ON mcf.manifest_id = ne.source_manifest_id
             AND mcf.capability_name = 'exact_name_profile'
            WHERE ne.namespace = $1
              AND ne.logical_name_id = $2
              AND ne.derivation_kind = ANY($3::TEXT[])
              AND ne.event_kind = ANY($4::TEXT[])
              AND (
                  ne.event_kind <> 'PermissionChanged'
                  OR ne.resource_id = $5
              )
              AND ne.canonicality_state {CANONICAL_STATE_FILTER}
            ORDER BY
                ne.block_number NULLS FIRST,
                CASE
                    WHEN ne.event_kind = 'ResolverChanged'
                     AND ne.after_state ->> 'source_event' = 'AuthorityEpochChanged'
                    THEN -1
                    ELSE COALESCE(ne.log_index, 2147483647)
                END,
                COALESCE(ne.log_index, 2147483647),
                ne.event_identity
            "#
        ))
        .bind(&name.namespace)
        .bind(&name.logical_name_id)
        .bind(&derivation_kinds)
        .bind(&event_kinds)
        .bind(current_resource_id)
        .fetch_all(pool)
        .await
    }
    .with_context(|| {
        format!(
            "failed to load authority normalized events for {}",
            name.logical_name_id
        )
    })?;

    Ok(rows)
}
