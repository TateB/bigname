use anyhow::{Context, Result};
use futures_util::{Stream, StreamExt};
use sqlx::PgPool;

use super::{
    constants::{CANONICAL_STATE_FILTER, RELEVANT_EVENT_KINDS},
    model::{CurrentBindingSeed, RelevantEvent},
    source_policy::{authority_derivation_kinds, authority_source_families},
};

pub(super) async fn load_current_bindings_for_address(
    pool: &PgPool,
    address: &str,
) -> Result<Vec<CurrentBindingSeed>> {
    let rows = sqlx::query_as::<_, CurrentBindingSeed>(&format!(
        r#"
        WITH affected_names AS (
            SELECT DISTINCT ne.logical_name_id
            FROM normalized_events ne
            WHERE ne.logical_name_id IS NOT NULL
              AND ne.event_kind = 'RegistrationGranted'
              AND ne.canonicality_state {CANONICAL_STATE_FILTER}
              AND ne.after_state ->> 'registrant' IS NOT NULL
              AND ne.after_state ->> 'registrant' <> ''
              AND lower(ne.after_state ->> 'registrant') = $1

            UNION

            SELECT DISTINCT ne.logical_name_id
            FROM normalized_events ne
            WHERE ne.logical_name_id IS NOT NULL
              AND ne.event_kind = 'TokenControlTransferred'
              AND ne.canonicality_state {CANONICAL_STATE_FILTER}
              AND ne.after_state ->> 'to' IS NOT NULL
              AND ne.after_state ->> 'to' <> ''
              AND lower(ne.after_state ->> 'to') = $1

            UNION

            SELECT DISTINCT ne.logical_name_id
            FROM normalized_events ne
            WHERE ne.logical_name_id IS NOT NULL
              AND ne.event_kind = 'AuthorityTransferred'
              AND ne.canonicality_state {CANONICAL_STATE_FILTER}
              AND ne.after_state ->> 'owner' IS NOT NULL
              AND ne.after_state ->> 'owner' <> ''
              AND lower(ne.after_state ->> 'owner') = $1
        )
        SELECT
            ns.logical_name_id,
            ns.namespace,
            ns.canonical_display_name,
            ns.normalized_name,
            ns.namehash,
            ns.chain_id AS surface_chain_id,
            ns.block_hash AS surface_block_hash,
            ns.block_number AS surface_block_number,
            surface_block.block_timestamp AS surface_block_timestamp,
            ns.canonicality_state::TEXT AS surface_state,
            sb.surface_binding_id,
            sb.resource_id,
            r.token_lineage_id,
            sb.binding_kind::TEXT AS binding_kind,
            sb.chain_id AS binding_chain_id,
            sb.block_hash AS binding_block_hash,
            sb.block_number AS binding_block_number,
            binding_block.block_timestamp AS binding_block_timestamp,
            sb.canonicality_state::TEXT AS binding_state,
            r.canonicality_state::TEXT AS resource_state,
            tl.canonicality_state::TEXT AS token_lineage_state
        FROM surface_bindings sb
        JOIN affected_names affected
          ON affected.logical_name_id = sb.logical_name_id
        JOIN name_surfaces ns
          ON ns.logical_name_id = sb.logical_name_id
         AND ns.canonicality_state {CANONICAL_STATE_FILTER}
        JOIN resources r
          ON r.resource_id = sb.resource_id
         AND r.canonicality_state {CANONICAL_STATE_FILTER}
        LEFT JOIN token_lineages tl
          ON tl.token_lineage_id = r.token_lineage_id
         AND tl.canonicality_state {CANONICAL_STATE_FILTER}
        LEFT JOIN chain_lineage surface_block
          ON surface_block.chain_id = ns.chain_id
         AND surface_block.block_hash = ns.block_hash
        LEFT JOIN chain_lineage binding_block
          ON binding_block.chain_id = sb.chain_id
         AND binding_block.block_hash = sb.block_hash
        WHERE sb.active_to IS NULL
          AND sb.canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY ns.logical_name_id
        "#
    ))
    .bind(address)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load current bindings for address_names_current address {address}")
    })?;

    Ok(rows)
}

pub(super) fn stream_current_bindings<'a>(
    pool: &'a PgPool,
) -> impl Stream<Item = Result<CurrentBindingSeed>> + 'a {
    sqlx::query_as::<_, CurrentBindingSeed>(
        r#"
        SELECT
            ns.logical_name_id,
            ns.namespace,
            ns.canonical_display_name,
            ns.normalized_name,
            ns.namehash,
            ns.chain_id AS surface_chain_id,
            ns.block_hash AS surface_block_hash,
            ns.block_number AS surface_block_number,
            surface_block.block_timestamp AS surface_block_timestamp,
            ns.canonicality_state::TEXT AS surface_state,
            sb.surface_binding_id,
            sb.resource_id,
            r.token_lineage_id,
            sb.binding_kind::TEXT AS binding_kind,
            sb.chain_id AS binding_chain_id,
            sb.block_hash AS binding_block_hash,
            sb.block_number AS binding_block_number,
            binding_block.block_timestamp AS binding_block_timestamp,
            sb.canonicality_state::TEXT AS binding_state,
            r.canonicality_state::TEXT AS resource_state,
            tl.canonicality_state::TEXT AS token_lineage_state
        FROM surface_bindings sb
        JOIN name_surfaces ns
          ON ns.logical_name_id = sb.logical_name_id
         AND ns.canonicality_state IN (
             'canonical'::canonicality_state,
             'safe'::canonicality_state,
             'finalized'::canonicality_state
         )
        JOIN resources r
          ON r.resource_id = sb.resource_id
         AND r.canonicality_state IN (
             'canonical'::canonicality_state,
             'safe'::canonicality_state,
             'finalized'::canonicality_state
         )
        LEFT JOIN token_lineages tl
          ON tl.token_lineage_id = r.token_lineage_id
         AND tl.canonicality_state IN (
             'canonical'::canonicality_state,
             'safe'::canonicality_state,
             'finalized'::canonicality_state
         )
        LEFT JOIN chain_lineage surface_block
          ON surface_block.chain_id = ns.chain_id
         AND surface_block.block_hash = ns.block_hash
        LEFT JOIN chain_lineage binding_block
          ON binding_block.chain_id = sb.chain_id
         AND binding_block.block_hash = sb.block_hash
        WHERE sb.active_to IS NULL
          AND sb.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY ns.logical_name_id
        "#,
    )
    .fetch(pool)
    .map(|row| row.context("failed to stream current bindings for address_names_current rebuild"))
}

pub(super) async fn load_relevant_events(
    pool: &PgPool,
    namespace: &str,
    logical_name_id: &str,
    authority_chain_id: &str,
) -> Result<Vec<RelevantEvent>> {
    let event_kinds = RELEVANT_EVENT_KINDS
        .iter()
        .map(|kind| (*kind).to_owned())
        .collect::<Vec<_>>();
    let derivation_kinds = authority_derivation_kinds(namespace)
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let source_families = authority_source_families(namespace);

    let rows = sqlx::query_as::<_, RelevantEvent>(&format!(
        r#"
        SELECT
            ne.normalized_event_id,
            ne.event_kind,
            ne.source_family,
            ne.manifest_version,
            ne.source_manifest_id,
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
        WHERE ne.namespace = $1
          AND ne.logical_name_id = $2
          AND ne.derivation_kind = ANY($3::TEXT[])
          AND ne.event_kind = ANY($4::TEXT[])
          AND ne.source_family = ANY($5::TEXT[])
          AND ne.chain_id = $6
          AND ne.canonicality_state {CANONICAL_STATE_FILTER}
        ORDER BY
            ne.block_number NULLS FIRST,
            COALESCE(ne.log_index, 2147483647),
            ne.event_identity
        "#
    ))
    .bind(namespace)
    .bind(logical_name_id)
    .bind(&derivation_kinds)
    .bind(&event_kinds)
    .bind(&source_families)
    .bind(authority_chain_id)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load address-name normalized events for {logical_name_id}")
    })?;

    Ok(rows)
}
