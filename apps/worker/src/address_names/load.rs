use anyhow::{Context, Result};
use bigname_storage::sql_row;
use futures_util::{Stream, StreamExt};
use sqlx::{PgPool, Row};

use super::{
    constants::{CANONICAL_STATE_FILTER, RELEVANT_EVENT_KINDS},
    model::{CurrentBindingSeed, RelevantEvent},
    source_policy::{authority_derivation_kinds, authority_source_families},
    util::{parse_canonicality_state, parse_surface_binding_kind},
};

pub(super) async fn load_current_bindings(pool: &PgPool) -> Result<Vec<CurrentBindingSeed>> {
    let rows = sqlx::query(&format!(
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
    .fetch_all(pool)
    .await
    .context("failed to load current bindings for address_names_current rebuild")?;

    rows.into_iter().map(decode_current_binding_seed).collect()
}

pub(super) fn stream_current_bindings<'a>(
    pool: &'a PgPool,
) -> impl Stream<Item = Result<CurrentBindingSeed>> + 'a {
    sqlx::query(
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
    .map(|row| {
        row.context("failed to stream current bindings for address_names_current rebuild")
            .and_then(decode_current_binding_seed)
    })
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

    let rows = sqlx::query(&format!(
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

    rows.into_iter().map(decode_relevant_event).collect()
}

fn decode_current_binding_seed(row: sqlx::postgres::PgRow) -> Result<CurrentBindingSeed> {
    Ok(CurrentBindingSeed {
        logical_name_id: sql_row::get(&row, "logical_name_id")?,
        namespace: sql_row::get(&row, "namespace")?,
        canonical_display_name: sql_row::get(&row, "canonical_display_name")?,
        normalized_name: sql_row::get(&row, "normalized_name")?,
        namehash: sql_row::get(&row, "namehash")?,
        surface_chain_id: sql_row::get(&row, "surface_chain_id")?,
        surface_block_hash: sql_row::get(&row, "surface_block_hash")?,
        surface_block_number: sql_row::get(&row, "surface_block_number")?,
        surface_block_timestamp: sql_row::get(&row, "surface_block_timestamp")?,
        surface_state: parse_canonicality_state(&sql_row::get::<String>(&row, "surface_state")?)?,
        surface_binding_id: sql_row::get(&row, "surface_binding_id")?,
        resource_id: sql_row::get(&row, "resource_id")?,
        token_lineage_id: sql_row::get(&row, "token_lineage_id")?,
        binding_kind: parse_surface_binding_kind(&sql_row::get::<String>(&row, "binding_kind")?)?,
        binding_chain_id: sql_row::get(&row, "binding_chain_id")?,
        binding_block_hash: sql_row::get(&row, "binding_block_hash")?,
        binding_block_number: sql_row::get(&row, "binding_block_number")?,
        binding_block_timestamp: sql_row::get(&row, "binding_block_timestamp")?,
        binding_state: parse_canonicality_state(&sql_row::get::<String>(&row, "binding_state")?)?,
        resource_state: parse_canonicality_state(&sql_row::get::<String>(&row, "resource_state")?)?,
        token_lineage_state: row
            .try_get::<Option<String>, _>("token_lineage_state")
            .context("missing token_lineage_state")?
            .map(|value| parse_canonicality_state(&value))
            .transpose()?,
    })
}

fn decode_relevant_event(row: sqlx::postgres::PgRow) -> Result<RelevantEvent> {
    Ok(RelevantEvent {
        normalized_event_id: sql_row::get(&row, "normalized_event_id")?,
        event_kind: sql_row::get(&row, "event_kind")?,
        source_family: sql_row::get(&row, "source_family")?,
        manifest_version: sql_row::get(&row, "manifest_version")?,
        source_manifest_id: sql_row::get(&row, "source_manifest_id")?,
        chain_id: sql_row::get(&row, "chain_id")?,
        block_number: sql_row::get(&row, "block_number")?,
        block_hash: sql_row::get(&row, "block_hash")?,
        block_timestamp: sql_row::get(&row, "block_timestamp")?,
        raw_fact_ref: sql_row::get(&row, "raw_fact_ref")?,
        canonicality_state: parse_canonicality_state(&sql_row::get::<String>(
            &row,
            "canonicality_state",
        )?)?,
        after_state: sql_row::get(&row, "after_state")?,
    })
}
