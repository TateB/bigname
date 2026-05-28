use anyhow::{Context, Result};
use bigname_domain::normalization::normalize_name;
use bigname_storage::sql_row;
use sqlx::PgPool;

use crate::ens_v2_common::{
    ActiveEmitter, active_emitter_for_block, emitters_by_address, normalize_address,
    source_scope_bindings,
};

use super::{
    constants::{RESOLVER_EDGE_KIND, SOURCE_FAMILY_ENS_V2_RESOLVER_L1},
    types::{NameLink, ResolverRawLogRow},
    util::{event_position_timestamp, logical_name_id},
};

pub(super) async fn load_name_link_by_namehash(
    pool: &PgPool,
    raw_log: &ResolverRawLogRow,
    namehash: &str,
) -> Result<NameLink> {
    let position = event_position_timestamp(raw_log);
    let row = sqlx::query(
        r#"
        SELECT
            ns.logical_name_id,
            ns.normalized_name,
            ns.canonical_display_name,
            ns.namehash,
            sb.resource_id
        FROM name_surfaces ns
        LEFT JOIN surface_bindings sb
          ON sb.logical_name_id = ns.logical_name_id
         AND sb.active_from <= $3
         AND (sb.active_to IS NULL OR sb.active_to > $3)
         AND sb.canonicality_state IN (
            'canonical'::canonicality_state,
            'safe'::canonicality_state,
            'finalized'::canonicality_state
         )
        WHERE ns.namespace = $1
          AND lower(ns.namehash) = lower($2)
          AND ns.canonicality_state IN (
            'canonical'::canonicality_state,
            'safe'::canonicality_state,
            'finalized'::canonicality_state
         )
        ORDER BY sb.active_from DESC NULLS LAST, sb.surface_binding_id DESC NULLS LAST
        LIMIT 1
        "#,
    )
    .bind(&raw_log.namespace)
    .bind(namehash)
    .bind(position)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load name link for namespace {} node {namehash} at chain position",
            raw_log.namespace
        )
    })?;

    row.map(decode_name_link)
        .transpose()
        .map(|link| link.unwrap_or_else(NameLink::unknown))
}

pub(super) async fn load_name_link_by_name(
    pool: &PgPool,
    raw_log: &ResolverRawLogRow,
    name: &str,
) -> Result<NameLink> {
    let Ok(normalized) = normalize_name(name) else {
        return Ok(NameLink::unknown());
    };
    let normalized_name = normalized.normalized_name;
    let position = event_position_timestamp(raw_log);
    let row = sqlx::query(
        r#"
        SELECT
            ns.logical_name_id,
            ns.normalized_name,
            ns.canonical_display_name,
            ns.namehash,
            sb.resource_id
        FROM name_surfaces ns
        LEFT JOIN surface_bindings sb
          ON sb.logical_name_id = ns.logical_name_id
         AND sb.active_from <= $3
         AND (sb.active_to IS NULL OR sb.active_to > $3)
         AND sb.canonicality_state IN (
            'canonical'::canonicality_state,
            'safe'::canonicality_state,
            'finalized'::canonicality_state
         )
        WHERE ns.namespace = $1
          AND ns.normalized_name = $2
          AND ns.canonicality_state IN (
            'canonical'::canonicality_state,
            'safe'::canonicality_state,
            'finalized'::canonicality_state
         )
        ORDER BY sb.active_from DESC NULLS LAST, sb.surface_binding_id DESC NULLS LAST
        LIMIT 1
        "#,
    )
    .bind(&raw_log.namespace)
    .bind(&normalized_name)
    .bind(position)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load name link for {}:{normalized_name} at chain position",
            raw_log.namespace
        )
    })?;

    Ok(row.map(decode_name_link).transpose()?.unwrap_or(NameLink {
        logical_name_id: Some(logical_name_id(&raw_log.namespace, &normalized_name)),
        normalized_name: Some(normalized_name.clone()),
        canonical_display_name: Some(normalized.canonical_display_name),
        namehash: None,
        resource_id: None,
    }))
}

fn decode_name_link(row: sqlx::postgres::PgRow) -> Result<NameLink> {
    Ok(NameLink {
        logical_name_id: sql_row::get(&row, "logical_name_id")?,
        resource_id: sql_row::get(&row, "resource_id")?,
        normalized_name: sql_row::get(&row, "normalized_name")?,
        canonical_display_name: sql_row::get(&row, "canonical_display_name")?,
        namehash: sql_row::get(&row, "namehash")?,
    })
}

pub(super) async fn load_resolver_raw_logs(
    pool: &PgPool,
    chain: &str,
    emitters: &[ActiveEmitter],
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    max_block_number: Option<i64>,
) -> Result<Vec<ResolverRawLogRow>> {
    if emitters.is_empty() {
        return Ok(Vec::new());
    }

    let active_emitters_by_address = emitters_by_address(emitters);
    let watched_addresses = active_emitters_by_address
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    let (scope_addresses, scope_from_blocks, scope_to_blocks) =
        source_scope_bindings(source_scope, SOURCE_FAMILY_ENS_V2_RESOLVER_L1);
    if source_scope.is_some() && scope_addresses.is_empty() {
        return Ok(Vec::new());
    }
    let has_max_block_number = max_block_number.is_some();
    let max_block_number = max_block_number.unwrap_or(i64::MAX);
    let rows = sqlx::query(
        r#"
        SELECT
            rl.chain_id,
            rl.block_hash,
            rl.block_number,
            rb.block_timestamp
              + (((rl.transaction_index * 1000) + GREATEST(rl.log_index, 0)) * INTERVAL '1 microsecond')
              AS event_position_timestamp,
            rl.transaction_hash,
            rl.transaction_index,
            rl.log_index,
            rl.emitting_address,
            rl.topics,
            rl.data,
            rl.canonicality_state::TEXT AS canonicality_state
        FROM raw_logs rl
        JOIN chain_lineage rb
          ON rb.chain_id = rl.chain_id
         AND rb.block_hash = rl.block_hash
        WHERE rl.chain_id = $1
          AND LOWER(rl.emitting_address) = ANY($2::TEXT[])
          AND ($3::BOOLEAN = FALSE OR rl.block_hash = ANY($4::TEXT[]))
          AND ($9::BOOLEAN = FALSE OR rl.block_number <= $10::BIGINT)
          AND (
              $5::BOOLEAN = FALSE
              OR EXISTS (
                  SELECT 1
                  FROM unnest($6::TEXT[], $7::BIGINT[], $8::BIGINT[])
                    AS source_scope(address, from_block, to_block)
                  WHERE LOWER(rl.emitting_address) = source_scope.address
                    AND rl.block_number >= source_scope.from_block
                    AND rl.block_number <= source_scope.to_block
              )
          )
          AND rl.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY rl.block_number, rl.transaction_index, rl.log_index, LOWER(rl.emitting_address)
        "#,
    )
    .bind(chain)
    .bind(&watched_addresses)
    .bind(restrict_to_block_hashes)
    .bind(block_hashes)
    .bind(source_scope.is_some())
    .bind(&scope_addresses)
    .bind(&scope_from_blocks)
    .bind(&scope_to_blocks)
    .bind(has_max_block_number)
    .bind(max_block_number)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load ENSv2 resolver raw logs for chain {chain}"))?;

    let mut output = Vec::new();
    for row in rows {
        let emitting_address =
            normalize_address(&sql_row::get::<String>(&row, "emitting_address")?);
        let block_number = sql_row::get(&row, "block_number")?;
        let Some(emitter) = active_emitters_by_address
            .get(&emitting_address)
            .and_then(|emitters| active_emitter_for_block(emitters, block_number))
        else {
            continue;
        };
        output.push(ResolverRawLogRow {
            chain_id: sql_row::get(&row, "chain_id")?,
            block_hash: sql_row::get(&row, "block_hash")?,
            block_number,
            event_position_timestamp: sql_row::get(&row, "event_position_timestamp")?,
            transaction_hash: sql_row::get(&row, "transaction_hash")?,
            transaction_index: sql_row::get(&row, "transaction_index")?,
            log_index: sql_row::get(&row, "log_index")?,
            emitting_address,
            emitting_contract_instance_id: emitter.contract_instance_id,
            topics: sql_row::get(&row, "topics")?,
            data: sql_row::get(&row, "data")?,
            canonicality_state: sql_row::get(&row, "canonicality_state")?,
            source_manifest_id: emitter.source_manifest_id,
            namespace: emitter.namespace.clone(),
            source_family: emitter.source_family.clone(),
            manifest_version: emitter.manifest_version,
        });
    }
    Ok(output)
}

pub(super) async fn load_active_emitters(pool: &PgPool, chain: &str) -> Result<Vec<ActiveEmitter>> {
    crate::ens_v2_common::load_active_emitters(
        pool,
        chain,
        SOURCE_FAMILY_ENS_V2_RESOLVER_L1,
        RESOLVER_EDGE_KIND,
        "ENSv2 resolver",
    )
    .await
}
