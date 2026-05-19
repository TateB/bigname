use bigname_storage::sql_row;
use std::collections::HashMap;

use anyhow::{Context, Result};
use bigname_storage::CanonicalityState;
use sqlx::PgPool;

use crate::ens_v2_common::{normalize_address, source_scope_bindings};

use super::{SOURCE_FAMILY_ENS_V2_REGISTRAR_L1, active_emitters::ActiveEmitter};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RegistrarRawLogRow {
    pub(super) chain_id: String,
    pub(super) block_hash: String,
    pub(super) block_number: i64,
    pub(super) transaction_hash: String,
    pub(super) transaction_index: i64,
    pub(super) log_index: i64,
    pub(super) emitting_address: String,
    pub(super) topics: Vec<String>,
    pub(super) data: Vec<u8>,
    pub(super) canonicality_state: CanonicalityState,
    pub(super) source_manifest_id: i64,
    pub(super) namespace: String,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
}

pub(super) async fn load_registrar_raw_logs(
    pool: &PgPool,
    chain: &str,
    emitters: &[ActiveEmitter],
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    max_block_number: Option<i64>,
) -> Result<Vec<RegistrarRawLogRow>> {
    if emitters.is_empty() {
        return Ok(Vec::new());
    }

    let emitters_by_address = emitters
        .iter()
        .cloned()
        .map(|emitter| (emitter.address.clone(), emitter))
        .collect::<HashMap<_, _>>();
    let watched_addresses = emitters_by_address.keys().cloned().collect::<Vec<_>>();
    let (scope_addresses, scope_from_blocks, scope_to_blocks) =
        source_scope_bindings(source_scope, SOURCE_FAMILY_ENS_V2_REGISTRAR_L1);
    if source_scope.is_some() && scope_addresses.is_empty() {
        return Ok(Vec::new());
    }
    let has_max_block_number = max_block_number.is_some();
    let max_block_number = max_block_number.unwrap_or(i64::MAX);
    let rows = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            emitting_address,
            topics,
            data,
            canonicality_state::TEXT AS canonicality_state
        FROM raw_logs
        WHERE chain_id = $1
          AND LOWER(emitting_address) = ANY($2::TEXT[])
          AND ($3::BOOLEAN = FALSE OR block_hash = ANY($4::TEXT[]))
          AND ($9::BOOLEAN = FALSE OR block_number <= $10::BIGINT)
          AND (
              $5::BOOLEAN = FALSE
              OR EXISTS (
                  SELECT 1
                  FROM unnest($6::TEXT[], $7::BIGINT[], $8::BIGINT[])
                    AS source_scope(address, from_block, to_block)
                  WHERE LOWER(emitting_address) = source_scope.address
                    AND block_number >= source_scope.from_block
                    AND block_number <= source_scope.to_block
              )
          )
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY block_number, transaction_index, log_index, LOWER(emitting_address)
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
    .with_context(|| format!("failed to load ENSv2 registrar raw logs for chain {chain}"))?;

    rows.into_iter()
        .map(|row| {
            let emitting_address = normalize_address(
                &sql_row::get::<String>(&row, "emitting_address")?,
            );
            let emitter = emitters_by_address
                .get(&emitting_address)
                .with_context(|| {
                    format!(
                        "missing ENSv2 registrar emitter attribution for chain {chain} address {emitting_address}"
                    )
                })?;
            Ok(RegistrarRawLogRow {
                chain_id: sql_row::get(&row, "chain_id")?,
                block_hash: sql_row::get(&row, "block_hash")?,
                block_number: sql_row::get(&row, "block_number")?,
                transaction_hash: sql_row::get(&row, "transaction_hash")?,
                transaction_index: sql_row::get(&row, "transaction_index")?,
                log_index: sql_row::get(&row, "log_index")?,
                emitting_address,
                topics: sql_row::get(&row, "topics")?,
                data: sql_row::get(&row, "data")?,
                canonicality_state: sql_row::get(&row, "canonicality_state")?,
                source_manifest_id: emitter.source_manifest_id,
                namespace: emitter.namespace.clone(),
                source_family: emitter.source_family.clone(),
                manifest_version: emitter.manifest_version,
            })
        })
        .collect()
}
