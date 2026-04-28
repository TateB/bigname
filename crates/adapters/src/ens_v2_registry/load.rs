use std::collections::HashMap;

use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

use super::{
    emitters::{emitter_for_block_and_scope, scoped_ranges_for_active_emitters},
    types::{ActiveEmitter, RegistryRawLogRow, RegistryRawLogSourceScopeTarget},
    util::{normalize_address, parse_canonicality_state},
};

pub(super) async fn load_registry_raw_logs(
    pool: &PgPool,
    chain: &str,
    emitters: &[ActiveEmitter],
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[RegistryRawLogSourceScopeTarget]>,
) -> Result<Vec<RegistryRawLogRow>> {
    if emitters.is_empty() {
        return Ok(Vec::new());
    }

    let mut emitters_by_address = HashMap::<String, Vec<ActiveEmitter>>::new();
    for emitter in emitters.iter().cloned() {
        emitters_by_address
            .entry(emitter.address.clone())
            .or_default()
            .push(emitter);
    }
    let watched_addresses = emitters_by_address.keys().cloned().collect::<Vec<_>>();
    let watched_range_addresses = emitters
        .iter()
        .map(|emitter| emitter.address.clone())
        .collect::<Vec<_>>();
    let watched_range_from_blocks = emitters
        .iter()
        .map(|emitter| emitter.active_from_block_number.unwrap_or(0))
        .collect::<Vec<_>>();
    let watched_range_to_blocks = emitters
        .iter()
        .map(|emitter| emitter.active_to_block_number.unwrap_or(i64::MAX))
        .collect::<Vec<_>>();

    let scoped_ranges = source_scope
        .map(|source_scope| scoped_ranges_for_active_emitters(source_scope, emitters))
        .transpose()?;
    let rows = if let Some(scoped_ranges) = scoped_ranges.as_ref() {
        if scoped_ranges.is_empty() {
            return Ok(Vec::new());
        }
        let scoped_addresses = scoped_ranges
            .iter()
            .map(|target| target.address.clone())
            .collect::<Vec<_>>();
        let scoped_from_blocks = scoped_ranges
            .iter()
            .map(|target| target.effective_from_block)
            .collect::<Vec<_>>();
        let scoped_to_blocks = scoped_ranges
            .iter()
            .map(|target| target.effective_to_block)
            .collect::<Vec<_>>();

        sqlx::query(
            r#"
            SELECT
                rl.chain_id,
                rl.block_hash,
                rl.block_number,
                rb.block_timestamp,
                rl.transaction_hash,
                rl.transaction_index,
                rl.log_index,
                rl.emitting_address,
                rl.topics,
                rl.data,
                rl.canonicality_state::TEXT AS canonicality_state
            FROM raw_logs rl
            JOIN raw_blocks rb
              ON rb.chain_id = rl.chain_id
             AND rb.block_hash = rl.block_hash
            WHERE rl.chain_id = $1
              AND rl.emitting_address = ANY($2::TEXT[])
              AND ($3::BOOLEAN = FALSE OR rl.block_hash = ANY($4::TEXT[]))
              AND EXISTS (
                  SELECT 1
                  FROM unnest($5::TEXT[], $6::BIGINT[], $7::BIGINT[]) AS watched(
                      address,
                      effective_from_block,
                      effective_to_block
                  )
                  WHERE watched.address = rl.emitting_address
                    AND rl.block_number BETWEEN watched.effective_from_block
                        AND watched.effective_to_block
              )
              AND EXISTS (
                  SELECT 1
                  FROM unnest($8::TEXT[], $9::BIGINT[], $10::BIGINT[]) AS scoped(
                      address,
                      effective_from_block,
                      effective_to_block
                  )
                  WHERE scoped.address = rl.emitting_address
                    AND rl.block_number BETWEEN scoped.effective_from_block
                        AND scoped.effective_to_block
              )
              AND rl.canonicality_state <> 'orphaned'::canonicality_state
            ORDER BY rl.block_number, rl.transaction_index, rl.log_index, rl.emitting_address
            "#,
        )
        .bind(chain)
        .bind(&watched_addresses)
        .bind(restrict_to_block_hashes)
        .bind(block_hashes)
        .bind(&watched_range_addresses)
        .bind(&watched_range_from_blocks)
        .bind(&watched_range_to_blocks)
        .bind(&scoped_addresses)
        .bind(&scoped_from_blocks)
        .bind(&scoped_to_blocks)
        .fetch_all(pool)
        .await
        .with_context(|| {
            format!("failed to load scoped ENSv2 registry raw logs for chain {chain}")
        })?
    } else {
        sqlx::query(
            r#"
            SELECT
                rl.chain_id,
                rl.block_hash,
                rl.block_number,
                rb.block_timestamp,
                rl.transaction_hash,
                rl.transaction_index,
                rl.log_index,
                rl.emitting_address,
                rl.topics,
                rl.data,
                rl.canonicality_state::TEXT AS canonicality_state
            FROM raw_logs rl
            JOIN raw_blocks rb
              ON rb.chain_id = rl.chain_id
             AND rb.block_hash = rl.block_hash
            WHERE rl.chain_id = $1
              AND rl.emitting_address = ANY($2::TEXT[])
              AND ($3::BOOLEAN = FALSE OR rl.block_hash = ANY($4::TEXT[]))
              AND rl.canonicality_state <> 'orphaned'::canonicality_state
            ORDER BY rl.block_number, rl.transaction_index, rl.log_index, rl.emitting_address
            "#,
        )
        .bind(chain)
        .bind(&watched_addresses)
        .bind(restrict_to_block_hashes)
        .bind(block_hashes)
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to load ENSv2 registry raw logs for chain {chain}"))?
    };

    let mut output = Vec::new();
    for row in rows {
        let emitting_address = normalize_address(
            &row.try_get::<String, _>("emitting_address")
                .context("missing emitting_address")?,
        );
        let block_number = row
            .try_get("block_number")
            .context("missing block_number")?;
        let Some(emitter) = emitters_by_address
            .get(&emitting_address)
            .and_then(|emitters| emitter_for_block_and_scope(emitters, block_number, source_scope))
        else {
            continue;
        };
        output.push(RegistryRawLogRow {
            chain_id: row.try_get("chain_id").context("missing chain_id")?,
            block_hash: row.try_get("block_hash").context("missing block_hash")?,
            block_number,
            block_timestamp: row
                .try_get("block_timestamp")
                .context("missing block_timestamp")?,
            transaction_hash: row
                .try_get("transaction_hash")
                .context("missing transaction_hash")?,
            transaction_index: row
                .try_get("transaction_index")
                .context("missing transaction_index")?,
            log_index: row.try_get("log_index").context("missing log_index")?,
            emitting_address,
            topics: row.try_get("topics").context("missing topics")?,
            data: row.try_get("data").context("missing data")?,
            canonicality_state: parse_canonicality_state(
                &row.try_get::<String, _>("canonicality_state")
                    .context("missing canonicality_state")?,
            )?,
            emitting_contract_instance_id: emitter.contract_instance_id,
            source_manifest_id: emitter.source_manifest_id,
            namespace: emitter.namespace.clone(),
            source_family: emitter.source_family.clone(),
            manifest_version: emitter.manifest_version,
            normalizer_version: emitter.normalizer_version.clone(),
        });
    }
    Ok(output)
}
