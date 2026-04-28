use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use bigname_storage::CanonicalityState;
use sqlx::{PgPool, Row};

use super::active_emitters::ActiveEmitter;

#[derive(Clone, Debug)]
pub(super) struct ReverseRawLogRow {
    pub(super) chain_id: String,
    pub(super) block_hash: String,
    pub(super) block_number: i64,
    pub(super) transaction_hash: String,
    pub(super) transaction_index: i64,
    pub(super) log_index: i64,
    pub(super) emitting_address: String,
    pub(super) emitting_contract_instance_id: sqlx::types::Uuid,
    pub(super) topics: Vec<String>,
    pub(super) canonicality_state: CanonicalityState,
    pub(super) source_manifest_id: i64,
    pub(super) namespace: String,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
}

pub(super) async fn load_reverse_raw_logs(
    pool: &PgPool,
    chain: &str,
    active_emitters: &[ActiveEmitter],
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
) -> Result<Vec<ReverseRawLogRow>> {
    let emitters_by_address = active_emitters
        .iter()
        .cloned()
        .map(|emitter| (emitter.address.clone(), emitter))
        .collect::<HashMap<_, _>>();
    let watched_addresses = emitters_by_address.keys().cloned().collect::<Vec<_>>();
    let (scope_addresses, scope_from_blocks, scope_to_blocks) =
        reverse_source_scope_bindings(source_scope);
    if source_scope.is_some() && scope_addresses.is_empty() {
        return Ok(Vec::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT
            rl.chain_id AS chain_id,
            rl.block_hash AS block_hash,
            rl.block_number AS block_number,
            rl.transaction_hash AS transaction_hash,
            rl.transaction_index AS transaction_index,
            rl.log_index AS log_index,
            rl.emitting_address AS emitting_address,
            rl.topics AS topics,
            rl.canonicality_state::TEXT AS canonicality_state
        FROM raw_logs rl
        WHERE rl.chain_id = $1
          AND LOWER(rl.emitting_address) = ANY($2::TEXT[])
          AND ($3::BOOLEAN = FALSE OR rl.block_hash = ANY($4::TEXT[]))
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
        ORDER BY rl.block_number, rl.transaction_index, rl.log_index
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
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load ENSv1 reverse raw logs for chain {chain}"))?;

    rows.into_iter()
        .map(|row| {
            let address = row
                .try_get::<String, _>("emitting_address")
                .context("missing emitting_address")?
                .to_ascii_lowercase();
            let emitter = emitters_by_address.get(&address).with_context(|| {
                format!("missing active emitter metadata for chain {chain} address {address}")
            })?;

            Ok(ReverseRawLogRow {
                chain_id: row.try_get("chain_id").context("missing chain_id")?,
                block_hash: row.try_get("block_hash").context("missing block_hash")?,
                block_number: row
                    .try_get("block_number")
                    .context("missing block_number")?,
                transaction_hash: row
                    .try_get("transaction_hash")
                    .context("missing transaction_hash")?,
                transaction_index: row
                    .try_get("transaction_index")
                    .context("missing transaction_index")?,
                log_index: row.try_get("log_index").context("missing log_index")?,
                emitting_address: address,
                emitting_contract_instance_id: emitter.contract_instance_id,
                topics: row.try_get("topics").context("missing topics")?,
                canonicality_state: parse_canonicality_state(
                    &row.try_get::<String, _>("canonicality_state")
                        .context("missing canonicality_state")?,
                )?,
                source_manifest_id: emitter.source_manifest_id,
                namespace: emitter.namespace.clone(),
                source_family: emitter.source_family.clone(),
                manifest_version: emitter.manifest_version,
            })
        })
        .collect()
}

fn reverse_source_scope_bindings(
    source_scope: Option<&[(String, String, i64, i64)]>,
) -> (Vec<String>, Vec<i64>, Vec<i64>) {
    let mut addresses = Vec::new();
    let mut from_blocks = Vec::new();
    let mut to_blocks = Vec::new();
    for (source_family, address, from_block, to_block) in source_scope.unwrap_or(&[]) {
        if source_family != "ens_v1_reverse_l1" && source_family != "basenames_base_primary" {
            continue;
        }
        addresses.push(address.to_ascii_lowercase());
        from_blocks.push(*from_block);
        to_blocks.push(*to_block);
    }
    (addresses, from_blocks, to_blocks)
}

fn parse_canonicality_state(value: &str) -> Result<CanonicalityState> {
    match value {
        "observed" => Ok(CanonicalityState::Observed),
        "canonical" => Ok(CanonicalityState::Canonical),
        "safe" => Ok(CanonicalityState::Safe),
        "finalized" => Ok(CanonicalityState::Finalized),
        "orphaned" => Ok(CanonicalityState::Orphaned),
        _ => bail!("unknown canonicality_state value {value}"),
    }
}
