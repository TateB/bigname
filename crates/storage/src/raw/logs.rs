use anyhow::{Context, Result};
use sqlx::PgPool;

use super::{
    decode::decode_raw_log_replay_input,
    types::RawLogReplayInput,
    validation::{validate_replay_hashes, validate_replay_range},
};

/// List canonical persisted raw logs in a finite range for later adapter-owned
/// normalized-event replay. This is read-only: it performs no RPC fetch,
/// checkpoint mutation, projection rebuild, or normalized-event write.
pub async fn list_canonical_raw_log_replay_inputs(
    pool: &PgPool,
    chain_id: &str,
    range_start_block_number: i64,
    range_end_block_number: i64,
) -> Result<Vec<RawLogReplayInput>> {
    validate_replay_range(chain_id, range_start_block_number, range_end_block_number)?;

    let rows = sqlx::query(
        r#"
        SELECT
            logs.raw_log_id,
            logs.chain_id,
            logs.block_hash,
            logs.block_number,
            lineage.parent_hash,
            lineage.block_timestamp,
            lineage.canonicality_state::TEXT AS lineage_canonicality_state,
            logs.transaction_hash,
            logs.transaction_index,
            logs.log_index,
            logs.emitting_address,
            logs.topics,
            logs.data,
            logs.canonicality_state::TEXT AS raw_canonicality_state
        FROM raw_logs AS logs
        JOIN chain_lineage AS lineage
          ON lineage.chain_id = logs.chain_id
         AND lineage.block_hash = logs.block_hash
        WHERE logs.chain_id = $1
          AND logs.block_number >= $2
          AND logs.block_number <= $3
          AND lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND logs.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY logs.block_number, logs.block_hash, logs.transaction_index, logs.log_index, logs.raw_log_id
        "#,
    )
    .bind(chain_id)
    .bind(range_start_block_number)
    .bind(range_end_block_number)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to list canonical raw log replay inputs for chain {chain_id} range {range_start_block_number}..={range_end_block_number}"
        )
    })?;

    rows.into_iter().map(decode_raw_log_replay_input).collect()
}

/// List canonical persisted raw logs for an explicit stored block-hash set.
/// Duplicate input hashes collapse to stored raw log rows in stable block/log
/// order; missing, observed, and orphaned block identities are not inferred.
pub async fn list_canonical_raw_log_replay_inputs_for_block_hashes(
    pool: &PgPool,
    chain_id: &str,
    block_hashes: &[String],
) -> Result<Vec<RawLogReplayInput>> {
    validate_replay_hashes(chain_id, block_hashes)?;
    if block_hashes.is_empty() {
        return Ok(Vec::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT
            logs.raw_log_id,
            logs.chain_id,
            logs.block_hash,
            logs.block_number,
            lineage.parent_hash,
            lineage.block_timestamp,
            lineage.canonicality_state::TEXT AS lineage_canonicality_state,
            logs.transaction_hash,
            logs.transaction_index,
            logs.log_index,
            logs.emitting_address,
            logs.topics,
            logs.data,
            logs.canonicality_state::TEXT AS raw_canonicality_state
        FROM raw_logs AS logs
        JOIN chain_lineage AS lineage
          ON lineage.chain_id = logs.chain_id
         AND lineage.block_hash = logs.block_hash
        WHERE logs.chain_id = $1
          AND logs.block_hash = ANY($2::TEXT[])
          AND lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND logs.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY logs.block_number, logs.block_hash, logs.transaction_index, logs.log_index, logs.raw_log_id
        "#,
    )
    .bind(chain_id)
    .bind(block_hashes)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to list canonical raw log replay inputs for chain {chain_id} across {} block hashes",
            block_hashes.len()
        )
    })?;

    rows.into_iter().map(decode_raw_log_replay_input).collect()
}
