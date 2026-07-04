use alloy_primitives::{hex, keccak256};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct BaseNormalizedRederiveRawFactRangeProof {
    pub replay_target_block: i64,
    pub canonical_raw_log_count: i64,
    pub canonical_raw_log_checksum: String,
    pub canonical_lineage_count: i64,
    pub canonical_lineage_checksum: String,
}

impl BaseNormalizedRederiveRawFactRangeProof {
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

pub fn base_normalized_rederive_json_digest<T>(value: &T) -> Result<String>
where
    T: Serialize + ?Sized,
{
    let bytes =
        serde_json::to_vec(value).context("failed to serialize Base rederive digest input")?;
    Ok(format!("keccak256:{}", hex::encode(keccak256(bytes))))
}

pub(super) async fn load_raw_fact_range_proof(
    pool: &PgPool,
    replay_target_block: i64,
) -> Result<BaseNormalizedRederiveRawFactRangeProof> {
    let row = sqlx::query(raw_fact_range_proof_sql())
        .bind(replay_target_block)
        .fetch_one(pool)
        .await
        .context("failed to load Base normalized-event rederive raw-fact range proof")?;
    raw_fact_range_proof_from_row(&row)
}

pub(super) async fn load_raw_fact_range_proof_from(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    replay_target_block: i64,
) -> Result<BaseNormalizedRederiveRawFactRangeProof> {
    let row = sqlx::query(raw_fact_range_proof_sql())
        .bind(replay_target_block)
        .fetch_one(&mut **transaction)
        .await
        .context("failed to load Base normalized-event rederive raw-fact range proof")?;
    raw_fact_range_proof_from_row(&row)
}

fn raw_fact_range_proof_sql() -> &'static str {
    r#"
    WITH canonical_raw_logs AS (
        SELECT
            raw_logs.chain_id,
            raw_logs.block_hash,
            raw_logs.block_number,
            raw_logs.transaction_hash,
            raw_logs.transaction_index,
            raw_logs.log_index,
            raw_logs.emitting_address,
            raw_logs.topics,
            raw_logs.data
        FROM raw_logs
        JOIN chain_lineage lineage
          ON lineage.chain_id = raw_logs.chain_id
         AND lineage.block_hash = raw_logs.block_hash
        WHERE raw_logs.chain_id = 'base-mainnet'
          AND raw_logs.block_number BETWEEN 17571485 AND $1
          AND raw_logs.canonicality_state IN ('canonical'::canonicality_state, 'safe'::canonicality_state, 'finalized'::canonicality_state)
          AND lineage.canonicality_state IN ('canonical'::canonicality_state, 'safe'::canonicality_state, 'finalized'::canonicality_state)
    ),
    canonical_lineage AS (
        SELECT chain_id, block_hash, parent_hash, block_number, block_timestamp
        FROM chain_lineage
        WHERE chain_id = 'base-mainnet'
          AND block_number BETWEEN 17571485 AND $1
          AND canonicality_state IN ('canonical'::canonicality_state, 'safe'::canonicality_state, 'finalized'::canonicality_state)
    )
    SELECT
        $1::BIGINT AS replay_target_block,
        (SELECT COUNT(*)::BIGINT FROM canonical_raw_logs) AS canonical_raw_log_count,
        (
            SELECT COALESCE(
                SUM(hashtextextended(
                    concat_ws('|',
                        chain_id,
                        block_hash,
                        block_number::TEXT,
                        transaction_hash,
                        transaction_index::TEXT,
                        log_index::TEXT,
                        emitting_address,
                        array_to_json(topics)::TEXT,
                        encode(data, 'hex')
                    ),
                    0
                )::NUMERIC),
                0
            )::TEXT
            FROM canonical_raw_logs
        ) AS canonical_raw_log_checksum,
        (SELECT COUNT(*)::BIGINT FROM canonical_lineage) AS canonical_lineage_count,
        (
            SELECT COALESCE(
                SUM(hashtextextended(
                    concat_ws('|',
                        chain_id,
                        block_hash,
                        COALESCE(parent_hash, ''),
                        block_number::TEXT,
                        block_timestamp::TEXT
                    ),
                    0
                )::NUMERIC),
                0
            )::TEXT
            FROM canonical_lineage
        ) AS canonical_lineage_checksum
    "#
}

fn raw_fact_range_proof_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<BaseNormalizedRederiveRawFactRangeProof> {
    Ok(BaseNormalizedRederiveRawFactRangeProof {
        replay_target_block: row.try_get("replay_target_block")?,
        canonical_raw_log_count: row.try_get("canonical_raw_log_count")?,
        canonical_raw_log_checksum: row.try_get("canonical_raw_log_checksum")?,
        canonical_lineage_count: row.try_get("canonical_lineage_count")?,
        canonical_lineage_checksum: row.try_get("canonical_lineage_checksum")?,
    })
}
