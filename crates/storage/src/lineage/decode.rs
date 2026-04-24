use anyhow::{Context, Result};
use sqlx::{Row, postgres::PgRow};

use super::types::{CanonicalityState, ChainLineageBlock};

pub(crate) fn decode_lineage_block(row: PgRow) -> Result<ChainLineageBlock> {
    let canonicality_state = CanonicalityState::parse(
        &row.try_get::<String, _>("canonicality_state")
            .context("failed to decode lineage canonicality_state")?,
    )?;

    Ok(ChainLineageBlock {
        chain_id: row
            .try_get::<String, _>("chain_id")
            .context("failed to decode lineage chain_id")?,
        block_hash: row
            .try_get::<String, _>("block_hash")
            .context("failed to decode lineage block_hash")?,
        parent_hash: row
            .try_get::<Option<String>, _>("parent_hash")
            .context("failed to decode lineage parent_hash")?,
        block_number: row
            .try_get::<i64, _>("block_number")
            .context("failed to decode lineage block_number")?,
        block_timestamp: row
            .try_get::<sqlx::types::time::OffsetDateTime, _>("block_timestamp")
            .context("failed to decode lineage block_timestamp")?,
        logs_bloom: row
            .try_get::<Option<Vec<u8>>, _>("logs_bloom")
            .context("failed to decode lineage logs_bloom")?,
        transactions_root: row
            .try_get::<Option<String>, _>("transactions_root")
            .context("failed to decode lineage transactions_root")?,
        receipts_root: row
            .try_get::<Option<String>, _>("receipts_root")
            .context("failed to decode lineage receipts_root")?,
        state_root: row
            .try_get::<Option<String>, _>("state_root")
            .context("failed to decode lineage state_root")?,
        canonicality_state,
    })
}
