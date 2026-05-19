use anyhow::Result;
use sqlx::postgres::PgRow;

use super::types::{RawBlock, RawLogReplayInput};
pub(super) fn decode_raw_log_replay_input(row: PgRow) -> Result<RawLogReplayInput> {
    Ok(RawLogReplayInput {
        raw_log_id: crate::sql_row::get(&row, "raw_log_id")?,
        chain_id: crate::sql_row::get(&row, "chain_id")?,
        block_hash: crate::sql_row::get(&row, "block_hash")?,
        block_number: crate::sql_row::get(&row, "block_number")?,
        parent_hash: crate::sql_row::get(&row, "parent_hash")?,
        block_timestamp: crate::sql_row::get(&row, "block_timestamp")?,
        lineage_canonicality_state: crate::sql_row::get(&row, "lineage_canonicality_state")?,
        transaction_hash: crate::sql_row::get(&row, "transaction_hash")?,
        transaction_index: crate::sql_row::get(&row, "transaction_index")?,
        log_index: crate::sql_row::get(&row, "log_index")?,
        emitting_address: crate::sql_row::get(&row, "emitting_address")?,
        topics: crate::sql_row::get(&row, "topics")?,
        data: crate::sql_row::get(&row, "data")?,
        raw_canonicality_state: crate::sql_row::get(&row, "raw_canonicality_state")?,
    })
}

pub(super) fn decode_raw_block(row: PgRow) -> Result<RawBlock> {
    Ok(RawBlock {
        chain_id: crate::sql_row::get(&row, "chain_id")?,
        block_hash: crate::sql_row::get(&row, "block_hash")?,
        parent_hash: crate::sql_row::get(&row, "parent_hash")?,
        block_number: crate::sql_row::get(&row, "block_number")?,
        block_timestamp: crate::sql_row::get(&row, "block_timestamp")?,
        logs_bloom: crate::sql_row::get(&row, "logs_bloom")?,
        transactions_root: crate::sql_row::get(&row, "transactions_root")?,
        receipts_root: crate::sql_row::get(&row, "receipts_root")?,
        state_root: crate::sql_row::get(&row, "state_root")?,
        canonicality_state: crate::sql_row::get(&row, "canonicality_state")?,
    })
}
