use anyhow::Result;
use sqlx::postgres::PgRow;

use super::types::{RawLog, RawReceipt, RawTransaction};
pub(super) fn decode_raw_transaction(row: PgRow) -> Result<RawTransaction> {
    Ok(RawTransaction {
        chain_id: crate::sql_row::get(&row, "chain_id")?,
        block_hash: crate::sql_row::get(&row, "block_hash")?,
        block_number: crate::sql_row::get(&row, "block_number")?,
        transaction_hash: crate::sql_row::get(&row, "transaction_hash")?,
        transaction_index: crate::sql_row::get(&row, "transaction_index")?,
        from_address: crate::sql_row::get(&row, "from_address")?,
        to_address: crate::sql_row::get(&row, "to_address")?,
        canonicality_state: crate::sql_row::get(&row, "canonicality_state")?,
    })
}

pub(super) fn decode_raw_receipt(row: PgRow) -> Result<RawReceipt> {
    Ok(RawReceipt {
        chain_id: crate::sql_row::get(&row, "chain_id")?,
        block_hash: crate::sql_row::get(&row, "block_hash")?,
        block_number: crate::sql_row::get(&row, "block_number")?,
        transaction_hash: crate::sql_row::get(&row, "transaction_hash")?,
        transaction_index: crate::sql_row::get(&row, "transaction_index")?,
        contract_address: crate::sql_row::get(&row, "contract_address")?,
        status: crate::sql_row::get(&row, "status")?,
        gas_used: crate::sql_row::get(&row, "gas_used")?,
        cumulative_gas_used: crate::sql_row::get(&row, "cumulative_gas_used")?,
        logs_bloom: crate::sql_row::get(&row, "logs_bloom")?,
        canonicality_state: crate::sql_row::get(&row, "canonicality_state")?,
    })
}

pub(super) fn decode_raw_log(row: PgRow) -> Result<RawLog> {
    Ok(RawLog {
        chain_id: crate::sql_row::get(&row, "chain_id")?,
        block_hash: crate::sql_row::get(&row, "block_hash")?,
        block_number: crate::sql_row::get(&row, "block_number")?,
        transaction_hash: crate::sql_row::get(&row, "transaction_hash")?,
        transaction_index: crate::sql_row::get(&row, "transaction_index")?,
        log_index: crate::sql_row::get(&row, "log_index")?,
        emitting_address: crate::sql_row::get(&row, "emitting_address")?,
        topics: crate::sql_row::get(&row, "topics")?,
        data: crate::sql_row::get(&row, "data")?,
        canonicality_state: crate::sql_row::get(&row, "canonicality_state")?,
    })
}
