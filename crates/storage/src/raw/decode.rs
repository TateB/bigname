use anyhow::{Context, Result};
use sqlx::{Row, postgres::PgRow};

use super::types::{RawBlock, RawLogReplayInput};
use crate::CanonicalityState;

pub(super) fn decode_raw_log_replay_input(row: PgRow) -> Result<RawLogReplayInput> {
    Ok(RawLogReplayInput {
        raw_log_id: row.try_get("raw_log_id").context("missing raw_log_id")?,
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number")?,
        parent_hash: row.try_get("parent_hash").context("missing parent_hash")?,
        block_timestamp: row
            .try_get("block_timestamp")
            .context("missing block_timestamp")?,
        lineage_canonicality_state: CanonicalityState::parse(
            &row.try_get::<String, _>("lineage_canonicality_state")
                .context("missing lineage_canonicality_state")?,
        )?,
        transaction_hash: row
            .try_get("transaction_hash")
            .context("missing transaction_hash")?,
        transaction_index: row
            .try_get("transaction_index")
            .context("missing transaction_index")?,
        log_index: row.try_get("log_index").context("missing log_index")?,
        emitting_address: row
            .try_get("emitting_address")
            .context("missing emitting_address")?,
        topics: row.try_get("topics").context("missing topics")?,
        data: row.try_get("data").context("missing data")?,
        raw_canonicality_state: CanonicalityState::parse(
            &row.try_get::<String, _>("raw_canonicality_state")
                .context("missing raw_canonicality_state")?,
        )?,
    })
}

pub(super) fn decode_raw_block(row: PgRow) -> Result<RawBlock> {
    Ok(RawBlock {
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        parent_hash: row.try_get("parent_hash").context("missing parent_hash")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number")?,
        block_timestamp: row
            .try_get("block_timestamp")
            .context("missing block_timestamp")?,
        logs_bloom: row.try_get("logs_bloom").context("missing logs_bloom")?,
        transactions_root: row
            .try_get("transactions_root")
            .context("missing transactions_root")?,
        receipts_root: row
            .try_get("receipts_root")
            .context("missing receipts_root")?,
        state_root: row.try_get("state_root").context("missing state_root")?,
        canonicality_state: CanonicalityState::parse(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing canonicality_state")?,
        )?,
    })
}
