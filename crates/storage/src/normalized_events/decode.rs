use anyhow::Result;
use sqlx::postgres::PgRow;

use super::types::NormalizedEvent;

pub(super) fn decode_normalized_event(row: PgRow) -> Result<NormalizedEvent> {
    Ok(NormalizedEvent {
        event_identity: crate::sql_row::get(&row, "event_identity")?,
        namespace: crate::sql_row::get(&row, "namespace")?,
        logical_name_id: crate::sql_row::get(&row, "logical_name_id")?,
        resource_id: crate::sql_row::get(&row, "resource_id")?,
        event_kind: crate::sql_row::get(&row, "event_kind")?,
        source_family: crate::sql_row::get(&row, "source_family")?,
        manifest_version: crate::sql_row::get(&row, "manifest_version")?,
        source_manifest_id: crate::sql_row::get(&row, "source_manifest_id")?,
        chain_id: crate::sql_row::get(&row, "chain_id")?,
        block_number: crate::sql_row::get(&row, "block_number")?,
        block_hash: crate::sql_row::get(&row, "block_hash")?,
        transaction_hash: crate::sql_row::get(&row, "transaction_hash")?,
        log_index: crate::sql_row::get(&row, "log_index")?,
        raw_fact_ref: crate::sql_row::get(&row, "raw_fact_ref")?,
        derivation_kind: crate::sql_row::get(&row, "derivation_kind")?,
        canonicality_state: crate::sql_row::get(&row, "canonicality_state")?,
        before_state: crate::sql_row::get(&row, "before_state")?,
        after_state: crate::sql_row::get(&row, "after_state")?,
    })
}
