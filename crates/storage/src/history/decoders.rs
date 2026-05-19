use anyhow::{Result, bail};
use serde_json::Value;
use sqlx::postgres::PgRow;

use super::{HistoryEvent, address_matches::AddressHistoryAnchor};

pub(super) fn decode_address_history_anchor(row: PgRow) -> Result<AddressHistoryAnchor> {
    Ok(AddressHistoryAnchor {
        logical_name_id: crate::sql_row::get(&row, "logical_name_id")?,
        resource_id: crate::sql_row::get(&row, "resource_id")?,
    })
}

pub(super) fn decode_history_event(row: PgRow) -> Result<HistoryEvent> {
    let provenance: Value = crate::sql_row::get(&row, "provenance")?;
    let coverage: Value = crate::sql_row::get(&row, "coverage")?;
    ensure_json_object(&provenance, "provenance")?;
    ensure_json_object(&coverage, "coverage")?;

    Ok(HistoryEvent {
        normalized_event_id: crate::sql_row::get(&row, "normalized_event_id")?,
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
        block_timestamp: crate::sql_row::get(&row, "block_timestamp")?,
        transaction_hash: crate::sql_row::get(&row, "transaction_hash")?,
        log_index: crate::sql_row::get(&row, "log_index")?,
        raw_fact_ref: crate::sql_row::get(&row, "raw_fact_ref")?,
        derivation_kind: crate::sql_row::get(&row, "derivation_kind")?,
        canonicality_state: crate::sql_row::get(&row, "canonicality_state")?,
        before_state: crate::sql_row::get(&row, "before_state")?,
        after_state: crate::sql_row::get(&row, "after_state")?,
        provenance,
        coverage,
    })
}

fn ensure_json_object(value: &Value, field_name: &str) -> Result<()> {
    if !value.is_object() {
        bail!("history field {field_name} must be a JSON object");
    }

    Ok(())
}
