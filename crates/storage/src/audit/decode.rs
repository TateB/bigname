use anyhow::{Context, Result};
use sqlx::Row;

use crate::{CanonicalityState, ChainLineageBlock};

use super::{
    manifest_state::build_manifest_alert_state,
    types::{
        ManifestDriftAlertKind, ManifestDriftAlertLifecycleStatus, ManifestDriftAlertObservation,
        StoredLineageRangeBlock,
    },
};

pub(super) fn decode_stored_lineage_block(
    row: sqlx::postgres::PgRow,
) -> Result<StoredLineageRangeBlock> {
    Ok(ChainLineageBlock {
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

pub(super) fn decode_manifest_drift_alert_observation(
    row: sqlx::postgres::PgRow,
) -> Result<ManifestDriftAlertObservation> {
    let observation_kind = crate::sql_row::get::<String>(&row, "observation_kind")?;
    let alert_kind = ManifestDriftAlertKind::parse_observation_kind(&observation_kind)?;
    let lifecycle_status = ManifestDriftAlertLifecycleStatus::parse(
        &crate::sql_row::get::<String>(&row, "lifecycle_status")?,
    )?;
    let observed_canonicality_state = crate::sql_row::get(&row, "observed_canonicality_state")?;
    let last_observed_at = crate::sql_row::get(&row, "last_observed_at")?;
    let raw_fact_ref = crate::sql_row::get(&row, "raw_fact_ref")?;
    let alert_state = build_manifest_alert_state(
        alert_kind,
        lifecycle_status,
        &row,
        observed_canonicality_state,
    )?;

    Ok(ManifestDriftAlertObservation {
        normalized_event_id: crate::sql_row::get(&row, "manifest_alert_observation_id")?,
        event_identity: crate::sql_row::get(&row, "observation_identity")?,
        alert_kind,
        namespace: crate::sql_row::get(&row, "namespace")?,
        source_family: crate::sql_row::get(&row, "source_family")?,
        manifest_version: crate::sql_row::get(&row, "manifest_version")?,
        source_manifest_id: crate::sql_row::get(&row, "source_manifest_id")?,
        chain_id: crate::sql_row::get(&row, "chain_id")?,
        block_number: crate::sql_row::get(&row, "observed_block_number")?,
        block_hash: crate::sql_row::get(&row, "observed_block_hash")?,
        raw_fact_ref,
        canonicality_state: observed_canonicality_state.unwrap_or(CanonicalityState::Observed),
        alert_state,
        observed_at: last_observed_at,
    })
}

pub(super) fn decode_count(row: &sqlx::postgres::PgRow, column_name: &str) -> Result<u64> {
    let count = row
        .try_get::<i64, _>(column_name)
        .with_context(|| format!("missing {column_name}"))?;
    u64::try_from(count).with_context(|| format!("{column_name} does not fit in u64"))
}
