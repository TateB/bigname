use anyhow::{Context, Result};
use sqlx::{Row, postgres::PgRow};

use super::types::DeclaredChildEventSource;

pub(super) fn decode_declared_child_event_source(row: PgRow) -> Result<DeclaredChildEventSource> {
    Ok(DeclaredChildEventSource {
        parent_logical_name_id: crate::sql_row::get(&row, "parent_logical_name_id")?,
        child_logical_name_id: crate::sql_row::get(&row, "child_logical_name_id")?,
        namespace: crate::sql_row::get(&row, "namespace")?,
        canonical_display_name: crate::sql_row::get(&row, "canonical_display_name")?,
        normalized_name: crate::sql_row::get(&row, "normalized_name")?,
        namehash: crate::sql_row::get(&row, "namehash")?,
        normalized_event_id: crate::sql_row::get(&row, "normalized_event_id")?,
        event_identity: crate::sql_row::get(&row, "event_identity")?,
        source_family: crate::sql_row::get(&row, "source_family")?,
        manifest_version: crate::sql_row::get(&row, "manifest_version")?,
        source_manifest_id: crate::sql_row::get(&row, "source_manifest_id")?,
        chain_id: row
            .try_get::<Option<String>, _>("chain_id")
            .context("missing chain_id")?
            .context("declared child source is missing chain_id")?,
        block_number: row
            .try_get::<Option<i64>, _>("block_number")
            .context("missing block_number")?
            .context("declared child source is missing block_number")?,
        block_hash: row
            .try_get::<Option<String>, _>("block_hash")
            .context("missing block_hash")?
            .context("declared child source is missing block_hash")?,
        transaction_hash: row
            .try_get::<Option<String>, _>("transaction_hash")
            .context("missing transaction_hash")?
            .context("declared child source is missing transaction_hash")?,
        log_index: row
            .try_get::<Option<i64>, _>("log_index")
            .context("missing log_index")?
            .context("declared child source is missing log_index")?,
        raw_fact_ref: crate::sql_row::get(&row, "raw_fact_ref")?,
        normalized_event_ids: crate::sql_row::get(&row, "normalized_event_ids")?,
        raw_fact_refs: crate::sql_row::get(&row, "raw_fact_refs")?,
        manifest_versions: crate::sql_row::get(&row, "manifest_versions")?,
    })
}
