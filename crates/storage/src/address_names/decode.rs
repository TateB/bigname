use anyhow::{Context, Result};
use sqlx::{Row, postgres::PgRow};

use super::types::{
    AddressNameCurrentEntry, AddressNameCurrentRow, AddressNameRelation,
    AddressNamesCurrentProvenanceSummary, AddressNamesCurrentSummary,
};
pub(super) fn decode_address_name_current_row(row: PgRow) -> Result<AddressNameCurrentRow> {
    let relation = row
        .try_get::<String, _>("relation")
        .context("missing relation")
        .and_then(|value| AddressNameRelation::parse(&value))?;
    let binding_kind = crate::sql_row::get(&row, "binding_kind")?;

    Ok(AddressNameCurrentRow {
        address: crate::sql_row::get(&row, "address")?,
        logical_name_id: crate::sql_row::get(&row, "logical_name_id")?,
        relation,
        namespace: crate::sql_row::get(&row, "namespace")?,
        canonical_display_name: crate::sql_row::get(&row, "canonical_display_name")?,
        normalized_name: crate::sql_row::get(&row, "normalized_name")?,
        namehash: crate::sql_row::get(&row, "namehash")?,
        surface_binding_id: crate::sql_row::get(&row, "surface_binding_id")?,
        resource_id: crate::sql_row::get(&row, "resource_id")?,
        token_lineage_id: crate::sql_row::get(&row, "token_lineage_id")?,
        binding_kind,
        provenance: crate::sql_row::get(&row, "provenance")?,
        coverage: crate::sql_row::get(&row, "coverage")?,
        chain_positions: crate::sql_row::get(&row, "chain_positions")?,
        canonicality_summary: crate::sql_row::get(&row, "canonicality_summary")?,
        manifest_version: crate::sql_row::get(&row, "manifest_version")?,
        last_recomputed_at: crate::sql_row::get(&row, "last_recomputed_at")?,
    })
}

pub(super) fn decode_address_name_current_entry(row: PgRow) -> Result<AddressNameCurrentEntry> {
    let binding_kind = crate::sql_row::get(&row, "binding_kind")?;
    let relations = row
        .try_get::<Vec<String>, _>("relations")
        .context("missing relations")?
        .into_iter()
        .map(|value| AddressNameRelation::parse(&value))
        .collect::<Result<Vec<_>>>()?;

    Ok(AddressNameCurrentEntry {
        address: crate::sql_row::get(&row, "address")?,
        logical_name_id: crate::sql_row::get(&row, "logical_name_id")?,
        namespace: crate::sql_row::get(&row, "namespace")?,
        canonical_display_name: crate::sql_row::get(&row, "canonical_display_name")?,
        normalized_name: crate::sql_row::get(&row, "normalized_name")?,
        namehash: crate::sql_row::get(&row, "namehash")?,
        surface_binding_id: crate::sql_row::get(&row, "surface_binding_id")?,
        resource_id: crate::sql_row::get(&row, "resource_id")?,
        token_lineage_id: crate::sql_row::get(&row, "token_lineage_id")?,
        binding_kind,
        relations,
        provenance: crate::sql_row::get(&row, "provenance")?,
        coverage: crate::sql_row::get(&row, "coverage")?,
        chain_positions: crate::sql_row::get(&row, "chain_positions")?,
        canonicality_summary: crate::sql_row::get(&row, "canonicality_summary")?,
        manifest_version: crate::sql_row::get(&row, "manifest_version")?,
        last_recomputed_at: crate::sql_row::get(&row, "last_recomputed_at")?,
    })
}

pub(super) fn decode_address_names_current_summary(
    row: PgRow,
) -> Result<AddressNamesCurrentSummary> {
    let grouped_entry_count = crate::sql_row::get::<i64>(&row, "grouped_entry_count")?;
    let grouped_entry_count =
        u64::try_from(grouped_entry_count).context("negative grouped_entry_count")?;

    Ok(AddressNamesCurrentSummary {
        grouped_entry_count,
        provenance: AddressNamesCurrentProvenanceSummary {
            normalized_event_ids: crate::sql_row::get(&row, "provenance_normalized_event_ids")?,
            raw_fact_refs: crate::sql_row::get(&row, "provenance_raw_fact_refs")?,
            manifest_versions: crate::sql_row::get(&row, "provenance_manifest_versions")?,
            derivation_kind: crate::sql_row::get(&row, "provenance_derivation_kind")?,
        },
        chain_positions: crate::sql_row::get(&row, "chain_positions")?,
        consistency: crate::sql_row::get(&row, "consistency")?,
        last_recomputed_at: crate::sql_row::get(&row, "last_recomputed_at")?,
    })
}
