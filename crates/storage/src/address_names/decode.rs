use anyhow::{Context, Result, bail};
use sqlx::{Row, postgres::PgRow};

use super::types::{
    AddressNameCurrentEntry, AddressNameCurrentRow, AddressNameRelation,
    AddressNamesCurrentProvenanceSummary, AddressNamesCurrentSummary,
};
use crate::SurfaceBindingKind;

pub(super) fn decode_address_name_current_row(row: PgRow) -> Result<AddressNameCurrentRow> {
    let relation = row
        .try_get::<String, _>("relation")
        .context("missing relation")
        .and_then(|value| AddressNameRelation::parse(&value))?;
    let binding_kind = row
        .try_get::<String, _>("binding_kind")
        .context("missing binding_kind")
        .and_then(|value| parse_surface_binding_kind(&value))?;

    Ok(AddressNameCurrentRow {
        address: row.try_get("address").context("missing address")?,
        logical_name_id: row
            .try_get("logical_name_id")
            .context("missing logical_name_id")?,
        relation,
        namespace: row.try_get("namespace").context("missing namespace")?,
        canonical_display_name: row
            .try_get("canonical_display_name")
            .context("missing canonical_display_name")?,
        normalized_name: row
            .try_get("normalized_name")
            .context("missing normalized_name")?,
        namehash: row.try_get("namehash").context("missing namehash")?,
        surface_binding_id: row
            .try_get("surface_binding_id")
            .context("missing surface_binding_id")?,
        resource_id: row.try_get("resource_id").context("missing resource_id")?,
        token_lineage_id: row
            .try_get("token_lineage_id")
            .context("missing token_lineage_id")?,
        binding_kind,
        provenance: row.try_get("provenance").context("missing provenance")?,
        coverage: row.try_get("coverage").context("missing coverage")?,
        chain_positions: row
            .try_get("chain_positions")
            .context("missing chain_positions")?,
        canonicality_summary: row
            .try_get("canonicality_summary")
            .context("missing canonicality_summary")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
        last_recomputed_at: row
            .try_get("last_recomputed_at")
            .context("missing last_recomputed_at")?,
    })
}

pub(super) fn decode_address_name_current_entry(row: PgRow) -> Result<AddressNameCurrentEntry> {
    let binding_kind = row
        .try_get::<String, _>("binding_kind")
        .context("missing binding_kind")
        .and_then(|value| parse_surface_binding_kind(&value))?;
    let relations = row
        .try_get::<Vec<String>, _>("relations")
        .context("missing relations")?
        .into_iter()
        .map(|value| AddressNameRelation::parse(&value))
        .collect::<Result<Vec<_>>>()?;

    Ok(AddressNameCurrentEntry {
        address: row.try_get("address").context("missing address")?,
        logical_name_id: row
            .try_get("logical_name_id")
            .context("missing logical_name_id")?,
        namespace: row.try_get("namespace").context("missing namespace")?,
        canonical_display_name: row
            .try_get("canonical_display_name")
            .context("missing canonical_display_name")?,
        normalized_name: row
            .try_get("normalized_name")
            .context("missing normalized_name")?,
        namehash: row.try_get("namehash").context("missing namehash")?,
        surface_binding_id: row
            .try_get("surface_binding_id")
            .context("missing surface_binding_id")?,
        resource_id: row.try_get("resource_id").context("missing resource_id")?,
        token_lineage_id: row
            .try_get("token_lineage_id")
            .context("missing token_lineage_id")?,
        binding_kind,
        relations,
        provenance: row.try_get("provenance").context("missing provenance")?,
        coverage: row.try_get("coverage").context("missing coverage")?,
        chain_positions: row
            .try_get("chain_positions")
            .context("missing chain_positions")?,
        canonicality_summary: row
            .try_get("canonicality_summary")
            .context("missing canonicality_summary")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
        last_recomputed_at: row
            .try_get("last_recomputed_at")
            .context("missing last_recomputed_at")?,
    })
}

pub(super) fn decode_address_names_current_summary(
    row: PgRow,
) -> Result<AddressNamesCurrentSummary> {
    let grouped_entry_count = row
        .try_get::<i64, _>("grouped_entry_count")
        .context("missing grouped_entry_count")?;
    let grouped_entry_count =
        u64::try_from(grouped_entry_count).context("negative grouped_entry_count")?;

    Ok(AddressNamesCurrentSummary {
        grouped_entry_count,
        provenance: AddressNamesCurrentProvenanceSummary {
            normalized_event_ids: row
                .try_get("provenance_normalized_event_ids")
                .context("missing provenance_normalized_event_ids")?,
            raw_fact_refs: row
                .try_get("provenance_raw_fact_refs")
                .context("missing provenance_raw_fact_refs")?,
            manifest_versions: row
                .try_get("provenance_manifest_versions")
                .context("missing provenance_manifest_versions")?,
            derivation_kind: row
                .try_get("provenance_derivation_kind")
                .context("missing provenance_derivation_kind")?,
        },
        chain_positions: row
            .try_get("chain_positions")
            .context("missing chain_positions")?,
        consistency: row.try_get("consistency").context("missing consistency")?,
        last_recomputed_at: row
            .try_get("last_recomputed_at")
            .context("missing last_recomputed_at")?,
    })
}

fn parse_surface_binding_kind(value: &str) -> Result<SurfaceBindingKind> {
    match value {
        "declared_registry_path" => Ok(SurfaceBindingKind::DeclaredRegistryPath),
        "linked_subregistry_path" => Ok(SurfaceBindingKind::LinkedSubregistryPath),
        "resolver_alias_path" => Ok(SurfaceBindingKind::ResolverAliasPath),
        "observed_wildcard_path" => Ok(SurfaceBindingKind::ObservedWildcardPath),
        "migration_rebind" => Ok(SurfaceBindingKind::MigrationRebind),
        "observed_only" => Ok(SurfaceBindingKind::ObservedOnly),
        _ => bail!("unknown surface binding kind {value}"),
    }
}
