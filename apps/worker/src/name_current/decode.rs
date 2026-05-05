use anyhow::{Context, Result};
use bigname_storage::sql_row;
use bigname_storage::{CanonicalityState, SurfaceBindingKind};
use sqlx::Row;

use super::types::{CurrentBindingContext, NameSurfaceSeed, RelevantEvent};

pub(super) fn decode_name_surface_seed(row: sqlx::postgres::PgRow) -> Result<NameSurfaceSeed> {
    Ok(NameSurfaceSeed {
        logical_name_id: row
            .try_get("logical_name_id")
            .context("missing name_surface logical_name_id")?,
        namespace: row
            .try_get("namespace")
            .context("missing name_surface namespace")?,
        canonical_display_name: row
            .try_get("canonical_display_name")
            .context("missing name_surface canonical_display_name")?,
        normalized_name: row
            .try_get("normalized_name")
            .context("missing name_surface normalized_name")?,
        namehash: row
            .try_get("namehash")
            .context("missing name_surface namehash")?,
        chain_id: row
            .try_get("chain_id")
            .context("missing name_surface chain_id")?,
        block_hash: row
            .try_get("block_hash")
            .context("missing name_surface block_hash")?,
        block_number: row
            .try_get("block_number")
            .context("missing name_surface block_number")?,
        block_timestamp: row
            .try_get("block_timestamp")
            .context("missing chain_lineage.block_timestamp join for name_surface")?,
        canonicality_state: parse_canonicality_state(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing name_surface canonicality_state")?,
        )?,
    })
}

pub(super) fn decode_current_binding_context(
    row: sqlx::postgres::PgRow,
) -> Result<CurrentBindingContext> {
    Ok(CurrentBindingContext {
        surface_binding_id: row
            .try_get("surface_binding_id")
            .context("missing surface_binding_id in current binding context")?,
        resource_id: row
            .try_get("resource_id")
            .context("missing resource_id in current binding context")?,
        token_lineage_id: row
            .try_get("token_lineage_id")
            .context("missing token_lineage_id in current binding context")?,
        binding_kind: parse_surface_binding_kind(
            &row.try_get::<String, _>("binding_kind")
                .context("missing binding_kind in current binding context")?,
        )?,
        chain_id: row
            .try_get("chain_id")
            .context("missing chain_id in current binding context")?,
        block_hash: row
            .try_get("block_hash")
            .context("missing block_hash in current binding context")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number in current binding context")?,
        block_timestamp: row
            .try_get("block_timestamp")
            .context("missing block_timestamp in current binding context")?,
        surface_binding_state: parse_canonicality_state(
            &row.try_get::<String, _>("surface_binding_state")
                .context("missing surface_binding_state in current binding context")?,
        )?,
        resource_state: parse_canonicality_state(
            &row.try_get::<String, _>("resource_state")
                .context("missing resource_state in current binding context")?,
        )?,
        token_lineage_state: row
            .try_get::<Option<String>, _>("token_lineage_state")
            .context("missing token_lineage_state in current binding context")?
            .map(|value| parse_canonicality_state(&value))
            .transpose()?,
    })
}

pub(super) fn decode_relevant_event(row: sqlx::postgres::PgRow) -> Result<RelevantEvent> {
    Ok(RelevantEvent {
        normalized_event_id: sql_row::get(&row, "normalized_event_id")?,
        resource_id: sql_row::get(&row, "resource_id")?,
        event_kind: sql_row::get(&row, "event_kind")?,
        source_family: sql_row::get(&row, "source_family")?,
        manifest_version: sql_row::get(&row, "manifest_version")?,
        source_manifest_id: sql_row::get(&row, "source_manifest_id")?,
        source_manifest_version: sql_row::get(&row, "source_manifest_version")?,
        source_manifest_namespace: sql_row::get(&row, "source_manifest_namespace")?,
        source_manifest_source_family: sql_row::get(&row, "source_manifest_source_family")?,
        source_manifest_chain: sql_row::get(&row, "source_manifest_chain")?,
        source_manifest_deployment_epoch: sql_row::get(&row, "source_manifest_deployment_epoch")?,
        source_manifest_rollout_status: sql_row::get(&row, "source_manifest_rollout_status")?,
        exact_name_profile_status: sql_row::get(&row, "exact_name_profile_status")?,
        chain_id: sql_row::get(&row, "chain_id")?,
        block_number: sql_row::get(&row, "block_number")?,
        block_hash: sql_row::get(&row, "block_hash")?,
        block_timestamp: sql_row::get(&row, "block_timestamp")?,
        raw_fact_ref: sql_row::get(&row, "raw_fact_ref")?,
        canonicality_state: parse_canonicality_state(&sql_row::get::<String>(
            &row,
            "canonicality_state",
        )?)?,
        after_state: sql_row::get(&row, "after_state")?,
    })
}

pub(super) fn parse_canonicality_state(value: &str) -> Result<CanonicalityState> {
    CanonicalityState::parse(value)
}

pub(super) fn parse_surface_binding_kind(value: &str) -> Result<SurfaceBindingKind> {
    SurfaceBindingKind::parse(value)
}
