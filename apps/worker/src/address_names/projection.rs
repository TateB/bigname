use anyhow::Result;
use bigname_storage::{AddressNameCurrentRow, AddressNameRelation};
use serde_json::{Value, json};
use sqlx::{PgPool, types::time::OffsetDateTime};

use super::{
    constants::{ADDRESS_NAMES_CURRENT_DERIVATION_KIND, ADDRESS_NAMES_ENUMERATION_BASIS},
    load::load_relevant_events,
    model::{CurrentBindingSeed, ProjectedRelations, RelevantEvent},
    positions::{build_canonicality_summary, build_chain_positions, max_timestamp},
    relations::project_relations,
    source_policy::address_names_source_classes,
    util::dedupe_json_values,
};

pub(super) async fn build_rows_for_binding(
    pool: &PgPool,
    binding: &CurrentBindingSeed,
    address_filter: Option<&str>,
) -> Result<Vec<AddressNameCurrentRow>> {
    let events = load_relevant_events(
        pool,
        &binding.namespace,
        &binding.logical_name_id,
        &binding.surface_chain_id,
        binding.resource_id,
    )
    .await?;
    let relations = project_relations(binding, &events);
    build_relation_rows(binding, &events, relations, address_filter)
}

fn build_relation_rows(
    binding: &CurrentBindingSeed,
    events: &[RelevantEvent],
    relations: ProjectedRelations,
    address_filter: Option<&str>,
) -> Result<Vec<AddressNameCurrentRow>> {
    let manifest_version = events
        .iter()
        .map(|event| event.manifest_version)
        .max()
        .unwrap_or(1);
    let last_recomputed_at = max_timestamp(binding, events).unwrap_or(OffsetDateTime::UNIX_EPOCH);
    let provenance = build_provenance(events)?;
    let coverage = json!({
        "status": "full",
        "exhaustiveness": "authoritative",
        "source_classes_considered": address_names_source_classes(&binding.namespace, events),
        "unsupported_reason": Value::Null,
        "enumeration_basis": ADDRESS_NAMES_ENUMERATION_BASIS,
    });
    let chain_positions = build_chain_positions(binding, events);
    let canonicality_summary = build_canonicality_summary(binding, events);

    let mut rows = Vec::new();
    for (relation, address) in [
        (AddressNameRelation::Registrant, relations.registrant),
        (AddressNameRelation::TokenHolder, relations.token_holder),
        (
            AddressNameRelation::EffectiveController,
            relations.effective_controller,
        ),
    ] {
        let Some(address) = address else {
            continue;
        };
        if address_filter.is_some_and(|value| value != address) {
            continue;
        }

        rows.push(AddressNameCurrentRow {
            address,
            logical_name_id: binding.logical_name_id.clone(),
            relation,
            namespace: binding.namespace.clone(),
            canonical_display_name: binding.canonical_display_name.clone(),
            normalized_name: binding.normalized_name.clone(),
            namehash: binding.namehash.clone(),
            surface_binding_id: binding.surface_binding_id,
            resource_id: binding.resource_id,
            token_lineage_id: binding.token_lineage_id,
            binding_kind: binding.binding_kind,
            provenance: provenance.clone(),
            coverage: coverage.clone(),
            chain_positions: chain_positions.clone(),
            canonicality_summary: canonicality_summary.clone(),
            manifest_version,
            last_recomputed_at,
        });
    }

    Ok(rows)
}

fn build_provenance(events: &[RelevantEvent]) -> Result<Value> {
    let normalized_event_ids = events
        .iter()
        .map(|event| Value::String(event.normalized_event_id.to_string()))
        .collect::<Vec<_>>();
    let raw_fact_refs = dedupe_json_values(events.iter().map(|event| event.raw_fact_ref.clone()))?;
    let manifest_versions = dedupe_json_values(events.iter().map(|event| {
        json!({
            "source_manifest_id": event.source_manifest_id,
            "source_family": event.source_family,
            "manifest_version": event.manifest_version,
        })
    }))?;

    Ok(json!({
        "normalized_event_ids": normalized_event_ids,
        "raw_fact_refs": raw_fact_refs,
        "manifest_versions": manifest_versions,
        "execution_trace_id": Value::Null,
        "derivation_kind": ADDRESS_NAMES_CURRENT_DERIVATION_KIND,
    }))
}
