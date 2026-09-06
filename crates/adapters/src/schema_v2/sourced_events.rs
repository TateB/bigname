use anyhow::Context;

use super::{
    catalog::{Catalog, Selected},
    model::{BatchOutput, RawLogInput},
    protocol::{Interpreted, SourcedEventBatch},
    state::State,
};

pub(super) fn prepare_v1_state_derived_events(
    selected: &Selected,
    raw: &RawLogInput,
    interpreted: &mut Interpreted,
    state: &mut State,
) -> anyhow::Result<()> {
    super::protocol::v1::materialize_wrapper_surface(selected, raw, state, interpreted)?;
    for event in interpreted.events.iter().chain(
        interpreted
            .sourced_events
            .iter()
            .flat_map(|batch| batch.events.iter()),
    ) {
        let Some((node, resolver, resource_id)) = (event.event_kind == "ResolverChanged")
            .then(|| {
                Some((
                    event.after_state["child_node"]
                        .as_str()
                        .or_else(|| event.after_state["node"].as_str())
                        .or_else(|| event.after_state["namehash"].as_str())?,
                    event.after_state.get("resolver")?.as_str()?,
                    event.resource_id?,
                ))
            })
            .flatten()
        else {
            continue;
        };
        state.remember_v1_resolver_linked_resource(
            &selected.source.namespace,
            node,
            resolver,
            resource_id,
            event.logical_name_id.clone(),
        );
    }
    Ok(())
}

pub(super) fn materialize(
    catalog: &Catalog,
    namespace: &str,
    raw: &RawLogInput,
    batches: Vec<SourcedEventBatch>,
    state: &mut State,
    output: &mut BatchOutput,
) -> anyhow::Result<()> {
    for batch in batches {
        let node = batch
            .events
            .first()
            .and_then(|event| event.after_state.get("node"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let source = catalog.provenance(batch.source_manifest_id).with_context(|| {
            format!(
                "state-derived source manifest is missing for namespace {namespace}, namehash {node}, manifest {}",
                batch.source_manifest_id
            )
        })?;
        super::normalized::materialize_for_provenance(source, raw, batch.events, state, output);
    }
    Ok(())
}
