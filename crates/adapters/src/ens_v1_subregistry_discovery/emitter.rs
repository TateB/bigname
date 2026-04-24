use std::collections::BTreeMap;

use anyhow::Result;
use bigname_storage::upsert_normalized_events;
use sqlx::PgPool;

use super::{
    assignment::ObservedRegistryAssignment, event::build_registry_changed_event,
    loader::load_active_registry_edges_by_observation_key,
};

pub(super) async fn emit_registry_changed_events(
    pool: &PgPool,
    latest_assignments: &BTreeMap<String, ObservedRegistryAssignment>,
    discovery_sources: &[String],
) -> Result<()> {
    let active_edges_by_observation_key =
        load_active_registry_edges_by_observation_key(pool, discovery_sources).await?;
    let events = latest_assignments
        .values()
        .filter_map(|assignment| {
            build_registry_changed_event(
                assignment,
                active_edges_by_observation_key.get(&(
                    assignment.observation.discovery_source.clone(),
                    assignment.observation_key.clone(),
                )),
            )
            .transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    upsert_normalized_events(pool, &events).await?;
    Ok(())
}
