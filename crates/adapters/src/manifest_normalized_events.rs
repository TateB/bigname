use std::collections::BTreeMap;

use anyhow::Result;
use bigname_manifests::load_manifest_drift_inputs;
use bigname_storage::upsert_normalized_events;
use sqlx::PgPool;

mod builders;
mod constants;
mod drift_alerts;
mod loading;
mod types;
mod utils;

use crate::normalized_event_support::count_events_by_kind;
use builders::build_normalized_events;
use loading::{load_active_capabilities, load_normalized_event_counts_by_kind};
pub use types::{ManifestNormalizedEventKindSyncSummary, ManifestNormalizedEventSyncSummary};
use utils::active_proxy_contracts_by_manifest;

/// Sync manifest-derived normalized events from stored active manifest state.
pub async fn sync_manifest_normalized_events(
    pool: &PgPool,
) -> Result<ManifestNormalizedEventSyncSummary> {
    let drift_inputs = load_manifest_drift_inputs(pool).await?;
    if drift_inputs.active_manifests.is_empty() {
        return Ok(ManifestNormalizedEventSyncSummary {
            total_synced_count: 0,
            total_inserted_count: 0,
            by_kind: BTreeMap::new(),
        });
    }

    let capabilities = load_active_capabilities(pool).await?;
    let contracts = active_proxy_contracts_by_manifest(&drift_inputs);
    let before_counts = load_normalized_event_counts_by_kind(pool).await?;
    let events = build_normalized_events(&drift_inputs, &capabilities, &contracts)?;

    if events.is_empty() {
        return Ok(ManifestNormalizedEventSyncSummary {
            total_synced_count: 0,
            total_inserted_count: 0,
            by_kind: BTreeMap::new(),
        });
    }

    let synced_by_kind = count_events_by_kind(&events);
    upsert_normalized_events(pool, &events).await?;
    let after_counts = load_normalized_event_counts_by_kind(pool).await?;

    let mut by_kind = BTreeMap::new();
    let mut total_inserted_count = 0;
    for (kind, synced_count) in synced_by_kind {
        let inserted_count = after_counts
            .get(&kind)
            .copied()
            .unwrap_or(0)
            .saturating_sub(before_counts.get(&kind).copied().unwrap_or(0));
        total_inserted_count += inserted_count;
        by_kind.insert(
            kind,
            ManifestNormalizedEventKindSyncSummary {
                synced_count,
                inserted_count,
            },
        );
    }

    Ok(ManifestNormalizedEventSyncSummary {
        total_synced_count: events.len(),
        total_inserted_count,
        by_kind,
    })
}

#[cfg(test)]
mod tests;
