use anyhow::Result;
use bigname_manifests::reconcile_discovery_observations;
use sqlx::PgPool;

use super::{
    EnsV1SubregistryDiscoverySyncSummary, SUBREGISTRY_CHECKPOINT_RECONCILIATION_PAGE_LIMIT,
    checkpoint::SubregistryReplayCheckpoint,
};

pub(super) async fn reconcile_subregistry_discovery_from_checkpoint(
    pool: &PgPool,
    checkpoint: &SubregistryReplayCheckpoint,
    discovery_sources: &[String],
    reconciliation: &mut EnsV1SubregistryDiscoverySyncSummary,
) -> Result<()> {
    for discovery_source in discovery_sources {
        let mut source_observations = Vec::new();
        let mut after_key = None::<String>;
        loop {
            let page = checkpoint
                .load_assignment_page(
                    pool,
                    discovery_source,
                    after_key.as_deref(),
                    SUBREGISTRY_CHECKPOINT_RECONCILIATION_PAGE_LIMIT,
                )
                .await?;
            let Some((last_key, _)) = page.last() else {
                break;
            };
            after_key = Some(last_key.clone());
            source_observations.extend(
                page.iter()
                    .map(|(_, assignment)| assignment.discovery_observation())
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        let source_reconciliation =
            reconcile_discovery_observations(pool, discovery_source, &source_observations).await?;
        reconciliation.active_edge_count += source_reconciliation.active_edge_count;
        reconciliation.admitted_edge_count += source_reconciliation.admitted_edge_count;
        reconciliation.inserted_edge_count += source_reconciliation.inserted_edge_count;
        reconciliation.deactivated_edge_count += source_reconciliation.deactivated_edge_count;
    }
    Ok(())
}
