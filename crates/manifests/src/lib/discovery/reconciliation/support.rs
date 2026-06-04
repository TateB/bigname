use std::collections::HashMap;

use anyhow::{Context, Result};

use crate::discovery::{
    provenance::observation_key,
    types::{DiscoveryObservation, ObservationTerminalState},
};

pub(super) async fn lock_discovery_reconciliation(
    executor: &mut sqlx::postgres::PgConnection,
    discovery_source: &str,
) -> Result<()> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(discovery_source)
        .execute(executor)
        .await
        .with_context(|| {
            format!("failed to acquire discovery reconciliation lock for {discovery_source}")
        })?;

    Ok(())
}

pub(super) fn observation_terminal_states(
    observations: &[DiscoveryObservation],
) -> Result<HashMap<String, ObservationTerminalState>> {
    observations
        .iter()
        .map(|observation| {
            Ok((
                observation_key(observation)?,
                ObservationTerminalState {
                    chain: observation.chain.clone(),
                    block_number: observation.active_from_block_number,
                    block_hash: observation.active_from_block_hash.clone(),
                },
            ))
        })
        .collect()
}
