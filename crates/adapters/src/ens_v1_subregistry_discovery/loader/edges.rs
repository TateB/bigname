use std::collections::HashMap;

use super::ActiveRegistryEdge;
use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

pub(in crate::ens_v1_subregistry_discovery) async fn load_registry_edges_by_observation_point(
    pool: &PgPool,
    requested_edges: &[(String, String, i64, String)],
) -> Result<HashMap<(String, String, i64, String), ActiveRegistryEdge>> {
    if requested_edges.is_empty() {
        return Ok(HashMap::new());
    }

    let discovery_sources = requested_edges
        .iter()
        .map(|(discovery_source, _, _, _)| discovery_source.clone())
        .collect::<Vec<_>>();
    let observation_keys = requested_edges
        .iter()
        .map(|(_, observation_key, _, _)| observation_key.clone())
        .collect::<Vec<_>>();
    let active_from_block_numbers = requested_edges
        .iter()
        .map(|(_, _, active_from_block_number, _)| *active_from_block_number)
        .collect::<Vec<_>>();
    let active_from_block_hashes = requested_edges
        .iter()
        .map(|(_, _, _, active_from_block_hash)| active_from_block_hash.clone())
        .collect::<Vec<_>>();

    let rows = sqlx::query(
        r#"
        SELECT
            discovery_edges.provenance ->> 'observation_key' AS observation_key,
            discovery_edges.discovery_source,
            discovery_edges.active_from_block_number,
            discovery_edges.active_from_block_hash,
            discovery_edges.from_contract_instance_id,
            discovery_edges.to_contract_instance_id
        FROM discovery_edges
        JOIN unnest(
            $1::TEXT[],
            $2::TEXT[],
            $3::BIGINT[],
            $4::TEXT[]
        ) AS requested(
            discovery_source,
            observation_key,
            active_from_block_number,
            active_from_block_hash
        )
         ON requested.discovery_source = discovery_edges.discovery_source
         AND requested.observation_key = discovery_edges.provenance ->> 'observation_key'
         AND requested.active_from_block_number = discovery_edges.active_from_block_number
         AND requested.active_from_block_hash = discovery_edges.active_from_block_hash
        AND discovery_edges.edge_kind IN ('subregistry', 'resolver')
        "#,
    )
    .bind(&discovery_sources)
    .bind(&observation_keys)
    .bind(&active_from_block_numbers)
    .bind(&active_from_block_hashes)
    .fetch_all(pool)
    .await
    .context("failed to load ENSv1 registry discovery edges for normalized-event emission")?;

    rows.into_iter()
        .map(|row| {
            let edge = ActiveRegistryEdge {
                observation_key: row
                    .try_get::<Option<String>, _>("observation_key")
                    .context("failed to read observation_key")?
                    .context("active ENSv1 registry edge is missing provenance.observation_key")?,
                discovery_source: row
                    .try_get("discovery_source")
                    .context("failed to read discovery_source")?,
                from_contract_instance_id: row
                    .try_get("from_contract_instance_id")
                    .context("failed to read from_contract_instance_id")?,
                to_contract_instance_id: row
                    .try_get("to_contract_instance_id")
                    .context("failed to read to_contract_instance_id")?,
            };
            Ok((
                (
                    edge.discovery_source.clone(),
                    edge.observation_key.clone(),
                    row.try_get("active_from_block_number")
                        .context("failed to read active_from_block_number")?,
                    row.try_get("active_from_block_hash")
                        .context("failed to read active_from_block_hash")?,
                ),
                edge,
            ))
        })
        .collect()
}
