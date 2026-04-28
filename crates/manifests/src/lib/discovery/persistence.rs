use anyhow::{Context, Result};
use sqlx::PgPool;

use super::loading::load_discovery_admission_state;
use super::provenance::{discovery_edge_provenance, is_zero_address};
use super::types::{DiscoveryObservation, DiscoveryPersistenceSummary};
use crate::{
    CONTRACT_KIND_CONTRACT, ensure_contract_instance_address_seed,
    reconcile_active_contract_instance_addresses, resolve_contract_instance_by_address,
};

pub async fn persist_discovery_observation(
    pool: &PgPool,
    observation: &DiscoveryObservation,
) -> Result<DiscoveryPersistenceSummary> {
    if is_zero_address(&observation.to_address) {
        return Ok(DiscoveryPersistenceSummary {
            admitted_edge_count: 0,
            inserted_edge_count: 0,
            admitted_edges: Vec::new(),
        });
    }

    let admission_state = load_discovery_admission_state(pool).await?;
    let admitted_candidates = admission_state.admit_candidate(&observation.candidate());
    let mut inserted_edge_count = 0;
    let mut admitted_edges = Vec::new();
    let mut transaction = pool
        .begin()
        .await
        .context("failed to start discovery-edge persistence transaction")?;

    for mut admitted_edge in admitted_candidates {
        let to_contract_instance_id = match admitted_edge.to_contract_instance_id {
            Some(contract_instance_id) => contract_instance_id,
            None => {
                resolve_contract_instance_by_address(
                    transaction.as_mut(),
                    &admitted_edge.chain,
                    &admitted_edge.to_address,
                    CONTRACT_KIND_CONTRACT,
                    &serde_json::json!({
                        "source": "discovery_observation",
                        "edge_kind": admitted_edge.edge_kind,
                        "discovery_source": admitted_edge.discovery_source,
                    }),
                )
                .await?
            }
        };
        admitted_edge.to_contract_instance_id = Some(to_contract_instance_id);
        ensure_contract_instance_address_seed(
            transaction.as_mut(),
            to_contract_instance_id,
            &admitted_edge.chain,
            &admitted_edge.to_address,
            Some(admitted_edge.source_manifest_id),
            &serde_json::json!({
                "source": "discovery_observation_seed",
                "edge_kind": admitted_edge.edge_kind,
                "discovery_source": admitted_edge.discovery_source,
            }),
        )
        .await?;

        let exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM discovery_edges
                WHERE chain_id = $1
                  AND edge_kind = $2
                  AND from_contract_instance_id = $3
                  AND to_contract_instance_id = $4
                  AND discovery_source = $5
                  AND source_manifest_id = $6
                  AND admission = $7
                  AND active_from_block_number IS NOT DISTINCT FROM $8
                  AND active_from_block_hash IS NOT DISTINCT FROM $9
                  AND active_to_block_number IS NOT DISTINCT FROM $10
                  AND active_to_block_hash IS NOT DISTINCT FROM $11
                  AND deactivated_at IS NULL
            )
            "#,
        )
        .bind(&admitted_edge.chain)
        .bind(&admitted_edge.edge_kind)
        .bind(admitted_edge.from_contract_instance_id)
        .bind(to_contract_instance_id)
        .bind(&admitted_edge.discovery_source)
        .bind(admitted_edge.source_manifest_id)
        .bind(&admitted_edge.admission)
        .bind(observation.active_from_block_number)
        .bind(observation.active_from_block_hash.as_deref())
        .bind(observation.active_to_block_number)
        .bind(observation.active_to_block_hash.as_deref())
        .fetch_one(transaction.as_mut())
        .await
        .context("failed to check for an existing discovery edge")?;

        if !exists {
            let provenance = serde_json::to_string(&discovery_edge_provenance(
                &observation.provenance,
                &admitted_edge.edge_kind,
                &admitted_edge.from_role,
            )?)
            .context("failed to serialize discovery-edge provenance")?;
            sqlx::query(
                r#"
                INSERT INTO discovery_edges (
                    chain_id,
                    edge_kind,
                    from_contract_instance_id,
                    to_contract_instance_id,
                    discovery_source,
                    source_manifest_id,
                    admission,
                    active_from_block_number,
                    active_from_block_hash,
                    active_to_block_number,
                    active_to_block_hash,
                    provenance
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb)
                "#,
            )
            .bind(&admitted_edge.chain)
            .bind(&admitted_edge.edge_kind)
            .bind(admitted_edge.from_contract_instance_id)
            .bind(to_contract_instance_id)
            .bind(&admitted_edge.discovery_source)
            .bind(admitted_edge.source_manifest_id)
            .bind(&admitted_edge.admission)
            .bind(observation.active_from_block_number)
            .bind(observation.active_from_block_hash.as_deref())
            .bind(observation.active_to_block_number)
            .bind(observation.active_to_block_hash.as_deref())
            .bind(provenance)
            .execute(transaction.as_mut())
            .await
            .context("failed to insert an admitted discovery edge")?;
            inserted_edge_count += 1;
        }

        admitted_edges.push(admitted_edge);
    }

    if inserted_edge_count > 0 {
        reconcile_active_contract_instance_addresses(transaction.as_mut()).await?;
    }

    transaction
        .commit()
        .await
        .context("failed to commit discovery-edge persistence transaction")?;

    Ok(DiscoveryPersistenceSummary {
        admitted_edge_count: admitted_edges.len(),
        inserted_edge_count,
        admitted_edges,
    })
}
