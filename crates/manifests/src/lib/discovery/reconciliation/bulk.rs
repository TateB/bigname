use anyhow::{Context, Result};
use sqlx::{Postgres, QueryBuilder, types::Uuid};

use crate::CONTRACT_KIND_CONTRACT;

use super::super::types::ReconciledDiscoveryEdgeSpec;

const CONTRACT_INSTANCE_SEED_BATCH_SIZE: usize = 1000;
const DISCOVERY_EDGE_INSERT_BATCH_SIZE: usize = 1000;

#[derive(Clone, Debug)]
pub(super) struct PendingContractInstanceSeed {
    pub(super) contract_instance_id: Uuid,
    pub(super) chain: String,
    pub(super) address: String,
    pub(super) source_manifest_id: i64,
    pub(super) instance_provenance_json: serde_json::Value,
    pub(super) address_provenance_json: serde_json::Value,
}

pub(super) async fn insert_pending_contract_instance_seeds(
    executor: &mut sqlx::postgres::PgConnection,
    seeds: &[PendingContractInstanceSeed],
) -> Result<()> {
    if seeds.is_empty() {
        return Ok(());
    }

    for chunk in seeds.chunks(CONTRACT_INSTANCE_SEED_BATCH_SIZE) {
        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO contract_instances (
                contract_instance_id,
                chain_id,
                contract_kind,
                provenance
            )
            "#,
        );
        builder.push_values(chunk, |mut row, seed| {
            row.push_bind(seed.contract_instance_id)
                .push_bind(&seed.chain)
                .push_bind(CONTRACT_KIND_CONTRACT)
                .push_bind(&seed.instance_provenance_json);
        });
        builder
            .build()
            .execute(&mut *executor)
            .await
            .context("failed to bulk insert discovered contract instances")?;
    }

    for chunk in seeds.chunks(CONTRACT_INSTANCE_SEED_BATCH_SIZE) {
        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO contract_instance_addresses (
                contract_instance_id,
                chain_id,
                address,
                source_manifest_id,
                provenance
            )
            "#,
        );
        builder.push_values(chunk, |mut row, seed| {
            row.push_bind(seed.contract_instance_id)
                .push_bind(&seed.chain)
                .push_bind(&seed.address)
                .push_bind(seed.source_manifest_id)
                .push_bind(&seed.address_provenance_json);
        });
        builder.push(
            r#"
            ON CONFLICT (contract_instance_id)
            WHERE deactivated_at IS NULL
            DO NOTHING
            "#,
        );
        builder
            .build()
            .execute(&mut *executor)
            .await
            .context("failed to bulk seed discovered contract-instance addresses")?;
    }

    Ok(())
}

pub(super) async fn insert_reconciled_discovery_edges(
    executor: &mut sqlx::postgres::PgConnection,
    edges: &[&ReconciledDiscoveryEdgeSpec],
) -> Result<usize> {
    if edges.is_empty() {
        return Ok(0);
    }

    for chunk in edges.chunks(DISCOVERY_EDGE_INSERT_BATCH_SIZE) {
        let provenance_values = chunk
            .iter()
            .map(|edge| {
                serde_json::from_str::<serde_json::Value>(&edge.provenance_json).with_context(
                    || {
                        format!(
                            "failed to parse reconciled discovery-edge provenance for {} {} -> {}",
                            edge.edge_kind,
                            edge.from_contract_instance_id,
                            edge.to_contract_instance_id
                        )
                    },
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let mut builder = QueryBuilder::<Postgres>::new(
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
                provenance
            )
            "#,
        );
        builder.push_values(
            chunk.iter().zip(provenance_values.iter()),
            |mut row, (edge, provenance)| {
                row.push_bind(&edge.chain)
                    .push_bind(&edge.edge_kind)
                    .push_bind(edge.from_contract_instance_id)
                    .push_bind(edge.to_contract_instance_id)
                    .push_bind(&edge.discovery_source)
                    .push_bind(edge.source_manifest_id)
                    .push_bind(&edge.admission)
                    .push_bind(edge.active_from_block_number)
                    .push_bind(edge.active_from_block_hash.as_deref())
                    .push_bind(provenance);
            },
        );
        builder
            .build()
            .execute(&mut *executor)
            .await
            .context("failed to bulk insert reconciled discovery edges")?;
    }

    Ok(edges.len())
}
