use anyhow::{Context, Result};
use sqlx::{Row, postgres::PgConnection, types::Uuid};

use super::super::types::{ExistingReconciledDiscoveryEdge, ReconciledDiscoveryEdgeSpec};
use crate::normalize_address;

pub(super) async fn load_active_reconciled_discovery_edges(
    executor: &mut PgConnection,
    discovery_source: &str,
) -> Result<Vec<ExistingReconciledDiscoveryEdge>> {
    let existing_rows = sqlx::query(
        r#"
        SELECT
            de.discovery_edge_id,
            de.provenance ->> 'observation_key' AS observation_key,
            de.chain_id,
            de.edge_kind,
            de.from_contract_instance_id,
            de.to_contract_instance_id,
            de.discovery_source,
            de.source_manifest_id,
            de.admission,
            de.active_from_block_number,
            de.active_from_block_hash,
            de.provenance,
            cia.address AS to_address
        FROM discovery_edges de
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = de.to_contract_instance_id
         AND cia.deactivated_at IS NULL
        WHERE de.discovery_source = $1
          AND de.deactivated_at IS NULL
        "#,
    )
    .bind(discovery_source)
    .fetch_all(executor)
    .await
    .with_context(|| {
        format!("failed to load active discovery edges for discovery_source {discovery_source}")
    })?;

    existing_rows.into_iter().map(edge_from_row).collect()
}

pub(super) async fn load_active_reconciled_discovery_edges_by_observation_keys(
    executor: &mut PgConnection,
    discovery_source: &str,
    observation_keys: &[String],
) -> Result<Vec<ExistingReconciledDiscoveryEdge>> {
    if observation_keys.is_empty() {
        return Ok(Vec::new());
    }

    let existing_rows = sqlx::query(
        r#"
        SELECT
            de.discovery_edge_id,
            de.provenance ->> 'observation_key' AS observation_key,
            de.chain_id,
            de.edge_kind,
            de.from_contract_instance_id,
            de.to_contract_instance_id,
            de.discovery_source,
            de.source_manifest_id,
            de.admission,
            de.active_from_block_number,
            de.active_from_block_hash,
            de.provenance,
            cia.address AS to_address
        FROM discovery_edges de
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = de.to_contract_instance_id
         AND cia.deactivated_at IS NULL
        WHERE de.discovery_source = $1
          AND de.provenance ->> 'observation_key' = ANY($2::TEXT[])
          AND de.deactivated_at IS NULL
        "#,
    )
    .bind(discovery_source)
    .bind(observation_keys)
    .fetch_all(executor)
    .await
    .with_context(|| {
        format!(
            "failed to load touched active discovery edges for discovery_source {discovery_source}"
        )
    })?;

    existing_rows.into_iter().map(edge_from_row).collect()
}

pub(super) async fn load_active_reconciled_discovery_descendant_edges(
    executor: &mut PgConnection,
    discovery_source: &str,
    chain: &str,
    parent_contract_instance_ids: &[Uuid],
) -> Result<Vec<ExistingReconciledDiscoveryEdge>> {
    if parent_contract_instance_ids.is_empty() {
        return Ok(Vec::new());
    }

    let existing_rows = sqlx::query(
        r#"
        WITH RECURSIVE descendant_edges AS (
            SELECT de.discovery_edge_id
            FROM discovery_edges de
            WHERE de.discovery_source = $1
              AND de.chain_id = $2
              AND de.deactivated_at IS NULL
              AND de.from_contract_instance_id = ANY($3::UUID[])
            UNION
            SELECT child.discovery_edge_id
            FROM discovery_edges child
            JOIN descendant_edges parent_ids ON true
            JOIN discovery_edges parent
              ON parent.discovery_edge_id = parent_ids.discovery_edge_id
            WHERE child.discovery_source = $1
              AND child.chain_id = $2
              AND child.deactivated_at IS NULL
              AND parent.edge_kind = 'subregistry'
              AND child.from_contract_instance_id = parent.to_contract_instance_id
        )
        SELECT
            de.discovery_edge_id,
            de.provenance ->> 'observation_key' AS observation_key,
            de.chain_id,
            de.edge_kind,
            de.from_contract_instance_id,
            de.to_contract_instance_id,
            de.discovery_source,
            de.source_manifest_id,
            de.admission,
            de.active_from_block_number,
            de.active_from_block_hash,
            de.provenance,
            cia.address AS to_address
        FROM discovery_edges de
        JOIN descendant_edges descendant
          ON descendant.discovery_edge_id = de.discovery_edge_id
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = de.to_contract_instance_id
         AND cia.deactivated_at IS NULL
        "#,
    )
    .bind(discovery_source)
    .bind(chain)
    .bind(parent_contract_instance_ids)
    .fetch_all(executor)
    .await
    .with_context(|| {
        format!(
            "failed to load descendant active discovery edges for discovery_source {discovery_source}"
        )
    })?;

    existing_rows.into_iter().map(edge_from_row).collect()
}

fn edge_from_row(row: sqlx::postgres::PgRow) -> Result<ExistingReconciledDiscoveryEdge> {
    let observation_key = row
        .try_get::<Option<String>, _>("observation_key")
        .context("failed to read observation_key")?
        .context("active reconciled discovery edge is missing provenance.observation_key")?;
    Ok(ExistingReconciledDiscoveryEdge {
        discovery_edge_id: row
            .try_get("discovery_edge_id")
            .context("failed to read discovery_edge_id")?,
        to_address: normalize_address(
            &row.try_get::<String, _>("to_address")
                .context("failed to read to_address")?,
        ),
        spec: ReconciledDiscoveryEdgeSpec {
            observation_key,
            chain: row.try_get("chain_id").context("failed to read chain_id")?,
            edge_kind: row
                .try_get("edge_kind")
                .context("failed to read edge_kind")?,
            from_contract_instance_id: row
                .try_get("from_contract_instance_id")
                .context("failed to read from_contract_instance_id")?,
            to_contract_instance_id: row
                .try_get("to_contract_instance_id")
                .context("failed to read to_contract_instance_id")?,
            discovery_source: row
                .try_get("discovery_source")
                .context("failed to read discovery_source")?,
            source_manifest_id: row
                .try_get::<Option<i64>, _>("source_manifest_id")
                .context("failed to read source_manifest_id")?
                .unwrap_or(-1),
            admission: row
                .try_get("admission")
                .context("failed to read admission")?,
            active_from_block_number: row
                .try_get("active_from_block_number")
                .context("failed to read active_from_block_number")?,
            active_from_block_hash: row
                .try_get("active_from_block_hash")
                .context("failed to read active_from_block_hash")?,
            provenance_json: row
                .try_get::<serde_json::Value, _>("provenance")
                .context("failed to read provenance")?
                .to_string(),
        },
    })
}
