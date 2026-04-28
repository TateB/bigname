use std::collections::BTreeSet;

use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

use crate::{WatchedContract, WatchedContractSource, normalize_address};

use super::super::types::ManifestCodeHashObservation;

pub async fn load_manifest_code_hash_observations(
    pool: &PgPool,
) -> Result<Vec<ManifestCodeHashObservation>> {
    let rows = sqlx::query(
        r#"
        WITH active_targets AS (
            SELECT
                mv.chain AS chain,
                mv.source_family AS source_family,
                mci.contract_instance_id AS contract_instance_id,
                cia.address AS address,
                CASE
                    WHEN mci.declaration_kind = 'root' THEN 'manifest_root'
                    ELSE 'manifest_contract'
                END::TEXT AS source,
                mv.manifest_id AS source_manifest_id
            FROM manifest_versions mv
            JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = mci.contract_instance_id
             AND cia.deactivated_at IS NULL
            WHERE mv.rollout_status = 'active'

            UNION

            SELECT
                de.chain_id AS chain,
                COALESCE(target_mv.source_family, mv.source_family) AS source_family,
                de.to_contract_instance_id AS contract_instance_id,
                cia.address AS address,
                'discovery_edge'::TEXT AS source,
                COALESCE(target_mv.manifest_id, de.source_manifest_id) AS source_manifest_id
            FROM discovery_edges de
            JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
            LEFT JOIN manifest_versions target_mv
              ON target_mv.rollout_status = 'active'
             AND target_mv.namespace = mv.namespace
             AND target_mv.chain = de.chain_id
             AND target_mv.deployment_epoch = mv.deployment_epoch
             AND target_mv.source_family = CASE
                 WHEN de.edge_kind = 'resolver' AND mv.source_family = 'ens_v1_registry_l1'
                     THEN 'ens_v1_resolver_l1'
                 WHEN de.edge_kind = 'resolver' AND mv.source_family = 'basenames_base_registry'
                     THEN 'basenames_base_resolver'
                 ELSE NULL
             END
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = de.to_contract_instance_id
             AND cia.deactivated_at IS NULL
            WHERE mv.rollout_status = 'active'
              AND de.deactivated_at IS NULL
              AND de.edge_kind <> 'migration'
              AND (
                  de.edge_kind <> 'resolver'
                  OR mv.source_family NOT IN ('ens_v1_registry_l1', 'basenames_base_registry')
                  OR target_mv.manifest_id IS NOT NULL
              )
        )
        SELECT
            active_targets.chain,
            active_targets.source_family,
            active_targets.contract_instance_id,
            active_targets.address,
            active_targets.source,
            active_targets.source_manifest_id,
            raw_code_hashes.block_hash,
            raw_code_hashes.block_number,
            raw_code_hashes.code_hash,
            raw_code_hashes.code_byte_length,
            raw_code_hashes.canonicality_state::TEXT AS canonicality_state
        FROM active_targets
        JOIN LATERAL (
            SELECT
                block_hash,
                block_number,
                code_hash,
                code_byte_length,
                canonicality_state
            FROM raw_code_hashes
            WHERE raw_code_hashes.chain_id = active_targets.chain
              AND raw_code_hashes.contract_address = active_targets.address
              AND raw_code_hashes.canonicality_state <> 'orphaned'
            ORDER BY
                raw_code_hashes.block_number DESC,
                CASE raw_code_hashes.canonicality_state
                    WHEN 'finalized' THEN 4
                    WHEN 'safe' THEN 3
                    WHEN 'canonical' THEN 2
                    WHEN 'observed' THEN 1
                    ELSE 0
                END DESC,
                raw_code_hashes.raw_code_hash_id DESC
            LIMIT 1
        ) raw_code_hashes ON TRUE
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load manifest code-hash observations")?;

    rows.into_iter()
        .map(decode_manifest_code_hash_observation)
        .collect()
}

pub async fn load_manifest_code_hash_observations_for_watched_contracts(
    pool: &PgPool,
    watched_contracts: &[WatchedContract],
) -> Result<Vec<ManifestCodeHashObservation>> {
    if watched_contracts.is_empty() {
        return Ok(Vec::new());
    }

    let targets = watched_contracts
        .iter()
        .map(|contract| {
            (
                contract.chain.clone(),
                contract.source_family.clone(),
                contract.contract_instance_id,
                normalize_address(&contract.address),
                watched_contract_source_to_db_value(contract.source).to_owned(),
                contract.source_manifest_id.unwrap_or_default(),
            )
        })
        .collect::<BTreeSet<_>>();
    let chains = targets
        .iter()
        .map(|(chain, _, _, _, _, _)| chain.clone())
        .collect::<Vec<_>>();
    let source_families = targets
        .iter()
        .map(|(_, source_family, _, _, _, _)| source_family.clone())
        .collect::<Vec<_>>();
    let contract_instance_ids = targets
        .iter()
        .map(|(_, _, contract_instance_id, _, _, _)| *contract_instance_id)
        .collect::<Vec<_>>();
    let addresses = targets
        .iter()
        .map(|(_, _, _, address, _, _)| address.clone())
        .collect::<Vec<_>>();
    let sources = targets
        .iter()
        .map(|(_, _, _, _, source, _)| source.clone())
        .collect::<Vec<_>>();
    let source_manifest_ids = targets
        .iter()
        .map(|(_, _, _, _, _, source_manifest_id)| *source_manifest_id)
        .collect::<Vec<_>>();

    let rows = sqlx::query(
        r#"
        WITH active_targets AS (
            SELECT DISTINCT
                chain,
                source_family,
                contract_instance_id,
                address,
                source,
                NULLIF(source_manifest_id, 0) AS source_manifest_id
            FROM UNNEST(
                $1::TEXT[],
                $2::TEXT[],
                $3::UUID[],
                $4::TEXT[],
                $5::TEXT[],
                $6::BIGINT[]
            ) AS target(
                chain,
                source_family,
                contract_instance_id,
                address,
                source,
                source_manifest_id
            )
        )
        SELECT
            active_targets.chain,
            active_targets.source_family,
            active_targets.contract_instance_id,
            active_targets.address,
            active_targets.source,
            active_targets.source_manifest_id,
            raw_code_hashes.block_hash,
            raw_code_hashes.block_number,
            raw_code_hashes.code_hash,
            raw_code_hashes.code_byte_length,
            raw_code_hashes.canonicality_state::TEXT AS canonicality_state
        FROM active_targets
        JOIN LATERAL (
            SELECT
                block_hash,
                block_number,
                code_hash,
                code_byte_length,
                canonicality_state
            FROM raw_code_hashes
            WHERE raw_code_hashes.chain_id = active_targets.chain
              AND raw_code_hashes.contract_address = active_targets.address
              AND raw_code_hashes.canonicality_state <> 'orphaned'
            ORDER BY
                raw_code_hashes.block_number DESC,
                CASE raw_code_hashes.canonicality_state
                    WHEN 'finalized' THEN 4
                    WHEN 'safe' THEN 3
                    WHEN 'canonical' THEN 2
                    WHEN 'observed' THEN 1
                    ELSE 0
                END DESC,
                raw_code_hashes.raw_code_hash_id DESC
            LIMIT 1
        ) raw_code_hashes ON TRUE
        "#,
    )
    .bind(&chains)
    .bind(&source_families)
    .bind(&contract_instance_ids)
    .bind(&addresses)
    .bind(&sources)
    .bind(&source_manifest_ids)
    .fetch_all(pool)
    .await
    .context("failed to load scoped manifest code-hash observations")?;

    rows.into_iter()
        .map(decode_manifest_code_hash_observation)
        .collect()
}

fn watched_contract_source_to_db_value(source: WatchedContractSource) -> &'static str {
    match source {
        WatchedContractSource::ManifestRoot => "manifest_root",
        WatchedContractSource::ManifestContract => "manifest_contract",
        WatchedContractSource::DiscoveryEdge => "discovery_edge",
    }
}

fn decode_manifest_code_hash_observation(
    row: sqlx::postgres::PgRow,
) -> Result<ManifestCodeHashObservation> {
    let source = row
        .try_get::<String, _>("source")
        .context("failed to read code-hash source")?;
    let address = row
        .try_get::<String, _>("address")
        .context("failed to read code-hash address")?;
    Ok(ManifestCodeHashObservation {
        chain: row.try_get("chain").context("failed to read chain")?,
        source_family: row
            .try_get("source_family")
            .context("failed to read code-hash source_family")?,
        contract_instance_id: row
            .try_get("contract_instance_id")
            .context("failed to read code-hash contract_instance_id")?,
        address: normalize_address(&address),
        source: WatchedContractSource::from_db_value(&source)?,
        source_manifest_id: row
            .try_get("source_manifest_id")
            .context("failed to read code-hash source_manifest_id")?,
        block_hash: row
            .try_get("block_hash")
            .context("failed to read code-hash block_hash")?,
        block_number: row
            .try_get("block_number")
            .context("failed to read code-hash block_number")?,
        code_hash: row
            .try_get("code_hash")
            .context("failed to read code_hash")?,
        code_byte_length: row
            .try_get("code_byte_length")
            .context("failed to read code_byte_length")?,
        canonicality_state: row
            .try_get("canonicality_state")
            .context("failed to read code-hash canonicality_state")?,
    })
}
