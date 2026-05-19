use std::collections::BTreeSet;

use anyhow::{Context, Result};
use sqlx::PgPool;

use crate::{WatchedContract, normalize_address};

pub async fn load_watched_contracts_by_source_family_and_addresses(
    pool: &PgPool,
    source_family: &str,
    targets: &[(String, String)],
) -> Result<Vec<WatchedContract>> {
    load_watched_contracts_by_addresses_scoped(pool, targets, Some(source_family)).await
}

pub async fn load_watched_contracts_by_addresses(
    pool: &PgPool,
    targets: &[(String, String)],
) -> Result<Vec<WatchedContract>> {
    load_watched_contracts_by_addresses_scoped(pool, targets, None).await
}

async fn load_watched_contracts_by_addresses_scoped(
    pool: &PgPool,
    targets: &[(String, String)],
    source_family: Option<&str>,
) -> Result<Vec<WatchedContract>> {
    if targets.is_empty() {
        return Ok(Vec::new());
    }

    let targets = targets
        .iter()
        .map(|(chain, address)| (chain.clone(), normalize_address(address)))
        .collect::<BTreeSet<_>>();
    let chains = targets
        .iter()
        .map(|(chain, _)| chain.clone())
        .collect::<Vec<_>>();
    let addresses = targets
        .iter()
        .map(|(_, address)| address.clone())
        .collect::<Vec<_>>();

    let mut rows = sqlx::query(
        r#"
        WITH target_addresses AS (
            SELECT DISTINCT chain, address
            FROM UNNEST($1::TEXT[], $2::TEXT[]) AS target(chain, address)
        )
        SELECT
            target.chain AS chain,
            mv.source_family AS source_family,
            LOWER(cia.address) AS address,
            mci.contract_instance_id AS contract_instance_id,
            CASE
                WHEN mci.declaration_kind = 'root' THEN 'manifest_root'
                ELSE 'manifest_contract'
            END::TEXT AS source,
            mv.manifest_id AS source_manifest_id,
            CASE
                WHEN manifest_range.start_block IS NULL THEN cia.active_from_block_number
                WHEN cia.active_from_block_number IS NULL THEN manifest_range.start_block
                ELSE GREATEST(manifest_range.start_block, cia.active_from_block_number)
            END AS active_from_block_number,
            cia.active_to_block_number AS active_to_block_number
        FROM target_addresses target
        JOIN contract_instance_addresses cia
          ON cia.chain_id = target.chain
         AND LOWER(cia.address) = target.address
         AND cia.deactivated_at IS NULL
        JOIN manifest_contract_instances mci
          ON mci.contract_instance_id = cia.contract_instance_id
        JOIN manifest_versions mv
          ON mv.manifest_id = mci.manifest_id
         AND mv.chain = target.chain
        LEFT JOIN LATERAL (
            SELECT (entry ->> 'start_block')::BIGINT AS start_block
            FROM jsonb_array_elements(
                CASE
                    WHEN mci.declaration_kind = 'root' THEN mv.manifest_payload -> 'roots'
                    ELSE mv.manifest_payload -> 'contracts'
                END
            ) entry
            WHERE (
                    mci.declaration_kind = 'root'
                    AND entry ->> 'name' = mci.declaration_name
                )
               OR (
                    mci.declaration_kind = 'contract'
                    AND entry ->> 'role' = mci.declaration_name
                )
            ORDER BY start_block NULLS LAST
            LIMIT 1
        ) manifest_range ON TRUE
        WHERE mv.rollout_status = 'active'
          AND ($3::TEXT IS NULL OR mv.source_family = $3)
        ORDER BY 1, 2, 3, 5, 6, 4
        "#,
    )
    .bind(&chains)
    .bind(&addresses)
    .bind(source_family)
    .fetch_all(pool)
    .await
    .context("failed to load manifest-declared watched contracts for scoped addresses")?;

    let mut discovery_rows = sqlx::query(
        r#"
        WITH target_addresses AS (
            SELECT DISTINCT chain, address
            FROM UNNEST($1::TEXT[], $2::TEXT[]) AS target(chain, address)
        )
        SELECT
            de.chain_id AS chain,
            COALESCE(target_mv.source_family, mv.source_family) AS source_family,
            LOWER(cia.address) AS address,
            de.to_contract_instance_id AS contract_instance_id,
            'discovery_edge'::TEXT AS source,
            COALESCE(target_mv.manifest_id, de.source_manifest_id) AS source_manifest_id,
            CASE
                WHEN de.active_from_block_number IS NULL THEN cia.active_from_block_number
                WHEN cia.active_from_block_number IS NULL THEN de.active_from_block_number
                ELSE GREATEST(de.active_from_block_number, cia.active_from_block_number)
            END AS active_from_block_number,
            CASE
                WHEN de.active_to_block_number IS NULL THEN cia.active_to_block_number
                WHEN cia.active_to_block_number IS NULL THEN de.active_to_block_number
                ELSE LEAST(de.active_to_block_number, cia.active_to_block_number)
            END AS active_to_block_number
        FROM target_addresses target
        JOIN contract_instance_addresses cia
          ON cia.chain_id = target.chain
         AND LOWER(cia.address) = target.address
         AND cia.deactivated_at IS NULL
        JOIN discovery_edges de
          ON de.chain_id = target.chain
         AND de.to_contract_instance_id = cia.contract_instance_id
         AND de.deactivated_at IS NULL
         AND de.edge_kind <> 'migration'
        JOIN manifest_versions mv
          ON mv.manifest_id = de.source_manifest_id
         AND mv.rollout_status = 'active'
        LEFT JOIN manifest_versions target_mv
          ON target_mv.rollout_status = 'active'
         AND target_mv.namespace = mv.namespace
         AND target_mv.chain = de.chain_id
         AND target_mv.deployment_epoch = mv.deployment_epoch
         AND target_mv.source_family = CASE
             WHEN de.edge_kind = 'resolver' AND mv.source_family = 'ens_v1_registry_l1'
                 THEN 'ens_v1_resolver_l1'
             WHEN de.edge_kind = 'resolver' AND mv.source_family = 'ens_v2_registry_l1'
                 THEN 'ens_v2_resolver_l1'
             WHEN de.edge_kind = 'resolver' AND mv.source_family = 'basenames_base_registry'
                 THEN 'basenames_base_resolver'
             ELSE NULL
         END
        WHERE (
              $3::TEXT IS NULL
              OR COALESCE(target_mv.source_family, mv.source_family) = $3
          )
          AND (
              de.edge_kind <> 'resolver'
              OR mv.source_family NOT IN (
                  'ens_v1_registry_l1',
                  'ens_v2_registry_l1',
                  'basenames_base_registry'
              )
              OR target_mv.manifest_id IS NOT NULL
          )
          AND (
              de.active_from_block_number IS NULL
              OR cia.active_to_block_number IS NULL
              OR de.active_from_block_number <= cia.active_to_block_number
          )
          AND (
              cia.active_from_block_number IS NULL
              OR de.active_to_block_number IS NULL
              OR cia.active_from_block_number <= de.active_to_block_number
          )
        ORDER BY 1, 2, 3, 5, 6, 4
        "#,
    )
    .bind(&chains)
    .bind(&addresses)
    .bind(source_family)
    .fetch_all(pool)
    .await
    .context("failed to load discovery-edge watched contracts for scoped addresses")?;

    rows.append(&mut discovery_rows);
    let mut watched_contracts = super::watched_contracts_from_rows(rows)?;
    super::sort_and_dedup_watched_contracts(&mut watched_contracts);

    Ok(watched_contracts)
}
