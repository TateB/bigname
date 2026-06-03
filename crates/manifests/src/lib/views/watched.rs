#[path = "watched/scoped.rs"]
mod scoped;
#[path = "watched/selection.rs"]
mod selection;

use anyhow::{Context, Result};
use sqlx::{PgPool, Row, postgres::PgRow};

use crate::{WatchedContract, WatchedContractSource, normalize_address};

pub use scoped::{
    load_watched_contracts_by_addresses, load_watched_contracts_by_source_family_and_addresses,
};
pub use selection::*;

pub async fn load_watched_contracts(pool: &PgPool) -> Result<Vec<WatchedContract>> {
    let rows = sqlx::query(
        r#"
        SELECT
            chain,
            source_family,
            address,
            contract_instance_id,
            source,
            source_manifest_id,
            active_from_block_number,
            active_to_block_number
        FROM (
            SELECT
                mv.chain AS chain,
                mv.source_family AS source_family,
                cia.address AS address,
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
            FROM manifest_versions mv
            JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
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
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = mci.contract_instance_id
             AND cia.deactivated_at IS NULL
            WHERE mv.rollout_status = 'active'

            UNION

            SELECT
                de.chain_id AS chain,
                COALESCE(target_mv.source_family, mv.source_family) AS source_family,
                cia.address AS address,
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
                 WHEN de.edge_kind = 'resolver' AND mv.source_family = 'ens_v2_registry_l1'
                     THEN 'ens_v2_resolver_l1'
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
        ) watched_contracts
        ORDER BY 1, 2, 3, 5, 6, 4
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load watched contracts")?;

    watched_contracts_from_rows(rows)
}

pub async fn load_watched_contracts_by_chain(
    pool: &PgPool,
    chain: &str,
) -> Result<Vec<WatchedContract>> {
    let rows = sqlx::query(
        r#"
        SELECT
            chain,
            source_family,
            address,
            contract_instance_id,
            source,
            source_manifest_id,
            active_from_block_number,
            active_to_block_number
        FROM (
            SELECT
                mv.chain AS chain,
                mv.source_family AS source_family,
                cia.address AS address,
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
            FROM manifest_versions mv
            JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
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
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = mci.contract_instance_id
             AND cia.deactivated_at IS NULL
            WHERE mv.rollout_status = 'active'
              AND mv.chain = $1

            UNION

            SELECT
                de.chain_id AS chain,
                COALESCE(target_mv.source_family, mv.source_family) AS source_family,
                cia.address AS address,
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
                 WHEN de.edge_kind = 'resolver' AND mv.source_family = 'ens_v2_registry_l1'
                     THEN 'ens_v2_resolver_l1'
                 WHEN de.edge_kind = 'resolver' AND mv.source_family = 'basenames_base_registry'
                     THEN 'basenames_base_resolver'
                 ELSE NULL
             END
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = de.to_contract_instance_id
             AND cia.deactivated_at IS NULL
            WHERE mv.rollout_status = 'active'
              AND de.chain_id = $1
              AND de.deactivated_at IS NULL
              AND de.edge_kind <> 'migration'
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
        ) watched_contracts
        ORDER BY 1, 2, 3, 5, 6, 4
        "#,
    )
    .bind(chain)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load watched contracts for chain {chain}"))?;

    watched_contracts_from_rows(rows)
}

pub async fn load_watched_contracts_by_source_family(
    pool: &PgPool,
    source_family: &str,
) -> Result<Vec<WatchedContract>> {
    let rows = sqlx::query(
        r#"
        SELECT
            chain,
            source_family,
            address,
            contract_instance_id,
            source,
            source_manifest_id,
            active_from_block_number,
            active_to_block_number
        FROM (
            SELECT
                mv.chain AS chain,
                mv.source_family AS source_family,
                cia.address AS address,
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
            FROM manifest_versions mv
            JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
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
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = mci.contract_instance_id
             AND cia.deactivated_at IS NULL
            WHERE mv.rollout_status = 'active'
              AND mv.source_family = $1

            UNION

            SELECT
                de.chain_id AS chain,
                COALESCE(target_mv.source_family, mv.source_family) AS source_family,
                cia.address AS address,
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
                 WHEN de.edge_kind = 'resolver' AND mv.source_family = 'ens_v2_registry_l1'
                     THEN 'ens_v2_resolver_l1'
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
              AND COALESCE(target_mv.source_family, mv.source_family) = $1
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
        ) watched_contracts
        ORDER BY 1, 2, 3, 5, 6, 4
        "#,
    )
    .bind(source_family)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load watched contracts for source family {source_family}")
    })?;

    watched_contracts_from_rows(rows)
}

pub async fn load_manifest_declared_watched_contracts(
    pool: &PgPool,
) -> Result<Vec<WatchedContract>> {
    let rows = sqlx::query(
        r#"
        SELECT
            mv.chain AS chain,
            mv.source_family AS source_family,
            cia.address AS address,
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
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
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
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = mci.contract_instance_id
         AND cia.deactivated_at IS NULL
        WHERE mv.rollout_status = 'active'
        ORDER BY chain, source_family, address, source, source_manifest_id, contract_instance_id
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load manifest-declared watched contracts")?;

    watched_contracts_from_rows(rows)
}

fn watched_contracts_from_rows(rows: Vec<PgRow>) -> Result<Vec<WatchedContract>> {
    rows.into_iter()
        .map(|row| {
            let source = row
                .try_get::<String, _>("source")
                .context("failed to read watched contract source")?;
            Ok(WatchedContract {
                chain: row
                    .try_get("chain")
                    .context("failed to read watched contract chain")?,
                source_family: row
                    .try_get("source_family")
                    .context("failed to read watched contract source_family")?,
                address: normalize_address(
                    &row.try_get::<String, _>("address")
                        .context("failed to read watched contract address")?,
                ),
                contract_instance_id: row
                    .try_get("contract_instance_id")
                    .context("failed to read watched contract_instance_id")?,
                source: WatchedContractSource::from_db_value(&source)?,
                source_manifest_id: row
                    .try_get("source_manifest_id")
                    .context("failed to read watched contract source_manifest_id")?,
                active_from_block_number: row
                    .try_get("active_from_block_number")
                    .context("failed to read watched contract active_from_block_number")?,
                active_to_block_number: row
                    .try_get("active_to_block_number")
                    .context("failed to read watched contract active_to_block_number")?,
            })
        })
        .collect()
}

fn sort_and_dedup_watched_contracts(watched_contracts: &mut Vec<WatchedContract>) {
    watched_contracts.sort_by(|left, right| {
        (
            left.chain.as_str(),
            left.source_family.as_str(),
            left.address.as_str(),
            left.source,
            left.source_manifest_id,
            left.contract_instance_id,
            left.active_from_block_number,
            left.active_to_block_number,
        )
            .cmp(&(
                right.chain.as_str(),
                right.source_family.as_str(),
                right.address.as_str(),
                right.source,
                right.source_manifest_id,
                right.contract_instance_id,
                right.active_from_block_number,
                right.active_to_block_number,
            ))
    });
    watched_contracts.dedup_by(|left, right| {
        left.chain == right.chain
            && left.source_family == right.source_family
            && left.address == right.address
            && left.source == right.source
            && left.source_manifest_id == right.source_manifest_id
            && left.contract_instance_id == right.contract_instance_id
            && left.active_from_block_number == right.active_from_block_number
            && left.active_to_block_number == right.active_to_block_number
    });
}
