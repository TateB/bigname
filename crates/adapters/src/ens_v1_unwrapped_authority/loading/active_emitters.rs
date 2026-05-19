use bigname_storage::sql_row;
use std::collections::{HashMap, HashSet};

use super::super::scope::{
    AuthorityRawLogSourceScopeTarget, is_generic_resolver_event_source_scope_target,
};
use super::super::*;
use anyhow::{Context, Result};
use bigname_manifests::{
    WatchedContract, WatchedContractSource, load_manifest_declared_watched_contracts,
    load_watched_contracts,
};
use sqlx::{PgPool, types::Uuid};

use crate::adapter_manifest::{required_source_manifest_id, watched_contract_manifest_ids};

pub(in crate::ens_v1_unwrapped_authority) async fn load_active_emitters(
    pool: &PgPool,
    chain: &str,
    source_scope: Option<&[AuthorityRawLogSourceScopeTarget]>,
) -> Result<Vec<ActiveEmitter>> {
    let watched_contracts = load_scoped_watched_contracts(pool, chain, source_scope).await?;
    if watched_contracts.is_empty() {
        return Ok(Vec::new());
    }
    let contract_roles = load_manifest_contract_roles(pool, &watched_contracts).await?;

    let manifest_ids = watched_contract_manifest_ids(&watched_contracts)?;
    let active_manifests = load_active_manifest_metadata(pool, &manifest_ids).await?;
    let mut emitters = Vec::new();
    for watched_contract in watched_contracts {
        let source_manifest_id = required_source_manifest_id(&watched_contract)?;
        let Some(manifest) = active_manifests.get(&source_manifest_id) else {
            continue;
        };
        if manifest.source_family != SOURCE_FAMILY_ENS_V1_REGISTRAR_L1
            && manifest.source_family != SOURCE_FAMILY_ENS_V1_REGISTRY_L1
            && manifest.source_family != SOURCE_FAMILY_ENS_V1_RESOLVER_L1
            && manifest.source_family != SOURCE_FAMILY_ENS_V1_WRAPPER_L1
            && manifest.source_family != SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR
            && manifest.source_family != SOURCE_FAMILY_BASENAMES_BASE_REGISTRY
            && manifest.source_family != SOURCE_FAMILY_BASENAMES_BASE_RESOLVER
        {
            continue;
        }

        let candidate = ActiveEmitter {
            address: watched_contract.address.to_ascii_lowercase(),
            contract_instance_id: watched_contract.contract_instance_id,
            source_manifest_id,
            namespace: manifest.namespace.clone(),
            source_family: manifest.source_family.clone(),
            manifest_version: manifest.manifest_version,
            normalizer_version: manifest.normalizer_version.clone(),
            contract_role: contract_roles
                .get(&(source_manifest_id, watched_contract.contract_instance_id))
                .cloned(),
            active_from_block_number: watched_contract.active_from_block_number,
            active_to_block_number: watched_contract.active_to_block_number,
            source_rank: source_rank(watched_contract.source),
        };

        emitters.push(candidate);
    }

    emitters.sort_by(|left, right| {
        left.address
            .cmp(&right.address)
            .then(left.source_rank.cmp(&right.source_rank))
            .then(left.source_manifest_id.cmp(&right.source_manifest_id))
    });
    Ok(emitters)
}

pub(in crate::ens_v1_unwrapped_authority) async fn load_generic_resolver_event_sources(
    pool: &PgPool,
    chain: &str,
    source_scope: Option<&[AuthorityRawLogSourceScopeTarget]>,
) -> Result<Vec<GenericResolverEventSource>> {
    let scope_ranges = match source_scope {
        Some(source_scope) => {
            let ranges = source_scope
                .iter()
                .filter(|target| is_generic_resolver_event_source_scope_target(target))
                .map(|target| {
                    (
                        Some(target.effective_from_block),
                        Some(target.effective_to_block),
                    )
                })
                .collect::<Vec<_>>();
            if ranges.is_empty() {
                return Ok(Vec::new());
            }
            ranges
        }
        None => vec![(None, None)],
    };

    let manifests = load_active_manifest_metadata_for_source_family(
        pool,
        chain,
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
    )
    .await?;
    let mut sources = Vec::new();
    for manifest in manifests {
        for (effective_from_block, effective_to_block) in &scope_ranges {
            sources.push(GenericResolverEventSource {
                source_manifest_id: manifest.manifest_id,
                namespace: manifest.namespace.clone(),
                source_family: manifest.source_family.clone(),
                manifest_version: manifest.manifest_version,
                normalizer_version: manifest.normalizer_version.clone(),
                effective_from_block: *effective_from_block,
                effective_to_block: *effective_to_block,
            });
        }
    }
    sources.sort_by(|left, right| {
        left.effective_from_block
            .cmp(&right.effective_from_block)
            .then(left.effective_to_block.cmp(&right.effective_to_block))
            .then(left.source_manifest_id.cmp(&right.source_manifest_id))
    });
    Ok(sources)
}

async fn load_scoped_watched_contracts(
    pool: &PgPool,
    chain: &str,
    source_scope: Option<&[AuthorityRawLogSourceScopeTarget]>,
) -> Result<Vec<WatchedContract>> {
    let Some(source_scope) = source_scope else {
        return load_watched_contracts(pool)
            .await
            .context("failed to load watched contracts for ENSv1 unwrapped authority attribution")
            .map(|contracts| {
                contracts
                    .into_iter()
                    .filter(|contract| contract.chain == chain)
                    .collect()
            });
    };

    let manifest_declared = load_manifest_declared_watched_contracts(pool)
        .await
        .context(
            "failed to load manifest-declared watched contracts for scoped ENSv1 unwrapped authority attribution",
        )?;
    let manifest_declared =
        filter_watched_contracts_by_scope(manifest_declared, chain, source_scope);
    if source_scope_covered_by_watched_contracts(source_scope, &manifest_declared) {
        return Ok(manifest_declared);
    }

    let mut watched_contracts = manifest_declared;
    watched_contracts.extend(
        load_scoped_discovery_watched_contracts(pool, chain, source_scope)
            .await
            .context(
                "failed to load scoped discovery watched contracts for ENSv1 unwrapped authority attribution",
            )?,
    );
    watched_contracts.sort_by(|left, right| {
        left.chain
            .cmp(&right.chain)
            .then(left.source_family.cmp(&right.source_family))
            .then(left.address.cmp(&right.address))
            .then(source_rank(left.source).cmp(&source_rank(right.source)))
            .then(left.source_manifest_id.cmp(&right.source_manifest_id))
            .then(left.contract_instance_id.cmp(&right.contract_instance_id))
    });
    Ok(watched_contracts)
}

async fn load_scoped_discovery_watched_contracts(
    pool: &PgPool,
    chain: &str,
    source_scope: &[AuthorityRawLogSourceScopeTarget],
) -> Result<Vec<WatchedContract>> {
    let scoped_targets = source_scope
        .iter()
        .filter(|target| is_unwrapped_authority_source_family(&target.source_family))
        .filter(|target| !is_generic_resolver_event_source_scope_target(target))
        .collect::<Vec<_>>();
    if scoped_targets.is_empty() {
        return Ok(Vec::new());
    }

    let scoped_source_families = scoped_targets
        .iter()
        .map(|target| target.source_family.clone())
        .collect::<Vec<_>>();
    let scoped_addresses = scoped_targets
        .iter()
        .map(|target| target.address.clone())
        .collect::<Vec<_>>();
    let scoped_from_blocks = scoped_targets
        .iter()
        .map(|target| target.effective_from_block)
        .collect::<Vec<_>>();
    let scoped_to_blocks = scoped_targets
        .iter()
        .map(|target| target.effective_to_block)
        .collect::<Vec<_>>();

    let exact_block_scope = scoped_targets
        .iter()
        .all(|target| target.effective_from_block == target.effective_to_block);

    // Live and block-hash replay scopes are exact block probes. For those, the replay only needs
    // to prove that a discovery edge admits the target at that block; loading every historical
    // edge for the same target repeatedly burns memory and query time.
    let rows = if exact_block_scope {
        sqlx::query(
            r#"
            WITH scoped_targets AS (
                SELECT DISTINCT
                    source_family,
                    address,
                    effective_from_block,
                    effective_to_block
                FROM unnest($2::TEXT[], $3::TEXT[], $4::BIGINT[], $5::BIGINT[]) AS scoped(
                    source_family,
                    address,
                    effective_from_block,
                    effective_to_block
                )
            ),
            scoped_addresses AS (
                SELECT
                    scoped.source_family AS scoped_source_family,
                    scoped.effective_from_block,
                    scoped.effective_to_block,
                    cia.contract_instance_id,
                    cia.chain_id,
                    cia.address
                FROM scoped_targets scoped
                JOIN contract_instance_addresses cia
                  ON cia.chain_id = $1
                 AND lower(cia.address) = scoped.address
                 AND cia.deactivated_at IS NULL
                 AND (
                     cia.active_from_block_number IS NULL
                     OR cia.active_from_block_number <= scoped.effective_to_block
                 )
                 AND (
                     cia.active_to_block_number IS NULL
                     OR scoped.effective_from_block <= cia.active_to_block_number
                 )
            ),
            direct_other_edge_sources AS (
                SELECT
                    mv.chain,
                    mv.source_family AS edge_source_family,
                    mv.manifest_id AS edge_source_manifest_id,
                    mv.source_family AS source_family,
                    mv.manifest_id AS source_manifest_id
                FROM manifest_versions mv
                WHERE mv.rollout_status = 'active'
                  AND mv.chain = $1
                  AND mv.source_family NOT IN ('ens_v1_registry_l1', 'basenames_base_registry')
            ),
            direct_registry_edge_sources AS (
                SELECT
                    mv.chain,
                    mv.source_family AS edge_source_family,
                    mv.manifest_id AS edge_source_manifest_id,
                    mv.source_family AS source_family,
                    mv.manifest_id AS source_manifest_id
                FROM manifest_versions mv
                WHERE mv.rollout_status = 'active'
                  AND mv.chain = $1
                  AND mv.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
            ),
            resolver_edge_sources AS (
                SELECT
                    mv.chain,
                    mv.source_family AS edge_source_family,
                    mv.manifest_id AS edge_source_manifest_id,
                    target_mv.source_family AS source_family,
                    target_mv.manifest_id AS source_manifest_id
                FROM manifest_versions mv
                JOIN manifest_versions target_mv
                  ON target_mv.rollout_status = 'active'
                 AND target_mv.namespace = mv.namespace
                 AND target_mv.chain = mv.chain
                 AND target_mv.deployment_epoch = mv.deployment_epoch
                 AND target_mv.source_family = CASE
                     WHEN mv.source_family = 'ens_v1_registry_l1'
                         THEN 'ens_v1_resolver_l1'
                     WHEN mv.source_family = 'basenames_base_registry'
                         THEN 'basenames_base_resolver'
                     ELSE NULL
                 END
                WHERE mv.rollout_status = 'active'
                  AND mv.chain = $1
                  AND mv.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
            )
            SELECT
                chain,
                source_family,
                address,
                contract_instance_id,
                source_manifest_id,
                active_from_block_number,
                active_to_block_number
            FROM (
                SELECT
                    scoped.chain_id AS chain,
                    candidate.source_family AS source_family,
                    scoped.address AS address,
                    scoped.contract_instance_id AS contract_instance_id,
                    candidate.source_manifest_id AS source_manifest_id,
                    scoped.effective_from_block AS active_from_block_number,
                    scoped.effective_to_block AS active_to_block_number
                FROM scoped_addresses scoped
                JOIN direct_other_edge_sources candidate
                  ON candidate.chain = scoped.chain_id
                 AND candidate.source_family = scoped.scoped_source_family
                JOIN LATERAL (
                    SELECT 1
                    FROM discovery_edges de
                    WHERE de.source_manifest_id = candidate.edge_source_manifest_id
                      AND de.to_contract_instance_id = scoped.contract_instance_id
                      AND de.chain_id = scoped.chain_id
                      AND de.deactivated_at IS NULL
                      AND de.edge_kind <> 'migration'
                      AND (
                          de.active_from_block_number IS NULL
                          OR de.active_from_block_number <= scoped.effective_to_block
                      )
                      AND (
                          de.active_to_block_number IS NULL
                          OR scoped.effective_from_block <= de.active_to_block_number
                      )
                    LIMIT 1
                ) active_edge ON TRUE

                UNION

                SELECT
                    scoped.chain_id AS chain,
                    candidate.source_family AS source_family,
                    scoped.address AS address,
                    scoped.contract_instance_id AS contract_instance_id,
                    candidate.source_manifest_id AS source_manifest_id,
                    scoped.effective_from_block AS active_from_block_number,
                    scoped.effective_to_block AS active_to_block_number
                FROM scoped_addresses scoped
                JOIN direct_registry_edge_sources candidate
                  ON candidate.chain = scoped.chain_id
                 AND candidate.source_family = scoped.scoped_source_family
                JOIN LATERAL (
                    SELECT 1
                    FROM discovery_edges de
                    WHERE de.source_manifest_id = candidate.edge_source_manifest_id
                      AND de.to_contract_instance_id = scoped.contract_instance_id
                      AND de.chain_id = scoped.chain_id
                      AND de.deactivated_at IS NULL
                      AND de.edge_kind <> 'migration'
                      AND de.edge_kind <> 'resolver'
                      AND (
                          de.active_from_block_number IS NULL
                          OR de.active_from_block_number <= scoped.effective_to_block
                      )
                      AND (
                          de.active_to_block_number IS NULL
                          OR scoped.effective_from_block <= de.active_to_block_number
                      )
                    LIMIT 1
                ) active_edge ON TRUE

                UNION

                SELECT
                    scoped.chain_id AS chain,
                    candidate.source_family AS source_family,
                    scoped.address AS address,
                    scoped.contract_instance_id AS contract_instance_id,
                    candidate.source_manifest_id AS source_manifest_id,
                    scoped.effective_from_block AS active_from_block_number,
                    scoped.effective_to_block AS active_to_block_number
                FROM scoped_addresses scoped
                JOIN resolver_edge_sources candidate
                  ON candidate.chain = scoped.chain_id
                 AND candidate.source_family = scoped.scoped_source_family
                JOIN LATERAL (
                    SELECT 1
                    FROM discovery_edges de
                    WHERE de.source_manifest_id = candidate.edge_source_manifest_id
                      AND de.to_contract_instance_id = scoped.contract_instance_id
                      AND de.chain_id = scoped.chain_id
                      AND de.deactivated_at IS NULL
                      AND de.edge_kind = 'resolver'
                      AND (
                          de.active_from_block_number IS NULL
                          OR de.active_from_block_number <= scoped.effective_to_block
                      )
                      AND (
                          de.active_to_block_number IS NULL
                          OR scoped.effective_from_block <= de.active_to_block_number
                      )
                    LIMIT 1
                ) active_edge ON TRUE
            ) discovered
            ORDER BY chain, source_family, address, source_manifest_id, contract_instance_id
            "#,
        )
        .bind(chain)
        .bind(&scoped_source_families)
        .bind(&scoped_addresses)
        .bind(&scoped_from_blocks)
        .bind(&scoped_to_blocks)
        .fetch_all(pool)
        .await
    } else {
        sqlx::query(
        r#"
        WITH scoped_targets AS (
            SELECT DISTINCT
                source_family,
                address,
                effective_from_block,
                effective_to_block
            FROM unnest($2::TEXT[], $3::TEXT[], $4::BIGINT[], $5::BIGINT[]) AS scoped(
                source_family,
                address,
                effective_from_block,
                effective_to_block
            )
        ),
        scoped_addresses AS (
            SELECT
                scoped.source_family AS scoped_source_family,
                scoped.effective_from_block,
                scoped.effective_to_block,
                cia.contract_instance_id,
                cia.chain_id,
                cia.address,
                cia.active_from_block_number AS address_active_from_block_number,
                cia.active_to_block_number AS address_active_to_block_number
            FROM scoped_targets scoped
            JOIN contract_instance_addresses cia
              ON cia.chain_id = $1
             AND lower(cia.address) = scoped.address
             AND cia.deactivated_at IS NULL
        ),
        direct_other_edge_sources AS (
            SELECT
                mv.chain,
                mv.source_family AS edge_source_family,
                mv.manifest_id AS edge_source_manifest_id,
                mv.source_family AS source_family,
                mv.manifest_id AS source_manifest_id
            FROM manifest_versions mv
            WHERE mv.rollout_status = 'active'
              AND mv.chain = $1
              AND mv.source_family NOT IN ('ens_v1_registry_l1', 'basenames_base_registry')
        ),
        direct_registry_edge_sources AS (
            SELECT
                mv.chain,
                mv.source_family AS edge_source_family,
                mv.manifest_id AS edge_source_manifest_id,
                mv.source_family AS source_family,
                mv.manifest_id AS source_manifest_id
            FROM manifest_versions mv
            WHERE mv.rollout_status = 'active'
              AND mv.chain = $1
              AND mv.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
        ),
        resolver_edge_sources AS (
            SELECT
                mv.chain,
                mv.source_family AS edge_source_family,
                mv.manifest_id AS edge_source_manifest_id,
                target_mv.source_family AS source_family,
                target_mv.manifest_id AS source_manifest_id
            FROM manifest_versions mv
            JOIN manifest_versions target_mv
              ON target_mv.rollout_status = 'active'
             AND target_mv.namespace = mv.namespace
             AND target_mv.chain = mv.chain
             AND target_mv.deployment_epoch = mv.deployment_epoch
             AND target_mv.source_family = CASE
                 WHEN mv.source_family = 'ens_v1_registry_l1'
                     THEN 'ens_v1_resolver_l1'
                 WHEN mv.source_family = 'basenames_base_registry'
                     THEN 'basenames_base_resolver'
                 ELSE NULL
             END
            WHERE mv.rollout_status = 'active'
              AND mv.chain = $1
              AND mv.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
        ),
        direct_other_discovery_scoped AS (
            SELECT
                de.chain_id AS chain,
                candidate.source_family AS source_family,
                scoped.address AS address,
                de.to_contract_instance_id AS contract_instance_id,
                candidate.source_manifest_id AS source_manifest_id,
                GREATEST(
                    scoped.effective_from_block,
                    COALESCE(de.active_from_block_number, scoped.effective_from_block),
                    COALESCE(scoped.address_active_from_block_number, scoped.effective_from_block)
                ) AS active_from_block_number,
                LEAST(
                    scoped.effective_to_block,
                    COALESCE(de.active_to_block_number, scoped.effective_to_block),
                    COALESCE(scoped.address_active_to_block_number, scoped.effective_to_block)
                ) AS active_to_block_number
            FROM scoped_addresses scoped
            JOIN direct_other_edge_sources candidate
              ON candidate.chain = scoped.chain_id
             AND candidate.source_family = scoped.scoped_source_family
            JOIN discovery_edges de
              ON de.source_manifest_id = candidate.edge_source_manifest_id
             AND de.to_contract_instance_id = scoped.contract_instance_id
             AND de.chain_id = scoped.chain_id
             AND de.deactivated_at IS NULL
             AND de.edge_kind <> 'migration'
            WHERE (
                  de.active_from_block_number IS NULL
                  OR scoped.address_active_to_block_number IS NULL
                  OR de.active_from_block_number <= scoped.address_active_to_block_number
              )
              AND (
                  scoped.address_active_from_block_number IS NULL
                  OR de.active_to_block_number IS NULL
                  OR scoped.address_active_from_block_number <= de.active_to_block_number
              )
              AND scoped.effective_from_block <= COALESCE(
                  CASE
                      WHEN de.active_to_block_number IS NULL THEN scoped.address_active_to_block_number
                      WHEN scoped.address_active_to_block_number IS NULL THEN de.active_to_block_number
                      ELSE LEAST(de.active_to_block_number, scoped.address_active_to_block_number)
                  END,
                  9223372036854775807
              )
              AND COALESCE(
                  CASE
                      WHEN de.active_from_block_number IS NULL THEN scoped.address_active_from_block_number
                      WHEN scoped.address_active_from_block_number IS NULL THEN de.active_from_block_number
                      ELSE GREATEST(de.active_from_block_number, scoped.address_active_from_block_number)
                  END,
                  0
              ) <= scoped.effective_to_block
        ),
        direct_registry_discovery_scoped AS (
            SELECT
                de.chain_id AS chain,
                candidate.source_family AS source_family,
                scoped.address AS address,
                de.to_contract_instance_id AS contract_instance_id,
                candidate.source_manifest_id AS source_manifest_id,
                GREATEST(
                    scoped.effective_from_block,
                    COALESCE(de.active_from_block_number, scoped.effective_from_block),
                    COALESCE(scoped.address_active_from_block_number, scoped.effective_from_block)
                ) AS active_from_block_number,
                LEAST(
                    scoped.effective_to_block,
                    COALESCE(de.active_to_block_number, scoped.effective_to_block),
                    COALESCE(scoped.address_active_to_block_number, scoped.effective_to_block)
                ) AS active_to_block_number
            FROM scoped_addresses scoped
            JOIN direct_registry_edge_sources candidate
              ON candidate.chain = scoped.chain_id
             AND candidate.source_family = scoped.scoped_source_family
            JOIN discovery_edges de
              ON de.source_manifest_id = candidate.edge_source_manifest_id
             AND de.to_contract_instance_id = scoped.contract_instance_id
             AND de.chain_id = scoped.chain_id
             AND de.deactivated_at IS NULL
             AND de.edge_kind <> 'migration'
             AND de.edge_kind <> 'resolver'
            WHERE (
                  de.active_from_block_number IS NULL
                  OR scoped.address_active_to_block_number IS NULL
                  OR de.active_from_block_number <= scoped.address_active_to_block_number
              )
              AND (
                  scoped.address_active_from_block_number IS NULL
                  OR de.active_to_block_number IS NULL
                  OR scoped.address_active_from_block_number <= de.active_to_block_number
              )
              AND scoped.effective_from_block <= COALESCE(
                  CASE
                      WHEN de.active_to_block_number IS NULL THEN scoped.address_active_to_block_number
                      WHEN scoped.address_active_to_block_number IS NULL THEN de.active_to_block_number
                      ELSE LEAST(de.active_to_block_number, scoped.address_active_to_block_number)
                  END,
                  9223372036854775807
              )
              AND COALESCE(
                  CASE
                      WHEN de.active_from_block_number IS NULL THEN scoped.address_active_from_block_number
                      WHEN scoped.address_active_from_block_number IS NULL THEN de.active_from_block_number
                      ELSE GREATEST(de.active_from_block_number, scoped.address_active_from_block_number)
                  END,
                  0
              ) <= scoped.effective_to_block
        ),
        resolver_discovery_scoped AS (
            SELECT
                de.chain_id AS chain,
                candidate.source_family AS source_family,
                scoped.address AS address,
                de.to_contract_instance_id AS contract_instance_id,
                candidate.source_manifest_id AS source_manifest_id,
                GREATEST(
                    scoped.effective_from_block,
                    COALESCE(de.active_from_block_number, scoped.effective_from_block),
                    COALESCE(scoped.address_active_from_block_number, scoped.effective_from_block)
                ) AS active_from_block_number,
                LEAST(
                    scoped.effective_to_block,
                    COALESCE(de.active_to_block_number, scoped.effective_to_block),
                    COALESCE(scoped.address_active_to_block_number, scoped.effective_to_block)
                ) AS active_to_block_number
            FROM scoped_addresses scoped
            JOIN resolver_edge_sources candidate
              ON candidate.chain = scoped.chain_id
             AND candidate.source_family = scoped.scoped_source_family
            JOIN discovery_edges de
              ON de.source_manifest_id = candidate.edge_source_manifest_id
             AND de.to_contract_instance_id = scoped.contract_instance_id
             AND de.chain_id = scoped.chain_id
             AND de.deactivated_at IS NULL
             AND de.edge_kind = 'resolver'
            WHERE (
                  de.active_from_block_number IS NULL
                  OR scoped.address_active_to_block_number IS NULL
                  OR de.active_from_block_number <= scoped.address_active_to_block_number
              )
              AND (
                  scoped.address_active_from_block_number IS NULL
                  OR de.active_to_block_number IS NULL
                  OR scoped.address_active_from_block_number <= de.active_to_block_number
              )
              AND scoped.effective_from_block <= COALESCE(
                  CASE
                      WHEN de.active_to_block_number IS NULL THEN scoped.address_active_to_block_number
                      WHEN scoped.address_active_to_block_number IS NULL THEN de.active_to_block_number
                      ELSE LEAST(de.active_to_block_number, scoped.address_active_to_block_number)
                  END,
                  9223372036854775807
              )
              AND COALESCE(
                  CASE
                      WHEN de.active_from_block_number IS NULL THEN scoped.address_active_from_block_number
                      WHEN scoped.address_active_from_block_number IS NULL THEN de.active_from_block_number
                      ELSE GREATEST(de.active_from_block_number, scoped.address_active_from_block_number)
                  END,
                  0
              ) <= scoped.effective_to_block
        )
        SELECT DISTINCT
            chain,
            source_family,
            address,
            contract_instance_id,
            source_manifest_id,
            active_from_block_number,
            active_to_block_number
        FROM direct_other_discovery_scoped

        UNION

        SELECT
            chain,
            source_family,
            address,
            contract_instance_id,
            source_manifest_id,
            active_from_block_number,
            active_to_block_number
        FROM direct_registry_discovery_scoped

        UNION

        SELECT
            chain,
            source_family,
            address,
            contract_instance_id,
            source_manifest_id,
            active_from_block_number,
            active_to_block_number
        FROM resolver_discovery_scoped
        ORDER BY chain, source_family, address, source_manifest_id, contract_instance_id
        "#,
        )
        .bind(chain)
        .bind(&scoped_source_families)
        .bind(&scoped_addresses)
        .bind(&scoped_from_blocks)
        .bind(&scoped_to_blocks)
        .fetch_all(pool)
        .await
    }
    .with_context(|| {
        format!("failed to load scoped ENSv1 unwrapped authority discovery contracts for {chain}")
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(WatchedContract {
                chain: sql_row::get(&row, "chain")?,
                source_family: sql_row::get(&row, "source_family")?,
                address: sql_row::get::<String>(&row, "address")?.to_ascii_lowercase(),
                contract_instance_id: sql_row::get(&row, "contract_instance_id")?,
                source: WatchedContractSource::DiscoveryEdge,
                source_manifest_id: sql_row::get(&row, "source_manifest_id")?,
                active_from_block_number: sql_row::get(&row, "active_from_block_number")?,
                active_to_block_number: sql_row::get(&row, "active_to_block_number")?,
            })
        })
        .collect()
}

fn filter_watched_contracts_by_scope(
    contracts: Vec<WatchedContract>,
    chain: &str,
    source_scope: &[AuthorityRawLogSourceScopeTarget],
) -> Vec<WatchedContract> {
    contracts
        .into_iter()
        .filter(|contract| contract.chain == chain)
        .filter(|contract| {
            source_scope.iter().any(|target| {
                target.source_family == contract.source_family
                    && target.address.eq_ignore_ascii_case(&contract.address)
                    && watched_contract_intersects_source_scope(contract, target)
            })
        })
        .collect()
}

fn source_scope_covered_by_watched_contracts(
    source_scope: &[AuthorityRawLogSourceScopeTarget],
    watched_contracts: &[WatchedContract],
) -> bool {
    source_scope
        .iter()
        .filter(|target| is_unwrapped_authority_source_family(&target.source_family))
        .filter(|target| !is_generic_resolver_event_source_scope_target(target))
        .all(|target| {
            watched_contracts.iter().any(|contract| {
                target.source_family == contract.source_family
                    && target.address.eq_ignore_ascii_case(&contract.address)
                    && watched_contract_intersects_source_scope(contract, target)
            })
        })
}

fn is_unwrapped_authority_source_family(source_family: &str) -> bool {
    source_family == SOURCE_FAMILY_ENS_V1_REGISTRAR_L1
        || source_family == SOURCE_FAMILY_ENS_V1_REGISTRY_L1
        || source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1
        || source_family == SOURCE_FAMILY_ENS_V1_WRAPPER_L1
        || source_family == SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR
        || source_family == SOURCE_FAMILY_BASENAMES_BASE_REGISTRY
        || source_family == SOURCE_FAMILY_BASENAMES_BASE_RESOLVER
}

fn watched_contract_intersects_source_scope(
    contract: &WatchedContract,
    target: &AuthorityRawLogSourceScopeTarget,
) -> bool {
    let contract_from = contract.active_from_block_number.unwrap_or(0);
    let contract_to = contract.active_to_block_number.unwrap_or(i64::MAX);
    target.effective_from_block <= contract_to && contract_from <= target.effective_to_block
}

async fn load_active_manifest_metadata(
    pool: &PgPool,
    manifest_ids: &[i64],
) -> Result<HashMap<i64, ActiveManifestMetadata>> {
    let rows = sqlx::query(
        r#"
        SELECT manifest_id, chain, namespace, source_family, manifest_version, normalizer_version
        FROM manifest_versions
        WHERE rollout_status = 'active'
          AND manifest_id = ANY($1::BIGINT[])
        "#,
    )
    .bind(manifest_ids)
    .fetch_all(pool)
    .await
    .context("failed to load active manifest metadata for ENSv1 unwrapped authority")?;

    rows.into_iter()
        .map(|row| {
            let manifest = ActiveManifestMetadata {
                manifest_id: sql_row::get(&row, "manifest_id")?,
                chain: sql_row::get(&row, "chain")?,
                namespace: sql_row::get(&row, "namespace")?,
                source_family: sql_row::get(&row, "source_family")?,
                manifest_version: sql_row::get(&row, "manifest_version")?,
                normalizer_version: sql_row::get(&row, "normalizer_version")?,
            };
            Ok((manifest.manifest_id, manifest))
        })
        .collect()
}

async fn load_manifest_contract_roles(
    pool: &PgPool,
    watched_contracts: &[WatchedContract],
) -> Result<HashMap<(i64, Uuid), String>> {
    let manifest_ids = watched_contracts
        .iter()
        .filter_map(|contract| contract.source_manifest_id)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let contract_instance_ids = watched_contracts
        .iter()
        .map(|contract| contract.contract_instance_id)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if manifest_ids.is_empty() || contract_instance_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT manifest_id, contract_instance_id, role
        FROM manifest_contract_instances
        WHERE declaration_kind = 'contract'
          AND manifest_id = ANY($1::BIGINT[])
          AND contract_instance_id = ANY($2::UUID[])
        "#,
    )
    .bind(&manifest_ids)
    .bind(&contract_instance_ids)
    .fetch_all(pool)
    .await
    .context("failed to load manifest contract roles for ENSv1 unwrapped authority")?;

    rows.into_iter()
        .map(|row| {
            Ok((
                (
                    sql_row::get(&row, "manifest_id")?,
                    sql_row::get(&row, "contract_instance_id")?,
                ),
                sql_row::get(&row, "role")?,
            ))
        })
        .collect()
}

fn source_rank(source: WatchedContractSource) -> i32 {
    crate::adapter_manifest::source_rank(source)
}

async fn load_active_manifest_metadata_for_source_family(
    pool: &PgPool,
    chain: &str,
    source_family: &str,
) -> Result<Vec<ActiveManifestMetadata>> {
    let rows = sqlx::query(
        r#"
        SELECT manifest_id, chain, namespace, source_family, manifest_version, normalizer_version
        FROM manifest_versions
        WHERE rollout_status = 'active'
          AND chain = $1
          AND source_family = $2
        ORDER BY manifest_id
        "#,
    )
    .bind(chain)
    .bind(source_family)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load active {source_family} manifest metadata for {chain}")
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(ActiveManifestMetadata {
                manifest_id: sql_row::get(&row, "manifest_id")?,
                chain: sql_row::get(&row, "chain")?,
                namespace: sql_row::get(&row, "namespace")?,
                source_family: sql_row::get(&row, "source_family")?,
                manifest_version: sql_row::get(&row, "manifest_version")?,
                normalizer_version: sql_row::get(&row, "normalizer_version")?,
            })
        })
        .collect()
}
