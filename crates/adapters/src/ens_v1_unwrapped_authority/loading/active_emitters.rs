use std::collections::{HashMap, HashSet};

use super::super::scope::AuthorityRawLogSourceScopeTarget;
use super::super::*;
use anyhow::{Context, Result};
use bigname_manifests::{
    WatchedContract, WatchedContractSource, load_manifest_declared_watched_contracts,
    load_watched_contracts,
};
use sqlx::{PgPool, Row, types::Uuid};

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

    let manifest_ids = watched_contracts
        .iter()
        .map(|contract| {
            contract.source_manifest_id.with_context(|| {
                format!(
                    "watched contract {} on {} is missing source_manifest_id",
                    contract.address, contract.chain
                )
            })
        })
        .collect::<Result<HashSet<_>>>()?
        .into_iter()
        .collect::<Vec<_>>();

    let active_manifests = load_active_manifest_metadata(pool, &manifest_ids).await?;
    let mut emitters = Vec::new();
    for watched_contract in watched_contracts {
        let Some(source_manifest_id) = watched_contract.source_manifest_id else {
            continue;
        };
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
    let scoped_source_families = source_scope
        .iter()
        .map(|target| target.source_family.clone())
        .collect::<Vec<_>>();
    let scoped_addresses = source_scope
        .iter()
        .map(|target| target.address.clone())
        .collect::<Vec<_>>();
    let scoped_from_blocks = source_scope
        .iter()
        .map(|target| target.effective_from_block)
        .collect::<Vec<_>>();
    let scoped_to_blocks = source_scope
        .iter()
        .map(|target| target.effective_to_block)
        .collect::<Vec<_>>();

    let rows = sqlx::query(
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
             AND cia.address = scoped.address
             AND cia.deactivated_at IS NULL
        )
        SELECT
            de.chain_id AS chain,
            COALESCE(target_mv.source_family, mv.source_family) AS source_family,
            scoped.address AS address,
            de.to_contract_instance_id AS contract_instance_id,
            COALESCE(target_mv.manifest_id, de.source_manifest_id) AS source_manifest_id,
            CASE
                WHEN de.active_from_block_number IS NULL THEN scoped.address_active_from_block_number
                WHEN scoped.address_active_from_block_number IS NULL THEN de.active_from_block_number
                ELSE GREATEST(de.active_from_block_number, scoped.address_active_from_block_number)
            END AS active_from_block_number,
            CASE
                WHEN de.active_to_block_number IS NULL THEN scoped.address_active_to_block_number
                WHEN scoped.address_active_to_block_number IS NULL THEN de.active_to_block_number
                ELSE LEAST(de.active_to_block_number, scoped.address_active_to_block_number)
            END AS active_to_block_number
        FROM scoped_addresses scoped
        JOIN discovery_edges de
          ON de.chain_id = scoped.chain_id
         AND de.to_contract_instance_id = scoped.contract_instance_id
         AND de.deactivated_at IS NULL
         AND de.edge_kind <> 'migration'
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
        WHERE mv.rollout_status = 'active'
          AND (
              de.edge_kind <> 'resolver'
              OR mv.source_family NOT IN ('ens_v1_registry_l1', 'basenames_base_registry')
              OR target_mv.manifest_id IS NOT NULL
          )
          AND scoped.scoped_source_family = COALESCE(target_mv.source_family, mv.source_family)
          AND (
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
    .with_context(|| {
        format!("failed to load scoped ENSv1 unwrapped authority discovery contracts for {chain}")
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(WatchedContract {
                chain: row.try_get("chain").context("missing chain")?,
                source_family: row
                    .try_get("source_family")
                    .context("missing source_family")?,
                address: row
                    .try_get::<String, _>("address")
                    .context("missing address")?
                    .to_ascii_lowercase(),
                contract_instance_id: row
                    .try_get("contract_instance_id")
                    .context("missing contract_instance_id")?,
                source: WatchedContractSource::DiscoveryEdge,
                source_manifest_id: row
                    .try_get("source_manifest_id")
                    .context("missing source_manifest_id")?,
                active_from_block_number: row
                    .try_get("active_from_block_number")
                    .context("missing active_from_block_number")?,
                active_to_block_number: row
                    .try_get("active_to_block_number")
                    .context("missing active_to_block_number")?,
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
                    && target.address == contract.address
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
        .all(|target| {
            watched_contracts.iter().any(|contract| {
                target.source_family == contract.source_family
                    && target.address == contract.address
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
                manifest_id: row.try_get("manifest_id").context("missing manifest_id")?,
                chain: row.try_get("chain").context("missing chain")?,
                namespace: row.try_get("namespace").context("missing namespace")?,
                source_family: row
                    .try_get("source_family")
                    .context("missing source_family")?,
                manifest_version: row
                    .try_get("manifest_version")
                    .context("missing manifest_version")?,
                normalizer_version: row
                    .try_get("normalizer_version")
                    .context("missing normalizer_version")?,
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
                    row.try_get("manifest_id").context("missing manifest_id")?,
                    row.try_get("contract_instance_id")
                        .context("missing contract_instance_id")?,
                ),
                row.try_get("role").context("missing role")?,
            ))
        })
        .collect()
}

fn source_rank(source: WatchedContractSource) -> i32 {
    match source {
        WatchedContractSource::ManifestRoot => 0,
        WatchedContractSource::ManifestContract => 1,
        WatchedContractSource::DiscoveryEdge => 2,
    }
}
