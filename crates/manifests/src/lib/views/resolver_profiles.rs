use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::{ResolverProfileAdmission, WatchedContract, WatchedContractSource, normalize_address};

#[path = "resolver_profiles/ens_v1.rs"]
mod ens_v1;

pub use ens_v1::*;

use super::{
    drift::{
        load_manifest_code_hash_observations,
        load_manifest_code_hash_observations_for_watched_contracts,
    },
    types::ManifestCodeHashObservation,
    watched::load_watched_contracts_by_source_family,
};

const BASENAMES_BASE_RESOLVER_SOURCE_FAMILY: &str = "basenames_base_resolver";
const BASENAMES_L2_RESOLVER_ROLE: &str = "resolver";
const BASENAMES_L2_RESOLVER_COMPATIBLE_PROFILE: &str = "l2_resolver_compatible";
const BASENAMES_L2_RESOLVER_PROFILE_FACT_FAMILIES: [&str; 2] =
    ["resolver_record", "resolver_authorization"];
pub(super) const RESOLVER_PROFILE_STATUS_PENDING: &str = "pending";
pub(super) const RESOLVER_PROFILE_STATUS_SUPPORTED: &str = "supported";
pub(super) const RESOLVER_PROFILE_STATUS_UNSUPPORTED: &str = "unsupported";
const RESOLVER_PROFILE_BASIS_BASENAMES_L2_RESOLVER_SEED: &str = "manifest_l2_resolver_seed";
pub(super) const RESOLVER_PROFILE_BASIS_CODE_HASH_MATCH: &str = "code_hash_match";
pub(super) const RESOLVER_PROFILE_BASIS_CODE_HASH_PENDING: &str = "code_hash_pending";
pub(super) const RESOLVER_PROFILE_BASIS_CODE_HASH_MISMATCH: &str = "code_hash_mismatch";

pub async fn load_basenames_l2_resolver_profile_admissions(
    pool: &PgPool,
) -> Result<Vec<ResolverProfileAdmission>> {
    let l2_resolver_seed_ids = load_resolver_profile_seed_ids(
        pool,
        "basenames",
        BASENAMES_BASE_RESOLVER_SOURCE_FAMILY,
        BASENAMES_L2_RESOLVER_ROLE,
        "Basenames L2Resolver",
    )
    .await?;
    let watched_contracts =
        load_watched_contracts_by_source_family(pool, BASENAMES_BASE_RESOLVER_SOURCE_FAMILY)
            .await?;
    let code_hash_observations = load_manifest_code_hash_observations(pool).await?;

    Ok(derive_basenames_l2_resolver_profile_admissions(
        &watched_contracts,
        &code_hash_observations,
        &l2_resolver_seed_ids,
    ))
}

pub async fn load_basenames_l2_resolver_profile_admissions_for_targets(
    pool: &PgPool,
    targets: &[(String, String)],
) -> Result<Vec<ResolverProfileAdmission>> {
    if targets.is_empty() {
        return Ok(Vec::new());
    }

    let l2_resolver_seed_contracts = load_resolver_profile_seed_watched_contracts(
        pool,
        "basenames",
        BASENAMES_BASE_RESOLVER_SOURCE_FAMILY,
        BASENAMES_L2_RESOLVER_ROLE,
        "Basenames L2Resolver",
    )
    .await?;
    let l2_resolver_seed_ids = l2_resolver_seed_contracts
        .iter()
        .map(|contract| contract.contract_instance_id)
        .collect::<Vec<_>>();
    let target_contracts = load_resolver_profile_target_watched_contracts(
        pool,
        BASENAMES_BASE_RESOLVER_SOURCE_FAMILY,
        targets,
    )
    .await?;
    let mut code_hash_targets = l2_resolver_seed_contracts.clone();
    code_hash_targets.extend(target_contracts.clone());
    let code_hash_observations =
        load_manifest_code_hash_observations_for_watched_contracts(pool, &code_hash_targets)
            .await?;

    Ok(derive_code_hash_resolver_profile_admissions(
        &target_contracts,
        &code_hash_observations,
        &l2_resolver_seed_ids,
        ResolverProfileAdmissionConfig {
            source_family: BASENAMES_BASE_RESOLVER_SOURCE_FAMILY,
            profile: BASENAMES_L2_RESOLVER_COMPATIBLE_PROFILE,
            fact_families: &BASENAMES_L2_RESOLVER_PROFILE_FACT_FAMILIES,
            manifest_seed_basis: RESOLVER_PROFILE_BASIS_BASENAMES_L2_RESOLVER_SEED,
        },
    ))
}

pub fn derive_basenames_l2_resolver_profile_admissions(
    watched_contracts: &[WatchedContract],
    code_hash_observations: &[ManifestCodeHashObservation],
    l2_resolver_seed_ids: &[Uuid],
) -> Vec<ResolverProfileAdmission> {
    derive_code_hash_resolver_profile_admissions(
        watched_contracts,
        code_hash_observations,
        l2_resolver_seed_ids,
        ResolverProfileAdmissionConfig {
            source_family: BASENAMES_BASE_RESOLVER_SOURCE_FAMILY,
            profile: BASENAMES_L2_RESOLVER_COMPATIBLE_PROFILE,
            fact_families: &BASENAMES_L2_RESOLVER_PROFILE_FACT_FAMILIES,
            manifest_seed_basis: RESOLVER_PROFILE_BASIS_BASENAMES_L2_RESOLVER_SEED,
        },
    )
}

pub(super) async fn load_resolver_profile_seed_ids(
    pool: &PgPool,
    namespace: &str,
    source_family: &str,
    role: &str,
    context_label: &str,
) -> Result<Vec<Uuid>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT mci.contract_instance_id
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
        WHERE mv.rollout_status = 'active'
          AND mv.namespace = $1
          AND mv.source_family = $2
          AND mci.declaration_kind = 'contract'
          AND mci.role = $3
        ORDER BY mci.contract_instance_id
        "#,
    )
    .bind(namespace)
    .bind(source_family)
    .bind(role)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load {context_label} profile seed contract instances"))?;

    rows.into_iter()
        .map(|row| {
            row.try_get("contract_instance_id").with_context(|| {
                format!("failed to read {context_label} seed contract_instance_id")
            })
        })
        .collect()
}

pub(super) async fn load_resolver_profile_seed_watched_contracts(
    pool: &PgPool,
    namespace: &str,
    source_family: &str,
    role: &str,
    context_label: &str,
) -> Result<Vec<WatchedContract>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT
            mv.chain AS chain,
            mv.source_family AS source_family,
            cia.address AS address,
            mci.contract_instance_id AS contract_instance_id,
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
            FROM jsonb_array_elements(mv.manifest_payload -> 'contracts') entry
            WHERE entry ->> 'role' = mci.declaration_name
            ORDER BY start_block NULLS LAST
            LIMIT 1
        ) manifest_range ON TRUE
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = mci.contract_instance_id
         AND cia.deactivated_at IS NULL
        WHERE mv.rollout_status = 'active'
          AND mv.namespace = $1
          AND mv.source_family = $2
          AND mci.declaration_kind = 'contract'
          AND mci.role = $3
        ORDER BY mv.chain, cia.address, mci.contract_instance_id
        "#,
    )
    .bind(namespace)
    .bind(source_family)
    .bind(role)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load {context_label} seed watched contracts"))?;

    rows.into_iter()
        .map(|row| {
            let address = row
                .try_get::<String, _>("address")
                .context("failed to read resolver seed address")?;
            Ok(WatchedContract {
                chain: row.try_get("chain").context("failed to read seed chain")?,
                source_family: row
                    .try_get("source_family")
                    .context("failed to read seed source_family")?,
                address: normalize_address(&address),
                contract_instance_id: row
                    .try_get("contract_instance_id")
                    .context("failed to read seed contract_instance_id")?,
                source: WatchedContractSource::ManifestContract,
                source_manifest_id: row
                    .try_get("source_manifest_id")
                    .context("failed to read seed source_manifest_id")?,
                active_from_block_number: row
                    .try_get("active_from_block_number")
                    .context("failed to read seed active_from_block_number")?,
                active_to_block_number: row
                    .try_get("active_to_block_number")
                    .context("failed to read seed active_to_block_number")?,
            })
        })
        .collect()
}

pub(super) async fn load_resolver_profile_target_watched_contracts(
    pool: &PgPool,
    source_family: &str,
    targets: &[(String, String)],
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

    let rows = sqlx::query(
        r#"
        WITH target_addresses AS (
            SELECT DISTINCT chain, address
            FROM UNNEST($1::TEXT[], $2::TEXT[]) AS target(chain, address)
        ),
        target_instances AS (
            SELECT
                target.chain,
                target.address,
                cia.contract_instance_id,
                cia.active_from_block_number,
                cia.active_to_block_number
            FROM target_addresses target
            JOIN contract_instance_addresses cia
              ON cia.chain_id = target.chain
             AND cia.address = target.address
             AND cia.deactivated_at IS NULL
        ),
        manifest_declared AS (
            SELECT DISTINCT
                ti.chain AS chain,
                mv.source_family AS source_family,
                ti.address AS address,
                mci.contract_instance_id AS contract_instance_id,
                CASE
                    WHEN mci.declaration_kind = 'root' THEN 'manifest_root'
                    ELSE 'manifest_contract'
                END::TEXT AS source,
                mv.manifest_id AS source_manifest_id,
                CASE
                    WHEN manifest_range.start_block IS NULL THEN ti.active_from_block_number
                    WHEN ti.active_from_block_number IS NULL THEN manifest_range.start_block
                    ELSE GREATEST(manifest_range.start_block, ti.active_from_block_number)
                END AS active_from_block_number,
                ti.active_to_block_number AS active_to_block_number
            FROM target_instances ti
            JOIN manifest_contract_instances mci
              ON mci.contract_instance_id = ti.contract_instance_id
            JOIN manifest_versions mv
              ON mv.manifest_id = mci.manifest_id
             AND mv.chain = ti.chain
             AND mv.rollout_status = 'active'
             AND mv.source_family = $3
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
              AND mv.source_family = $3
              AND mv.source_family NOT IN (
                  'ens_v1_registry_l1',
                  'ens_v2_registry_l1',
                  'basenames_base_registry'
              )
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
              AND mv.source_family = $3
              AND mv.source_family IN (
                  'ens_v1_registry_l1',
                  'ens_v2_registry_l1',
                  'basenames_base_registry'
              )
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
                 WHEN mv.source_family = 'ens_v2_registry_l1'
                     THEN 'ens_v2_resolver_l1'
                 WHEN mv.source_family = 'basenames_base_registry'
                     THEN 'basenames_base_resolver'
                 ELSE NULL
             END
            WHERE mv.rollout_status = 'active'
              AND mv.source_family IN (
                  'ens_v1_registry_l1',
                  'ens_v2_registry_l1',
                  'basenames_base_registry'
              )
              AND target_mv.source_family = $3
        ),
        direct_other_discovery_scoped AS (
            SELECT
                ti.chain AS chain,
                candidate.source_family AS source_family,
                ti.address AS address,
                ti.contract_instance_id AS contract_instance_id,
                'discovery_edge'::TEXT AS source,
                candidate.source_manifest_id AS source_manifest_id,
                CASE
                    WHEN active_edge.active_from_block_number IS NULL THEN ti.active_from_block_number
                    WHEN ti.active_from_block_number IS NULL THEN active_edge.active_from_block_number
                    ELSE GREATEST(active_edge.active_from_block_number, ti.active_from_block_number)
                END AS active_from_block_number,
                CASE
                    WHEN active_edge.active_to_block_number IS NULL THEN ti.active_to_block_number
                    WHEN ti.active_to_block_number IS NULL THEN active_edge.active_to_block_number
                    ELSE LEAST(active_edge.active_to_block_number, ti.active_to_block_number)
                END AS active_to_block_number
            FROM target_instances ti
            JOIN direct_other_edge_sources candidate
              ON candidate.chain = ti.chain
            JOIN LATERAL (
                SELECT de.active_from_block_number, de.active_to_block_number
                FROM discovery_edges de
                WHERE de.chain_id = ti.chain
                  AND de.to_contract_instance_id = ti.contract_instance_id
                  AND de.source_manifest_id = candidate.edge_source_manifest_id
                  AND de.deactivated_at IS NULL
                  AND de.edge_kind <> 'migration'
                  AND (
                      de.active_from_block_number IS NULL
                      OR ti.active_to_block_number IS NULL
                      OR de.active_from_block_number <= ti.active_to_block_number
                  )
                  AND (
                      ti.active_from_block_number IS NULL
                      OR de.active_to_block_number IS NULL
                      OR ti.active_from_block_number <= de.active_to_block_number
                  )
                LIMIT 1
            ) active_edge ON TRUE
        ),
        direct_registry_discovery_scoped AS (
            SELECT
                ti.chain AS chain,
                candidate.source_family AS source_family,
                ti.address AS address,
                ti.contract_instance_id AS contract_instance_id,
                'discovery_edge'::TEXT AS source,
                candidate.source_manifest_id AS source_manifest_id,
                CASE
                    WHEN active_edge.active_from_block_number IS NULL THEN ti.active_from_block_number
                    WHEN ti.active_from_block_number IS NULL THEN active_edge.active_from_block_number
                    ELSE GREATEST(active_edge.active_from_block_number, ti.active_from_block_number)
                END AS active_from_block_number,
                CASE
                    WHEN active_edge.active_to_block_number IS NULL THEN ti.active_to_block_number
                    WHEN ti.active_to_block_number IS NULL THEN active_edge.active_to_block_number
                    ELSE LEAST(active_edge.active_to_block_number, ti.active_to_block_number)
                END AS active_to_block_number
            FROM target_instances ti
            JOIN direct_registry_edge_sources candidate
              ON candidate.chain = ti.chain
            JOIN LATERAL (
                SELECT de.active_from_block_number, de.active_to_block_number
                FROM discovery_edges de
                WHERE de.chain_id = ti.chain
                  AND de.to_contract_instance_id = ti.contract_instance_id
                  AND de.source_manifest_id = candidate.edge_source_manifest_id
                  AND de.deactivated_at IS NULL
                  AND de.edge_kind <> 'migration'
                  AND de.edge_kind <> 'resolver'
                  AND (
                      de.active_from_block_number IS NULL
                      OR ti.active_to_block_number IS NULL
                      OR de.active_from_block_number <= ti.active_to_block_number
                  )
                  AND (
                      ti.active_from_block_number IS NULL
                      OR de.active_to_block_number IS NULL
                      OR ti.active_from_block_number <= de.active_to_block_number
                  )
                LIMIT 1
            ) active_edge ON TRUE
        ),
        resolver_discovery_scoped AS (
            SELECT
                ti.chain AS chain,
                candidate.source_family AS source_family,
                ti.address AS address,
                ti.contract_instance_id AS contract_instance_id,
                'discovery_edge'::TEXT AS source,
                candidate.source_manifest_id AS source_manifest_id,
                CASE
                    WHEN active_edge.active_from_block_number IS NULL THEN ti.active_from_block_number
                    WHEN ti.active_from_block_number IS NULL THEN active_edge.active_from_block_number
                    ELSE GREATEST(active_edge.active_from_block_number, ti.active_from_block_number)
                END AS active_from_block_number,
                CASE
                    WHEN active_edge.active_to_block_number IS NULL THEN ti.active_to_block_number
                    WHEN ti.active_to_block_number IS NULL THEN active_edge.active_to_block_number
                    ELSE LEAST(active_edge.active_to_block_number, ti.active_to_block_number)
                END AS active_to_block_number
            FROM target_instances ti
            JOIN resolver_edge_sources candidate
              ON candidate.chain = ti.chain
            JOIN LATERAL (
                SELECT de.active_from_block_number, de.active_to_block_number
                FROM discovery_edges de
                WHERE de.chain_id = ti.chain
                  AND de.to_contract_instance_id = ti.contract_instance_id
                  AND de.source_manifest_id = candidate.edge_source_manifest_id
                  AND de.deactivated_at IS NULL
                  AND de.edge_kind = 'resolver'
                  AND (
                      de.active_from_block_number IS NULL
                      OR ti.active_to_block_number IS NULL
                      OR de.active_from_block_number <= ti.active_to_block_number
                  )
                  AND (
                      ti.active_from_block_number IS NULL
                      OR de.active_to_block_number IS NULL
                      OR ti.active_from_block_number <= de.active_to_block_number
                  )
                LIMIT 1
            ) active_edge ON TRUE
        ),
        discovery_scoped AS (
            SELECT
                chain,
                source_family,
                address,
                contract_instance_id,
                source,
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
                source,
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
                source,
                source_manifest_id,
                active_from_block_number,
                active_to_block_number
            FROM resolver_discovery_scoped
        )
        SELECT
            chain,
            source_family,
            address,
            contract_instance_id,
            source,
            source_manifest_id,
            active_from_block_number,
            active_to_block_number
        FROM manifest_declared

        UNION

        SELECT
            chain,
            source_family,
            address,
            contract_instance_id,
            source,
            source_manifest_id,
            active_from_block_number,
            active_to_block_number
        FROM discovery_scoped

        ORDER BY 1, 2, 3, 5, 6, 4
        "#,
    )
    .bind(&chains)
    .bind(&addresses)
    .bind(source_family)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load scoped resolver-profile targets for {source_family}")
    })?;

    rows.into_iter()
        .map(|row| {
            let source = row
                .try_get::<String, _>("source")
                .context("failed to read resolver-profile target source")?;
            Ok(WatchedContract {
                chain: row
                    .try_get("chain")
                    .context("failed to read resolver-profile target chain")?,
                source_family: row
                    .try_get("source_family")
                    .context("failed to read resolver-profile target source_family")?,
                address: normalize_address(
                    &row.try_get::<String, _>("address")
                        .context("failed to read resolver-profile target address")?,
                ),
                contract_instance_id: row
                    .try_get("contract_instance_id")
                    .context("failed to read resolver-profile target contract_instance_id")?,
                source: WatchedContractSource::from_db_value(&source)?,
                source_manifest_id: row
                    .try_get("source_manifest_id")
                    .context("failed to read resolver-profile target source_manifest_id")?,
                active_from_block_number: row
                    .try_get("active_from_block_number")
                    .context("failed to read resolver-profile target active_from_block_number")?,
                active_to_block_number: row
                    .try_get("active_to_block_number")
                    .context("failed to read resolver-profile target active_to_block_number")?,
            })
        })
        .collect()
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ResolverProfileAdmissionConfig {
    pub(super) source_family: &'static str,
    pub(super) profile: &'static str,
    pub(super) fact_families: &'static [&'static str],
    pub(super) manifest_seed_basis: &'static str,
}

pub(super) fn derive_code_hash_resolver_profile_admissions(
    watched_contracts: &[WatchedContract],
    code_hash_observations: &[ManifestCodeHashObservation],
    resolver_seed_ids: &[Uuid],
    config: ResolverProfileAdmissionConfig,
) -> Vec<ResolverProfileAdmission> {
    let resolver_seed_ids = resolver_seed_ids.iter().copied().collect::<BTreeSet<_>>();
    let observed_code_hashes =
        latest_resolver_code_hashes_by_contract_id(code_hash_observations, config.source_family);
    let seed_code_hashes = resolver_seed_ids
        .iter()
        .filter_map(|contract_instance_id| {
            observed_code_hashes
                .get(contract_instance_id)
                .map(|code_hash| (*contract_instance_id, code_hash.clone()))
        })
        .collect::<Vec<_>>();

    let mut admissions = Vec::new();
    for watched_contract in watched_contracts
        .iter()
        .filter(|contract| contract.source_family == config.source_family)
    {
        let profile_match = classify_resolver_profile_match(
            watched_contract.contract_instance_id,
            &resolver_seed_ids,
            &seed_code_hashes,
            observed_code_hashes.get(&watched_contract.contract_instance_id),
            config.manifest_seed_basis,
        );

        for fact_family in config.fact_families {
            admissions.push(ResolverProfileAdmission {
                chain: watched_contract.chain.clone(),
                source_family: watched_contract.source_family.clone(),
                contract_instance_id: watched_contract.contract_instance_id,
                address: watched_contract.address.clone(),
                source: watched_contract.source,
                source_manifest_id: watched_contract.source_manifest_id,
                active_from_block_number: watched_contract.active_from_block_number,
                active_to_block_number: watched_contract.active_to_block_number,
                profile: config.profile.to_owned(),
                fact_family: (*fact_family).to_owned(),
                status: profile_match.status.clone(),
                admission_basis: profile_match.admission_basis.clone(),
                observed_code_hash: profile_match.observed_code_hash.clone(),
                matched_code_hash: profile_match.matched_code_hash.clone(),
                matched_contract_instance_id: profile_match.matched_contract_instance_id,
            });
        }
    }

    sort_resolver_profile_admissions(&mut admissions);
    admissions
}

pub(super) fn sort_resolver_profile_admissions(admissions: &mut [ResolverProfileAdmission]) {
    admissions.sort_by(|left, right| {
        (
            left.chain.as_str(),
            left.source_family.as_str(),
            left.address.as_str(),
            left.contract_instance_id,
            left.active_from_block_number,
            left.active_to_block_number,
            left.profile.as_str(),
            left.fact_family.as_str(),
        )
            .cmp(&(
                right.chain.as_str(),
                right.source_family.as_str(),
                right.address.as_str(),
                right.contract_instance_id,
                right.active_from_block_number,
                right.active_to_block_number,
                right.profile.as_str(),
                right.fact_family.as_str(),
            ))
    });
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResolverProfileMatch {
    status: String,
    admission_basis: String,
    observed_code_hash: Option<String>,
    matched_code_hash: Option<String>,
    matched_contract_instance_id: Option<Uuid>,
}

fn classify_resolver_profile_match(
    contract_instance_id: Uuid,
    resolver_seed_ids: &BTreeSet<Uuid>,
    seed_code_hashes: &[(Uuid, String)],
    observed_code_hash: Option<&String>,
    manifest_seed_basis: &str,
) -> ResolverProfileMatch {
    if resolver_seed_ids.contains(&contract_instance_id) {
        return ResolverProfileMatch {
            status: RESOLVER_PROFILE_STATUS_SUPPORTED.to_owned(),
            admission_basis: manifest_seed_basis.to_owned(),
            observed_code_hash: observed_code_hash.cloned(),
            matched_code_hash: observed_code_hash.cloned(),
            matched_contract_instance_id: Some(contract_instance_id),
        };
    }

    let Some(observed_code_hash) = observed_code_hash else {
        return ResolverProfileMatch {
            status: RESOLVER_PROFILE_STATUS_PENDING.to_owned(),
            admission_basis: RESOLVER_PROFILE_BASIS_CODE_HASH_PENDING.to_owned(),
            observed_code_hash: None,
            matched_code_hash: None,
            matched_contract_instance_id: None,
        };
    };

    if let Some((matched_contract_instance_id, matched_code_hash)) = seed_code_hashes
        .iter()
        .find(|(_, seed_code_hash)| seed_code_hash == observed_code_hash)
    {
        return ResolverProfileMatch {
            status: RESOLVER_PROFILE_STATUS_SUPPORTED.to_owned(),
            admission_basis: RESOLVER_PROFILE_BASIS_CODE_HASH_MATCH.to_owned(),
            observed_code_hash: Some(observed_code_hash.clone()),
            matched_code_hash: Some(matched_code_hash.clone()),
            matched_contract_instance_id: Some(*matched_contract_instance_id),
        };
    }

    ResolverProfileMatch {
        status: RESOLVER_PROFILE_STATUS_UNSUPPORTED.to_owned(),
        admission_basis: RESOLVER_PROFILE_BASIS_CODE_HASH_MISMATCH.to_owned(),
        observed_code_hash: Some(observed_code_hash.clone()),
        matched_code_hash: None,
        matched_contract_instance_id: None,
    }
}

pub(super) fn latest_resolver_code_hashes_by_contract_id(
    code_hash_observations: &[ManifestCodeHashObservation],
    source_family: &str,
) -> BTreeMap<Uuid, String> {
    let mut latest_observations = BTreeMap::<Uuid, &ManifestCodeHashObservation>::new();
    for observation in code_hash_observations
        .iter()
        .filter(|observation| observation.source_family == source_family)
    {
        latest_observations
            .entry(observation.contract_instance_id)
            .and_modify(|current| {
                if (
                    observation.block_number,
                    observation.block_hash.as_str(),
                    observation.code_hash.as_str(),
                ) > (
                    current.block_number,
                    current.block_hash.as_str(),
                    current.code_hash.as_str(),
                ) {
                    *current = observation;
                }
            })
            .or_insert(observation);
    }

    latest_observations
        .into_iter()
        .map(|(contract_instance_id, observation)| {
            (contract_instance_id, observation.code_hash.clone())
        })
        .collect()
}
