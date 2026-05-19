use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::{ResolverProfileAdmission, WatchedContract, WatchedContractSource, normalize_address};

use super::super::{
    drift::{
        load_manifest_code_hash_observations,
        load_manifest_code_hash_observations_for_watched_contracts,
    },
    types::ManifestCodeHashObservation,
    watched::load_watched_contracts_by_source_family,
};
use super::{
    RESOLVER_PROFILE_BASIS_CODE_HASH_MATCH, RESOLVER_PROFILE_BASIS_CODE_HASH_MISMATCH,
    RESOLVER_PROFILE_BASIS_CODE_HASH_PENDING, RESOLVER_PROFILE_STATUS_PENDING,
    RESOLVER_PROFILE_STATUS_SUPPORTED, RESOLVER_PROFILE_STATUS_UNSUPPORTED,
    latest_resolver_code_hashes_by_contract_id, sort_resolver_profile_admissions,
};

#[path = "ens_v1/profile_config.rs"]
mod profile_config;

use profile_config::{
    DEFAULT_PENDING_FACT_FAMILIES, ENS_V1_PUBLIC_RESOLVER_COMPATIBLE_PROFILE,
    ENS_V1_PUBLIC_RESOLVER_ROLE, ENS_V1_RESOLVER_PROFILE_CONFIGS, ENS_V1_RESOLVER_SOURCE_FAMILY,
    EnsV1ResolverProfileConfig, profile_config_for_role,
};

#[derive(Clone, Debug)]
struct EnsV1ResolverProfileSeed {
    contract: WatchedContract,
    config: &'static EnsV1ResolverProfileConfig,
}

pub async fn load_ens_v1_public_resolver_profile_admissions(
    pool: &PgPool,
) -> Result<Vec<ResolverProfileAdmission>> {
    let seed_contracts = load_ens_v1_resolver_profile_seed_watched_contracts(pool).await?;
    let watched_contracts =
        load_watched_contracts_by_source_family(pool, ENS_V1_RESOLVER_SOURCE_FAMILY).await?;
    let code_hash_observations = load_manifest_code_hash_observations(pool).await?;

    Ok(derive_ens_v1_resolver_profile_admissions(
        &watched_contracts,
        &code_hash_observations,
        &seed_contracts,
    ))
}

pub async fn load_ens_v1_public_resolver_profile_admissions_for_targets(
    pool: &PgPool,
    targets: &[(String, String)],
) -> Result<Vec<ResolverProfileAdmission>> {
    if targets.is_empty() {
        return Ok(Vec::new());
    }

    let seed_contracts = load_ens_v1_resolver_profile_seed_watched_contracts(pool).await?;
    let target_contracts = super::load_resolver_profile_target_watched_contracts(
        pool,
        ENS_V1_RESOLVER_SOURCE_FAMILY,
        targets,
    )
    .await?;
    let mut code_hash_targets = seed_contracts
        .iter()
        .map(|seed| seed.contract.clone())
        .collect::<Vec<_>>();
    code_hash_targets.extend(target_contracts.clone());
    let code_hash_observations =
        load_manifest_code_hash_observations_for_watched_contracts(pool, &code_hash_targets)
            .await?;

    Ok(derive_ens_v1_resolver_profile_admissions(
        &target_contracts,
        &code_hash_observations,
        &seed_contracts,
    ))
}

pub fn derive_ens_v1_public_resolver_profile_admissions(
    watched_contracts: &[WatchedContract],
    code_hash_observations: &[ManifestCodeHashObservation],
    public_resolver_seed_ids: &[Uuid],
) -> Vec<ResolverProfileAdmission> {
    let public_resolver_seed_ids = public_resolver_seed_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let public_resolver_config =
        profile_config_for_role(ENS_V1_PUBLIC_RESOLVER_ROLE).expect("latest profile must exist");
    let seed_contracts = watched_contracts
        .iter()
        .filter(|contract| public_resolver_seed_ids.contains(&contract.contract_instance_id))
        .cloned()
        .map(|contract| EnsV1ResolverProfileSeed {
            contract,
            config: public_resolver_config,
        })
        .collect::<Vec<_>>();

    derive_ens_v1_resolver_profile_admissions(
        watched_contracts,
        code_hash_observations,
        &seed_contracts,
    )
}

async fn load_ens_v1_resolver_profile_seed_watched_contracts(
    pool: &PgPool,
) -> Result<Vec<EnsV1ResolverProfileSeed>> {
    let roles = ENS_V1_RESOLVER_PROFILE_CONFIGS
        .iter()
        .map(|config| config.role.to_owned())
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT
            mv.chain AS chain,
            mv.source_family AS source_family,
            cia.address AS address,
            mci.contract_instance_id AS contract_instance_id,
            mv.manifest_id AS source_manifest_id,
            mci.role AS role,
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
          AND mv.namespace = 'ens'
          AND mv.source_family = $1
          AND mci.declaration_kind = 'contract'
          AND mci.role = ANY($2::TEXT[])
        ORDER BY mv.chain, cia.address, mci.contract_instance_id
        "#,
    )
    .bind(ENS_V1_RESOLVER_SOURCE_FAMILY)
    .bind(&roles)
    .fetch_all(pool)
    .await
    .context("failed to load ENSv1 resolver profile seed contracts")?;

    rows.into_iter()
        .map(|row| {
            let role = row
                .try_get::<String, _>("role")
                .context("failed to read ENSv1 resolver seed role")?;
            let config = profile_config_for_role(&role)
                .with_context(|| format!("unsupported ENSv1 resolver profile role {role}"))?;
            let address = row
                .try_get::<String, _>("address")
                .context("failed to read ENSv1 resolver seed address")?;
            Ok(EnsV1ResolverProfileSeed {
                contract: WatchedContract {
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
                },
                config,
            })
        })
        .collect()
}

fn derive_ens_v1_resolver_profile_admissions(
    watched_contracts: &[WatchedContract],
    code_hash_observations: &[ManifestCodeHashObservation],
    seed_contracts: &[EnsV1ResolverProfileSeed],
) -> Vec<ResolverProfileAdmission> {
    let observed_code_hashes = latest_resolver_code_hashes_by_contract_id(
        code_hash_observations,
        ENS_V1_RESOLVER_SOURCE_FAMILY,
    );
    let seed_contracts_by_id = seed_contracts
        .iter()
        .map(|seed| (seed.contract.contract_instance_id, seed))
        .collect::<BTreeMap<_, _>>();
    let seed_code_hashes = seed_contracts
        .iter()
        .filter_map(|seed| {
            observed_code_hashes
                .get(&seed.contract.contract_instance_id)
                .map(|code_hash| SeedCodeHash {
                    contract_instance_id: seed.contract.contract_instance_id,
                    code_hash: code_hash.clone(),
                    config: seed.config,
                })
        })
        .collect::<Vec<_>>();

    let mut admissions = Vec::new();
    for watched_contract in watched_contracts
        .iter()
        .filter(|contract| contract.source_family == ENS_V1_RESOLVER_SOURCE_FAMILY)
    {
        let profile_match = classify_ens_v1_resolver_profile_match(
            watched_contract.contract_instance_id,
            &seed_contracts_by_id,
            &seed_code_hashes,
            observed_code_hashes.get(&watched_contract.contract_instance_id),
        );
        push_profile_admissions(watched_contract, profile_match, &mut admissions);
    }

    sort_resolver_profile_admissions(&mut admissions);
    admissions
}

#[derive(Clone, Debug)]
struct SeedCodeHash {
    contract_instance_id: Uuid,
    code_hash: String,
    config: &'static EnsV1ResolverProfileConfig,
}

#[derive(Clone, Debug)]
struct EnsV1ResolverProfileMatch {
    config: Option<&'static EnsV1ResolverProfileConfig>,
    status: String,
    admission_basis: String,
    observed_code_hash: Option<String>,
    matched_code_hash: Option<String>,
    matched_contract_instance_id: Option<Uuid>,
}

fn classify_ens_v1_resolver_profile_match(
    contract_instance_id: Uuid,
    seed_contracts_by_id: &BTreeMap<Uuid, &EnsV1ResolverProfileSeed>,
    seed_code_hashes: &[SeedCodeHash],
    observed_code_hash: Option<&String>,
) -> EnsV1ResolverProfileMatch {
    if let Some(seed) = seed_contracts_by_id.get(&contract_instance_id) {
        return EnsV1ResolverProfileMatch {
            config: Some(seed.config),
            status: RESOLVER_PROFILE_STATUS_SUPPORTED.to_owned(),
            admission_basis: seed.config.manifest_seed_basis.to_owned(),
            observed_code_hash: observed_code_hash.cloned(),
            matched_code_hash: observed_code_hash.cloned(),
            matched_contract_instance_id: Some(contract_instance_id),
        };
    }

    let Some(observed_code_hash) = observed_code_hash else {
        return EnsV1ResolverProfileMatch {
            config: None,
            status: RESOLVER_PROFILE_STATUS_PENDING.to_owned(),
            admission_basis: RESOLVER_PROFILE_BASIS_CODE_HASH_PENDING.to_owned(),
            observed_code_hash: None,
            matched_code_hash: None,
            matched_contract_instance_id: None,
        };
    };

    if let Some(seed) = seed_code_hashes
        .iter()
        .find(|seed| seed.code_hash == *observed_code_hash)
    {
        return EnsV1ResolverProfileMatch {
            config: Some(seed.config),
            status: RESOLVER_PROFILE_STATUS_SUPPORTED.to_owned(),
            admission_basis: RESOLVER_PROFILE_BASIS_CODE_HASH_MATCH.to_owned(),
            observed_code_hash: Some(observed_code_hash.clone()),
            matched_code_hash: Some(seed.code_hash.clone()),
            matched_contract_instance_id: Some(seed.contract_instance_id),
        };
    }

    EnsV1ResolverProfileMatch {
        config: None,
        status: RESOLVER_PROFILE_STATUS_UNSUPPORTED.to_owned(),
        admission_basis: RESOLVER_PROFILE_BASIS_CODE_HASH_MISMATCH.to_owned(),
        observed_code_hash: Some(observed_code_hash.clone()),
        matched_code_hash: None,
        matched_contract_instance_id: None,
    }
}

fn push_profile_admissions(
    watched_contract: &WatchedContract,
    profile_match: EnsV1ResolverProfileMatch,
    admissions: &mut Vec<ResolverProfileAdmission>,
) {
    if let Some(config) = profile_match.config {
        for fact in config.fact_families {
            push_admission(
                watched_contract,
                config.profile,
                fact.fact_family,
                fact.status,
                &profile_match,
                admissions,
            );
        }
        return;
    }

    for fact_family in DEFAULT_PENDING_FACT_FAMILIES {
        push_admission(
            watched_contract,
            ENS_V1_PUBLIC_RESOLVER_COMPATIBLE_PROFILE,
            fact_family,
            &profile_match.status,
            &profile_match,
            admissions,
        );
    }
}

fn push_admission(
    watched_contract: &WatchedContract,
    profile: &str,
    fact_family: &str,
    status: &str,
    profile_match: &EnsV1ResolverProfileMatch,
    admissions: &mut Vec<ResolverProfileAdmission>,
) {
    admissions.push(ResolverProfileAdmission {
        chain: watched_contract.chain.clone(),
        source_family: watched_contract.source_family.clone(),
        contract_instance_id: watched_contract.contract_instance_id,
        address: watched_contract.address.clone(),
        source: watched_contract.source,
        source_manifest_id: watched_contract.source_manifest_id,
        active_from_block_number: watched_contract.active_from_block_number,
        active_to_block_number: watched_contract.active_to_block_number,
        profile: profile.to_owned(),
        fact_family: fact_family.to_owned(),
        status: status.to_owned(),
        admission_basis: profile_match.admission_basis.clone(),
        observed_code_hash: profile_match.observed_code_hash.clone(),
        matched_code_hash: profile_match.matched_code_hash.clone(),
        matched_contract_instance_id: profile_match.matched_contract_instance_id,
    });
}
