use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::{ResolverProfileAdmission, WatchedContract};

use super::{
    drift::load_manifest_code_hash_observations, types::ManifestCodeHashObservation,
    watched::load_watched_contracts,
};

const ENS_V1_RESOLVER_SOURCE_FAMILY: &str = "ens_v1_resolver_l1";
const ENS_V1_PUBLIC_RESOLVER_ROLE: &str = "public_resolver";
const ENS_V1_PUBLIC_RESOLVER_COMPATIBLE_PROFILE: &str = "public_resolver_compatible";
const ENS_V1_PUBLIC_RESOLVER_PROFILE_FACT_FAMILIES: [&str; 3] = [
    "resolver_record",
    "resolver_record_version",
    "resolver_authorization",
];
const BASENAMES_BASE_RESOLVER_SOURCE_FAMILY: &str = "basenames_base_resolver";
const BASENAMES_L2_RESOLVER_ROLE: &str = "resolver";
const BASENAMES_L2_RESOLVER_COMPATIBLE_PROFILE: &str = "l2_resolver_compatible";
const BASENAMES_L2_RESOLVER_PROFILE_FACT_FAMILIES: [&str; 2] =
    ["resolver_record", "resolver_authorization"];
const RESOLVER_PROFILE_STATUS_PENDING: &str = "pending";
const RESOLVER_PROFILE_STATUS_SUPPORTED: &str = "supported";
const RESOLVER_PROFILE_STATUS_UNSUPPORTED: &str = "unsupported";
const RESOLVER_PROFILE_BASIS_MANIFEST_SEED: &str = "manifest_public_resolver_seed";
const RESOLVER_PROFILE_BASIS_BASENAMES_L2_RESOLVER_SEED: &str = "manifest_l2_resolver_seed";
const RESOLVER_PROFILE_BASIS_CODE_HASH_MATCH: &str = "code_hash_match";
const RESOLVER_PROFILE_BASIS_CODE_HASH_PENDING: &str = "code_hash_pending";
const RESOLVER_PROFILE_BASIS_CODE_HASH_MISMATCH: &str = "code_hash_mismatch";

pub async fn load_ens_v1_public_resolver_profile_admissions(
    pool: &PgPool,
) -> Result<Vec<ResolverProfileAdmission>> {
    let public_resolver_seed_ids = load_resolver_profile_seed_ids(
        pool,
        "ens",
        ENS_V1_RESOLVER_SOURCE_FAMILY,
        ENS_V1_PUBLIC_RESOLVER_ROLE,
        "ENSv1 PublicResolver",
    )
    .await?;
    let watched_contracts = load_watched_contracts(pool).await?;
    let code_hash_observations = load_manifest_code_hash_observations(pool).await?;

    Ok(derive_code_hash_resolver_profile_admissions(
        &watched_contracts,
        &code_hash_observations,
        &public_resolver_seed_ids,
        ResolverProfileAdmissionConfig {
            source_family: ENS_V1_RESOLVER_SOURCE_FAMILY,
            profile: ENS_V1_PUBLIC_RESOLVER_COMPATIBLE_PROFILE,
            fact_families: &ENS_V1_PUBLIC_RESOLVER_PROFILE_FACT_FAMILIES,
            manifest_seed_basis: RESOLVER_PROFILE_BASIS_MANIFEST_SEED,
        },
    ))
}

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
    let watched_contracts = load_watched_contracts(pool).await?;
    let code_hash_observations = load_manifest_code_hash_observations(pool).await?;

    Ok(derive_basenames_l2_resolver_profile_admissions(
        &watched_contracts,
        &code_hash_observations,
        &l2_resolver_seed_ids,
    ))
}

pub fn derive_ens_v1_public_resolver_profile_admissions(
    watched_contracts: &[WatchedContract],
    code_hash_observations: &[ManifestCodeHashObservation],
    public_resolver_seed_ids: &[Uuid],
) -> Vec<ResolverProfileAdmission> {
    derive_code_hash_resolver_profile_admissions(
        watched_contracts,
        code_hash_observations,
        public_resolver_seed_ids,
        ResolverProfileAdmissionConfig {
            source_family: ENS_V1_RESOLVER_SOURCE_FAMILY,
            profile: ENS_V1_PUBLIC_RESOLVER_COMPATIBLE_PROFILE,
            fact_families: &ENS_V1_PUBLIC_RESOLVER_PROFILE_FACT_FAMILIES,
            manifest_seed_basis: RESOLVER_PROFILE_BASIS_MANIFEST_SEED,
        },
    )
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

async fn load_resolver_profile_seed_ids(
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

#[derive(Clone, Copy, Debug)]
struct ResolverProfileAdmissionConfig {
    source_family: &'static str,
    profile: &'static str,
    fact_families: &'static [&'static str],
    manifest_seed_basis: &'static str,
}

fn derive_code_hash_resolver_profile_admissions(
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

    admissions.sort_by(|left, right| {
        (
            left.chain.as_str(),
            left.source_family.as_str(),
            left.address.as_str(),
            left.contract_instance_id,
            left.active_from_block_number,
            left.active_to_block_number,
            left.fact_family.as_str(),
        )
            .cmp(&(
                right.chain.as_str(),
                right.source_family.as_str(),
                right.address.as_str(),
                right.contract_instance_id,
                right.active_from_block_number,
                right.active_to_block_number,
                right.fact_family.as_str(),
            ))
    });
    admissions
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

fn latest_resolver_code_hashes_by_contract_id(
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
