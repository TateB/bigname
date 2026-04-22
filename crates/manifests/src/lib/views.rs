use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::{
    ActiveManifestVersion, CapabilityFlag, CapabilitySupportStatus,
    MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE, MANIFEST_PROXY_IMPLEMENTATION_EDGE_KIND,
    NamespaceManifestSnapshot, ResolverProfileAdmission, WatchedBackfillTarget, WatchedChainPlan,
    WatchedContract, WatchedContractChainSummary, WatchedContractSource, WatchedContractSummary,
    WatchedSourceSelector, WatchedSourceSelectorPlan, WatchedTargetIdentity, normalize_address,
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

#[derive(Clone, Debug, PartialEq)]
pub struct ManifestDriftInputs {
    pub active_manifests: Vec<ManifestDriftActiveManifest>,
    pub declared_contracts: Vec<ManifestDeclaredContractDriftInput>,
    pub proxy_implementation_edges: Vec<ManifestProxyImplementationDriftEdge>,
    pub code_hash_observations: Vec<ManifestCodeHashObservation>,
    pub normalized_manifest_events: Vec<ManifestNormalizedEventInput>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ManifestDriftActiveManifest {
    pub manifest_id: i64,
    pub manifest_version: u64,
    pub namespace: String,
    pub source_family: String,
    pub chain: String,
    pub deployment_epoch: String,
    pub normalizer_version: String,
    pub file_path: String,
    pub manifest_payload: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestDeclaredContractDriftInput {
    pub manifest_id: i64,
    pub manifest_version: u64,
    pub namespace: String,
    pub source_family: String,
    pub chain: String,
    pub deployment_epoch: String,
    pub declaration_kind: String,
    pub declaration_name: String,
    pub contract_instance_id: Uuid,
    pub declared_address: String,
    pub code_hash: Option<String>,
    pub abi_ref: Option<String>,
    pub role: Option<String>,
    pub proxy_kind: Option<String>,
    pub implementation_contract_instance_id: Option<Uuid>,
    pub declared_implementation_address: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ManifestProxyImplementationDriftEdge {
    pub discovery_edge_id: i64,
    pub source_manifest_id: i64,
    pub manifest_version: u64,
    pub namespace: String,
    pub source_family: String,
    pub chain: String,
    pub proxy_contract_instance_id: Uuid,
    pub proxy_address: Option<String>,
    pub implementation_contract_instance_id: Uuid,
    pub implementation_address: Option<String>,
    pub declaration_name: Option<String>,
    pub role: Option<String>,
    pub proxy_kind: Option<String>,
    pub admission: String,
    pub active_from_block_number: Option<i64>,
    pub active_to_block_number: Option<i64>,
    pub provenance: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestCodeHashObservation {
    pub chain: String,
    pub source_family: String,
    pub contract_instance_id: Uuid,
    pub address: String,
    pub source: WatchedContractSource,
    pub source_manifest_id: Option<i64>,
    pub block_hash: String,
    pub block_number: i64,
    pub code_hash: String,
    pub code_byte_length: i64,
    pub canonicality_state: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ManifestNormalizedEventInput {
    pub normalized_event_id: i64,
    pub event_identity: String,
    pub namespace: String,
    pub logical_name_id: Option<String>,
    pub resource_id: Option<Uuid>,
    pub event_kind: String,
    pub source_family: String,
    pub manifest_version: u64,
    pub source_manifest_id: Option<i64>,
    pub chain_id: Option<String>,
    pub block_number: Option<i64>,
    pub block_hash: Option<String>,
    pub transaction_hash: Option<String>,
    pub log_index: Option<i64>,
    pub raw_fact_ref: Value,
    pub derivation_kind: String,
    pub canonicality_state: String,
    pub before_state: Value,
    pub after_state: Value,
}

pub async fn load_manifest_drift_inputs(pool: &PgPool) -> Result<ManifestDriftInputs> {
    Ok(ManifestDriftInputs {
        active_manifests: load_manifest_drift_active_manifests(pool).await?,
        declared_contracts: load_manifest_declared_contract_drift_inputs(pool).await?,
        proxy_implementation_edges: load_manifest_proxy_implementation_drift_edges(pool).await?,
        code_hash_observations: load_manifest_code_hash_observations(pool).await?,
        normalized_manifest_events: load_manifest_normalized_event_inputs(pool).await?,
    })
}

pub async fn load_manifest_drift_active_manifests(
    pool: &PgPool,
) -> Result<Vec<ManifestDriftActiveManifest>> {
    let rows = sqlx::query(
        r#"
        SELECT
            manifest_id,
            manifest_version,
            namespace,
            source_family,
            chain,
            deployment_epoch,
            normalizer_version,
            file_path,
            manifest_payload
        FROM manifest_versions
        WHERE rollout_status = 'active'
        ORDER BY namespace, source_family, chain, deployment_epoch, manifest_version
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load active manifest drift inputs")?;

    rows.into_iter()
        .map(|row| {
            let manifest_version = row
                .try_get::<i64, _>("manifest_version")
                .context("failed to read manifest drift manifest_version")?;
            Ok(ManifestDriftActiveManifest {
                manifest_id: row
                    .try_get("manifest_id")
                    .context("failed to read manifest drift manifest_id")?,
                manifest_version: u64::try_from(manifest_version)
                    .context("manifest_version must be non-negative")?,
                namespace: row
                    .try_get("namespace")
                    .context("failed to read manifest drift namespace")?,
                source_family: row
                    .try_get("source_family")
                    .context("failed to read manifest drift source_family")?,
                chain: row
                    .try_get("chain")
                    .context("failed to read manifest drift chain")?,
                deployment_epoch: row
                    .try_get("deployment_epoch")
                    .context("failed to read manifest drift deployment_epoch")?,
                normalizer_version: row
                    .try_get("normalizer_version")
                    .context("failed to read manifest drift normalizer_version")?,
                file_path: row
                    .try_get("file_path")
                    .context("failed to read manifest drift file_path")?,
                manifest_payload: row
                    .try_get("manifest_payload")
                    .context("failed to read manifest drift manifest_payload")?,
            })
        })
        .collect()
}

pub async fn load_manifest_declared_contract_drift_inputs(
    pool: &PgPool,
) -> Result<Vec<ManifestDeclaredContractDriftInput>> {
    let rows = sqlx::query(
        r#"
        SELECT
            mv.manifest_id,
            mv.manifest_version,
            mv.namespace,
            mv.source_family,
            mv.chain,
            mv.deployment_epoch,
            mci.declaration_kind,
            mci.declaration_name,
            mci.contract_instance_id,
            mci.declared_address,
            mci.code_hash,
            mci.abi_ref,
            mci.role,
            mci.proxy_kind,
            mci.implementation_contract_instance_id,
            mci.declared_implementation_address
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
        WHERE mv.rollout_status = 'active'
        ORDER BY
            mv.namespace,
            mv.source_family,
            mv.chain,
            mv.deployment_epoch,
            mv.manifest_version,
            mci.declaration_kind,
            mci.declaration_name
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load manifest declared contract drift inputs")?;

    rows.into_iter()
        .map(|row| {
            let manifest_version = row
                .try_get::<i64, _>("manifest_version")
                .context("failed to read declared contract manifest_version")?;
            let declared_address = row
                .try_get::<String, _>("declared_address")
                .context("failed to read declared contract address")?;
            let declared_implementation_address = row
                .try_get::<Option<String>, _>("declared_implementation_address")
                .context("failed to read declared implementation address")?
                .map(|address| normalize_address(&address));
            Ok(ManifestDeclaredContractDriftInput {
                manifest_id: row
                    .try_get("manifest_id")
                    .context("failed to read declared contract manifest_id")?,
                manifest_version: u64::try_from(manifest_version)
                    .context("manifest_version must be non-negative")?,
                namespace: row
                    .try_get("namespace")
                    .context("failed to read declared contract namespace")?,
                source_family: row
                    .try_get("source_family")
                    .context("failed to read declared contract source_family")?,
                chain: row
                    .try_get("chain")
                    .context("failed to read declared contract chain")?,
                deployment_epoch: row
                    .try_get("deployment_epoch")
                    .context("failed to read declared contract deployment_epoch")?,
                declaration_kind: row
                    .try_get("declaration_kind")
                    .context("failed to read declaration_kind")?,
                declaration_name: row
                    .try_get("declaration_name")
                    .context("failed to read declaration_name")?,
                contract_instance_id: row
                    .try_get("contract_instance_id")
                    .context("failed to read declared contract_instance_id")?,
                declared_address: normalize_address(&declared_address),
                code_hash: row
                    .try_get("code_hash")
                    .context("failed to read code_hash")?,
                abi_ref: row.try_get("abi_ref").context("failed to read abi_ref")?,
                role: row.try_get("role").context("failed to read role")?,
                proxy_kind: row
                    .try_get("proxy_kind")
                    .context("failed to read proxy_kind")?,
                implementation_contract_instance_id: row
                    .try_get("implementation_contract_instance_id")
                    .context("failed to read implementation_contract_instance_id")?,
                declared_implementation_address,
            })
        })
        .collect()
}

pub async fn load_manifest_proxy_implementation_drift_edges(
    pool: &PgPool,
) -> Result<Vec<ManifestProxyImplementationDriftEdge>> {
    let rows = sqlx::query(
        r#"
        SELECT
            de.discovery_edge_id,
            de.source_manifest_id,
            mv.manifest_version,
            mv.namespace,
            mv.source_family,
            de.chain_id,
            de.from_contract_instance_id AS proxy_contract_instance_id,
            proxy_address.address AS proxy_address,
            de.to_contract_instance_id AS implementation_contract_instance_id,
            implementation_address.address AS implementation_address,
            mci.declaration_name,
            mci.role,
            mci.proxy_kind,
            de.admission,
            de.active_from_block_number,
            de.active_to_block_number,
            de.provenance
        FROM discovery_edges de
        JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
        LEFT JOIN contract_instance_addresses proxy_address
          ON proxy_address.contract_instance_id = de.from_contract_instance_id
         AND proxy_address.deactivated_at IS NULL
        LEFT JOIN contract_instance_addresses implementation_address
          ON implementation_address.contract_instance_id = de.to_contract_instance_id
         AND implementation_address.deactivated_at IS NULL
        LEFT JOIN manifest_contract_instances mci
          ON mci.manifest_id = mv.manifest_id
         AND mci.contract_instance_id = de.from_contract_instance_id
         AND mci.implementation_contract_instance_id = de.to_contract_instance_id
        WHERE mv.rollout_status = 'active'
          AND de.deactivated_at IS NULL
          AND de.edge_kind = $1
          AND de.discovery_source = $2
        ORDER BY mv.namespace, mv.source_family, de.chain_id, proxy_address.address, implementation_address.address
        "#,
    )
    .bind(MANIFEST_PROXY_IMPLEMENTATION_EDGE_KIND)
    .bind(MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE)
    .fetch_all(pool)
    .await
    .context("failed to load manifest proxy implementation drift edges")?;

    rows.into_iter()
        .map(|row| {
            let manifest_version = row
                .try_get::<i64, _>("manifest_version")
                .context("failed to read proxy edge manifest_version")?;
            let proxy_address = row
                .try_get::<Option<String>, _>("proxy_address")
                .context("failed to read proxy edge proxy_address")?
                .map(|address| normalize_address(&address));
            let implementation_address = row
                .try_get::<Option<String>, _>("implementation_address")
                .context("failed to read proxy edge implementation_address")?
                .map(|address| normalize_address(&address));
            Ok(ManifestProxyImplementationDriftEdge {
                discovery_edge_id: row
                    .try_get("discovery_edge_id")
                    .context("failed to read proxy edge discovery_edge_id")?,
                source_manifest_id: row
                    .try_get("source_manifest_id")
                    .context("failed to read proxy edge source_manifest_id")?,
                manifest_version: u64::try_from(manifest_version)
                    .context("manifest_version must be non-negative")?,
                namespace: row
                    .try_get("namespace")
                    .context("failed to read proxy edge namespace")?,
                source_family: row
                    .try_get("source_family")
                    .context("failed to read proxy edge source_family")?,
                chain: row
                    .try_get("chain_id")
                    .context("failed to read proxy edge chain_id")?,
                proxy_contract_instance_id: row
                    .try_get("proxy_contract_instance_id")
                    .context("failed to read proxy_contract_instance_id")?,
                proxy_address,
                implementation_contract_instance_id: row
                    .try_get("implementation_contract_instance_id")
                    .context("failed to read implementation_contract_instance_id")?,
                implementation_address,
                declaration_name: row
                    .try_get("declaration_name")
                    .context("failed to read proxy edge declaration_name")?,
                role: row
                    .try_get("role")
                    .context("failed to read proxy edge role")?,
                proxy_kind: row
                    .try_get("proxy_kind")
                    .context("failed to read proxy edge proxy_kind")?,
                admission: row
                    .try_get("admission")
                    .context("failed to read proxy edge admission")?,
                active_from_block_number: row
                    .try_get("active_from_block_number")
                    .context("failed to read proxy edge active_from_block_number")?,
                active_to_block_number: row
                    .try_get("active_to_block_number")
                    .context("failed to read proxy edge active_to_block_number")?,
                provenance: row
                    .try_get("provenance")
                    .context("failed to read proxy edge provenance")?,
            })
        })
        .collect()
}

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
        SELECT DISTINCT ON (
            active_targets.chain,
            active_targets.source_family,
            active_targets.contract_instance_id,
            active_targets.address,
            active_targets.source,
            active_targets.source_manifest_id
        )
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
        JOIN raw_code_hashes
          ON raw_code_hashes.chain_id = active_targets.chain
         AND raw_code_hashes.contract_address = active_targets.address
        WHERE raw_code_hashes.canonicality_state <> 'orphaned'
        ORDER BY
            active_targets.chain,
            active_targets.source_family,
            active_targets.contract_instance_id,
            active_targets.address,
            active_targets.source,
            active_targets.source_manifest_id,
            raw_code_hashes.block_number DESC,
            CASE raw_code_hashes.canonicality_state
                WHEN 'finalized' THEN 4
                WHEN 'safe' THEN 3
                WHEN 'canonical' THEN 2
                WHEN 'observed' THEN 1
                ELSE 0
            END DESC,
            raw_code_hashes.raw_code_hash_id DESC
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load manifest code-hash observations")?;

    rows.into_iter()
        .map(|row| {
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
        })
        .collect()
}

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

pub async fn load_manifest_normalized_event_inputs(
    pool: &PgPool,
) -> Result<Vec<ManifestNormalizedEventInput>> {
    let rows = sqlx::query(
        r#"
        SELECT
            normalized_event_id,
            event_identity,
            namespace,
            logical_name_id,
            resource_id,
            event_kind,
            source_family,
            manifest_version,
            source_manifest_id,
            chain_id,
            block_number,
            block_hash,
            transaction_hash,
            log_index,
            raw_fact_ref,
            derivation_kind,
            canonicality_state::TEXT AS canonicality_state,
            before_state,
            after_state
        FROM normalized_events
        WHERE event_kind IN (
            'SourceManifestUpdated',
            'ProxyImplementationChanged',
            'CapabilityChanged'
        )
          AND canonicality_state <> 'orphaned'
        ORDER BY namespace, source_family, manifest_version, event_kind, normalized_event_id
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load manifest normalized-event inputs")?;

    rows.into_iter()
        .map(|row| {
            let manifest_version = row
                .try_get::<i64, _>("manifest_version")
                .context("failed to read manifest event manifest_version")?;
            Ok(ManifestNormalizedEventInput {
                normalized_event_id: row
                    .try_get("normalized_event_id")
                    .context("failed to read normalized_event_id")?,
                event_identity: row
                    .try_get("event_identity")
                    .context("failed to read event_identity")?,
                namespace: row
                    .try_get("namespace")
                    .context("failed to read manifest event namespace")?,
                logical_name_id: row
                    .try_get("logical_name_id")
                    .context("failed to read logical_name_id")?,
                resource_id: row
                    .try_get("resource_id")
                    .context("failed to read resource_id")?,
                event_kind: row
                    .try_get("event_kind")
                    .context("failed to read event_kind")?,
                source_family: row
                    .try_get("source_family")
                    .context("failed to read manifest event source_family")?,
                manifest_version: u64::try_from(manifest_version)
                    .context("manifest_version must be non-negative")?,
                source_manifest_id: row
                    .try_get("source_manifest_id")
                    .context("failed to read source_manifest_id")?,
                chain_id: row.try_get("chain_id").context("failed to read chain_id")?,
                block_number: row
                    .try_get("block_number")
                    .context("failed to read block_number")?,
                block_hash: row
                    .try_get("block_hash")
                    .context("failed to read block_hash")?,
                transaction_hash: row
                    .try_get("transaction_hash")
                    .context("failed to read transaction_hash")?,
                log_index: row
                    .try_get("log_index")
                    .context("failed to read log_index")?,
                raw_fact_ref: row
                    .try_get("raw_fact_ref")
                    .context("failed to read raw_fact_ref")?,
                derivation_kind: row
                    .try_get("derivation_kind")
                    .context("failed to read derivation_kind")?,
                canonicality_state: row
                    .try_get("canonicality_state")
                    .context("failed to read manifest event canonicality_state")?,
                before_state: row
                    .try_get("before_state")
                    .context("failed to read before_state")?,
                after_state: row
                    .try_get("after_state")
                    .context("failed to read after_state")?,
            })
        })
        .collect()
}
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
                cia.active_from_block_number AS active_from_block_number,
                cia.active_to_block_number AS active_to_block_number
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
        ORDER BY chain, source_family, address, source, source_manifest_id, contract_instance_id
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load watched contracts")?;

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

pub fn summarize_watched_contracts(
    watched_contracts: &[WatchedContract],
) -> WatchedContractSummary {
    let mut unique_contracts = HashSet::new();
    let mut chains = BTreeMap::<String, WatchedContractChainSummary>::new();
    let mut manifest_root_count = 0;
    let mut manifest_contract_count = 0;
    let mut discovery_edge_count = 0;

    for watched_contract in watched_contracts {
        unique_contracts.insert((
            watched_contract.chain.clone(),
            watched_contract.address.clone(),
        ));

        let chain_summary = chains
            .entry(watched_contract.chain.clone())
            .or_insert_with(|| WatchedContractChainSummary {
                chain: watched_contract.chain.clone(),
                unique_contract_count: 0,
                manifest_root_count: 0,
                manifest_contract_count: 0,
                discovery_edge_count: 0,
            });

        match watched_contract.source {
            WatchedContractSource::ManifestRoot => {
                manifest_root_count += 1;
                chain_summary.manifest_root_count += 1;
            }
            WatchedContractSource::ManifestContract => {
                manifest_contract_count += 1;
                chain_summary.manifest_contract_count += 1;
            }
            WatchedContractSource::DiscoveryEdge => {
                discovery_edge_count += 1;
                chain_summary.discovery_edge_count += 1;
            }
        }
    }

    for chain_summary in chains.values_mut() {
        chain_summary.unique_contract_count = watched_contracts
            .iter()
            .filter(|contract| contract.chain == chain_summary.chain)
            .map(|contract| contract.address.as_str())
            .collect::<HashSet<_>>()
            .len();
    }

    WatchedContractSummary {
        unique_contract_count: unique_contracts.len(),
        source_entry_count: watched_contracts.len(),
        manifest_root_count,
        manifest_contract_count,
        discovery_edge_count,
        chains: chains.into_values().collect(),
    }
}

pub fn plan_watched_contracts(watched_contracts: &[WatchedContract]) -> Vec<WatchedChainPlan> {
    let mut plans = BTreeMap::<String, WatchedChainPlan>::new();

    for watched_contract in watched_contracts {
        let plan = plans
            .entry(watched_contract.chain.clone())
            .or_insert_with(|| WatchedChainPlan {
                chain: watched_contract.chain.clone(),
                addresses: Vec::new(),
                manifest_root_entry_count: 0,
                manifest_contract_entry_count: 0,
                discovery_edge_entry_count: 0,
            });

        if !plan.addresses.contains(&watched_contract.address) {
            plan.addresses.push(watched_contract.address.clone());
        }

        match watched_contract.source {
            WatchedContractSource::ManifestRoot => plan.manifest_root_entry_count += 1,
            WatchedContractSource::ManifestContract => plan.manifest_contract_entry_count += 1,
            WatchedContractSource::DiscoveryEdge => plan.discovery_edge_entry_count += 1,
        }
    }

    let mut plans = plans.into_values().collect::<Vec<_>>();
    for plan in &mut plans {
        plan.addresses.sort();
    }
    plans
}

pub fn resolve_watched_source_selector(
    watched_contracts: &[WatchedContract],
    chain: &str,
    selector: WatchedSourceSelector,
    range_start_block_number: i64,
    range_end_block_number: i64,
) -> Result<WatchedSourceSelectorPlan> {
    if range_start_block_number < 0 {
        bail!("watched source selector range start must be non-negative");
    }
    if range_end_block_number < 0 {
        bail!("watched source selector range end must be non-negative");
    }
    if range_start_block_number > range_end_block_number {
        bail!(
            "watched source selector range start {range_start_block_number} is after end {range_end_block_number}"
        );
    }

    let selector_kind = selector.kind();
    let source_family = match &selector {
        WatchedSourceSelector::SourceFamily(source_family) => Some(source_family.clone()),
        _ => None,
    };
    let requested_watched_targets = normalized_requested_targets(&selector)?;
    let requested_target_ids = requested_watched_targets
        .iter()
        .map(|target| target.contract_instance_id)
        .collect::<BTreeSet<_>>();

    let selected_contracts = watched_contracts
        .iter()
        .filter(|watched_contract| watched_contract.chain == chain)
        .filter(|watched_contract| {
            watched_contract_range_intersects(
                watched_contract,
                range_start_block_number,
                range_end_block_number,
            )
        })
        .filter(|watched_contract| match &selector {
            WatchedSourceSelector::WholeActiveWatchedChain => true,
            WatchedSourceSelector::SourceFamily(source_family) => {
                watched_contract.source_family == *source_family
            }
            WatchedSourceSelector::WatchedTargetSet(_) => {
                requested_target_ids.contains(&watched_contract.contract_instance_id)
            }
        })
        .cloned()
        .collect::<Vec<_>>();

    match &selector {
        WatchedSourceSelector::WholeActiveWatchedChain => {
            if selected_contracts.is_empty() {
                bail!(
                    "watched source selector whole_active_watched_chain found no active watched targets for chain {chain}"
                );
            }
        }
        WatchedSourceSelector::SourceFamily(source_family) => {
            if selected_contracts.is_empty() {
                bail!(
                    "watched source selector source_family {source_family} found no active watched targets for chain {chain}"
                );
            }
        }
        WatchedSourceSelector::WatchedTargetSet(_) => {
            if requested_watched_targets.is_empty() {
                bail!("watched_target_set selector must include at least one contract_instance_id");
            }

            let selected_target_ids = selected_contracts
                .iter()
                .map(|watched_contract| watched_contract.contract_instance_id)
                .collect::<BTreeSet<_>>();
            for requested_target in &requested_watched_targets {
                if !selected_target_ids.contains(&requested_target.contract_instance_id) {
                    bail!(
                        "watched target {} is not active for chain {chain} in the selected range",
                        requested_target.contract_instance_id
                    );
                }
            }
        }
    }

    let selected_targets = selected_backfill_targets(
        &selected_contracts,
        range_start_block_number,
        range_end_block_number,
    )?;
    let watched_chain_plan = plan_watched_contracts(&selected_contracts)
        .into_iter()
        .next()
        .unwrap_or_else(|| WatchedChainPlan {
            chain: chain.to_owned(),
            addresses: Vec::new(),
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 0,
            discovery_edge_entry_count: 0,
        });

    Ok(WatchedSourceSelectorPlan {
        chain: chain.to_owned(),
        selector_kind,
        source_family,
        requested_watched_targets,
        selected_targets,
        watched_chain_plan,
    })
}

pub fn plan_watched_contracts_for_source_family(
    watched_contracts: &[WatchedContract],
    chain: &str,
    source_family: &str,
    range_start_block_number: i64,
    range_end_block_number: i64,
) -> Result<WatchedChainPlan> {
    Ok(resolve_watched_source_selector(
        watched_contracts,
        chain,
        WatchedSourceSelector::SourceFamily(source_family.to_owned()),
        range_start_block_number,
        range_end_block_number,
    )?
    .watched_chain_plan)
}

fn normalized_requested_targets(
    selector: &WatchedSourceSelector,
) -> Result<Vec<WatchedTargetIdentity>> {
    let mut requested_watched_targets = match selector {
        WatchedSourceSelector::WatchedTargetSet(targets) => targets.clone(),
        _ => Vec::new(),
    };
    requested_watched_targets.sort();
    requested_watched_targets.dedup();
    Ok(requested_watched_targets)
}

fn watched_contract_range_intersects(
    watched_contract: &WatchedContract,
    range_start_block_number: i64,
    range_end_block_number: i64,
) -> bool {
    watched_contract_effective_range(
        watched_contract,
        range_start_block_number,
        range_end_block_number,
    )
    .is_some()
}

fn watched_contract_effective_range(
    watched_contract: &WatchedContract,
    range_start_block_number: i64,
    range_end_block_number: i64,
) -> Option<(i64, i64)> {
    let effective_from_block = watched_contract
        .active_from_block_number
        .map_or(range_start_block_number, |active_from| {
            active_from.max(range_start_block_number)
        });
    let effective_to_block = watched_contract
        .active_to_block_number
        .map_or(range_end_block_number, |active_to| {
            active_to.min(range_end_block_number)
        });

    (effective_from_block <= effective_to_block)
        .then_some((effective_from_block, effective_to_block))
}

fn selected_backfill_targets(
    watched_contracts: &[WatchedContract],
    range_start_block_number: i64,
    range_end_block_number: i64,
) -> Result<Vec<WatchedBackfillTarget>> {
    let mut addresses_by_identity = BTreeMap::<(String, uuid::Uuid), String>::new();
    let mut selected_targets = BTreeSet::<WatchedBackfillTarget>::new();

    for watched_contract in watched_contracts {
        let Some((effective_from_block, effective_to_block)) = watched_contract_effective_range(
            watched_contract,
            range_start_block_number,
            range_end_block_number,
        ) else {
            continue;
        };

        let target = WatchedBackfillTarget {
            source_family: watched_contract.source_family.clone(),
            contract_instance_id: watched_contract.contract_instance_id,
            address: watched_contract.address.clone(),
            effective_from_block,
            effective_to_block,
        };
        let identity = (target.source_family.clone(), target.contract_instance_id);
        if let Some(existing_address) = addresses_by_identity.get(&identity) {
            if existing_address != &target.address {
                bail!(
                    "source identity conflict for watched target {} in source family {}",
                    target.contract_instance_id,
                    target.source_family
                );
            }
        } else {
            addresses_by_identity.insert(identity, target.address.clone());
        }
        selected_targets.insert(target);
    }

    Ok(selected_targets.into_iter().collect())
}

pub async fn load_watched_contract_summary(pool: &PgPool) -> Result<WatchedContractSummary> {
    let watched_contracts = load_watched_contracts(pool).await?;
    Ok(summarize_watched_contracts(&watched_contracts))
}

pub async fn load_watched_chain_plan(pool: &PgPool) -> Result<Vec<WatchedChainPlan>> {
    let watched_contracts = load_watched_contracts(pool).await?;
    Ok(plan_watched_contracts(&watched_contracts))
}

pub async fn load_watched_source_selector_plan(
    pool: &PgPool,
    chain: &str,
    selector: WatchedSourceSelector,
    range_start_block_number: i64,
    range_end_block_number: i64,
) -> Result<WatchedSourceSelectorPlan> {
    let watched_contracts = load_watched_contracts(pool).await?;
    resolve_watched_source_selector(
        &watched_contracts,
        chain,
        selector,
        range_start_block_number,
        range_end_block_number,
    )
}

pub async fn load_active_manifests_for_namespace(
    pool: &PgPool,
    namespace: &str,
) -> Result<Vec<ActiveManifestVersion>> {
    let manifest_rows = sqlx::query(
        r#"
        SELECT manifest_id, manifest_version, source_family, chain, deployment_epoch, normalizer_version
        FROM manifest_versions
        WHERE rollout_status = 'active'
          AND namespace = $1
        ORDER BY source_family, chain, deployment_epoch, manifest_version
        "#,
    )
    .bind(namespace)
    .fetch_all(pool)
    .await
    .context("failed to load active manifests")?;

    let capability_rows = sqlx::query(
        r#"
        SELECT
            mv.manifest_id AS manifest_id,
            mcf.capability_name AS capability_name,
            mcf.status::TEXT AS status,
            mcf.notes AS notes
        FROM manifest_versions mv
        JOIN manifest_capability_flags mcf ON mcf.manifest_id = mv.manifest_id
        WHERE mv.rollout_status = 'active'
          AND mv.namespace = $1
        ORDER BY mv.source_family, mv.chain, mv.deployment_epoch, mv.manifest_version, mcf.capability_name
        "#,
    )
    .bind(namespace)
    .fetch_all(pool)
    .await
    .context("failed to load active manifest capability flags")?;

    let mut capability_flags_by_manifest_id: HashMap<i64, BTreeMap<String, CapabilityFlag>> =
        HashMap::new();
    for row in capability_rows {
        let manifest_id = row
            .try_get("manifest_id")
            .context("failed to read capability manifest_id")?;
        let capability_name = row
            .try_get::<String, _>("capability_name")
            .context("failed to read capability_name")?;
        let status = row
            .try_get::<String, _>("status")
            .context("failed to read capability status")?;
        let notes = row
            .try_get("notes")
            .context("failed to read capability notes")?;
        capability_flags_by_manifest_id
            .entry(manifest_id)
            .or_default()
            .insert(
                capability_name,
                CapabilityFlag {
                    status: CapabilitySupportStatus::from_db_value(&status)?,
                    notes,
                },
            );
    }

    manifest_rows
        .into_iter()
        .map(|row| {
            let manifest_id = row
                .try_get("manifest_id")
                .context("failed to read manifest_id from active manifest row")?;
            let manifest_version = row
                .try_get::<i64, _>("manifest_version")
                .context("failed to read manifest_version from active manifest row")?;
            Ok(ActiveManifestVersion {
                manifest_version: u64::try_from(manifest_version)
                    .context("manifest_version must be non-negative")?,
                source_family: row
                    .try_get("source_family")
                    .context("failed to read source_family from active manifest row")?,
                chain: row
                    .try_get("chain")
                    .context("failed to read chain from active manifest row")?,
                deployment_epoch: row
                    .try_get("deployment_epoch")
                    .context("failed to read deployment_epoch from active manifest row")?,
                normalizer_version: row
                    .try_get("normalizer_version")
                    .context("failed to read normalizer_version from active manifest row")?,
                capability_flags: capability_flags_by_manifest_id
                    .remove(&manifest_id)
                    .unwrap_or_default(),
            })
        })
        .collect()
}

pub async fn load_namespace_manifest_snapshot(
    pool: &PgPool,
    namespace: &str,
) -> Result<NamespaceManifestSnapshot> {
    let manifests = load_active_manifests_for_namespace(pool, namespace).await?;
    let last_updated = sqlx::query_scalar::<_, String>(
        r#"
        SELECT COALESCE(
            TO_CHAR(MAX(loaded_at AT TIME ZONE 'UTC'), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
        )
        FROM manifest_versions
        WHERE namespace = $1
        "#,
    )
    .bind(namespace)
    .fetch_one(pool)
    .await
    .context("failed to load namespace manifest freshness timestamp")?;

    Ok(NamespaceManifestSnapshot {
        manifests,
        last_updated,
    })
}
