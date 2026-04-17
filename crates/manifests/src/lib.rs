//! Repository manifest loading, persistence, and discovery admission.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};

/// Shared manifest-loader status for bootstrap logging and health reporting.
pub const fn bootstrap_status() -> &'static str {
    "manifest-loader-ready"
}

/// Parsed and validated repository manifest tree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestRepository {
    root: PathBuf,
    manifests: Vec<LoadedManifest>,
    summary: ManifestLoadSummary,
}

impl ManifestRepository {
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn manifests(&self) -> &[LoadedManifest] {
        &self.manifests
    }

    pub fn summary(&self) -> &ManifestLoadSummary {
        &self.summary
    }
}

/// One manifest file plus the validated repository-relative metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedManifest {
    pub path: PathBuf,
    pub relative_path: PathBuf,
    pub version_tag: String,
    pub manifest: SourceManifest,
}

/// Summary used by binaries that only need startup status and counts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestLoadSummary {
    pub root: PathBuf,
    pub status: ManifestLoadStatus,
    pub namespace_count: usize,
    pub source_family_count: usize,
    pub manifest_count: usize,
}

/// Repository-level load outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestLoadStatus {
    Loaded,
    Empty,
    MissingRoot,
    InvalidRoot,
}

impl ManifestLoadStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Loaded => "loaded",
            Self::Empty => "empty",
            Self::MissingRoot => "missing_root",
            Self::InvalidRoot => "invalid_root",
        }
    }
}

/// Result of syncing the checked-in manifest repository into Postgres.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestSyncSummary {
    pub status: ManifestSyncStatus,
    pub synced_manifest_count: usize,
    pub active_manifest_count: usize,
    pub root_count: usize,
    pub contract_count: usize,
    pub capability_count: usize,
    pub discovery_rule_count: usize,
    pub removed_manifest_count: usize,
    pub cleared_discovery_edge_count: usize,
}

impl ManifestSyncSummary {
    fn skipped(status: ManifestSyncStatus) -> Self {
        Self {
            status,
            synced_manifest_count: 0,
            active_manifest_count: 0,
            root_count: 0,
            contract_count: 0,
            capability_count: 0,
            discovery_rule_count: 0,
            removed_manifest_count: 0,
            cleared_discovery_edge_count: 0,
        }
    }
}

/// Persistence outcome used for logging.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestSyncStatus {
    Synced,
    SkippedMissingRoot,
    SkippedInvalidRoot,
}

impl ManifestSyncStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Synced => "synced",
            Self::SkippedMissingRoot => "skipped_missing_root",
            Self::SkippedInvalidRoot => "skipped_invalid_root",
        }
    }
}

const MANIFEST_PROXY_IMPLEMENTATION_EDGE_KIND: &str = "proxy_implementation";
const MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE: &str = "manifest_declared_proxy";
const MANIFEST_PROXY_IMPLEMENTATION_ADMISSION: &str = "manifest_declared";

/// Stored admission view rebuilt from the persisted manifest tables.
#[derive(Clone, Debug)]
pub struct DiscoveryAdmissionState {
    pub active_manifest_count: usize,
    pub active_root_count: usize,
    pub active_contract_count: usize,
    pub active_rule_count: usize,
    active_roots: Vec<StoredActiveRoot>,
    active_root_manifest_ids: HashSet<i64>,
    active_contracts: Vec<StoredActiveContract>,
    rules_by_manifest_id: HashMap<i64, Vec<StoredDiscoveryRule>>,
}

impl DiscoveryAdmissionState {
    /// Whether an address is directly authoritative via an active manifest root or contract.
    pub fn has_authoritative_address(&self, chain: &str, address: &str) -> bool {
        let normalized_address = normalize_address(address);

        self.active_roots
            .iter()
            .any(|root| root.chain == chain && root.address == normalized_address)
            || self
                .active_contracts
                .iter()
                .any(|contract| contract.chain == chain && contract.address == normalized_address)
    }

    /// Admit a discovered edge using only the stored active manifest contracts and rules.
    pub fn admit_candidate(
        &self,
        candidate: &DiscoveryCandidate<'_>,
    ) -> Vec<AdmittedDiscoveryEdge> {
        let normalized_from_address = normalize_address(candidate.from_address);
        let normalized_to_address = normalize_address(candidate.to_address);
        let mut admitted_edges = HashSet::new();

        for contract in self.active_contracts.iter().filter(|contract| {
            contract.chain == candidate.chain && contract.address == normalized_from_address
        }) {
            if !self
                .active_root_manifest_ids
                .contains(&contract.manifest_id)
            {
                continue;
            }

            let Some(rules) = self.rules_by_manifest_id.get(&contract.manifest_id) else {
                continue;
            };

            for rule in rules.iter().filter(|rule| {
                rule.edge_kind == candidate.edge_kind && rule.from_role == contract.role
            }) {
                admitted_edges.insert(AdmittedDiscoveryEdge {
                    source_manifest_id: contract.manifest_id,
                    chain: candidate.chain.to_owned(),
                    from_address: normalized_from_address.clone(),
                    to_address: normalized_to_address.clone(),
                    edge_kind: candidate.edge_kind.to_owned(),
                    discovery_source: candidate.discovery_source.to_owned(),
                    admission: rule.admission.clone(),
                    from_role: contract.role.clone(),
                });
            }
        }

        let mut admitted_edges = admitted_edges.into_iter().collect::<Vec<_>>();
        admitted_edges.sort_by(|left, right| {
            (
                left.source_manifest_id,
                &left.chain,
                &left.from_address,
                &left.to_address,
                &left.edge_kind,
                &left.discovery_source,
                &left.admission,
                &left.from_role,
            )
                .cmp(&(
                    right.source_manifest_id,
                    &right.chain,
                    &right.from_address,
                    &right.to_address,
                    &right.edge_kind,
                    &right.discovery_source,
                    &right.admission,
                    &right.from_role,
                ))
        });
        admitted_edges
    }
}

/// Admit one observed discovery candidate from stored active manifests and persist new edges.
pub async fn persist_discovery_observation(
    pool: &PgPool,
    observation: &DiscoveryObservation,
) -> Result<DiscoveryPersistenceSummary> {
    let admission_state = load_discovery_admission_state(pool).await?;
    let admitted_edges = admission_state.admit_candidate(&observation.candidate());
    let mut inserted_edge_count = 0;
    let mut transaction = pool
        .begin()
        .await
        .context("failed to start discovery-edge persistence transaction")?;

    for admitted_edge in &admitted_edges {
        let exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM discovery_edges
                WHERE chain_id = $1
                  AND edge_kind = $2
                  AND from_address = $3
                  AND to_address = $4
                  AND discovery_source = $5
                  AND source_manifest_id = $6
                  AND admission = $7
                  AND active_from_block_number IS NOT DISTINCT FROM $8
                  AND active_from_block_hash IS NOT DISTINCT FROM $9
                  AND active_to_block_number IS NOT DISTINCT FROM $10
                  AND active_to_block_hash IS NOT DISTINCT FROM $11
            )
            "#,
        )
        .bind(&admitted_edge.chain)
        .bind(&admitted_edge.edge_kind)
        .bind(&admitted_edge.from_address)
        .bind(&admitted_edge.to_address)
        .bind(&admitted_edge.discovery_source)
        .bind(admitted_edge.source_manifest_id)
        .bind(&admitted_edge.admission)
        .bind(observation.active_from_block_number)
        .bind(observation.active_from_block_hash.as_deref())
        .bind(observation.active_to_block_number)
        .bind(observation.active_to_block_hash.as_deref())
        .fetch_one(transaction.as_mut())
        .await
        .context("failed to check for an existing discovery edge")?;

        if exists {
            continue;
        }

        let provenance = serde_json::to_string(&observation.provenance)
            .context("failed to serialize discovery-edge provenance")?;
        sqlx::query(
            r#"
            INSERT INTO discovery_edges (
                chain_id,
                edge_kind,
                from_address,
                to_address,
                discovery_source,
                source_manifest_id,
                admission,
                active_from_block_number,
                active_from_block_hash,
                active_to_block_number,
                active_to_block_hash,
                provenance
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb)
            "#,
        )
        .bind(&admitted_edge.chain)
        .bind(&admitted_edge.edge_kind)
        .bind(&admitted_edge.from_address)
        .bind(&admitted_edge.to_address)
        .bind(&admitted_edge.discovery_source)
        .bind(admitted_edge.source_manifest_id)
        .bind(&admitted_edge.admission)
        .bind(observation.active_from_block_number)
        .bind(observation.active_from_block_hash.as_deref())
        .bind(observation.active_to_block_number)
        .bind(observation.active_to_block_hash.as_deref())
        .bind(provenance)
        .execute(transaction.as_mut())
        .await
        .context("failed to insert an admitted discovery edge")?;
        inserted_edge_count += 1;
    }

    transaction
        .commit()
        .await
        .context("failed to commit discovery-edge persistence transaction")?;

    Ok(DiscoveryPersistenceSummary {
        admitted_edge_count: admitted_edges.len(),
        inserted_edge_count,
        admitted_edges,
    })
}

/// Load the canonical watched contract set from active manifests plus admitted discovery edges.
pub async fn load_watched_contracts(pool: &PgPool) -> Result<Vec<WatchedContract>> {
    let rows = sqlx::query(
        r#"
        SELECT chain, address, source, source_manifest_id
        FROM (
            SELECT
                mv.chain AS chain,
                mr.address AS address,
                'manifest_root'::text AS source,
                mv.manifest_id AS source_manifest_id
            FROM manifest_versions mv
            JOIN manifest_roots mr ON mr.manifest_id = mv.manifest_id
            WHERE mv.rollout_status = 'active'

            UNION

            SELECT
                mv.chain AS chain,
                mc.address AS address,
                'manifest_contract'::text AS source,
                mv.manifest_id AS source_manifest_id
            FROM manifest_versions mv
            JOIN manifest_contracts mc ON mc.manifest_id = mv.manifest_id
            WHERE mv.rollout_status = 'active'

            UNION

            SELECT
                de.chain_id AS chain,
                de.to_address AS address,
                'discovery_edge'::text AS source,
                de.source_manifest_id AS source_manifest_id
            FROM discovery_edges de
            JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
            WHERE mv.rollout_status = 'active'
        ) watched_contracts
        ORDER BY chain, address, source, source_manifest_id
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
                address: normalize_address(
                    &row.try_get::<String, _>("address")
                        .context("failed to read watched contract address")?,
                ),
                source: WatchedContractSource::from_db_value(&source)?,
                source_manifest_id: row
                    .try_get("source_manifest_id")
                    .context("failed to read watched contract source_manifest_id")?,
            })
        })
        .collect()
}

/// Summarize the canonical watched-contract set for logging or startup checks.
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

/// Build the per-chain runtime plan from the canonical watched-contract set.
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

/// Load and summarize the canonical watched-contract set from active manifests and discovery edges.
pub async fn load_watched_contract_summary(pool: &PgPool) -> Result<WatchedContractSummary> {
    let watched_contracts = load_watched_contracts(pool).await?;
    Ok(summarize_watched_contracts(&watched_contracts))
}

/// Load the per-chain runtime plan from the canonical watched-contract set.
pub async fn load_watched_chain_plan(pool: &PgPool) -> Result<Vec<WatchedChainPlan>> {
    let watched_contracts = load_watched_contracts(pool).await?;
    Ok(plan_watched_contracts(&watched_contracts))
}

/// Load the active persisted manifests for one namespace.
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
            mcf.status::text AS status,
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

/// Load the active persisted manifests and freshness timestamp for one namespace.
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct StoredActiveRoot {
    manifest_id: i64,
    chain: String,
    address: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StoredActiveContract {
    manifest_id: i64,
    chain: String,
    role: String,
    address: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StoredDiscoveryRule {
    edge_kind: String,
    from_role: String,
    admission: String,
}

/// Candidate discovery edge to evaluate against stored manifest state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiscoveryCandidate<'a> {
    pub chain: &'a str,
    pub from_address: &'a str,
    pub to_address: &'a str,
    pub edge_kind: &'a str,
    pub discovery_source: &'a str,
}

/// One admitted discovery edge returned by `DiscoveryAdmissionState`.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AdmittedDiscoveryEdge {
    pub source_manifest_id: i64,
    pub chain: String,
    pub from_address: String,
    pub to_address: String,
    pub edge_kind: String,
    pub discovery_source: String,
    pub admission: String,
    pub from_role: String,
}

/// Persistable discovery-edge observation with provenance and optional active ranges.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryObservation {
    pub chain: String,
    pub from_address: String,
    pub to_address: String,
    pub edge_kind: String,
    pub discovery_source: String,
    pub active_from_block_number: Option<i64>,
    pub active_from_block_hash: Option<String>,
    pub active_to_block_number: Option<i64>,
    pub active_to_block_hash: Option<String>,
    pub provenance: serde_json::Value,
}

impl DiscoveryObservation {
    pub fn candidate(&self) -> DiscoveryCandidate<'_> {
        DiscoveryCandidate {
            chain: &self.chain,
            from_address: &self.from_address,
            to_address: &self.to_address,
            edge_kind: &self.edge_kind,
            discovery_source: &self.discovery_source,
        }
    }
}

/// Result of admitting and persisting discovery edges.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryPersistenceSummary {
    pub admitted_edge_count: usize,
    pub inserted_edge_count: usize,
    pub admitted_edges: Vec<AdmittedDiscoveryEdge>,
}

/// One watched contract address derived from active manifests and admitted discovery edges.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WatchedContract {
    pub chain: String,
    pub address: String,
    pub source: WatchedContractSource,
    pub source_manifest_id: Option<i64>,
}

/// Stored source explaining why a contract is watched.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum WatchedContractSource {
    ManifestRoot,
    ManifestContract,
    DiscoveryEdge,
}

impl WatchedContractSource {
    fn from_db_value(value: &str) -> Result<Self> {
        match value {
            "manifest_root" => Ok(Self::ManifestRoot),
            "manifest_contract" => Ok(Self::ManifestContract),
            "discovery_edge" => Ok(Self::DiscoveryEdge),
            _ => bail!("unsupported watched contract source {value}"),
        }
    }
}

/// Summary of the canonical watched-contract set rebuilt from stored manifest state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchedContractSummary {
    pub unique_contract_count: usize,
    pub source_entry_count: usize,
    pub manifest_root_count: usize,
    pub manifest_contract_count: usize,
    pub discovery_edge_count: usize,
    pub chains: Vec<WatchedContractChainSummary>,
}

/// Per-chain runtime plan rebuilt from the canonical watched-contract set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchedChainPlan {
    pub chain: String,
    pub addresses: Vec<String>,
    pub manifest_root_entry_count: usize,
    pub manifest_contract_entry_count: usize,
    pub discovery_edge_entry_count: usize,
}

/// Per-chain watched-contract summary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchedContractChainSummary {
    pub chain: String,
    pub unique_contract_count: usize,
    pub manifest_root_count: usize,
    pub manifest_contract_count: usize,
    pub discovery_edge_count: usize,
}

/// One active persisted manifest exposed to consumers that only need version and capability state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ActiveManifestVersion {
    pub manifest_version: u64,
    pub source_family: String,
    pub chain: String,
    pub deployment_epoch: String,
    pub normalizer_version: String,
    pub capability_flags: BTreeMap<String, CapabilityFlag>,
}

/// Active manifest view for one namespace plus the storage timestamp used for API freshness.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceManifestSnapshot {
    pub manifests: Vec<ActiveManifestVersion>,
    pub last_updated: String,
}

/// Checked-in source manifest parsed from TOML.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceManifest {
    pub manifest_version: u64,
    pub namespace: String,
    pub source_family: String,
    pub chain: String,
    pub deployment_epoch: String,
    pub rollout_status: RolloutStatus,
    pub normalizer_version: String,
    pub capability_flags: BTreeMap<String, CapabilityFlag>,
    pub roots: Vec<ManifestRoot>,
    pub contracts: Vec<ManifestContract>,
    pub discovery_rules: Vec<DiscoveryRule>,
}

/// Manifest rollout states frozen in `docs/manifests.md`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RolloutStatus {
    Draft,
    Shadow,
    Active,
    Deprecated,
}

impl RolloutStatus {
    pub const fn as_db_value(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Shadow => "shadow",
            Self::Active => "active",
            Self::Deprecated => "deprecated",
        }
    }

    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }
}

/// Capability support states frozen in `docs/manifests.md`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilitySupportStatus {
    Unsupported,
    Shadow,
    Supported,
}

impl CapabilitySupportStatus {
    pub const fn as_db_value(self) -> &'static str {
        match self {
            Self::Unsupported => "unsupported",
            Self::Shadow => "shadow",
            Self::Supported => "supported",
        }
    }

    fn from_db_value(value: &str) -> Result<Self> {
        match value {
            "unsupported" => Ok(Self::Unsupported),
            "shadow" => Ok(Self::Shadow),
            "supported" => Ok(Self::Supported),
            _ => bail!("unsupported capability status {value}"),
        }
    }
}

/// One named capability entry under `[capability_flags]`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CapabilityFlag {
    pub status: CapabilitySupportStatus,
    pub notes: Option<String>,
}

/// Root declaration from a manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ManifestRoot {
    pub name: String,
    pub address: String,
    pub code_hash: Option<String>,
    pub abi_ref: Option<String>,
}

/// Contract declaration from a manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ManifestContract {
    pub role: String,
    pub address: String,
    pub proxy_kind: String,
    pub implementation: Option<String>,
}

/// Discovery rule declaration from a manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DiscoveryRule {
    pub edge_kind: String,
    pub from_role: String,
    pub admission: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct RawSourceManifest {
    manifest_version: u64,
    namespace: String,
    source_family: String,
    chain: String,
    deployment_epoch: String,
    rollout_status: RolloutStatus,
    normalizer_version: String,
    capability_flags: BTreeMap<String, RawCapabilityFlag>,
    roots: Vec<ManifestRoot>,
    contracts: Vec<ManifestContract>,
    discovery_rules: Vec<DiscoveryRule>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(untagged)]
enum RawCapabilityFlag {
    Status(CapabilitySupportStatus),
    Detailed(CapabilityFlag),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ManifestStorageKey {
    namespace: String,
    source_family: String,
    chain: String,
    deployment_epoch: String,
    manifest_version: i64,
}

impl ManifestStorageKey {
    fn from_loaded_manifest(loaded_manifest: &LoadedManifest) -> Result<Self> {
        Ok(Self {
            namespace: loaded_manifest.manifest.namespace.clone(),
            source_family: loaded_manifest.manifest.source_family.clone(),
            chain: loaded_manifest.manifest.chain.clone(),
            deployment_epoch: loaded_manifest.manifest.deployment_epoch.clone(),
            manifest_version: i64::try_from(loaded_manifest.manifest.manifest_version)
                .context("manifest_version does not fit into BIGINT")?,
        })
    }
}

impl From<RawSourceManifest> for SourceManifest {
    fn from(value: RawSourceManifest) -> Self {
        Self {
            manifest_version: value.manifest_version,
            namespace: value.namespace,
            source_family: value.source_family,
            chain: value.chain,
            deployment_epoch: value.deployment_epoch,
            rollout_status: value.rollout_status,
            normalizer_version: value.normalizer_version,
            capability_flags: value
                .capability_flags
                .into_iter()
                .map(|(name, flag)| {
                    let flag = match flag {
                        RawCapabilityFlag::Status(status) => CapabilityFlag {
                            status,
                            notes: None,
                        },
                        RawCapabilityFlag::Detailed(flag) => flag,
                    };
                    (name, flag)
                })
                .collect(),
            roots: value.roots,
            contracts: value.contracts,
            discovery_rules: value.discovery_rules,
        }
    }
}

/// Load and validate the checked-in repository manifest tree.
pub fn load_repository(root: impl AsRef<Path>) -> Result<ManifestRepository> {
    let root = root.as_ref();
    let display_root = canonicalize_for_logging(root);

    if !root.exists() {
        return Ok(ManifestRepository {
            root: display_root.clone(),
            manifests: Vec::new(),
            summary: ManifestLoadSummary {
                root: display_root,
                status: ManifestLoadStatus::MissingRoot,
                namespace_count: 0,
                source_family_count: 0,
                manifest_count: 0,
            },
        });
    }

    if !root.is_dir() {
        return Ok(ManifestRepository {
            root: display_root.clone(),
            manifests: Vec::new(),
            summary: ManifestLoadSummary {
                root: display_root,
                status: ManifestLoadStatus::InvalidRoot,
                namespace_count: 0,
                source_family_count: 0,
                manifest_count: 0,
            },
        });
    }

    let mut manifests = Vec::new();
    let mut namespace_count = 0;
    let mut source_family_count = 0;

    for namespace in read_dir_sorted(root)
        .with_context(|| format!("failed to read manifests root {}", root.display()))?
    {
        if !namespace
            .file_type()
            .with_context(|| format!("failed to inspect {}", namespace.path().display()))?
            .is_dir()
        {
            continue;
        }

        namespace_count += 1;
        let namespace_name = namespace.file_name().to_string_lossy().into_owned();

        for source_family in read_dir_sorted(&namespace.path()).with_context(|| {
            format!(
                "failed to read namespace directory {}",
                namespace.path().display()
            )
        })? {
            if !source_family
                .file_type()
                .with_context(|| format!("failed to inspect {}", source_family.path().display()))?
                .is_dir()
            {
                continue;
            }

            source_family_count += 1;
            let source_family_name = source_family.file_name().to_string_lossy().into_owned();

            for manifest in read_dir_sorted(&source_family.path()).with_context(|| {
                format!(
                    "failed to read source family directory {}",
                    source_family.path().display()
                )
            })? {
                if !manifest
                    .file_type()
                    .with_context(|| format!("failed to inspect {}", manifest.path().display()))?
                    .is_file()
                {
                    continue;
                }

                if manifest.path().extension().and_then(|part| part.to_str()) != Some("toml") {
                    continue;
                }

                manifests.push(load_manifest_file(
                    root,
                    &manifest.path(),
                    &namespace_name,
                    &source_family_name,
                )?);
            }
        }
    }

    let manifest_count = manifests.len();
    let status = if manifests.is_empty() {
        ManifestLoadStatus::Empty
    } else {
        ManifestLoadStatus::Loaded
    };

    Ok(ManifestRepository {
        root: display_root.clone(),
        manifests,
        summary: ManifestLoadSummary {
            root: display_root,
            status,
            namespace_count,
            source_family_count,
            manifest_count,
        },
    })
}

/// Sync the repository snapshot into the persisted `manifest_*` tables.
pub async fn sync_repository(
    pool: &PgPool,
    repository: &ManifestRepository,
) -> Result<ManifestSyncSummary> {
    match repository.summary().status {
        ManifestLoadStatus::MissingRoot => {
            return Ok(ManifestSyncSummary::skipped(
                ManifestSyncStatus::SkippedMissingRoot,
            ));
        }
        ManifestLoadStatus::InvalidRoot => {
            return Ok(ManifestSyncSummary::skipped(
                ManifestSyncStatus::SkippedInvalidRoot,
            ));
        }
        ManifestLoadStatus::Loaded | ManifestLoadStatus::Empty => {}
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to start manifest sync transaction")?;

    let existing_manifests = sqlx::query(
        r#"
        SELECT manifest_id, namespace, source_family, chain, deployment_epoch, manifest_version
        FROM manifest_versions
        "#,
    )
    .fetch_all(transaction.as_mut())
    .await
    .context("failed to load existing manifest versions")?;

    let mut retained_keys = HashSet::new();
    let mut sync_summary = ManifestSyncSummary {
        status: ManifestSyncStatus::Synced,
        synced_manifest_count: repository.manifests().len(),
        active_manifest_count: repository
            .manifests()
            .iter()
            .filter(|loaded_manifest| loaded_manifest.manifest.rollout_status.is_active())
            .count(),
        root_count: 0,
        contract_count: 0,
        capability_count: 0,
        discovery_rule_count: 0,
        removed_manifest_count: 0,
        cleared_discovery_edge_count: 0,
    };

    for loaded_manifest in repository.manifests() {
        let storage_key = ManifestStorageKey::from_loaded_manifest(loaded_manifest)?;
        retained_keys.insert(storage_key);

        let manifest_id = upsert_manifest_version(transaction.as_mut(), loaded_manifest).await?;
        replace_manifest_children(transaction.as_mut(), manifest_id, &loaded_manifest.manifest)
            .await?;

        sync_summary.root_count += loaded_manifest.manifest.roots.len();
        sync_summary.contract_count += loaded_manifest.manifest.contracts.len();
        sync_summary.capability_count += loaded_manifest.manifest.capability_flags.len();
        sync_summary.discovery_rule_count += loaded_manifest.manifest.discovery_rules.len();
    }

    for existing_manifest in existing_manifests {
        let manifest_id = existing_manifest
            .try_get::<i64, _>("manifest_id")
            .context("failed to read existing manifest_id")?;
        let manifest_version = existing_manifest
            .try_get::<i64, _>("manifest_version")
            .context("failed to read existing manifest_version")?;
        let storage_key = ManifestStorageKey {
            namespace: existing_manifest
                .try_get("namespace")
                .context("failed to read existing namespace")?,
            source_family: existing_manifest
                .try_get("source_family")
                .context("failed to read existing source_family")?,
            chain: existing_manifest
                .try_get("chain")
                .context("failed to read existing chain")?,
            deployment_epoch: existing_manifest
                .try_get("deployment_epoch")
                .context("failed to read existing deployment_epoch")?,
            manifest_version,
        };

        if retained_keys.contains(&storage_key) {
            continue;
        }

        sqlx::query("DELETE FROM manifest_versions WHERE manifest_id = $1")
            .bind(manifest_id)
            .execute(transaction.as_mut())
            .await
            .with_context(|| format!("failed to delete stale manifest_id {manifest_id}"))?;
        sync_summary.removed_manifest_count += 1;
    }

    sync_summary.cleared_discovery_edge_count = sqlx::query("DELETE FROM discovery_edges")
        .execute(transaction.as_mut())
        .await
        .context("failed to clear stale discovery_edges during manifest sync")?
        .rows_affected() as usize;
    insert_manifest_declared_proxy_edges(transaction.as_mut()).await?;

    transaction
        .commit()
        .await
        .context("failed to commit manifest sync transaction")?;

    Ok(sync_summary)
}

async fn insert_manifest_declared_proxy_edges(
    executor: &mut sqlx::postgres::PgConnection,
) -> Result<()> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT
            mv.manifest_id AS manifest_id,
            mv.chain AS chain,
            mc.address AS from_address,
            mc.implementation AS to_address,
            mc.proxy_kind AS proxy_kind
        FROM manifest_versions mv
        JOIN manifest_contracts mc ON mc.manifest_id = mv.manifest_id
        WHERE mv.rollout_status = 'active'
          AND mc.implementation IS NOT NULL
        ORDER BY mv.manifest_id, mv.chain, mc.address, mc.implementation
        "#,
    )
    .fetch_all(&mut *executor)
    .await
    .context("failed to load manifest-declared proxy implementation edges")?;

    for row in rows {
        let manifest_id = row
            .try_get::<i64, _>("manifest_id")
            .context("failed to read manifest-declared proxy edge manifest_id")?;
        let chain = row
            .try_get::<String, _>("chain")
            .context("failed to read manifest-declared proxy edge chain")?;
        let from_address = normalize_address(
            &row.try_get::<String, _>("from_address")
                .context("failed to read manifest-declared proxy edge from_address")?,
        );
        let to_address = normalize_address(
            &row.try_get::<String, _>("to_address")
                .context("failed to read manifest-declared proxy edge to_address")?,
        );
        let proxy_kind = row
            .try_get::<String, _>("proxy_kind")
            .context("failed to read manifest-declared proxy edge proxy_kind")?;
        let provenance = serde_json::json!({
            "source": "manifest_contract",
            "proxy_kind": proxy_kind,
        });

        sqlx::query(
            r#"
            INSERT INTO discovery_edges (
                chain_id,
                edge_kind,
                from_address,
                to_address,
                discovery_source,
                source_manifest_id,
                admission,
                active_from_block_number,
                active_from_block_hash,
                active_to_block_number,
                active_to_block_hash,
                provenance
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, NULL, NULL, NULL, $8::jsonb)
            "#,
        )
        .bind(chain)
        .bind(MANIFEST_PROXY_IMPLEMENTATION_EDGE_KIND)
        .bind(from_address)
        .bind(to_address)
        .bind(MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE)
        .bind(manifest_id)
        .bind(MANIFEST_PROXY_IMPLEMENTATION_ADMISSION)
        .bind(provenance.to_string())
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to insert manifest-declared proxy implementation edge for manifest_id {manifest_id}"
            )
        })?;
    }

    Ok(())
}

/// Rebuild discovery admission directly from the stored active manifest state.
pub async fn load_discovery_admission_state(pool: &PgPool) -> Result<DiscoveryAdmissionState> {
    let active_manifest_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT FROM manifest_versions WHERE rollout_status = 'active'",
    )
    .fetch_one(pool)
    .await
    .context("failed to count active manifest versions")? as usize;

    let active_root_rows = sqlx::query(
        r#"
        SELECT mv.manifest_id, mv.chain, mr.address
        FROM manifest_versions mv
        JOIN manifest_roots mr ON mr.manifest_id = mv.manifest_id
        WHERE mv.rollout_status = 'active'
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load active manifest roots")?;

    let active_contract_rows = sqlx::query(
        r#"
        SELECT mv.manifest_id, mv.chain, mc.role, mc.address
        FROM manifest_versions mv
        JOIN manifest_contracts mc ON mc.manifest_id = mv.manifest_id
        WHERE mv.rollout_status = 'active'
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load active manifest contracts")?;

    let active_rule_rows = sqlx::query(
        r#"
        SELECT mv.manifest_id, mdr.edge_kind, mdr.from_role, mdr.admission
        FROM manifest_versions mv
        JOIN manifest_discovery_rules mdr ON mdr.manifest_id = mv.manifest_id
        WHERE mv.rollout_status = 'active'
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load active discovery rules")?;

    let active_roots = active_root_rows
        .into_iter()
        .map(|row| {
            Ok(StoredActiveRoot {
                manifest_id: row
                    .try_get("manifest_id")
                    .context("failed to read active root manifest_id")?,
                chain: row
                    .try_get("chain")
                    .context("failed to read active root chain")?,
                address: normalize_address(
                    &row.try_get::<String, _>("address")
                        .context("failed to read active root address")?,
                ),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let active_root_manifest_ids = active_roots.iter().map(|root| root.manifest_id).collect();

    let active_contracts = active_contract_rows
        .into_iter()
        .map(|row| {
            Ok(StoredActiveContract {
                manifest_id: row
                    .try_get("manifest_id")
                    .context("failed to read active contract manifest_id")?,
                chain: row
                    .try_get("chain")
                    .context("failed to read active contract chain")?,
                role: row
                    .try_get("role")
                    .context("failed to read active contract role")?,
                address: normalize_address(
                    &row.try_get::<String, _>("address")
                        .context("failed to read active contract address")?,
                ),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let mut rules_by_manifest_id: HashMap<i64, Vec<StoredDiscoveryRule>> = HashMap::new();
    for row in active_rule_rows {
        let manifest_id = row
            .try_get("manifest_id")
            .context("failed to read active rule manifest_id")?;
        let rule = StoredDiscoveryRule {
            edge_kind: row
                .try_get("edge_kind")
                .context("failed to read active rule edge_kind")?,
            from_role: row
                .try_get("from_role")
                .context("failed to read active rule from_role")?,
            admission: row
                .try_get("admission")
                .context("failed to read active rule admission")?,
        };
        rules_by_manifest_id
            .entry(manifest_id)
            .or_default()
            .push(rule);
    }

    let active_rule_count = rules_by_manifest_id.values().map(Vec::len).sum();

    Ok(DiscoveryAdmissionState {
        active_manifest_count,
        active_root_count: active_roots.len(),
        active_contract_count: active_contracts.len(),
        active_rule_count,
        active_roots,
        active_root_manifest_ids,
        active_contracts,
        rules_by_manifest_id,
    })
}

fn load_manifest_file(
    root: &Path,
    path: &Path,
    namespace_name: &str,
    source_family_name: &str,
) -> Result<LoadedManifest> {
    let relative_path = path
        .strip_prefix(root)
        .with_context(|| {
            format!(
                "manifest path {} is not under repository root {}",
                path.display(),
                root.display()
            )
        })?
        .to_path_buf();
    let version_tag = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .map(ToOwned::to_owned)
        .with_context(|| format!("manifest path {} is missing a file stem", path.display()))?;
    let raw_manifest = fs::read_to_string(path)
        .with_context(|| format!("failed to read manifest file {}", path.display()))?;
    let manifest: SourceManifest = toml::from_str::<RawSourceManifest>(&raw_manifest)
        .with_context(|| format!("failed to parse manifest TOML {}", path.display()))?
        .into();

    validate_manifest_metadata(
        &manifest,
        path,
        &relative_path,
        namespace_name,
        source_family_name,
    )?;

    Ok(LoadedManifest {
        path: path.to_path_buf(),
        relative_path,
        version_tag,
        manifest,
    })
}

async fn upsert_manifest_version(
    executor: &mut sqlx::postgres::PgConnection,
    loaded_manifest: &LoadedManifest,
) -> Result<i64> {
    let manifest_payload = serde_json::to_string(&loaded_manifest.manifest)
        .context("failed to serialize manifest payload")?;
    let manifest_key = ManifestStorageKey::from_loaded_manifest(loaded_manifest)?;

    let row = sqlx::query(
        r#"
        INSERT INTO manifest_versions (
            manifest_version,
            namespace,
            source_family,
            chain,
            deployment_epoch,
            rollout_status,
            normalizer_version,
            file_path,
            manifest_payload
        )
        VALUES ($1, $2, $3, $4, $5, $6::manifest_rollout_status, $7, $8, $9::jsonb)
        ON CONFLICT (namespace, source_family, chain, deployment_epoch, manifest_version)
        DO UPDATE SET
            rollout_status = EXCLUDED.rollout_status,
            normalizer_version = EXCLUDED.normalizer_version,
            file_path = EXCLUDED.file_path,
            manifest_payload = EXCLUDED.manifest_payload,
            loaded_at = now()
        RETURNING manifest_id
        "#,
    )
    .bind(manifest_key.manifest_version)
    .bind(&manifest_key.namespace)
    .bind(&manifest_key.source_family)
    .bind(&manifest_key.chain)
    .bind(&manifest_key.deployment_epoch)
    .bind(loaded_manifest.manifest.rollout_status.as_db_value())
    .bind(&loaded_manifest.manifest.normalizer_version)
    .bind(loaded_manifest.relative_path.to_string_lossy().into_owned())
    .bind(manifest_payload)
    .fetch_one(executor)
    .await
    .with_context(|| {
        format!(
            "failed to upsert manifest version from {}",
            loaded_manifest.path.display()
        )
    })?;

    row.try_get("manifest_id")
        .context("failed to read manifest_id from manifest upsert")
}

async fn replace_manifest_children(
    executor: &mut sqlx::postgres::PgConnection,
    manifest_id: i64,
    manifest: &SourceManifest,
) -> Result<()> {
    sqlx::query("DELETE FROM manifest_roots WHERE manifest_id = $1")
        .bind(manifest_id)
        .execute(&mut *executor)
        .await
        .with_context(|| format!("failed to clear manifest_roots for manifest_id {manifest_id}"))?;
    sqlx::query("DELETE FROM manifest_contracts WHERE manifest_id = $1")
        .bind(manifest_id)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!("failed to clear manifest_contracts for manifest_id {manifest_id}")
        })?;
    sqlx::query("DELETE FROM manifest_capability_flags WHERE manifest_id = $1")
        .bind(manifest_id)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!("failed to clear manifest_capability_flags for manifest_id {manifest_id}")
        })?;
    sqlx::query("DELETE FROM manifest_discovery_rules WHERE manifest_id = $1")
        .bind(manifest_id)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!("failed to clear manifest_discovery_rules for manifest_id {manifest_id}")
        })?;

    for root in &manifest.roots {
        sqlx::query(
            r#"
            INSERT INTO manifest_roots (manifest_id, name, address, code_hash, abi_ref)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(manifest_id)
        .bind(&root.name)
        .bind(normalize_address(&root.address))
        .bind(root.code_hash.as_deref())
        .bind(root.abi_ref.as_deref())
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to insert manifest root {} for manifest_id {manifest_id}",
                root.name
            )
        })?;
    }

    for contract in &manifest.contracts {
        let normalized_implementation = contract
            .implementation
            .as_ref()
            .map(|value| normalize_address(value));
        sqlx::query(
            r#"
            INSERT INTO manifest_contracts (
                manifest_id,
                role,
                address,
                proxy_kind,
                implementation
            )
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(manifest_id)
        .bind(&contract.role)
        .bind(normalize_address(&contract.address))
        .bind(&contract.proxy_kind)
        .bind(normalized_implementation.as_deref())
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to insert manifest contract role {} for manifest_id {manifest_id}",
                contract.role
            )
        })?;
    }

    for (capability_name, capability_flag) in &manifest.capability_flags {
        sqlx::query(
            r#"
            INSERT INTO manifest_capability_flags (
                manifest_id,
                capability_name,
                status,
                notes
            )
            VALUES ($1, $2, $3::capability_support_status, $4)
            "#,
        )
        .bind(manifest_id)
        .bind(capability_name)
        .bind(capability_flag.status.as_db_value())
        .bind(capability_flag.notes.as_deref())
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to insert capability {} for manifest_id {manifest_id}",
                capability_name
            )
        })?;
    }

    for discovery_rule in &manifest.discovery_rules {
        sqlx::query(
            r#"
            INSERT INTO manifest_discovery_rules (
                manifest_id,
                edge_kind,
                from_role,
                admission
            )
            VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(manifest_id)
        .bind(&discovery_rule.edge_kind)
        .bind(&discovery_rule.from_role)
        .bind(&discovery_rule.admission)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to insert discovery rule {} for manifest_id {manifest_id}",
                discovery_rule.edge_kind
            )
        })?;
    }

    Ok(())
}

fn validate_manifest_metadata(
    manifest: &SourceManifest,
    path: &Path,
    relative_path: &Path,
    namespace_name: &str,
    source_family_name: &str,
) -> Result<()> {
    let depth = relative_path.iter().count();
    if depth != 3 {
        bail!(
            "manifest path {} must match manifests/<namespace>/<source_family>/<version>.toml",
            path.display()
        );
    }

    if manifest.namespace != namespace_name {
        bail!(
            "manifest namespace {} does not match directory {} for {}",
            manifest.namespace,
            namespace_name,
            path.display()
        );
    }

    if manifest.source_family != source_family_name {
        bail!(
            "manifest source_family {} does not match directory {} for {}",
            manifest.source_family,
            source_family_name,
            path.display()
        );
    }

    Ok(())
}

fn read_dir_sorted(path: &Path) -> Result<Vec<fs::DirEntry>> {
    let mut entries = fs::read_dir(path)
        .with_context(|| format!("failed to read directory {}", path.display()))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("failed to iterate directory {}", path.display()))?;
    entries.sort_by_key(|entry| entry.file_name());
    Ok(entries)
}

fn canonicalize_for_logging(root: &Path) -> PathBuf {
    fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf())
}

fn normalize_address(value: &str) -> String {
    value.to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use bigname_storage::default_database_url;
    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
        query_scalar,
    };

    use super::*;

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);
    const TEST_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> Result<Self> {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "bigname-manifests-tests-{}-{unique}-{sequence}",
                std::process::id(),
            ));
            fs::create_dir_all(&path)
                .with_context(|| format!("failed to create test directory {}", path.display()))?;
            Ok(Self { path })
        }

        fn write_manifest(
            &self,
            namespace: &str,
            source_family: &str,
            version_tag: &str,
            contents: &str,
        ) -> Result<PathBuf> {
            let directory = self.path.join(namespace).join(source_family);
            fs::create_dir_all(&directory)
                .with_context(|| format!("failed to create {}", directory.display()))?;
            let path = directory.join(format!("{version_tag}.toml"));
            fs::write(&path, contents)
                .with_context(|| format!("failed to write {}", path.display()))?;
            Ok(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    struct TestDatabase {
        admin_pool: PgPool,
        pool: PgPool,
        database_name: String,
    }

    impl TestDatabase {
        async fn new() -> Result<Self> {
            let database_url = std::env::var("BIGNAME_DATABASE_URL")
                .or_else(|_| std::env::var("DATABASE_URL"))
                .unwrap_or_else(|_| default_database_url().to_owned());
            let base_options = PgConnectOptions::from_str(&database_url)
                .context("failed to parse database URL for manifest integration tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_manifests_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for manifest integration tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect manifest integration test pool")?;

            TEST_MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for manifest integration tests")?;

            Ok(Self {
                admin_pool,
                pool,
                database_name,
            })
        }

        fn pool(&self) -> &PgPool {
            &self.pool
        }

        async fn cleanup(self) -> Result<()> {
            self.pool.close().await;
            sqlx::query(&format!(
                r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
                self.database_name
            ))
            .execute(&self.admin_pool)
            .await
            .with_context(|| format!("failed to drop test database {}", self.database_name))?;
            self.admin_pool.close().await;
            Ok(())
        }
    }

    #[test]
    fn reports_missing_root() -> Result<()> {
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "bigname-manifests-missing-{}-{}-{sequence}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos()
        ));

        let repository = load_repository(&root)?;

        assert_eq!(repository.summary().status, ManifestLoadStatus::MissingRoot);
        assert!(repository.manifests().is_empty());

        Ok(())
    }

    #[test]
    fn reports_empty_root() -> Result<()> {
        let test_dir = TestDir::new()?;

        let repository = load_repository(&test_dir.path)?;

        assert_eq!(repository.summary().status, ManifestLoadStatus::Empty);
        assert_eq!(repository.summary().namespace_count, 0);
        assert_eq!(repository.summary().source_family_count, 0);
        assert_eq!(repository.summary().manifest_count, 0);

        Ok(())
    }

    #[test]
    fn loads_valid_repository_manifest() -> Result<()> {
        let test_dir = TestDir::new()?;
        test_dir.write_manifest(
            "ens",
            "ens_v2_registry_l1",
            "v1",
            r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v2_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v2"
rollout_status = "active"
normalizer_version = "uts46-v1"

[capability_flags]
declared_children = "supported"

[capability_flags.verified_resolution]
status = "shadow"
notes = "tracked but not yet served"

[[roots]]
name = "RootRegistry"
address = "0x0000000000000000000000000000000000000000"
code_hash = "sha256:test"
abi_ref = "abis/root_registry.json"

[[contracts]]
role = "registry"
address = "0x0000000000000000000000000000000000000000"
proxy_kind = "none"

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"
"#,
        )?;

        let repository = load_repository(&test_dir.path)?;

        assert_eq!(repository.summary().status, ManifestLoadStatus::Loaded);
        assert_eq!(repository.summary().namespace_count, 1);
        assert_eq!(repository.summary().source_family_count, 1);
        assert_eq!(repository.summary().manifest_count, 1);
        assert_eq!(repository.manifests().len(), 1);
        assert_eq!(repository.manifests()[0].version_tag, "v1");
        assert_eq!(repository.manifests()[0].manifest.namespace, "ens");
        assert_eq!(
            repository.manifests()[0]
                .manifest
                .capability_flags
                .get("declared_children")
                .expect("declared_children capability")
                .status,
            CapabilitySupportStatus::Supported
        );
        assert_eq!(
            repository.manifests()[0]
                .manifest
                .capability_flags
                .get("verified_resolution")
                .expect("verified_resolution capability")
                .notes
                .as_deref(),
            Some("tracked but not yet served")
        );

        Ok(())
    }

    #[test]
    fn rejects_namespace_mismatch() -> Result<()> {
        let test_dir = TestDir::new()?;
        let path = test_dir.write_manifest(
            "ens",
            "ens_v2_registry_l1",
            "v1",
            r#"
manifest_version = 1
namespace = "basenames"
source_family = "ens_v2_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v2"
rollout_status = "active"
normalizer_version = "uts46-v1"

[capability_flags]
declared_children = "supported"

[[roots]]
name = "RootRegistry"
address = "0x0000000000000000000000000000000000000000"

[[contracts]]
role = "registry"
address = "0x0000000000000000000000000000000000000000"
proxy_kind = "none"

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"
"#,
        )?;

        let error = load_repository(&test_dir.path).expect_err("namespace mismatch must fail");

        assert!(
            error.to_string().contains("does not match directory"),
            "unexpected error for {}: {error:#}",
            path.display()
        );

        Ok(())
    }

    #[tokio::test]
    async fn persists_repository_and_rebuilds_discovery_admission_from_storage() -> Result<()> {
        let test_dir = TestDir::new()?;
        test_dir.write_manifest(
            "ens",
            "ens_v2_registry_l1",
            "v1",
            r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v2_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v2"
rollout_status = "active"
normalizer_version = "uts46-v1"

[capability_flags]
declared_children = { status = "supported", notes = "declared children are enabled" }
verified_resolution = "shadow"

[[roots]]
name = "RootRegistry"
address = "0x0000000000000000000000000000000000000001"

[[contracts]]
role = "registry"
address = "0x00000000000000000000000000000000000000AA"
proxy_kind = "erc1967"
implementation = "0x00000000000000000000000000000000000000DD"

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"
"#,
        )?;
        test_dir.write_manifest(
            "basenames",
            "base_registry",
            "v1",
            r#"
manifest_version = 1
namespace = "basenames"
source_family = "base_registry"
chain = "base-mainnet"
deployment_epoch = "basenames_v1"
rollout_status = "shadow"
normalizer_version = "uts46-v1"

[capability_flags]
declared_children = "shadow"

[[roots]]
name = "BaseRoot"
address = "0x0000000000000000000000000000000000000002"

[[contracts]]
role = "registry"
address = "0x00000000000000000000000000000000000000BB"
proxy_kind = "none"

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"
"#,
        )?;

        let repository = load_repository(&test_dir.path)?;
        let database = TestDatabase::new().await?;

        let sync_summary = sync_repository(database.pool(), &repository).await?;
        assert_eq!(sync_summary.status, ManifestSyncStatus::Synced);
        assert_eq!(sync_summary.synced_manifest_count, 2);
        assert_eq!(sync_summary.active_manifest_count, 1);
        assert_eq!(sync_summary.root_count, 2);
        assert_eq!(sync_summary.contract_count, 2);
        assert_eq!(sync_summary.capability_count, 3);
        assert_eq!(sync_summary.discovery_rule_count, 3);
        assert_eq!(sync_summary.removed_manifest_count, 0);
        assert_eq!(sync_summary.cleared_discovery_edge_count, 0);

        assert_eq!(
            query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM manifest_versions")
                .fetch_one(database.pool())
                .await?,
            2
        );
        assert_eq!(
            query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM manifest_roots")
                .fetch_one(database.pool())
                .await?,
            2
        );
        assert_eq!(
            query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM manifest_contracts")
                .fetch_one(database.pool())
                .await?,
            2
        );
        assert_eq!(
            query_scalar::<_, Option<String>>(
                "SELECT implementation FROM manifest_contracts WHERE role = 'registry' AND address = '0x00000000000000000000000000000000000000aa'"
            )
            .fetch_one(database.pool())
            .await?,
            Some("0x00000000000000000000000000000000000000dd".to_owned())
        );
        assert_eq!(
            query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM manifest_capability_flags")
                .fetch_one(database.pool())
                .await?,
            3
        );
        assert_eq!(
            query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM manifest_discovery_rules")
                .fetch_one(database.pool())
                .await?,
            3
        );
        assert_eq!(
            query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM discovery_edges")
                .fetch_one(database.pool())
                .await?,
            1
        );
        let proxy_edge = sqlx::query(
            r#"
            SELECT
                edge_kind,
                from_address,
                to_address,
                discovery_source,
                admission,
                provenance
            FROM discovery_edges
            ORDER BY chain_id, from_address, to_address
            LIMIT 1
            "#,
        )
        .fetch_one(database.pool())
        .await?;
        assert_eq!(
            proxy_edge.try_get::<String, _>("edge_kind")?,
            MANIFEST_PROXY_IMPLEMENTATION_EDGE_KIND
        );
        assert_eq!(
            proxy_edge.try_get::<String, _>("from_address")?,
            "0x00000000000000000000000000000000000000aa".to_owned()
        );
        assert_eq!(
            proxy_edge.try_get::<String, _>("to_address")?,
            "0x00000000000000000000000000000000000000dd".to_owned()
        );
        assert_eq!(
            proxy_edge.try_get::<String, _>("discovery_source")?,
            MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE
        );
        assert_eq!(
            proxy_edge.try_get::<String, _>("admission")?,
            MANIFEST_PROXY_IMPLEMENTATION_ADMISSION
        );
        assert_eq!(
            proxy_edge.try_get::<serde_json::Value, _>("provenance")?,
            serde_json::json!({
                "source": "manifest_contract",
                "proxy_kind": "erc1967",
            })
        );

        let admission_state = load_discovery_admission_state(database.pool()).await?;
        assert_eq!(admission_state.active_manifest_count, 1);
        assert_eq!(admission_state.active_root_count, 1);
        assert_eq!(admission_state.active_contract_count, 1);
        assert_eq!(admission_state.active_rule_count, 2);
        assert!(admission_state.has_authoritative_address(
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000aa"
        ));
        assert!(!admission_state.has_authoritative_address(
            "base-mainnet",
            "0x00000000000000000000000000000000000000bb"
        ));

        let admitted_edges = admission_state.admit_candidate(&DiscoveryCandidate {
            chain: "ethereum-mainnet",
            from_address: "0x00000000000000000000000000000000000000AA",
            to_address: "0x00000000000000000000000000000000000000CC",
            edge_kind: "subregistry",
            discovery_source: "unit-test",
        });
        assert_eq!(admitted_edges.len(), 1);
        assert_eq!(admitted_edges[0].admission, "reachable_from_root");
        assert_eq!(admitted_edges[0].from_role, "registry");
        assert_eq!(
            admitted_edges[0].to_address,
            "0x00000000000000000000000000000000000000cc"
        );
        let persistence_summary = persist_discovery_observation(
            database.pool(),
            &DiscoveryObservation {
                chain: "ethereum-mainnet".to_owned(),
                from_address: "0x00000000000000000000000000000000000000AA".to_owned(),
                to_address: "0x00000000000000000000000000000000000000CC".to_owned(),
                edge_kind: "subregistry".to_owned(),
                discovery_source: "unit-test".to_owned(),
                active_from_block_number: Some(123),
                active_from_block_hash: Some(
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                ),
                active_to_block_number: None,
                active_to_block_hash: None,
                provenance: serde_json::json!({
                    "provider": "unit-test",
                    "kind": "subregistry",
                }),
            },
        )
        .await?;
        assert_eq!(persistence_summary.admitted_edge_count, 1);
        assert_eq!(persistence_summary.inserted_edge_count, 1);

        let repeated_persistence_summary = persist_discovery_observation(
            database.pool(),
            &DiscoveryObservation {
                chain: "ethereum-mainnet".to_owned(),
                from_address: "0x00000000000000000000000000000000000000AA".to_owned(),
                to_address: "0x00000000000000000000000000000000000000CC".to_owned(),
                edge_kind: "subregistry".to_owned(),
                discovery_source: "unit-test".to_owned(),
                active_from_block_number: Some(123),
                active_from_block_hash: Some(
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                ),
                active_to_block_number: None,
                active_to_block_hash: None,
                provenance: serde_json::json!({
                    "provider": "unit-test",
                    "kind": "subregistry",
                }),
            },
        )
        .await?;
        assert_eq!(repeated_persistence_summary.admitted_edge_count, 1);
        assert_eq!(repeated_persistence_summary.inserted_edge_count, 0);
        assert_eq!(
            query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM discovery_edges")
                .fetch_one(database.pool())
                .await?,
            2
        );

        let watched_contracts = load_watched_contracts(database.pool()).await?;
        assert_eq!(watched_contracts.len(), 4);
        assert!(watched_contracts.iter().any(|contract| {
            contract.chain == "ethereum-mainnet"
                && contract.address == "0x0000000000000000000000000000000000000001"
                && contract.source == WatchedContractSource::ManifestRoot
        }));
        assert!(watched_contracts.iter().any(|contract| {
            contract.chain == "ethereum-mainnet"
                && contract.address == "0x00000000000000000000000000000000000000aa"
                && contract.source == WatchedContractSource::ManifestContract
        }));
        assert!(watched_contracts.iter().any(|contract| {
            contract.chain == "ethereum-mainnet"
                && contract.address == "0x00000000000000000000000000000000000000dd"
                && contract.source == WatchedContractSource::DiscoveryEdge
        }));
        assert!(watched_contracts.iter().any(|contract| {
            contract.chain == "ethereum-mainnet"
                && contract.address == "0x00000000000000000000000000000000000000cc"
                && contract.source == WatchedContractSource::DiscoveryEdge
        }));

        let active_ens_manifests =
            load_active_manifests_for_namespace(database.pool(), "ens").await?;
        assert_eq!(active_ens_manifests.len(), 1);
        assert_eq!(active_ens_manifests[0].manifest_version, 1);
        assert_eq!(active_ens_manifests[0].source_family, "ens_v2_registry_l1");
        assert_eq!(active_ens_manifests[0].chain, "ethereum-mainnet");
        assert_eq!(active_ens_manifests[0].deployment_epoch, "ens_v2");
        assert_eq!(active_ens_manifests[0].normalizer_version, "uts46-v1");
        assert_eq!(
            active_ens_manifests[0]
                .capability_flags
                .get("declared_children")
                .expect("declared_children capability"),
            &CapabilityFlag {
                status: CapabilitySupportStatus::Supported,
                notes: Some("declared children are enabled".to_owned()),
            }
        );
        assert_eq!(
            active_ens_manifests[0]
                .capability_flags
                .get("verified_resolution")
                .expect("verified_resolution capability"),
            &CapabilityFlag {
                status: CapabilitySupportStatus::Shadow,
                notes: None,
            }
        );
        assert!(
            load_active_manifests_for_namespace(database.pool(), "basenames")
                .await?
                .is_empty()
        );
        let ens_snapshot = load_namespace_manifest_snapshot(database.pool(), "ens").await?;
        assert_eq!(ens_snapshot.manifests, active_ens_manifests);
        assert!(ens_snapshot.last_updated.ends_with('Z'));
        let basenames_snapshot =
            load_namespace_manifest_snapshot(database.pool(), "basenames").await?;
        assert!(basenames_snapshot.manifests.is_empty());
        assert!(basenames_snapshot.last_updated.ends_with('Z'));
        let watched_summary = load_watched_contract_summary(database.pool()).await?;
        assert_eq!(watched_summary.unique_contract_count, 4);
        assert_eq!(watched_summary.source_entry_count, 4);
        assert_eq!(watched_summary.manifest_root_count, 1);
        assert_eq!(watched_summary.manifest_contract_count, 1);
        assert_eq!(watched_summary.discovery_edge_count, 2);
        assert_eq!(
            watched_summary.chains,
            vec![WatchedContractChainSummary {
                chain: "ethereum-mainnet".to_owned(),
                unique_contract_count: 4,
                manifest_root_count: 1,
                manifest_contract_count: 1,
                discovery_edge_count: 2,
            }]
        );
        let watched_chain_plan = load_watched_chain_plan(database.pool()).await?;
        assert_eq!(
            watched_chain_plan,
            vec![WatchedChainPlan {
                chain: "ethereum-mainnet".to_owned(),
                addresses: vec![
                    "0x0000000000000000000000000000000000000001".to_owned(),
                    "0x00000000000000000000000000000000000000aa".to_owned(),
                    "0x00000000000000000000000000000000000000cc".to_owned(),
                    "0x00000000000000000000000000000000000000dd".to_owned(),
                ],
                manifest_root_entry_count: 1,
                manifest_contract_entry_count: 1,
                discovery_edge_entry_count: 2,
            }]
        );

        let empty_dir = TestDir::new()?;
        let empty_repository = load_repository(&empty_dir.path)?;
        let empty_sync_summary = sync_repository(database.pool(), &empty_repository).await?;
        assert_eq!(empty_sync_summary.status, ManifestSyncStatus::Synced);
        assert_eq!(empty_sync_summary.synced_manifest_count, 0);
        assert_eq!(empty_sync_summary.removed_manifest_count, 2);
        assert_eq!(empty_sync_summary.cleared_discovery_edge_count, 2);
        assert_eq!(
            query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM manifest_versions")
                .fetch_one(database.pool())
                .await?,
            0
        );
        assert_eq!(
            query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM discovery_edges")
                .fetch_one(database.pool())
                .await?,
            0
        );
        let cleared_admission_state = load_discovery_admission_state(database.pool()).await?;
        assert_eq!(cleared_admission_state.active_manifest_count, 0);
        assert!(
            cleared_admission_state
                .admit_candidate(&DiscoveryCandidate {
                    chain: "ethereum-mainnet",
                    from_address: "0x00000000000000000000000000000000000000AA",
                    to_address: "0x00000000000000000000000000000000000000CC",
                    edge_kind: "subregistry",
                    discovery_source: "unit-test",
                })
                .is_empty()
        );

        database.cleanup().await?;
        Ok(())
    }
}
