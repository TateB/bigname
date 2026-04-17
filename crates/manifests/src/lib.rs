//! Repository manifest loading, persistence, and discovery admission.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use uuid::Uuid;

pub const fn bootstrap_status() -> &'static str {
    "manifest-loader-ready"
}

const DECLARATION_KIND_ROOT: &str = "root";
const DECLARATION_KIND_CONTRACT: &str = "contract";
const CONTRACT_KIND_ROOT: &str = "root";
const CONTRACT_KIND_CONTRACT: &str = "contract";
const MANIFEST_PROXY_IMPLEMENTATION_EDGE_KIND: &str = "proxy_implementation";
const MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE: &str = "manifest_declared_proxy";
const MANIFEST_PROXY_IMPLEMENTATION_ADMISSION: &str = "manifest_declared";
const MANIFEST_SUCCESSOR_EDGE_KIND: &str = "migration";
const MANIFEST_SUCCESSOR_DISCOVERY_SOURCE: &str = "manifest_successor";
const MANIFEST_SUCCESSOR_ADMISSION: &str = "manifest_successor";
const REACHABLE_FROM_ROOT_ADMISSION: &str = "reachable_from_root";
const PROPAGATED_ROLE_PROVENANCE_FIELD: &str = "propagated_role";
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedManifest {
    pub path: PathBuf,
    pub relative_path: PathBuf,
    pub version_tag: String,
    pub manifest: SourceManifest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestLoadSummary {
    pub root: PathBuf,
    pub status: ManifestLoadStatus,
    pub namespace_count: usize,
    pub source_family_count: usize,
    pub manifest_count: usize,
}

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

#[derive(Clone, Debug)]
pub struct DiscoveryAdmissionState {
    pub active_manifest_count: usize,
    pub active_root_count: usize,
    pub active_contract_count: usize,
    pub active_rule_count: usize,
    active_roots: Vec<StoredActiveRoot>,
    active_root_manifest_ids: HashSet<i64>,
    active_contracts: Vec<StoredActiveContract>,
    known_contract_instances_by_address: HashMap<(String, String), Uuid>,
    rules_by_manifest_id: HashMap<i64, Vec<StoredDiscoveryRule>>,
}

impl DiscoveryAdmissionState {
    pub fn has_authoritative_address(&self, chain: &str, address: &str) -> bool {
        let normalized_address = normalize_address(address);
        let key = (chain.to_owned(), normalized_address);

        self.active_roots
            .iter()
            .any(|root| root.chain == key.0 && root.address == key.1)
            || self
                .active_contracts
                .iter()
                .any(|contract| contract.chain == key.0 && contract.address == key.1)
    }

    pub fn admit_candidate(
        &self,
        candidate: &DiscoveryCandidate<'_>,
    ) -> Vec<AdmittedDiscoveryEdge> {
        self.admit_candidate_against_contracts(&self.active_contracts, candidate)
    }

    fn admit_candidate_against_contracts(
        &self,
        active_contracts: &[StoredActiveContract],
        candidate: &DiscoveryCandidate<'_>,
    ) -> Vec<AdmittedDiscoveryEdge> {
        let normalized_from_address = normalize_address(candidate.from_address);
        let normalized_to_address = normalize_address(candidate.to_address);
        let mut admitted_edges = HashSet::new();

        for contract in active_contracts.iter().filter(|contract| {
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
                    from_contract_instance_id: contract.contract_instance_id,
                    to_contract_instance_id: self
                        .known_contract_instances_by_address
                        .get(&(candidate.chain.to_owned(), normalized_to_address.clone()))
                        .copied(),
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
                left.chain.as_str(),
                left.from_contract_instance_id,
                left.to_contract_instance_id,
                left.to_address.as_str(),
                left.edge_kind.as_str(),
                left.discovery_source.as_str(),
                left.admission.as_str(),
                left.from_role.as_str(),
            )
                .cmp(&(
                    right.source_manifest_id,
                    right.chain.as_str(),
                    right.from_contract_instance_id,
                    right.to_contract_instance_id,
                    right.to_address.as_str(),
                    right.edge_kind.as_str(),
                    right.discovery_source.as_str(),
                    right.admission.as_str(),
                    right.from_role.as_str(),
                ))
        });
        admitted_edges
    }
}

pub async fn persist_discovery_observation(
    pool: &PgPool,
    observation: &DiscoveryObservation,
) -> Result<DiscoveryPersistenceSummary> {
    let admission_state = load_discovery_admission_state(pool).await?;
    let admitted_candidates = admission_state.admit_candidate(&observation.candidate());
    let mut inserted_edge_count = 0;
    let mut admitted_edges = Vec::new();
    let mut transaction = pool
        .begin()
        .await
        .context("failed to start discovery-edge persistence transaction")?;

    for mut admitted_edge in admitted_candidates {
        let to_contract_instance_id = match admitted_edge.to_contract_instance_id {
            Some(contract_instance_id) => contract_instance_id,
            None => {
                resolve_contract_instance_by_address(
                    transaction.as_mut(),
                    &admitted_edge.chain,
                    &admitted_edge.to_address,
                    CONTRACT_KIND_CONTRACT,
                    &serde_json::json!({
                        "source": "discovery_observation",
                        "edge_kind": admitted_edge.edge_kind,
                        "discovery_source": admitted_edge.discovery_source,
                    }),
                )
                .await?
            }
        };
        admitted_edge.to_contract_instance_id = Some(to_contract_instance_id);
        ensure_contract_instance_address_seed(
            transaction.as_mut(),
            to_contract_instance_id,
            &admitted_edge.chain,
            &admitted_edge.to_address,
            Some(admitted_edge.source_manifest_id),
            &serde_json::json!({
                "source": "discovery_observation_seed",
                "edge_kind": admitted_edge.edge_kind,
                "discovery_source": admitted_edge.discovery_source,
            }),
        )
        .await?;

        let exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM discovery_edges
                WHERE chain_id = $1
                  AND edge_kind = $2
                  AND from_contract_instance_id = $3
                  AND to_contract_instance_id = $4
                  AND discovery_source = $5
                  AND source_manifest_id = $6
                  AND admission = $7
                  AND active_from_block_number IS NOT DISTINCT FROM $8
                  AND active_from_block_hash IS NOT DISTINCT FROM $9
                  AND active_to_block_number IS NOT DISTINCT FROM $10
                  AND active_to_block_hash IS NOT DISTINCT FROM $11
                  AND deactivated_at IS NULL
            )
            "#,
        )
        .bind(&admitted_edge.chain)
        .bind(&admitted_edge.edge_kind)
        .bind(admitted_edge.from_contract_instance_id)
        .bind(to_contract_instance_id)
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

        if !exists {
            let provenance = serde_json::to_string(&with_propagated_role(
                &observation.provenance,
                &admitted_edge.from_role,
            )?)
            .context("failed to serialize discovery-edge provenance")?;
            sqlx::query(
                r#"
                INSERT INTO discovery_edges (
                    chain_id,
                    edge_kind,
                    from_contract_instance_id,
                    to_contract_instance_id,
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
            .bind(admitted_edge.from_contract_instance_id)
            .bind(to_contract_instance_id)
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

        admitted_edges.push(admitted_edge);
    }

    reconcile_active_contract_instance_addresses(transaction.as_mut()).await?;

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

fn observation_key(observation: &DiscoveryObservation) -> Result<String> {
    observation
        .provenance
        .get("observation_key")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .with_context(|| {
            format!(
                "discovery observation for {} {} is missing provenance.observation_key",
                observation.discovery_source, observation.from_address
            )
        })
}

fn observation_terminal_states(
    observations: &[DiscoveryObservation],
) -> Result<HashMap<String, ObservationTerminalState>> {
    observations
        .iter()
        .map(|observation| {
            Ok((
                observation_key(observation)?,
                ObservationTerminalState {
                    chain: observation.chain.clone(),
                    block_number: observation.active_from_block_number,
                    block_hash: observation.active_from_block_hash.clone(),
                },
            ))
        })
        .collect()
}

fn is_zero_address(value: &str) -> bool {
    normalize_address(value) == ZERO_ADDRESS
}

fn with_propagated_role(
    provenance: &serde_json::Value,
    from_role: &str,
) -> Result<serde_json::Value> {
    let mut provenance = provenance.clone();
    let Some(object) = provenance.as_object_mut() else {
        bail!("discovery observation provenance must be a JSON object");
    };
    object.insert(
        PROPAGATED_ROLE_PROVENANCE_FIELD.to_owned(),
        serde_json::Value::String(from_role.to_owned()),
    );
    Ok(provenance)
}

fn cascade_deactivation_terminal_states(
    existing_edges: &[ExistingReconciledDiscoveryEdge],
    desired_set: &HashSet<ReconciledDiscoveryEdgeSpec>,
    observations_by_key: &HashMap<String, &DiscoveryObservation>,
    direct_terminal_states_by_key: &HashMap<String, ObservationTerminalState>,
) -> Result<HashMap<String, ObservationTerminalState>> {
    let mut terminal_states_by_key = HashMap::<String, ObservationTerminalState>::new();
    let mut removed_parent_addresses = HashMap::<String, ObservationTerminalState>::new();

    for existing_edge in existing_edges
        .iter()
        .filter(|edge| !desired_set.contains(&edge.spec))
    {
        let Some(observation) = observations_by_key.get(&existing_edge.spec.observation_key) else {
            continue;
        };
        let Some(terminal_state) = direct_terminal_states_by_key
            .get(&existing_edge.spec.observation_key)
            .cloned()
        else {
            continue;
        };
        let next_address = normalize_address(&observation.to_address);
        if !is_zero_address(&next_address) && next_address == existing_edge.to_address {
            continue;
        }

        terminal_states_by_key.insert(
            existing_edge.spec.observation_key.clone(),
            terminal_state.clone(),
        );
        removed_parent_addresses.insert(existing_edge.to_address.clone(), terminal_state);
    }

    let mut changed = true;
    while changed {
        changed = false;

        for existing_edge in existing_edges
            .iter()
            .filter(|edge| !desired_set.contains(&edge.spec))
        {
            if terminal_states_by_key.contains_key(&existing_edge.spec.observation_key) {
                continue;
            }
            let Some(observation) = observations_by_key.get(&existing_edge.spec.observation_key)
            else {
                continue;
            };
            let parent_address = normalize_address(&observation.from_address);
            let Some(terminal_state) = removed_parent_addresses.get(&parent_address).cloned()
            else {
                continue;
            };

            terminal_states_by_key.insert(
                existing_edge.spec.observation_key.clone(),
                terminal_state.clone(),
            );
            removed_parent_addresses.insert(existing_edge.to_address.clone(), terminal_state);
            changed = true;
        }
    }

    Ok(terminal_states_by_key)
}

pub async fn reconcile_discovery_observations(
    pool: &PgPool,
    discovery_source: &str,
    observations: &[DiscoveryObservation],
) -> Result<DiscoveryReconciliationSummary> {
    let admission_state =
        load_discovery_admission_state_with_excluded_source(pool, Some(discovery_source)).await?;
    let direct_terminal_states_by_key = observation_terminal_states(observations)?;
    let observations_by_key = observations
        .iter()
        .map(|observation| Ok((observation_key(observation)?, observation)))
        .collect::<Result<HashMap<_, _>>>()?;
    let mut transaction = pool
        .begin()
        .await
        .context("failed to start discovery-edge reconciliation transaction")?;

    let (desired_edges, admitted_edges) = resolve_reconciled_discovery_edge_specs(
        &admission_state,
        transaction.as_mut(),
        observations,
    )
    .await?;
    let existing_rows = sqlx::query(
        r#"
        SELECT
            de.discovery_edge_id,
            de.provenance ->> 'observation_key' AS observation_key,
            de.chain_id,
            de.edge_kind,
            de.from_contract_instance_id,
            de.to_contract_instance_id,
            de.discovery_source,
            de.source_manifest_id,
            de.admission,
            de.active_from_block_number,
            de.active_from_block_hash,
            de.provenance,
            cia.address AS to_address
        FROM discovery_edges de
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = de.to_contract_instance_id
         AND cia.deactivated_at IS NULL
        WHERE de.discovery_source = $1
          AND de.deactivated_at IS NULL
        "#,
    )
    .bind(discovery_source)
    .fetch_all(transaction.as_mut())
    .await
    .with_context(|| {
        format!("failed to load active discovery edges for discovery_source {discovery_source}")
    })?;

    let existing_edges = existing_rows
        .into_iter()
        .map(|row| {
            let observation_key = row
                .try_get::<Option<String>, _>("observation_key")
                .context("failed to read observation_key")?
                .context(
                    "active reconciled discovery edge is missing provenance.observation_key",
                )?;
            Ok(ExistingReconciledDiscoveryEdge {
                discovery_edge_id: row
                    .try_get("discovery_edge_id")
                    .context("failed to read discovery_edge_id")?,
                to_address: normalize_address(
                    &row.try_get::<String, _>("to_address")
                        .context("failed to read to_address")?,
                ),
                spec: ReconciledDiscoveryEdgeSpec {
                    observation_key,
                    chain: row.try_get("chain_id").context("failed to read chain_id")?,
                    edge_kind: row
                        .try_get("edge_kind")
                        .context("failed to read edge_kind")?,
                    from_contract_instance_id: row
                        .try_get("from_contract_instance_id")
                        .context("failed to read from_contract_instance_id")?,
                    to_contract_instance_id: row
                        .try_get("to_contract_instance_id")
                        .context("failed to read to_contract_instance_id")?,
                    discovery_source: row
                        .try_get("discovery_source")
                        .context("failed to read discovery_source")?,
                    source_manifest_id: row
                        .try_get::<Option<i64>, _>("source_manifest_id")
                        .context("failed to read source_manifest_id")?
                        .unwrap_or(-1),
                    admission: row
                        .try_get("admission")
                        .context("failed to read admission")?,
                    active_from_block_number: row
                        .try_get("active_from_block_number")
                        .context("failed to read active_from_block_number")?,
                    active_from_block_hash: row
                        .try_get("active_from_block_hash")
                        .context("failed to read active_from_block_hash")?,
                    provenance_json: row
                        .try_get::<serde_json::Value, _>("provenance")
                        .context("failed to read provenance")?
                        .to_string(),
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let desired_set = desired_edges.iter().cloned().collect::<HashSet<_>>();
    let existing_set = existing_edges
        .iter()
        .map(|edge| edge.spec.clone())
        .collect::<HashSet<_>>();
    let deactivation_terminal_states_by_key = cascade_deactivation_terminal_states(
        &existing_edges,
        &desired_set,
        &observations_by_key,
        &direct_terminal_states_by_key,
    )?;

    let mut deactivated_edge_count = 0;
    for existing_edge in existing_edges {
        if desired_set.contains(&existing_edge.spec) {
            continue;
        }

        let terminal_state =
            deactivation_terminal_states_by_key.get(&existing_edge.spec.observation_key);

        sqlx::query(
            r#"
            UPDATE discovery_edges
            SET active_to_block_number = COALESCE($2, active_to_block_number),
                active_to_block_hash = COALESCE($3, active_to_block_hash),
                deactivated_at = COALESCE(
                    (
                        SELECT GREATEST(discovery_edges.admitted_at, rb.block_timestamp)
                        FROM raw_blocks rb
                        WHERE rb.chain_id = $4
                          AND rb.block_hash = $3
                        LIMIT 1
                    ),
                    now()
                )
            WHERE discovery_edge_id = $1
              AND deactivated_at IS NULL
            "#,
        )
        .bind(existing_edge.discovery_edge_id)
        .bind(terminal_state.and_then(|state| state.block_number))
        .bind(terminal_state.and_then(|state| state.block_hash.as_deref()))
        .bind(terminal_state.map(|state| state.chain.as_str()))
        .execute(transaction.as_mut())
        .await
        .with_context(|| {
            format!(
                "failed to deactivate reconciled discovery_edge_id {}",
                existing_edge.discovery_edge_id
            )
        })?;
        deactivated_edge_count += 1;
    }

    let mut inserted_edge_count = 0;
    for desired_edge in &desired_edges {
        if existing_set.contains(desired_edge) {
            continue;
        }

        sqlx::query(
            r#"
            INSERT INTO discovery_edges (
                chain_id,
                edge_kind,
                from_contract_instance_id,
                to_contract_instance_id,
                discovery_source,
                source_manifest_id,
                admission,
                active_from_block_number,
                active_from_block_hash,
                active_to_block_number,
                active_to_block_hash,
                provenance
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, NULL, $10::jsonb)
            "#,
        )
        .bind(&desired_edge.chain)
        .bind(&desired_edge.edge_kind)
        .bind(desired_edge.from_contract_instance_id)
        .bind(desired_edge.to_contract_instance_id)
        .bind(&desired_edge.discovery_source)
        .bind(desired_edge.source_manifest_id)
        .bind(&desired_edge.admission)
        .bind(desired_edge.active_from_block_number)
        .bind(desired_edge.active_from_block_hash.as_deref())
        .bind(&desired_edge.provenance_json)
        .execute(transaction.as_mut())
        .await
        .with_context(|| {
            format!(
                "failed to insert reconciled discovery edge {} {} -> {}",
                desired_edge.edge_kind,
                desired_edge.from_contract_instance_id,
                desired_edge.to_contract_instance_id
            )
        })?;
        inserted_edge_count += 1;
    }

    reconcile_active_contract_instance_addresses(transaction.as_mut()).await?;

    transaction
        .commit()
        .await
        .context("failed to commit discovery-edge reconciliation transaction")?;

    Ok(DiscoveryReconciliationSummary {
        active_edge_count: desired_edges.len(),
        admitted_edge_count: admitted_edges.len(),
        inserted_edge_count,
        deactivated_edge_count,
        admitted_edges,
    })
}

async fn resolve_reconciled_discovery_edge_specs(
    admission_state: &DiscoveryAdmissionState,
    executor: &mut sqlx::postgres::PgConnection,
    observations: &[DiscoveryObservation],
) -> Result<(Vec<ReconciledDiscoveryEdgeSpec>, Vec<AdmittedDiscoveryEdge>)> {
    let mut desired_edges = HashSet::new();
    let mut admitted_edges = HashSet::new();
    let mut active_contracts = admission_state.active_contracts.clone();

    loop {
        let mut changed = false;

        for observation in observations {
            let observation_key = observation_key(observation)?;
            if is_zero_address(&observation.to_address) {
                continue;
            }

            for mut admitted_edge in admission_state
                .admit_candidate_against_contracts(&active_contracts, &observation.candidate())
            {
                let to_contract_instance_id = match admitted_edge.to_contract_instance_id {
                    Some(contract_instance_id) => contract_instance_id,
                    None => {
                        resolve_contract_instance_by_address(
                            executor,
                            &admitted_edge.chain,
                            &admitted_edge.to_address,
                            CONTRACT_KIND_CONTRACT,
                            &serde_json::json!({
                                "source": "discovery_observation",
                                "edge_kind": admitted_edge.edge_kind,
                                "discovery_source": admitted_edge.discovery_source,
                            }),
                        )
                        .await?
                    }
                };
                admitted_edge.to_contract_instance_id = Some(to_contract_instance_id);
                ensure_contract_instance_address_seed(
                    executor,
                    to_contract_instance_id,
                    &admitted_edge.chain,
                    &admitted_edge.to_address,
                    Some(admitted_edge.source_manifest_id),
                    &serde_json::json!({
                        "source": "discovery_observation_seed",
                        "edge_kind": admitted_edge.edge_kind,
                        "discovery_source": admitted_edge.discovery_source,
                    }),
                )
                .await?;

                let provenance =
                    with_propagated_role(&observation.provenance, &admitted_edge.from_role)?;
                let desired_edge = ReconciledDiscoveryEdgeSpec {
                    observation_key: observation_key.clone(),
                    chain: admitted_edge.chain.clone(),
                    edge_kind: admitted_edge.edge_kind.clone(),
                    from_contract_instance_id: admitted_edge.from_contract_instance_id,
                    to_contract_instance_id,
                    discovery_source: admitted_edge.discovery_source.clone(),
                    source_manifest_id: admitted_edge.source_manifest_id,
                    admission: admitted_edge.admission.clone(),
                    active_from_block_number: observation.active_from_block_number,
                    active_from_block_hash: observation.active_from_block_hash.clone(),
                    provenance_json: serde_json::to_string(&provenance)
                        .context("failed to serialize reconciled discovery-edge provenance")?,
                };
                changed |= desired_edges.insert(desired_edge);
                changed |= admitted_edges.insert(admitted_edge.clone());

                if admitted_edge.admission == REACHABLE_FROM_ROOT_ADMISSION {
                    let derived_contract = StoredActiveContract {
                        manifest_id: admitted_edge.source_manifest_id,
                        chain: admitted_edge.chain.clone(),
                        role: admitted_edge.from_role.clone(),
                        contract_instance_id: to_contract_instance_id,
                        address: admitted_edge.to_address.clone(),
                    };
                    if !active_contracts.contains(&derived_contract) {
                        active_contracts.push(derived_contract);
                        changed = true;
                    }
                }
            }
        }

        if !changed {
            break;
        }
    }

    let mut desired_edges = desired_edges.into_iter().collect::<Vec<_>>();
    desired_edges.sort_by(|left, right| {
        (
            left.observation_key.as_str(),
            left.chain.as_str(),
            left.edge_kind.as_str(),
            left.from_contract_instance_id,
            left.to_contract_instance_id,
            left.discovery_source.as_str(),
            left.source_manifest_id,
            left.admission.as_str(),
            left.active_from_block_number,
            left.active_from_block_hash.as_deref(),
            left.provenance_json.as_str(),
        )
            .cmp(&(
                right.observation_key.as_str(),
                right.chain.as_str(),
                right.edge_kind.as_str(),
                right.from_contract_instance_id,
                right.to_contract_instance_id,
                right.discovery_source.as_str(),
                right.source_manifest_id,
                right.admission.as_str(),
                right.active_from_block_number,
                right.active_from_block_hash.as_deref(),
                right.provenance_json.as_str(),
            ))
    });
    let mut admitted_edges = admitted_edges.into_iter().collect::<Vec<_>>();
    admitted_edges.sort_by(|left, right| {
        (
            left.source_manifest_id,
            left.chain.as_str(),
            left.from_contract_instance_id,
            left.to_contract_instance_id,
            left.to_address.as_str(),
            left.edge_kind.as_str(),
            left.discovery_source.as_str(),
            left.admission.as_str(),
            left.from_role.as_str(),
        )
            .cmp(&(
                right.source_manifest_id,
                right.chain.as_str(),
                right.from_contract_instance_id,
                right.to_contract_instance_id,
                right.to_address.as_str(),
                right.edge_kind.as_str(),
                right.discovery_source.as_str(),
                right.admission.as_str(),
                right.from_role.as_str(),
            ))
    });

    Ok((desired_edges, admitted_edges))
}

pub async fn load_watched_contracts(pool: &PgPool) -> Result<Vec<WatchedContract>> {
    let rows = sqlx::query(
        r#"
        SELECT chain, address, contract_instance_id, source, source_manifest_id
        FROM (
            SELECT
                mv.chain AS chain,
                cia.address AS address,
                mci.contract_instance_id AS contract_instance_id,
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
                cia.address AS address,
                de.to_contract_instance_id AS contract_instance_id,
                'discovery_edge'::TEXT AS source,
                de.source_manifest_id AS source_manifest_id
            FROM discovery_edges de
            JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = de.to_contract_instance_id
             AND cia.deactivated_at IS NULL
            WHERE mv.rollout_status = 'active'
              AND de.deactivated_at IS NULL
              AND de.edge_kind <> 'migration'
        ) watched_contracts
        ORDER BY chain, address, source, source_manifest_id, contract_instance_id
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
                contract_instance_id: row
                    .try_get("contract_instance_id")
                    .context("failed to read watched contract_instance_id")?,
                source: WatchedContractSource::from_db_value(&source)?,
                source_manifest_id: row
                    .try_get("source_manifest_id")
                    .context("failed to read watched contract source_manifest_id")?,
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

pub async fn load_watched_contract_summary(pool: &PgPool) -> Result<WatchedContractSummary> {
    let watched_contracts = load_watched_contracts(pool).await?;
    Ok(summarize_watched_contracts(&watched_contracts))
}

pub async fn load_watched_chain_plan(pool: &PgPool) -> Result<Vec<WatchedChainPlan>> {
    let watched_contracts = load_watched_contracts(pool).await?;
    Ok(plan_watched_contracts(&watched_contracts))
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct StoredActiveRoot {
    manifest_id: i64,
    chain: String,
    contract_instance_id: Uuid,
    address: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct StoredActiveContract {
    manifest_id: i64,
    chain: String,
    role: String,
    contract_instance_id: Uuid,
    address: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StoredDiscoveryRule {
    edge_kind: String,
    from_role: String,
    admission: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiscoveryCandidate<'a> {
    pub chain: &'a str,
    pub from_address: &'a str,
    pub to_address: &'a str,
    pub edge_kind: &'a str,
    pub discovery_source: &'a str,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AdmittedDiscoveryEdge {
    pub source_manifest_id: i64,
    pub chain: String,
    pub from_contract_instance_id: Uuid,
    pub to_contract_instance_id: Option<Uuid>,
    pub from_address: String,
    pub to_address: String,
    pub edge_kind: String,
    pub discovery_source: String,
    pub admission: String,
    pub from_role: String,
}

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryPersistenceSummary {
    pub admitted_edge_count: usize,
    pub inserted_edge_count: usize,
    pub admitted_edges: Vec<AdmittedDiscoveryEdge>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryReconciliationSummary {
    pub active_edge_count: usize,
    pub admitted_edge_count: usize,
    pub inserted_edge_count: usize,
    pub deactivated_edge_count: usize,
    pub admitted_edges: Vec<AdmittedDiscoveryEdge>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WatchedContract {
    pub chain: String,
    pub address: String,
    pub contract_instance_id: Uuid,
    pub source: WatchedContractSource,
    pub source_manifest_id: Option<i64>,
}

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchedContractSummary {
    pub unique_contract_count: usize,
    pub source_entry_count: usize,
    pub manifest_root_count: usize,
    pub manifest_contract_count: usize,
    pub discovery_edge_count: usize,
    pub chains: Vec<WatchedContractChainSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchedChainPlan {
    pub chain: String,
    pub addresses: Vec<String>,
    pub manifest_root_entry_count: usize,
    pub manifest_contract_entry_count: usize,
    pub discovery_edge_entry_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchedContractChainSummary {
    pub chain: String,
    pub unique_contract_count: usize,
    pub manifest_root_count: usize,
    pub manifest_contract_count: usize,
    pub discovery_edge_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ActiveManifestVersion {
    pub manifest_version: u64,
    pub source_family: String,
    pub chain: String,
    pub deployment_epoch: String,
    pub normalizer_version: String,
    pub capability_flags: BTreeMap<String, CapabilityFlag>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceManifestSnapshot {
    pub manifests: Vec<ActiveManifestVersion>,
    pub last_updated: String,
}

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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CapabilityFlag {
    pub status: CapabilitySupportStatus,
    pub notes: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ManifestRoot {
    pub name: String,
    pub address: String,
    pub code_hash: Option<String>,
    pub abi_ref: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ManifestContract {
    pub role: String,
    pub address: String,
    pub proxy_kind: String,
    pub implementation: Option<String>,
}

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

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DeclarationKey {
    declaration_kind: String,
    declaration_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PersistedManifestEntry {
    key: DeclarationKey,
    contract_instance_id: Uuid,
    declared_address: String,
    code_hash: Option<String>,
    abi_ref: Option<String>,
    role: Option<String>,
    proxy_kind: Option<String>,
    implementation_contract_instance_id: Option<Uuid>,
    declared_implementation_address: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ManifestTransition {
    source_manifest_id: i64,
    chain: String,
    declaration_kind: String,
    declaration_name: String,
    from_contract_instance_id: Uuid,
    from_address: String,
    to_contract_instance_id: Uuid,
    to_address: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ManagedEdgeSpec {
    chain: String,
    edge_kind: String,
    from_contract_instance_id: Uuid,
    to_contract_instance_id: Uuid,
    discovery_source: String,
    source_manifest_id: i64,
    admission: String,
    provenance_json: String,
}

#[derive(Clone, Debug)]
struct ExistingManagedEdge {
    discovery_edge_id: i64,
    spec: ManagedEdgeSpec,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ReconciledDiscoveryEdgeSpec {
    observation_key: String,
    chain: String,
    edge_kind: String,
    from_contract_instance_id: Uuid,
    to_contract_instance_id: Uuid,
    discovery_source: String,
    source_manifest_id: i64,
    admission: String,
    active_from_block_number: Option<i64>,
    active_from_block_hash: Option<String>,
    provenance_json: String,
}

#[derive(Clone, Debug)]
struct ExistingReconciledDiscoveryEdge {
    discovery_edge_id: i64,
    spec: ReconciledDiscoveryEdgeSpec,
    to_address: String,
}

#[derive(Clone, Debug)]
struct ObservationTerminalState {
    chain: String,
    block_number: Option<i64>,
    block_hash: Option<String>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ActiveAddressSpec {
    contract_instance_id: Uuid,
    chain: String,
    address: String,
    source_manifest_id: Option<i64>,
    provenance_json: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ManifestLineageKey {
    namespace: String,
    source_family: String,
    chain: String,
    deployment_epoch: String,
    declaration_kind: String,
    declaration_name: String,
}

#[derive(Clone, Debug)]
struct OrderedManifestEntry {
    manifest_id: i64,
    manifest_version: i64,
    rollout_status: String,
    chain: String,
    lineage_key: ManifestLineageKey,
    contract_instance_id: Uuid,
    declared_address: String,
}

#[derive(Clone, Debug)]
struct CurrentActiveAddressRow {
    contract_instance_id: Uuid,
    chain: String,
    address: String,
}

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
    let mut in_place_transitions = Vec::new();
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
        let existing_entries =
            load_existing_manifest_entries(transaction.as_mut(), manifest_id).await?;
        let planned_entries = plan_manifest_entries(
            transaction.as_mut(),
            manifest_id,
            loaded_manifest,
            &existing_entries,
        )
        .await?;

        if loaded_manifest.manifest.rollout_status.is_active() {
            for planned_entry in &planned_entries {
                if let Some(existing_entry) = existing_entries.get(&planned_entry.key)
                    && existing_entry.contract_instance_id != planned_entry.contract_instance_id
                {
                    in_place_transitions.push(ManifestTransition {
                        source_manifest_id: manifest_id,
                        chain: loaded_manifest.manifest.chain.clone(),
                        declaration_kind: planned_entry.key.declaration_kind.clone(),
                        declaration_name: planned_entry.key.declaration_name.clone(),
                        from_contract_instance_id: existing_entry.contract_instance_id,
                        from_address: existing_entry.declared_address.clone(),
                        to_contract_instance_id: planned_entry.contract_instance_id,
                        to_address: planned_entry.declared_address.clone(),
                    });
                }
            }
        }

        replace_manifest_children(
            transaction.as_mut(),
            manifest_id,
            &loaded_manifest.manifest,
            &planned_entries,
        )
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

    sync_summary.cleared_discovery_edge_count =
        reconcile_manifest_source_graph(transaction.as_mut(), &in_place_transitions).await?;

    transaction
        .commit()
        .await
        .context("failed to commit manifest sync transaction")?;

    Ok(sync_summary)
}

pub async fn load_discovery_admission_state(pool: &PgPool) -> Result<DiscoveryAdmissionState> {
    load_discovery_admission_state_with_excluded_source(pool, None).await
}

async fn load_discovery_admission_state_with_excluded_source(
    pool: &PgPool,
    excluded_discovery_source: Option<&str>,
) -> Result<DiscoveryAdmissionState> {
    let active_manifest_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT FROM manifest_versions WHERE rollout_status = 'active'",
    )
    .fetch_one(pool)
    .await
    .context("failed to count active manifest versions")? as usize;

    let active_root_rows = sqlx::query(
        r#"
        SELECT mv.manifest_id, mv.chain, mci.contract_instance_id, cia.address
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = mci.contract_instance_id
         AND cia.deactivated_at IS NULL
        WHERE mv.rollout_status = 'active'
          AND mci.declaration_kind = 'root'
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load active manifest roots")?;

    let active_contract_rows = sqlx::query(
        r#"
        SELECT mv.manifest_id, mv.chain, mci.role, mci.contract_instance_id, cia.address
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = mci.contract_instance_id
         AND cia.deactivated_at IS NULL
        WHERE mv.rollout_status = 'active'
          AND mci.declaration_kind = 'contract'
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load active manifest contracts")?;

    let active_discovered_parent_rows = sqlx::query(
        r#"
        SELECT
            mv.manifest_id,
            mv.chain,
            de.provenance ->> 'propagated_role' AS role,
            de.to_contract_instance_id AS contract_instance_id,
            cia.address AS address
        FROM discovery_edges de
        JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = de.to_contract_instance_id
         AND cia.deactivated_at IS NULL
        WHERE mv.rollout_status = 'active'
          AND de.deactivated_at IS NULL
          AND de.edge_kind <> 'migration'
          AND de.admission = $1
          AND de.provenance ? $2
          AND ($3::TEXT IS NULL OR de.discovery_source <> $3)
        "#,
    )
    .bind(REACHABLE_FROM_ROOT_ADMISSION)
    .bind(PROPAGATED_ROLE_PROVENANCE_FIELD)
    .bind(excluded_discovery_source)
    .fetch_all(pool)
    .await
    .context("failed to load active transitive discovery parents")?;

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

    let known_address_rows = sqlx::query(
        r#"
        SELECT chain_id, address, contract_instance_id
        FROM contract_instance_addresses
        ORDER BY chain_id, address, (deactivated_at IS NULL) DESC, admitted_at DESC
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load known contract-instance addresses")?;

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
                contract_instance_id: row
                    .try_get("contract_instance_id")
                    .context("failed to read active root contract_instance_id")?,
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
        .chain(active_discovered_parent_rows)
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
                contract_instance_id: row
                    .try_get("contract_instance_id")
                    .context("failed to read active contract contract_instance_id")?,
                address: normalize_address(
                    &row.try_get::<String, _>("address")
                        .context("failed to read active contract address")?,
                ),
            })
        })
        .collect::<Result<HashSet<_>>>()?
        .into_iter()
        .collect::<Vec<_>>();

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

    let mut known_contract_instances_by_address = HashMap::new();
    for row in known_address_rows {
        let chain = row
            .try_get::<String, _>("chain_id")
            .context("failed to read known address chain_id")?;
        let address = normalize_address(
            &row.try_get::<String, _>("address")
                .context("failed to read known address")?,
        );
        known_contract_instances_by_address
            .entry((chain, address))
            .or_insert(
                row.try_get("contract_instance_id")
                    .context("failed to read known address contract_instance_id")?,
            );
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
        known_contract_instances_by_address,
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

async fn load_existing_manifest_entries(
    executor: &mut sqlx::postgres::PgConnection,
    manifest_id: i64,
) -> Result<HashMap<DeclarationKey, PersistedManifestEntry>> {
    let rows = sqlx::query(
        r#"
        SELECT
            declaration_kind,
            declaration_name,
            contract_instance_id,
            declared_address,
            code_hash,
            abi_ref,
            role,
            proxy_kind,
            implementation_contract_instance_id,
            declared_implementation_address
        FROM manifest_contract_instances
        WHERE manifest_id = $1
        "#,
    )
    .bind(manifest_id)
    .fetch_all(executor)
    .await
    .with_context(|| {
        format!("failed to load existing manifest children for manifest_id {manifest_id}")
    })?;

    rows.into_iter()
        .map(|row| {
            let declaration_kind = row
                .try_get::<String, _>("declaration_kind")
                .context("failed to read declaration_kind")?;
            let declaration_name = row
                .try_get::<String, _>("declaration_name")
                .context("failed to read declaration_name")?;
            let entry = PersistedManifestEntry {
                key: DeclarationKey {
                    declaration_kind: declaration_kind.clone(),
                    declaration_name: declaration_name.clone(),
                },
                contract_instance_id: row
                    .try_get("contract_instance_id")
                    .context("failed to read contract_instance_id")?,
                declared_address: row
                    .try_get("declared_address")
                    .context("failed to read declared_address")?,
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
                declared_implementation_address: row
                    .try_get("declared_implementation_address")
                    .context("failed to read declared_implementation_address")?,
            };
            Ok((entry.key.clone(), entry))
        })
        .collect()
}

async fn plan_manifest_entries(
    executor: &mut sqlx::postgres::PgConnection,
    manifest_id: i64,
    loaded_manifest: &LoadedManifest,
    existing_entries: &HashMap<DeclarationKey, PersistedManifestEntry>,
) -> Result<Vec<PersistedManifestEntry>> {
    let mut planned_entries = Vec::new();
    let mut planned_contract_instance_ids_by_address = HashMap::<String, Uuid>::new();

    for root in &loaded_manifest.manifest.roots {
        let key = DeclarationKey {
            declaration_kind: DECLARATION_KIND_ROOT.to_owned(),
            declaration_name: root.name.clone(),
        };
        let declared_address = normalize_address(&root.address);
        let contract_instance_id =
            match planned_contract_instance_ids_by_address.get(&declared_address) {
                Some(contract_instance_id) => *contract_instance_id,
                None => {
                    let contract_instance_id = resolve_manifest_entry_contract_instance_id(
                        executor,
                        manifest_id,
                        loaded_manifest,
                        &key,
                        &declared_address,
                        existing_entries.get(&key),
                        CONTRACT_KIND_ROOT,
                    )
                    .await?;
                    planned_contract_instance_ids_by_address
                        .insert(declared_address.clone(), contract_instance_id);
                    contract_instance_id
                }
            };

        planned_entries.push(PersistedManifestEntry {
            key,
            contract_instance_id,
            declared_address,
            code_hash: root.code_hash.clone(),
            abi_ref: root.abi_ref.clone(),
            role: None,
            proxy_kind: None,
            implementation_contract_instance_id: None,
            declared_implementation_address: None,
        });
    }

    for contract in &loaded_manifest.manifest.contracts {
        let key = DeclarationKey {
            declaration_kind: DECLARATION_KIND_CONTRACT.to_owned(),
            declaration_name: contract.role.clone(),
        };
        let declared_address = normalize_address(&contract.address);
        let contract_instance_id =
            match planned_contract_instance_ids_by_address.get(&declared_address) {
                Some(contract_instance_id) => *contract_instance_id,
                None => {
                    let contract_instance_id = resolve_manifest_entry_contract_instance_id(
                        executor,
                        manifest_id,
                        loaded_manifest,
                        &key,
                        &declared_address,
                        existing_entries.get(&key),
                        CONTRACT_KIND_CONTRACT,
                    )
                    .await?;
                    planned_contract_instance_ids_by_address
                        .insert(declared_address.clone(), contract_instance_id);
                    contract_instance_id
                }
            };

        let declared_implementation_address = contract
            .implementation
            .as_ref()
            .map(|value| normalize_address(value));
        if declared_implementation_address.as_deref() == Some(declared_address.as_str()) {
            bail!(
                "manifest contract role {} in {} cannot declare the proxy address as its own implementation",
                contract.role,
                loaded_manifest.path.display()
            );
        }
        let implementation_contract_instance_id =
            if let Some(implementation_address) = &declared_implementation_address {
                Some(
                    resolve_contract_instance_by_address(
                        executor,
                        &loaded_manifest.manifest.chain,
                        implementation_address,
                        CONTRACT_KIND_CONTRACT,
                        &serde_json::json!({
                            "source": "manifest_contract_implementation",
                            "manifest_id": manifest_id,
                            "role": contract.role,
                        }),
                    )
                    .await?,
                )
            } else {
                None
            };

        planned_entries.push(PersistedManifestEntry {
            key,
            contract_instance_id,
            declared_address,
            code_hash: None,
            abi_ref: None,
            role: Some(contract.role.clone()),
            proxy_kind: Some(contract.proxy_kind.clone()),
            implementation_contract_instance_id,
            declared_implementation_address,
        });
    }

    Ok(planned_entries)
}

async fn resolve_manifest_entry_contract_instance_id(
    executor: &mut sqlx::postgres::PgConnection,
    manifest_id: i64,
    loaded_manifest: &LoadedManifest,
    key: &DeclarationKey,
    declared_address: &str,
    existing_entry: Option<&PersistedManifestEntry>,
    contract_kind: &str,
) -> Result<Uuid> {
    if let Some(existing_entry) = existing_entry
        && existing_entry.declared_address == declared_address
    {
        return Ok(existing_entry.contract_instance_id);
    }

    if let Some(previous_entry) =
        load_latest_related_manifest_entry(executor, manifest_id, loaded_manifest, key).await?
        && previous_entry.declared_address == declared_address
    {
        return Ok(previous_entry.contract_instance_id);
    }

    resolve_contract_instance_by_address(
        executor,
        &loaded_manifest.manifest.chain,
        declared_address,
        contract_kind,
        &serde_json::json!({
            "source": "manifest_declaration",
            "manifest_id": manifest_id,
            "declaration_kind": key.declaration_kind,
            "declaration_name": key.declaration_name,
        }),
    )
    .await
}

async fn load_latest_related_manifest_entry(
    executor: &mut sqlx::postgres::PgConnection,
    manifest_id: i64,
    loaded_manifest: &LoadedManifest,
    key: &DeclarationKey,
) -> Result<Option<PersistedManifestEntry>> {
    let row = sqlx::query(
        r#"
        SELECT
            mci.contract_instance_id,
            mci.declared_address,
            mci.code_hash,
            mci.abi_ref,
            mci.role,
            mci.proxy_kind,
            mci.implementation_contract_instance_id,
            mci.declared_implementation_address
        FROM manifest_contract_instances mci
        JOIN manifest_versions mv ON mv.manifest_id = mci.manifest_id
        WHERE mv.namespace = $1
          AND mv.source_family = $2
          AND mv.chain = $3
          AND mv.deployment_epoch = $4
          AND mci.declaration_kind = $5
          AND mci.declaration_name = $6
          AND mci.manifest_id <> $7
        ORDER BY mv.manifest_version DESC, mci.manifest_contract_instance_id DESC
        LIMIT 1
        "#,
    )
    .bind(&loaded_manifest.manifest.namespace)
    .bind(&loaded_manifest.manifest.source_family)
    .bind(&loaded_manifest.manifest.chain)
    .bind(&loaded_manifest.manifest.deployment_epoch)
    .bind(&key.declaration_kind)
    .bind(&key.declaration_name)
    .bind(manifest_id)
    .fetch_optional(executor)
    .await
    .with_context(|| {
        format!(
            "failed to load prior declaration state for {} {}",
            key.declaration_kind, key.declaration_name
        )
    })?;

    row.map(|row| {
        Ok(PersistedManifestEntry {
            key: key.clone(),
            contract_instance_id: row
                .try_get("contract_instance_id")
                .context("failed to read prior contract_instance_id")?,
            declared_address: row
                .try_get("declared_address")
                .context("failed to read prior declared_address")?,
            code_hash: row
                .try_get("code_hash")
                .context("failed to read prior code_hash")?,
            abi_ref: row
                .try_get("abi_ref")
                .context("failed to read prior abi_ref")?,
            role: row.try_get("role").context("failed to read prior role")?,
            proxy_kind: row
                .try_get("proxy_kind")
                .context("failed to read prior proxy_kind")?,
            implementation_contract_instance_id: row
                .try_get("implementation_contract_instance_id")
                .context("failed to read prior implementation_contract_instance_id")?,
            declared_implementation_address: row
                .try_get("declared_implementation_address")
                .context("failed to read prior declared_implementation_address")?,
        })
    })
    .transpose()
}

async fn resolve_contract_instance_by_address(
    executor: &mut sqlx::postgres::PgConnection,
    chain: &str,
    address: &str,
    contract_kind: &str,
    provenance: &serde_json::Value,
) -> Result<Uuid> {
    let normalized_address = normalize_address(address);

    if let Some(contract_instance_id) =
        find_contract_instance_by_address(executor, chain, &normalized_address).await?
    {
        return Ok(contract_instance_id);
    }

    let contract_instance_id = Uuid::new_v4();
    let provenance = serde_json::to_string(provenance)
        .context("failed to serialize contract-instance provenance")?;

    sqlx::query(
        r#"
        INSERT INTO contract_instances (
            contract_instance_id,
            chain_id,
            contract_kind,
            provenance
        )
        VALUES ($1, $2, $3, $4::jsonb)
        "#,
    )
    .bind(contract_instance_id)
    .bind(chain)
    .bind(contract_kind)
    .bind(provenance)
    .execute(executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert contract_instance_id {contract_instance_id} for chain {chain} address {normalized_address}"
        )
    })?;

    Ok(contract_instance_id)
}

async fn find_contract_instance_by_address(
    executor: &mut sqlx::postgres::PgConnection,
    chain: &str,
    address: &str,
) -> Result<Option<Uuid>> {
    sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT contract_instance_id
        FROM contract_instance_addresses
        WHERE chain_id = $1
          AND address = $2
        ORDER BY (deactivated_at IS NULL) DESC, admitted_at DESC
        LIMIT 1
        "#,
    )
    .bind(chain)
    .bind(address)
    .fetch_optional(executor)
    .await
    .with_context(|| {
        format!("failed to resolve contract instance for chain {chain} address {address}")
    })
}

async fn ensure_contract_instance_address_seed(
    executor: &mut sqlx::postgres::PgConnection,
    contract_instance_id: Uuid,
    chain: &str,
    address: &str,
    source_manifest_id: Option<i64>,
    provenance: &serde_json::Value,
) -> Result<()> {
    let exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS(
            SELECT 1
            FROM contract_instance_addresses
            WHERE contract_instance_id = $1
        )
        "#,
    )
    .bind(contract_instance_id)
    .fetch_one(&mut *executor)
    .await
    .with_context(|| {
        format!(
            "failed to check seeded address rows for contract_instance_id {contract_instance_id}"
        )
    })?;

    if exists {
        return Ok(());
    }

    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address,
            source_manifest_id,
            provenance
        )
        VALUES ($1, $2, $3, $4, $5::jsonb)
        "#,
    )
    .bind(contract_instance_id)
    .bind(chain)
    .bind(address)
    .bind(source_manifest_id)
    .bind(
        serde_json::to_string(provenance)
            .context("failed to serialize contract-instance address seed provenance")?,
    )
    .execute(&mut *executor)
    .await
    .with_context(|| {
        format!(
            "failed to seed contract-instance address row for contract_instance_id {contract_instance_id}"
        )
    })?;

    Ok(())
}

async fn replace_manifest_children(
    executor: &mut sqlx::postgres::PgConnection,
    manifest_id: i64,
    manifest: &SourceManifest,
    planned_entries: &[PersistedManifestEntry],
) -> Result<()> {
    sqlx::query("DELETE FROM manifest_contract_instances WHERE manifest_id = $1")
        .bind(manifest_id)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!("failed to clear manifest_contract_instances for manifest_id {manifest_id}")
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

    for entry in planned_entries {
        sqlx::query(
            r#"
            INSERT INTO manifest_contract_instances (
                manifest_id,
                declaration_kind,
                declaration_name,
                contract_instance_id,
                declared_address,
                code_hash,
                abi_ref,
                role,
                proxy_kind,
                implementation_contract_instance_id,
                declared_implementation_address
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            "#,
        )
        .bind(manifest_id)
        .bind(&entry.key.declaration_kind)
        .bind(&entry.key.declaration_name)
        .bind(entry.contract_instance_id)
        .bind(&entry.declared_address)
        .bind(entry.code_hash.as_deref())
        .bind(entry.abi_ref.as_deref())
        .bind(entry.role.as_deref())
        .bind(entry.proxy_kind.as_deref())
        .bind(entry.implementation_contract_instance_id)
        .bind(entry.declared_implementation_address.as_deref())
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to insert manifest entry {} {} for manifest_id {manifest_id}",
                entry.key.declaration_kind, entry.key.declaration_name
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

async fn reconcile_manifest_source_graph(
    executor: &mut sqlx::postgres::PgConnection,
    in_place_transitions: &[ManifestTransition],
) -> Result<usize> {
    let desired_proxy_edges = load_desired_proxy_edges(executor).await?;
    let desired_successor_edges =
        load_desired_manifest_successor_edges(executor, in_place_transitions).await?;

    let mut cleared_edge_count = 0;
    cleared_edge_count += reconcile_managed_edges(
        executor,
        &desired_proxy_edges,
        MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE,
    )
    .await?;
    cleared_edge_count += reconcile_managed_edges(
        executor,
        &desired_successor_edges,
        MANIFEST_SUCCESSOR_DISCOVERY_SOURCE,
    )
    .await?;

    reconcile_active_contract_instance_addresses(executor).await?;

    Ok(cleared_edge_count)
}

async fn load_desired_proxy_edges(
    executor: &mut sqlx::postgres::PgConnection,
) -> Result<Vec<ManagedEdgeSpec>> {
    let rows = sqlx::query(
        r#"
        SELECT
            mv.manifest_id,
            mv.chain,
            mci.contract_instance_id,
            mci.implementation_contract_instance_id,
            mci.declaration_name,
            mci.proxy_kind,
            mci.declared_address,
            mci.declared_implementation_address
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
        WHERE mv.rollout_status = 'active'
          AND mci.declaration_kind = 'contract'
          AND mci.implementation_contract_instance_id IS NOT NULL
        ORDER BY mv.manifest_id, mci.declaration_name
        "#,
    )
    .fetch_all(executor)
    .await
    .context("failed to load desired proxy edges")?;

    rows.into_iter()
        .map(|row| {
            let implementation_contract_instance_id = row
                .try_get::<Uuid, _>("implementation_contract_instance_id")
                .context("failed to read implementation_contract_instance_id")?;
            let provenance_json = serde_json::json!({
                "source": "manifest_contract",
                "declaration_name": row
                    .try_get::<String, _>("declaration_name")
                    .context("failed to read declaration_name")?,
                "proxy_kind": row
                    .try_get::<String, _>("proxy_kind")
                    .context("failed to read proxy_kind")?,
                "from_address": row
                    .try_get::<String, _>("declared_address")
                    .context("failed to read declared_address")?,
                "to_address": row
                    .try_get::<Option<String>, _>("declared_implementation_address")
                    .context("failed to read declared_implementation_address")?,
            })
            .to_string();
            Ok(ManagedEdgeSpec {
                chain: row.try_get("chain").context("failed to read chain")?,
                edge_kind: MANIFEST_PROXY_IMPLEMENTATION_EDGE_KIND.to_owned(),
                from_contract_instance_id: row
                    .try_get("contract_instance_id")
                    .context("failed to read contract_instance_id")?,
                to_contract_instance_id: implementation_contract_instance_id,
                discovery_source: MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE.to_owned(),
                source_manifest_id: row
                    .try_get("manifest_id")
                    .context("failed to read manifest_id")?,
                admission: MANIFEST_PROXY_IMPLEMENTATION_ADMISSION.to_owned(),
                provenance_json,
            })
        })
        .collect()
}

async fn load_desired_manifest_successor_edges(
    executor: &mut sqlx::postgres::PgConnection,
    in_place_transitions: &[ManifestTransition],
) -> Result<Vec<ManagedEdgeSpec>> {
    let rows = sqlx::query(
        r#"
        SELECT
            mv.manifest_id,
            mv.manifest_version,
            mv.rollout_status::TEXT AS rollout_status,
            mv.namespace,
            mv.source_family,
            mv.chain,
            mv.deployment_epoch,
            mci.declaration_kind,
            mci.declaration_name,
            mci.contract_instance_id,
            mci.declared_address
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
        ORDER BY
            mv.namespace,
            mv.source_family,
            mv.chain,
            mv.deployment_epoch,
            mci.declaration_kind,
            mci.declaration_name,
            mv.manifest_version,
            mv.manifest_id
        "#,
    )
    .fetch_all(executor)
    .await
    .context("failed to load ordered manifest entries for successor continuity")?;

    let mut desired = HashSet::new();
    let mut last_by_lineage = HashMap::<ManifestLineageKey, OrderedManifestEntry>::new();

    for row in rows {
        let entry = OrderedManifestEntry {
            manifest_id: row
                .try_get("manifest_id")
                .context("failed to read manifest_id")?,
            manifest_version: row
                .try_get("manifest_version")
                .context("failed to read manifest_version")?,
            rollout_status: row
                .try_get("rollout_status")
                .context("failed to read rollout_status")?,
            chain: row.try_get("chain").context("failed to read chain")?,
            lineage_key: ManifestLineageKey {
                namespace: row
                    .try_get("namespace")
                    .context("failed to read namespace")?,
                source_family: row
                    .try_get("source_family")
                    .context("failed to read source_family")?,
                chain: row
                    .try_get("chain")
                    .context("failed to read lineage chain")?,
                deployment_epoch: row
                    .try_get("deployment_epoch")
                    .context("failed to read deployment_epoch")?,
                declaration_kind: row
                    .try_get("declaration_kind")
                    .context("failed to read declaration_kind")?,
                declaration_name: row
                    .try_get("declaration_name")
                    .context("failed to read declaration_name")?,
            },
            contract_instance_id: row
                .try_get("contract_instance_id")
                .context("failed to read contract_instance_id")?,
            declared_address: row
                .try_get("declared_address")
                .context("failed to read declared_address")?,
        };

        if let Some(previous_entry) =
            last_by_lineage.insert(entry.lineage_key.clone(), entry.clone())
            && entry.rollout_status == "active"
            && previous_entry.contract_instance_id != entry.contract_instance_id
            && previous_entry.declared_address != entry.declared_address
        {
            desired.insert(ManagedEdgeSpec {
                chain: entry.chain.clone(),
                edge_kind: MANIFEST_SUCCESSOR_EDGE_KIND.to_owned(),
                from_contract_instance_id: previous_entry.contract_instance_id,
                to_contract_instance_id: entry.contract_instance_id,
                discovery_source: MANIFEST_SUCCESSOR_DISCOVERY_SOURCE.to_owned(),
                source_manifest_id: entry.manifest_id,
                admission: MANIFEST_SUCCESSOR_ADMISSION.to_owned(),
                provenance_json: serde_json::json!({
                    "source": "manifest_successor",
                    "declaration_kind": entry.lineage_key.declaration_kind,
                    "declaration_name": entry.lineage_key.declaration_name,
                    "from_address": previous_entry.declared_address,
                    "to_address": entry.declared_address,
                    "manifest_version": entry.manifest_version,
                })
                .to_string(),
            });
        }
    }

    for transition in in_place_transitions {
        desired.insert(ManagedEdgeSpec {
            chain: transition.chain.clone(),
            edge_kind: MANIFEST_SUCCESSOR_EDGE_KIND.to_owned(),
            from_contract_instance_id: transition.from_contract_instance_id,
            to_contract_instance_id: transition.to_contract_instance_id,
            discovery_source: MANIFEST_SUCCESSOR_DISCOVERY_SOURCE.to_owned(),
            source_manifest_id: transition.source_manifest_id,
            admission: MANIFEST_SUCCESSOR_ADMISSION.to_owned(),
            provenance_json: serde_json::json!({
                "source": "manifest_successor",
                "declaration_kind": transition.declaration_kind,
                "declaration_name": transition.declaration_name,
                "from_address": transition.from_address,
                "to_address": transition.to_address,
                "manifest_update": "in_place",
            })
            .to_string(),
        });
    }

    Ok(desired.into_iter().collect())
}

async fn reconcile_managed_edges(
    executor: &mut sqlx::postgres::PgConnection,
    desired_edges: &[ManagedEdgeSpec],
    discovery_source: &str,
) -> Result<usize> {
    let existing_rows = sqlx::query(
        r#"
        SELECT
            discovery_edge_id,
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            source_manifest_id,
            admission,
            provenance
        FROM discovery_edges
        WHERE discovery_source = $1
          AND deactivated_at IS NULL
        "#,
    )
    .bind(discovery_source)
    .fetch_all(&mut *executor)
    .await
    .with_context(|| {
        format!("failed to load active managed edges for discovery_source {discovery_source}")
    })?;

    let existing_edges = existing_rows
        .into_iter()
        .map(|row| {
            Ok(ExistingManagedEdge {
                discovery_edge_id: row
                    .try_get("discovery_edge_id")
                    .context("failed to read discovery_edge_id")?,
                spec: ManagedEdgeSpec {
                    chain: row.try_get("chain_id").context("failed to read chain_id")?,
                    edge_kind: row
                        .try_get("edge_kind")
                        .context("failed to read edge_kind")?,
                    from_contract_instance_id: row
                        .try_get("from_contract_instance_id")
                        .context("failed to read from_contract_instance_id")?,
                    to_contract_instance_id: row
                        .try_get("to_contract_instance_id")
                        .context("failed to read to_contract_instance_id")?,
                    discovery_source: row
                        .try_get("discovery_source")
                        .context("failed to read discovery_source")?,
                    source_manifest_id: row
                        .try_get::<Option<i64>, _>("source_manifest_id")
                        .context("failed to read source_manifest_id")?
                        .unwrap_or(-1),
                    admission: row
                        .try_get("admission")
                        .context("failed to read admission")?,
                    provenance_json: row
                        .try_get::<serde_json::Value, _>("provenance")
                        .context("failed to read provenance")?
                        .to_string(),
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let desired_set = desired_edges.iter().cloned().collect::<HashSet<_>>();
    let existing_set = existing_edges
        .iter()
        .map(|edge| edge.spec.clone())
        .collect::<HashSet<_>>();

    let mut cleared_edge_count = 0;
    for existing_edge in existing_edges {
        if desired_set.contains(&existing_edge.spec) {
            continue;
        }

        sqlx::query(
            r#"
            UPDATE discovery_edges
            SET deactivated_at = now()
            WHERE discovery_edge_id = $1
              AND deactivated_at IS NULL
            "#,
        )
        .bind(existing_edge.discovery_edge_id)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to deactivate managed discovery_edge_id {}",
                existing_edge.discovery_edge_id
            )
        })?;
        cleared_edge_count += 1;
    }

    for desired_edge in desired_edges {
        if existing_set.contains(desired_edge) {
            continue;
        }

        sqlx::query(
            r#"
            INSERT INTO discovery_edges (
                chain_id,
                edge_kind,
                from_contract_instance_id,
                to_contract_instance_id,
                discovery_source,
                source_manifest_id,
                admission,
                provenance
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
            "#,
        )
        .bind(&desired_edge.chain)
        .bind(&desired_edge.edge_kind)
        .bind(desired_edge.from_contract_instance_id)
        .bind(desired_edge.to_contract_instance_id)
        .bind(&desired_edge.discovery_source)
        .bind(desired_edge.source_manifest_id)
        .bind(&desired_edge.admission)
        .bind(&desired_edge.provenance_json)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to insert managed edge {} {} -> {}",
                desired_edge.edge_kind,
                desired_edge.from_contract_instance_id,
                desired_edge.to_contract_instance_id
            )
        })?;
    }

    Ok(cleared_edge_count)
}

async fn reconcile_active_contract_instance_addresses(
    executor: &mut sqlx::postgres::PgConnection,
) -> Result<()> {
    let desired_specs = load_desired_active_address_specs(executor).await?;
    let desired_ids = desired_specs
        .iter()
        .map(|spec| spec.contract_instance_id)
        .collect::<HashSet<_>>();

    let existing_active_rows = sqlx::query(
        r#"
        SELECT contract_instance_id, chain_id, address
        FROM contract_instance_addresses
        WHERE deactivated_at IS NULL
        "#,
    )
    .fetch_all(&mut *executor)
    .await
    .context("failed to load active contract-instance addresses")?;

    let existing_active = existing_active_rows
        .into_iter()
        .map(|row| {
            Ok(CurrentActiveAddressRow {
                contract_instance_id: row
                    .try_get("contract_instance_id")
                    .context("failed to read active contract_instance_id")?,
                chain: row
                    .try_get("chain_id")
                    .context("failed to read active chain_id")?,
                address: row
                    .try_get("address")
                    .context("failed to read active address")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    for existing_row in &existing_active {
        if desired_ids.contains(&existing_row.contract_instance_id) {
            continue;
        }

        sqlx::query(
            r#"
            UPDATE contract_instance_addresses
            SET deactivated_at = now()
            WHERE contract_instance_id = $1
              AND deactivated_at IS NULL
            "#,
        )
        .bind(existing_row.contract_instance_id)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to deactivate active address row for contract_instance_id {}",
                existing_row.contract_instance_id
            )
        })?;
    }

    let existing_active_map = existing_active
        .into_iter()
        .map(|row| (row.contract_instance_id, row))
        .collect::<HashMap<_, _>>();

    for desired_spec in desired_specs {
        if let Some(existing_row) = existing_active_map.get(&desired_spec.contract_instance_id) {
            if existing_row.chain != desired_spec.chain
                || existing_row.address != desired_spec.address
            {
                bail!(
                    "contract_instance_id {} changed address from {}:{} to {}:{}; successor addresses must rotate IDs",
                    desired_spec.contract_instance_id,
                    existing_row.chain,
                    existing_row.address,
                    desired_spec.chain,
                    desired_spec.address
                );
            }
            continue;
        }

        sqlx::query(
            r#"
            INSERT INTO contract_instance_addresses (
                contract_instance_id,
                chain_id,
                address,
                source_manifest_id,
                provenance
            )
            VALUES ($1, $2, $3, $4, $5::jsonb)
            "#,
        )
        .bind(desired_spec.contract_instance_id)
        .bind(&desired_spec.chain)
        .bind(&desired_spec.address)
        .bind(desired_spec.source_manifest_id)
        .bind(&desired_spec.provenance_json)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to activate address {} for contract_instance_id {}",
                desired_spec.address, desired_spec.contract_instance_id
            )
        })?;
    }

    Ok(())
}

async fn load_desired_active_address_specs(
    executor: &mut sqlx::postgres::PgConnection,
) -> Result<Vec<ActiveAddressSpec>> {
    let manifest_rows = sqlx::query(
        r#"
        SELECT
            mv.manifest_id,
            mv.chain,
            mci.declaration_kind,
            mci.declaration_name,
            mci.contract_instance_id,
            mci.declared_address,
            mci.implementation_contract_instance_id,
            mci.declared_implementation_address
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
        WHERE mv.rollout_status = 'active'
        ORDER BY mv.manifest_id, mci.declaration_kind, mci.declaration_name
        "#,
    )
    .fetch_all(&mut *executor)
    .await
    .context("failed to load active manifest address specs")?;

    let discovery_endpoint_rows = sqlx::query(
        r#"
        WITH active_discovery_endpoints AS (
            SELECT de.source_manifest_id, de.from_contract_instance_id AS contract_instance_id
            FROM discovery_edges de
            JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
            WHERE mv.rollout_status = 'active'
              AND de.deactivated_at IS NULL
              AND de.edge_kind <> 'migration'

            UNION

            SELECT de.source_manifest_id, de.to_contract_instance_id AS contract_instance_id
            FROM discovery_edges de
            JOIN manifest_versions mv ON mv.manifest_id = de.source_manifest_id
            WHERE mv.rollout_status = 'active'
              AND de.deactivated_at IS NULL
              AND de.edge_kind <> 'migration'
        )
        SELECT
            endpoints.source_manifest_id,
            cia.contract_instance_id,
            cia.chain_id,
            cia.address
        FROM active_discovery_endpoints endpoints
        JOIN LATERAL (
            SELECT contract_instance_id, chain_id, address
            FROM contract_instance_addresses
            WHERE contract_instance_id = endpoints.contract_instance_id
            ORDER BY (deactivated_at IS NULL) DESC, admitted_at DESC
            LIMIT 1
        ) cia ON TRUE
        "#,
    )
    .fetch_all(&mut *executor)
    .await
    .context("failed to load discovery-edge endpoint address specs")?;

    let mut specs = HashMap::<Uuid, ActiveAddressSpec>::new();

    for row in manifest_rows {
        let contract_instance_id = row
            .try_get::<Uuid, _>("contract_instance_id")
            .context("failed to read manifest contract_instance_id")?;
        let declaration_kind = row
            .try_get::<String, _>("declaration_kind")
            .context("failed to read declaration_kind")?;
        let declaration_name = row
            .try_get::<String, _>("declaration_name")
            .context("failed to read declaration_name")?;
        let chain: String = row.try_get("chain").context("failed to read chain")?;
        let manifest_id = row
            .try_get::<i64, _>("manifest_id")
            .context("failed to read manifest_id")?;
        let declared_address = row
            .try_get::<String, _>("declared_address")
            .context("failed to read declared_address")?;

        specs
            .entry(contract_instance_id)
            .or_insert(ActiveAddressSpec {
                contract_instance_id,
                chain: chain.clone(),
                address: declared_address.clone(),
                source_manifest_id: Some(manifest_id),
                provenance_json: serde_json::json!({
                    "source": "manifest_declared",
                    "declaration_kind": declaration_kind,
                    "declaration_name": declaration_name,
                })
                .to_string(),
            });

        let implementation_contract_instance_id = row
            .try_get::<Option<Uuid>, _>("implementation_contract_instance_id")
            .context("failed to read implementation_contract_instance_id")?;
        let declared_implementation_address = row
            .try_get::<Option<String>, _>("declared_implementation_address")
            .context("failed to read declared_implementation_address")?;
        if let (Some(implementation_contract_instance_id), Some(implementation_address)) = (
            implementation_contract_instance_id,
            declared_implementation_address,
        ) {
            specs
                .entry(implementation_contract_instance_id)
                .or_insert(ActiveAddressSpec {
                    contract_instance_id: implementation_contract_instance_id,
                    chain: chain.clone(),
                    address: implementation_address.clone(),
                    source_manifest_id: Some(manifest_id),
                    provenance_json: serde_json::json!({
                        "source": "manifest_proxy_implementation",
                        "proxy_contract_instance_id": contract_instance_id,
                        "proxy_address": declared_address,
                    })
                    .to_string(),
                });
        }
    }

    for row in discovery_endpoint_rows {
        let contract_instance_id = row
            .try_get::<Uuid, _>("contract_instance_id")
            .context("failed to read discovery endpoint contract_instance_id")?;
        specs
            .entry(contract_instance_id)
            .or_insert(ActiveAddressSpec {
                contract_instance_id,
                chain: row
                    .try_get("chain_id")
                    .context("failed to read discovery endpoint chain_id")?,
                address: row
                    .try_get("address")
                    .context("failed to read discovery endpoint address")?,
                source_manifest_id: row
                    .try_get("source_manifest_id")
                    .context("failed to read discovery endpoint source_manifest_id")?,
                provenance_json: serde_json::json!({
                    "source": "discovery_edge_endpoint",
                })
                .to_string(),
            });
    }

    Ok(specs.into_values().collect())
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

    fn manifest_contents(
        rollout_status: &str,
        root_address: &str,
        contract_address: &str,
        implementation: Option<&str>,
    ) -> String {
        let implementation = implementation
            .map(|value| format!("implementation = \"{value}\"\n"))
            .unwrap_or_default();
        format!(
            r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v2_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v2"
rollout_status = "{rollout_status}"
normalizer_version = "uts46-v1"

[capability_flags]
declared_children = "supported"

[[roots]]
name = "RootRegistry"
address = "{root_address}"

[[contracts]]
role = "registry"
address = "{contract_address}"
proxy_kind = "erc1967"
{implementation}

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"
"#
        )
    }

    async fn load_single_contract_instance_for_address(
        pool: &PgPool,
        chain: &str,
        address: &str,
    ) -> Result<Uuid> {
        query_scalar::<_, Uuid>(
            r#"
            SELECT contract_instance_id
            FROM contract_instance_addresses
            WHERE chain_id = $1
              AND address = $2
            ORDER BY (deactivated_at IS NULL) DESC, admitted_at DESC
            LIMIT 1
            "#,
        )
        .bind(chain)
        .bind(normalize_address(address))
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to load contract instance for {chain} {address}"))
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
    fn loads_valid_repository_manifest() -> Result<()> {
        let test_dir = TestDir::new()?;
        test_dir.write_manifest(
            "ens",
            "ens_v2_registry_l1",
            "v1",
            &manifest_contents(
                "active",
                "0x0000000000000000000000000000000000000001",
                "0x00000000000000000000000000000000000000AA",
                Some("0x00000000000000000000000000000000000000DD"),
            ),
        )?;

        let repository = load_repository(&test_dir.path)?;

        assert_eq!(repository.summary().status, ManifestLoadStatus::Loaded);
        assert_eq!(repository.summary().namespace_count, 1);
        assert_eq!(repository.summary().source_family_count, 1);
        assert_eq!(repository.summary().manifest_count, 1);
        assert_eq!(repository.manifests().len(), 1);
        assert_eq!(repository.manifests()[0].version_tag, "v1");
        assert_eq!(repository.manifests()[0].manifest.namespace, "ens");

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
    async fn reuses_contract_instance_ids_across_inactive_gaps() -> Result<()> {
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;

        test_dir.write_manifest(
            "ens",
            "ens_v2_registry_l1",
            "v1",
            &manifest_contents(
                "active",
                "0x0000000000000000000000000000000000000001",
                "0x00000000000000000000000000000000000000AA",
                Some("0x00000000000000000000000000000000000000DD"),
            ),
        )?;
        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
        let first_contract_instance_id = load_single_contract_instance_for_address(
            database.pool(),
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000aa",
        )
        .await?;

        let empty_dir = TestDir::new()?;
        sync_repository(database.pool(), &load_repository(&empty_dir.path)?).await?;
        assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM contract_instance_addresses WHERE contract_instance_id = $1 AND deactivated_at IS NULL"
            )
            .bind(first_contract_instance_id)
            .fetch_one(database.pool())
            .await?,
            0
        );

        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
        let reused_contract_instance_id = load_single_contract_instance_for_address(
            database.pool(),
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000aa",
        )
        .await?;

        assert_eq!(first_contract_instance_id, reused_contract_instance_id);
        assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM contract_instance_addresses WHERE contract_instance_id = $1"
            )
            .bind(first_contract_instance_id)
            .fetch_one(database.pool())
            .await?,
            2
        );

        database.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn keeps_proxy_instance_stable_across_implementation_churn() -> Result<()> {
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;

        test_dir.write_manifest(
            "ens",
            "ens_v2_registry_l1",
            "v1",
            &manifest_contents(
                "active",
                "0x0000000000000000000000000000000000000001",
                "0x00000000000000000000000000000000000000AA",
                Some("0x00000000000000000000000000000000000000DD"),
            ),
        )?;
        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

        let proxy_contract_instance_id = load_single_contract_instance_for_address(
            database.pool(),
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000aa",
        )
        .await?;
        let first_implementation_id = load_single_contract_instance_for_address(
            database.pool(),
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000dd",
        )
        .await?;

        test_dir.write_manifest(
            "ens",
            "ens_v2_registry_l1",
            "v1",
            &manifest_contents(
                "active",
                "0x0000000000000000000000000000000000000001",
                "0x00000000000000000000000000000000000000AA",
                Some("0x00000000000000000000000000000000000000EE"),
            ),
        )?;
        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

        let proxy_after_churn = load_single_contract_instance_for_address(
            database.pool(),
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000aa",
        )
        .await?;
        let second_implementation_id = load_single_contract_instance_for_address(
            database.pool(),
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000ee",
        )
        .await?;

        assert_eq!(proxy_contract_instance_id, proxy_after_churn);
        assert_ne!(first_implementation_id, second_implementation_id);
        assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
            )
            .bind(MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE)
            .fetch_one(database.pool())
            .await?,
            1
        );
        assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM contract_instance_addresses WHERE contract_instance_id = $1 AND deactivated_at IS NULL"
            )
            .bind(first_implementation_id)
            .fetch_one(database.pool())
            .await?,
            0
        );

        database.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn rotates_successor_addresses_and_persists_migration_continuity() -> Result<()> {
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;

        test_dir.write_manifest(
            "ens",
            "ens_v2_registry_l1",
            "v1",
            &manifest_contents(
                "active",
                "0x0000000000000000000000000000000000000001",
                "0x00000000000000000000000000000000000000AA",
                Some("0x00000000000000000000000000000000000000DD"),
            ),
        )?;
        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

        let original_contract_instance_id = load_single_contract_instance_for_address(
            database.pool(),
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000aa",
        )
        .await?;

        test_dir.write_manifest(
            "ens",
            "ens_v2_registry_l1",
            "v1",
            &manifest_contents(
                "active",
                "0x0000000000000000000000000000000000000001",
                "0x00000000000000000000000000000000000000BB",
                Some("0x00000000000000000000000000000000000000DD"),
            ),
        )?;
        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

        let successor_contract_instance_id = load_single_contract_instance_for_address(
            database.pool(),
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000bb",
        )
        .await?;
        assert_ne!(
            original_contract_instance_id,
            successor_contract_instance_id
        );
        assert_eq!(
            query_scalar::<_, i64>(
                r#"
                SELECT COUNT(*)::BIGINT
                FROM discovery_edges
                WHERE discovery_source = $1
                  AND edge_kind = $2
                  AND from_contract_instance_id = $3
                  AND to_contract_instance_id = $4
                  AND deactivated_at IS NULL
                "#
            )
            .bind(MANIFEST_SUCCESSOR_DISCOVERY_SOURCE)
            .bind(MANIFEST_SUCCESSOR_EDGE_KIND)
            .bind(original_contract_instance_id)
            .bind(successor_contract_instance_id)
            .fetch_one(database.pool())
            .await?,
            1
        );

        database.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn watched_plan_does_not_expand_migration_edges() -> Result<()> {
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;

        test_dir.write_manifest(
            "ens",
            "ens_v2_registry_l1",
            "v1",
            &manifest_contents(
                "active",
                "0x0000000000000000000000000000000000000001",
                "0x00000000000000000000000000000000000000AA",
                Some("0x00000000000000000000000000000000000000DD"),
            ),
        )?;
        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

        test_dir.write_manifest(
            "ens",
            "ens_v2_registry_l1",
            "v1",
            &manifest_contents(
                "active",
                "0x0000000000000000000000000000000000000001",
                "0x00000000000000000000000000000000000000BB",
                Some("0x00000000000000000000000000000000000000DD"),
            ),
        )?;
        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

        assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE edge_kind = $1 AND deactivated_at IS NULL"
            )
            .bind(MANIFEST_SUCCESSOR_EDGE_KIND)
            .fetch_one(database.pool())
            .await?,
            1
        );

        let watched_summary = load_watched_contract_summary(database.pool()).await?;
        assert_eq!(watched_summary.unique_contract_count, 3);
        assert_eq!(watched_summary.manifest_root_count, 1);
        assert_eq!(watched_summary.manifest_contract_count, 1);
        assert_eq!(watched_summary.discovery_edge_count, 1);

        let watched_chain_plan = load_watched_chain_plan(database.pool()).await?;
        assert_eq!(
            watched_chain_plan,
            vec![WatchedChainPlan {
                chain: "ethereum-mainnet".to_owned(),
                addresses: vec![
                    "0x0000000000000000000000000000000000000001".to_owned(),
                    "0x00000000000000000000000000000000000000bb".to_owned(),
                    "0x00000000000000000000000000000000000000dd".to_owned(),
                ],
                manifest_root_entry_count: 1,
                manifest_contract_entry_count: 1,
                discovery_edge_entry_count: 1,
            }]
        );

        database.cleanup().await?;
        Ok(())
    }

    #[tokio::test]
    async fn rebuilds_watched_plan_from_active_contract_instance_address_ranges() -> Result<()> {
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;

        test_dir.write_manifest(
            "ens",
            "ens_v2_registry_l1",
            "v1",
            &manifest_contents(
                "active",
                "0x0000000000000000000000000000000000000001",
                "0x00000000000000000000000000000000000000AA",
                Some("0x00000000000000000000000000000000000000DD"),
            ),
        )?;
        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

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
        assert!(
            persistence_summary.admitted_edges[0]
                .to_contract_instance_id
                .is_some()
        );

        let watched_contracts = load_watched_contracts(database.pool()).await?;
        assert_eq!(watched_contracts.len(), 4);
        assert!(watched_contracts.iter().any(|contract| {
            contract.address == "0x0000000000000000000000000000000000000001"
                && contract.source == WatchedContractSource::ManifestRoot
        }));
        assert!(watched_contracts.iter().any(|contract| {
            contract.address == "0x00000000000000000000000000000000000000aa"
                && contract.source == WatchedContractSource::ManifestContract
        }));
        assert!(watched_contracts.iter().any(|contract| {
            contract.address == "0x00000000000000000000000000000000000000dd"
                && contract.source == WatchedContractSource::DiscoveryEdge
        }));
        assert!(watched_contracts.iter().any(|contract| {
            contract.address == "0x00000000000000000000000000000000000000cc"
                && contract.source == WatchedContractSource::DiscoveryEdge
        }));

        let watched_summary = load_watched_contract_summary(database.pool()).await?;
        assert_eq!(watched_summary.unique_contract_count, 4);
        assert_eq!(watched_summary.manifest_root_count, 1);
        assert_eq!(watched_summary.manifest_contract_count, 1);
        assert_eq!(watched_summary.discovery_edge_count, 2);

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

        database.cleanup().await?;
        Ok(())
    }
}
