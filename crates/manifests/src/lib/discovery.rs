use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::{
    CONTRACT_KIND_CONTRACT, PROPAGATED_ROLE_PROVENANCE_FIELD, REACHABLE_FROM_ROOT_ADMISSION,
    ZERO_ADDRESS, ensure_contract_instance_address_seed, normalize_address,
    reconcile_active_contract_instance_addresses, resolve_contract_instance_by_address,
};

const TRANSITIVE_DISCOVERY_EDGE_KIND: &str = "subregistry";
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
            let provenance = serde_json::to_string(&discovery_edge_provenance(
                &observation.provenance,
                &admitted_edge.edge_kind,
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

fn discovery_edge_provenance(
    provenance: &serde_json::Value,
    edge_kind: &str,
    from_role: &str,
) -> Result<serde_json::Value> {
    let mut provenance = provenance.clone();
    let Some(object) = provenance.as_object_mut() else {
        bail!("discovery observation provenance must be a JSON object");
    };
    if discovery_edge_propagates_role(edge_kind) {
        object.insert(
            PROPAGATED_ROLE_PROVENANCE_FIELD.to_owned(),
            serde_json::Value::String(from_role.to_owned()),
        );
    }
    Ok(provenance)
}

fn discovery_edge_propagates_role(edge_kind: &str) -> bool {
    edge_kind == TRANSITIVE_DISCOVERY_EDGE_KIND
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

                let provenance = discovery_edge_provenance(
                    &observation.provenance,
                    &admitted_edge.edge_kind,
                    &admitted_edge.from_role,
                )?;
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

                if admitted_edge.admission == REACHABLE_FROM_ROOT_ADMISSION
                    && discovery_edge_propagates_role(&admitted_edge.edge_kind)
                {
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

struct StoredActiveRoot {
    manifest_id: i64,
    chain: String,
    _contract_instance_id: Uuid,
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
          AND de.edge_kind = $4
          AND de.admission = $1
          AND de.provenance ? $2
          AND ($3::TEXT IS NULL OR de.discovery_source <> $3)
        "#,
    )
    .bind(REACHABLE_FROM_ROOT_ADMISSION)
    .bind(PROPAGATED_ROLE_PROVENANCE_FIELD)
    .bind(excluded_discovery_source)
    .bind(TRANSITIVE_DISCOVERY_EDGE_KIND)
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
                _contract_instance_id: row
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
