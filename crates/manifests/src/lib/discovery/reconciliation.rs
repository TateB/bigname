#[path = "reconciliation/bulk.rs"]
mod bulk;
#[path = "reconciliation/existing.rs"]
mod existing;
#[path = "reconciliation/support.rs"]
mod support;

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use anyhow::{Context, Result, ensure};
use sqlx::{PgPool, types::Uuid};

use super::admission::DiscoveryAdmissionState;
use super::loading::{
    load_discovery_admission_state_with_excluded_source as load_admission_state,
    load_scoped_discovery_admission_state_with_excluded_source as load_scoped_admission_state,
};
use super::provenance::{
    discovery_edge_propagates_role, discovery_edge_provenance, is_zero_address, observation_key,
};
use super::types::{
    AdmittedDiscoveryEdge, DiscoveryObservation, DiscoveryReconciliationSummary,
    ExistingReconciledDiscoveryEdge, ObservationTerminalState, ReconciledDiscoveryEdgeSpec,
    StoredActiveContract,
};
use crate::{
    REACHABLE_FROM_ROOT_ADMISSION, normalize_address, reconcile_active_contract_instance_addresses,
    reconcile_active_contract_instance_addresses_for_ids,
};

use self::bulk::{
    PendingContractInstanceSeed, insert_pending_contract_instance_seeds,
    insert_reconciled_discovery_edges,
};
use self::existing::{
    load_active_reconciled_discovery_descendant_edges, load_active_reconciled_discovery_edges,
    load_active_reconciled_discovery_edges_by_observation_keys,
};
use self::support::{lock_discovery_reconciliation, observation_terminal_states};

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
    let mut transaction = pool
        .begin()
        .await
        .context("failed to start discovery-edge reconciliation transaction")?;
    lock_discovery_reconciliation(transaction.as_mut(), discovery_source).await?;

    let admission_state =
        load_admission_state(transaction.as_mut(), Some(discovery_source)).await?;
    let direct_terminal_states_by_key = observation_terminal_states(observations)?;
    let observations_by_key = observations
        .iter()
        .map(|observation| Ok((observation_key(observation)?, observation)))
        .collect::<Result<HashMap<_, _>>>()?;

    let (desired_edges, admitted_edges) = resolve_reconciled_discovery_edge_specs(
        &admission_state,
        transaction.as_mut(),
        observations,
    )
    .await?;
    let existing_edges =
        load_active_reconciled_discovery_edges(transaction.as_mut(), discovery_source).await?;

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
                        FROM chain_lineage rb
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

    let new_edges = desired_edges
        .iter()
        .filter(|desired_edge| !existing_set.contains(*desired_edge))
        .collect::<Vec<_>>();
    let inserted_edge_count =
        insert_reconciled_discovery_edges(transaction.as_mut(), &new_edges).await?;

    if inserted_edge_count > 0 || deactivated_edge_count > 0 {
        reconcile_active_contract_instance_addresses(transaction.as_mut()).await?;
    }

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

pub async fn reconcile_scoped_discovery_observations(
    pool: &PgPool,
    discovery_source: &str,
    observations: &[DiscoveryObservation],
) -> Result<DiscoveryReconciliationSummary> {
    if observations.is_empty() {
        return Ok(DiscoveryReconciliationSummary {
            active_edge_count: 0,
            admitted_edge_count: 0,
            inserted_edge_count: 0,
            deactivated_edge_count: 0,
            admitted_edges: Vec::new(),
        });
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to start scoped discovery-edge reconciliation transaction")?;
    lock_discovery_reconciliation(transaction.as_mut(), discovery_source).await?;

    for observation in observations {
        ensure!(
            observation.discovery_source == discovery_source,
            "scoped discovery observation for {} cannot be reconciled under {}",
            observation.discovery_source,
            discovery_source
        );
    }

    let admission_state =
        load_scoped_admission_state(transaction.as_mut(), Some(discovery_source), observations)
            .await?;
    let direct_terminal_states_by_key = observation_terminal_states(observations)?;
    let observations_by_key = observations
        .iter()
        .map(|observation| Ok((observation_key(observation)?, observation)))
        .collect::<Result<HashMap<_, _>>>()?;
    let mut touched_observation_keys = direct_terminal_states_by_key
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    touched_observation_keys.sort();

    let (desired_edges, admitted_edges) = resolve_reconciled_discovery_edge_specs(
        &admission_state,
        transaction.as_mut(),
        observations,
    )
    .await?;
    let existing_edges = load_active_reconciled_discovery_edges_by_observation_keys(
        transaction.as_mut(),
        discovery_source,
        &touched_observation_keys,
    )
    .await?;

    let desired_set = desired_edges.iter().cloned().collect::<HashSet<_>>();
    let existing_set = existing_edges
        .iter()
        .map(|edge| edge.spec.clone())
        .collect::<HashSet<_>>();
    let mut deactivation_terminal_states_by_edge_id =
        BTreeMap::<i64, ObservationTerminalState>::new();
    let mut removed_parent_edges = Vec::<(String, Uuid, ObservationTerminalState)>::new();
    let mut affected_contract_instance_ids = HashSet::<Uuid>::new();

    for existing_edge in &existing_edges {
        if desired_set.contains(&existing_edge.spec) {
            continue;
        }
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

        deactivation_terminal_states_by_edge_id
            .insert(existing_edge.discovery_edge_id, terminal_state.clone());
        affected_contract_instance_ids.insert(existing_edge.spec.from_contract_instance_id);
        affected_contract_instance_ids.insert(existing_edge.spec.to_contract_instance_id);
        if discovery_edge_propagates_role(&existing_edge.spec.edge_kind) {
            removed_parent_edges.push((
                existing_edge.spec.chain.clone(),
                existing_edge.spec.to_contract_instance_id,
                terminal_state,
            ));
        }
    }

    for (chain, parent_contract_instance_id, terminal_state) in removed_parent_edges {
        let descendants = load_active_reconciled_discovery_descendant_edges(
            transaction.as_mut(),
            discovery_source,
            &chain,
            &[parent_contract_instance_id],
        )
        .await?;
        for descendant in descendants {
            if desired_set.contains(&descendant.spec) {
                continue;
            }
            deactivation_terminal_states_by_edge_id
                .entry(descendant.discovery_edge_id)
                .or_insert_with(|| terminal_state.clone());
            affected_contract_instance_ids.insert(descendant.spec.from_contract_instance_id);
            affected_contract_instance_ids.insert(descendant.spec.to_contract_instance_id);
        }
    }

    let mut deactivated_edge_count = 0;
    for (discovery_edge_id, terminal_state) in deactivation_terminal_states_by_edge_id {
        sqlx::query(
            r#"
            UPDATE discovery_edges
            SET active_to_block_number = COALESCE($2, active_to_block_number),
                active_to_block_hash = COALESCE($3, active_to_block_hash),
                deactivated_at = COALESCE(
                    (
                        SELECT GREATEST(discovery_edges.admitted_at, rb.block_timestamp)
                        FROM chain_lineage rb
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
        .bind(discovery_edge_id)
        .bind(terminal_state.block_number)
        .bind(terminal_state.block_hash.as_deref())
        .bind(terminal_state.chain.as_str())
        .execute(transaction.as_mut())
        .await
        .with_context(|| {
            format!(
                "failed to deactivate scoped discovery_edge_id {}",
                discovery_edge_id
            )
        })?;
        deactivated_edge_count += 1;
    }

    let new_edges = desired_edges
        .iter()
        .filter(|desired_edge| !existing_set.contains(*desired_edge))
        .collect::<Vec<_>>();
    for new_edge in &new_edges {
        affected_contract_instance_ids.insert(new_edge.from_contract_instance_id);
        affected_contract_instance_ids.insert(new_edge.to_contract_instance_id);
    }
    let inserted_edge_count =
        insert_reconciled_discovery_edges(transaction.as_mut(), &new_edges).await?;

    if inserted_edge_count > 0 || deactivated_edge_count > 0 {
        reconcile_active_contract_instance_addresses_for_ids(
            transaction.as_mut(),
            &affected_contract_instance_ids,
        )
        .await?;
    }

    transaction
        .commit()
        .await
        .context("failed to commit scoped discovery-edge reconciliation transaction")?;

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
    let mut active_contracts = admission_state
        .active_contracts
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    let mut active_contracts_by_address =
        HashMap::<(String, String), Vec<StoredActiveContract>>::new();
    for contract in &active_contracts {
        active_contracts_by_address
            .entry((contract.chain.clone(), contract.address.clone()))
            .or_default()
            .push(contract.clone());
    }
    let mut pending_contract_instance_seeds =
        HashMap::<(String, String), PendingContractInstanceSeed>::new();
    let mut observations_by_from_address =
        HashMap::<(String, String), Vec<&DiscoveryObservation>>::new();
    for observation in observations {
        if is_zero_address(&observation.to_address) {
            continue;
        }
        observations_by_from_address
            .entry((
                observation.chain.clone(),
                normalize_address(&observation.from_address),
            ))
            .or_default()
            .push(observation);
    }

    let mut queued_contract_keys = active_contracts_by_address
        .keys()
        .cloned()
        .collect::<HashSet<_>>();
    let mut pending_contract_keys = queued_contract_keys
        .iter()
        .cloned()
        .collect::<VecDeque<_>>();
    while let Some(contract_key) = pending_contract_keys.pop_front() {
        queued_contract_keys.remove(&contract_key);
        let Some(key_observations) = observations_by_from_address.get(&contract_key) else {
            continue;
        };

        for &observation in key_observations {
            let observation_key = observation_key(observation)?;

            for mut admitted_edge in admission_state.admit_candidate_against_contract_lookup(
                &active_contracts_by_address,
                &observation.candidate(),
            ) {
                let to_contract_instance_id = match admitted_edge.to_contract_instance_id {
                    Some(contract_instance_id) => contract_instance_id,
                    None => {
                        let resolved_key = (
                            admitted_edge.chain.clone(),
                            normalize_address(&admitted_edge.to_address),
                        );
                        if let Some(seed) = pending_contract_instance_seeds.get(&resolved_key) {
                            seed.contract_instance_id
                        } else {
                            let contract_instance_id = Uuid::new_v4();
                            let instance_provenance_json = serde_json::json!({
                                "source": "discovery_observation",
                                "edge_kind": admitted_edge.edge_kind,
                                "discovery_source": admitted_edge.discovery_source,
                            });
                            let address_provenance_json = serde_json::json!({
                                "source": "discovery_observation_seed",
                                "edge_kind": admitted_edge.edge_kind,
                                "discovery_source": admitted_edge.discovery_source,
                            });
                            pending_contract_instance_seeds.insert(
                                resolved_key,
                                PendingContractInstanceSeed {
                                    contract_instance_id,
                                    chain: admitted_edge.chain.clone(),
                                    address: normalize_address(&admitted_edge.to_address),
                                    source_manifest_id: admitted_edge.source_manifest_id,
                                    instance_provenance_json,
                                    address_provenance_json,
                                },
                            );
                            contract_instance_id
                        }
                    }
                };
                admitted_edge.to_contract_instance_id = Some(to_contract_instance_id);

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
                desired_edges.insert(desired_edge);
                admitted_edges.insert(admitted_edge.clone());

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
                    if active_contracts.insert(derived_contract.clone()) {
                        let derived_contract_key = (
                            derived_contract.chain.clone(),
                            derived_contract.address.clone(),
                        );
                        active_contracts_by_address
                            .entry(derived_contract_key.clone())
                            .or_default()
                            .push(derived_contract);
                        if queued_contract_keys.insert(derived_contract_key.clone()) {
                            pending_contract_keys.push_back(derived_contract_key);
                        }
                    }
                }
            }
        }
    }

    let mut pending_contract_instance_seeds = pending_contract_instance_seeds
        .into_values()
        .collect::<Vec<_>>();
    pending_contract_instance_seeds.sort_by(|left, right| {
        (left.chain.as_str(), left.address.as_str())
            .cmp(&(right.chain.as_str(), right.address.as_str()))
    });
    insert_pending_contract_instance_seeds(executor, &pending_contract_instance_seeds).await?;

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
