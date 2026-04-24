use std::collections::{BTreeMap, BTreeSet, HashSet};

use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Row};

use crate::{
    WatchedBackfillTarget, WatchedChainPlan, WatchedContract, WatchedContractChainSummary,
    WatchedContractSource, WatchedContractSummary, WatchedSourceSelector,
    WatchedSourceSelectorPlan, WatchedTargetIdentity, normalize_address,
};

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
