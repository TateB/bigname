use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use bigname_manifests::{load_manifest_declared_watched_contracts, load_watched_contracts};
use sqlx::PgPool;

use crate::adapter_manifest::{
    active_manifest_for_watched_contract, ensure_watched_contract_manifest_chain,
    load_active_manifest_metadata, source_rank, watched_contract_manifest_ids,
};

use super::types::{ActiveEmitter, RawLogSourceScopeTarget};

pub(super) fn normalized_source_scope_targets(
    source_scope: &[(String, String, i64, i64)],
) -> Vec<RawLogSourceScopeTarget> {
    source_scope
        .iter()
        .map(
            |(source_family, address, effective_from_block, effective_to_block)| {
                RawLogSourceScopeTarget {
                    source_family: source_family.clone(),
                    address: address.to_ascii_lowercase(),
                    effective_from_block: *effective_from_block,
                    effective_to_block: *effective_to_block,
                }
            },
        )
        .collect()
}

pub(super) async fn load_active_emitters(
    pool: &PgPool,
    chain: &str,
    scoped_emitter_identities: Option<&HashSet<(String, String)>>,
) -> Result<Vec<ActiveEmitter>> {
    let watched_contracts = if scoped_emitter_identities.is_some() {
        load_manifest_declared_watched_contracts(pool)
            .await
            .context("failed to load manifest-declared watched contracts for scoped adapter emitter attribution")?
    } else {
        load_watched_contracts(pool)
            .await
            .context("failed to load watched contracts for adapter emitter attribution")?
    };
    let watched_contracts = watched_contracts
        .into_iter()
        .filter(|contract| contract.chain == chain)
        .filter(|contract| {
            scoped_emitter_identities.is_none_or(|scope| {
                scope.contains(&(contract.source_family.clone(), contract.address.clone()))
            })
        })
        .collect::<Vec<_>>();
    if watched_contracts.is_empty() {
        return Ok(Vec::new());
    }

    let manifest_ids = watched_contract_manifest_ids(&watched_contracts)?;
    let active_manifests =
        load_active_manifest_metadata(pool, &manifest_ids, "watched contracts").await?;

    let mut emitters_by_address = HashMap::<String, ActiveEmitter>::new();
    for watched_contract in watched_contracts {
        let (source_manifest_id, manifest) =
            active_manifest_for_watched_contract(&active_manifests, &watched_contract)?;
        ensure_watched_contract_manifest_chain(&watched_contract, manifest, source_manifest_id)?;

        let candidate = ActiveEmitter {
            address: watched_contract.address.clone(),
            contract_instance_id: watched_contract.contract_instance_id,
            source_manifest_id,
            namespace: manifest.namespace.clone(),
            source_family: manifest.source_family.clone(),
            manifest_version: manifest.manifest_version,
            source_rank: source_rank(watched_contract.source),
        };

        match emitters_by_address.get(&candidate.address) {
            Some(current) if !candidate_precedes(&candidate, current) => {}
            _ => {
                emitters_by_address.insert(candidate.address.clone(), candidate);
            }
        }
    }

    let mut emitters = emitters_by_address.into_values().collect::<Vec<_>>();
    emitters.sort_by(|left, right| {
        left.address
            .cmp(&right.address)
            .then(left.source_rank.cmp(&right.source_rank))
            .then(left.source_manifest_id.cmp(&right.source_manifest_id))
            .then(left.contract_instance_id.cmp(&right.contract_instance_id))
    });
    Ok(emitters)
}

fn candidate_precedes(candidate: &ActiveEmitter, current: &ActiveEmitter) -> bool {
    (
        candidate.source_rank,
        candidate.source_manifest_id,
        candidate.contract_instance_id,
    ) < (
        current.source_rank,
        current.source_manifest_id,
        current.contract_instance_id,
    )
}
