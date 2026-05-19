use std::collections::HashMap;

use anyhow::{Context, Result};
use bigname_manifests::load_manifest_declared_watched_contracts;
use sqlx::PgPool;

use crate::adapter_manifest::{
    active_manifest_for_watched_contract, ensure_watched_contract_manifest_chain,
    load_active_manifest_metadata, source_rank, watched_contract_manifest_ids,
};

use super::helpers::supports_reverse_claim_source_family;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ActiveEmitter {
    pub(super) address: String,
    pub(super) contract_instance_id: sqlx::types::Uuid,
    pub(super) source_manifest_id: i64,
    pub(super) namespace: String,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
    source_rank: i32,
}

pub(super) async fn load_active_emitters(pool: &PgPool, chain: &str) -> Result<Vec<ActiveEmitter>> {
    let watched_contracts = load_manifest_declared_watched_contracts(pool)
        .await
        .context(
            "failed to load manifest-declared watched contracts for ENSv1 reverse attribution",
        )?;
    let watched_contracts = watched_contracts
        .into_iter()
        .filter(|contract| contract.chain == chain)
        .collect::<Vec<_>>();
    if watched_contracts.is_empty() {
        return Ok(Vec::new());
    }

    let manifest_ids = watched_contract_manifest_ids(&watched_contracts)?;
    let active_manifests =
        load_active_manifest_metadata(pool, &manifest_ids, "ENSv1 reverse").await?;

    let mut emitters_by_address = HashMap::<String, ActiveEmitter>::new();
    for watched_contract in watched_contracts {
        let (source_manifest_id, manifest) =
            active_manifest_for_watched_contract(&active_manifests, &watched_contract)?;
        ensure_watched_contract_manifest_chain(&watched_contract, manifest, source_manifest_id)?;
        if !supports_reverse_claim_source_family(&manifest.source_family) {
            continue;
        }

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
