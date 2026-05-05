use bigname_storage::sql_row;
use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use bigname_manifests::{WatchedContractSource, load_watched_contracts};
use sqlx::{PgPool, types::Uuid};

use crate::adapter_manifest::watched_contract_manifest_ids;

use super::{
    constants::*,
    types::{ActiveEmitter, ActiveManifestMetadata, RegistryRawLogSourceScopeTarget},
    util::normalize_address,
};

pub(super) async fn load_active_emitters(
    pool: &PgPool,
    chain: &str,
    scoped_emitter_identities: Option<&HashSet<(String, String)>>,
) -> Result<Vec<ActiveEmitter>> {
    let watched_contracts = load_watched_contracts(pool)
        .await
        .context("failed to load watched contracts for ENSv2 registry adapter")?;
    let watched_contracts = watched_contracts
        .into_iter()
        .filter(|contract| contract.chain == chain)
        .collect::<Vec<_>>();
    if watched_contracts.is_empty() {
        return Ok(Vec::new());
    }

    let manifest_ids = watched_contract_manifest_ids(&watched_contracts)?;
    let active_manifests = load_active_manifest_metadata(pool, &manifest_ids).await?;

    let mut emitter_candidates = Vec::new();
    for watched_contract in watched_contracts {
        let source_manifest_id = watched_contract
            .source_manifest_id
            .context("watched contract missing source_manifest_id after validation")?;
        let manifest = active_manifests.get(&source_manifest_id).with_context(|| {
            format!("missing active manifest metadata for manifest_id {source_manifest_id}")
        })?;
        if scoped_emitter_identities.is_some_and(|scope| {
            !scope.contains(&(
                manifest.source_family.clone(),
                normalize_address(&watched_contract.address),
            ))
        }) {
            continue;
        }
        if manifest.source_family != SOURCE_FAMILY_ENS_V2_ROOT_L1
            && manifest.source_family != SOURCE_FAMILY_ENS_V2_REGISTRY_L1
        {
            continue;
        }
        if manifest.chain != watched_contract.chain {
            bail!(
                "watched contract chain {} does not match active manifest chain {} for manifest_id {}",
                watched_contract.chain,
                manifest.chain,
                source_manifest_id
            );
        }

        let candidate = ActiveEmitter {
            address: watched_contract.address.clone(),
            contract_instance_id: watched_contract.contract_instance_id,
            source_manifest_id,
            namespace: manifest.namespace.clone(),
            source_family: manifest.source_family.clone(),
            manifest_version: manifest.manifest_version,
            normalizer_version: manifest.normalizer_version.clone(),
            role: manifest.role.clone(),
            source: watched_contract.source,
            source_rank: source_rank(watched_contract.source),
            active_from_block_number: watched_contract.active_from_block_number,
            active_to_block_number: watched_contract.active_to_block_number,
        };

        emitter_candidates.push(candidate);
    }

    Ok(preferred_emitters_by_scope(emitter_candidates))
}

pub(super) fn source_scope_target_intersects_active_emitter(
    target: &RegistryRawLogSourceScopeTarget,
    emitter: &ActiveEmitter,
) -> bool {
    if target.source_family != emitter.source_family || target.address != emitter.address {
        return false;
    }

    let emitter_from = emitter.active_from_block_number.unwrap_or(0);
    let emitter_to = emitter.active_to_block_number.unwrap_or(i64::MAX);

    target.effective_from_block <= emitter_to && emitter_from <= target.effective_to_block
}

pub(super) fn emitter_sort_key(
    emitter: &ActiveEmitter,
) -> (&str, &str, Option<i64>, Option<i64>, i32, i64, Uuid) {
    (
        &emitter.source_family,
        &emitter.address,
        emitter.active_from_block_number,
        emitter.active_to_block_number,
        emitter.source_rank,
        emitter.source_manifest_id,
        emitter.contract_instance_id,
    )
}

pub(super) fn sort_emitters_by_scope(emitters: &mut [ActiveEmitter]) {
    emitters.sort_by(|left, right| emitter_sort_key(left).cmp(&emitter_sort_key(right)));
}

pub(super) fn preferred_emitters_by_scope(
    candidates: impl IntoIterator<Item = ActiveEmitter>,
) -> Vec<ActiveEmitter> {
    let mut emitters_by_scope =
        HashMap::<(String, String, Option<i64>, Option<i64>), ActiveEmitter>::new();
    for candidate in candidates {
        let scope_key = (
            candidate.source_family.clone(),
            candidate.address.clone(),
            candidate.active_from_block_number,
            candidate.active_to_block_number,
        );
        match emitters_by_scope.get(&scope_key) {
            Some(current) if !candidate_precedes(&candidate, current) => {}
            _ => {
                emitters_by_scope.insert(scope_key, candidate);
            }
        }
    }

    let mut emitters = emitters_by_scope.into_values().collect::<Vec<_>>();
    sort_emitters_by_scope(&mut emitters);
    emitters
}

async fn load_active_manifest_metadata(
    pool: &PgPool,
    manifest_ids: &[i64],
) -> Result<HashMap<i64, ActiveManifestMetadata>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT ON (mv.manifest_id)
            mv.manifest_id,
            mv.chain,
            mv.namespace,
            mv.source_family,
            mv.manifest_version,
            mv.normalizer_version,
            mci.role
        FROM manifest_versions mv
        LEFT JOIN manifest_contract_instances mci
          ON mci.manifest_id = mv.manifest_id
         AND mci.declaration_kind = 'contract'
        WHERE mv.rollout_status = 'active'
          AND mv.manifest_id = ANY($1::BIGINT[])
        ORDER BY mv.manifest_id, mci.manifest_contract_instance_id
        "#,
    )
    .bind(manifest_ids)
    .fetch_all(pool)
    .await
    .context("failed to load active manifest metadata for ENSv2 registry emitters")?;

    rows.into_iter()
        .map(|row| {
            let manifest = ActiveManifestMetadata {
                manifest_id: sql_row::get(&row, "manifest_id")?,
                chain: sql_row::get(&row, "chain")?,
                namespace: sql_row::get(&row, "namespace")?,
                source_family: sql_row::get(&row, "source_family")?,
                manifest_version: sql_row::get(&row, "manifest_version")?,
                normalizer_version: sql_row::get(&row, "normalizer_version")?,
                role: sql_row::get(&row, "role")?,
            };
            Ok((manifest.manifest_id, manifest))
        })
        .collect()
}

pub(super) fn normalized_source_scope_targets(
    source_scope: &[(String, String, i64, i64)],
) -> Vec<RegistryRawLogSourceScopeTarget> {
    source_scope
        .iter()
        .map(
            |(source_family, address, effective_from_block, effective_to_block)| {
                RegistryRawLogSourceScopeTarget {
                    source_family: source_family.clone(),
                    address: normalize_address(address),
                    effective_from_block: *effective_from_block,
                    effective_to_block: *effective_to_block,
                }
            },
        )
        .collect()
}

pub(super) fn scoped_ranges_for_active_emitters(
    source_scope: &[RegistryRawLogSourceScopeTarget],
    emitters: &[ActiveEmitter],
) -> Result<Vec<RegistryRawLogSourceScopeTarget>> {
    let mut ranges = Vec::new();
    for target in source_scope {
        if target.effective_to_block < target.effective_from_block {
            bail!(
                "ENSv2 registry source scope range {}..={} is invalid for {} {}",
                target.effective_from_block,
                target.effective_to_block,
                target.source_family,
                target.address
            );
        }
        if emitters
            .iter()
            .any(|emitter| source_scope_target_intersects_active_emitter(target, emitter))
        {
            ranges.push(target.clone());
        }
    }
    Ok(ranges)
}

pub(super) fn emitter_for_block_and_scope<'a>(
    emitters: &'a [ActiveEmitter],
    block_number: i64,
    source_scope: Option<&[RegistryRawLogSourceScopeTarget]>,
) -> Option<&'a ActiveEmitter> {
    let Some(source_scope) = source_scope else {
        return emitters
            .iter()
            .find(|emitter| emitter_active_at_block(emitter, block_number));
    };

    emitters.iter().find(|emitter| {
        emitter_active_at_block(emitter, block_number)
            && source_scope.iter().any(|target| {
                target.source_family == emitter.source_family
                    && target.address == emitter.address
                    && block_number >= target.effective_from_block
                    && block_number <= target.effective_to_block
            })
    })
}

pub(super) fn emitter_active_at_block(emitter: &ActiveEmitter, block_number: i64) -> bool {
    emitter
        .active_from_block_number
        .is_none_or(|active_from| block_number >= active_from)
        && emitter
            .active_to_block_number
            .is_none_or(|active_to| block_number <= active_to)
}

pub(super) fn source_rank(source: WatchedContractSource) -> i32 {
    crate::adapter_manifest::source_rank(source)
}

pub(super) fn candidate_precedes(candidate: &ActiveEmitter, current: &ActiveEmitter) -> bool {
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
