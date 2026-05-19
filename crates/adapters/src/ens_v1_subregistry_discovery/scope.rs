use anyhow::{Result, bail};
use sqlx::PgPool;

use crate::registry_migration_cache::{
    MigratedRegistryNodes, RegistryMigrationMarkerEmitter,
    load_migrated_registry_nodes_before_block as load_cached_migrated_registry_nodes_before_block,
};

use super::{
    CONTRACT_ROLE_REGISTRY, ENS_V1_REGISTRY_SOURCE_FAMILY, hex_topic::normalize_address,
    loader::ActiveEmitter, migration_guard::new_owner_child_node_from_topics,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RegistryRawLogSourceScopeTarget {
    pub(super) source_family: String,
    pub(super) address: String,
    pub(super) effective_from_block: i64,
    pub(super) effective_to_block: i64,
}

pub(super) fn normalized_registry_source_scope_targets(
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

pub(super) async fn load_migrated_registry_nodes_before_block(
    pool: &PgPool,
    chain: &str,
    emitters: &[ActiveEmitter],
    before_block: i64,
) -> Result<MigratedRegistryNodes> {
    let current_registry_emitters = emitters
        .iter()
        .filter(|emitter| {
            emitter.source_family == ENS_V1_REGISTRY_SOURCE_FAMILY
                && emitter.contract_role.as_deref() == Some(CONTRACT_ROLE_REGISTRY)
        })
        .collect::<Vec<_>>();
    if current_registry_emitters.is_empty() {
        return Ok(MigratedRegistryNodes::empty());
    }

    let emitters = current_registry_emitters
        .iter()
        .map(|emitter| {
            RegistryMigrationMarkerEmitter::new(
                &emitter.address,
                emitter.active_from_block_number.unwrap_or(0),
                emitter.active_to_block_number.unwrap_or(i64::MAX),
            )
        })
        .collect::<Vec<_>>();
    load_cached_migrated_registry_nodes_before_block(
        pool,
        chain,
        &emitters,
        before_block,
        &super::hex_topic::new_owner_topic0(),
        new_owner_child_node_from_topics,
    )
    .await
}

pub(super) fn scoped_ranges_for_active_emitters(
    source_scope: &[RegistryRawLogSourceScopeTarget],
    emitters: &[ActiveEmitter],
) -> Result<Vec<RegistryRawLogSourceScopeTarget>> {
    let mut ranges = Vec::new();
    for target in source_scope {
        if target.effective_to_block < target.effective_from_block {
            bail!(
                "ENSv1 registry source scope range {}..={} is invalid for {} {}",
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

fn source_scope_target_intersects_active_emitter(
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

pub(super) fn emitter_for_block_and_scope<'a>(
    emitters: &'a [ActiveEmitter],
    block_number: i64,
    source_scope: Option<&[RegistryRawLogSourceScopeTarget]>,
) -> Option<&'a ActiveEmitter> {
    let Some(source_scope) = source_scope else {
        return emitters
            .iter()
            .filter(|emitter| emitter_active_at_block(emitter, block_number))
            .min_by(|left, right| {
                (
                    left.source_rank,
                    left.source_manifest_id,
                    left.contract_instance_id,
                )
                    .cmp(&(
                        right.source_rank,
                        right.source_manifest_id,
                        right.contract_instance_id,
                    ))
            });
    };

    emitters
        .iter()
        .filter(|emitter| emitter_active_at_block(emitter, block_number))
        .filter(|emitter| {
            source_scope.iter().any(|target| {
                target.source_family == emitter.source_family
                    && target.address == emitter.address
                    && block_number >= target.effective_from_block
                    && block_number <= target.effective_to_block
            })
        })
        .min_by(|left, right| {
            (
                left.source_rank,
                left.source_manifest_id,
                left.contract_instance_id,
            )
                .cmp(&(
                    right.source_rank,
                    right.source_manifest_id,
                    right.contract_instance_id,
                ))
        })
}

fn emitter_active_at_block(emitter: &ActiveEmitter, block_number: i64) -> bool {
    emitter
        .active_from_block_number
        .is_none_or(|active_from| block_number >= active_from)
        && emitter
            .active_to_block_number
            .is_none_or(|active_to| block_number <= active_to)
}
