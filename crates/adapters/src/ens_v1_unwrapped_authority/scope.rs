use super::migration_guard::registry_new_owner_child_node_from_topics;
use super::*;
use crate::registry_migration_cache::{
    MigratedRegistryNodes, RegistryMigrationMarkerEmitter,
    load_migrated_registry_nodes_before_block as load_cached_migrated_registry_nodes_before_block,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct AuthorityRawLogSourceScopeTarget {
    pub(super) source_family: String,
    pub(super) address: String,
    pub(super) effective_from_block: i64,
    pub(super) effective_to_block: i64,
}

pub(super) fn normalized_authority_source_scope_targets(
    source_scope: &[(String, String, i64, i64)],
) -> Vec<AuthorityRawLogSourceScopeTarget> {
    source_scope
        .iter()
        .map(
            |(source_family, address, effective_from_block, effective_to_block)| {
                AuthorityRawLogSourceScopeTarget {
                    source_family: source_family.clone(),
                    address: normalize_source_scope_address(address),
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
    active_emitters: &[ActiveEmitter],
    before_block: i64,
    event_topics: &AuthorityEventTopics,
) -> Result<MigratedRegistryNodes> {
    let current_registry_emitters = active_emitters
        .iter()
        .filter(|emitter| {
            emitter.source_family == SOURCE_FAMILY_ENS_V1_REGISTRY_L1
                && emitter.contract_role.as_deref() == Some("registry")
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
        event_topics.topic0(NEW_OWNER_SIGNATURE)?,
        registry_new_owner_child_node_from_topics,
    )
    .await
}

pub(super) fn scoped_ranges_for_active_emitters(
    source_scope: &[AuthorityRawLogSourceScopeTarget],
    active_emitters: &[ActiveEmitter],
) -> Result<Vec<AuthorityRawLogSourceScopeTarget>> {
    let mut ranges = Vec::new();
    for target in source_scope {
        if is_generic_resolver_event_source_scope_target(target) {
            continue;
        }
        if target.effective_to_block < target.effective_from_block {
            bail!(
                "ENSv1 unwrapped authority source scope range {}..={} is invalid for {} {}",
                target.effective_from_block,
                target.effective_to_block,
                target.source_family,
                target.address
            );
        }
        if active_emitters
            .iter()
            .any(|emitter| source_scope_target_intersects_active_emitter(target, emitter))
        {
            ranges.push(target.clone());
        }
    }
    Ok(ranges)
}

fn source_scope_target_intersects_active_emitter(
    target: &AuthorityRawLogSourceScopeTarget,
    emitter: &ActiveEmitter,
) -> bool {
    if target.source_family != emitter.source_family || target.address != emitter.address {
        return false;
    }

    let emitter_from = emitter.active_from_block_number.unwrap_or(0);
    let emitter_to = emitter.active_to_block_number.unwrap_or(i64::MAX);
    target.effective_from_block <= emitter_to && emitter_from <= target.effective_to_block
}

pub(super) fn is_generic_resolver_event_source_scope_target(
    target: &AuthorityRawLogSourceScopeTarget,
) -> bool {
    target.source_family == SOURCE_FAMILY_ENS_V1_RESOLVER_L1
        && target.address == GENERIC_SOURCE_SCOPE_ADDRESS
}

fn normalize_source_scope_address(address: &str) -> String {
    if address == GENERIC_SOURCE_SCOPE_ADDRESS {
        GENERIC_SOURCE_SCOPE_ADDRESS.to_owned()
    } else {
        address.to_ascii_lowercase()
    }
}

pub(super) fn emitter_for_block_and_scope<'a>(
    emitters: &'a [ActiveEmitter],
    block_number: i64,
    source_scope: Option<&[AuthorityRawLogSourceScopeTarget]>,
) -> Option<&'a ActiveEmitter> {
    let Some(source_scope) = source_scope else {
        return emitters
            .iter()
            .filter(|emitter| emitter_active_at_block(emitter, block_number))
            .min_by(|left, right| {
                (left.source_rank, left.source_manifest_id)
                    .cmp(&(right.source_rank, right.source_manifest_id))
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
            (left.source_rank, left.source_manifest_id)
                .cmp(&(right.source_rank, right.source_manifest_id))
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
