use crate::registry_migration_cache::MigratedRegistryNodes;
use anyhow::{Context, Result};

use super::{
    CONTRACT_ROLE_REGISTRY, CONTRACT_ROLE_REGISTRY_OLD, ENS_V1_REGISTRY_SOURCE_FAMILY,
    assignment::{ObservedRegistryAssignment, RegistryDiscoveryKind},
    hex_topic::{
        ZERO_NODE, child_node, new_owner_topic0, new_resolver_topic0, new_ttl_topic0,
        normalize_hex_32, registry_transfer_topic0,
    },
    loader::{ActiveEmitter, RegistryRawLogRow},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum RegistryMigrationGuardAction {
    MarkMigrated(String),
    SuppressIfMigrated(String),
    OldRootResolverException,
    None,
}

impl RegistryMigrationGuardAction {
    pub(super) fn suppressed_by(&self, migrated_nodes: &MigratedRegistryNodes) -> bool {
        matches!(self, Self::SuppressIfMigrated(node) if migrated_nodes.contains(node))
    }

    pub(super) fn mark_migrated_node(&self) -> Option<&str> {
        match self {
            Self::MarkMigrated(node) => Some(node),
            Self::SuppressIfMigrated(_) | Self::OldRootResolverException | Self::None => None,
        }
    }
}

pub(super) fn registry_migration_guard_action(
    raw_log: &RegistryRawLogRow,
) -> Result<RegistryMigrationGuardAction> {
    if raw_log.source_family != ENS_V1_REGISTRY_SOURCE_FAMILY {
        return Ok(RegistryMigrationGuardAction::None);
    }
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(RegistryMigrationGuardAction::None);
    };

    if topic0.eq_ignore_ascii_case(&new_owner_topic0()) {
        let node = new_owner_child_node(raw_log)?;
        return Ok(if is_old_registry(raw_log) {
            RegistryMigrationGuardAction::SuppressIfMigrated(node)
        } else {
            RegistryMigrationGuardAction::MarkMigrated(node)
        });
    }

    if !is_old_registry(raw_log) {
        return Ok(RegistryMigrationGuardAction::None);
    }

    if topic0.eq_ignore_ascii_case(&new_resolver_topic0()) {
        let node = indexed_node(raw_log, "NewResolver")?;
        return Ok(if node == ZERO_NODE {
            RegistryMigrationGuardAction::OldRootResolverException
        } else {
            RegistryMigrationGuardAction::SuppressIfMigrated(node)
        });
    }
    if topic0.eq_ignore_ascii_case(&registry_transfer_topic0()) {
        return Ok(RegistryMigrationGuardAction::SuppressIfMigrated(
            indexed_node(raw_log, "Transfer")?,
        ));
    }
    if topic0.eq_ignore_ascii_case(&new_ttl_topic0()) {
        return Ok(RegistryMigrationGuardAction::SuppressIfMigrated(
            indexed_node(raw_log, "NewTTL")?,
        ));
    }

    Ok(RegistryMigrationGuardAction::None)
}

pub(super) fn rewrite_old_registry_assignment(
    assignment: &mut ObservedRegistryAssignment,
    current_registry: Option<&ActiveEmitter>,
    action: &RegistryMigrationGuardAction,
) {
    if !matches!(
        action,
        RegistryMigrationGuardAction::SuppressIfMigrated(_)
            | RegistryMigrationGuardAction::OldRootResolverException
    ) {
        return;
    }
    if assignment.raw_log.contract_role.as_deref() != Some(CONTRACT_ROLE_REGISTRY_OLD) {
        return;
    }
    let Some(current_registry) = current_registry else {
        return;
    };

    if assignment.discovery_kind == RegistryDiscoveryKind::Resolver {
        let node = assignment.node.as_deref().unwrap_or(ZERO_NODE);
        assignment.observation_key = format!("resolver:{}:{node}", current_registry.address);
    }
    assignment.from_address = current_registry.address.clone();
    assignment.migration_epoch_input = true;
    if matches!(
        action,
        RegistryMigrationGuardAction::OldRootResolverException
    ) {
        assignment.old_root_resolver_exception = true;
    }
}

pub(super) fn current_registry_emitter(emitters: &[ActiveEmitter]) -> Option<&ActiveEmitter> {
    emitters
        .iter()
        .filter(|emitter| {
            emitter.source_family == ENS_V1_REGISTRY_SOURCE_FAMILY
                && emitter.contract_role.as_deref() == Some(CONTRACT_ROLE_REGISTRY)
        })
        .min_by(|left, right| emitter_precedence(left).cmp(&emitter_precedence(right)))
}

fn emitter_precedence(emitter: &ActiveEmitter) -> (i32, i64, sqlx::types::Uuid) {
    (
        emitter.source_rank,
        emitter.source_manifest_id,
        emitter.contract_instance_id,
    )
}

fn is_old_registry(raw_log: &RegistryRawLogRow) -> bool {
    raw_log.contract_role.as_deref() == Some(CONTRACT_ROLE_REGISTRY_OLD)
}

pub(super) fn new_owner_child_node_from_topics(topics: &[String]) -> Result<String> {
    let parent_node = topics
        .get(1)
        .context("NewOwner log is missing indexed parent node topic")?;
    let labelhash = topics
        .get(2)
        .context("NewOwner log is missing indexed labelhash topic")?;
    child_node(parent_node, labelhash)
}

fn new_owner_child_node(raw_log: &RegistryRawLogRow) -> Result<String> {
    new_owner_child_node_from_topics(&raw_log.topics)
}

fn indexed_node(raw_log: &RegistryRawLogRow, event_name: &str) -> Result<String> {
    normalize_hex_32(
        raw_log
            .topics
            .get(1)
            .with_context(|| format!("{event_name} log is missing indexed node topic"))?,
    )
}
