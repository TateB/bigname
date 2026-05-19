use super::*;
use crate::evm_abi;
use crate::registry_migration_cache::MigratedRegistryNodes;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum RegistryMigrationGuardAction {
    MarkMigrated(String),
    SuppressIfMigrated(String),
    None,
}

impl RegistryMigrationGuardAction {
    pub(super) fn suppressed_by(&self, migrated_nodes: &MigratedRegistryNodes) -> bool {
        matches!(self, Self::SuppressIfMigrated(node) if migrated_nodes.contains(node))
    }

    pub(super) fn mark_migrated_node(&self) -> Option<&str> {
        match self {
            Self::MarkMigrated(node) => Some(node),
            Self::SuppressIfMigrated(_) | Self::None => None,
        }
    }
}

pub(super) fn registry_migration_guard_action(
    raw_log: &AuthorityRawLogRow,
    event_topics: &AuthorityEventTopics,
) -> Result<RegistryMigrationGuardAction> {
    if raw_log.source_family != SOURCE_FAMILY_ENS_V1_REGISTRY_L1 {
        return Ok(RegistryMigrationGuardAction::None);
    }
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(RegistryMigrationGuardAction::None);
    };

    if event_topics.matches(NEW_OWNER_SIGNATURE, topic0)? {
        let node = registry_new_owner_child_node(raw_log)?;
        return Ok(if is_old_registry(raw_log) {
            RegistryMigrationGuardAction::SuppressIfMigrated(node)
        } else {
            RegistryMigrationGuardAction::MarkMigrated(node)
        });
    }

    if !is_old_registry(raw_log) {
        return Ok(RegistryMigrationGuardAction::None);
    }

    if event_topics.matches(NEW_RESOLVER_SIGNATURE, topic0)? {
        return Ok(RegistryMigrationGuardAction::SuppressIfMigrated(
            registry_indexed_node(raw_log, "NewResolver")?,
        ));
    }
    if event_topics.matches(REGISTRY_TRANSFER_SIGNATURE, topic0)? {
        return Ok(RegistryMigrationGuardAction::SuppressIfMigrated(
            registry_indexed_node(raw_log, "Transfer")?,
        ));
    }
    if event_topics.matches(NEW_TTL_SIGNATURE, topic0)? {
        return Ok(RegistryMigrationGuardAction::SuppressIfMigrated(
            registry_indexed_node(raw_log, "NewTTL")?,
        ));
    }

    Ok(RegistryMigrationGuardAction::None)
}

fn is_old_registry(raw_log: &AuthorityRawLogRow) -> bool {
    raw_log.contract_role.as_deref() == Some(CONTRACT_ROLE_REGISTRY_OLD)
}

pub(super) fn registry_new_owner_child_node_from_topics(topics: &[String]) -> Result<String> {
    let parent_node = normalize_hex_32(
        topics
            .get(1)
            .context("NewOwner log is missing parent node")?,
    )?;
    let labelhash = normalize_hex_32(
        topics
            .get(2)
            .context("NewOwner log is missing indexed labelhash")?,
    )?;
    hash_pair(&parent_node, &labelhash)
}

fn registry_new_owner_child_node(raw_log: &AuthorityRawLogRow) -> Result<String> {
    registry_new_owner_child_node_from_topics(&raw_log.topics)
}

fn registry_indexed_node(raw_log: &AuthorityRawLogRow, event_name: &str) -> Result<String> {
    normalize_hex_32(
        raw_log
            .topics
            .get(1)
            .with_context(|| format!("{event_name} log is missing indexed node"))?,
    )
}

fn hash_pair(left: &str, right: &str) -> Result<String> {
    evm_abi::child_namehash_hex(left, right)
}
