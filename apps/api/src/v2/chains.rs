use bigname_storage::{
    BASE_MAINNET_CHAIN_ID as STORAGE_BASE_MAINNET_CHAIN_ID,
    ETHEREUM_MAINNET_CHAIN_ID as STORAGE_ETHEREUM_MAINNET_CHAIN_ID,
};

const ETHEREUM_SEPOLIA_CHAIN_ID: &str = "ethereum-sepolia";
const BASE_SEPOLIA_CHAIN_ID: &str = "base-sepolia";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeploymentProfile {
    Mainnet,
    Sepolia,
}

struct ChainIdMapping {
    slug: &'static str,
    numeric: u64,
    profile: DeploymentProfile,
}

// V2 snapshot tokens stay storage-native, while meta.as_of renders numeric EVM
// chain ids. Mainnet remains the default exact-name profile, but a position
// pinned `at` token can select the ENSv2 Sepolia profile, so the registry must
// cover those slugs for both emitted metadata and token replay.
const CHAIN_ID_MAPPINGS: &[ChainIdMapping] = &[
    ChainIdMapping {
        slug: STORAGE_ETHEREUM_MAINNET_CHAIN_ID,
        numeric: 1,
        profile: DeploymentProfile::Mainnet,
    },
    ChainIdMapping {
        slug: STORAGE_BASE_MAINNET_CHAIN_ID,
        numeric: 8453,
        profile: DeploymentProfile::Mainnet,
    },
    ChainIdMapping {
        slug: ETHEREUM_SEPOLIA_CHAIN_ID,
        numeric: 11_155_111,
        profile: DeploymentProfile::Sepolia,
    },
    ChainIdMapping {
        slug: BASE_SEPOLIA_CHAIN_ID,
        numeric: 84_532,
        profile: DeploymentProfile::Sepolia,
    },
];

pub(crate) fn slug_to_numeric(slug: &str) -> Option<u64> {
    CHAIN_ID_MAPPINGS
        .iter()
        .find(|mapping| mapping.slug == slug)
        .map(|mapping| mapping.numeric)
}

pub(crate) fn numeric_to_slug(chain_id: u64) -> Option<&'static str> {
    CHAIN_ID_MAPPINGS
        .iter()
        .find(|mapping| mapping.numeric == chain_id)
        .map(|mapping| mapping.slug)
}

pub(crate) fn deployment_profile_for_slug(slug: &str) -> Option<DeploymentProfile> {
    CHAIN_ID_MAPPINGS
        .iter()
        .find(|mapping| mapping.slug == slug)
        .map(|mapping| mapping.profile)
}

pub(crate) fn snapshot_slot_for_slug(slug: &str) -> Option<&'static str> {
    match slug {
        STORAGE_ETHEREUM_MAINNET_CHAIN_ID => Some("ethereum"),
        STORAGE_BASE_MAINNET_CHAIN_ID => Some("base"),
        ETHEREUM_SEPOLIA_CHAIN_ID => Some(ETHEREUM_SEPOLIA_CHAIN_ID),
        BASE_SEPOLIA_CHAIN_ID => Some(BASE_SEPOLIA_CHAIN_ID),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_id_registry_round_trips_all_known_mappings() {
        for (slug, numeric) in [
            (STORAGE_ETHEREUM_MAINNET_CHAIN_ID, 1),
            (STORAGE_BASE_MAINNET_CHAIN_ID, 8453),
            (ETHEREUM_SEPOLIA_CHAIN_ID, 11_155_111),
            (BASE_SEPOLIA_CHAIN_ID, 84_532),
        ] {
            assert_eq!(slug_to_numeric(slug), Some(numeric));
            assert_eq!(numeric_to_slug(numeric), Some(slug));
        }
    }

    #[test]
    fn chain_id_registry_rejects_unknown_values() {
        assert_eq!(slug_to_numeric("unknown-mainnet"), None);
        assert_eq!(numeric_to_slug(99_999_999), None);
    }

    #[test]
    fn mainnet_slugs_reuse_storage_constants() {
        assert_eq!(
            numeric_to_slug(1),
            Some(bigname_storage::ETHEREUM_MAINNET_CHAIN_ID)
        );
        assert_eq!(
            numeric_to_slug(8453),
            Some(bigname_storage::BASE_MAINNET_CHAIN_ID)
        );
    }

    #[test]
    fn chain_id_registry_classifies_deployment_profiles() {
        assert_eq!(
            deployment_profile_for_slug(STORAGE_ETHEREUM_MAINNET_CHAIN_ID),
            Some(DeploymentProfile::Mainnet)
        );
        assert_eq!(
            deployment_profile_for_slug(STORAGE_BASE_MAINNET_CHAIN_ID),
            Some(DeploymentProfile::Mainnet)
        );
        assert_eq!(
            deployment_profile_for_slug(ETHEREUM_SEPOLIA_CHAIN_ID),
            Some(DeploymentProfile::Sepolia)
        );
        assert_eq!(
            deployment_profile_for_slug(BASE_SEPOLIA_CHAIN_ID),
            Some(DeploymentProfile::Sepolia)
        );
        assert_eq!(deployment_profile_for_slug("unknown-mainnet"), None);
    }

    #[test]
    fn snapshot_slots_use_profile_vocabulary() {
        assert_eq!(
            snapshot_slot_for_slug(STORAGE_ETHEREUM_MAINNET_CHAIN_ID),
            Some("ethereum")
        );
        assert_eq!(
            snapshot_slot_for_slug(STORAGE_BASE_MAINNET_CHAIN_ID),
            Some("base")
        );
        assert_eq!(
            snapshot_slot_for_slug(ETHEREUM_SEPOLIA_CHAIN_ID),
            Some("ethereum-sepolia")
        );
        assert_eq!(
            snapshot_slot_for_slug(BASE_SEPOLIA_CHAIN_ID),
            Some("base-sepolia")
        );
        assert_eq!(snapshot_slot_for_slug("unknown-mainnet"), None);
    }
}
