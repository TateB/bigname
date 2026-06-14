use bigname_storage::{
    BASE_MAINNET_CHAIN_ID as STORAGE_BASE_MAINNET_CHAIN_ID,
    ETHEREUM_MAINNET_CHAIN_ID as STORAGE_ETHEREUM_MAINNET_CHAIN_ID,
};

const ETHEREUM_SEPOLIA_CHAIN_ID: &str = "ethereum-sepolia";
const BASE_SEPOLIA_CHAIN_ID: &str = "base-sepolia";

struct ChainIdMapping {
    slug: &'static str,
    numeric: u64,
}

// Only the two mainnet mappings are exercised today: the current scope builder
// emits mainnet storage slugs. The Sepolia slugs are provisional placeholders
// using the existing "<chain>-<network>" convention; no codebase-wide Sepolia
// storage constants exist yet, so verify them against real testnet config
// before relying on Sepolia snapshots. The numeric ids are canonical EVM chain
// ids.
const CHAIN_ID_MAPPINGS: &[ChainIdMapping] = &[
    ChainIdMapping {
        slug: STORAGE_ETHEREUM_MAINNET_CHAIN_ID,
        numeric: 1,
    },
    ChainIdMapping {
        slug: STORAGE_BASE_MAINNET_CHAIN_ID,
        numeric: 8453,
    },
    ChainIdMapping {
        slug: ETHEREUM_SEPOLIA_CHAIN_ID,
        numeric: 11_155_111,
    },
    ChainIdMapping {
        slug: BASE_SEPOLIA_CHAIN_ID,
        numeric: 84_532,
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
}
