use super::super::{RESOLVER_PROFILE_STATUS_SUPPORTED, RESOLVER_PROFILE_STATUS_UNSUPPORTED};

pub(super) const ENS_V1_RESOLVER_SOURCE_FAMILY: &str = "ens_v1_resolver_l1";
pub(super) const ENS_V1_PUBLIC_RESOLVER_COMPATIBLE_PROFILE: &str = "public_resolver_compatible";
const ENS_V1_PUBLIC_RESOLVER_WRAPPER_AWARE_PROFILE: &str = "public_resolver_wrapper_aware";
const ENS_V1_PUBLIC_RESOLVER_MULTICOIN_DNS_PROFILE: &str = "public_resolver_legacy_multicoin_dns";
const ENS_V1_PUBLIC_RESOLVER_MULTICOIN_PROFILE: &str = "public_resolver_legacy_multicoin";
const ENS_V1_PUBLIC_RESOLVER_ETH_ADDR_TEXT_PROFILE: &str = "public_resolver_legacy_eth_addr_text";
const ENS_V1_PUBLIC_RESOLVER_ETH_ADDR_PROFILE: &str = "public_resolver_legacy_eth_addr";
pub(super) const ENS_V1_PUBLIC_RESOLVER_ROLE: &str = "public_resolver";
const RESOLVER_PROFILE_BASIS_MANIFEST_SEED: &str = "manifest_public_resolver_seed";
const RESOLVER_PROFILE_BASIS_FIRST_PARTY_KNOWN_RESOLVER: &str =
    "first_party_known_resolver_admission";

const FACT_RESOLVER_RECORD: &str = "resolver_record";
const FACT_RESOLVER_RECORD_ADDR: &str = "resolver_record:addr";
const FACT_RESOLVER_RECORD_MULTICOIN_ADDR: &str = "resolver_record:multicoin_addr";
const FACT_RESOLVER_RECORD_NAME: &str = "resolver_record:name";
const FACT_RESOLVER_RECORD_TEXT: &str = "resolver_record:text";
const FACT_RESOLVER_RECORD_ABI: &str = "resolver_record:abi";
const FACT_RESOLVER_RECORD_CONTENTHASH: &str = "resolver_record:contenthash";
const FACT_RESOLVER_RECORD_DNS: &str = "resolver_record:dns";
const FACT_RESOLVER_RECORD_INTERFACE: &str = "resolver_record:interface";
const FACT_RESOLVER_RECORD_DATA: &str = "resolver_record:data";
const FACT_RESOLVER_RECORD_VERSION: &str = "resolver_record_version";
const FACT_RESOLVER_AUTHORIZATION: &str = "resolver_authorization";
const FACT_NAME_WRAPPER_AWARE: &str = "resolver_feature:name_wrapper_aware";
const FACT_DEFAULT_COIN_TYPE: &str = "resolver_feature:default_coin_type";

pub(super) const DEFAULT_PENDING_FACT_FAMILIES: [&str; 3] = [
    FACT_RESOLVER_RECORD,
    FACT_RESOLVER_RECORD_VERSION,
    FACT_RESOLVER_AUTHORIZATION,
];

#[derive(Clone, Copy, Debug)]
pub(super) struct ResolverProfileFact {
    pub(super) fact_family: &'static str,
    pub(super) status: &'static str,
}

impl ResolverProfileFact {
    const fn supported(fact_family: &'static str) -> Self {
        Self {
            fact_family,
            status: RESOLVER_PROFILE_STATUS_SUPPORTED,
        }
    }

    const fn unsupported(fact_family: &'static str) -> Self {
        Self {
            fact_family,
            status: RESOLVER_PROFILE_STATUS_UNSUPPORTED,
        }
    }
}

const LATEST_FACTS: &[ResolverProfileFact] = &[
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_ADDR),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_MULTICOIN_ADDR),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_NAME),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_TEXT),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_ABI),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_CONTENTHASH),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_DNS),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_INTERFACE),
    ResolverProfileFact::unsupported(FACT_RESOLVER_RECORD_DATA),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_VERSION),
    ResolverProfileFact::supported(FACT_RESOLVER_AUTHORIZATION),
    ResolverProfileFact::supported(FACT_NAME_WRAPPER_AWARE),
    ResolverProfileFact::supported(FACT_DEFAULT_COIN_TYPE),
];

const WRAPPER_AWARE_FACTS: &[ResolverProfileFact] = &[
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_ADDR),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_MULTICOIN_ADDR),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_NAME),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_TEXT),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_ABI),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_CONTENTHASH),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_DNS),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_INTERFACE),
    ResolverProfileFact::unsupported(FACT_RESOLVER_RECORD_DATA),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_VERSION),
    ResolverProfileFact::supported(FACT_RESOLVER_AUTHORIZATION),
    ResolverProfileFact::supported(FACT_NAME_WRAPPER_AWARE),
    ResolverProfileFact::unsupported(FACT_DEFAULT_COIN_TYPE),
];

const LEGACY_ADDR_TEXT_FACTS: &[ResolverProfileFact] = &[
    ResolverProfileFact::unsupported(FACT_RESOLVER_RECORD),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_ADDR),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_MULTICOIN_ADDR),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_NAME),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_TEXT),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_ABI),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_CONTENTHASH),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_DNS),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_INTERFACE),
    ResolverProfileFact::unsupported(FACT_RESOLVER_RECORD_DATA),
    ResolverProfileFact::unsupported(FACT_RESOLVER_RECORD_VERSION),
    ResolverProfileFact::supported(FACT_RESOLVER_AUTHORIZATION),
    ResolverProfileFact::unsupported(FACT_NAME_WRAPPER_AWARE),
    ResolverProfileFact::unsupported(FACT_DEFAULT_COIN_TYPE),
];

const LEGACY_ADDR_TEXT_NO_DNS_FACTS: &[ResolverProfileFact] = &[
    ResolverProfileFact::unsupported(FACT_RESOLVER_RECORD),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_ADDR),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_MULTICOIN_ADDR),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_NAME),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_TEXT),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_ABI),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_CONTENTHASH),
    ResolverProfileFact::unsupported(FACT_RESOLVER_RECORD_DNS),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_INTERFACE),
    ResolverProfileFact::unsupported(FACT_RESOLVER_RECORD_DATA),
    ResolverProfileFact::unsupported(FACT_RESOLVER_RECORD_VERSION),
    ResolverProfileFact::supported(FACT_RESOLVER_AUTHORIZATION),
    ResolverProfileFact::unsupported(FACT_NAME_WRAPPER_AWARE),
    ResolverProfileFact::unsupported(FACT_DEFAULT_COIN_TYPE),
];

const LEGACY_ETH_ADDR_TEXT_FACTS: &[ResolverProfileFact] = &[
    ResolverProfileFact::unsupported(FACT_RESOLVER_RECORD),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_ADDR),
    ResolverProfileFact::unsupported(FACT_RESOLVER_RECORD_MULTICOIN_ADDR),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_NAME),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_TEXT),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_ABI),
    ResolverProfileFact::unsupported(FACT_RESOLVER_RECORD_CONTENTHASH),
    ResolverProfileFact::unsupported(FACT_RESOLVER_RECORD_DNS),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_INTERFACE),
    ResolverProfileFact::unsupported(FACT_RESOLVER_RECORD_DATA),
    ResolverProfileFact::unsupported(FACT_RESOLVER_RECORD_VERSION),
    ResolverProfileFact::supported(FACT_RESOLVER_AUTHORIZATION),
    ResolverProfileFact::unsupported(FACT_NAME_WRAPPER_AWARE),
    ResolverProfileFact::unsupported(FACT_DEFAULT_COIN_TYPE),
];

const LEGACY_ADDR_ONLY_FACTS: &[ResolverProfileFact] = &[
    ResolverProfileFact::unsupported(FACT_RESOLVER_RECORD),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_ADDR),
    ResolverProfileFact::unsupported(FACT_RESOLVER_RECORD_MULTICOIN_ADDR),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_NAME),
    ResolverProfileFact::unsupported(FACT_RESOLVER_RECORD_TEXT),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_ABI),
    ResolverProfileFact::unsupported(FACT_RESOLVER_RECORD_CONTENTHASH),
    ResolverProfileFact::unsupported(FACT_RESOLVER_RECORD_DNS),
    ResolverProfileFact::supported(FACT_RESOLVER_RECORD_INTERFACE),
    ResolverProfileFact::unsupported(FACT_RESOLVER_RECORD_DATA),
    ResolverProfileFact::unsupported(FACT_RESOLVER_RECORD_VERSION),
    ResolverProfileFact::supported(FACT_RESOLVER_AUTHORIZATION),
    ResolverProfileFact::unsupported(FACT_NAME_WRAPPER_AWARE),
    ResolverProfileFact::unsupported(FACT_DEFAULT_COIN_TYPE),
];

#[derive(Clone, Copy, Debug)]
pub(super) struct EnsV1ResolverProfileConfig {
    pub(super) role: &'static str,
    pub(super) profile: &'static str,
    pub(super) fact_families: &'static [ResolverProfileFact],
    pub(super) manifest_seed_basis: &'static str,
}

pub(super) const ENS_V1_RESOLVER_PROFILE_CONFIGS: &[EnsV1ResolverProfileConfig] = &[
    EnsV1ResolverProfileConfig {
        role: ENS_V1_PUBLIC_RESOLVER_ROLE,
        profile: ENS_V1_PUBLIC_RESOLVER_COMPATIBLE_PROFILE,
        fact_families: LATEST_FACTS,
        manifest_seed_basis: RESOLVER_PROFILE_BASIS_MANIFEST_SEED,
    },
    EnsV1ResolverProfileConfig {
        role: "public_resolver_231b0ee",
        profile: ENS_V1_PUBLIC_RESOLVER_WRAPPER_AWARE_PROFILE,
        fact_families: WRAPPER_AWARE_FACTS,
        manifest_seed_basis: RESOLVER_PROFILE_BASIS_FIRST_PARTY_KNOWN_RESOLVER,
    },
    EnsV1ResolverProfileConfig {
        role: "public_resolver_4976fb03",
        profile: ENS_V1_PUBLIC_RESOLVER_MULTICOIN_DNS_PROFILE,
        fact_families: LEGACY_ADDR_TEXT_FACTS,
        manifest_seed_basis: RESOLVER_PROFILE_BASIS_FIRST_PARTY_KNOWN_RESOLVER,
    },
    EnsV1ResolverProfileConfig {
        role: "public_resolver_daaf96c3",
        profile: ENS_V1_PUBLIC_RESOLVER_MULTICOIN_DNS_PROFILE,
        fact_families: LEGACY_ADDR_TEXT_FACTS,
        manifest_seed_basis: RESOLVER_PROFILE_BASIS_FIRST_PARTY_KNOWN_RESOLVER,
    },
    EnsV1ResolverProfileConfig {
        role: "public_resolver_226159d5",
        profile: ENS_V1_PUBLIC_RESOLVER_MULTICOIN_PROFILE,
        fact_families: LEGACY_ADDR_TEXT_NO_DNS_FACTS,
        manifest_seed_basis: RESOLVER_PROFILE_BASIS_FIRST_PARTY_KNOWN_RESOLVER,
    },
    EnsV1ResolverProfileConfig {
        role: "public_resolver_5ffc0143",
        profile: ENS_V1_PUBLIC_RESOLVER_ETH_ADDR_TEXT_PROFILE,
        fact_families: LEGACY_ETH_ADDR_TEXT_FACTS,
        manifest_seed_basis: RESOLVER_PROFILE_BASIS_FIRST_PARTY_KNOWN_RESOLVER,
    },
    EnsV1ResolverProfileConfig {
        role: "public_resolver_1da02271",
        profile: ENS_V1_PUBLIC_RESOLVER_ETH_ADDR_PROFILE,
        fact_families: LEGACY_ADDR_ONLY_FACTS,
        manifest_seed_basis: RESOLVER_PROFILE_BASIS_FIRST_PARTY_KNOWN_RESOLVER,
    },
];

pub(super) fn profile_config_for_role(role: &str) -> Option<&'static EnsV1ResolverProfileConfig> {
    ENS_V1_RESOLVER_PROFILE_CONFIGS
        .iter()
        .find(|config| config.role == role)
}
