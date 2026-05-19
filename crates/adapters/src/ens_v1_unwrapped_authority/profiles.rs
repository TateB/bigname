use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AuthorityProfile {
    Ens,
    Basenames,
}

impl AuthorityProfile {
    pub(super) const fn namespace(self) -> &'static str {
        match self {
            Self::Ens => "ens",
            Self::Basenames => "basenames",
        }
    }

    pub(super) const fn registrar_source_family(self) -> &'static str {
        match self {
            Self::Ens => SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
            Self::Basenames => SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
        }
    }

    pub(super) const fn registry_source_family(self) -> &'static str {
        match self {
            Self::Ens => SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
            Self::Basenames => SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
        }
    }

    pub(super) const fn resolver_source_family(self) -> &'static str {
        match self {
            Self::Ens => SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
            Self::Basenames => SOURCE_FAMILY_BASENAMES_BASE_RESOLVER,
        }
    }

    pub(super) const fn wrapper_source_family(self) -> Option<&'static str> {
        match self {
            Self::Ens => Some(SOURCE_FAMILY_ENS_V1_WRAPPER_L1),
            Self::Basenames => None,
        }
    }

    pub(super) fn root_node(self) -> String {
        match self {
            Self::Ens => eth_node(),
            Self::Basenames => base_eth_node(),
        }
    }

    pub(super) fn observe_name(
        self,
        label: &str,
        normalizer_version: &str,
    ) -> Result<NameMetadata> {
        observe_registrar_name_with_version(label, self, normalizer_version)
    }
}

pub(super) fn default_registrar_source_family(namespace: &str) -> &'static str {
    match namespace {
        "basenames" => SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR,
        _ => SOURCE_FAMILY_ENS_V1_REGISTRAR_L1,
    }
}

pub(super) fn authority_profile_for_source_family(source_family: &str) -> Option<AuthorityProfile> {
    match source_family {
        SOURCE_FAMILY_ENS_V1_REGISTRAR_L1
        | SOURCE_FAMILY_ENS_V1_REGISTRY_L1
        | SOURCE_FAMILY_ENS_V1_RESOLVER_L1
        | SOURCE_FAMILY_ENS_V1_WRAPPER_L1 => Some(AuthorityProfile::Ens),
        SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR
        | SOURCE_FAMILY_BASENAMES_BASE_REGISTRY
        | SOURCE_FAMILY_BASENAMES_BASE_RESOLVER => Some(AuthorityProfile::Basenames),
        _ => None,
    }
}
