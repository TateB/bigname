use super::{
    BASE_NORMALIZED_REDERIVE_ADAPTER, BASE_NORMALIZED_REDERIVE_BACKLOG_CURSOR_KIND,
    BASE_NORMALIZED_REDERIVE_CURSOR_KIND, BASE_NORMALIZED_REDERIVE_DISCOVERY_ADAPTER,
    BASE_NORMALIZED_REDERIVE_REGISTRY_RESOLVER_CHANGED_DERIVATION_KIND,
    BASE_NORMALIZED_REDERIVE_REVERSE_CLAIM_ADAPTER,
    BASE_NORMALIZED_REDERIVE_REVERSE_CLAIM_DERIVATION_KIND,
    BASE_NORMALIZED_REDERIVE_SUBREGISTRY_CHANGED_DERIVATION_KIND,
    BASE_NORMALIZED_REDERIVE_UNWRAPPED_AUTHORITY_DERIVATION_KIND,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BaseNormalizedRederiveScopeRule {
    pub adapter: &'static str,
    pub derivation_kinds: &'static [&'static str],
    pub source_families: &'static [&'static str],
}

pub fn base_normalized_rederive_scope_rules() -> &'static [BaseNormalizedRederiveScopeRule] {
    &[
        BaseNormalizedRederiveScopeRule {
            adapter: BASE_NORMALIZED_REDERIVE_REVERSE_CLAIM_ADAPTER,
            derivation_kinds: &[BASE_NORMALIZED_REDERIVE_REVERSE_CLAIM_DERIVATION_KIND],
            source_families: &["ens_v1_reverse_l1", "basenames_base_primary"],
        },
        BaseNormalizedRederiveScopeRule {
            adapter: BASE_NORMALIZED_REDERIVE_DISCOVERY_ADAPTER,
            derivation_kinds: &[
                BASE_NORMALIZED_REDERIVE_REGISTRY_RESOLVER_CHANGED_DERIVATION_KIND,
                BASE_NORMALIZED_REDERIVE_SUBREGISTRY_CHANGED_DERIVATION_KIND,
            ],
            source_families: &["ens_v1_registry_l1", "basenames_base_registry"],
        },
        BaseNormalizedRederiveScopeRule {
            adapter: BASE_NORMALIZED_REDERIVE_ADAPTER,
            derivation_kinds: &[BASE_NORMALIZED_REDERIVE_UNWRAPPED_AUTHORITY_DERIVATION_KIND],
            source_families: &[
                "ens_v1_registrar_l1",
                "ens_v1_registry_l1",
                "ens_v1_resolver_l1",
                "ens_v1_wrapper_l1",
                "basenames_base_registrar",
                "basenames_base_registry",
                "basenames_base_resolver",
            ],
        },
    ]
}

pub(super) fn reverse_claim_derivation_kind() -> String {
    BASE_NORMALIZED_REDERIVE_REVERSE_CLAIM_DERIVATION_KIND.to_owned()
}

pub(super) fn reverse_claim_source_families() -> Vec<String> {
    vec![
        "ens_v1_reverse_l1".to_owned(),
        "basenames_base_primary".to_owned(),
    ]
}

pub(super) fn subregistry_derivation_kinds() -> Vec<String> {
    vec![
        BASE_NORMALIZED_REDERIVE_REGISTRY_RESOLVER_CHANGED_DERIVATION_KIND.to_owned(),
        BASE_NORMALIZED_REDERIVE_SUBREGISTRY_CHANGED_DERIVATION_KIND.to_owned(),
    ]
}

pub(super) fn subregistry_source_families() -> Vec<String> {
    vec![
        "ens_v1_registry_l1".to_owned(),
        "basenames_base_registry".to_owned(),
    ]
}

pub(super) fn unwrapped_authority_derivation_kind() -> String {
    BASE_NORMALIZED_REDERIVE_UNWRAPPED_AUTHORITY_DERIVATION_KIND.to_owned()
}

pub(super) fn unwrapped_authority_source_families() -> Vec<String> {
    vec![
        "ens_v1_registrar_l1".to_owned(),
        "ens_v1_registry_l1".to_owned(),
        "ens_v1_resolver_l1".to_owned(),
        "ens_v1_wrapper_l1".to_owned(),
        "basenames_base_registrar".to_owned(),
        "basenames_base_registry".to_owned(),
        "basenames_base_resolver".to_owned(),
    ]
}

pub(super) fn cursor_kinds() -> Vec<String> {
    [
        BASE_NORMALIZED_REDERIVE_CURSOR_KIND,
        BASE_NORMALIZED_REDERIVE_BACKLOG_CURSOR_KIND,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

pub(super) fn checkpoint_adapters() -> Vec<String> {
    [
        BASE_NORMALIZED_REDERIVE_REVERSE_CLAIM_ADAPTER,
        BASE_NORMALIZED_REDERIVE_DISCOVERY_ADAPTER,
        BASE_NORMALIZED_REDERIVE_ADAPTER,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

pub(super) fn current_projection_replay_status_projections() -> Vec<String> {
    [
        "address_names_current",
        "children_current",
        "name_current",
        "permissions_current",
        "primary_names_current",
        "record_inventory_current",
        "resolver_current",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}
