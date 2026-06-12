use super::{
    constants::{
        BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY, BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
        BASENAMES_BASE_RESOLVER_SOURCE_FAMILY, ENS_V1_AUTHORITY_DERIVATION_KIND,
        ENS_V1_REGISTRAR_SOURCE_FAMILY, ENS_V1_REGISTRY_SOURCE_FAMILY,
        ENS_V1_RESOLVER_SOURCE_FAMILY, ENS_V1_WRAPPER_SOURCE_FAMILY,
        ENS_V2_REGISTRY_DERIVATION_KIND, ENS_V2_REGISTRY_SOURCE_FAMILY, ENS_V2_ROOT_SOURCE_FAMILY,
    },
    model::RelevantEvent,
};

pub(super) fn authority_source_families(namespace: &str) -> Vec<&'static str> {
    match namespace {
        "basenames" => vec![
            BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY,
            BASENAMES_BASE_REGISTRY_SOURCE_FAMILY,
            BASENAMES_BASE_RESOLVER_SOURCE_FAMILY,
        ],
        _ => vec![
            ENS_V1_REGISTRAR_SOURCE_FAMILY,
            ENS_V1_REGISTRY_SOURCE_FAMILY,
            ENS_V1_RESOLVER_SOURCE_FAMILY,
            ENS_V1_WRAPPER_SOURCE_FAMILY,
            ENS_V2_ROOT_SOURCE_FAMILY,
            ENS_V2_REGISTRY_SOURCE_FAMILY,
        ],
    }
}

pub(super) fn authority_derivation_kinds(namespace: &str) -> Vec<&'static str> {
    match namespace {
        "basenames" => vec![ENS_V1_AUTHORITY_DERIVATION_KIND],
        _ => vec![
            ENS_V1_AUTHORITY_DERIVATION_KIND,
            ENS_V2_REGISTRY_DERIVATION_KIND,
        ],
    }
}

pub(super) fn address_names_source_classes(
    namespace: &str,
    events: &[RelevantEvent],
) -> Vec<&'static str> {
    if namespace == "basenames" {
        return vec!["ensv1_registry_path"];
    }

    let has_ens_v1 = events
        .iter()
        .any(|event| event.source_family.starts_with("ens_v1_"));
    let has_ens_v2 = events
        .iter()
        .any(|event| event.source_family.starts_with("ens_v2_"));

    match (has_ens_v1, has_ens_v2) {
        (false, true) => vec!["ensv2_registry_resource_surface"],
        (true, true) => vec!["ensv1_registry_path", "ensv2_registry_resource_surface"],
        _ => vec!["ensv1_registry_path"],
    }
}
