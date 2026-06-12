pub(super) const ENS_V1_AUTHORITY_DERIVATION_KIND: &str = "ens_v1_unwrapped_authority";
pub(super) const ENS_V2_REGISTRY_DERIVATION_KIND: &str = "ens_v2_registry_resource_surface";
pub(super) const ADDRESS_NAMES_CURRENT_DERIVATION_KIND: &str = "address_names_current_rebuild";
pub(super) const ADDRESS_NAMES_ENUMERATION_BASIS: &str = "surface_current_relations";
pub(super) const ENS_V1_REGISTRAR_SOURCE_FAMILY: &str = "ens_v1_registrar_l1";
pub(super) const ENS_V1_REGISTRY_SOURCE_FAMILY: &str = "ens_v1_registry_l1";
pub(super) const ENS_V1_RESOLVER_SOURCE_FAMILY: &str = "ens_v1_resolver_l1";
pub(super) const ENS_V1_WRAPPER_SOURCE_FAMILY: &str = "ens_v1_wrapper_l1";
pub(super) const ENS_V2_ROOT_SOURCE_FAMILY: &str = "ens_v2_root_l1";
pub(super) const ENS_V2_REGISTRY_SOURCE_FAMILY: &str = "ens_v2_registry_l1";
pub(super) const BASENAMES_BASE_REGISTRAR_SOURCE_FAMILY: &str = "basenames_base_registrar";
pub(super) const BASENAMES_BASE_REGISTRY_SOURCE_FAMILY: &str = "basenames_base_registry";
pub(super) const BASENAMES_BASE_RESOLVER_SOURCE_FAMILY: &str = "basenames_base_resolver";
pub(super) const RELEVANT_EVENT_KINDS: &[&str] = &[
    "RegistrationGranted",
    "TokenControlTransferred",
    "AuthorityTransferred",
    "AuthorityEpochChanged",
    "PermissionChanged",
    "PermissionScopeChanged",
    "TokenRegenerated",
];
pub(super) const CANONICAL_STATE_FILTER: &str = r#"
  IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  )
"#;
