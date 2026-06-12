pub(super) const SOURCE_FAMILY_ENS_V2_ROOT_L1: &str = "ens_v2_root_l1";
pub(super) const SOURCE_FAMILY_ENS_V2_REGISTRY_L1: &str = "ens_v2_registry_l1";
pub(super) const SOURCE_FAMILY_ENS_V2_RESOLVER_L1: &str = "ens_v2_resolver_l1";
pub(super) const DERIVATION_KIND_ENS_V2_PERMISSIONS: &str = "ens_v2_permissions";
pub(super) const RESOLVER_EDGE_KIND: &str = "resolver";
pub(super) const EVENT_KIND_PERMISSION_CHANGED: &str = "PermissionChanged";
pub(super) const EVENT_KIND_ROOT_PERMISSION_CHANGED: &str = "RootPermissionChanged";

pub(super) const ABI_EVENT_NAMED_RESOURCE_SIGNATURE: &str = "NamedResource(uint256,bytes)";
pub(super) const ABI_EVENT_NAMED_TEXT_RESOURCE_SIGNATURE: &str =
    "NamedTextResource(uint256,bytes,bytes32,string)";
pub(super) const ABI_EVENT_NAMED_ADDR_RESOURCE_SIGNATURE: &str =
    "NamedAddrResource(uint256,bytes,uint256)";
pub(super) const ABI_EVENT_EAC_ROLES_CHANGED_SIGNATURE: &str =
    "EACRolesChanged(uint256,address,uint256,uint256)";
