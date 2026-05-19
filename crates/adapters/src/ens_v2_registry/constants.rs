pub(super) const SOURCE_FAMILY_ENS_V2_ROOT_L1: &str = "ens_v2_root_l1";
pub(super) const SOURCE_FAMILY_ENS_V2_REGISTRY_L1: &str = "ens_v2_registry_l1";
pub(super) const DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE: &str =
    "ens_v2_registry_resource_surface";
pub(super) const RESOLVER_EDGE_KIND: &str = "resolver";
pub(super) const SUBREGISTRY_EDGE_KIND: &str = "subregistry";
pub(super) const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

pub(super) const EVENT_KIND_REGISTRATION_GRANTED: &str = "RegistrationGranted";
pub(super) const EVENT_KIND_REGISTRATION_RESERVED: &str = "RegistrationReserved";
pub(super) const EVENT_KIND_REGISTRATION_RELEASED: &str = "RegistrationReleased";
pub(super) const EVENT_KIND_REGISTRATION_RENEWED: &str = "RegistrationRenewed";
pub(super) const EVENT_KIND_EXPIRY_CHANGED: &str = "ExpiryChanged";
pub(super) const EVENT_KIND_AUTHORITY_TRANSFERRED: &str = "AuthorityTransferred";
pub(super) const EVENT_KIND_RESOLVER_CHANGED: &str = "ResolverChanged";
pub(super) const EVENT_KIND_SUBREGISTRY_CHANGED: &str = "SubregistryChanged";
pub(super) const EVENT_KIND_PARENT_CHANGED: &str = "ParentChanged";
pub(super) const EVENT_KIND_TOKEN_RESOURCE_LINKED: &str = "TokenResourceLinked";
pub(super) const EVENT_KIND_TOKEN_REGENERATED: &str = "TokenRegenerated";
pub(super) const EVENT_KIND_SURFACE_BOUND: &str = "SurfaceBound";

pub(super) const ABI_EVENT_LABEL_REGISTERED_SIGNATURE: &str =
    "LabelRegistered(uint256,bytes32,string,address,uint64,address)";
pub(super) const ABI_EVENT_LABEL_RESERVED_SIGNATURE: &str =
    "LabelReserved(uint256,bytes32,string,uint64,address)";
pub(super) const ABI_EVENT_LABEL_UNREGISTERED_SIGNATURE: &str =
    "LabelUnregistered(uint256,address)";
pub(super) const ABI_EVENT_EXPIRY_UPDATED_SIGNATURE: &str = "ExpiryUpdated(uint256,uint64,address)";
pub(super) const ABI_EVENT_SUBREGISTRY_UPDATED_SIGNATURE: &str =
    "SubregistryUpdated(uint256,address,address)";
pub(super) const ABI_EVENT_RESOLVER_UPDATED_SIGNATURE: &str =
    "ResolverUpdated(uint256,address,address)";
pub(super) const ABI_EVENT_TOKEN_REGENERATED_SIGNATURE: &str = "TokenRegenerated(uint256,uint256)";
pub(super) const ABI_EVENT_PARENT_UPDATED_SIGNATURE: &str = "ParentUpdated(address,string,address)";
pub(super) const ABI_EVENT_TOKEN_RESOURCE_SIGNATURE: &str = "TokenResource(uint256,uint256)";

pub(super) const ABI_EVENT_SIGNATURES: [&str; 9] = [
    ABI_EVENT_LABEL_REGISTERED_SIGNATURE,
    ABI_EVENT_LABEL_RESERVED_SIGNATURE,
    ABI_EVENT_LABEL_UNREGISTERED_SIGNATURE,
    ABI_EVENT_EXPIRY_UPDATED_SIGNATURE,
    ABI_EVENT_SUBREGISTRY_UPDATED_SIGNATURE,
    ABI_EVENT_RESOLVER_UPDATED_SIGNATURE,
    ABI_EVENT_TOKEN_RESOURCE_SIGNATURE,
    ABI_EVENT_TOKEN_REGENERATED_SIGNATURE,
    ABI_EVENT_PARENT_UPDATED_SIGNATURE,
];
