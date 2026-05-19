pub(super) const SOURCE_FAMILY_ENS_V1_REGISTRAR_L1: &str = "ens_v1_registrar_l1";
pub(super) const SOURCE_FAMILY_ENS_V1_REGISTRY_L1: &str = "ens_v1_registry_l1";
pub(super) const SOURCE_FAMILY_ENS_V1_RESOLVER_L1: &str = "ens_v1_resolver_l1";
pub(super) const SOURCE_FAMILY_ENS_V1_WRAPPER_L1: &str = "ens_v1_wrapper_l1";
pub(super) const SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR: &str = "basenames_base_registrar";
pub(super) const SOURCE_FAMILY_BASENAMES_BASE_REGISTRY: &str = "basenames_base_registry";
pub(super) const SOURCE_FAMILY_BASENAMES_BASE_RESOLVER: &str = "basenames_base_resolver";
pub(super) const CONTRACT_ROLE_REGISTRY_OLD: &str = "registry_old";
pub(super) const GENERIC_SOURCE_SCOPE_ADDRESS: &str = "*";

pub(super) const DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY: &str = "ens_v1_unwrapped_authority";
pub(super) const EVENT_KIND_AUTHORITY_EPOCH_CHANGED: &str = "AuthorityEpochChanged";
pub(super) const EVENT_KIND_AUTHORITY_TRANSFERRED: &str = "AuthorityTransferred";
pub(super) const EVENT_KIND_EXPIRY_CHANGED: &str = "ExpiryChanged";
pub(super) const EVENT_KIND_PERMISSION_CHANGED: &str = "PermissionChanged";
pub(super) const EVENT_KIND_PERMISSION_SCOPE_CHANGED: &str = "PermissionScopeChanged";
pub(super) const EVENT_KIND_RECORD_CHANGED: &str = "RecordChanged";
pub(super) const EVENT_KIND_RECORD_VERSION_CHANGED: &str = "RecordVersionChanged";
pub(super) const EVENT_KIND_REGISTRATION_GRANTED: &str = "RegistrationGranted";
pub(super) const EVENT_KIND_REGISTRATION_RELEASED: &str = "RegistrationReleased";
pub(super) const EVENT_KIND_REGISTRATION_RENEWED: &str = "RegistrationRenewed";
pub(super) const EVENT_KIND_RESOLVER_CHANGED: &str = "ResolverChanged";
pub(super) const EVENT_KIND_SURFACE_BOUND: &str = "SurfaceBound";
pub(super) const EVENT_KIND_SURFACE_UNBOUND: &str = "SurfaceUnbound";
pub(super) const EVENT_KIND_TOKEN_CONTROL_TRANSFERRED: &str = "TokenControlTransferred";

pub(super) const NAME_REGISTERED_SIGNATURE: &str =
    "NameRegistered(string,bytes32,address,uint256,uint256)";
pub(super) const WRAPPED_NAME_REGISTERED_SIGNATURE: &str =
    "NameRegistered(string,bytes32,address,uint256,uint256,uint256)";
pub(super) const UNWRAPPED_NAME_REGISTERED_SIGNATURE: &str =
    "NameRegistered(string,bytes32,address,uint256,uint256,uint256,bytes32)";
pub(super) const BASENAMES_NAME_REGISTERED_SIGNATURE: &str =
    "NameRegistered(string,bytes32,address,uint256)";
pub(super) const NAME_RENEWED_SIGNATURE: &str = "NameRenewed(string,bytes32,uint256,uint256)";
pub(super) const UNWRAPPED_NAME_RENEWED_SIGNATURE: &str =
    "NameRenewed(string,bytes32,uint256,uint256,bytes32)";
pub(super) const BASENAMES_NAME_RENEWED_SIGNATURE: &str = "NameRenewed(string,bytes32,uint256)";
pub(super) const ADDR_CHANGED_SIGNATURE: &str = "AddrChanged(bytes32,address)";
pub(super) const ADDRESS_CHANGED_SIGNATURE: &str = "AddressChanged(bytes32,uint256,bytes)";
pub(super) const NAME_CHANGED_SIGNATURE: &str = "NameChanged(bytes32,string)";
pub(super) const NEW_RESOLVER_SIGNATURE: &str = "NewResolver(bytes32,address)";
pub(super) const ABI_CHANGED_SIGNATURE: &str = "ABIChanged(bytes32,uint256)";
pub(super) const TEXT_CHANGED_WITHOUT_VALUE_SIGNATURE: &str = "TextChanged(bytes32,string,string)";
pub(super) const TEXT_CHANGED_WITH_VALUE_SIGNATURE: &str =
    "TextChanged(bytes32,string,string,string)";
pub(super) const CONTENT_CHANGED_SIGNATURE: &str = "ContentChanged(bytes32,bytes32)";
pub(super) const CONTENTHASH_CHANGED_SIGNATURE: &str = "ContenthashChanged(bytes32,bytes)";
pub(super) const DNS_RECORD_CHANGED_SIGNATURE: &str =
    "DNSRecordChanged(bytes32,bytes,uint16,bytes)";
pub(super) const DNS_RECORD_DELETED_SIGNATURE: &str = "DNSRecordDeleted(bytes32,bytes,uint16)";
pub(super) const DNS_ZONEHASH_CHANGED_SIGNATURE: &str = "DNSZonehashChanged(bytes32,bytes,bytes)";
pub(super) const DATA_CHANGED_SIGNATURE: &str = "DataChanged(bytes32,string,string,bytes)";
pub(super) const INTERFACE_CHANGED_SIGNATURE: &str = "InterfaceChanged(bytes32,bytes4,address)";
#[cfg(test)]
pub(super) const PUBKEY_CHANGED_SIGNATURE: &str = "PubkeyChanged(bytes32,bytes32,bytes32)";
pub(super) const TRANSFER_SIGNATURE: &str = "Transfer(address,address,uint256)";
pub(super) const REGISTRY_TRANSFER_SIGNATURE: &str = "Transfer(bytes32,address)";
pub(super) const NEW_OWNER_SIGNATURE: &str = "NewOwner(bytes32,bytes32,address)";
pub(super) const NEW_TTL_SIGNATURE: &str = "NewTTL(bytes32,uint64)";
pub(super) const VERSION_CHANGED_SIGNATURE: &str = "VersionChanged(bytes32,uint64)";
pub(super) const NAME_WRAPPED_SIGNATURE: &str = "NameWrapped(bytes32,bytes,address,uint32,uint64)";
pub(super) const NAME_UNWRAPPED_SIGNATURE: &str = "NameUnwrapped(bytes32,address)";
pub(super) const FUSES_SET_SIGNATURE: &str = "FusesSet(bytes32,uint32)";
pub(super) const EXPIRY_EXTENDED_SIGNATURE: &str = "ExpiryExtended(bytes32,uint64)";
pub(super) const TRANSFER_SINGLE_SIGNATURE: &str =
    "TransferSingle(address,address,address,uint256,uint256)";

pub(super) const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
pub(super) const ENS_NORMALIZER_VERSION: &str = "ensip15@2026-04-16";
pub(super) const ENS_GRACE_PERIOD_SECS: i64 = 90 * 24 * 60 * 60;
pub(super) const ENS_NATIVE_COIN_TYPE: &str = "60";
pub(super) const EVENT_KIND_REVERSE_CHANGED: &str = "ReverseChanged";
pub(super) const PERMISSION_POWER_RESOURCE_CONTROL: &str = "resource_control";
pub(super) const PERMISSION_POWER_RESOLVER_CONTROL: &str = "resolver_control";
pub(super) const PERMISSION_TRANSFER_BEHAVIOR: &str = "replace_on_authority_change";
pub(super) const CONTRACT_ROLE_REVERSE_REGISTRAR: &str = "reverse_registrar";
pub(super) const DERIVATION_KIND_ENS_V1_REVERSE_CLAIM: &str = "ens_v1_reverse_claim";
