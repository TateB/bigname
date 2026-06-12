pub(super) const DERIVATION_KIND_RAW_LOG_PREIMAGE_OBSERVATION: &str =
    "raw_log_preimage_observation";
pub(super) const EVENT_KIND_PREIMAGE_OBSERVED: &str = "PreimageObserved";
pub(super) const SOURCE_FAMILY_ENS_V1_REGISTRAR_L1: &str = "ens_v1_registrar_l1";
pub(super) const SOURCE_FAMILY_ENS_V1_WRAPPER_L1: &str = "ens_v1_wrapper_l1";
pub(super) const SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR: &str = "basenames_base_registrar";
pub(super) const SOURCE_FAMILY_ENS_V2_ROOT_L1: &str = "ens_v2_root_l1";
pub(super) const SOURCE_FAMILY_ENS_V2_REGISTRY_L1: &str = "ens_v2_registry_l1";
pub(super) const SOURCE_FAMILY_ENS_V2_REGISTRAR_L1: &str = "ens_v2_registrar_l1";
pub(super) const SOURCE_FAMILY_ENS_V2_RESOLVER_L1: &str = "ens_v2_resolver_l1";
pub(super) const SOURCE_EVENT_LABEL_REGISTERED: &str = "LabelRegistered";
pub(super) const SOURCE_EVENT_LABEL_RESERVED: &str = "LabelReserved";
pub(super) const SOURCE_EVENT_PARENT_UPDATED: &str = "ParentUpdated";
pub(super) const SOURCE_EVENT_NAME_REGISTERED: &str = "NameRegistered";
pub(super) const SOURCE_EVENT_NAME_RENEWED: &str = "NameRenewed";
pub(super) const SOURCE_EVENT_NAME_WRAPPED: &str = "NameWrapped";
pub(super) const SOURCE_EVENT_ALIAS_CHANGED: &str = "AliasChanged";
pub(super) const SOURCE_EVENT_NAMED_RESOURCE: &str = "NamedResource";
pub(super) const SOURCE_EVENT_NAMED_TEXT_RESOURCE: &str = "NamedTextResource";
pub(super) const SOURCE_EVENT_NAMED_ADDR_RESOURCE: &str = "NamedAddrResource";
pub(super) const NAME_WRAPPED_SIGNATURE: &str = "NameWrapped(bytes32,bytes,address,uint32,uint64)";
pub(super) const ENS_V1_NAME_REGISTERED_SIGNATURE: &str =
    "NameRegistered(string,bytes32,address,uint256,uint256)";
pub(super) const ENS_V1_WRAPPED_NAME_REGISTERED_SIGNATURE: &str =
    "NameRegistered(string,bytes32,address,uint256,uint256,uint256)";
pub(super) const ENS_V1_UNWRAPPED_NAME_REGISTERED_SIGNATURE: &str =
    "NameRegistered(string,bytes32,address,uint256,uint256,uint256,bytes32)";
pub(super) const ENS_V1_NAME_RENEWED_SIGNATURE: &str =
    "NameRenewed(string,bytes32,uint256,uint256)";
pub(super) const ENS_V1_UNWRAPPED_NAME_RENEWED_SIGNATURE: &str =
    "NameRenewed(string,bytes32,uint256,uint256,bytes32)";
pub(super) const BASENAMES_NAME_REGISTERED_SIGNATURE: &str =
    "NameRegistered(string,bytes32,address,uint256)";
pub(super) const BASENAMES_NAME_RENEWED_SIGNATURE: &str = "NameRenewed(string,bytes32,uint256)";
pub(super) const LABEL_REGISTERED_SIGNATURE: &str =
    "LabelRegistered(uint256,bytes32,string,address,uint64,address)";
pub(super) const LABEL_RESERVED_SIGNATURE: &str =
    "LabelReserved(uint256,bytes32,string,uint64,address)";
pub(super) const PARENT_UPDATED_SIGNATURE: &str = "ParentUpdated(address,string,address)";
pub(super) const ENS_V2_NAME_REGISTERED_SIGNATURE: &str =
    "NameRegistered(uint256,string,address,address,address,uint64,address,bytes32,uint256,uint256)";
pub(super) const ENS_V2_NAME_RENEWED_SIGNATURE: &str =
    "NameRenewed(uint256,string,uint64,uint64,address,bytes32,uint256)";
pub(super) const ALIAS_CHANGED_SIGNATURE: &str = "AliasChanged(bytes,bytes,bytes,bytes)";
pub(super) const NAMED_RESOURCE_SIGNATURE: &str = "NamedResource(uint256,bytes)";
pub(super) const NAMED_TEXT_RESOURCE_SIGNATURE: &str =
    "NamedTextResource(uint256,bytes,bytes32,string)";
pub(super) const NAMED_ADDR_RESOURCE_SIGNATURE: &str = "NamedAddrResource(uint256,bytes,uint256)";

pub(super) const ENS_V1_WRAPPER_PREIMAGE_EVENT_SIGNATURES: [&str; 1] = [NAME_WRAPPED_SIGNATURE];
pub(super) const ENS_V1_REGISTRAR_PREIMAGE_EVENT_SIGNATURES: [&str; 5] = [
    ENS_V1_NAME_REGISTERED_SIGNATURE,
    ENS_V1_WRAPPED_NAME_REGISTERED_SIGNATURE,
    ENS_V1_UNWRAPPED_NAME_REGISTERED_SIGNATURE,
    ENS_V1_NAME_RENEWED_SIGNATURE,
    ENS_V1_UNWRAPPED_NAME_RENEWED_SIGNATURE,
];
pub(super) const BASENAMES_REGISTRAR_PREIMAGE_EVENT_SIGNATURES: [&str; 2] = [
    BASENAMES_NAME_REGISTERED_SIGNATURE,
    BASENAMES_NAME_RENEWED_SIGNATURE,
];
pub(super) const ENS_V2_REGISTRY_PREIMAGE_EVENT_SIGNATURES: [&str; 3] = [
    LABEL_REGISTERED_SIGNATURE,
    LABEL_RESERVED_SIGNATURE,
    PARENT_UPDATED_SIGNATURE,
];
pub(super) const ENS_V2_REGISTRAR_PREIMAGE_EVENT_SIGNATURES: [&str; 2] = [
    ENS_V2_NAME_REGISTERED_SIGNATURE,
    ENS_V2_NAME_RENEWED_SIGNATURE,
];
pub(super) const ENS_V2_RESOLVER_PREIMAGE_EVENT_SIGNATURES: [&str; 4] = [
    ALIAS_CHANGED_SIGNATURE,
    NAMED_RESOURCE_SIGNATURE,
    NAMED_TEXT_RESOURCE_SIGNATURE,
    NAMED_ADDR_RESOURCE_SIGNATURE,
];
pub(super) const PREIMAGE_EVENT_SIGNATURES: [&str; 17] = [
    NAME_WRAPPED_SIGNATURE,
    ENS_V1_NAME_REGISTERED_SIGNATURE,
    ENS_V1_WRAPPED_NAME_REGISTERED_SIGNATURE,
    ENS_V1_UNWRAPPED_NAME_REGISTERED_SIGNATURE,
    ENS_V1_NAME_RENEWED_SIGNATURE,
    ENS_V1_UNWRAPPED_NAME_RENEWED_SIGNATURE,
    BASENAMES_NAME_REGISTERED_SIGNATURE,
    BASENAMES_NAME_RENEWED_SIGNATURE,
    LABEL_REGISTERED_SIGNATURE,
    LABEL_RESERVED_SIGNATURE,
    PARENT_UPDATED_SIGNATURE,
    ENS_V2_NAME_REGISTERED_SIGNATURE,
    ENS_V2_NAME_RENEWED_SIGNATURE,
    ALIAS_CHANGED_SIGNATURE,
    NAMED_RESOURCE_SIGNATURE,
    NAMED_TEXT_RESOURCE_SIGNATURE,
    NAMED_ADDR_RESOURCE_SIGNATURE,
];
