pub(super) const SOURCE_FAMILY_ENS_V2_RESOLVER_L1: &str = "ens_v2_resolver_l1";
pub(super) const DERIVATION_KIND_ENS_V2_RESOLVER: &str = "ens_v2_resolver";
pub(super) const DERIVATION_KIND_RAW_LOG_PREIMAGE_OBSERVATION: &str =
    "raw_log_preimage_observation";
pub(super) const RESOLVER_EDGE_KIND: &str = "resolver";

pub(super) const EVENT_KIND_PREIMAGE_OBSERVED: &str = "PreimageObserved";
pub(super) const EVENT_KIND_ALIAS_CHANGED: &str = "AliasChanged";
pub(super) const EVENT_KIND_RECORD_CHANGED: &str = "RecordChanged";
pub(super) const EVENT_KIND_RECORD_VERSION_CHANGED: &str = "RecordVersionChanged";

pub(super) const ABI_EVENT_ADDRESS_CHANGED_SIGNATURE: &str =
    "AddressChanged(bytes32,uint256,bytes)";
pub(super) const ABI_EVENT_TEXT_CHANGED_SIGNATURE: &str =
    "TextChanged(bytes32,string,string,string)";
pub(super) const ABI_EVENT_CONTENTHASH_CHANGED_SIGNATURE: &str =
    "ContenthashChanged(bytes32,bytes)";
pub(super) const ABI_EVENT_NAME_CHANGED_SIGNATURE: &str = "NameChanged(bytes32,string)";
pub(super) const ABI_EVENT_VERSION_CHANGED_SIGNATURE: &str = "VersionChanged(bytes32,uint64)";
pub(super) const ABI_EVENT_ALIAS_CHANGED_SIGNATURE: &str = "AliasChanged(bytes,bytes,bytes,bytes)";
pub(super) const ABI_EVENT_NAMED_RESOURCE_SIGNATURE: &str = "NamedResource(uint256,bytes)";
pub(super) const ABI_EVENT_NAMED_TEXT_RESOURCE_SIGNATURE: &str =
    "NamedTextResource(uint256,bytes,bytes32,string)";
pub(super) const ABI_EVENT_NAMED_ADDR_RESOURCE_SIGNATURE: &str =
    "NamedAddrResource(uint256,bytes,uint256)";

pub(super) const ABI_EVENT_SIGNATURES: [&str; 9] = [
    ABI_EVENT_ADDRESS_CHANGED_SIGNATURE,
    ABI_EVENT_TEXT_CHANGED_SIGNATURE,
    ABI_EVENT_CONTENTHASH_CHANGED_SIGNATURE,
    ABI_EVENT_NAME_CHANGED_SIGNATURE,
    ABI_EVENT_VERSION_CHANGED_SIGNATURE,
    ABI_EVENT_ALIAS_CHANGED_SIGNATURE,
    ABI_EVENT_NAMED_RESOURCE_SIGNATURE,
    ABI_EVENT_NAMED_TEXT_RESOURCE_SIGNATURE,
    ABI_EVENT_NAMED_ADDR_RESOURCE_SIGNATURE,
];
