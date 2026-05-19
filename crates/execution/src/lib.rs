//! ENS verified-resolution exact-surface execution persistence bootstrap.

mod ens_resolution;
mod ens_resolution_abi;
mod ens_resolution_call;
mod ens_resolution_ccip;
mod ens_text_records;
mod json_helpers;
mod persistence;
mod primary_name;
mod revalidation;
mod rpc;
mod validation;

pub use bigname_storage::{
    CanonicalityState, ExecutionTraceStep, load_execution_outcome, load_execution_trace,
    load_raw_call_snapshots_by_block_hash,
};
pub use ens_resolution::{
    EnsResolutionRecord, OnDemandEnsResolutionError, OnDemandEnsResolutionErrorKind,
    OnDemandEnsResolutionRequest, execute_ens_universal_resolver_verified_resolution,
};
pub use ens_text_records::{
    EnsTextRecordMulticallBlock, EnsTextRecordMulticallRequest, EnsTextRecordMulticallResult,
    MULTICALL3_ADDRESS, ens_namehash_hex, execute_ens_text_record_multicall,
};
pub use persistence::{
    LoadedEnsVerifiedPrimaryName, PersistEnsExactNameVerifiedResolutionRequest,
    PersistEnsVerifiedPrimaryNameRequest, PersistedVerifiedPrimaryNameIdentity,
    PersistedVerifiedResolutionIdentity, VerifiedPrimaryNameReadbackProvenance,
    load_persisted_ens_verified_primary_name,
    persist_basenames_exact_name_verified_resolution_transport_direct,
    persist_ens_exact_name_verified_resolution_direct, persist_ens_verified_primary_name,
};
pub use rpc::ChainRpcUrls;

pub const VERIFIED_RESOLUTION_REQUEST_TYPE: &str = "verified_resolution";
pub const VERIFIED_PRIMARY_NAME_REQUEST_TYPE: &str = "verified_primary_name";
pub const ENS_NAMESPACE: &str = bigname_storage::ENS_NAMESPACE;
pub const BASENAMES_NAMESPACE: &str = bigname_storage::BASENAMES_NAMESPACE;
pub const BASE_MAINNET_CHAIN_ID: &str = bigname_storage::BASE_MAINNET_CHAIN_ID;
pub const ETHEREUM_MAINNET_CHAIN_ID: &str = bigname_storage::ETHEREUM_MAINNET_CHAIN_ID;
pub const ENS_EXECUTION_SOURCE_FAMILY: &str = "ens_execution";
pub const ENS_UNIVERSAL_RESOLVER_ROLE: &str = "universal_resolver";
pub const ENS_UNIVERSAL_RESOLVER_ADDRESS: &str = "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe";
pub const BASENAMES_EXECUTION_SOURCE_FAMILY: &str = "basenames_execution";
pub const BASENAMES_L1_RESOLVER_ROLE: &str = "l1_resolver";
pub const BASENAMES_L1_RESOLVER_ADDRESS: &str = bigname_storage::BASENAMES_L1_RESOLVER_ADDRESS;
pub const DECLARED_REGISTRY_PATH_BINDING_KIND: &str = "declared_registry_path";
pub const LINKED_SUBREGISTRY_PATH_BINDING_KIND: &str = "linked_subregistry_path";
pub const RESOLVER_ALIAS_PATH_BINDING_KIND: &str = "resolver_alias_path";
pub const OBSERVED_WILDCARD_PATH_BINDING_KIND: &str = "observed_wildcard_path";
pub const MIGRATION_REBIND_BINDING_KIND: &str = "migration_rebind";
pub const OBSERVED_ONLY_BINDING_KIND: &str = "observed_only";

/// Current execution bootstrap status.
pub const fn bootstrap_status() -> &'static str {
    "ens-direct-and-basenames-transport-verified-resolution-producer-ready"
}

#[cfg(test)]
mod tests;
