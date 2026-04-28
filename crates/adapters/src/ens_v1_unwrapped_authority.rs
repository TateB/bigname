use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::registry_migration_cache::MigratedRegistryNodes;
use anyhow::{Context, Result, bail};
use bigname_storage::{
    CanonicalityState, NameSurface, NormalizedEvent, Resource, SurfaceBinding, SurfaceBindingKind,
    TokenLineage, load_name_surface_including_noncanonical, load_resource_including_noncanonical,
    load_surface_binding_including_noncanonical, load_token_lineage_including_noncanonical,
    upsert_name_surfaces, upsert_normalized_events_with_summary, upsert_resources,
    upsert_surface_bindings, upsert_token_lineages,
};
use serde_json::{Map, Value, json};
use sha3::{Digest, Keccak256};
use sqlx::{
    PgPool, Row,
    types::{Uuid, time::OffsetDateTime},
};

const SOURCE_FAMILY_ENS_V1_REGISTRAR_L1: &str = "ens_v1_registrar_l1";
const SOURCE_FAMILY_ENS_V1_REGISTRY_L1: &str = "ens_v1_registry_l1";
const SOURCE_FAMILY_ENS_V1_RESOLVER_L1: &str = "ens_v1_resolver_l1";
const SOURCE_FAMILY_ENS_V1_WRAPPER_L1: &str = "ens_v1_wrapper_l1";
const SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR: &str = "basenames_base_registrar";
const SOURCE_FAMILY_BASENAMES_BASE_REGISTRY: &str = "basenames_base_registry";
const SOURCE_FAMILY_BASENAMES_BASE_RESOLVER: &str = "basenames_base_resolver";
const CONTRACT_ROLE_REGISTRY_OLD: &str = "registry_old";

const DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY: &str = "ens_v1_unwrapped_authority";
const EVENT_KIND_AUTHORITY_EPOCH_CHANGED: &str = "AuthorityEpochChanged";
const EVENT_KIND_AUTHORITY_TRANSFERRED: &str = "AuthorityTransferred";
const EVENT_KIND_EXPIRY_CHANGED: &str = "ExpiryChanged";
const EVENT_KIND_PERMISSION_CHANGED: &str = "PermissionChanged";
const EVENT_KIND_PERMISSION_SCOPE_CHANGED: &str = "PermissionScopeChanged";
const EVENT_KIND_RECORD_CHANGED: &str = "RecordChanged";
const EVENT_KIND_RECORD_VERSION_CHANGED: &str = "RecordVersionChanged";
const EVENT_KIND_REGISTRATION_GRANTED: &str = "RegistrationGranted";
const EVENT_KIND_REGISTRATION_RELEASED: &str = "RegistrationReleased";
const EVENT_KIND_REGISTRATION_RENEWED: &str = "RegistrationRenewed";
const EVENT_KIND_RESOLVER_CHANGED: &str = "ResolverChanged";
const EVENT_KIND_SURFACE_BOUND: &str = "SurfaceBound";
const EVENT_KIND_SURFACE_UNBOUND: &str = "SurfaceUnbound";
const EVENT_KIND_TOKEN_CONTROL_TRANSFERRED: &str = "TokenControlTransferred";

const NAME_REGISTERED_SIGNATURE: &str = "NameRegistered(string,bytes32,address,uint256,uint256)";
const WRAPPED_NAME_REGISTERED_SIGNATURE: &str =
    "NameRegistered(string,bytes32,address,uint256,uint256,uint256)";
const UNWRAPPED_NAME_REGISTERED_SIGNATURE: &str =
    "NameRegistered(string,bytes32,address,uint256,uint256,uint256,bytes32)";
const NAME_RENEWED_SIGNATURE: &str = "NameRenewed(string,bytes32,uint256,uint256)";
const UNWRAPPED_NAME_RENEWED_SIGNATURE: &str =
    "NameRenewed(string,bytes32,uint256,uint256,bytes32)";
const ADDR_CHANGED_SIGNATURE: &str = "AddrChanged(bytes32,address)";
const ADDRESS_CHANGED_SIGNATURE: &str = "AddressChanged(bytes32,uint256,bytes)";
const NAME_CHANGED_SIGNATURE: &str = "NameChanged(bytes32,string)";
const NEW_RESOLVER_SIGNATURE: &str = "NewResolver(bytes32,address)";
const TEXT_CHANGED_SIGNATURE: &str = "TextChanged(bytes32,string,string)";
const TRANSFER_SIGNATURE: &str = "Transfer(address,address,uint256)";
const REGISTRY_TRANSFER_SIGNATURE: &str = "Transfer(bytes32,address)";
const NEW_OWNER_SIGNATURE: &str = "NewOwner(bytes32,bytes32,address)";
const NEW_TTL_SIGNATURE: &str = "NewTTL(bytes32,uint64)";
const VERSION_CHANGED_SIGNATURE: &str = "VersionChanged(bytes32,uint64)";
const NAME_WRAPPED_SIGNATURE: &str = "NameWrapped(bytes32,bytes,address,uint32,uint64)";
const NAME_UNWRAPPED_SIGNATURE: &str = "NameUnwrapped(bytes32,address)";
const FUSES_SET_SIGNATURE: &str = "FusesSet(bytes32,uint32)";
const EXPIRY_EXTENDED_SIGNATURE: &str = "ExpiryExtended(bytes32,uint64)";
const TRANSFER_SINGLE_SIGNATURE: &str = "TransferSingle(address,address,address,uint256,uint256)";

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
const ENS_NORMALIZER_VERSION: &str = "ensip15@2026-04-16";
const ENS_GRACE_PERIOD_SECS: i64 = 90 * 24 * 60 * 60;
const ENS_NATIVE_COIN_TYPE: &str = "60";
const EVENT_KIND_REVERSE_CHANGED: &str = "ReverseChanged";
const PERMISSION_POWER_RESOURCE_CONTROL: &str = "resource_control";
const PERMISSION_POWER_RESOLVER_CONTROL: &str = "resolver_control";
const PERMISSION_TRANSFER_BEHAVIOR: &str = "replace_on_authority_change";
const CONTRACT_ROLE_REVERSE_REGISTRAR: &str = "reverse_registrar";
const DERIVATION_KIND_ENS_V1_REVERSE_CLAIM: &str = "ens_v1_reverse_claim";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV1UnwrappedAuthoritySyncSummary {
    pub scanned_log_count: usize,
    pub matched_log_count: usize,
    pub total_name_surface_count: usize,
    pub total_resource_count: usize,
    pub total_surface_binding_count: usize,
    pub total_normalized_event_count: usize,
    pub total_normalized_event_inserted_count: usize,
    pub by_kind: BTreeMap<String, usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveEmitter {
    address: String,
    contract_instance_id: Uuid,
    source_manifest_id: i64,
    namespace: String,
    source_family: String,
    manifest_version: i64,
    normalizer_version: String,
    contract_role: Option<String>,
    active_from_block_number: Option<i64>,
    active_to_block_number: Option<i64>,
    source_rank: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveManifestMetadata {
    manifest_id: i64,
    chain: String,
    namespace: String,
    source_family: String,
    manifest_version: i64,
    normalizer_version: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RawBlockSnapshot {
    chain_id: String,
    block_hash: String,
    block_number: i64,
    block_timestamp: OffsetDateTime,
    canonicality_state: CanonicalityState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AuthorityRawLogRow {
    chain_id: String,
    block_hash: String,
    block_number: i64,
    block_timestamp: OffsetDateTime,
    transaction_hash: String,
    transaction_index: i64,
    log_index: i64,
    emitting_address: String,
    topics: Vec<String>,
    data: Vec<u8>,
    canonicality_state: CanonicalityState,
    source_manifest_id: i64,
    namespace: String,
    source_family: String,
    manifest_version: i64,
    normalizer_version: String,
    contract_role: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObservationRef {
    chain_id: String,
    block_hash: String,
    block_number: i64,
    block_timestamp: OffsetDateTime,
    transaction_hash: Option<String>,
    transaction_index: Option<i64>,
    log_index: Option<i64>,
    canonicality_state: CanonicalityState,
    namespace: String,
    source_manifest_id: i64,
    source_family: String,
    manifest_version: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RawLogPosition {
    block_hash: String,
    transaction_hash: String,
    log_index: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NameRegistrationObservation {
    label: String,
    labelhash: String,
    registrant: String,
    expiry: OffsetDateTime,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NameRenewalObservation {
    label: String,
    labelhash: String,
    expiry: OffsetDateTime,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TokenTransferObservation {
    labelhash: String,
    from_address: String,
    to_address: String,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RegistryOwnerObservation {
    labelhash: String,
    owner: String,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResolverObservation {
    namehash: String,
    resolver: String,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecordSelector {
    record_key: String,
    record_family: String,
    selector_key: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecordChangeObservation {
    namehash: String,
    resolver: String,
    selector: RecordSelector,
    value: Option<Value>,
    raw_name: Option<String>,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecordVersionObservation {
    namehash: String,
    resolver: String,
    record_version: i64,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WrapperNameWrappedObservation {
    name: NameMetadata,
    owner: String,
    fuses: i64,
    expiry: OffsetDateTime,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WrapperNameUnwrappedObservation {
    namehash: String,
    owner: String,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WrapperFusesObservation {
    namehash: String,
    fuses: i64,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WrapperExpiryObservation {
    namehash: String,
    expiry: OffsetDateTime,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WrapperTokenTransferObservation {
    namehash: String,
    from_address: String,
    to_address: String,
    value: i64,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum AuthorityObservation {
    RegistrationGranted(NameRegistrationObservation),
    RegistrationRenewed(NameRenewalObservation),
    TokenTransferred(TokenTransferObservation),
    RegistryOwnerChanged(RegistryOwnerObservation),
    ResolverChanged(ResolverObservation),
    RecordChanged(RecordChangeObservation),
    RecordVersionChanged(RecordVersionObservation),
    WrapperNameWrapped(WrapperNameWrappedObservation),
    WrapperNameUnwrapped(WrapperNameUnwrappedObservation),
    WrapperFusesSet(WrapperFusesObservation),
    WrapperExpiryExtended(WrapperExpiryObservation),
    WrapperTokenTransferred(WrapperTokenTransferObservation),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReverseClaimProvenance {
    source_family: String,
    contract_role: String,
    contract_instance_id: Option<String>,
    emitting_address: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReverseClaimSource {
    address: String,
    namespace: String,
    coin_type: String,
    reverse_name: String,
    reverse_node: String,
    claim_provenance: ReverseClaimProvenance,
}

#[derive(Clone, Debug)]
struct ReverseClaimSourceHistory {
    claim_source: ReverseClaimSource,
    current_resolver: Option<String>,
    current_record_version: Option<i64>,
    events: Vec<NormalizedEvent>,
}

impl ReverseClaimSource {
    fn as_value(&self) -> Value {
        json!({
            "address": self.address,
            "namespace": self.namespace,
            "coin_type": self.coin_type,
            "reverse_name": self.reverse_name,
            "reverse_node": self.reverse_node,
            "claim_provenance": {
                "source_family": self.claim_provenance.source_family,
                "contract_role": self.claim_provenance.contract_role,
                "contract_instance_id": self.claim_provenance.contract_instance_id,
                "emitting_address": self.claim_provenance.emitting_address,
            },
        })
    }
}

#[derive(Clone, Debug)]
struct CanonicalBlockIndex {
    blocks: Vec<RawBlockSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NameMetadata {
    namespace: String,
    logical_name_id: String,
    input_name: String,
    canonical_display_name: String,
    normalized_name: String,
    dns_encoded_name: Vec<u8>,
    namehash: String,
    labelhashes: Vec<String>,
    normalizer_version: String,
}

#[derive(Clone, Debug)]
struct RegistrationLease {
    authority_key: String,
    labelhash: String,
    registrant: String,
    expiry: OffsetDateTime,
    release_ref: Option<BoundaryRef>,
    start_ref: ObservationRef,
}

#[derive(Clone, Debug)]
struct WrapperAuthority {
    authority_key: String,
    node: String,
    owner: String,
    fuses: i64,
    expiry: OffsetDateTime,
    start_ref: ObservationRef,
    end_ref: Option<ObservationRef>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BoundaryRef {
    chain_id: String,
    block_hash: String,
    block_number: i64,
    block_timestamp: OffsetDateTime,
    canonicality_state: CanonicalityState,
    namespace: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AuthorityKind {
    RegistryOnly,
    Registrar,
    Wrapper,
}

impl AuthorityKind {
    const fn as_str(&self) -> &'static str {
        match self {
            Self::RegistryOnly => "registry_only",
            Self::Registrar => "registrar",
            Self::Wrapper => "wrapper",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PermissionAction {
    Grant,
    Revoke,
}

impl PermissionAction {
    const fn as_str(&self) -> &'static str {
        match self {
            Self::Grant => "grant",
            Self::Revoke => "revoke",
        }
    }
}

#[derive(Clone, Debug)]
struct AuthorityAnchor {
    kind: AuthorityKind,
    authority_key: String,
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    binding_source_family: String,
    binding_manifest_version: i64,
    binding_manifest_id: i64,
}

#[derive(Clone, Debug)]
struct OpenBinding {
    surface_binding_id: Uuid,
    authority: AuthorityAnchor,
    active_from: OffsetDateTime,
    anchor_ref: BoundaryRef,
}

#[derive(Clone, Debug)]
struct BindingSegment {
    surface_binding_id: Uuid,
    authority: AuthorityAnchor,
    active_from: OffsetDateTime,
    active_to: Option<OffsetDateTime>,
    anchor_ref: BoundaryRef,
}

#[derive(Clone, Debug)]
struct NameHistory {
    name: Option<NameMetadata>,
    labelhash: String,
    first_name_ref: Option<ObservationRef>,
    current_registration: Option<RegistrationLease>,
    current_wrapper_key: Option<String>,
    wrapper_authorities: BTreeMap<String, WrapperAuthority>,
    current_registry_owner: Option<String>,
    current_resolver: Option<String>,
    current_record_version: Option<i64>,
    open_binding: Option<OpenBinding>,
    bindings: Vec<BindingSegment>,
    events: Vec<NormalizedEvent>,
    registry_resource_anchor: Option<BoundaryRef>,
    latest_registry_owner_ref: Option<ObservationRef>,
    latest_registry_owner_before_registration: Option<ObservationRef>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AuthorityProfile {
    Ens,
    Basenames,
}
mod abi;
mod apply;
mod apply_registrar;
mod apply_registry;
mod apply_resolver;
mod apply_wrapper;
mod event_builders;
mod event_state;
mod finalization;
mod ids;
mod loading;
mod materialization;
mod migration_guard;
mod names;
mod observation;
mod permissions;
mod profiles;
mod release_events;
mod resolver_gate;
mod reverse_claims;
mod scope;
mod transition;

pub use self::pipeline::sync_ens_v1_unwrapped_authority;

use self::{
    abi::*, apply::*, apply_registrar::*, apply_registry::*, apply_resolver::*, apply_wrapper::*,
    event_builders::*, event_state::*, finalization::*, ids::*, loading::*, materialization::*,
    migration_guard::*, names::*, observation::*, permissions::*, profiles::*, release_events::*,
    resolver_gate::*, reverse_claims::*, scope::*, transition::*,
};

mod pipeline;

#[cfg(test)]
mod tests;
