use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    time::Instant,
};

use crate::normalized_event_support::count_events_by_kind;
use crate::registry_migration_cache::MigratedRegistryNodes;
use anyhow::{Context, Result, bail};
use bigname_storage::{
    CanonicalityState, NameSurface, NormalizedEvent, Resource, SurfaceBinding, SurfaceBindingKind,
    TokenLineage, upsert_name_surfaces_without_snapshots, upsert_normalized_events_count_only,
    upsert_resources_without_snapshots, upsert_surface_bindings_without_snapshots,
    upsert_token_lineages_without_snapshots,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sqlx::{
    PgPool, Row,
    types::{Uuid, time::OffsetDateTime},
};

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
pub struct EnsV1TextRecordChange {
    pub record_key: String,
    pub record_family: String,
    pub selector_key: String,
    pub value: Option<String>,
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
struct GenericResolverEventSource {
    source_manifest_id: i64,
    namespace: String,
    source_family: String,
    manifest_version: i64,
    normalizer_version: String,
    effective_from_block: Option<i64>,
    effective_to_block: Option<i64>,
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ObservationRef {
    chain_id: String,
    block_hash: String,
    block_number: i64,
    #[serde(with = "time::serde::timestamp")]
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct RawLogPosition {
    block_hash: String,
    transaction_hash: String,
    log_index: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NameRegistrationObservation {
    label: String,
    labelhash: String,
    registrant: String,
    #[serde(with = "time::serde::timestamp")]
    expiry: OffsetDateTime,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct NameRenewalObservation {
    label: String,
    labelhash: String,
    #[serde(with = "time::serde::timestamp")]
    expiry: OffsetDateTime,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct TokenTransferObservation {
    labelhash: String,
    from_address: String,
    to_address: String,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct RegistryOwnerObservation {
    labelhash: String,
    owner: String,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ResolverObservation {
    namehash: String,
    resolver: String,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct RecordSelector {
    record_key: String,
    record_family: String,
    selector_key: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct RecordChangeObservation {
    namehash: String,
    resolver: String,
    selector: RecordSelector,
    value: Option<Value>,
    raw_name: Option<String>,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct RecordVersionObservation {
    namehash: String,
    resolver: String,
    record_version: i64,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct WrapperNameWrappedObservation {
    name: NameMetadata,
    owner: String,
    fuses: i64,
    #[serde(with = "time::serde::timestamp")]
    expiry: OffsetDateTime,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct WrapperNameUnwrappedObservation {
    namehash: String,
    owner: String,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct WrapperFusesObservation {
    namehash: String,
    fuses: i64,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct WrapperExpiryObservation {
    namehash: String,
    #[serde(with = "time::serde::timestamp")]
    expiry: OffsetDateTime,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct WrapperTokenTransferObservation {
    namehash: String,
    from_address: String,
    to_address: String,
    value: i64,
    reference: ObservationRef,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ReverseClaimProvenance {
    source_family: String,
    contract_role: String,
    contract_instance_id: Option<String>,
    emitting_address: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ReverseClaimSource {
    address: String,
    namespace: String,
    coin_type: String,
    reverse_name: String,
    reverse_node: String,
    claim_provenance: ReverseClaimProvenance,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

#[derive(Clone, Debug, Deserialize, Serialize)]
struct RegistrationLease {
    authority_key: String,
    labelhash: String,
    registrant: String,
    #[serde(with = "time::serde::timestamp")]
    expiry: OffsetDateTime,
    release_ref: Option<BoundaryRef>,
    start_ref: ObservationRef,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct WrapperAuthority {
    authority_key: String,
    node: String,
    owner: String,
    fuses: i64,
    #[serde(with = "time::serde::timestamp")]
    expiry: OffsetDateTime,
    start_ref: ObservationRef,
    end_ref: Option<ObservationRef>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct BoundaryRef {
    chain_id: String,
    block_hash: String,
    block_number: i64,
    #[serde(with = "time::serde::timestamp")]
    block_timestamp: OffsetDateTime,
    canonicality_state: CanonicalityState,
    namespace: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

#[derive(Clone, Debug, Deserialize, Serialize)]
struct AuthorityAnchor {
    kind: AuthorityKind,
    authority_key: String,
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    binding_source_family: String,
    binding_manifest_version: i64,
    binding_manifest_id: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct OpenBinding {
    surface_binding_id: Uuid,
    authority: AuthorityAnchor,
    #[serde(with = "time::serde::timestamp")]
    active_from: OffsetDateTime,
    anchor_ref: BoundaryRef,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BindingSegment {
    surface_binding_id: Uuid,
    authority: AuthorityAnchor,
    #[serde(with = "time::serde::timestamp")]
    active_from: OffsetDateTime,
    #[serde(with = "time::serde::timestamp::option")]
    active_to: Option<OffsetDateTime>,
    anchor_ref: BoundaryRef,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct NameHistory {
    name: Option<NameMetadata>,
    labelhash: String,
    first_name_ref: Option<ObservationRef>,
    current_registration: Option<RegistrationLease>,
    superseded_registration: Option<RegistrationLease>,
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

fn source_manifest_id_if_known(source_manifest_id: i64) -> Option<i64> {
    (source_manifest_id > 0).then_some(source_manifest_id)
}

mod abi;
mod apply;
mod apply_registrar;
mod apply_registry;
mod apply_resolver;
mod apply_wrapper;
mod checkpoint;
mod constants;
mod event_builders;
mod event_state;
mod event_topics;
mod finalization;
mod ids;
mod loading;
mod materialization;
mod migration_guard;
mod names;
mod observation;
mod permissions;
mod preload;
mod profiles;
mod release_events;
mod resolver_gate;
mod reverse_claims;
mod scope;
mod transition;

pub use self::pipeline::{
    sync_ens_v1_unwrapped_authority,
    sync_ens_v1_unwrapped_authority_with_replay_checkpoint_and_log_limit,
};
pub use checkpoint::clear_replay_adapter_checkpoints;

use self::{
    abi::*, apply::*, apply_registrar::*, apply_registry::*, apply_resolver::*, apply_wrapper::*,
    checkpoint::*, event_builders::*, event_state::*, event_topics::*, finalization::*, ids::*,
    loading::*, materialization::*, migration_guard::*, names::*, observation::*, permissions::*,
    preload::*, profiles::*, release_events::*, resolver_gate::*, reverse_claims::*, scope::*,
    transition::*,
};
use constants::*;

pub fn decode_ens_v1_text_record_change(
    topics: &[String],
    data: &[u8],
) -> Result<Option<EnsV1TextRecordChange>> {
    let raw_log = AuthorityRawLogRow {
        chain_id: String::new(),
        block_hash: String::new(),
        block_number: 0,
        block_timestamp: OffsetDateTime::UNIX_EPOCH,
        transaction_hash: String::new(),
        transaction_index: 0,
        log_index: 0,
        emitting_address: String::new(),
        topics: topics.to_vec(),
        data: data.to_vec(),
        canonicality_state: CanonicalityState::Observed,
        source_manifest_id: 0,
        namespace: String::new(),
        source_family: SOURCE_FAMILY_ENS_V1_RESOLVER_L1.to_owned(),
        manifest_version: 0,
        normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
        contract_role: None,
    };
    observation::decode_text_record_change(
        &raw_log,
        &AuthorityEventTopics::for_ens_v1_text_decoding(),
    )
}

mod pipeline;

#[cfg(test)]
mod tests;
